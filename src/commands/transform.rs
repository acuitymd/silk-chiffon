use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::{
    AllColumnsBloomFilterConfig, BloomFilterConfig, DEFAULT_BLOOM_FILTER_FPP, DataFormat,
    ListOutputsFormat, PartitionStrategy, SortSpec, TransformCommand, default_thread_budget,
    io_strategies::{
        OutputFileInfo, input_strategy::InputStrategy, output_strategy::SinkFactory,
        path_template::PathTemplate,
    },
    operations::{query::QueryOperation, sort::SortOperation},
    pipeline::Pipeline,
    sinks::{
        arrow::{ArrowSink, ArrowSinkOptions},
        data_sink::DataSink,
        parquet::{ParquetRuntimes, ParquetSink, ParquetSinkOptions},
        vortex::{VortexSink, VortexSinkOptions},
    },
    sources::{
        arrow::ArrowDataSource, data_source::DataSource, parquet::ParquetDataSource,
        vortex::VortexDataSource,
    },
    utils::memory::{estimate_sort_spill_reservation, sample_avg_row_bytes},
};
use anyhow::{Result, anyhow};
use apply_if::ApplyIf;
use arrow::datatypes::SchemaRef;
use camino::Utf8Path;
use glob::glob;
use owo_colors::OwoColorize;
use tabled::{builder::Builder, settings::Style};

pub async fn run(args: TransformCommand) -> Result<()> {
    let TransformCommand {
        from,
        from_many,
        to,
        to_many,
        by,
        partition_strategy,
        max_open_partitions,
        exclude_columns,
        list_outputs,
        list_outputs_file,
        create_dirs,
        overwrite,
        query,
        dialect,
        sort_by,
        memory_budget,
        non_spillable_reserve,
        memory_pool_top_consumers,
        preserve_input_order,
        target_partitions,
        input_format,
        output_format,
        arrow_compression,
        arrow_format,
        arrow_record_batch_size,
        arrow_writing_queue_size,
        parquet_bloom_all,
        parquet_bloom_all_off,
        parquet_bloom_column,
        parquet_bloom_column_off,
        parquet_buffer_size,
        parquet_dictionary_column,
        parquet_column_encoding,
        parquet_column_encoding_threads,
        parquet_dictionary_column_off,
        parquet_compression,
        parquet_compression_level,
        parquet_ingestion_queue_size,
        parquet_encoding_queue_size,
        parquet_writing_queue_size,
        parquet_encoding,
        parquet_io_threads,
        parquet_dictionary_all_off,
        parquet_row_group_concurrency,
        parquet_row_group_size,
        parquet_sorted_metadata,
        parquet_statistics,
        parquet_writer_version,
        parquet_data_page_size,
        parquet_data_page_row_limit,
        parquet_dictionary_page_size,
        parquet_write_batch_size,
        parquet_offset_index,
        parquet_page_header_statistics,
        parquet_arrow_metadata,
        thread_budget,
        spill_path,
        spill_compression,
        vortex_record_batch_size,
    } = args;

    let usable_cpus = thread_budget
        .map(|spec| spec.resolve())
        .unwrap_or_else(default_thread_budget);
    let quarter_cpus = (usable_cpus / 4).max(1);
    let three_quarter_cpus = (usable_cpus * 3 / 4).max(1);

    // allocate threads based on workload:
    // - sorting is CPU-intensive in DataFusion, so give encoding fewer threads
    // - without sorting, encoding is the bottleneck, so give it more threads
    // NOTE: output partitioning with sort-single strategy requires sorting by partition columns
    let has_sort =
        sort_by.is_some() || (by.is_some() && partition_strategy == PartitionStrategy::SortSingle);
    let default_encoding_threads = if has_sort {
        quarter_cpus
    } else {
        three_quarter_cpus
    };
    let runtimes = Arc::new(ParquetRuntimes::try_new(
        parquet_column_encoding_threads.unwrap_or(default_encoding_threads),
        parquet_io_threads.unwrap_or(1),
    )?);

    for disabled_col in &parquet_bloom_column_off {
        if parquet_bloom_column.iter().any(|c| &c.name == disabled_col) {
            anyhow::bail!(
                "column '{}' specified in both --parquet-bloom-column-off and --parquet-bloom-column",
                disabled_col
            );
        }
    }

    for disabled_col in &parquet_dictionary_column_off {
        if parquet_dictionary_column
            .iter()
            .any(|c| &c.name == disabled_col)
        {
            anyhow::bail!(
                "column '{}' specified in both --parquet-dictionary-column-off and --parquet-dictionary-column",
                disabled_col
            );
        }
    }

    let all_enabled = if parquet_bloom_all_off {
        None
    } else {
        parquet_bloom_all.or(Some(AllColumnsBloomFilterConfig {
            fpp: DEFAULT_BLOOM_FILTER_FPP,
            ndv: None,
        }))
    };
    let bloom_filter =
        BloomFilterConfig::try_new(all_enabled, parquet_bloom_column, parquet_bloom_column_off)
            .map_err(anyhow::Error::msg)?;

    if preserve_input_order && from.is_none() {
        anyhow::bail!("--preserve-input-order requires --from (single input file)");
    }

    let effective_target_partitions = if preserve_input_order {
        Some(1)
    } else if target_partitions.is_some() {
        target_partitions
    } else if has_sort {
        Some(three_quarter_cpus)
    } else {
        None
    };

    let total_budget = memory_budget.resolve();

    let effective_memory_limit = if has_sort {
        // sorting needs more DataFusion memory, leave 40% for encoding/queues
        Some(total_budget * 60 / 100)
    } else {
        // no sorting: DataFusion just needs query overhead, give more to encoding
        Some(total_budget * 20 / 100)
    };

    if non_spillable_reserve.is_some() && effective_memory_limit.is_none() {
        anyhow::bail!("--non-spillable-reserve requires a memory limit (--memory-budget)");
    }

    let effective_non_spillable_reserve = non_spillable_reserve
        .zip(effective_memory_limit)
        .map(|(spec, pool_size)| spec.resolve(pool_size))
        .transpose()?;

    let mut pipeline = Pipeline::new()
        .with_query_dialect(dialect)
        .with_memory_limit(effective_memory_limit)
        .with_non_spillable_reserve(effective_non_spillable_reserve)
        .with_memory_pool_top_consumers(memory_pool_top_consumers)
        .with_target_partitions(effective_target_partitions)
        .with_spill_path(spill_path)
        .with_spill_compression(spill_compression);

    let (input_paths, should_glob) = if let Some(single_input) = from {
        (vec![single_input], false)
    } else {
        (from_many, true)
    };

    // resolve input paths (glob-expand if needed), build sources, and create InputStrategy
    let input_strategy = if !should_glob && input_paths.len() == 1 {
        let input_path = &input_paths[0];
        let source = make_source(input_path, input_format)?;
        InputStrategy::Single(source)
    } else {
        let mut expanded_paths = Vec::new();

        for pattern in &input_paths {
            for entry in glob(pattern)
                .map_err(|e| anyhow!("Error expanding glob pattern {}: {}", pattern, e))?
            {
                expanded_paths.push(
                    entry
                        .map_err(|e| anyhow!("Error decoding file path: {}", e))?
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }

        expanded_paths.sort();
        expanded_paths.dedup();

        if expanded_paths.is_empty() {
            anyhow::bail!("No input files found matching patterns: {:?}", input_paths);
        }

        let mut sources: Vec<Box<dyn DataSource>> = Vec::new();
        let mut schema: Option<SchemaRef> = None;
        for input_path in &expanded_paths {
            let source = make_source(input_path, input_format)?;
            if let Some(ref schema) = schema {
                let source_schema = source.schema()?;
                if *schema != source_schema {
                    anyhow::bail!(
                        "Schema mismatch for input file {} (does not match other file(s))",
                        input_path
                    );
                }
            } else {
                schema = Some(source.schema()?);
            }
            sources.push(source);
        }
        InputStrategy::Multiple(sources)
    };

    // sample rows to estimate sort spill reservation before handing strategy to pipeline
    if has_sort {
        let avg_row_bytes = sample_avg_row_bytes(&input_strategy, 100_000).await?;

        if avg_row_bytes > 0 {
            let total_rows = input_strategy.row_count().unwrap_or(0);
            let total_in_memory_bytes = total_rows.saturating_mul(avg_row_bytes);

            let memory_limit = effective_memory_limit.unwrap_or(total_budget * 60 / 100);
            let partitions = effective_target_partitions.unwrap_or(three_quarter_cpus);
            let memory_per_partition = memory_limit / partitions.max(1);

            let reservation = estimate_sort_spill_reservation(
                avg_row_bytes,
                total_in_memory_bytes,
                memory_per_partition,
                8192, // DataFusion default batch size
            );

            pipeline = pipeline.with_sort_spill_reservation_bytes(reservation);
        }
    }

    pipeline = pipeline.with_input_strategy(input_strategy);

    let list_outputs_format = list_outputs;

    // The overall sort order is determined by the following:
    //
    //   1. The sort order specified by the partition columns
    //   2. The sort order specified by the user
    //
    // We need the data sorted by the partition columns first so that the data can
    // be partitioned into individual files per partition as we output the data. Any
    // other alternative would require us to either:
    //
    //   1. Keep the files open per partition for the entire duration of the partition,
    //      which would be inefficient and require us to manage a lot of open file handles
    //      and use a lot of memory.
    //   2. Write multiple files per partition, managing how many file handles are open
    //      at any given time and how much memory is currently being used. If you still
    //      wanted to have a single file per partition you would need to come back later
    //      and merge the files together.
    //
    // Once we have sorted the data for partitioning there is nothing to sort for those
    // columns within each file since they are just a single value, so we remove them from
    // the user-specified sort order.

    let partition_columns = if let Some(ref by_cols) = by {
        by_cols
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    // sort-single: global sort by partition columns, emits one partition file at a time
    // nosort-multi/nosort-evict: file handles per partition, no sort needed
    let partition_sort_spec = match partition_strategy {
        PartitionStrategy::SortSingle => SortSpec::from(partition_columns.clone()),
        PartitionStrategy::NosortMulti | PartitionStrategy::NosortEvict => SortSpec::default(),
    };

    let user_sort_spec = sort_by.clone().unwrap_or(SortSpec::default());

    let user_sort_spec_without_partition_cols =
        user_sort_spec.without_columns_named(&partition_columns);

    let parquet_sort_spec =
        if parquet_sorted_metadata && !user_sort_spec_without_partition_cols.is_empty() {
            Some(user_sort_spec_without_partition_cols.clone())
        } else {
            None
        };

    let mut full_sort_spec = partition_sort_spec.clone();
    full_sort_spec.extend(&user_sort_spec_without_partition_cols);

    let arrow_opts = ArrowSinkOptions::new()
        .with_compression(arrow_compression)
        .with_format(arrow_format)
        .with_record_batch_size(arrow_record_batch_size)
        .with_queue_depth(arrow_writing_queue_size);

    let parquet_opts = ParquetSinkOptions::new()
        .with_parquet_compression(parquet_compression, parquet_compression_level)?
        .with_statistics(parquet_statistics)
        .with_writer_version(parquet_writer_version)
        .with_ingestion_queue_size(parquet_ingestion_queue_size)
        .with_encoding_queue_size(parquet_encoding_queue_size)
        .with_writing_queue_size(parquet_writing_queue_size)
        .with_no_dictionary(parquet_dictionary_all_off)
        .with_dictionary_configs(&parquet_dictionary_column)
        .with_column_no_dictionary(parquet_dictionary_column_off)
        .with_encoding(parquet_encoding)
        .with_column_encodings(parquet_column_encoding)
        .with_bloom_filters(bloom_filter)
        .with_offset_index_enabled(parquet_offset_index)
        .with_skip_arrow_metadata(!parquet_arrow_metadata)
        .with_page_header_statistics(parquet_page_header_statistics)
        .apply_if_some(parquet_buffer_size, ParquetSinkOptions::with_buffer_size)
        .apply_if_some(
            parquet_row_group_size,
            ParquetSinkOptions::with_max_row_group_size,
        )
        .apply_if_some(
            parquet_row_group_concurrency,
            ParquetSinkOptions::with_max_row_group_concurrency,
        )
        .apply_if_some(
            parquet_data_page_size,
            ParquetSinkOptions::with_data_page_size_limit,
        )
        .apply_if_some(
            parquet_data_page_row_limit,
            ParquetSinkOptions::with_data_page_row_count_limit,
        )
        .apply_if_some(
            parquet_dictionary_page_size,
            ParquetSinkOptions::with_dictionary_page_size_limit,
        )
        .apply_if_some(
            parquet_write_batch_size,
            ParquetSinkOptions::with_write_batch_size,
        )
        .apply_if_some(parquet_sort_spec, ParquetSinkOptions::with_sort_spec);

    let vortex_opts = VortexSinkOptions::new().apply_if_some(
        vortex_record_batch_size,
        VortexSinkOptions::with_record_batch_size,
    );

    let sink_factory = create_sink_factory(
        output_format,
        arrow_opts.clone(),
        parquet_opts.clone(),
        vortex_opts,
        Arc::clone(&runtimes),
    )?;

    if let Some(output_path) = to {
        pipeline = pipeline.with_output_strategy_with_single_sink(
            output_path,
            sink_factory,
            exclude_columns.clone(),
            create_dirs,
            overwrite,
        );
    } else if let Some(template) = to_many {
        let path_template = PathTemplate::new(template);

        if max_open_partitions.is_some() && partition_strategy != PartitionStrategy::NosortEvict {
            anyhow::bail!(
                "--max-open-partitions is only supported with --partition-strategy=nosort-evict"
            );
        }

        let max_open_partitions = NonZeroUsize::new(max_open_partitions.unwrap_or(100))
            .ok_or_else(|| anyhow!("--max-open-partitions must be at least 1"))?;

        match partition_strategy {
            PartitionStrategy::NosortMulti => {
                pipeline = pipeline.with_multi_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    sink_factory,
                    exclude_columns.clone(),
                    create_dirs,
                    overwrite,
                    list_outputs.unwrap_or_default(),
                );
            }
            PartitionStrategy::NosortEvict => {
                // each partition gets its own writer, so we minimize per-writer
                // concurrency to avoid scheduling overhead from 100+ parallel pipelines
                let evict_sink_factory = create_sink_factory(
                    output_format,
                    arrow_opts,
                    parquet_opts
                        .with_ingestion_queue_size(1)
                        .with_encoding_queue_size(1)
                        .with_writing_queue_size(1)
                        .with_max_row_group_concurrency(1),
                    vortex_opts,
                    runtimes,
                )?;
                pipeline = pipeline.with_evict_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    evict_sink_factory,
                    exclude_columns.clone(),
                    create_dirs,
                    overwrite,
                    list_outputs.unwrap_or_default(),
                    max_open_partitions,
                );
            }
            PartitionStrategy::SortSingle => {
                pipeline = pipeline.with_single_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    sink_factory,
                    exclude_columns.clone(),
                    create_dirs,
                    overwrite,
                    list_outputs.unwrap_or_default(),
                );
            }
        }
    }

    if let Some(q) = query {
        pipeline = pipeline.with_operation(Box::new(QueryOperation::new(q)));
    }

    if !full_sort_spec.is_empty() {
        pipeline = pipeline.with_operation(Box::new(SortOperation::new(full_sort_spec.columns)));
    }

    let files = pipeline.execute().await?;

    if let Some(format) = list_outputs_format {
        print_output_files(&files, format, list_outputs_file.as_deref())?;
    }

    Ok(())
}

fn print_output_files(
    files: &[OutputFileInfo],
    format: ListOutputsFormat,
    output_path: Option<&Utf8Path>,
) -> Result<()> {
    let output = match format {
        ListOutputsFormat::None => return Ok(()),
        ListOutputsFormat::Text => {
            if files.is_empty() {
                return Ok(());
            }

            let mut builder = Builder::default();

            let mut header: Vec<String> = files
                .first()
                .map(|f| {
                    f.partition_values
                        .iter()
                        .map(|pv| to_title_case(&pv.column))
                        .collect()
                })
                .unwrap_or_default();
            header.push("Path".to_string());
            header.push("Row Count".to_string());

            if output_path.is_none() {
                // writing to stdout, so use colors
                let colored_header: Vec<String> =
                    header.iter().map(|h| h.bold().to_string()).collect();
                builder.push_record(colored_header);
            } else {
                builder.push_record(header);
            }

            // sort by path for consistent output
            let mut sorted_files = files.to_vec();
            sorted_files.sort_by(|a, b| a.path.cmp(&b.path));

            for file in &sorted_files {
                let mut row: Vec<String> = file
                    .partition_values
                    .iter()
                    .map(|pv| {
                        let val = format_json_value(&pv.value);
                        if output_path.is_none() {
                            // again, writing to stdout, so use colors
                            val.green().to_string()
                        } else {
                            val
                        }
                    })
                    .collect();
                row.push(file.path.clone());
                let row_count = file.row_count.to_string();
                if output_path.is_none() {
                    // once again, writing to stdout, so use colors
                    row.push(row_count.cyan().to_string());
                } else {
                    row.push(row_count);
                }
                builder.push_record(row);
            }

            builder.build().with(Style::rounded()).to_string()
        }
        ListOutputsFormat::Json => serde_json::to_string_pretty(files)?,
    };

    if let Some(path) = output_path {
        std::fs::write(path, output)?;
    } else {
        println!("{}", output);
    }

    Ok(())
}

fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}

fn to_title_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn make_source(path: &str, input_format: Option<DataFormat>) -> Result<Box<dyn DataSource>> {
    let format = detect_format(path, input_format)?;
    Ok(match format {
        DataFormat::Arrow => Box::new(ArrowDataSource::new(path.to_string())),
        DataFormat::Parquet => Box::new(ParquetDataSource::new(path.to_string())),
        DataFormat::Vortex => Box::new(VortexDataSource::new(path.to_string())),
    })
}

fn detect_format(path: &str, explicit_format: Option<DataFormat>) -> Result<DataFormat> {
    if let Some(format) = explicit_format {
        return Ok(format);
    }

    let path_obj = std::path::Path::new(path);
    if let Some(ext) = path_obj.extension() {
        if ext.eq_ignore_ascii_case("arrow") {
            return Ok(DataFormat::Arrow);
        } else if ext.eq_ignore_ascii_case("parquet") {
            return Ok(DataFormat::Parquet);
        } else if ext.eq_ignore_ascii_case("vortex") {
            return Ok(DataFormat::Vortex);
        }
    }

    Err(anyhow!(
        "Could not detect format from path '{}'. Use --input-format or --output-format to specify explicitly.",
        path
    ))
}

fn create_sink_factory(
    output_format: Option<DataFormat>,
    arrow_opts: ArrowSinkOptions,
    parquet_opts: ParquetSinkOptions,
    vortex_opts: VortexSinkOptions,
    runtimes: Arc<ParquetRuntimes>,
) -> Result<SinkFactory> {
    Ok(Box::new(move |path: String, schema: SchemaRef| {
        let detected_format = detect_format(&path, output_format)?;

        let sink: Box<dyn DataSink> = match detected_format {
            DataFormat::Arrow => {
                Box::new(ArrowSink::create(path.into(), &schema, arrow_opts.clone())?)
            }
            DataFormat::Parquet => Box::new(ParquetSink::create(
                path.into(),
                &schema,
                &parquet_opts,
                Arc::clone(&runtimes),
            )?),
            DataFormat::Vortex => Box::new(VortexSink::create(path.into(), &schema, vortex_opts)?),
        };

        Ok(sink)
    }))
}
