use std::sync::Arc;

use crate::{
    AllColumnsBloomFilterConfig, ArrowCompression, ArrowIPCFormat, BloomFilterConfig,
    ColumnDictionaryConfig, ColumnEncodingConfig, DEFAULT_BLOOM_FILTER_FPP, DataFormat,
    DictionaryMode, ListOutputsFormat, ParquetCompression, ParquetEncoding, ParquetStatistics,
    ParquetWriterVersion, PartitionStrategy, SortSpec, TransformCommand, default_thread_budget,
    io_strategies::{OutputFileInfo, output_strategy::SinkFactory, path_template::PathTemplate},
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
};
use anyhow::{Result, anyhow};
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
        exclude_columns,
        list_outputs,
        list_outputs_file,
        create_dirs,
        overwrite,
        query,
        dialect,
        sort_by,
        memory_budget,
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

    let usable_cpus = thread_budget.unwrap_or_else(default_thread_budget);
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

    // memory budget split: DataFusion gets a portion based on workload
    let total_budget = memory_budget.unwrap_or_else(|| {
        let available = crate::utils::memory::available_memory();
        available * 3 / 4
    });
    let effective_memory_limit = if has_sort {
        // sorting needs more DataFusion memory, leave 40% for encoding/queues
        Some(total_budget * 60 / 100)
    } else {
        // no sorting: DataFusion just needs query overhead, give more to encoding
        Some(total_budget * 20 / 100)
    };

    let mut pipeline = Pipeline::new()
        .with_query_dialect(dialect)
        .with_memory_limit(effective_memory_limit)
        .with_target_partitions(effective_target_partitions)
        .with_spill_path(spill_path)
        .with_spill_compression(spill_compression);

    let (input_paths, should_glob) = if let Some(single_input) = from {
        (vec![single_input], false)
    } else {
        (from_many, true)
    };

    let setup_result: Result<()> = {
        if !should_glob && input_paths.len() == 1 {
            let input_path = &input_paths[0];
            let detected_input_format = detect_format(input_path, input_format)?;

            let source: Box<dyn DataSource> = match detected_input_format {
                DataFormat::Arrow => Box::new(ArrowDataSource::new(input_path.clone())),
                DataFormat::Parquet => Box::new(ParquetDataSource::new(input_path.clone())),
                DataFormat::Vortex => Box::new(VortexDataSource::new(input_path.clone())),
            };

            pipeline = pipeline.with_input_strategy_with_single_source(source);
            Ok(())
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
            for input_path in expanded_paths {
                let detected_input_format = detect_format(&input_path, input_format)?;
                let source: Box<dyn DataSource> = match detected_input_format {
                    DataFormat::Arrow => Box::new(ArrowDataSource::new(input_path.clone())),
                    DataFormat::Parquet => Box::new(ParquetDataSource::new(input_path.clone())),
                    DataFormat::Vortex => Box::new(VortexDataSource::new(input_path.clone())),
                };
                if let Some(ref schema) = schema {
                    let source_schema = source.schema()?;
                    if *schema != source_schema {
                        anyhow::bail!(
                            "Schema mismatch for input file {} (does not match other file(s))",
                            &input_path
                        );
                    }
                } else {
                    schema = Some(source.schema()?);
                }
                sources.push(source);
            }
            pipeline = pipeline.with_input_strategy_with_multiple_sources(sources);
            Ok(())
        }
    };

    setup_result?;

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

    // sort-single: requires global sort by partition columns (one file at a time)
    // nosort-multi: keeps file handles open per partition, no sort required
    let partition_sort_spec = match partition_strategy {
        PartitionStrategy::SortSingle => SortSpec::from(partition_columns.clone()),
        PartitionStrategy::NosortMulti => SortSpec::default(),
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

    let sink_factory = create_sink_factory(
        output_format,
        arrow_compression,
        arrow_format,
        arrow_record_batch_size,
        arrow_writing_queue_size,
        bloom_filter,
        parquet_buffer_size,
        parquet_dictionary_column,
        parquet_column_encoding,
        parquet_dictionary_column_off,
        parquet_compression,
        parquet_ingestion_queue_size,
        parquet_encoding_queue_size,
        parquet_writing_queue_size,
        parquet_encoding,
        parquet_dictionary_all_off,
        parquet_row_group_concurrency,
        parquet_row_group_size,
        parquet_sort_spec,
        parquet_statistics,
        parquet_writer_version,
        parquet_data_page_size,
        parquet_data_page_row_limit,
        parquet_dictionary_page_size,
        parquet_write_batch_size,
        parquet_offset_index,
        parquet_arrow_metadata,
        parquet_page_header_statistics,
        vortex_record_batch_size,
        runtimes,
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

        if partition_strategy == PartitionStrategy::NosortMulti {
            pipeline = pipeline.with_multi_writer_partitioned_sink(
                partition_columns,
                path_template,
                sink_factory,
                exclude_columns.clone(),
                create_dirs,
                overwrite,
                list_outputs.unwrap_or_default(),
            );
        } else {
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

#[allow(clippy::too_many_arguments)]
fn create_sink_factory(
    output_format: Option<DataFormat>,
    arrow_compression: ArrowCompression,
    arrow_format: ArrowIPCFormat,
    arrow_record_batch_size: usize,
    arrow_writing_queue_size: usize,
    parquet_bloom_filter: BloomFilterConfig,
    parquet_buffer_size: Option<usize>,
    parquet_dictionary_column: Vec<ColumnDictionaryConfig>,
    parquet_column_encoding: Vec<ColumnEncodingConfig>,
    parquet_dictionary_column_off: Vec<String>,
    parquet_compression: ParquetCompression,
    parquet_ingestion_queue_size: usize,
    parquet_encoding_queue_size: usize,
    parquet_writing_queue_size: usize,
    parquet_encoding: Option<ParquetEncoding>,
    parquet_dictionary_all_off: bool,
    parquet_row_group_concurrency: Option<usize>,
    parquet_row_group_size: Option<usize>,
    parquet_sort_spec: Option<SortSpec>,
    parquet_statistics: ParquetStatistics,
    parquet_writer_version: ParquetWriterVersion,
    parquet_data_page_size: Option<usize>,
    parquet_data_page_row_limit: Option<usize>,
    parquet_dictionary_page_size: Option<usize>,
    parquet_write_batch_size: Option<usize>,
    parquet_offset_index: bool,
    parquet_arrow_metadata: bool,
    parquet_page_header_statistics: bool,
    vortex_record_batch_size: Option<usize>,
    runtimes: Arc<ParquetRuntimes>,
) -> Result<SinkFactory> {
    Ok(Box::new(move |path: String, schema: SchemaRef| {
        let detected_format = detect_format(&path, output_format)?;

        let sink: Box<dyn DataSink> = match detected_format {
            DataFormat::Arrow => {
                let options = ArrowSinkOptions::new()
                    .with_compression(arrow_compression)
                    .with_format(arrow_format)
                    .with_record_batch_size(arrow_record_batch_size)
                    .with_queue_depth(arrow_writing_queue_size);
                Box::new(ArrowSink::create(path.into(), &schema, options)?)
            }
            DataFormat::Parquet => {
                let mut options = ParquetSinkOptions::new()
                    .with_compression(parquet_compression)
                    .with_statistics(parquet_statistics)
                    .with_writer_version(parquet_writer_version)
                    .with_ingestion_queue_size(parquet_ingestion_queue_size)
                    .with_encoding_queue_size(parquet_encoding_queue_size)
                    .with_writing_queue_size(parquet_writing_queue_size);
                if let Some(row_group_size) = parquet_row_group_size {
                    options = options.with_max_row_group_size(row_group_size);
                }
                if let Some(buffer_size) = parquet_buffer_size {
                    options = options.with_buffer_size(buffer_size);
                }
                if let Some(concurrency) = parquet_row_group_concurrency {
                    options = options.with_max_row_group_concurrency(concurrency);
                }
                if parquet_dictionary_all_off {
                    options = options.with_no_dictionary(true);
                }

                let mut column_dictionary_always = Vec::new();
                let mut column_dictionary_analyze = Vec::new();
                for config in &parquet_dictionary_column {
                    match config.mode {
                        DictionaryMode::Always => {
                            column_dictionary_always.push(config.name.clone());
                        }
                        DictionaryMode::Analyze => {
                            column_dictionary_analyze.push(config.name.clone());
                        }
                    }
                }

                options = options
                    .with_column_dictionary_always(column_dictionary_always)
                    .with_column_dictionary_analyze(column_dictionary_analyze)
                    .with_column_no_dictionary(parquet_dictionary_column_off.clone())
                    .with_encoding(parquet_encoding)
                    .with_column_encodings(parquet_column_encoding.clone())
                    .with_bloom_filters(parquet_bloom_filter.clone())
                    .with_offset_index_enabled(parquet_offset_index)
                    .with_skip_arrow_metadata(!parquet_arrow_metadata)
                    .with_page_header_statistics(parquet_page_header_statistics);
                if let Some(size) = parquet_data_page_size {
                    options = options.with_data_page_size_limit(size);
                }
                if let Some(limit) = parquet_data_page_row_limit {
                    options = options.with_data_page_row_count_limit(limit);
                }
                if let Some(size) = parquet_dictionary_page_size {
                    options = options.with_dictionary_page_size_limit(size);
                }
                if let Some(size) = parquet_write_batch_size {
                    options = options.with_write_batch_size(size);
                }
                if let Some(sort_spec) = parquet_sort_spec.clone() {
                    options = options.with_sort_spec(sort_spec);
                }
                Box::new(ParquetSink::create(
                    path.into(),
                    &schema,
                    &options,
                    Arc::clone(&runtimes),
                )?)
            }
            DataFormat::Vortex => {
                let mut options = VortexSinkOptions::new();
                if let Some(batch_size) = vortex_record_batch_size {
                    options = options.with_record_batch_size(batch_size);
                }
                Box::new(VortexSink::create(path.into(), &schema, options)?)
            }
        };

        Ok(sink)
    }))
}
