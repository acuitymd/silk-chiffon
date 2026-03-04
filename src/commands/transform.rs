use std::sync::Arc;

use crate::{
    AllColumnsBloomFilterConfig, BloomFilterConfig, DEFAULT_BLOOM_FILTER_FPP, DataFormat,
    DictionaryMode, ListOutputsFormat, PartitionStrategy, SortSpec, TransformCommand,
    default_thread_budget,
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
    sources::data_source::{DataSource, TARGET_SAMPLE_ROWS},
    utils::memory::{
        BudgetPlan, WorkloadKind, compute_sort_spill_reservation, estimate_max_sort_partitions,
        sort_merge_row_overhead,
    },
};
use anyhow::{Context, Result, anyhow};
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

    let (input_paths, should_glob) = if let Some(single_input) = from {
        (vec![single_input], false)
    } else {
        (from_many, true)
    };

    let (input_strategy, input_schema) =
        build_input_strategy(&input_paths, should_glob, input_format)?;

    let output_path_for_format = to.as_deref().or(to_many.as_deref());
    let detected_output_format = output_path_for_format
        .map(|p| detect_format(p, output_format))
        .transpose()?
        .or(output_format);

    let mut arrow_options = ArrowSinkOptions::new()
        .with_compression(arrow_compression)
        .with_format(arrow_format)
        .with_record_batch_size(arrow_record_batch_size);
    if let Some(queue_size) = arrow_writing_queue_size {
        arrow_options = arrow_options.with_queue_depth(queue_size);
    }

    let compression = parquet_compression.to_compression(parquet_compression_level)?;
    let mut parquet_options = ParquetSinkOptions::new()
        .with_compression(compression)
        .with_statistics(parquet_statistics)
        .with_writer_version(parquet_writer_version);
    if let Some(size) = parquet_ingestion_queue_size {
        parquet_options = parquet_options.with_ingestion_queue_size(size);
    }
    if let Some(size) = parquet_encoding_queue_size {
        parquet_options = parquet_options.with_encoding_queue_size(size);
    }
    if let Some(size) = parquet_writing_queue_size {
        parquet_options = parquet_options.with_writing_queue_size(size);
    }
    parquet_options = parquet_options
        .with_encoding(parquet_encoding)
        .with_column_encodings(parquet_column_encoding)
        .with_bloom_filters(bloom_filter)
        .with_offset_index_enabled(parquet_offset_index)
        .with_skip_arrow_metadata(!parquet_arrow_metadata)
        .with_page_header_statistics(parquet_page_header_statistics);
    if let Some(row_group_size) = parquet_row_group_size {
        parquet_options = parquet_options.with_max_row_group_size(row_group_size);
    }
    if let Some(buffer_size) = parquet_buffer_size {
        parquet_options = parquet_options.with_buffer_size(buffer_size);
    }
    if let Some(concurrency) = parquet_row_group_concurrency {
        parquet_options = parquet_options.with_max_row_group_concurrency(concurrency);
    }
    if parquet_dictionary_all_off {
        parquet_options = parquet_options.with_no_dictionary(true);
    }
    {
        let mut column_dictionary_always = Vec::new();
        let mut column_dictionary_analyze = Vec::new();
        for config in &parquet_dictionary_column {
            match config.mode {
                DictionaryMode::Always => column_dictionary_always.push(config.name.clone()),
                DictionaryMode::Analyze => column_dictionary_analyze.push(config.name.clone()),
            }
        }
        parquet_options = parquet_options
            .with_column_dictionary_always(column_dictionary_always)
            .with_column_dictionary_analyze(column_dictionary_analyze)
            .with_column_no_dictionary(parquet_dictionary_column_off);
    }
    if let Some(size) = parquet_data_page_size {
        parquet_options = parquet_options.with_data_page_size_limit(size);
    }
    if let Some(limit) = parquet_data_page_row_limit {
        parquet_options = parquet_options.with_data_page_row_count_limit(limit);
    }
    if let Some(size) = parquet_dictionary_page_size {
        parquet_options = parquet_options.with_dictionary_page_size_limit(size);
    }
    if let Some(size) = parquet_write_batch_size {
        parquet_options = parquet_options.with_write_batch_size(size);
    }

    let mut vortex_options = VortexSinkOptions::new();
    if let Some(batch_size) = vortex_record_batch_size {
        vortex_options = vortex_options.with_record_batch_size(batch_size);
    }

    let workload = WorkloadKind::new(has_sort, query.is_some());
    let resolved_budget = memory_budget.resolve();

    // only sample row sizes when a budget is active — row_size() opens the file
    // and streams up to TARGET_SAMPLE_ROWS rows, which is wasted I/O otherwise
    let (budget, row_bytes) = if let Some(total) = resolved_budget {
        let rb = input_strategy
            .row_size(&input_schema)
            .context("failed to sample row sizes from input")?;
        let sink_needs = match detected_output_format {
            Some(DataFormat::Parquet) | None => parquet_options.estimate_sink_needs(rb),
            Some(DataFormat::Arrow) => arrow_options.estimate_sink_needs(rb),
            Some(DataFormat::Vortex) => vortex_options.estimate_sink_needs(rb),
        };
        let plan = BudgetPlan::new(total, workload, rb, sink_needs);
        tracing::info!("memory budget: {plan}");
        (Some(plan), Some(rb))
    } else {
        tracing::info!("memory budget: unlimited (no pool)");
        (None, None)
    };

    let effective_target_partitions = if preserve_input_order {
        Some(1)
    } else if target_partitions.is_some() {
        target_partitions
    } else if let Some(ref budget) = budget {
        if has_sort {
            let max_sort = estimate_max_sort_partitions(budget.datafusion_pool);
            let capped = three_quarter_cpus.min(max_sort);
            tracing::info!(
                "sort partition scaling: max_sort={max_sort}, cpu_default={three_quarter_cpus}, effective={capped}"
            );
            Some(capped)
        } else {
            None
        }
    } else {
        None
    };

    // the merge phase tracks both batch data and RowConverter Rows encoding,
    // so the reservation must hold back enough pool space for the encoding overhead.
    // covers both explicit --sort-by and partition-driven sorts (sort-single strategy).
    // must include all sorted columns (partition + user sort) since DataFusion's
    // RowConverter encodes every column in the sort key during the merge phase.
    let sort_col_names: Option<Vec<String>> = {
        let mut cols = Vec::new();
        if partition_strategy == PartitionStrategy::SortSingle
            && let Some(ref by_val) = by
        {
            cols.extend(by_val.split(',').map(|s| s.trim().to_string()));
        }
        if let Some(sort_spec) = &sort_by {
            for c in &sort_spec.columns {
                if !cols.contains(&c.name) {
                    cols.push(c.name.clone());
                }
            }
        }
        if cols.is_empty() { None } else { Some(cols) }
    };
    let sort_spill_reservation = if let (Some(budget), Some(col_names)) = (&budget, &sort_col_names)
    {
        let avg_sizes =
            input_strategy.estimate_column_sizes(&input_schema, col_names, TARGET_SAMPLE_ROWS)?;
        let rb = row_bytes.expect("row_bytes is Some when budget is Some");
        let (data_bytes, encoding_bytes) =
            sort_merge_row_overhead(&input_schema, col_names, &avg_sizes, rb);
        let reservation =
            compute_sort_spill_reservation(budget.datafusion_pool, data_bytes, encoding_bytes);
        tracing::info!(
            "sort spill reservation: {}, data_bytes/row={data_bytes}, encoding_bytes/row={encoding_bytes}",
            humansize::format_size(reservation, humansize::BINARY),
        );
        Some(reservation)
    } else {
        None
    };

    let sink_budget = budget.as_ref().map(|b| b.sink_budget);
    arrow_options = arrow_options
        .with_memory_budget(sink_budget)
        .with_row_bytes(row_bytes);
    parquet_options = parquet_options
        .with_memory_budget(sink_budget)
        .with_row_bytes(row_bytes);
    vortex_options = vortex_options
        .with_memory_budget(sink_budget)
        .with_row_bytes(row_bytes);

    let mut pipeline = Pipeline::new()
        .with_query_dialect(dialect)
        .with_memory_limit(budget.map(|b| b.datafusion_pool))
        .with_target_partitions(effective_target_partitions)
        .with_sort_spill_reservation(sort_spill_reservation)
        .with_spill_path(spill_path)
        .with_spill_compression(spill_compression)
        .with_input_strategy(input_strategy);

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

    let mut full_sort_spec = partition_sort_spec.clone();
    full_sort_spec.extend(&user_sort_spec_without_partition_cols);

    if parquet_sorted_metadata && !user_sort_spec_without_partition_cols.is_empty() {
        parquet_options =
            parquet_options.with_sort_spec(user_sort_spec_without_partition_cols.clone());
    }

    let sink_factory = create_sink_factory(
        output_format,
        arrow_options,
        parquet_options,
        vortex_options,
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

fn create_sink_factory(
    output_format: Option<DataFormat>,
    arrow_options: ArrowSinkOptions,
    parquet_options: ParquetSinkOptions,
    vortex_options: VortexSinkOptions,
    runtimes: Arc<ParquetRuntimes>,
) -> Result<SinkFactory> {
    Ok(Box::new(move |path: String, schema: SchemaRef| {
        let detected_format = detect_format(&path, output_format)?;

        let sink: Box<dyn DataSink> = match detected_format {
            DataFormat::Arrow => Box::new(ArrowSink::create(
                path.into(),
                &schema,
                arrow_options.clone(),
            )?),
            DataFormat::Parquet => Box::new(ParquetSink::create(
                path.into(),
                &schema,
                &parquet_options,
                Arc::clone(&runtimes),
            )?),
            DataFormat::Vortex => {
                Box::new(VortexSink::create(path.into(), &schema, vortex_options)?)
            }
        };

        Ok(sink)
    }))
}

fn build_input_strategy(
    paths: &[String],
    should_glob: bool,
    input_format: Option<DataFormat>,
) -> Result<(InputStrategy, SchemaRef)> {
    if !should_glob && paths.len() == 1 {
        let path = &paths[0];
        let source = detect_format(path, input_format)?.into_datasource(path.clone());
        let schema = source.schema()?;
        Ok((InputStrategy::Single(source), schema))
    } else {
        let mut expanded_paths = Vec::new();
        for pattern in paths {
            for entry in
                glob(pattern).map_err(|e| anyhow!("Error expanding glob pattern {pattern}: {e}"))?
            {
                expanded_paths.push(
                    entry
                        .map_err(|e| anyhow!("Error decoding file path: {e}"))?
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
        expanded_paths.sort();
        expanded_paths.dedup();

        if expanded_paths.is_empty() {
            anyhow::bail!("No input files found matching patterns: {:?}", paths);
        }

        let mut sources: Vec<Box<dyn DataSource>> = Vec::new();
        let mut schema: Option<SchemaRef> = None;
        for input_path in expanded_paths {
            let source = detect_format(&input_path, input_format)?.into_datasource(input_path.clone());
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
        let schema = schema.expect("expanded_paths is non-empty");
        Ok((InputStrategy::Multiple(sources), schema))
    }
}
