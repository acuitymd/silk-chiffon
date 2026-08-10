use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::{
    ListOutputsFormat, PartitionStrategy, SortDirection, SortSpec, TransformCommand,
    default_thread_budget,
    io_strategies::{
        OutputFileInfo, input_sources::InputSources, output_strategy::SinkOpenerFn,
        path_template::PathTemplate,
    },
    operations::{query::QueryOperation, sort::SortOperation},
    pipeline::Pipeline,
    sources::data_source::{DataSource, Replayability, RowCount},
    utils::memory::{estimate_sort_spill_reservation, measure_avg_input_row_bytes},
};
use anyhow::{Result, anyhow};
use camino::Utf8Path;
use glob::glob;
use owo_colors::OwoColorize;
use silk_chiffon_core::{
    OutputOrderingColumn, SinkBinding, SinkBindingConfig, SortDirection as CoreSortDirection,
    TransformBinding, TransformBindings,
};
use silk_chiffon_storage::{LocationInput, StorageHandle, StorageSession};
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
        thread_budget,
        spill_path,
        spill_compression,
        formats,
        storage,
    } = args;

    let usable_cpus = thread_budget
        .map(|spec| spec.resolve())
        .unwrap_or_else(default_thread_budget);
    let three_quarter_cpus = (usable_cpus * 3 / 4).max(1);

    let has_sort =
        sort_by.is_some() || (by.is_some() && partition_strategy == PartitionStrategy::SortSingle);

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
    let session = pipeline.create_session_context()?;

    let (input_paths, should_glob) = if let Some(single_input) = from {
        (vec![single_input], false)
    } else {
        (from_many, true)
    };

    let input_sources = if !should_glob && input_paths.len() == 1 {
        let handle = input_handle(&input_paths[0], &storage)?;
        let format = format_for_handle(
            &formats,
            input_format.as_deref(),
            &handle,
            &input_paths[0],
            "input",
        )?;
        let source = format.create_source(&handle, &session).await?;
        pipeline = pipeline.with_storage_handle(handle);
        InputSources::new(source)
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
        for input_path in &expanded_paths {
            let handle = input_handle(input_path, &storage)?;
            let format = format_for_handle(
                &formats,
                input_format.as_deref(),
                &handle,
                input_path,
                "input",
            )?;
            let source = format.create_source(&handle, &session).await?;
            pipeline = pipeline.with_storage_handle(handle);
            sources.push(source);
        }
        let mut sources = sources.into_iter();
        let mut inputs = InputSources::new(
            sources
                .next()
                .expect("empty path expansion is rejected above"),
        );
        for source in sources {
            inputs.push(source);
        }
        inputs
    };

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

    let mut full_sort_spec = partition_sort_spec.clone();
    full_sort_spec.extend(&user_sort_spec_without_partition_cols);

    if let Some(q) = &query {
        pipeline = pipeline.with_operation(Box::new(QueryOperation::new(q.clone())));
    }

    if !full_sort_spec.is_empty() {
        pipeline =
            pipeline.with_operation(Box::new(SortOperation::new(full_sort_spec.columns.clone())));
    }

    pipeline = pipeline.with_inputs(input_sources);
    let mut prepared = pipeline.prepare(session).await?;

    if has_sort && prepared.inputs().replayability() == Replayability::Replayable {
        let avg_row_bytes =
            measure_avg_input_row_bytes(prepared.session(), prepared.inputs(), 100_000).await?;
        if avg_row_bytes > 0 {
            let row_count = match prepared.inputs().row_count_capability() {
                Some(capability) => capability.row_count().await.unwrap_or(RowCount::Unknown),
                None => RowCount::Unknown,
            };
            let total_rows = match row_count {
                RowCount::Exact(rows) | RowCount::Estimated(rows) => {
                    usize::try_from(rows).unwrap_or(usize::MAX)
                }
                RowCount::Unknown => 100_000,
            };
            let total_in_memory_bytes = total_rows.saturating_mul(avg_row_bytes);
            let memory_limit = effective_memory_limit.unwrap_or(total_budget * 60 / 100);
            let partitions = effective_target_partitions.unwrap_or(three_quarter_cpus);
            let memory_per_partition = memory_limit / partitions.max(1);
            let reservation = estimate_sort_spill_reservation(
                avg_row_bytes,
                total_in_memory_bytes,
                memory_per_partition,
                8192,
            );
            prepared = prepared.with_sort_spill_reservation_bytes(reservation);
        }
    }

    let output_location = to
        .as_deref()
        .or(to_many.as_deref())
        .expect("Clap requires output");
    let output_handle = to
        .as_deref()
        .map(|output| output_handle(output, &storage))
        .transpose()?;
    let output_format = match &output_handle {
        Some(handle) => format_for_handle(
            &formats,
            output_format.as_deref(),
            handle,
            output_location,
            "output",
        )?,
        None => format_for_path(
            &formats,
            output_format.as_deref(),
            output_location,
            "output",
        )?,
    };
    let output_ordering = user_sort_spec_without_partition_cols
        .columns
        .iter()
        .map(|column| {
            OutputOrderingColumn::new(
                column.name.clone(),
                match column.direction {
                    SortDirection::Ascending => CoreSortDirection::Ascending,
                    SortDirection::Descending => CoreSortDirection::Descending,
                },
            )
        })
        .collect();
    let output_threads = if has_sort {
        (usable_cpus / 4).max(1)
    } else {
        three_quarter_cpus
    };
    let sink_context = SinkBindingConfig::new(
        NonZeroUsize::new(output_threads).expect("the thread budget is always positive"),
        output_ordering,
    );
    let sink_binding = output_format.bind_sink(&sink_context).await?;
    let sink_opener = storage_sink_opener(storage.clone(), sink_binding);

    if let Some(output_path) = to {
        let handle = output_handle.expect("an exact output creates a handle");
        let output_path = local_output_path(&output_path, &handle)?;
        prepared = prepared.with_storage_handle(&handle);
        prepared = prepared.with_output_strategy_with_single_sink(
            output_path,
            sink_opener,
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
                prepared = prepared.with_multi_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    sink_opener,
                    exclude_columns.clone(),
                    create_dirs,
                    overwrite,
                    list_outputs.unwrap_or_default(),
                );
            }
            PartitionStrategy::NosortEvict => {
                prepared = prepared.with_evict_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    sink_opener,
                    exclude_columns.clone(),
                    create_dirs,
                    overwrite,
                    list_outputs.unwrap_or_default(),
                    max_open_partitions,
                );
            }
            PartitionStrategy::SortSingle => {
                prepared = prepared.with_single_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    sink_opener,
                    exclude_columns.clone(),
                    create_dirs,
                    overwrite,
                    list_outputs.unwrap_or_default(),
                );
            }
        }
    }

    let files = prepared.execute().await?;

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

fn input_handle(input: &str, storage: &StorageSession) -> Result<StorageHandle> {
    let location = LocationInput::parse(input)?;
    Ok(storage.input_handle(&location)?)
}

fn output_handle(output: &str, storage: &StorageSession) -> Result<StorageHandle> {
    let location = LocationInput::parse(output)?;
    Ok(storage.output_handle(&location)?)
}

fn format_for_handle<'a>(
    formats: &'a TransformBindings,
    explicit_format: Option<&str>,
    handle: &StorageHandle,
    display_path: &str,
    direction: &str,
) -> Result<&'a TransformBinding> {
    if let Some(format) = explicit_format {
        return formats
            .get(format)
            .ok_or_else(|| anyhow!("format is not registered: {format}"));
    }
    let extension = std::path::Path::new(handle.url().path())
        .extension()
        .and_then(std::ffi::OsStr::to_str);
    format_for_extension(formats, extension, display_path, direction)
}

fn format_for_path<'a>(
    formats: &'a TransformBindings,
    explicit_format: Option<&str>,
    path: &str,
    direction: &str,
) -> Result<&'a TransformBinding> {
    if let Some(format) = explicit_format {
        return formats
            .get(format)
            .ok_or_else(|| anyhow!("format is not registered: {format}"));
    }
    let extension = std::path::Path::new(path)
        .extension()
        .and_then(std::ffi::OsStr::to_str);
    format_for_extension(formats, extension, path, direction)
}

fn format_for_extension<'a>(
    formats: &'a TransformBindings,
    extension: Option<&str>,
    path: &str,
    direction: &str,
) -> Result<&'a TransformBinding> {
    extension
        .and_then(|extension| formats.by_extension(extension))
        .ok_or_else(|| {
            anyhow!(
                "Could not detect format from path '{}'. Use --{}-format to specify explicitly.",
                path,
                direction
            )
        })
}

fn local_output_path(output: &str, handle: &StorageHandle) -> Result<String> {
    if !output.starts_with("file:///") {
        return Ok(output.to_owned());
    }
    handle
        .local_path()?
        .into_os_string()
        .into_string()
        .map_err(|path| anyhow!("Local path is not valid UTF-8: {}", path.to_string_lossy()))
}

fn storage_sink_opener(
    storage: StorageSession,
    sink_binding: Box<dyn SinkBinding>,
) -> SinkOpenerFn {
    let sink_binding: Arc<dyn SinkBinding> = sink_binding.into();
    Box::new(move |path, schema| {
        let storage = storage.clone();
        let sink_binding = Arc::clone(&sink_binding);
        Box::pin(async move {
            let location = LocationInput::parse(&path)?;
            let handle = storage.output_handle(&location)?;
            sink_binding.open_sink(handle, schema).await
        })
    })
}
