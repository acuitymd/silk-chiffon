use std::num::NonZeroUsize;

use crate::{
    ListOutputsFormat, PartitionStrategy, SortDirection, SortSpec, TransformCommand,
    default_thread_budget,
    operations::{query::QueryOperation, sort::SortOperation},
    utils::{
        memory::{estimate_sort_spill_reservation, measure_avg_input_row_bytes},
        projected_stream::project_stream,
    },
};
use anyhow::{Result, anyhow};
use camino::Utf8Path;
use owo_colors::OwoColorize;
use silk_chiffon_core::{
    DataSource, InputSources, OutputOrderingColumn, Pipeline, Replayability, RowCount,
    SinkBindingConfig, SinkConcurrency, SortDirection as CoreSortDirection,
};
use tabled::{builder::Builder, settings::Style};

mod file_input;
mod file_output;
mod scheme;

use file_input::FileInputRoute;
use file_output::{FileOutputReport, FileOutputRoute, FileOutputTarget};
use scheme::explicit_scheme;

pub async fn run(args: TransformCommand) -> Result<()> {
    let TransformCommand {
        inputs,
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
        service_inputs,
        service_outputs,
        input_schemes,
        output_schemes,
    } = args;

    let usable_cpus = thread_budget
        .map(|spec| spec.resolve())
        .unwrap_or_else(default_thread_budget);
    let three_quarter_cpus = (usable_cpus * 3 / 4).max(1);

    let has_sort =
        sort_by.is_some() || (by.is_some() && partition_strategy == PartitionStrategy::SortSingle);

    if preserve_input_order
        && !matches!(
            &inputs,
            crate::InputRequest::ExactReferences(references) if references.len() == 1
        )
    {
        anyhow::bail!("--preserve-input-order requires exactly one --from reference");
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

    let file_inputs = FileInputRoute::new(&storage, &formats, input_format.as_deref(), &session);
    let mut sources: Vec<Box<dyn DataSource>> = Vec::new();
    let mut used_file_input = false;
    match &inputs {
        crate::InputRequest::ExactReferences(references) => {
            for reference in references.iter() {
                let source = match explicit_scheme(reference) {
                    Some(scheme) => match input_schemes.owner(scheme) {
                        Some(crate::registration::InputSchemeOwner::FileInput) => {
                            used_file_input = true;
                            file_inputs.create_exact_source(reference).await?
                        }
                        Some(crate::registration::InputSchemeOwner::ServiceInput(index)) => {
                            service_inputs
                                .get(*index)
                                .create_source(reference, &session)
                                .await?
                        }
                        None => anyhow::bail!("unsupported input scheme {scheme:?}"),
                    },
                    None => {
                        used_file_input = true;
                        file_inputs.create_exact_source(reference).await?
                    }
                };
                sources.push(source);
            }
        }
        crate::InputRequest::Patterns(patterns) => {
            for pattern in patterns.iter() {
                if let Some(scheme) = explicit_scheme(pattern) {
                    match input_schemes.owner(scheme) {
                        Some(crate::registration::InputSchemeOwner::FileInput) => {}
                        Some(crate::registration::InputSchemeOwner::ServiceInput(index)) => {
                            anyhow::bail!(
                                "service input {:?} does not support --from-pattern \
                                 {pattern:?}; use --from",
                                service_inputs.get(*index).name()
                            );
                        }
                        None => anyhow::bail!("unsupported input scheme {scheme:?}"),
                    }
                }
            }
            used_file_input = true;
            sources.extend(file_inputs.create_pattern_sources(patterns.iter()).await?);
        }
    }
    if input_format.is_some() && !used_file_input {
        anyhow::bail!("--input-format applies only to file inputs");
    }
    let mut sources = sources.into_iter();
    let mut input_sources = InputSources::new(
        sources
            .next()
            .expect("InputRequest is nonempty and every reference creates one source"),
    );
    for source in sources {
        input_sources.push(source);
    }

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

    if let Some(target) = to.as_deref()
        && let Some(scheme) = explicit_scheme(target)
    {
        match output_schemes.owner(scheme) {
            Some(crate::registration::OutputSchemeOwner::ServiceOutput(index)) => {
                if output_format.is_some() {
                    anyhow::bail!("--output-format applies only to file outputs");
                }
                if list_outputs.is_some() {
                    anyhow::bail!("--list-outputs applies only to file outputs");
                }
                let projection = if exclude_columns.is_empty() {
                    None
                } else {
                    let schema = prepared.output_schema();
                    validate_excluded_columns(&schema, &exclude_columns)?;
                    projection_indices_excluding(&schema, &exclude_columns)
                };
                let mut stream = prepared.begin_execution()?.into_sendable_stream();
                if let Some(indices) = projection {
                    stream = project_stream(stream, indices)?;
                }
                service_outputs.get(*index).write(target, stream).await?;
                return Ok(());
            }
            Some(crate::registration::OutputSchemeOwner::FileOutput) => {}
            None => anyhow::bail!("unsupported output scheme {scheme:?}"),
        }
    }
    if let Some(template) = to_many.as_deref()
        && let Some(scheme) = explicit_scheme(template)
    {
        match output_schemes.owner(scheme) {
            Some(crate::registration::OutputSchemeOwner::FileOutput) => {}
            Some(crate::registration::OutputSchemeOwner::ServiceOutput(index)) => {
                anyhow::bail!(
                    "service output {:?} does not support --to-many {template:?}; use --to",
                    service_outputs.get(*index).name()
                );
            }
            None => anyhow::bail!("unsupported output scheme {scheme:?}"),
        }
    }

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
    let sink_concurrency = if to_many.is_some()
        && matches!(
            partition_strategy,
            PartitionStrategy::NosortMulti | PartitionStrategy::NosortEvict
        ) {
        SinkConcurrency::Concurrent
    } else {
        SinkConcurrency::Sequential
    };
    let sink_context = SinkBindingConfig::new(
        NonZeroUsize::new(output_threads).expect("the thread budget is always positive"),
        sink_concurrency,
        output_ordering,
    );
    let target = match (to, to_many) {
        (Some(target), None) => FileOutputTarget::Exact {
            target,
            exclude_columns,
            create_dirs,
            overwrite,
        },
        (None, Some(pattern)) => FileOutputTarget::Template {
            pattern,
            partition_fields: partition_columns,
            strategy: partition_strategy,
            max_open_partitions,
            exclude_columns,
            create_dirs,
            overwrite,
        },
        _ => unreachable!("Clap requires exactly one output mode"),
    };
    let output = FileOutputRoute::new(
        &storage,
        &formats,
        output_format.as_deref(),
        prepared.session(),
    )
    .bind(target, &sink_context, &prepared.output_schema())
    .await?;

    let execution = prepared.begin_execution()?;
    let report = output.write(execution.into_sendable_stream()).await?;

    if let Some(format) = list_outputs_format {
        print_output_files(&report, format, list_outputs_file.as_deref())?;
    }

    Ok(())
}

fn print_output_files(
    report: &FileOutputReport,
    format: ListOutputsFormat,
    output_path: Option<&Utf8Path>,
) -> Result<()> {
    let output = match format {
        ListOutputsFormat::None => return Ok(()),
        ListOutputsFormat::Text => {
            if report.outputs().is_empty() {
                return Ok(());
            }

            let mut builder = Builder::default();

            let mut header: Vec<String> = report
                .outputs()
                .first()
                .map(|output| {
                    output
                        .partition_fields
                        .iter()
                        .map(|value| to_title_case(&value.field))
                        .collect()
                })
                .unwrap_or_default();
            header.push("Durable Locations".to_string());
            header.push("Rows Written".to_string());

            if output_path.is_none() {
                let colored_header: Vec<String> =
                    header.iter().map(|h| h.bold().to_string()).collect();
                builder.push_record(colored_header);
            } else {
                builder.push_record(header);
            }

            for completed in report.outputs() {
                let mut row: Vec<String> = completed
                    .partition_fields
                    .iter()
                    .map(|value| {
                        let value = format_json_value(&value.value);
                        if output_path.is_none() {
                            value.green().to_string()
                        } else {
                            value
                        }
                    })
                    .collect();
                row.push(completed.durable_locations.join(", "));
                let rows_written = completed.rows_written.to_string();
                if output_path.is_none() {
                    row.push(rows_written.cyan().to_string());
                } else {
                    row.push(rows_written);
                }
                builder.push_record(row);
            }

            builder.build().with(Style::rounded()).to_string()
        }
        ListOutputsFormat::Json => serde_json::to_string_pretty(report)?,
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

fn validate_excluded_columns(
    schema: &arrow::datatypes::SchemaRef,
    exclude_columns: &[String],
) -> Result<()> {
    for column in exclude_columns {
        schema
            .column_with_name(column)
            .ok_or_else(|| anyhow!("Column {column:?} not found in schema"))?;
    }
    Ok(())
}

fn projection_indices_excluding(
    schema: &arrow::datatypes::SchemaRef,
    exclude_columns: &[String],
) -> Option<Vec<usize>> {
    (!exclude_columns.is_empty()).then(|| {
        (0..schema.fields().len())
            .filter(|index| !exclude_columns.contains(schema.field(*index).name()))
            .collect()
    })
}
