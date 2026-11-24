use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use crate::{PartitionStrategy, SortSpec, TransformCommand, default_thread_budget};
use anyhow::{Result, anyhow};
use camino::Utf8Path;
use datafusion::{
    arrow::datatypes::Schema,
    catalog::TableProvider,
    error::DataFusionError,
    execution::disk_manager::DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
    physical_plan::{
        ExecutionPlan, SendableRecordBatchStream,
        display::DisplayableExecutionPlan,
        execution_plan::EmissionType,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::TryStreamExt;
use owo_colors::OwoColorize;
use silk_chiffon_core::{
    OpenSinkMode, Pipeline, PreparedPipeline, PresentationMode, SinkBindingConfig, SortColumn,
    default_non_spillable_reserve, union_input_providers_by_name,
};
use tabled::{builder::Builder, settings::Style};

mod file_input;
mod file_output;
mod projection;
mod query;
mod scheme;
mod sort_memory;

use file_input::FileInputPreparer;
use file_output::{FileOutputBinder, FileOutputReport, FileOutputRequest};
use projection::project_stream;
use query::apply_query;
use scheme::explicit_scheme;

/// The share of the memory budget that the memory pool gets when the
/// transform sorts, and when it does not.
const SORT_POOL_PERCENT: usize = 75;
const POOL_PERCENT: usize = 25;

pub(crate) async fn run(args: TransformCommand) -> Result<()> {
    let started = Instant::now();
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
        sort_engine,
        memory_budget,
        non_spillable_reserve,
        memory_pool_top_consumers,
        preserve_input_order,
        target_partitions,
        input_format,
        allow_unmatched_patterns,
        output_format,
        thread_budget,
        spill_path,
        spill_compression,
        max_spill_size,
        explain_analyze,
        max_spill_merge_fan_in,
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
        && (inputs.exact_references.len() != 1 || !inputs.file_patterns.is_empty())
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

    // the pool's default reserve is a fifth of it, so a sort may buffer up to
    // 60% of the budget and a transform without one up to 20%, less what
    // consumers that cannot spill hold. The rest of the budget is for the
    // writers, which do not reserve from the pool
    let effective_memory_limit = if has_sort {
        Some(total_budget * SORT_POOL_PERCENT / 100)
    } else {
        Some(total_budget * POOL_PERCENT / 100)
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
        .with_spill_compression(spill_compression)
        .with_max_spill_size(max_spill_size)
        .with_max_spill_merge_fan_in(max_spill_merge_fan_in);
    let session = pipeline.create_session_context()?;

    let file_inputs = FileInputPreparer::new(&storage, &formats, input_format.as_deref(), &session);
    let mut providers: Vec<Arc<dyn TableProvider>> = Vec::new();
    let mut used_file_input = false;
    for pattern in &inputs.file_patterns {
        if let Some(scheme) = explicit_scheme(pattern) {
            match input_schemes.owner(scheme) {
                Some(crate::registration::InputSchemeOwner::FileInput) => {}
                Some(crate::registration::InputSchemeOwner::ServiceInput(index)) => {
                    anyhow::bail!(
                        "service input {:?} does not support --from-pattern \
                         {pattern:?}; use --from",
                        service_inputs[*index].name()
                    );
                }
                None => anyhow::bail!("unsupported input scheme {scheme:?}"),
            }
        }
    }
    for reference in &inputs.exact_references {
        let (provider, is_file) = match explicit_scheme(reference) {
            Some(scheme) => match input_schemes.owner(scheme) {
                Some(crate::registration::InputSchemeOwner::FileInput) => {
                    (file_inputs.prepare_exact(reference).await?, true)
                }
                Some(crate::registration::InputSchemeOwner::ServiceInput(index)) => (
                    service_inputs[*index]
                        .create_input_provider(reference, &session)
                        .await?,
                    false,
                ),
                None => anyhow::bail!("unsupported input scheme {scheme:?}"),
            },
            None => (file_inputs.prepare_exact(reference).await?, true),
        };
        used_file_input |= is_file;
        providers.push(provider);
    }
    let pattern_providers = file_inputs
        .prepare_patterns(&inputs.file_patterns, allow_unmatched_patterns)
        .await?;
    used_file_input |= !pattern_providers.is_empty();
    providers.extend(pattern_providers);
    if input_format.is_some() && !used_file_input {
        anyhow::bail!("--input-format applies only to file inputs");
    }
    if providers.is_empty() {
        anyhow::bail!("no inputs were selected");
    }
    let mut input = union_input_providers_by_name(&session, providers)?;
    let inputs_opened = Instant::now();

    let list_outputs_mode = list_outputs;

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

    if let Some(q) = &query {
        input = apply_query(&session, input, q).await?;
    }

    // names are resolved before they are compared, so that `--sort-by REGION`
    // and `--by region` name one column
    let input_schema = input.schema().as_arrow().clone();
    let mut user_sort_spec = SortSpec::default();
    user_sort_spec.extend(&SortSpec {
        columns: sort_columns(&sort_by.clone().unwrap_or_default().columns, &input_schema)?,
    });

    let user_sort_spec_without_partition_cols =
        user_sort_spec.without_columns_named(&partition_columns);

    let mut full_sort_spec = partition_sort_spec.clone();
    full_sort_spec.extend(&user_sort_spec_without_partition_cols);

    let mut prepared = pipeline
        .with_sort(full_sort_spec.columns.clone())
        .with_sort_engine(sort_engine)
        .prepare(input, session)
        .await?;

    let analysis = explain_analyze.then(|| ExecutionAnalysis {
        plan: Arc::clone(prepared.execution_plan()),
        started,
        inputs_opened,
        planned: Instant::now(),
    });

    if has_sort {
        let memory_limit = effective_memory_limit.unwrap_or(total_budget * SORT_POOL_PERCENT / 100);
        let partitions = effective_target_partitions.unwrap_or(three_quarter_cpus);
        // a sort can hold no more than the pool less its reserve, which the
        // pool keeps for consumers that cannot spill
        let reserve = effective_non_spillable_reserve
            .unwrap_or_else(|| default_non_spillable_reserve(memory_limit));
        let memory_per_partition = memory_limit.saturating_sub(reserve) / partitions.max(1);
        let reservation = sort_memory::sort_spill_reservation_from_plan(
            prepared.execution_plan(),
            memory_per_partition,
            8192,
        );
        prepared = prepared.with_sort_spill_reservation_bytes(reservation);
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
                let mut stream = begin_execution(prepared, max_spill_size)?;
                if let Some(indices) = projection {
                    stream = project_stream(stream, indices)?;
                }
                service_outputs[*index].consume(target, stream).await?;
                if let Some(analysis) = analysis {
                    analysis.print();
                }
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
                    service_outputs[*index].name()
                );
            }
            None => anyhow::bail!("unsupported output scheme {scheme:?}"),
        }
    }

    let output_ordering = user_sort_spec_without_partition_cols.columns.clone();
    // What was planned decides, not --sort-engine: the parallel engine falls
    // back to DataFusion's sort for keys it cannot encode, and an input already
    // in order is not sorted at all. A transform without --sort-by keeps the
    // share it had, whatever its --query plans
    let output_threads = if has_sort && merges_while_writing(prepared.execution_plan()) {
        (usable_cpus / 4).max(1)
    } else {
        three_quarter_cpus
    };
    let open_sink_mode = if to_many.is_some()
        && matches!(
            partition_strategy,
            PartitionStrategy::NosortMulti | PartitionStrategy::NosortEvict
        ) {
        OpenSinkMode::Multiple
    } else {
        OpenSinkMode::OneAtATime
    };
    let sink_context = SinkBindingConfig::new(
        NonZeroUsize::new(output_threads).expect("the thread budget is always positive"),
        open_sink_mode,
        output_ordering,
    );
    let target = match (to, to_many) {
        (Some(target), None) => FileOutputRequest::Exact {
            target,
            exclude_columns,
            create_dirs,
            overwrite,
        },
        (None, Some(pattern)) => FileOutputRequest::Template {
            pattern,
            partition_fields: partition_columns,
            strategy: partition_strategy,
            max_open_partitions,
            memory_budget: total_budget,
            exclude_columns,
            create_dirs,
            overwrite,
        },
        _ => unreachable!("Clap requires exactly one output mode"),
    };
    let output = FileOutputBinder::new(&storage, &formats, output_format.as_deref())
        .bind(target, &sink_context, &prepared.output_schema())
        .await?;

    let report = match output
        .write(begin_execution(prepared, max_spill_size)?)
        .await
    {
        Ok(report) => report,
        Err(failure) => {
            if let Some(mode) = list_outputs_mode
                && let Err(reporting) =
                    print_output_files(failure.report(), mode, list_outputs_file.as_deref())
            {
                return Err(with_cleanup_error(failure.into(), reporting));
            }
            return Err(failure.into());
        }
    };

    if let Some(mode) = list_outputs_mode {
        print_output_files(&report, mode, list_outputs_file.as_deref())?;
    }
    if let Some(analysis) = analysis {
        analysis.print();
    }

    Ok(())
}

/// The columns a sort names. A name that is a column exactly is taken as it
/// is. Any other is read as DataFusion reads an identifier, folded to lower
/// case unless it is quoted, which is how `--sort-by` has always read names.
///
/// The sort finds its columns by name alone, so a name that several columns
/// share, as after a self-join, is refused rather than sorted by the first.
fn sort_columns(columns: &[SortColumn], schema: &Schema) -> Result<Vec<SortColumn>> {
    columns
        .iter()
        .map(|column| {
            let name = if schema.field_with_name(column.name()).is_ok() {
                column.name().to_owned()
            } else {
                datafusion::common::Column::from_qualified_name(column.name()).name
            };
            let matching = schema
                .fields()
                .iter()
                .filter(|field| field.name() == &name)
                .count();
            if matching > 1 {
                let table = datafusion::common::Column::from_qualified_name(column.name())
                    .relation
                    .map_or_else(|| "b".to_owned(), |relation| relation.to_string());
                anyhow::bail!(
                    "--sort-by {:?} is ambiguous: the output has {matching} columns named \
                     {name:?}, and the sort finds its columns by name. Rename one in --query, \
                     as in SELECT {table}.{name} AS {table}_{name}, and sort by the new name",
                    column.name()
                );
            }
            Ok(SortColumn::new(
                name,
                column.direction(),
                column.null_placement(),
            ))
        })
        .collect()
}

/// Whether DataFusion's sort, or its merge of partitions in order, is still
/// merging while the output is written, sharing the CPUs with encoding. Below
/// an operator that takes all its input before it emits, such as the parallel
/// sort, the work is done by the time the output is written.
fn merges_while_writing(plan: &Arc<dyn ExecutionPlan>) -> bool {
    // a stack, not recursion: a plan's depth follows the --query
    let mut pending = vec![Arc::clone(plan)];
    while let Some(plan) = pending.pop() {
        if plan.is::<SortExec>() || plan.is::<SortPreservingMergeExec>() {
            return true;
        }
        if plan.properties().emission_type != EmissionType::Final {
            pending.extend(plan.children().into_iter().cloned());
        }
    }
    false
}

/// What `--explain-analyze` reports once the output is complete.
struct ExecutionAnalysis {
    plan: Arc<dyn ExecutionPlan>,
    started: Instant,
    inputs_opened: Instant,
    planned: Instant,
}

impl ExecutionAnalysis {
    fn print(&self) {
        let finished = Instant::now();
        eprintln!(
            "transform took {:.3}s: {:.3}s opening inputs, {:.3}s planning, {:.3}s executing",
            (finished - self.started).as_secs_f64(),
            (self.inputs_opened - self.started).as_secs_f64(),
            (self.planned - self.inputs_opened).as_secs_f64(),
            (finished - self.planned).as_secs_f64(),
        );
        eprintln!(
            "{}",
            DisplayableExecutionPlan::with_metrics(self.plan.as_ref()).indent(true)
        );
    }
}

fn print_output_files(
    report: &FileOutputReport,
    mode: PresentationMode,
    output_path: Option<&Utf8Path>,
) -> Result<()> {
    let output = match mode {
        PresentationMode::Text => {
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
        PresentationMode::Json => serde_json::to_string_pretty(report)?,
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

fn begin_execution(
    prepared: PreparedPipeline,
    max_spill_size: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    let limit = max_spill_size.map_or(DEFAULT_MAX_TEMP_DIRECTORY_SIZE, |size| size as u64);
    let stream = prepared.begin_execution()?.into_sendable_stream();
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        stream.schema(),
        stream.map_err(move |error| explain_spill_limit(error, limit)),
    )))
}

// DataFusion reports the spill limit as an I/O error from its spill writer,
// nested under whatever wrapped that writer, telling the user to raise a session
// config key that only --max-spill-size reaches. Its message is the only marker,
// so only an I/O error counts: a data error can quote the key from a value.
fn explain_spill_limit(error: DataFusionError, limit: u64) -> DataFusionError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if current
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.to_string().contains("max_temp_directory_size"))
        {
            return DataFusionError::ResourcesExhausted(format!(
                "spilling exceeded the {} disk limit; raise it with --max-spill-size",
                bytesize::ByteSize::b(limit),
            ));
        }
        source = current.source();
    }
    error
}

pub(super) fn with_cleanup_error(primary: anyhow::Error, cleanup: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(PrimaryWithCleanup { primary, cleanup })
}

#[derive(Debug)]
struct PrimaryWithCleanup {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}

impl std::fmt::Display for PrimaryWithCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}; cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}

impl std::error::Error for PrimaryWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary.source()
    }
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

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field},
    };
    use datafusion::{datasource::MemTable, prelude::ident};
    use silk_chiffon_core::{NullPlacement, SortDirection, SortEngine};

    use super::*;

    /// Plans a sort by `id` of one partition of rows, which `ordered` declares
    /// to be in order already.
    async fn planned_sort(
        engine: SortEngine,
        ordered: bool,
        query: Option<&str>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("other", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![3, 1, 2])),
            ],
        )
        .unwrap();
        let mut table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        if ordered {
            table = table.with_sort_order(vec![vec![ident("id").sort(true, false)]]);
        }
        let mut pipeline = Pipeline::new()
            .with_sort(vec![SortColumn::new(
                "id",
                SortDirection::Ascending,
                NullPlacement::Last,
            )])
            .with_sort_engine(engine);
        let session = pipeline.create_session_context().unwrap();
        let mut input = session.read_table(Arc::new(table)).unwrap();
        if let Some(query) = query {
            input = apply_query(&session, input, query).await.unwrap();
        }
        let prepared = pipeline.prepare(input, session).await.unwrap();
        Arc::clone(prepared.execution_plan())
    }

    #[tokio::test]
    async fn an_input_already_in_order_is_not_merged_while_writing() {
        let plan = planned_sort(SortEngine::Parallel, true, None).await;
        assert!(!merges_while_writing(&plan), "{}", plan.name());
    }

    #[tokio::test]
    async fn datafusion_sort_merges_while_writing() {
        let plan = planned_sort(SortEngine::DataFusion, false, None).await;
        assert!(merges_while_writing(&plan), "{}", plan.name());
    }

    #[tokio::test]
    async fn a_sort_below_the_parallel_sort_is_done_before_writing() {
        let query = "SELECT * FROM data ORDER BY other LIMIT 2";
        let plan = planned_sort(SortEngine::Parallel, false, Some(query)).await;
        assert!(!merges_while_writing(&plan), "{}", plan.name());
    }
}
