use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::{
    AllColumnsBloomFilterConfig, BloomFilterConfig, DEFAULT_BLOOM_FILTER_FPP, DataFormat,
    ListOutputsFormat, PartitionStrategy, SortSpec, StorageConfig, TransformCommand,
    default_thread_budget,
    io_strategies::{
        OutputFileInfo, input_strategy::InputStrategy, output_strategy::SinkFactory,
        path_template::PathTemplate,
    },
    operations::{query::QueryOperation, sort::SortOperation},
    pipeline::Pipeline,
    sinks::{
        arrow::{ArrowSink, ArrowSinkOptions},
        data_sink::{DataSink, SinkResult},
        parquet::{ParquetRuntimes, ParquetSink, ParquetSinkOptions},
        vortex::{VortexSink, VortexSinkOptions},
    },
    sources::{
        arrow::ArrowDataSource, data_source::DataSource, parquet::ParquetDataSource,
        vortex::VortexDataSource,
    },
    storage::{InputObject, ObjectLocation, OutputPolicy, StorageContext},
    utils::memory::{estimate_sort_spill_reservation, sample_avg_row_bytes},
};
use anyhow::{Result, anyhow};
use apply_if::ApplyIf;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use owo_colors::OwoColorize;
use tabled::{builder::Builder, settings::Style};

pub async fn run(args: TransformCommand) -> Result<()> {
    run_with_storage(args, &StorageConfig::default()).await
}

pub async fn run_with_storage(
    args: TransformCommand,
    storage_config: &StorageConfig,
) -> Result<()> {
    let storage = Arc::new(StorageContext::new(*storage_config)?);
    run_with_storage_context(args, storage).await
}

pub(crate) async fn run_with_storage_context(
    args: TransformCommand,
    storage: Arc<StorageContext>,
) -> Result<()> {
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

    let (inputs, multiple) = if let Some(single_input) = from {
        (vec![storage.resolve_input(&single_input).await?], false)
    } else {
        (storage.resolve_inputs(&from_many).await?, true)
    };

    let input_strategy = if !multiple {
        let source = make_source(&inputs[0], input_format)?;
        InputStrategy::Single(source)
    } else {
        let sources: Vec<Box<dyn DataSource>> = inputs
            .iter()
            .map(|input| make_source(input, input_format))
            .collect::<Result<_>>()?;
        InputStrategy::Multiple(sources)
    };
    input_strategy.validate_schemas().await?;

    // sample rows to estimate sort spill reservation before handing strategy to pipeline
    if has_sort {
        let avg_row_bytes = sample_avg_row_bytes(&input_strategy, 100_000).await?;

        if avg_row_bytes > 0 {
            let total_rows = input_strategy.row_count().await.unwrap_or(0);
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

    let output_policy = OutputPolicy::new(overwrite, create_dirs);
    let mut manifest_output = match (list_outputs_format, list_outputs_file.as_deref()) {
        (Some(ListOutputsFormat::Text | ListOutputsFormat::Json), Some(path)) => {
            Some(storage.create_output(path, output_policy).await?)
        }
        _ => None,
    };
    let manifest_location = manifest_output
        .as_ref()
        .map(|output| output.destination().clone());
    let output_factory_context = OutputFactoryContext {
        storage: Arc::clone(&storage),
        policy: output_policy,
        manifest: manifest_location.clone(),
    };

    if let (Some(manifest), Some(output_path)) = (&manifest_location, to.as_deref()) {
        let data_output = storage.resolve(output_path)?;
        ensure_distinct_outputs(manifest, &data_output)?;
    }

    let sink_factory = create_sink_factory(
        output_factory_context.clone(),
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
        );
    } else if let Some(template) = to_many {
        let path_template = PathTemplate::new(template);
        if let (Some(manifest), Some(output_path)) =
            (&manifest_location, path_template.static_location())
        {
            let data_output = storage.resolve(output_path)?;
            ensure_distinct_outputs(manifest, &data_output)?;
        }

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
                );
            }
            PartitionStrategy::NosortEvict => {
                // each partition gets its own writer, so we minimize per-writer
                // concurrency to avoid scheduling overhead from 100+ parallel pipelines
                let evict_sink_factory = create_sink_factory(
                    output_factory_context,
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
                    max_open_partitions,
                );
            }
            PartitionStrategy::SortSingle => {
                pipeline = pipeline.with_single_writer_partitioned_sink(
                    partition_columns,
                    path_template,
                    sink_factory,
                    exclude_columns.clone(),
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
        let output = render_output_files(&files, format, manifest_output.is_none())?;
        if let Some(mut manifest) = manifest_output.take() {
            manifest.write(Bytes::from(output)).await?;
            manifest.commit().await?;
        } else if !output.is_empty() {
            println!("{output}");
        }
    }

    Ok(())
}

fn render_output_files(
    files: &[OutputFileInfo],
    format: ListOutputsFormat,
    colors: bool,
) -> Result<String> {
    let mut sorted_files = files.to_vec();
    sorted_files.sort_by(|left, right| left.path.cmp(&right.path));
    let files = sorted_files.as_slice();
    let output = match format {
        ListOutputsFormat::None => return Ok(String::new()),
        ListOutputsFormat::Text => {
            if files.is_empty() {
                return Ok(String::new());
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

            if colors {
                // writing to stdout, so use colors
                let colored_header: Vec<String> =
                    header.iter().map(|h| h.bold().to_string()).collect();
                builder.push_record(colored_header);
            } else {
                builder.push_record(header);
            }

            for file in files {
                let mut row: Vec<String> = file
                    .partition_values
                    .iter()
                    .map(|pv| {
                        let val = format_json_value(&pv.value);
                        if colors {
                            // again, writing to stdout, so use colors
                            val.green().to_string()
                        } else {
                            val
                        }
                    })
                    .collect();
                row.push(file.path.clone());
                let row_count = file.row_count.to_string();
                if colors {
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

    Ok(output)
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

fn make_source(
    input: &InputObject,
    input_format: Option<DataFormat>,
) -> Result<Box<dyn DataSource>> {
    let format = detect_input_format(input, input_format)?;
    Ok(match format {
        DataFormat::Arrow => Box::new(ArrowDataSource::new(input.clone())),
        DataFormat::Parquet => Box::new(ParquetDataSource::new(input.clone())),
        DataFormat::Vortex => Box::new(VortexDataSource::new(input.clone())),
    })
}

fn detect_input_format(
    input: &InputObject,
    explicit_format: Option<DataFormat>,
) -> Result<DataFormat> {
    if let Some(format) = explicit_format {
        return Ok(format);
    }

    match input.extension() {
        Some(extension) if extension.eq_ignore_ascii_case("arrow") => Ok(DataFormat::Arrow),
        Some(extension) if extension.eq_ignore_ascii_case("parquet") => Ok(DataFormat::Parquet),
        Some(extension) if extension.eq_ignore_ascii_case("vortex") => Ok(DataFormat::Vortex),
        _ => Err(anyhow!(
            "Could not detect format from path '{}'. Use --input-format to specify explicitly.",
            input.location().display()
        )),
    }
}

fn detect_output_format(
    location: &ObjectLocation,
    explicit_format: Option<DataFormat>,
) -> Result<DataFormat> {
    if let Some(format) = explicit_format {
        return Ok(format);
    }

    if let Some(ext) = location.path().extension() {
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
        location.display()
    ))
}

#[derive(Clone)]
struct OutputFactoryContext {
    storage: Arc<StorageContext>,
    policy: OutputPolicy,
    manifest: Option<ObjectLocation>,
}

fn create_sink_factory(
    context: OutputFactoryContext,
    output_format: Option<DataFormat>,
    arrow_opts: ArrowSinkOptions,
    parquet_opts: ParquetSinkOptions,
    vortex_opts: VortexSinkOptions,
    runtimes: Arc<ParquetRuntimes>,
) -> Result<SinkFactory> {
    Ok(Box::new(move |path: String, schema: SchemaRef| {
        let context = context.clone();
        let arrow_opts = arrow_opts.clone();
        let parquet_opts = parquet_opts.clone();
        let runtimes = Arc::clone(&runtimes);
        let future = Box::pin(async move {
            let destination = context.storage.resolve(&path)?;
            let detected_format = detect_output_format(&destination, output_format)?;
            if let Some(manifest) = &context.manifest {
                ensure_distinct_outputs(manifest, &destination)?;
            }
            let output = context
                .storage
                .create_output_at(destination, context.policy)
                .await?;
            let sink: Box<dyn DataSink> = match detected_format {
                DataFormat::Arrow => Box::new(ArrowSink::create(output, &schema, arrow_opts)?),
                DataFormat::Parquet => Box::new(ParquetSink::create(
                    output,
                    &schema,
                    &parquet_opts,
                    runtimes,
                )?),
                DataFormat::Vortex => Box::new(VortexSink::create(output, &schema, vortex_opts)?),
            };
            Ok(sink)
        });
        Ok(Box::new(PendingSink::new(future)))
    }))
}

fn ensure_distinct_outputs(manifest: &ObjectLocation, data: &ObjectLocation) -> Result<()> {
    if manifest.same_object(data) {
        anyhow::bail!(
            "output manifest '{}' collides with data output '{}'",
            manifest.display(),
            data.display()
        );
    }
    Ok(())
}

type PendingSinkFuture = BoxFuture<'static, Result<Box<dyn DataSink>>>;

// OutputStrategy's factory boundary is synchronous. Deferring the async
// location resolution and ObjectOutput preflight keeps that API stable while
// ensuring no sink opens before its first write or finish.
struct PendingSink {
    future: std::sync::Mutex<Option<PendingSinkFuture>>,
    sink: Option<Box<dyn DataSink>>,
}

impl PendingSink {
    fn new(future: PendingSinkFuture) -> Self {
        Self {
            future: std::sync::Mutex::new(Some(future)),
            sink: None,
        }
    }

    async fn ensure_sink(&mut self) -> Result<&mut Box<dyn DataSink>> {
        if self.sink.is_none() {
            let future = self
                .future
                .lock()
                .map_err(|_| anyhow!("sink creation lock is poisoned"))?
                .take()
                .ok_or_else(|| anyhow!("sink creation already failed"))?;
            self.sink = Some(future.await?);
        }
        self.sink
            .as_mut()
            .ok_or_else(|| anyhow!("sink was not created"))
    }
}

#[async_trait]
impl DataSink for PendingSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.ensure_sink().await?.write_batch(batch).await
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        self.ensure_sink().await?.finish().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use tempfile::TempDir;

    use crate::utils::test_helpers::file_helpers;

    fn write_input(temp: &TempDir, names: &[&str]) -> String {
        let path = temp.path().join("input.arrow");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(
                    0..i32::try_from(names.len()).unwrap(),
                )),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap();
        file_helpers::write_arrow_file(&path, &schema, vec![batch]).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[tokio::test]
    async fn local_text_manifest_commits_after_partition_outputs() {
        let temp = TempDir::new().unwrap();
        let input = write_input(&temp, &["a"]);
        let mut command = TransformCommand {
            from: Some(input),
            to: None,
            to_many: Some(
                temp.path()
                    .join("output/{{name}}.arrow")
                    .to_string_lossy()
                    .into_owned(),
            ),
            by: Some("name".to_string()),
            output_format: Some(DataFormat::Arrow),
            list_outputs: Some(ListOutputsFormat::Text),
            ..Default::default()
        };
        let manifest = temp.path().join("manifests/outputs.txt");
        command.list_outputs_file = Some(manifest.to_string_lossy().into_owned());

        run(command).await.unwrap();

        let text = std::fs::read_to_string(manifest).unwrap();
        assert!(text.contains("output/a.arrow"));
    }

    #[tokio::test]
    async fn manifest_respects_local_directory_policy_before_pipeline_execution() {
        let temp = TempDir::new().unwrap();
        let input = write_input(&temp, &["a"]);
        let output = temp.path().join("output/{{name}}.arrow");
        let manifest = temp.path().join("missing/outputs.json");
        let make_command = |create_dirs| TransformCommand {
            from: Some(input.clone()),
            to: None,
            to_many: Some(output.to_string_lossy().into_owned()),
            by: Some("name".to_string()),
            output_format: Some(DataFormat::Arrow),
            list_outputs: Some(ListOutputsFormat::Json),
            list_outputs_file: Some(manifest.to_string_lossy().into_owned()),
            create_dirs,
            overwrite: true,
            ..Default::default()
        };

        let error = run(make_command(false)).await.unwrap_err();
        assert!(error.to_string().contains("parent directory"));
        assert!(!temp.path().join("output/a.arrow").exists());
        run(make_command(true)).await.unwrap();

        assert!(manifest.exists());
    }

    #[tokio::test]
    async fn static_manifest_collision_is_rejected_before_pipeline_execution() {
        let temp = TempDir::new().unwrap();
        let input = write_input(&temp, &["a"]);
        let collision = temp.path().join("collision.arrow");
        let command = TransformCommand {
            from: Some(input),
            to: None,
            to_many: Some(collision.to_string_lossy().into_owned()),
            by: Some("name".to_string()),
            list_outputs: Some(ListOutputsFormat::Json),
            list_outputs_file: Some(collision.to_string_lossy().into_owned()),
            output_format: Some(DataFormat::Arrow),
            ..Default::default()
        };

        let error = run(command).await.unwrap_err();

        assert!(error.to_string().contains("collides with data output"));
        assert!(!collision.exists());
    }
}
