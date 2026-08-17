mod partitioner;
mod report;
mod target_template;

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    num::NonZeroUsize,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use lru::LruCache;
use silk_chiffon_core::{
    DataSink, SinkBinding, SinkBindingConfig, SinkCompletion, TransformBinding, TransformBindings,
};
use silk_chiffon_storage::{
    ExistingOutput, LocationInput, OutputPreparation, StorageHandle, StorageSession,
};

use crate::{PartitionStrategy, utils::projected_stream::project_stream};

use partitioner::{
    PartitionValues, Partitioner, partition_key, partition_values_equal,
    validate_partition_columns_primitive,
};
pub(super) use report::FileOutputReport;
use report::{CompletedFileOutput, partition_field_values};
use target_template::OutputTargetTemplate;

pub(super) enum FileOutputRequest {
    Exact {
        target: String,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
    },
    Template {
        pattern: String,
        partition_fields: Vec<String>,
        strategy: PartitionStrategy,
        max_open_partitions: Option<usize>,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
    },
}

/// Binds file output behavior after the final plan and budgets are known.
pub(super) struct FileOutputBinder<'a> {
    storage: &'a StorageSession,
    formats: &'a TransformBindings,
    explicit_format: Option<&'a str>,
}

impl<'a> FileOutputBinder<'a> {
    pub(super) fn new(
        storage: &'a StorageSession,
        formats: &'a TransformBindings,
        explicit_format: Option<&'a str>,
    ) -> Self {
        Self {
            storage,
            formats,
            explicit_format,
        }
    }

    pub(super) async fn bind(
        &self,
        target: FileOutputRequest,
        sink_config: &SinkBindingConfig,
        output_schema: &SchemaRef,
    ) -> Result<BoundFileOutput> {
        match target {
            FileOutputRequest::Exact {
                target,
                exclude_columns,
                create_dirs,
                overwrite,
            } => {
                validate_excluded_columns(output_schema, &exclude_columns)?;
                let location = LocationInput::parse(&target)
                    .with_context(|| format!("while parsing exact file output {target:?}"))?;
                let handle = self
                    .storage
                    .prepare_output_target(&location, &output_preparation(overwrite, create_dirs))
                    .await
                    .with_context(|| format!("while preparing exact file output {target:?}"))?;
                let format = self.format_for_handle(&handle, &target)?;
                let sink_binding =
                    Arc::from(format.bind_sink(sink_config).await.with_context(|| {
                        format!("while binding format for exact file output {target:?}")
                    })?);
                Ok(BoundFileOutput::Exact {
                    target,
                    handle,
                    sink_binding,
                    exclude_columns,
                })
            }
            FileOutputRequest::Template {
                pattern,
                partition_fields,
                strategy,
                max_open_partitions,
                exclude_columns,
                create_dirs,
                overwrite,
            } => {
                validate_excluded_columns(output_schema, &exclude_columns)?;
                validate_partition_columns_primitive(output_schema, &partition_fields)?;
                if partition_fields.iter().any(|field| field == "file_number") {
                    anyhow::bail!(
                        "partition field \"file_number\" is reserved for nosort-evict output templates"
                    );
                }

                let template = OutputTargetTemplate::new(pattern.clone())
                    .with_context(|| format!("invalid file output template {pattern:?}"))?;
                let mut referenced_fields = template
                    .referenced_fields()
                    .with_context(|| format!("invalid file output template {pattern:?}"))?;
                if strategy == PartitionStrategy::NosortEvict {
                    template.require_file_number()?;
                    referenced_fields.remove("file_number");
                } else if referenced_fields.contains("file_number") {
                    anyhow::bail!(
                        "file_number is available only with --partition-strategy=nosort-evict"
                    );
                }
                for field in referenced_fields {
                    if !partition_fields.contains(&field) {
                        anyhow::bail!(
                            "file output template field {field:?} is not selected by --by"
                        );
                    }
                }
                if max_open_partitions.is_some() && strategy != PartitionStrategy::NosortEvict {
                    anyhow::bail!(
                        "--max-open-partitions is only supported with \
                         --partition-strategy=nosort-evict"
                    );
                }
                let max_open = NonZeroUsize::new(max_open_partitions.unwrap_or(100))
                    .ok_or_else(|| anyhow!("--max-open-partitions must be at least 1"))?;
                let format = self.format_for_extension(template.static_extension(), &pattern)?;
                let sink_binding =
                    Arc::from(format.bind_sink(sink_config).await.with_context(|| {
                        format!("while binding format for partitioned file output {pattern:?}")
                    })?);
                Ok(BoundFileOutput::Partitioned {
                    writer: PartitionedFileWriter {
                        storage: self.storage.clone(),
                        sink_binding,
                        partition_fields,
                        template,
                        exclude_columns,
                        preparation: output_preparation(overwrite, create_dirs),
                    },
                    strategy,
                    max_open,
                })
            }
        }
    }

    fn format_for_handle<'b>(
        &'b self,
        handle: &StorageHandle,
        target: &str,
    ) -> Result<&'b TransformBinding> {
        if let Some(format) = self.explicit_format {
            return self
                .formats
                .get(format)
                .ok_or_else(|| anyhow!("format is not registered: {format}"));
        }
        self.format_for_extension(handle.object_path().extension(), target)
    }

    fn format_for_extension(
        &self,
        extension: Option<&str>,
        target: &str,
    ) -> Result<&TransformBinding> {
        if let Some(format) = self.explicit_format {
            return self
                .formats
                .get(format)
                .ok_or_else(|| anyhow!("format is not registered: {format}"));
        }
        extension
            .and_then(|extension| self.formats.by_extension(extension))
            .ok_or_else(|| {
                anyhow!(
                    "Could not detect format from path {target:?}. Use \
                     --output-format to specify explicitly."
                )
            })
    }
}

pub(super) enum BoundFileOutput {
    Exact {
        target: String,
        handle: StorageHandle,
        sink_binding: Arc<dyn SinkBinding>,
        exclude_columns: Vec<String>,
    },
    Partitioned {
        writer: PartitionedFileWriter,
        strategy: PartitionStrategy,
        max_open: NonZeroUsize,
    },
}

impl BoundFileOutput {
    pub(super) async fn write(
        self,
        stream: SendableRecordBatchStream,
    ) -> std::result::Result<FileOutputReport, FileOutputFailure> {
        match self {
            Self::Exact {
                target,
                handle,
                sink_binding,
                exclude_columns,
            } => write_exact(target, handle, sink_binding, stream, exclude_columns).await,
            Self::Partitioned {
                writer,
                strategy,
                max_open,
            } => match strategy {
                PartitionStrategy::SortSingle => writer.write_sort_single(stream).await,
                PartitionStrategy::NosortMulti => writer.write_nosort_multi(stream).await,
                PartitionStrategy::NosortEvict => writer.write_nosort_evict(stream, max_open).await,
            },
        }
    }
}

pub(super) struct FileOutputFailure {
    primary: anyhow::Error,
    report: FileOutputReport,
}

impl FileOutputFailure {
    fn new(
        primary: anyhow::Error,
        completed: Vec<CompletedFileOutput>,
        cleanup_errors: Vec<anyhow::Error>,
    ) -> Self {
        let primary = cleanup_errors
            .into_iter()
            .fold(primary, |primary, cleanup| {
                crate::sinks::with_cleanup_error(primary, Some(cleanup))
            });
        Self {
            primary,
            report: FileOutputReport::new(completed),
        }
    }

    pub(super) fn report(&self) -> &FileOutputReport {
        &self.report
    }
}

impl fmt::Debug for FileOutputFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileOutputFailure")
            .field("primary", &self.primary)
            .field("report", &self.report)
            .finish()
    }
}

impl fmt::Display for FileOutputFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "file output failed after {} completed output(s): {:#}",
            self.report.outputs().len(),
            self.primary
        )
    }
}

impl std::error::Error for FileOutputFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

async fn write_exact(
    target: String,
    handle: StorageHandle,
    sink_binding: Arc<dyn SinkBinding>,
    stream: SendableRecordBatchStream,
    exclude_columns: Vec<String>,
) -> std::result::Result<FileOutputReport, FileOutputFailure> {
    let stream = match projection_indices_excluding(&stream.schema(), &exclude_columns) {
        Some(indices) => project_stream(stream, indices)
            .map_err(|error| FileOutputFailure::new(error.into(), Vec::new(), Vec::new()))?,
        None => stream,
    };
    let mut sink = sink_binding
        .open_sink(handle, Arc::clone(&stream.schema()))
        .await
        .with_context(|| format!("while opening exact file output {target:?}"))
        .map_err(|error| FileOutputFailure::new(error, Vec::new(), Vec::new()))?;
    if let Err(primary) = sink
        .write_stream(stream)
        .await
        .with_context(|| format!("while writing exact file output {target:?}"))
    {
        let cleanup = sink.abort().await.err().into_iter().collect();
        return Err(FileOutputFailure::new(primary, Vec::new(), cleanup));
    }
    let completion = sink
        .finish()
        .await
        .with_context(|| format!("while completing exact file output {target:?}"))
        .map_err(|error| FileOutputFailure::new(error, Vec::new(), Vec::new()))?;
    Ok(FileOutputReport::new(vec![completed_output(
        &completion,
        Vec::new(),
    )]))
}

struct OpenFileSink {
    target: String,
    sink: Box<dyn DataSink>,
    partition_values: PartitionValues,
}

impl OpenFileSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.sink
            .write_batch(batch)
            .await
            .with_context(|| format!("while writing partitioned file output {:?}", self.target))
    }
}

pub(super) struct PartitionedFileWriter {
    storage: StorageSession,
    sink_binding: Arc<dyn SinkBinding>,
    partition_fields: Vec<String>,
    template: OutputTargetTemplate,
    exclude_columns: Vec<String>,
    preparation: OutputPreparation,
}

impl PartitionedFileWriter {
    async fn write_sort_single(
        &self,
        stream: SendableRecordBatchStream,
    ) -> std::result::Result<FileOutputReport, FileOutputFailure> {
        let context = PartitionProjection::new(
            &stream.schema(),
            &self.partition_fields,
            &self.exclude_columns,
        )
        .map_err(|error| FileOutputFailure::new(error, Vec::new(), Vec::new()))?;
        let mut partitioned =
            Partitioner::new(self.partition_fields.clone()).partition_stream(stream);
        let mut current: Option<OpenFileSink> = None;
        let mut completed = Vec::new();

        while let Some(item) = partitioned.next().await {
            let (values, batch) = match item {
                Ok(item) => item,
                Err(primary) => {
                    let cleanup = self.abort_all(current.take().into_iter().collect()).await;
                    return Err(FileOutputFailure::new(primary, completed, cleanup));
                }
            };
            let changed = current
                .as_ref()
                .is_some_and(|open| !partition_values_equal(&open.partition_values, &values));
            if changed {
                let open = current.take().expect("checked above");
                match self.finish(open).await {
                    Ok(output) => completed.push(output),
                    Err(primary) => {
                        return Err(FileOutputFailure::new(primary, completed, Vec::new()));
                    }
                }
            }
            if current.is_none() {
                current = match self.open(&values, &context.projected_schema, None).await {
                    Ok(open) => Some(open),
                    Err(primary) => {
                        return Err(FileOutputFailure::new(primary, completed, Vec::new()));
                    }
                };
            }
            let projected = match context.project_batch(batch) {
                Ok(batch) => batch,
                Err(primary) => {
                    let cleanup = self.abort_all(current.take().into_iter().collect()).await;
                    return Err(FileOutputFailure::new(primary, completed, cleanup));
                }
            };
            if let Err(primary) = current
                .as_mut()
                .expect("current partition has an open sink")
                .write_batch(projected)
                .await
            {
                let cleanup = self.abort_all(current.take().into_iter().collect()).await;
                return Err(FileOutputFailure::new(primary, completed, cleanup));
            }
        }
        if let Some(open) = current {
            match self.finish(open).await {
                Ok(output) => completed.push(output),
                Err(primary) => {
                    return Err(FileOutputFailure::new(primary, completed, Vec::new()));
                }
            }
        }
        Ok(FileOutputReport::new(completed))
    }

    async fn write_nosort_multi(
        &self,
        stream: SendableRecordBatchStream,
    ) -> std::result::Result<FileOutputReport, FileOutputFailure> {
        let context = PartitionProjection::new(
            &stream.schema(),
            &self.partition_fields,
            &self.exclude_columns,
        )
        .map_err(|error| FileOutputFailure::new(error, Vec::new(), Vec::new()))?;
        let mut partitioned =
            Partitioner::new(self.partition_fields.clone()).partition_stream(stream);
        let mut open = HashMap::<String, OpenFileSink>::new();

        while let Some(item) = partitioned.next().await {
            let (values, batch) = match item {
                Ok(item) => item,
                Err(primary) => {
                    let cleanup = self.abort_all(open.into_values().collect()).await;
                    return Err(FileOutputFailure::new(primary, Vec::new(), cleanup));
                }
            };
            let key = partition_key(&values, &context.field_order);
            let sink = match open.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let new_sink = match self.open(&values, &context.projected_schema, None).await {
                        Ok(sink) => sink,
                        Err(primary) => {
                            let cleanup = self.abort_all(open.into_values().collect()).await;
                            return Err(FileOutputFailure::new(primary, Vec::new(), cleanup));
                        }
                    };
                    entry.insert(new_sink)
                }
            };
            let projected = match context.project_batch(batch) {
                Ok(batch) => batch,
                Err(primary) => {
                    let cleanup = self.abort_all(open.into_values().collect()).await;
                    return Err(FileOutputFailure::new(primary, Vec::new(), cleanup));
                }
            };
            if let Err(primary) = sink.write_batch(projected).await {
                let cleanup = self.abort_all(open.into_values().collect()).await;
                return Err(FileOutputFailure::new(primary, Vec::new(), cleanup));
            }
        }

        let mut remaining: Vec<_> = open.into_values().collect();
        remaining.sort_by(|left, right| left.target.cmp(&right.target));
        let mut completed = Vec::new();
        while !remaining.is_empty() {
            let sink = remaining.remove(0);
            match self.finish(sink).await {
                Ok(output) => completed.push(output),
                Err(primary) => {
                    let cleanup = self.abort_all(remaining).await;
                    return Err(FileOutputFailure::new(primary, completed, cleanup));
                }
            }
        }
        Ok(FileOutputReport::new(completed))
    }

    async fn write_nosort_evict(
        &self,
        stream: SendableRecordBatchStream,
        max_open: NonZeroUsize,
    ) -> std::result::Result<FileOutputReport, FileOutputFailure> {
        let context = PartitionProjection::new(
            &stream.schema(),
            &self.partition_fields,
            &self.exclude_columns,
        )
        .map_err(|error| FileOutputFailure::new(error, Vec::new(), Vec::new()))?;
        let mut partitioned =
            Partitioner::new(self.partition_fields.clone()).partition_stream(stream);
        let mut open = LruCache::<String, OpenFileSink>::new(max_open);
        let mut file_numbers = HashMap::<String, usize>::new();
        let mut completed = Vec::new();

        while let Some(item) = partitioned.next().await {
            let (values, batch) = match item {
                Ok(item) => item,
                Err(primary) => {
                    let cleanup = self
                        .abort_all(open.into_iter().map(|(_, sink)| sink).collect())
                        .await;
                    return Err(FileOutputFailure::new(primary, completed, cleanup));
                }
            };
            let key = partition_key(&values, &context.field_order);
            if !open.contains(&key) {
                if open.len() == max_open.get() {
                    let (_, evicted) = open.pop_lru().expect("a full cache has a victim");
                    match self.finish(evicted).await {
                        Ok(output) => completed.push(output),
                        Err(primary) => {
                            let cleanup = self
                                .abort_all(open.into_iter().map(|(_, sink)| sink).collect())
                                .await;
                            return Err(FileOutputFailure::new(primary, completed, cleanup));
                        }
                    }
                }

                let file_number = *file_numbers.get(&key).unwrap_or(&0);
                let new_sink = match self
                    .open(&values, &context.projected_schema, Some(file_number))
                    .await
                {
                    Ok(sink) => sink,
                    Err(primary) => {
                        let cleanup = self
                            .abort_all(open.into_iter().map(|(_, sink)| sink).collect())
                            .await;
                        return Err(FileOutputFailure::new(primary, completed, cleanup));
                    }
                };
                file_numbers.insert(key.clone(), file_number + 1);
                open.put(key.clone(), new_sink);
            }
            let projected = match context.project_batch(batch) {
                Ok(batch) => batch,
                Err(primary) => {
                    let cleanup = self
                        .abort_all(open.into_iter().map(|(_, sink)| sink).collect())
                        .await;
                    return Err(FileOutputFailure::new(primary, completed, cleanup));
                }
            };
            if let Err(primary) = open
                .get_mut(&key)
                .expect("the current partition has an open sink")
                .write_batch(projected)
                .await
            {
                let cleanup = self
                    .abort_all(open.into_iter().map(|(_, sink)| sink).collect())
                    .await;
                return Err(FileOutputFailure::new(primary, completed, cleanup));
            }
        }

        let mut remaining: Vec<_> = open.into_iter().map(|(_, sink)| sink).collect();
        remaining.sort_by(|left, right| left.target.cmp(&right.target));
        while !remaining.is_empty() {
            let sink = remaining.remove(0);
            match self.finish(sink).await {
                Ok(output) => completed.push(output),
                Err(primary) => {
                    let cleanup = self.abort_all(remaining).await;
                    return Err(FileOutputFailure::new(primary, completed, cleanup));
                }
            }
        }
        Ok(FileOutputReport::new(completed))
    }

    async fn open(
        &self,
        values: &PartitionValues,
        schema: &SchemaRef,
        file_number: Option<usize>,
    ) -> Result<OpenFileSink> {
        let location = self.template.render(values, file_number)?;
        let handle = self
            .storage
            .prepare_output_target(&location, &self.preparation)
            .await
            .context("while preparing partitioned file output")?;
        let target = handle.url().to_string();
        let sink = self
            .sink_binding
            .open_sink(handle, Arc::clone(schema))
            .await
            .with_context(|| format!("while opening partitioned file output {target:?}"))?;
        Ok(OpenFileSink {
            target,
            sink,
            partition_values: values.clone(),
        })
    }

    async fn finish(&self, open: OpenFileSink) -> Result<CompletedFileOutput> {
        let OpenFileSink {
            target,
            sink,
            partition_values,
        } = open;
        let fields = partition_field_values(&partition_values, &self.partition_fields);
        Ok(completed_output(
            &sink
                .finish()
                .await
                .with_context(|| format!("while completing partitioned file output {target:?}"))?,
            fields,
        ))
    }

    async fn abort_all(&self, mut open: Vec<OpenFileSink>) -> Vec<anyhow::Error> {
        open.sort_by(|left, right| left.target.cmp(&right.target));
        let mut errors = Vec::new();
        for sink in open {
            let target = sink.target.clone();
            if let Err(error) = sink
                .sink
                .abort()
                .await
                .with_context(|| format!("while aborting partitioned file output {target:?}"))
            {
                errors.push(error);
            }
        }
        errors
    }
}

struct PartitionProjection {
    field_order: Vec<String>,
    projected_indices: Option<Vec<usize>>,
    projected_schema: SchemaRef,
}

impl PartitionProjection {
    fn new(schema: &SchemaRef, fields: &[String], exclude_columns: &[String]) -> Result<Self> {
        validate_partition_columns_primitive(schema, fields)?;
        validate_excluded_columns(schema, exclude_columns)?;
        let projected_indices = projection_indices_excluding(schema, exclude_columns);
        let projected_schema = match &projected_indices {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => Arc::clone(schema),
        };
        Ok(Self {
            field_order: fields.to_vec(),
            projected_indices,
            projected_schema,
        })
    }

    fn project_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.projected_indices {
            Some(indices) => Ok(batch.project(indices)?),
            None => Ok(batch),
        }
    }
}

fn output_preparation(overwrite: bool, create_dirs: bool) -> OutputPreparation {
    OutputPreparation::new(
        if overwrite {
            ExistingOutput::Allow
        } else {
            ExistingOutput::RejectIfObserved
        },
        create_dirs,
    )
}

fn completed_output(
    completion: &SinkCompletion,
    partition_fields: Vec<report::PartitionFieldValue>,
) -> CompletedFileOutput {
    CompletedFileOutput {
        durable_locations: completion
            .durable_locations()
            .iter()
            .map(ToString::to_string)
            .collect(),
        rows_written: completion.rows_written(),
        partition_fields,
    }
}

fn validate_excluded_columns(schema: &SchemaRef, exclude_columns: &[String]) -> Result<()> {
    for column in exclude_columns {
        schema
            .column_with_name(column)
            .ok_or_else(|| anyhow!("Column {column:?} not found in schema"))?;
    }
    Ok(())
}

fn projection_indices_excluding(
    schema: &SchemaRef,
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
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
    };
    use async_trait::async_trait;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    use super::*;
    use crate::utils::test_helpers::prepared_local_output;

    struct FailingSinkBinding {
        aborts: Arc<AtomicUsize>,
    }

    struct FailingSink {
        aborts: Arc<AtomicUsize>,
    }

    #[derive(Default)]
    struct ScriptedState {
        events: Vec<String>,
        active: usize,
        maximum_active: usize,
        fail_open: Option<String>,
        fail_finish: Option<String>,
        fail_abort: Option<String>,
    }

    struct ScriptedSinkBinding {
        state: Arc<Mutex<ScriptedState>>,
    }

    struct ScriptedSink {
        target: String,
        target_url: url::Url,
        rows_written: u64,
        state: Arc<Mutex<ScriptedState>>,
    }

    #[async_trait]
    impl SinkBinding for FailingSinkBinding {
        async fn open_sink(
            &self,
            _handle: StorageHandle,
            _schema: SchemaRef,
        ) -> Result<Box<dyn DataSink>> {
            Ok(Box::new(FailingSink {
                aborts: Arc::clone(&self.aborts),
            }))
        }
    }

    #[async_trait]
    impl DataSink for FailingSink {
        async fn write_batch(&mut self, _batch: RecordBatch) -> Result<()> {
            Err(anyhow!("primary write failure"))
        }

        async fn finish(self: Box<Self>) -> Result<SinkCompletion> {
            unreachable!("a failed sink is aborted instead of finished")
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("cleanup failure"))
        }
    }

    #[async_trait]
    impl SinkBinding for ScriptedSinkBinding {
        async fn open_sink(
            &self,
            handle: StorageHandle,
            _schema: SchemaRef,
        ) -> Result<Box<dyn DataSink>> {
            let target = handle
                .url()
                .path_segments()
                .and_then(|mut segments| segments.next_back())
                .expect("test target has a filename")
                .to_owned();
            let mut state = self.state.lock().unwrap();
            state.events.push(format!("open:{target}"));
            if state.fail_open.as_ref() == Some(&target) {
                return Err(anyhow!("scripted open failure for {target}"));
            }
            state.active += 1;
            state.maximum_active = state.maximum_active.max(state.active);
            drop(state);
            Ok(Box::new(ScriptedSink {
                target,
                target_url: handle.url().clone(),
                rows_written: 0,
                state: Arc::clone(&self.state),
            }))
        }
    }

    #[async_trait]
    impl DataSink for ScriptedSink {
        async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
            self.rows_written += batch.num_rows() as u64;
            self.state
                .lock()
                .unwrap()
                .events
                .push(format!("write:{}", self.target));
            Ok(())
        }

        async fn finish(self: Box<Self>) -> Result<SinkCompletion> {
            let mut state = self.state.lock().unwrap();
            state.events.push(format!("finish:{}", self.target));
            state.active -= 1;
            if state.fail_finish.as_ref() == Some(&self.target) {
                return Err(anyhow!("scripted finish failure for {}", self.target));
            }
            drop(state);
            Ok(SinkCompletion::new(self.target_url, [], self.rows_written))
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.events.push(format!("abort:{}", self.target));
            state.active -= 1;
            if state.fail_abort.as_ref() == Some(&self.target) {
                return Err(anyhow!("scripted abort failure for {}", self.target));
            }
            Ok(())
        }
    }

    fn partition_stream(values: &[&str]) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "category",
            DataType::Utf8,
            false,
        )]));
        let batches = values
            .iter()
            .map(|value| {
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(StringArray::from(vec![*value]))],
                )
                .unwrap())
            })
            .collect::<Vec<datafusion::error::Result<_>>>();
        Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
    }

    fn scripted_partition_writer(
        pattern: &std::path::Path,
        state: Arc<Mutex<ScriptedState>>,
    ) -> PartitionedFileWriter {
        PartitionedFileWriter {
            storage: silk_chiffon_storage::local::session().unwrap(),
            sink_binding: Arc::new(ScriptedSinkBinding { state }),
            partition_fields: vec!["category".to_owned()],
            template: OutputTargetTemplate::new(pattern.to_string_lossy().into_owned()).unwrap(),
            exclude_columns: Vec::new(),
            preparation: OutputPreparation::new(ExistingOutput::Allow, false),
        }
    }

    #[tokio::test]
    async fn exact_write_failure_awaits_abort_and_retains_cleanup_context() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("output.arrow");
        let handle = prepared_local_output(&target);
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter([Ok(batch)]),
        ));
        let aborts = Arc::new(AtomicUsize::new(0));

        let failure = write_exact(
            target.to_string_lossy().into_owned(),
            handle,
            Arc::new(FailingSinkBinding {
                aborts: Arc::clone(&aborts),
            }),
            stream,
            Vec::new(),
        )
        .await
        .unwrap_err();

        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert!(failure.report().outputs().is_empty());
        let message = failure.to_string();
        assert!(message.contains("primary write failure"), "{message}");
        assert!(message.contains("cleanup failure"), "{message}");
    }

    #[tokio::test]
    async fn nosort_multi_open_failure_aborts_every_previously_open_sink() {
        let temporary = tempfile::tempdir().unwrap();
        let pattern = temporary.path().join("{{category}}.arrow");
        let state = Arc::new(Mutex::new(ScriptedState {
            fail_open: Some("b.arrow".to_owned()),
            ..ScriptedState::default()
        }));
        let writer = scripted_partition_writer(&pattern, Arc::clone(&state));

        let failure = writer
            .write_nosort_multi(partition_stream(&["a", "b"]))
            .await
            .unwrap_err();

        assert!(
            failure
                .to_string()
                .contains("scripted open failure for b.arrow")
        );
        assert!(failure.report().outputs().is_empty());
        let state = state.lock().unwrap();
        assert_eq!(state.active, 0);
        assert_eq!(
            state.events,
            [
                "open:a.arrow",
                "write:a.arrow",
                "open:b.arrow",
                "abort:a.arrow"
            ]
        );
    }

    #[tokio::test]
    async fn nosort_multi_finish_failure_reports_completed_and_aborts_remaining_in_order() {
        let temporary = tempfile::tempdir().unwrap();
        let pattern = temporary.path().join("{{category}}.arrow");
        let state = Arc::new(Mutex::new(ScriptedState {
            fail_finish: Some("b.arrow".to_owned()),
            fail_abort: Some("c.arrow".to_owned()),
            ..ScriptedState::default()
        }));
        let writer = scripted_partition_writer(&pattern, Arc::clone(&state));

        let failure = writer
            .write_nosort_multi(partition_stream(&["c", "a", "b"]))
            .await
            .unwrap_err();

        let message = failure.to_string();
        assert!(
            message.contains("scripted finish failure for b.arrow"),
            "{message}"
        );
        assert!(
            message.contains("scripted abort failure for c.arrow"),
            "{message}"
        );
        assert_eq!(failure.report().outputs().len(), 1);
        assert!(failure.report().outputs()[0].durable_locations[0].ends_with("/a.arrow"));
        let state = state.lock().unwrap();
        assert_eq!(state.active, 0);
        assert_eq!(
            state
                .events
                .iter()
                .filter(|event| event.starts_with("finish:") || event.starts_with("abort:"))
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["finish:a.arrow", "finish:b.arrow", "abort:c.arrow"]
        );
    }

    #[tokio::test]
    async fn nosort_evict_finishes_before_replacement_and_numbers_reopened_partitions() {
        let temporary = tempfile::tempdir().unwrap();
        let pattern = temporary.path().join("{{category}}_{{file_number}}.arrow");
        let state = Arc::new(Mutex::new(ScriptedState::default()));
        let writer = scripted_partition_writer(&pattern, Arc::clone(&state));

        let report = writer
            .write_nosort_evict(
                partition_stream(&["a", "b", "a"]),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(report.outputs().len(), 3);
        assert_eq!(
            report
                .outputs()
                .iter()
                .map(|output| output.rows_written)
                .sum::<u64>(),
            3
        );
        let state = state.lock().unwrap();
        assert_eq!(state.active, 0);
        assert_eq!(state.maximum_active, 1);
        assert_eq!(
            state.events,
            [
                "open:a_0.arrow",
                "write:a_0.arrow",
                "finish:a_0.arrow",
                "open:b_0.arrow",
                "write:b_0.arrow",
                "finish:b_0.arrow",
                "open:a_1.arrow",
                "write:a_1.arrow",
                "finish:a_1.arrow",
            ]
        );
    }
}
