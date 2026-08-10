mod partitioner;
mod path_template;
mod report;

use std::{
    collections::{HashMap, hash_map::Entry},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{physical_plan::SendableRecordBatchStream, prelude::SessionContext};
use futures::StreamExt;
use lru::LruCache;
use silk_chiffon_core::{
    DataSink, SinkBinding, SinkBindingConfig, SinkCompletion, TransformBinding, TransformBindings,
};
use silk_chiffon_storage::{LocationInput, StorageHandle, StorageSession};

use crate::{
    PartitionStrategy,
    utils::{filesystem::ensure_parent_dir_exists, projected_stream::project_stream},
};

use partitioner::{
    PartitionValues, Partitioner, partition_key, partition_values_equal,
    validate_partition_columns_primitive,
};
use path_template::PathTemplate;
pub(super) use report::FileOutputReport;
use report::{CompletedFileOutput, partition_field_values};

pub(super) enum FileOutputTarget {
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
pub(super) struct FileOutputRoute<'a> {
    storage: &'a StorageSession,
    formats: &'a TransformBindings,
    explicit_format: Option<&'a str>,
    session: &'a SessionContext,
}

impl<'a> FileOutputRoute<'a> {
    pub(super) fn new(
        storage: &'a StorageSession,
        formats: &'a TransformBindings,
        explicit_format: Option<&'a str>,
        session: &'a SessionContext,
    ) -> Self {
        Self {
            storage,
            formats,
            explicit_format,
            session,
        }
    }

    pub(super) async fn bind(
        &self,
        target: FileOutputTarget,
        sink_config: &SinkBindingConfig,
        output_schema: &SchemaRef,
    ) -> Result<FileOutput> {
        match target {
            FileOutputTarget::Exact {
                target,
                exclude_columns,
                create_dirs,
                overwrite,
            } => {
                validate_excluded_columns(output_schema, &exclude_columns)?;
                let handle = self.resolve_output(&target)?;
                let format = self.format_for_handle(&handle, &target)?;
                let sink_binding =
                    Arc::from(format.bind_sink(sink_config).await.with_context(|| {
                        format!("while binding format for exact file output {target:?}")
                    })?);
                prepare_output(&target, &handle, overwrite, create_dirs).await?;
                self.register_object_store(&handle);
                Ok(FileOutput::Exact {
                    target,
                    handle,
                    sink_binding,
                    exclude_columns,
                })
            }
            FileOutputTarget::Template {
                pattern,
                partition_fields,
                strategy,
                max_open_partitions,
                exclude_columns,
                create_dirs,
                overwrite,
            } => {
                validate_excluded_columns(output_schema, &exclude_columns)?;
                let template = PathTemplate::new(pattern.clone());
                let referenced_fields = template
                    .referenced_fields()
                    .with_context(|| format!("invalid file output template {pattern:?}"))?;
                validate_partition_columns_primitive(output_schema, &partition_fields)?;
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
                let format = self.format_for_path(&pattern)?;
                let sink_binding =
                    Arc::from(format.bind_sink(sink_config).await.with_context(|| {
                        format!("while binding format for partitioned file output {pattern:?}")
                    })?);
                Ok(FileOutput::Partitioned {
                    storage: self.storage.clone(),
                    session: self.session.clone(),
                    sink_binding,
                    partition_fields,
                    template,
                    strategy,
                    max_open,
                    exclude_columns,
                    create_dirs,
                    overwrite,
                })
            }
        }
    }

    fn resolve_output(&self, target: &str) -> Result<StorageHandle> {
        let location = LocationInput::parse(target)
            .with_context(|| format!("while parsing exact file output {target:?}"))?;
        self.storage
            .output_handle(&location)
            .with_context(|| format!("while resolving exact file output {target:?}"))
    }

    fn register_object_store(&self, handle: &StorageHandle) {
        self.session
            .runtime_env()
            .register_object_store(handle.store_url(), handle.object_store());
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
        let extension = Path::new(handle.url().path())
            .extension()
            .and_then(std::ffi::OsStr::to_str);
        self.format_for_extension(extension, target)
    }

    fn format_for_path(&self, path: &str) -> Result<&TransformBinding> {
        if let Some(format) = self.explicit_format {
            return self
                .formats
                .get(format)
                .ok_or_else(|| anyhow!("format is not registered: {format}"));
        }
        let extension = Path::new(path)
            .extension()
            .and_then(std::ffi::OsStr::to_str);
        self.format_for_extension(extension, path)
    }

    fn format_for_extension(
        &self,
        extension: Option<&str>,
        path: &str,
    ) -> Result<&TransformBinding> {
        extension
            .and_then(|extension| self.formats.by_extension(extension))
            .ok_or_else(|| {
                anyhow!(
                    "Could not detect format from path {path:?}. Use \
                     --output-format to specify explicitly."
                )
            })
    }
}

pub(super) enum FileOutput {
    Exact {
        target: String,
        handle: StorageHandle,
        sink_binding: Arc<dyn SinkBinding>,
        exclude_columns: Vec<String>,
    },
    Partitioned {
        storage: StorageSession,
        session: SessionContext,
        sink_binding: Arc<dyn SinkBinding>,
        partition_fields: Vec<String>,
        template: PathTemplate,
        strategy: PartitionStrategy,
        max_open: NonZeroUsize,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
    },
}

impl FileOutput {
    pub(super) async fn write(self, stream: SendableRecordBatchStream) -> Result<FileOutputReport> {
        match self {
            Self::Exact {
                target,
                handle,
                sink_binding,
                exclude_columns,
            } => write_exact(target, handle, sink_binding, stream, exclude_columns).await,
            Self::Partitioned {
                storage,
                session,
                sink_binding,
                partition_fields,
                template,
                strategy,
                max_open,
                exclude_columns,
                create_dirs,
                overwrite,
            } => {
                let state = PartitionedWriteState {
                    storage,
                    session,
                    sink_binding,
                    partition_fields,
                    template,
                    exclude_columns,
                    create_dirs,
                    overwrite,
                };
                match strategy {
                    PartitionStrategy::SortSingle => state.write_sorted(stream).await,
                    PartitionStrategy::NosortMulti => state.write_concurrent(stream).await,
                    PartitionStrategy::NosortEvict => state.write_evicting(stream, max_open).await,
                }
            }
        }
    }
}

async fn write_exact(
    target: String,
    handle: StorageHandle,
    sink_binding: Arc<dyn SinkBinding>,
    stream: SendableRecordBatchStream,
    exclude_columns: Vec<String>,
) -> Result<FileOutputReport> {
    let stream = match projection_indices_excluding(&stream.schema(), &exclude_columns) {
        Some(indices) => project_stream(stream, indices)?,
        None => stream,
    };
    let mut sink = sink_binding
        .open_sink(handle, Arc::clone(&stream.schema()))
        .await
        .with_context(|| format!("while opening exact file output {target:?}"))?;
    sink.write_stream(stream)
        .await
        .with_context(|| format!("while writing exact file output {target:?}"))?;
    Ok(FileOutputReport::new(vec![completed_output(
        &sink
            .finish()
            .await
            .with_context(|| format!("while completing exact file output {target:?}"))?,
        Vec::new(),
    )]))
}

struct OpenSink {
    target: String,
    sink: Box<dyn DataSink>,
    partition_values: PartitionValues,
}

impl OpenSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.sink
            .write_batch(batch)
            .await
            .with_context(|| format!("while writing partitioned file output {:?}", self.target))
    }
}

struct PartitionedWriteState {
    storage: StorageSession,
    session: SessionContext,
    sink_binding: Arc<dyn SinkBinding>,
    partition_fields: Vec<String>,
    template: PathTemplate,
    exclude_columns: Vec<String>,
    create_dirs: bool,
    overwrite: bool,
}

impl PartitionedWriteState {
    async fn write_sorted(&self, stream: SendableRecordBatchStream) -> Result<FileOutputReport> {
        let context = PartitionProjection::new(
            &stream.schema(),
            &self.partition_fields,
            &self.exclude_columns,
        )?;
        let mut partitioned =
            Partitioner::new(self.partition_fields.clone()).partition_stream(stream);
        let mut current: Option<OpenSink> = None;
        let mut completed = Vec::new();

        while let Some(item) = partitioned.next().await {
            let (values, batch) = item?;
            let changed = current
                .as_ref()
                .is_some_and(|open| !partition_values_equal(&open.partition_values, &values));
            if changed {
                completed.push(self.finish(current.take().unwrap()).await?);
            }
            if current.is_none() {
                current = Some(self.open(&values, &context.projected_schema, 0).await?);
            }
            current
                .as_mut()
                .unwrap()
                .write_batch(context.project_batch(batch)?)
                .await?;
        }
        if let Some(open) = current {
            completed.push(self.finish(open).await?);
        }
        Ok(FileOutputReport::new(completed))
    }

    async fn write_concurrent(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<FileOutputReport> {
        let context = PartitionProjection::new(
            &stream.schema(),
            &self.partition_fields,
            &self.exclude_columns,
        )?;
        let mut partitioned =
            Partitioner::new(self.partition_fields.clone()).partition_stream(stream);
        let mut open = HashMap::<String, OpenSink>::new();

        while let Some(item) = partitioned.next().await {
            let (values, batch) = item?;
            let key = partition_key(&values, &context.field_order);
            let sink = match open.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert(self.open(&values, &context.projected_schema, 0).await?)
                }
            };
            sink.write_batch(context.project_batch(batch)?).await?;
        }

        let mut completed = Vec::new();
        for (_, sink) in open {
            completed.push(self.finish(sink).await?);
        }
        Ok(FileOutputReport::new(completed))
    }

    async fn write_evicting(
        &self,
        stream: SendableRecordBatchStream,
        max_open: NonZeroUsize,
    ) -> Result<FileOutputReport> {
        let context = PartitionProjection::new(
            &stream.schema(),
            &self.partition_fields,
            &self.exclude_columns,
        )?;
        let mut partitioned =
            Partitioner::new(self.partition_fields.clone()).partition_stream(stream);
        let mut open = LruCache::<String, OpenSink>::new(max_open);
        let mut file_counts = HashMap::<String, usize>::new();
        let mut completed = Vec::new();

        while let Some(item) = partitioned.next().await {
            let (values, batch) = item?;
            let key = partition_key(&values, &context.field_order);
            if open.get(&key).is_none() {
                let file_index = file_counts.entry(key.clone()).or_insert(0);
                let new_sink = self
                    .open(&values, &context.projected_schema, *file_index)
                    .await?;
                *file_index += 1;
                if let Some((_, evicted)) = open.push(key.clone(), new_sink) {
                    completed.push(self.finish(evicted).await?);
                }
            }
            open.get_mut(&key)
                .expect("the current partition has an open sink")
                .write_batch(context.project_batch(batch)?)
                .await?;
        }

        for (_, sink) in open {
            completed.push(self.finish(sink).await?);
        }
        Ok(FileOutputReport::new(completed))
    }

    async fn open(
        &self,
        values: &PartitionValues,
        schema: &SchemaRef,
        file_index: usize,
    ) -> Result<OpenSink> {
        let target = if file_index == 0 {
            self.template.resolve(values)
        } else {
            self.template.resolve_with_index(values, file_index)
        };
        let location = LocationInput::parse(&target)
            .with_context(|| format!("while parsing partitioned file output {target:?}"))?;
        let handle = self
            .storage
            .output_handle(&location)
            .with_context(|| format!("while resolving partitioned file output {target:?}"))?;
        prepare_output(&target, &handle, self.overwrite, self.create_dirs).await?;
        self.session
            .runtime_env()
            .register_object_store(handle.store_url(), handle.object_store());
        let sink = self
            .sink_binding
            .open_sink(handle, Arc::clone(schema))
            .await
            .with_context(|| format!("while opening partitioned file output {target:?}"))?;
        Ok(OpenSink {
            target,
            sink,
            partition_values: values.clone(),
        })
    }

    async fn finish(&self, open: OpenSink) -> Result<CompletedFileOutput> {
        let OpenSink {
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

async fn prepare_output(
    display_target: &str,
    handle: &StorageHandle,
    overwrite: bool,
    create_dirs: bool,
) -> Result<()> {
    if handle.url().scheme() != "file" {
        return Ok(());
    }
    let path = handle.local_path()?;
    if !overwrite && path.exists() {
        anyhow::bail!(
            "Output file {display_target:?} already exists. Use --overwrite to overwrite."
        );
    }
    if create_dirs {
        ensure_parent_dir_exists(&path)
            .await
            .with_context(|| format!("Failed to create parent directory for {display_target:?}"))?;
    }
    Ok(())
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
