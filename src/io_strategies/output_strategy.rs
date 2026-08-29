//! Single and partitioned output orchestration.
//!
//! Strategies render provider-independent destination strings and delegate
//! object resolution to `SinkFactory`. Partition templates validate that
//! substitutions retain their original store identity. Results use the path
//! returned by the committed sink, and are sorted lexically before returning.
//! Dropping the open-sink collections on any stream or sink error starts the
//! concrete sinks' cancellation cleanup.

use std::{
    collections::{HashMap, hash_map::Entry},
    num::NonZeroUsize,
    sync::Arc,
};

use lru::LruCache;

use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::{execution::SendableRecordBatchStream, prelude::DataFrame};
use futures::StreamExt;

use crate::{
    io_strategies::{
        output_file_info::{OutputFileInfo, partition_values_to_json},
        partitioner::{
            PartitionValues, Partitioner, partition_key, partition_values_equal,
            validate_partition_columns_primitive,
        },
        path_template::PathTemplate,
    },
    sinks::data_sink::DataSink,
    utils::projected_stream::project_stream,
};

pub type TableName = String;

pub type SinkFactory = Box<dyn Fn(TableName, SchemaRef) -> Result<Box<dyn DataSink>>>;

struct OpenSink {
    sink: Box<dyn DataSink>,
    partition_values: PartitionValues,
}

impl OpenSink {
    fn new(sink: Box<dyn DataSink>, partition_values: PartitionValues) -> Self {
        Self {
            sink,
            partition_values,
        }
    }
}

/// Validates that all excluded columns exist in the schema.
fn validate_excluded_columns(schema: &SchemaRef, exclude_columns: &[String]) -> Result<()> {
    for col_name in exclude_columns {
        schema
            .column_with_name(col_name)
            .ok_or_else(|| anyhow!("Column '{col_name}' not found in schema"))?;
    }
    Ok(())
}

/// Returns column indices to keep after excluding the specified columns.
/// Returns None if no columns are excluded.
fn projection_indices_excluding(
    schema: &SchemaRef,
    exclude_columns: &[String],
) -> Option<Vec<usize>> {
    if exclude_columns.is_empty() {
        return None;
    }
    Some(
        (0..schema.fields().len())
            .filter(|i| !exclude_columns.contains(schema.field(*i).name()))
            .collect(),
    )
}

pub enum OutputStrategy {
    Single {
        path: TableName,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
    },
    /// Partitioned output with a single writer (requires sorted input).
    /// Opens one sink at a time and switches when partition values change.
    PartitionedSingleWriter {
        columns: Vec<String>,
        template: Box<PathTemplate>,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
    },
    /// Partitioned output with multiple writers (unsorted input).
    /// Keeps a sink open for each partition value seen, dispatching batches to the
    /// appropriate sink based on partition column values.
    PartitionedMultiWriter {
        columns: Vec<String>,
        template: Box<PathTemplate>,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
    },
    /// Like PartitionedMultiWriter but caps open writers with LRU eviction.
    /// Evicted partitions get new numbered files if they reappear.
    PartitionedEvictWriter {
        columns: Vec<String>,
        template: Box<PathTemplate>,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
        max_open: NonZeroUsize,
    },
}

impl OutputStrategy {
    /// May only be called once. Each subsequent call will overwrite the previous output.
    pub async fn write(&mut self, df: DataFrame) -> Result<Vec<OutputFileInfo>> {
        self.write_stream(df.execute_stream().await?).await
    }

    /// May only be called once. Each subsequent call will overwrite the previous output.
    pub async fn write_stream(
        &mut self,
        stream: SendableRecordBatchStream,
    ) -> Result<Vec<OutputFileInfo>> {
        match self {
            OutputStrategy::Single {
                path,
                sink_factory,
                exclude_columns,
            } => {
                let schema = stream.schema();
                validate_excluded_columns(&schema, exclude_columns)?;

                let stream = match projection_indices_excluding(&schema, exclude_columns) {
                    Some(indices) => project_stream(stream, indices)?,
                    None => stream,
                };

                let mut sink = sink_factory(path.clone(), Arc::clone(&stream.schema()))?;

                let result = sink.write_stream(stream).await?;

                Ok(vec![output_file_info(result, vec![])?])
            }
            OutputStrategy::PartitionedSingleWriter {
                sink_factory,
                columns,
                exclude_columns,
                template,
            } => {
                let partitioner = Partitioner::new(columns.clone());
                let column_order = columns.clone();
                let schema = stream.schema();

                validate_partition_columns_primitive(&schema, columns)?;
                validate_excluded_columns(&schema, exclude_columns)?;

                let projected_column_indices =
                    projection_indices_excluding(&schema, exclude_columns);
                let mut partitioned_stream = partitioner.partition_stream(stream);
                let mut current: Option<OpenSink> = None;
                let mut created_files: Vec<OutputFileInfo> = Vec::new();

                while let Some(result) = partitioned_stream.next().await {
                    let (partition_values, mut batch) = result?;
                    if let Some(ref indices) = projected_column_indices {
                        batch = batch.project(indices)?;
                    }

                    let partition_changed = current
                        .as_ref()
                        .map(|c| !partition_values_equal(&c.partition_values, &partition_values))
                        .unwrap_or(false);

                    if partition_changed {
                        let mut prev = current.take().unwrap();
                        let result = prev.sink.finish().await?;
                        created_files.push(output_file_info(
                            result,
                            partition_values_to_json(&prev.partition_values, &column_order),
                        )?);
                    }

                    if current.is_none() {
                        let path = template.resolve_checked(&partition_values)?;
                        let sink = sink_factory(path.clone(), Arc::clone(&batch.schema()))?;
                        current = Some(OpenSink::new(sink, partition_values.clone()));
                    }

                    current.as_mut().unwrap().sink.write_batch(batch).await?;
                }

                if let Some(mut prev) = current.take() {
                    let result = prev.sink.finish().await?;
                    created_files.push(output_file_info(
                        result,
                        partition_values_to_json(&prev.partition_values, &column_order),
                    )?);
                }

                sort_output_files(&mut created_files);
                Ok(created_files)
            }
            OutputStrategy::PartitionedMultiWriter {
                sink_factory,
                columns,
                exclude_columns,
                template,
            } => {
                let ctx = PartitionedWriteContext::new(&stream.schema(), columns, exclude_columns)?;
                let partitioner = Partitioner::new(columns.clone());
                let mut partitioned_stream = partitioner.partition_stream(stream);

                let mut open_sinks: HashMap<String, OpenSink> = HashMap::new();

                while let Some(result) = partitioned_stream.next().await {
                    let (partition_values, batch) = result?;
                    let key = partition_key(&partition_values, &ctx.column_order);

                    let open_sink = match open_sinks.entry(key) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            let path = template.resolve_checked(&partition_values)?;
                            let sink =
                                sink_factory(path.clone(), Arc::clone(&ctx.projected_schema))?;
                            e.insert(OpenSink::new(sink, partition_values.clone()))
                        }
                    };

                    let projected = ctx.project_batch(batch)?;
                    open_sink.sink.write_batch(projected).await?;
                }

                drain_open_sinks(open_sinks, &ctx.column_order).await
            }
            OutputStrategy::PartitionedEvictWriter {
                sink_factory,
                columns,
                exclude_columns,
                template,
                max_open,
            } => {
                let ctx = PartitionedWriteContext::new(&stream.schema(), columns, exclude_columns)?;
                let partitioner = Partitioner::new(columns.clone());
                let mut partitioned_stream = partitioner.partition_stream(stream);

                let mut cache: LruCache<String, OpenSink> = LruCache::new(*max_open);
                let mut file_counts: HashMap<String, usize> = HashMap::new();
                let mut created_files: Vec<OutputFileInfo> = Vec::new();

                while let Some(result) = partitioned_stream.next().await {
                    let (partition_values, batch) = result?;
                    let key = partition_key(&partition_values, &ctx.column_order);

                    if cache.get(&key).is_none() {
                        let file_idx = file_counts.entry(key.clone()).or_insert(0);
                        let path = if *file_idx == 0 {
                            template.resolve_checked(&partition_values)?
                        } else {
                            template.resolve_with_index_checked(&partition_values, *file_idx)?
                        };
                        *file_idx += 1;

                        let sink = sink_factory(path.clone(), Arc::clone(&ctx.projected_schema))?;
                        let new_sink = OpenSink::new(sink, partition_values.clone());

                        if let Some((_, mut evicted)) = cache.push(key.clone(), new_sink) {
                            let result = evicted.sink.finish().await?;
                            created_files.push(output_file_info(
                                result,
                                partition_values_to_json(
                                    &evicted.partition_values,
                                    &ctx.column_order,
                                ),
                            )?);
                        }
                    }

                    let open_sink = cache.get_mut(&key).unwrap();
                    let projected = ctx.project_batch(batch)?;
                    open_sink.sink.write_batch(projected).await?;
                }

                let mut remaining = drain_open_sinks(cache, &ctx.column_order).await?;
                created_files.append(&mut remaining);
                sort_output_files(&mut created_files);
                Ok(created_files)
            }
        }
    }
}

/// Shared setup for partitioned write strategies.
struct PartitionedWriteContext {
    column_order: Vec<String>,
    projected_column_indices: Option<Vec<usize>>,
    projected_schema: SchemaRef,
}

impl PartitionedWriteContext {
    fn new(schema: &SchemaRef, columns: &[String], exclude_columns: &[String]) -> Result<Self> {
        validate_partition_columns_primitive(schema, columns)?;
        validate_excluded_columns(schema, exclude_columns)?;

        let projected_column_indices = projection_indices_excluding(schema, exclude_columns);
        let projected_schema = match &projected_column_indices {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => Arc::clone(schema),
        };

        Ok(Self {
            column_order: columns.to_vec(),
            projected_column_indices,
            projected_schema,
        })
    }

    fn project_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match self.projected_column_indices {
            Some(ref indices) => Ok(batch.project(indices)?),
            None => Ok(batch),
        }
    }
}

async fn drain_open_sinks(
    sinks: impl IntoIterator<Item = (String, OpenSink)>,
    column_order: &[String],
) -> Result<Vec<OutputFileInfo>> {
    let mut created_files = Vec::new();
    for (_, mut open_sink) in sinks {
        let result = open_sink.sink.finish().await?;
        created_files.push(output_file_info(
            result,
            partition_values_to_json(&open_sink.partition_values, column_order),
        )?);
    }
    sort_output_files(&mut created_files);
    Ok(created_files)
}

fn output_file_info(
    result: crate::sinks::data_sink::SinkResult,
    partition_values: Vec<crate::io_strategies::output_file_info::PartitionColumnValue>,
) -> Result<OutputFileInfo> {
    let file_count = result.files_written.len();
    if file_count != 1 {
        return Err(anyhow!(
            "an output sink must commit exactly one output, but committed {}",
            file_count
        ));
    }
    let path = result
        .files_written
        .into_iter()
        .next()
        .expect("one sink output was checked above");
    Ok(OutputFileInfo {
        path,
        row_count: result.rows_written,
        partition_values,
    })
}

fn sort_output_files(files: &mut [OutputFileInfo]) {
    files.sort_by(|left, right| left.path.cmp(&right.path));
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        error::DataFusionError,
        physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    };
    use futures::stream;

    use crate::sinks::data_sink::{DataSink, SinkResult};

    use super::*;

    // mock sink for testing
    struct MockSink {
        name: String,
        batches: Arc<Mutex<Vec<RecordBatch>>>,
        finished: Arc<Mutex<bool>>,
    }

    struct DropTrackingSink {
        fail_writes: bool,
        drops: Arc<Mutex<usize>>,
    }

    impl Drop for DropTrackingSink {
        fn drop(&mut self) {
            *self.drops.lock().unwrap() += 1;
        }
    }

    #[async_trait::async_trait]
    impl DataSink for DropTrackingSink {
        async fn write_batch(&mut self, _batch: RecordBatch) -> Result<()> {
            if self.fail_writes {
                anyhow::bail!("injected partition sink failure");
            }
            Ok(())
        }

        async fn finish(&mut self) -> Result<SinkResult> {
            unreachable!("the failing write stops before sinks are finished")
        }
    }

    #[async_trait::async_trait]
    impl DataSink for MockSink {
        async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
            self.batches
                .lock()
                .map_err(|e| anyhow!("Failed to lock batches: {}", e))?
                .push(batch);
            Ok(())
        }

        async fn finish(&mut self) -> Result<SinkResult> {
            *self
                .finished
                .lock()
                .map_err(|e| anyhow!("Failed to lock finished: {}", e))? = true;
            Ok(SinkResult {
                files_written: vec![self.name.clone()],
                rows_written: 0,
            })
        }
    }

    fn create_test_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    #[tokio::test]
    async fn test_partitioned_single_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(Mutex::new(false));
        let batches_clone = Arc::clone(&batches_written);
        let finished_clone = Arc::clone(&finished);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::clone(&finished_clone),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn reports_the_path_committed_by_the_sink() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let stream = create_test_stream(vec![batch]);
        let sink_factory: SinkFactory = Box::new(|_, _| {
            Ok(Box::new(MockSink {
                name: "gs://bucket/committed.arrow".to_string(),
                batches: Arc::new(Mutex::new(Vec::new())),
                finished: Arc::new(Mutex::new(false)),
            }))
        });
        let mut strategy = OutputStrategy::Single {
            path: "gs://bucket/requested.arrow".to_string(),
            sink_factory,
            exclude_columns: vec![],
        };

        let files = strategy.write_stream(stream).await.unwrap();

        assert_eq!(files[0].path, "gs://bucket/committed.arrow");
    }

    #[tokio::test]
    async fn partition_failure_drops_every_open_sink() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "category",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let drops = Arc::new(Mutex::new(0));
        let created = Arc::new(Mutex::new(0));
        let sink_factory: SinkFactory = Box::new({
            let drops = Arc::clone(&drops);
            move |_, _| {
                let mut created = created.lock().unwrap();
                *created += 1;
                Ok(Box::new(DropTrackingSink {
                    fail_writes: *created == 2,
                    drops: Arc::clone(&drops),
                }))
            }
        });
        let mut strategy = OutputStrategy::PartitionedMultiWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.arrow".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        let error = strategy
            .write_stream(create_test_stream(vec![batch]))
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected partition sink failure")
        );
        assert_eq!(*drops.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_partitioned_multiple_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches_written);

        let sink_factory: SinkFactory = Box::new(move |_name, _schema| {
            Ok(Box::new(MockSink {
                name: _name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 2); // category 1
        assert_eq!(batches[1].num_rows(), 2); // category 2
        assert_eq!(batches[2].num_rows(), 2); // category 3
    }

    #[tokio::test]
    async fn test_partitioned_exclude_partition_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches_written);

        let sink_factory: SinkFactory = Box::new(move |_name, _schema| {
            Ok(Box::new(MockSink {
                name: _name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec!["category".to_string()],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        assert_eq!(batches.len(), 2);
        // partition column should be excluded
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "value");
    }

    #[tokio::test]
    async fn test_partitioned_with_string_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["us-west", "us-west", "us-east"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches_written);

        let sink_factory: SinkFactory = Box::new(move |_name, _schema| {
            Ok(Box::new(MockSink {
                name: _name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            columns: vec!["region".to_string()],
            template: Box::new(PathTemplate::new("output/{{region}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2); // us-west
        assert_eq!(batches[1].num_rows(), 1); // us-east
    }

    #[tokio::test]
    async fn test_partitioned_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch1, batch2]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches_written);

        let sink_factory: SinkFactory = Box::new(move |_name, _schema| {
            Ok(Box::new(MockSink {
                name: _name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        // should have 3 writes: 2 for category 1, 1 for category 2
        assert_eq!(batches.len(), 3);
    }

    #[tokio::test]
    async fn test_partitioned_finish_called_on_partition_change() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        // track which sinks were finished
        let sink1_finished = Arc::new(Mutex::new(false));
        let sink2_finished = Arc::new(Mutex::new(false));
        let sink3_finished = Arc::new(Mutex::new(false));

        let sink1_clone = Arc::clone(&sink1_finished);
        let sink2_clone = Arc::clone(&sink2_finished);
        let sink3_clone = Arc::clone(&sink3_finished);

        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            let mut count = call_count_clone.lock().unwrap();
            *count += 1;
            let finished_flag = match *count {
                1 => Arc::clone(&sink1_clone),
                2 => Arc::clone(&sink2_clone),
                3 => Arc::clone(&sink3_clone),
                _ => panic!("unexpected sink creation"),
            };

            Ok(Box::new(MockSink {
                name,
                batches: Arc::new(Mutex::new(Vec::new())),
                finished: finished_flag,
            }))
        });

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        // verify all sinks were finished
        assert!(
            *sink1_finished.lock().unwrap(),
            "sink 1 should be finished when partition changes"
        );
        assert!(
            *sink2_finished.lock().unwrap(),
            "sink 2 should be finished when partition changes"
        );
        assert!(
            *sink3_finished.lock().unwrap(),
            "sink 3 should be finished at end of stream"
        );
    }

    #[tokio::test]
    async fn test_partitioned_error_propagation() {
        // test that errors from the stream are properly propagated
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // create stream that returns an error
        let error_stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![
                Ok(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(vec!["us-west"])),
                        Arc::new(Int32Array::from(vec![1])),
                    ],
                )
                .unwrap()),
                Err(DataFusionError::Execution(
                    "simulated stream error".to_string(),
                )),
            ]),
        ));

        let batches = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(Mutex::new(false));

        let batches_clone = Arc::clone(&batches);
        let finished_clone = Arc::clone(&finished);

        let sink_factory = move |name: String, _schema: Arc<Schema>| {
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::clone(&finished_clone),
            }) as Box<dyn DataSink>)
        };

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            sink_factory: Box::new(sink_factory),
            columns: vec!["region".to_string()],
            template: Box::new(PathTemplate::new("output/{{region}}.parquet".to_string())),
            exclude_columns: vec![],
        };

        // should propagate the error
        let result = strategy.write_stream(error_stream).await;
        assert!(result.is_err(), "error should be propagated");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("simulated stream error"),
            "error message should be preserved"
        );
    }

    #[tokio::test]
    async fn test_partitioned_missing_column_error() {
        // test that partitioning on a non-existent column produces an error
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["us-west"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(Mutex::new(false));

        let batches_clone = Arc::clone(&batches);
        let finished_clone = Arc::clone(&finished);

        let sink_factory = move |name: String, _schema: Arc<Schema>| {
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::clone(&finished_clone),
            }) as Box<dyn DataSink>)
        };

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            sink_factory: Box::new(sink_factory),
            columns: vec!["nonexistent_column".to_string()],
            template: Box::new(PathTemplate::new(
                "output/{{nonexistent_column}}.parquet".to_string(),
            )),
            exclude_columns: vec![],
        };

        // should propagate the error about missing column
        let result = strategy.write_stream(stream).await;
        assert!(result.is_err(), "missing column error should be propagated");
    }

    #[tokio::test]
    async fn test_partitioned_same_partition_values_reuse_sink() {
        // test that consecutive batches with identical partition values reuse the same sink
        // even though they have different ArrayRef pointers
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // create two batches with the same partition value ("us-west")
        // but different ArrayRef instances (simulating what happens in real partitioning)
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["us-west"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["us-west"])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch1, batch2]);

        let batches = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(Mutex::new(false));
        let sink_create_count = Arc::new(Mutex::new(0));

        let batches_clone = Arc::clone(&batches);
        let finished_clone = Arc::clone(&finished);
        let sink_create_count_clone = Arc::clone(&sink_create_count);

        let sink_factory = move |name: String, _schema: Arc<Schema>| {
            *sink_create_count_clone.lock().unwrap() += 1;
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::clone(&finished_clone),
            }) as Box<dyn DataSink>)
        };

        let mut strategy = OutputStrategy::PartitionedSingleWriter {
            sink_factory: Box::new(sink_factory),
            columns: vec!["region".to_string()],
            template: Box::new(PathTemplate::new("output/{{region}}.parquet".to_string())),
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        // verify that the sink was only created once (not twice)
        assert_eq!(
            *sink_create_count.lock().unwrap(),
            1,
            "sink should be created only once for same partition values"
        );

        // verify both batches were written to the same sink
        assert_eq!(
            batches.lock().unwrap().len(),
            2,
            "both batches should be written"
        );

        // verify the sink was finished
        assert!(
            *finished.lock().unwrap(),
            "sink should be finished at end of stream"
        );
    }

    #[tokio::test]
    async fn test_low_cardinality_single_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(Mutex::new(false));
        let batches_clone = Arc::clone(&batches_written);
        let finished_clone = Arc::clone(&finished);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::clone(&finished_clone),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedMultiWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert!(*finished.lock().unwrap(), "sink should be finished");
    }

    #[tokio::test]
    async fn test_low_cardinality_interleaved_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "c", "a"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let finished_count = Arc::new(Mutex::new(0usize));

        let batches_clone = Arc::clone(&batches_written);
        let finished_count_clone = Arc::clone(&finished_count);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            let batches = Arc::clone(&batches_clone);
            let finished_count = Arc::clone(&finished_count_clone);

            Ok(Box::new(MockSinkWithCallback {
                name,
                batches,
                on_finish: Box::new(move || {
                    *finished_count.lock().unwrap() += 1;
                }),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedMultiWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        let total_rows: usize = batches_written
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 6, "all 6 rows should be written");

        assert_eq!(
            *finished_count.lock().unwrap(),
            3,
            "all 3 sinks should be finished"
        );
    }

    #[tokio::test]
    async fn test_low_cardinality_exclude_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1, 2])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches_written);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedMultiWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec!["category".to_string()],
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        for batch in batches.iter() {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "value");
        }
    }

    #[tokio::test]
    async fn test_low_cardinality_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["x", "y", "x"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["y", "x", "y"])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch1, batch2]);

        let sink_create_count = Arc::new(Mutex::new(0usize));
        let sink_create_count_clone = Arc::clone(&sink_create_count);

        let batches_written = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches_written);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            *sink_create_count_clone.lock().unwrap() += 1;
            Ok(Box::new(MockSink {
                name,
                batches: Arc::clone(&batches_clone),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedMultiWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
        };

        strategy.write_stream(stream).await.unwrap();

        assert_eq!(
            *sink_create_count.lock().unwrap(),
            2,
            "should create exactly 2 sinks (x and y)"
        );

        let total_rows: usize = batches_written
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 6, "all 6 rows should be written");
    }

    struct MockSinkWithCallback {
        name: String,
        batches: Arc<Mutex<Vec<RecordBatch>>>,
        on_finish: Box<dyn Fn() + Send + Sync>,
    }

    #[async_trait::async_trait]
    impl DataSink for MockSinkWithCallback {
        async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
            self.batches
                .lock()
                .map_err(|e| anyhow!("Failed to lock batches: {}", e))?
                .push(batch);
            Ok(())
        }

        async fn finish(&mut self) -> Result<SinkResult> {
            (self.on_finish)();
            Ok(SinkResult {
                files_written: vec![self.name.clone()],
                rows_written: 0,
            })
        }
    }

    #[tokio::test]
    async fn test_evict_under_cap() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let sink_create_count = Arc::new(Mutex::new(0usize));
        let sink_create_count_clone = Arc::clone(&sink_create_count);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            *sink_create_count_clone.lock().unwrap() += 1;
            Ok(Box::new(MockSink {
                name,
                batches: Arc::new(Mutex::new(Vec::new())),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedEvictWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
            max_open: NonZeroUsize::new(10).unwrap(),
        };

        let files = strategy.write_stream(stream).await.unwrap();
        assert_eq!(*sink_create_count.lock().unwrap(), 3);
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_evict_at_cap() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // a, b, c -> when c arrives with max_open=2, one of a/b gets evicted
        // then a again -> reopened with _1 suffix
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "a", "b"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let created_paths = Arc::new(Mutex::new(Vec::new()));
        let created_paths_clone = Arc::clone(&created_paths);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            created_paths_clone.lock().unwrap().push(name.clone());
            Ok(Box::new(MockSink {
                name,
                batches: Arc::new(Mutex::new(Vec::new())),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedEvictWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
            max_open: NonZeroUsize::new(2).unwrap(),
        };

        let files = strategy.write_stream(stream).await.unwrap();
        let paths = created_paths.lock().unwrap();

        // a, b, c (evicts a), a_1 (evicts b), b_1 (evicts c)
        assert!(paths.contains(&"output/a.parquet".to_string()));
        assert!(paths.contains(&"output/b.parquet".to_string()));
        assert!(paths.contains(&"output/c.parquet".to_string()));
        assert!(paths.contains(&"output/a_1.parquet".to_string()));
        assert!(paths.contains(&"output/b_1.parquet".to_string()));
        assert_eq!(paths.len(), 5);
        assert_eq!(files.len(), 5);
    }

    #[tokio::test]
    async fn test_evict_file_numbering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // with max_open=1, every new partition evicts the previous
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let created_paths = Arc::new(Mutex::new(Vec::new()));
        let created_paths_clone = Arc::clone(&created_paths);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            created_paths_clone.lock().unwrap().push(name.clone());
            Ok(Box::new(MockSink {
                name,
                batches: Arc::new(Mutex::new(Vec::new())),
                finished: Arc::new(Mutex::new(false)),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedEvictWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
            max_open: NonZeroUsize::new(1).unwrap(),
        };

        strategy.write_stream(stream).await.unwrap();
        let paths = created_paths.lock().unwrap();

        assert!(paths.contains(&"output/a.parquet".to_string()));
        assert!(paths.contains(&"output/b.parquet".to_string()));
        assert!(paths.contains(&"output/a_1.parquet".to_string()));
        assert!(paths.contains(&"output/b_1.parquet".to_string()));
        assert!(paths.contains(&"output/a_2.parquet".to_string()));
    }

    #[tokio::test]
    async fn test_evict_lru_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // write a, b, a, c -> with max_open=2, b should be evicted (not a)
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "c"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let stream = create_test_stream(vec![batch]);
        let finished_names = Arc::new(Mutex::new(Vec::new()));
        let finished_names_clone = Arc::clone(&finished_names);

        let sink_factory: SinkFactory = Box::new(move |name, _schema| {
            let finished_names = Arc::clone(&finished_names_clone);
            let name_for_finish = name.clone();
            Ok(Box::new(MockSinkWithCallback {
                name,
                batches: Arc::new(Mutex::new(Vec::new())),
                on_finish: Box::new(move || {
                    finished_names.lock().unwrap().push(name_for_finish.clone());
                }),
            }))
        });

        let mut strategy = OutputStrategy::PartitionedEvictWriter {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_columns: vec![],
            max_open: NonZeroUsize::new(2).unwrap(),
        };

        strategy.write_stream(stream).await.unwrap();
        let names = finished_names.lock().unwrap();

        // b should be evicted first (LRU), then a and c finished at end
        assert_eq!(
            names[0], "output/b.parquet",
            "b should be evicted first (LRU)"
        );
    }
}
