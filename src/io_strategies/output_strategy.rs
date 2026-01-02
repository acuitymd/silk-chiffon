use std::{
    collections::{HashMap, hash_map::Entry},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use arrow::datatypes::SchemaRef;
use datafusion::{execution::SendableRecordBatchStream, prelude::DataFrame};
use futures::StreamExt;

use crate::{
    ListOutputsFormat,
    io_strategies::{
        output_file_info::{OutputFileInfo, partition_values_to_json},
        partitioner::{
            PartitionValues, Partitioner, partition_key, partition_values_equal,
            validate_partition_columns_primitive,
        },
        path_template::PathTemplate,
    },
    sinks::data_sink::DataSink,
    utils::{filesystem::ensure_parent_dir_exists, projected_stream::project_stream},
};

pub type TableName = String;

pub type SinkFactory = Box<dyn Fn(TableName, SchemaRef) -> Result<Box<dyn DataSink>>>;

struct OpenSink {
    sink: Box<dyn DataSink>,
    path: String,
    partition_values: PartitionValues,
}

impl OpenSink {
    fn new(sink: Box<dyn DataSink>, path: String, partition_values: PartitionValues) -> Self {
        Self {
            sink,
            path,
            partition_values,
        }
    }
}

/// Validates output path: checks for existing file if not overwriting, creates parent dirs if needed.
async fn prepare_output_path(path: &str, overwrite: bool, create_dirs: bool) -> Result<()> {
    if !overwrite && Path::new(path).exists() {
        anyhow::bail!(
            "Output file '{}' already exists. Use --overwrite to overwrite.",
            path
        );
    }

    if create_dirs {
        ensure_parent_dir_exists(Path::new(path))
            .await
            .context(format!("Failed to create parent directory for '{}'", path))?;
    }

    Ok(())
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
        create_dirs: bool,
        overwrite: bool,
    },
    /// Partitioned output with a single writer (requires sorted input).
    /// Opens one sink at a time and switches when partition values change.
    PartitionedSingleWriter {
        columns: Vec<String>,
        template: Box<PathTemplate>,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
    },
    /// Partitioned output with multiple writers (unsorted input).
    /// Keeps a sink open for each partition value seen, dispatching batches to the
    /// appropriate sink based on partition column values.
    PartitionedMultiWriter {
        columns: Vec<String>,
        template: Box<PathTemplate>,
        sink_factory: SinkFactory,
        exclude_columns: Vec<String>,
        create_dirs: bool,
        overwrite: bool,
        list_outputs: ListOutputsFormat,
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
                create_dirs,
                overwrite,
            } => {
                prepare_output_path(path, *overwrite, *create_dirs).await?;

                let schema = stream.schema();
                validate_excluded_columns(&schema, exclude_columns)?;

                let stream = match projection_indices_excluding(&schema, exclude_columns) {
                    Some(indices) => project_stream(stream, indices)?,
                    None => stream,
                };

                let mut sink = sink_factory(path.clone(), Arc::clone(&stream.schema()))?;

                let result = sink.write_stream(stream).await?;

                Ok(vec![OutputFileInfo {
                    path: path.clone(),
                    row_count: result.rows_written,
                    partition_values: vec![],
                }])
            }
            OutputStrategy::PartitionedSingleWriter {
                sink_factory,
                columns,
                exclude_columns,
                template,
                create_dirs,
                overwrite,
                list_outputs: _,
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
                        created_files.push(OutputFileInfo {
                            path: prev.path,
                            row_count: result.rows_written,
                            partition_values: partition_values_to_json(
                                &prev.partition_values,
                                &column_order,
                            ),
                        });
                    }

                    if current.is_none() {
                        let path = template.resolve(&partition_values);
                        prepare_output_path(&path, *overwrite, *create_dirs).await?;
                        let sink = sink_factory(path.clone(), Arc::clone(&batch.schema()))?;
                        current = Some(OpenSink::new(sink, path, partition_values.clone()));
                    }

                    current.as_mut().unwrap().sink.write_batch(batch).await?;
                }

                if let Some(mut prev) = current.take() {
                    let result = prev.sink.finish().await?;
                    created_files.push(OutputFileInfo {
                        path: prev.path,
                        row_count: result.rows_written,
                        partition_values: partition_values_to_json(
                            &prev.partition_values,
                            &column_order,
                        ),
                    });
                }

                Ok(created_files)
            }
            OutputStrategy::PartitionedMultiWriter {
                sink_factory,
                columns,
                exclude_columns,
                template,
                create_dirs,
                overwrite,
                list_outputs: _,
            } => {
                let column_order = columns.clone();
                let schema = stream.schema();

                validate_partition_columns_primitive(&schema, columns)?;
                validate_excluded_columns(&schema, exclude_columns)?;

                let projected_column_indices =
                    projection_indices_excluding(&schema, exclude_columns);

                let projected_schema = match &projected_column_indices {
                    Some(indices) => Arc::new(schema.project(indices)?),
                    None => Arc::clone(&schema),
                };

                let partitioner = Partitioner::new(columns.clone());
                let mut partitioned_stream = partitioner.partition_stream(stream);

                let mut open_sinks: HashMap<String, OpenSink> = HashMap::new();

                while let Some(result) = partitioned_stream.next().await {
                    let (partition_values, batch) = result?;
                    let key = partition_key(&partition_values, &column_order);

                    let open_sink = match open_sinks.entry(key) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            let path = template.resolve(&partition_values);
                            prepare_output_path(&path, *overwrite, *create_dirs).await?;
                            let sink = sink_factory(path.clone(), Arc::clone(&projected_schema))?;
                            e.insert(OpenSink::new(sink, path, partition_values.clone()))
                        }
                    };

                    let projected = match projected_column_indices {
                        Some(ref indices) => batch.project(indices)?,
                        None => batch,
                    };

                    open_sink.sink.write_batch(projected).await?;
                }

                // finish all sinks and collect output info
                let mut created_files = Vec::new();
                for (_, mut open_sink) in open_sinks {
                    let result = open_sink.sink.finish().await?;
                    created_files.push(OutputFileInfo {
                        path: open_sink.path,
                        row_count: result.rows_written,
                        partition_values: partition_values_to_json(
                            &open_sink.partition_values,
                            &column_order,
                        ),
                    });
                }

                Ok(created_files)
            }
        }
    }
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
                files_written: vec![self.name.clone().into()],
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
        };

        strategy.write_stream(stream).await.unwrap();

        let batches = batches_written.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
            create_dirs: false,
            overwrite: false,
            list_outputs: ListOutputsFormat::None,
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
                files_written: vec![self.name.clone().into()],
                rows_written: 0,
            })
        }
    }
}
