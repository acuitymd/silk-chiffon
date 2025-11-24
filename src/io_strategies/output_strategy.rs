use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use datafusion::{execution::SendableRecordBatchStream, prelude::DataFrame};
use futures::StreamExt;

use crate::{
    io_strategies::{
        partitioner::{PartitionValues, Partitioner, partition_values_equal},
        path_template::PathTemplate,
    },
    sinks::data_sink::DataSink,
};

pub type TableName = String;

pub type SinkFactory = Box<dyn Fn(TableName, SchemaRef) -> Result<Box<dyn DataSink>>>;

pub enum OutputStrategy {
    Single {
        path: TableName,
        sink_factory: SinkFactory,
    },
    Partitioned {
        columns: Vec<String>,
        template: Box<PathTemplate>,
        sink_factory: SinkFactory,
        exclude_partition_columns: bool,
    },
}

impl OutputStrategy {
    pub async fn write(&mut self, df: DataFrame) -> Result<()> {
        self.write_stream(df.execute_stream().await?).await
    }

    pub async fn write_stream(&mut self, stream: SendableRecordBatchStream) -> Result<()> {
        match self {
            OutputStrategy::Single { path, sink_factory } => {
                let schema = stream.schema();
                let mut sink = sink_factory(path.clone(), schema)?;
                sink.write_stream(stream).await?;
                Ok(())
            }
            OutputStrategy::Partitioned {
                sink_factory,
                columns,
                exclude_partition_columns,
                template,
            } => {
                let partitioner = Partitioner::new(columns.clone());
                let schema = stream.schema();
                let non_partition_columns_indices: Vec<usize> = if *exclude_partition_columns {
                    (0..schema.fields().len())
                        .filter(|i| {
                            let field_name = schema.field(*i).name();
                            !columns.contains(field_name)
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let mut partitioned_stream = partitioner.partition_stream(stream);
                let mut current_sink: Option<Box<dyn DataSink>> = None;
                let mut most_recent_partition_values: Option<PartitionValues> = None;
                while let Some(result) = partitioned_stream.next().await {
                    let (partition_values, mut batch) = result?;
                    if *exclude_partition_columns {
                        batch = batch.project(&non_partition_columns_indices)?;
                    }

                    let partition_changed = most_recent_partition_values
                        .as_ref()
                        .map(|prev| !partition_values_equal(prev, &partition_values))
                        .unwrap_or(false);

                    if current_sink.is_some() && partition_changed {
                        current_sink.as_mut().unwrap().finish().await?;
                        current_sink = None;
                    }

                    if current_sink.is_none() {
                        current_sink = Some(sink_factory(
                            template.resolve(&partition_values),
                            Arc::clone(&batch.schema()),
                        )?);
                    }
                    current_sink.as_mut().unwrap().write_batch(batch).await?;
                    most_recent_partition_values = Some(partition_values);
                }

                if current_sink.is_some() {
                    current_sink.as_mut().unwrap().finish().await?;
                }

                Ok(())
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
            self.batches.lock().unwrap().push(batch);
            Ok(())
        }

        async fn finish(&mut self) -> Result<SinkResult> {
            *self.finished.lock().unwrap() = true;
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

        let mut strategy = OutputStrategy::Partitioned {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_partition_columns: true,
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

        let mut strategy = OutputStrategy::Partitioned {
            columns: vec!["region".to_string()],
            template: Box::new(PathTemplate::new("output/{{region}}.parquet".to_string())),
            sink_factory,
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            columns: vec!["category".to_string()],
            template: Box::new(PathTemplate::new("output/{{category}}.parquet".to_string())),
            sink_factory,
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            sink_factory: Box::new(sink_factory),
            columns: vec!["region".to_string()],
            template: Box::new(PathTemplate::new("output/{{region}}.parquet".to_string())),
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            sink_factory: Box::new(sink_factory),
            columns: vec!["nonexistent_column".to_string()],
            template: Box::new(PathTemplate::new(
                "output/{{nonexistent_column}}.parquet".to_string(),
            )),
            exclude_partition_columns: false,
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

        let mut strategy = OutputStrategy::Partitioned {
            sink_factory: Box::new(sink_factory),
            columns: vec!["region".to_string()],
            template: Box::new(PathTemplate::new("output/{{region}}.parquet".to_string())),
            exclude_partition_columns: false,
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
}
