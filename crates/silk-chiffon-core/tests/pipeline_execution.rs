use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{catalog::TableProvider, datasource::MemTable};
use futures::TryStreamExt;
use silk_chiffon_core::{DataSource, InputSources, Pipeline, Replayability};

struct TestSource {
    schema: SchemaRef,
}

#[async_trait]
impl DataSource for TestSource {
    fn name(&self) -> &str {
        "pipeline-test"
    }

    fn replayability(&self) -> Replayability {
        Replayability::Replayable
    }

    async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        Ok(Arc::new(MemTable::try_new(
            Arc::clone(&self.schema),
            vec![vec![batch]],
        )?))
    }
}

#[test]
fn pipeline_execution_boxes_the_complete_execution_lifetime() {
    futures::executor::block_on(async {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let inputs = InputSources::new(Box::new(TestSource {
            schema: Arc::clone(&schema),
        }));
        let mut pipeline = Pipeline::new().with_inputs(inputs);
        let session = pipeline.create_session_context().unwrap();
        let prepared = pipeline.prepare(session).await.unwrap();

        assert_eq!(prepared.output_schema(), schema);
        let batches = prepared
            .begin_execution()
            .unwrap()
            .into_sendable_stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    });
}
