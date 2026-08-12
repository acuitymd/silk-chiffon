use std::sync::Arc;

use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{catalog::TableProvider, datasource::MemTable};
use futures::TryStreamExt;
use silk_chiffon_core::{InputSources, Pipeline};

#[test]
fn pipeline_execution_boxes_the_complete_execution_lifetime() {
    futures::executor::block_on(async {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let provider = Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).unwrap())
            as Arc<dyn TableProvider>;
        let mut pipeline = Pipeline::new();
        let session = pipeline.create_session_context().unwrap();
        let inputs = InputSources::try_new(&session, vec![provider]).unwrap();
        let prepared = pipeline.with_inputs(inputs).prepare(session).await.unwrap();

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
