//! Channel-backed TableProvider for streaming DataFusion queries.
//!
//! Provides a TableProvider that reads record batches from a channel,
//! enabling streaming analysis while data is being ingested.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use tokio::sync::mpsc;

/// A TableProvider that reads RecordBatches from an mpsc channel.
///
/// Used for streaming analysis - batches flow through the channel and are
/// analyzed as they arrive. Marked as bounded so aggregations work.
///
/// We specifically can't use the StreamingTableProvider because it's not
/// bounded and so DataFusion refuses to aggregate on it. This is the
/// compromise.
pub struct ChannelTableProvider {
    schema: SchemaRef,
    receiver: std::sync::Mutex<Option<mpsc::Receiver<RecordBatch>>>,
}

impl std::fmt::Debug for ChannelTableProvider {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ChannelTableProvider")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ChannelTableProvider {
    pub fn new(schema: SchemaRef, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        Self {
            schema,
            receiver: std::sync::Mutex::new(Some(receiver)),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for ChannelTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let projected_schema = if let Some(proj) = projection {
            Arc::new(
                self.schema
                    .project(proj)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            )
        } else {
            Arc::clone(&self.schema)
        };

        // take the receiver out of the mutex - scan should only be called once
        let receiver = self.receiver.lock().unwrap().take().ok_or_else(|| {
            DataFusionError::Internal("ChannelTableProvider already scanned".into())
        })?;

        Ok(Arc::new(ChannelExec::new(
            projected_schema,
            receiver,
            projection.cloned(),
        )))
    }
}

/// ExecutionPlan that reads from a channel.
///
/// # Contract
/// This plan should only be executed once. Multiple calls to `execute()` will
/// create competing consumers on the same channel, leading to data being split
/// arbitrarily between streams. This matches DataFusion's general ExecutionPlan
/// pattern where plans are typically consumed once.
struct ChannelExec {
    schema: SchemaRef,
    receiver: std::sync::Mutex<Option<mpsc::Receiver<RecordBatch>>>,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl ChannelExec {
    fn new(
        schema: SchemaRef,
        receiver: mpsc::Receiver<RecordBatch>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            receiver: std::sync::Mutex::new(Some(receiver)),
            projection,
            properties,
        }
    }
}

impl ExecutionPlan for ChannelExec {
    fn name(&self) -> &str {
        "ChannelExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ChannelExec cannot have children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "ChannelExec only supports 1 partition".into(),
            ));
        }

        let schema = Arc::clone(&self.schema);
        let mut receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| DataFusionError::Internal("ChannelExec already consumed".into()))?;
        let projection = self.projection.clone();

        let stream = async_stream::stream! {
            while let Some(mut batch) = receiver.recv().await {
                if let Some(ref proj) = projection {
                    batch = batch.project(proj)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                }
                yield Ok(batch);
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for ChannelExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "ChannelExec")
    }
}

impl fmt::Debug for ChannelExec {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ChannelExec")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn create_batch(schema: &SchemaRef, ids: &[i32], values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_channel_table_provider_basic() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(4);

        let provider = ChannelTableProvider::new(Arc::clone(&schema), rx);

        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        // send batches in background
        let schema_clone = Arc::clone(&schema);
        tokio::spawn(async move {
            tx.send(create_batch(&schema_clone, &[1, 2, 3], &[10, 20, 30]))
                .await
                .unwrap();
            tx.send(create_batch(&schema_clone, &[4, 5], &[40, 50]))
                .await
                .unwrap();
            // drop tx to close channel
        });

        let results = ctx
            .sql("SELECT COUNT(*) as cnt FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let cnt = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_channel_table_approx_distinct() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(4);

        let provider = ChannelTableProvider::new(Arc::clone(&schema), rx);

        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        // send batches with some duplicate ids
        let schema_clone = Arc::clone(&schema);
        tokio::spawn(async move {
            tx.send(create_batch(&schema_clone, &[1, 1, 2], &[10, 20, 30]))
                .await
                .unwrap();
            tx.send(create_batch(&schema_clone, &[2, 3, 3], &[40, 50, 60]))
                .await
                .unwrap();
        });

        let results = ctx
            .sql("SELECT approx_distinct(id) as ndv FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let ndv = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0);
        // HLL is approximate, but with only 3 distinct values it should be exact
        assert_eq!(ndv, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_channel_table_empty() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel::<RecordBatch>(4);

        let provider = ChannelTableProvider::new(Arc::clone(&schema), rx);

        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        // close channel immediately
        drop(tx);

        let results = ctx
            .sql("SELECT COUNT(*) as cnt FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let cnt = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_channel_table_projection() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(4);

        let provider = ChannelTableProvider::new(Arc::clone(&schema), rx);

        let ctx = SessionContext::new();
        ctx.register_table("data", Arc::new(provider)).unwrap();

        let schema_clone = Arc::clone(&schema);
        tokio::spawn(async move {
            tx.send(create_batch(&schema_clone, &[1, 2, 3], &[10, 20, 30]))
                .await
                .unwrap();
        });

        // only select 'value' column - projection should exclude 'id'
        let results = ctx
            .sql("SELECT value FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_columns(), 1);
        assert_eq!(results[0].schema().field(0).name(), "value");
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[10, 20, 30]);
    }

    #[tokio::test]
    async fn test_channel_exec_partition_error() {
        use datafusion::physical_plan::ExecutionPlan;

        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(4);

        let exec = ChannelExec::new(Arc::clone(&schema), rx, None);
        let ctx = Arc::new(TaskContext::default());

        match exec.execute(1, ctx) {
            Err(e) => assert!(e.to_string().contains("only supports 1 partition")),
            Ok(_) => panic!("should fail for partition != 0"),
        }
    }

    #[tokio::test]
    async fn test_channel_exec_with_children_error() {
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(4);

        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(ChannelExec::new(Arc::clone(&schema), rx, None));

        // try to add a child - should fail
        let empty_child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let err = exec.with_new_children(vec![empty_child]).unwrap_err();
        assert!(err.to_string().contains("cannot have children"));
    }
}
