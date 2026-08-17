use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use silk_chiffon_storage::StorageHandle;
use url::Url;

/// Command-scoped format state that opens one or more output sinks.
///
/// A format binds its parsed CLI settings and shared resources once, after the input plan has
/// been validated. Partitioned output can then open many [`DataSink`] values without rebuilding
/// that state for each file.
#[async_trait]
pub trait SinkBinding: Send + Sync {
    /// Opens a sink for one resolved output and its projected schema.
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>>;
}

/// A format-independent writer for one logical output.
///
/// Writing and completion are separate because a sink may buffer encoded data, upload parts, or
/// write a format footer after its last input batch.
#[async_trait]
pub trait DataSink: Send + Sync {
    /// Writes every batch in a DataFusion stream without completing the sink.
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<()> {
        while let Some(batch) = stream.next().await {
            self.write_batch(batch?).await?;
        }
        Ok(())
    }

    /// Writes one batch without completing the sink.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Completes the output and reports the durable objects it produced.
    async fn finish(self: Box<Self>) -> Result<SinkResult>;
}

/// The durable outputs completed by a sink.
#[derive(Debug, Eq, PartialEq)]
pub struct SinkResult {
    /// Storage URLs for every object completed by the sink.
    pub files_written: Vec<Url>,
    /// Total rows written across the completed objects.
    pub rows_written: u64,
}
