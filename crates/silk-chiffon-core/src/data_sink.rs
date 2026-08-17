use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use silk_chiffon_storage::StorageHandle;
use url::Url;

/// Creates per-output sinks from state configured once for a command.
#[async_trait]
pub trait DataSinkFactory: Send + Sync {
    async fn create(&self, handle: StorageHandle, schema: SchemaRef) -> Result<Box<dyn DataSink>>;
}

/// A format-independent destination for Arrow record batches.
#[async_trait]
pub trait DataSink: Send + Sync {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<SinkResult> {
        while let Some(batch) = stream.next().await {
            self.write_batch(batch?).await?;
        }

        self.finish().await
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    async fn finish(&mut self) -> Result<SinkResult>;
}

/// The durable outputs completed by a sink.
#[derive(Debug, Eq, PartialEq)]
pub struct SinkResult {
    pub files_written: Vec<Url>,
    pub rows_written: u64,
}
