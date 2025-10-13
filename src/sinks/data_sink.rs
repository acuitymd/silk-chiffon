use std::path::PathBuf;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;

#[async_trait]
pub trait DataSink {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<SinkResult> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.write_batch(batch).await?;
        }

        self.finish().await
    }
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finish(&mut self) -> Result<SinkResult>;
}

#[derive(Debug)]
pub struct SinkResult {
    pub files_written: Vec<PathBuf>,
    pub rows_written: u64,
}
