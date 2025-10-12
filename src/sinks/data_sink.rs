use std::path::PathBuf;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

#[async_trait]
pub trait DataSink {
    async fn write_stream(&mut self, stream: SendableRecordBatchStream) -> Result<SinkResult>;
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finish(&mut self) -> Result<SinkResult>;
}

pub struct SinkResult {
    pub files_written: Vec<PathBuf>,
    pub rows_written: u64,
}
