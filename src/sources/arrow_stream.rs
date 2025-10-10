use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, datatypes::SchemaRef, ipc::reader::StreamReader};
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;
use std::{
    fs::File,
    io::BufReader,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::sources::data_source::DataSource;

pub struct ArrowStreamDataSource {
    path: String,
}

impl ArrowStreamDataSource {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

struct ArrowStreamSendableBatchReader {
    reader: StreamReader<BufReader<File>>,
}

impl ArrowStreamSendableBatchReader {
    pub fn new(path: String) -> Result<Self> {
        let file = File::open(&path)?;
        let reader = StreamReader::try_new_buffered(file, None)?;
        Ok(Self { reader })
    }
}

impl Stream for ArrowStreamSendableBatchReader {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.reader.next() {
            Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
            Some(Err(e)) => Poll::Ready(Some(Err(DataFusionError::Execution(e.to_string())))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ArrowStreamSendableBatchReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

#[async_trait]
impl DataSource for ArrowStreamDataSource {
    fn name(&self) -> &str {
        "arrow_stream"
    }

    async fn as_table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        Err(anyhow!(
            "ArrowStreamDataSource.as_table_provider is not implemented"
        ))
    }

    async fn as_stream(&self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(ArrowStreamSendableBatchReader::new(
            self.path.clone(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    const TEST_ARROW_STREAM_PATH: &str = "tests/files/people.stream.arrow";

    #[test]
    fn test_new() {
        let source = ArrowStreamDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        assert_eq!(source.path, TEST_ARROW_STREAM_PATH);
    }

    #[test]
    fn test_name() {
        let source = ArrowStreamDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        assert_eq!(source.name(), "arrow_stream");
    }

    #[tokio::test]
    async fn test_as_table_provider() {
        let source = ArrowStreamDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        let table_provider = source.as_table_provider().await;
        assert!(matches!(table_provider, Err(_)));
    }

    #[tokio::test]
    async fn test_as_stream() {
        let source = ArrowStreamDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        let mut stream = source.as_stream().await.unwrap();

        assert!(stream.schema().fields().len() > 0);
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }
}
