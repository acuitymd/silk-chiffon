use std::{
    fs::File,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef, ipc::reader::StreamReader};
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;
use tokio::sync::mpsc;

use crate::sources::data_source::DataSource;

#[derive(Debug)]
pub struct ArrowStreamDataSource {
    path: String,
}

impl ArrowStreamDataSource {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    fn get_schema(&self) -> Result<SchemaRef> {
        let file = File::open(&self.path)?;
        let reader = StreamReader::try_new_buffered(file, None)?;
        let schema = reader.schema();
        Ok(schema)
    }
}

struct ArrowStreamChannelStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
}

impl ArrowStreamChannelStream {
    fn new(
        schema: SchemaRef,
        receiver: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
    ) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for ArrowStreamChannelStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for ArrowStreamChannelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl DataSource for ArrowStreamDataSource {
    fn name(&self) -> &str {
        "arrow_stream"
    }

    async fn as_stream(&self) -> Result<SendableRecordBatchStream> {
        let path = self.path.clone();

        let schema = self.get_schema()?;
        let returned_schema = schema.clone();

        let (tx, rx) = mpsc::channel::<Result<RecordBatch, DataFusionError>>(32);

        thread::spawn(move || {
            let result = (|| -> Result<()> {
                let file = File::open(&path)?;
                let reader = StreamReader::try_new_buffered(file, None)?;

                for batch_result in reader {
                    let batch = batch_result?;
                    if tx.blocking_send(Ok(batch)).is_err() {
                        break; // rx was dropped
                    }
                }

                Ok(())
            })();

            if let Err(e) = result {
                // ignore the error if the channel is already closed
                let _ = tx.blocking_send(Err(DataFusionError::Execution(e.to_string())));
            }
        });

        Ok(Box::pin(ArrowStreamChannelStream::new(returned_schema, rx)))
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
        assert!(table_provider.is_err());
    }

    #[tokio::test]
    async fn test_as_stream() {
        let source = ArrowStreamDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        let mut stream = source.as_stream().await.unwrap();

        assert!(!stream.schema().fields().is_empty());
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }
}
