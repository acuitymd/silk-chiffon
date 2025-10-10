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
    pub fn create(path: String) -> Self {
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
        "arrow_file"
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
