use futures::stream::StreamExt;
use std::{fs::File, io::BufWriter, path::PathBuf};

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter},
};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

use crate::{
    sinks::data_sink::{DataSink, SinkResult},
    utils::arrow_io::{ArrowIPCFormat, RecordBatchWriterWithFinish},
};

pub struct ArrowSink {
    path: PathBuf,
    rows_written: u64,
    writer: Box<dyn RecordBatchWriterWithFinish>,
}

impl ArrowSink {
    pub fn create(
        path: PathBuf,
        schema: &SchemaRef,
        format: ArrowIPCFormat,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let file = BufWriter::new(File::create(&path)?);

        let writer: Box<dyn RecordBatchWriterWithFinish> = match format {
            ArrowIPCFormat::File => Box::new(FileWriter::try_new_with_options(
                file,
                &schema,
                write_options,
            )?),
            ArrowIPCFormat::Stream => Box::new(StreamWriter::try_new_with_options(
                file,
                &schema,
                write_options,
            )?),
        };

        Ok(Self {
            path,
            rows_written: 0,
            writer,
        })
    }
}

#[async_trait]
impl DataSink for ArrowSink {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<SinkResult> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.write_batch(&batch).await?;
        }

        self.finish().await
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.rows_written += batch.num_rows() as u64;
        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        self.writer.finish()?;
        Ok(SinkResult {
            files_written: vec![self.path.clone()],
            rows_written: 0,
        })
    }
}
