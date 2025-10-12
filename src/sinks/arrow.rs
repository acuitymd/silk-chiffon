use futures::stream::StreamExt;
use std::{fs::File, io::BufWriter, path::PathBuf};

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    compute::BatchCoalescer,
    datatypes::SchemaRef,
    ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions, StreamWriter},
    },
};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

use crate::{
    sinks::data_sink::{DataSink, SinkResult},
    utils::arrow_io::{ArrowIPCFormat, RecordBatchWriterWithFinish},
};

pub struct ArrowSinkOptions {
    format: ArrowIPCFormat,
    record_batch_size: usize,
    compression: Option<CompressionType>,
}

impl ArrowSinkOptions {
    pub fn new() -> Self {
        Self {
            format: ArrowIPCFormat::default(),
            record_batch_size: 122_880,
            compression: None,
        }
    }

    pub fn with_format(mut self, format: ArrowIPCFormat) -> Self {
        self.format = format;
        self
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }

    pub fn with_compression(mut self, compression: Option<CompressionType>) -> Self {
        self.compression = compression;
        self
    }
}

pub struct ArrowSink {
    path: PathBuf,
    rows_written: u64,
    writer: Box<dyn RecordBatchWriterWithFinish>,
    coalescer: BatchCoalescer,
}

impl ArrowSink {
    pub fn create(path: PathBuf, schema: &SchemaRef, options: ArrowSinkOptions) -> Result<Self> {
        let file = BufWriter::new(File::create(&path)?);
        let write_options = match options.compression {
            Some(compression) => {
                IpcWriteOptions::default().try_with_compression(Some(compression))?
            }
            None => IpcWriteOptions::default(),
        };

        let writer: Box<dyn RecordBatchWriterWithFinish> = match options.format {
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

        let coalescer = BatchCoalescer::new(schema.clone(), options.record_batch_size);

        Ok(Self {
            path,
            rows_written: 0,
            writer,
            coalescer,
        })
    }
}

#[async_trait]
impl DataSink for ArrowSink {
    async fn write_stream(&mut self, mut stream: SendableRecordBatchStream) -> Result<SinkResult> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            self.write_batch(batch).await?;
        }

        self.finish().await
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.coalescer.push_batch(batch)?;

        while let Some(completed_batch) = self.coalescer.next_completed_batch() {
            self.writer.write(&completed_batch)?;
            self.rows_written += completed_batch.num_rows() as u64;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        self.coalescer.finish_buffered_batch()?;

        if let Some(final_batch) = self.coalescer.next_completed_batch() {
            self.writer.write(&final_batch)?;
            self.rows_written += final_batch.num_rows() as u64;
        }

        self.writer.finish()?;

        Ok(SinkResult {
            files_written: vec![self.path.clone()],
            rows_written: self.rows_written,
        })
    }
}
