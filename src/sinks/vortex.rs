use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use tokio::fs::File;
use tokio::sync::mpsc;
use vortex::arrow::FromArrowArray;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::WriteOptionsSessionExt;
use vortex::stream::ArrayStreamAdapter;
use vortex::{ArrayRef, VortexSessionDefault};
use vortex_session::VortexSession;

use crate::{
    sinks::data_sink::{DataSink, SinkResult},
    utils::arrow_versioning::convert_arrow_57_to_56,
};

#[derive(Clone, Copy)]
pub struct VortexSinkOptions {
    record_batch_size: usize,
}

impl VortexSinkOptions {
    pub fn new() -> Self {
        Self {
            record_batch_size: 122_880,
        }
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }
}

impl Default for VortexSinkOptions {
    fn default() -> Self {
        Self::new()
    }
}

struct VortexSinkInner {
    rows_written: u64,
    coalescer: BatchCoalescer,
    sender: Option<mpsc::Sender<ArrayRef>>,
}

pub struct VortexSink {
    path: PathBuf,
    inner: Mutex<VortexSinkInner>,
    writer_task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl VortexSink {
    async fn flush_completed_batches(&mut self) -> Result<()> {
        while let Some(completed_batch) = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to lock inner: {}", e))?;
            inner.coalescer.next_completed_batch()
        } {
            let batch_v56 = convert_arrow_57_to_56(&completed_batch)?;
            let vortex_array = ArrayRef::from_arrow(batch_v56, false);

            let sender_clone = {
                let inner = self
                    .inner
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Failed to lock inner: {}", e))?;
                inner.sender.clone()
            };

            if let Some(sender) = sender_clone {
                sender
                    .send(vortex_array)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to send batch to writer: {}", e))?;
            }

            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to lock inner: {}", e))?;
            inner.rows_written += completed_batch.num_rows() as u64;
        }

        Ok(())
    }

    pub fn create(path: PathBuf, schema: &SchemaRef, options: VortexSinkOptions) -> Result<Self> {
        let coalescer = BatchCoalescer::new(Arc::clone(schema), options.record_batch_size);
        let (sender, receiver) = mpsc::channel(16);

        let path_clone = path.clone();
        let schema_clone = Arc::clone(schema);

        // the mega-hack! vortex doesn't support push-based writing in a way that
        // results in a Send struct, which we need for storing it in a struct that
        // implements async_trait. so we hack this by giving it a stream hooked up
        // to a channel and spawning a task that writes the arrays to the file
        // and can block until the channel is closed. the non-Send struct only
        // exists within the task, which is then safe to store a handle to within
        // the Sink struct.
        let writer_task = tokio::spawn(async move {
            Self::write_vortex_file(path_clone, schema_clone, receiver).await
        });

        let inner = VortexSinkInner {
            rows_written: 0,
            coalescer,
            sender: Some(sender),
        };

        Ok(Self {
            path,
            inner: Mutex::new(inner),
            writer_task: Some(writer_task),
        })
    }

    async fn write_vortex_file(
        path: PathBuf,
        schema: SchemaRef,
        mut receiver: mpsc::Receiver<ArrayRef>,
    ) -> Result<()> {
        let file = File::create(&path)
            .await
            .context("Failed to create Vortex file")?;

        let session = VortexSession::default();

        let batch_v56 = convert_arrow_57_to_56(&RecordBatch::new_empty(schema))?;
        // need an arrow 56 schema to convert to vortex dtype, so we can't just use the passed in schema
        let dtype = DType::from_arrow(batch_v56.schema());

        let array_stream = ArrayStreamAdapter::new(
            dtype.clone(),
            // a little hack to turn a channel into a stream
            stream::poll_fn(move |cx| receiver.poll_recv(cx).map(|opt| opt.map(Ok))),
        );

        session
            .write_options()
            .write(file, array_stream)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write Vortex file: {}", e))?;

        Ok(())
    }
}

#[async_trait]
impl DataSink for VortexSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to lock inner: {}", e))?;
            inner.coalescer.push_batch(batch)?;
        }

        self.flush_completed_batches().await?;

        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to lock inner: {}", e))?;
            inner.coalescer.finish_buffered_batch()?;
        }

        self.flush_completed_batches().await?;

        let rows_written = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to lock inner: {}", e))?;
            // make it impossible to write to the channel again, dropping the sender
            // which will also cause the writer task to finish.
            // IMPORTANT: rows_written must be updated when the batch is pushed, not when it's written
            //            in order for this to be correct
            inner.sender.take();
            inner.rows_written
        };

        // wait for the writer task to finish. the previous block will have dropped the sender,
        // which will cause the writer task to finish. so we just need to wait for it to finish
        // writing its last batches.
        if let Some(task) = self.writer_task.take() {
            task.await
                .map_err(|e| anyhow::anyhow!("Writer task panicked: {}", e))??;
        }

        Ok(SinkResult {
            files_written: vec![self.path.clone()],
            rows_written,
        })
    }
}
