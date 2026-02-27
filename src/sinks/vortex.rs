//! Vortex sink for writing record batches to Vortex compressed columnar format.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use tokio::sync::mpsc;
use tokio::{fs::File, sync::Mutex};
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;

use crate::sinks::{
    DEFAULT_RECORD_BATCH_SIZE,
    data_sink::{DataSink, SinkResult},
};
use crate::utils::memory::estimate_row_bytes;

#[derive(Clone, Copy)]
pub struct VortexSinkOptions {
    record_batch_size: usize,
    memory_budget: Option<usize>,
    row_bytes: Option<usize>,
}

impl VortexSinkOptions {
    pub fn new() -> Self {
        Self {
            record_batch_size: DEFAULT_RECORD_BATCH_SIZE,
            memory_budget: None,
            row_bytes: None,
        }
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }

    pub fn with_memory_budget(mut self, budget: Option<usize>) -> Self {
        self.memory_budget = budget;
        self
    }

    pub fn with_row_bytes(mut self, row_bytes: Option<usize>) -> Self {
        self.row_bytes = row_bytes;
        self
    }

    pub fn estimate_sink_needs(&self, row_bytes: usize) -> usize {
        self.record_batch_size.saturating_mul(row_bytes)
    }

    fn resolve_queue_depth(&self, schema: &SchemaRef) -> usize {
        if let Some(budget) = self.memory_budget {
            let row_bytes = self
                .row_bytes
                .unwrap_or_else(|| estimate_row_bytes(schema))
                .max(1);
            let batch_bytes = self.record_batch_size.saturating_mul(row_bytes);
            let derived = (budget / batch_bytes).max(1);
            tracing::debug!(
                budget,
                row_bytes,
                batch_bytes,
                queue_depth = derived,
                "vortex queue depth (budget-derived)"
            );
            return derived;
        }

        DEFAULT_QUEUE_DEPTH
    }
}

impl Default for VortexSinkOptions {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_QUEUE_DEPTH: usize = 16;

struct VortexSinkInner {
    rows_written: u64,
    coalescer: BatchCoalescer,
    sender: Option<mpsc::Sender<ArrayRef>>,
}

impl VortexSinkInner {
    async fn flush_completed_batches(&mut self) -> Result<()> {
        while let Some(completed_batch) = self.coalescer.next_completed_batch() {
            let vortex_array = ArrayRef::from_arrow(completed_batch.clone(), false);

            self.sender
                .as_ref()
                .ok_or_else(|| anyhow!("sender already closed"))?
                .send(vortex_array)
                .await?;

            self.rows_written += completed_batch.num_rows() as u64;
        }

        Ok(())
    }

    fn finish_buffered_batch(&mut self) -> Result<()> {
        self.coalescer
            .finish_buffered_batch()
            .map_err(|e| anyhow!("failed to finish buffered batch: {e}"))
    }

    fn drop_sender(&mut self) {
        self.sender.take();
    }
}

pub struct VortexSink {
    path: PathBuf,
    inner: Mutex<VortexSinkInner>,
    writer_task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl VortexSink {
    pub fn create(path: PathBuf, schema: &SchemaRef, options: VortexSinkOptions) -> Result<Self> {
        let queue_depth = options.resolve_queue_depth(schema);
        let coalescer = BatchCoalescer::new(Arc::clone(schema), options.record_batch_size);
        let (sender, receiver) = mpsc::channel(queue_depth);

        let path_clone = path.clone();
        let schema_clone = Arc::clone(schema);

        // the vortex lib doesn't support push-based writing in a way that
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

        let dtype = DType::from_arrow(schema);

        let array_stream = ArrayStreamAdapter::new(
            dtype.clone(),
            // a little hack to turn a channel into a stream
            stream::poll_fn(move |cx| receiver.poll_recv(cx).map(|opt| opt.map(Ok))),
        );

        session
            .write_options()
            .write(file, array_stream)
            .await
            .map_err(|e| anyhow::anyhow!("failed to write vortex file: {}", e))?;

        Ok(())
    }
}

#[async_trait]
impl DataSink for VortexSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.coalescer.push_batch(batch)?;
        inner.flush_completed_batches().await?;

        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let mut inner = self.inner.lock().await;
        inner.finish_buffered_batch()?;
        inner.flush_completed_batches().await?;

        // make it impossible to write to the channel again, dropping the sender
        // which will also cause the writer task to finish.
        // IMPORTANT: rows_written must be updated when the batch is pushed, not when it's written
        //            in order for this to be correct
        inner.drop_sender();

        // wait for the writer task to finish. the sender was dropped above,
        // which will cause the writer task to finish. so we just need to wait
        // for it to finish writing its last batches.
        self.writer_task
            .take()
            .ok_or_else(|| anyhow!("writer task already finished"))?
            .await
            .map_err(|e| anyhow!("error joining writer task: {e}"))?
            .map_err(|e| anyhow!("writer task errored: {e}"))?;

        Ok(SinkResult {
            files_written: vec![self.path.clone()],
            rows_written: inner.rows_written,
        })
    }
}
