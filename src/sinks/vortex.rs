use std::{io, sync::Arc};

use anyhow::{Result, anyhow};
use arrow::{array::RecordBatch, compute::BatchCoalescer, datatypes::SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Sink, SinkExt, stream};
use silk_chiffon_core::validate_batch_schema;
use silk_chiffon_storage::{ObjectUpload, ObjectUploadTask, StorageHandle};
use tokio::sync::mpsc;
use vortex::{
    VortexSessionDefault,
    array::{ArrayRef, stream::ArrayStreamAdapter},
    arrow::{FromArrowArray, FromArrowType},
    dtype::DType,
    file::WriteOptionsSessionExt,
    io::{IoBuf, VortexWrite},
    session::VortexSession,
};

use crate::sinks::{
    data_sink::{DataSink, SinkCompletion},
    with_cleanup_error,
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

impl VortexSinkInner {
    async fn flush_completed_batches(&mut self) -> Result<()> {
        while let Some(completed_batch) = self.coalescer.next_completed_batch() {
            let vortex_array = ArrayRef::from_arrow(completed_batch.clone(), false)?;

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
            .map_err(|error| anyhow!("failed to finish buffered batch: {error}"))
    }

    fn drop_sender(&mut self) {
        self.sender.take();
    }
}

pub struct VortexSink {
    schema: SchemaRef,
    inner: VortexSinkInner,
    task: Option<ObjectUploadTask<()>>,
}

impl VortexSink {
    pub fn create(
        handle: StorageHandle,
        schema: &SchemaRef,
        options: VortexSinkOptions,
    ) -> Result<Self> {
        let coalescer = BatchCoalescer::new(Arc::clone(schema), options.record_batch_size);
        let (sender, receiver) = mpsc::channel(16);
        let mut upload = ObjectUpload::new(handle);
        let writer = VortexUploadAdapter::new(upload.writer()?, upload.part_size().get());
        let sink_schema = Arc::clone(schema);
        let schema = Arc::clone(schema);
        let task = ObjectUploadTask::spawn("Vortex writer", upload, move |cancellation| {
            tokio::spawn(async move {
                cancellation
                    .run_until_cancelled(Self::write_vortex_file(writer, schema, receiver))
                    .await
                    .unwrap_or(Ok(()))
            })
        });

        Ok(Self {
            schema: sink_schema,
            inner: VortexSinkInner {
                rows_written: 0,
                coalescer,
                sender: Some(sender),
            },
            task: Some(task),
        })
    }

    async fn write_vortex_file<W>(
        writer: VortexUploadAdapter<W>,
        schema: SchemaRef,
        mut receiver: mpsc::Receiver<ArrayRef>,
    ) -> Result<()>
    where
        W: Sink<Bytes, Error = futures::channel::mpsc::SendError> + Send + Unpin,
    {
        let session = VortexSession::default();
        let dtype = DType::from_arrow(schema);
        let array_stream = ArrayStreamAdapter::new(
            dtype.clone(),
            stream::poll_fn(move |context| receiver.poll_recv(context).map(|item| item.map(Ok))),
        );

        session
            .write_options()
            .write(writer, array_stream)
            .await
            .map_err(|error| anyhow!("failed to write vortex file: {error}"))?;

        Ok(())
    }

    async fn abort_unfinished(&mut self) -> Vec<anyhow::Error> {
        self.stop_writer_input();
        match self.task.take() {
            Some(task) => task.abort().await.err().into_iter().collect(),
            None => Vec::new(),
        }
    }

    fn stop_writer_input(&mut self) {
        // Closing the channel looks like successful EOF, so publish cancellation first.
        if let Some(task) = &self.task {
            task.cancellation().cancel();
        }
        self.inner.drop_sender();
    }
}

impl Drop for VortexSink {
    fn drop(&mut self) {
        self.stop_writer_input();
    }
}

/// Bounded async adapter from Vortex's writer interface to one object upload.
struct VortexUploadAdapter<W> {
    writer: W,
    part_size: usize,
}

impl<W> VortexUploadAdapter<W> {
    fn new(writer: W, part_size: usize) -> Self {
        Self { writer, part_size }
    }
}

impl<W> VortexWrite for VortexUploadAdapter<W>
where
    W: Sink<Bytes, Error = futures::channel::mpsc::SendError> + Unpin,
{
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        for chunk in buffer.as_slice().chunks(self.part_size) {
            self.writer
                .send(Bytes::copy_from_slice(chunk))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "object upload stopped"))?;
        }
        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.writer
            .flush()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "object upload stopped"))
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.writer
            .close()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "object upload stopped"))
    }
}

#[async_trait]
impl DataSink for VortexSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        validate_batch_schema(&self.schema, batch.schema_ref())?;
        self.inner.coalescer.push_batch(batch)?;
        self.inner.flush_completed_batches().await
    }

    async fn finish(mut self: Box<Self>) -> Result<SinkCompletion> {
        let rows_written = {
            let result = async {
                self.inner.finish_buffered_batch()?;
                self.inner.flush_completed_batches().await?;
                self.inner.drop_sender();
                Ok::<_, anyhow::Error>(self.inner.rows_written)
            }
            .await;
            match result {
                Ok(rows_written) => rows_written,
                Err(primary) => {
                    let cleanup = self.abort_unfinished().await;
                    return Err(cleanup.into_iter().fold(primary, |primary, cleanup| {
                        with_cleanup_error(primary, Some(cleanup))
                    }));
                }
            }
        };

        let (_, url) = self
            .task
            .take()
            .ok_or_else(|| anyhow!("writer task already finished"))?
            .finish()
            .await?;

        Ok(SinkCompletion::new(url, [], rows_written))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        let mut errors = self.abort_unfinished().await.into_iter();
        match errors.next() {
            Some(primary) => Err(errors.fold(primary, |primary, cleanup| {
                with_cleanup_error(primary, Some(cleanup))
            })),
            None => Ok(()),
        }
    }
}
