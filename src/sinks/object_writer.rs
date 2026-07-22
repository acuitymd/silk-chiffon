//! Bounded byte handoff from format encoders to object storage.

use std::io::{self, Write};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::{sync::mpsc, task::JoinHandle};
use vortex::io::{IoBuf, VortexWrite};

use crate::storage::ObjectOutput;

const UPLOAD_QUEUE_DEPTH: usize = 1;

pub(crate) type UploadHandle = JoinHandle<Result<ObjectOutput>>;

pub(crate) fn spawn_object_upload(mut output: ObjectOutput) -> (mpsc::Sender<Bytes>, UploadHandle) {
    let (sender, mut receiver) = mpsc::channel::<Bytes>(UPLOAD_QUEUE_DEPTH);
    let handle = tokio::spawn(async move {
        while let Some(bytes) = receiver.recv().await {
            output.write(bytes).await?;
        }
        Ok(output)
    });
    (sender, handle)
}

pub(crate) async fn join_object_upload(handle: UploadHandle) -> Result<ObjectOutput> {
    handle.await.context("object uploader task panicked")?
}

pub(crate) struct DrainWriter {
    sender: mpsc::Sender<Bytes>,
    buffer: Vec<u8>,
    part_size: usize,
}

impl DrainWriter {
    pub(crate) fn new(sender: mpsc::Sender<Bytes>, part_size: usize) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(part_size),
            part_size,
        }
    }

    pub(crate) fn finish(mut self) -> io::Result<()> {
        self.drain()
    }

    fn drain(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.part_size),
        ));
        self.sender
            .blocking_send(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "object uploader stopped"))
    }
}

impl Write for DrainWriter {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len();
        while !bytes.is_empty() {
            let available = self.part_size - self.buffer.len();
            let take = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == self.part_size {
                self.drain()?;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.drain()
    }
}

pub(crate) struct VortexObjectWriter {
    sender: mpsc::Sender<Bytes>,
    part_size: usize,
}

impl VortexObjectWriter {
    pub(crate) fn new(sender: mpsc::Sender<Bytes>, part_size: usize) -> Self {
        Self { sender, part_size }
    }
}

impl VortexWrite for VortexObjectWriter {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        for chunk in buffer.as_slice().chunks(self.part_size) {
            self.sender
                .send(Bytes::copy_from_slice(chunk))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "object uploader stopped")
                })?;
        }
        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) async fn memory_output(
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    path: &str,
    part_size: usize,
) -> ObjectOutput {
    use datafusion::execution::object_store::ObjectStoreUrl;
    use object_store::path::Path;

    use crate::storage::{ObjectLocation, OutputPolicy, StorageConfig, StoreHandle, StoreKind};

    let handle = std::sync::Arc::new(StoreHandle::new(
        StoreKind::Gcs {
            bucket: "memory".to_string(),
        },
        ObjectStoreUrl::parse("gs://memory").unwrap(),
        store,
    ));
    let display = format!("gs://memory/{path}");
    let location = ObjectLocation::new(display.clone(), display, Path::from(path), handle);
    ObjectOutput::open(
        location,
        StorageConfig {
            max_requests: 64,
            upload_part_size: part_size,
            upload_concurrency: 2,
        },
        OutputPolicy::new(true, false),
    )
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test(flavor = "multi_thread")]
    async fn drain_writer_chunks_at_the_upload_part_size() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let handle = tokio::task::spawn_blocking(move || {
            let mut writer = DrainWriter::new(sender, 4);
            writer.write_all(b"abcdefghij").unwrap();
            writer.finish().unwrap();
        });

        let mut chunks = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            chunks.push(chunk);
        }
        handle.await.unwrap();

        assert_eq!(chunks, vec!["abcd", "efgh", "ij"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drain_writer_blocks_when_the_upload_queue_is_full() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let handle = tokio::task::spawn_blocking(move || {
            let mut writer = DrainWriter::new(sender, 4);
            writer.write_all(b"abcdefghijkl").unwrap();
            writer.finish().unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!handle.is_finished());

        let mut chunks = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            chunks.push(chunk);
        }
        handle.await.unwrap();

        assert_eq!(chunks, vec!["abcd", "efgh", "ijkl"]);
    }
}
