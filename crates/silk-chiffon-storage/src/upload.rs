//! Bounded one-object upload lifecycle shared by file formats.

use std::{
    fmt,
    io::{self, Write},
    num::NonZeroUsize,
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use clap::Args;
use futures::{Sink, SinkExt, StreamExt, channel::mpsc};
use object_store::{MultipartUpload, ObjectStoreExt, PutPayload};
use thiserror::Error;
use tokio::{
    sync::{Semaphore, oneshot},
    task::{JoinHandle, JoinSet},
};
use url::Url;

use crate::StorageHandle;

const DEFAULT_PART_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_PARTS: usize = 8;

/// Parsed command-line settings for storage-owned object uploads.
#[derive(Args, Clone, Debug)]
#[command(about = None, long_about = None)]
pub struct ObjectUploadArgs {
    /// Adaptive single-put threshold and multipart part size.
    #[arg(
        long = "object-store-upload-part-size",
        default_value = "10MiB",
        value_parser = parse_part_size
    )]
    part_size: NonZeroUsize,
    /// Maximum multipart part requests in flight across the command.
    #[arg(
        long = "object-store-max-in-flight-parts",
        default_value_t = NonZeroUsize::new(DEFAULT_MAX_IN_FLIGHT_PARTS).unwrap()
    )]
    max_in_flight_parts: NonZeroUsize,
}

impl ObjectUploadArgs {
    pub(crate) fn into_settings(self) -> ObjectUploadSettings {
        ObjectUploadSettings::new(self.part_size, self.max_in_flight_parts)
    }
}

/// Immutable upload limits shared by every output handle in one storage session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectUploadSettings {
    part_size: NonZeroUsize,
    max_in_flight_parts: NonZeroUsize,
}

impl ObjectUploadSettings {
    /// Creates upload settings from validated positive limits.
    pub const fn new(part_size: NonZeroUsize, max_in_flight_parts: NonZeroUsize) -> Self {
        Self {
            part_size,
            max_in_flight_parts,
        }
    }

    /// Returns the adaptive threshold and multipart part size.
    pub const fn part_size(self) -> NonZeroUsize {
        self.part_size
    }

    /// Returns the command-wide multipart request limit.
    pub const fn max_in_flight_parts(self) -> NonZeroUsize {
        self.max_in_flight_parts
    }
}

impl Default for ObjectUploadSettings {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(DEFAULT_PART_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_MAX_IN_FLIGHT_PARTS).unwrap(),
        )
    }
}

#[derive(Debug)]
pub(crate) struct ObjectUploadContext {
    pub(crate) settings: ObjectUploadSettings,
    part_permits: Arc<Semaphore>,
}

impl ObjectUploadContext {
    pub(crate) fn new(settings: ObjectUploadSettings) -> Self {
        Self {
            settings,
            part_permits: Arc::new(Semaphore::new(settings.max_in_flight_parts.get())),
        }
    }
}

/// Failure while writing, completing, aborting, or driving one object upload.
#[derive(Debug, Error)]
pub enum ObjectUploadError {
    #[error("failed to write output object {target}: {source}")]
    Write {
        target: Url,
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to complete output object {target}: {source}")]
    Complete {
        target: Url,
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to abort output object {target}: {source}")]
    Abort {
        target: Url,
        #[source]
        source: anyhow::Error,
    },
    #[error("output upload task for {target} failed: {source}")]
    Task {
        target: Url,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("output object {target} already has a byte writer")]
    WriterAlreadyTaken { target: Url },
}

enum TerminalAction {
    Complete,
    Abort,
}

struct ActiveUpload {
    terminal: Option<oneshot::Sender<TerminalAction>>,
    task: JoinHandle<Result<Option<Url>, ObjectUploadError>>,
}

/// Storage-owned state machine for writing exactly one object.
pub struct ObjectUpload {
    target: Url,
    settings: ObjectUploadSettings,
    worker: Option<UploadWorker>,
    active: Option<ActiveUpload>,
    direct_writer: Option<mpsc::Sender<Bytes>>,
}

impl fmt::Debug for ObjectUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectUpload")
            .field("target", &self.target)
            .field("active", &self.active.is_some())
            .finish()
    }
}

impl ObjectUpload {
    /// Creates an upload for one prepared output handle.
    pub fn new(handle: StorageHandle) -> Self {
        let target = handle.url().clone();
        let settings = handle.object_upload_context.settings;
        Self {
            target,
            settings,
            worker: Some(UploadWorker::new(handle)),
            active: None,
            direct_writer: None,
        }
    }

    /// Returns the maximum byte chunk accepted from a format bridge.
    pub fn part_size(&self) -> NonZeroUsize {
        self.settings.part_size
    }

    /// Writes bytes through the bounded async upload bridge.
    pub async fn write(&mut self, mut bytes: Bytes) -> Result<(), ObjectUploadError> {
        if self.direct_writer.is_none() {
            if self.active.is_some() {
                return Err(ObjectUploadError::WriterAlreadyTaken {
                    target: self.target.clone(),
                });
            }
            let sender = self.start_worker();
            self.direct_writer = Some(sender);
        }
        let part_size = self.settings.part_size.get();
        let sender = self.direct_writer.as_mut().expect("created above");
        while !bytes.is_empty() {
            let chunk = bytes.split_to(bytes.len().min(part_size));
            sender
                .send(chunk)
                .await
                .map_err(|source| ObjectUploadError::Write {
                    target: self.target.clone(),
                    source: anyhow::Error::new(source),
                })?;
        }
        Ok(())
    }

    /// Creates the bounded synchronous writer used by blocking codecs.
    pub fn blocking_writer(&mut self) -> Result<BlockingObjectUploadWriter, ObjectUploadError> {
        if self.active.is_some() {
            return Err(ObjectUploadError::WriterAlreadyTaken {
                target: self.target.clone(),
            });
        }
        let part_size = self.settings.part_size.get();
        Ok(BlockingObjectUploadWriter {
            sender: self.start_worker(),
            target: self.target.clone(),
            part_size,
        })
    }

    /// Creates the bounded async byte sink used by format-specific adapters.
    pub fn writer(
        &mut self,
    ) -> Result<impl Sink<Bytes, Error = mpsc::SendError> + Send + Unpin + use<>, ObjectUploadError>
    {
        if self.active.is_some() {
            return Err(ObjectUploadError::WriterAlreadyTaken {
                target: self.target.clone(),
            });
        }
        Ok(self.start_worker())
    }

    /// Makes the object durable and returns its canonical target URL.
    pub async fn complete(mut self) -> Result<Url, ObjectUploadError> {
        self.direct_writer.take();
        if let Some(worker) = self.worker.take() {
            return worker.complete().await;
        }
        let mut active = self.active.take().expect("an active upload has a task");
        let terminal_sent = active
            .terminal
            .take()
            .expect("terminal action has not been sent")
            .send(TerminalAction::Complete)
            .is_ok();
        let result = active
            .task
            .await
            .map_err(|source| ObjectUploadError::Task {
                target: self.target.clone(),
                source,
            })??;
        if !terminal_sent {
            return Err(ObjectUploadError::Complete {
                target: self.target.clone(),
                source: anyhow::anyhow!("upload task stopped before completion"),
            });
        }
        match result {
            Some(url) => Ok(url),
            None => Err(ObjectUploadError::Complete {
                target: self.target.clone(),
                source: anyhow::anyhow!("upload was aborted instead of completed"),
            }),
        }
    }

    /// Cancels in-flight work and awaits multipart cleanup.
    pub async fn abort(mut self) -> Result<(), ObjectUploadError> {
        self.direct_writer.take();
        if let Some(mut worker) = self.worker.take() {
            return worker.abort().await;
        }
        let mut active = self.active.take().expect("an active upload has a task");
        let terminal_sent = active
            .terminal
            .take()
            .expect("terminal action has not been sent")
            .send(TerminalAction::Abort)
            .is_ok();
        active
            .task
            .await
            .map_err(|source| ObjectUploadError::Task {
                target: self.target.clone(),
                source,
            })??;
        if !terminal_sent {
            return Err(ObjectUploadError::Abort {
                target: self.target.clone(),
                source: anyhow::anyhow!("upload task stopped before abort"),
            });
        }
        Ok(())
    }

    fn start_worker(&mut self) -> mpsc::Sender<Bytes> {
        let worker = self
            .worker
            .take()
            .expect("a writer can be created only once");
        let (sender, receiver) = mpsc::channel(1);
        let (terminal, terminal_receiver) = oneshot::channel();
        self.active = Some(ActiveUpload {
            terminal: Some(terminal),
            task: tokio::spawn(run_upload(worker, receiver, terminal_receiver)),
        });
        sender
    }
}

impl Drop for ObjectUpload {
    fn drop(&mut self) {
        self.direct_writer.take();
        if let Some(active) = self.active.as_mut()
            && let Some(terminal) = active.terminal.take()
        {
            let _ = terminal.send(TerminalAction::Abort);
        }
    }
}

/// Bounded `std::io::Write` bridge for synchronous format encoders.
pub struct BlockingObjectUploadWriter {
    sender: mpsc::Sender<Bytes>,
    target: Url,
    part_size: usize,
}

impl fmt::Debug for BlockingObjectUploadWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingObjectUploadWriter")
            .field("target", &self.target)
            .field("part_size", &self.part_size)
            .finish()
    }
}

impl Write for BlockingObjectUploadWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        for chunk in buffer.chunks(self.part_size) {
            futures::executor::block_on(self.sender.send(Bytes::copy_from_slice(chunk))).map_err(
                |_| io::Error::new(io::ErrorKind::BrokenPipe, "object upload task stopped"),
            )?;
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn run_upload(
    mut worker: UploadWorker,
    mut bytes: mpsc::Receiver<Bytes>,
    mut terminal: oneshot::Receiver<TerminalAction>,
) -> Result<Option<Url>, ObjectUploadError> {
    loop {
        tokio::select! {
            biased;
            action = &mut terminal => {
                return match action.unwrap_or(TerminalAction::Abort) {
                    TerminalAction::Abort => worker.abort().await.map(|()| None),
                    TerminalAction::Complete => {
                        bytes.close();
                        while let Some(chunk) = bytes.next().await {
                            worker.write(chunk).await?;
                        }
                        worker.complete().await.map(Some)
                    }
                };
            }
            chunk = bytes.next() => match chunk {
                Some(chunk) => worker.write(chunk).await?,
                None => {
                    return match terminal.await.unwrap_or(TerminalAction::Abort) {
                        TerminalAction::Abort => worker.abort().await.map(|()| None),
                        TerminalAction::Complete => worker.complete().await.map(Some),
                    };
                }
            },
        }
    }
}

struct UploadWorker {
    handle: StorageHandle,
    target: Url,
    settings: ObjectUploadSettings,
    part_permits: Arc<Semaphore>,
    buffer: BytesMut,
    multipart: Option<MultipartState>,
}

impl UploadWorker {
    fn new(handle: StorageHandle) -> Self {
        let context = Arc::clone(&handle.object_upload_context);
        Self {
            target: handle.url().clone(),
            handle,
            settings: context.settings,
            part_permits: Arc::clone(&context.part_permits),
            buffer: BytesMut::new(),
            multipart: None,
        }
    }

    async fn write(&mut self, bytes: Bytes) -> Result<(), ObjectUploadError> {
        if self.multipart.is_none()
            && self.buffer.len().saturating_add(bytes.len()) < self.settings.part_size.get()
        {
            self.buffer.extend_from_slice(&bytes);
            return Ok(());
        }

        if self.multipart.is_none() {
            let upload = self
                .handle
                .object_store()
                .put_multipart(self.handle.object_path())
                .await
                .map_err(|source| ObjectUploadError::Write {
                    target: self.target.clone(),
                    source: source.into(),
                })?;
            let mut multipart =
                MultipartState::new(upload, self.settings, Arc::clone(&self.part_permits));
            multipart
                .write(self.buffer.split().freeze())
                .await
                .map_err(|source| ObjectUploadError::Write {
                    target: self.target.clone(),
                    source,
                })?;
            self.multipart = Some(multipart);
        }

        let result = self
            .multipart
            .as_mut()
            .expect("created above")
            .write(bytes)
            .await;
        if let Err(primary) = result {
            let cleanup = self.abort_multipart().await.err();
            return Err(ObjectUploadError::Write {
                target: self.target.clone(),
                source: primary_with_cleanup(primary, cleanup),
            });
        }
        Ok(())
    }

    async fn complete(mut self) -> Result<Url, ObjectUploadError> {
        if self.multipart.is_none() {
            self.handle
                .object_store()
                .put(self.handle.object_path(), self.buffer.freeze().into())
                .await
                .map_err(|source| ObjectUploadError::Complete {
                    target: self.target.clone(),
                    source: source.into(),
                })?;
            return Ok(self.target);
        }

        let result = self
            .multipart
            .as_mut()
            .expect("checked above")
            .complete()
            .await;
        if let Err(primary) = result {
            let cleanup = self.abort_multipart().await.err();
            return Err(ObjectUploadError::Complete {
                target: self.target.clone(),
                source: primary_with_cleanup(primary, cleanup),
            });
        }
        Ok(self.target)
    }

    async fn abort(&mut self) -> Result<(), ObjectUploadError> {
        self.abort_multipart()
            .await
            .map_err(|source| ObjectUploadError::Abort {
                target: self.target.clone(),
                source,
            })
    }

    async fn abort_multipart(&mut self) -> anyhow::Result<()> {
        if let Some(mut multipart) = self.multipart.take() {
            multipart.abort().await?;
        }
        Ok(())
    }
}

struct MultipartState {
    upload: Box<dyn MultipartUpload>,
    settings: ObjectUploadSettings,
    part_permits: Arc<Semaphore>,
    buffer: BytesMut,
    tasks: JoinSet<object_store::Result<()>>,
}

impl MultipartState {
    fn new(
        upload: Box<dyn MultipartUpload>,
        settings: ObjectUploadSettings,
        part_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            upload,
            settings,
            part_permits,
            buffer: BytesMut::new(),
            tasks: JoinSet::new(),
        }
    }

    async fn write(&mut self, mut bytes: Bytes) -> anyhow::Result<()> {
        let part_size = self.settings.part_size.get();
        while !bytes.is_empty() {
            let remaining = part_size - self.buffer.len();
            let length = bytes.len().min(remaining);
            self.buffer.extend_from_slice(&bytes.split_to(length));
            if self.buffer.len() == part_size {
                let payload = self.buffer.split().freeze();
                self.start_part(payload).await?;
            }
        }
        Ok(())
    }

    async fn start_part(&mut self, bytes: Bytes) -> anyhow::Result<()> {
        while self.tasks.len() >= self.settings.max_in_flight_parts.get() {
            self.join_one().await?;
        }
        let part = self.upload.put_part(PutPayload::from_bytes(bytes));
        let permits = Arc::clone(&self.part_permits);
        self.tasks.spawn(async move {
            let _permit =
                permits
                    .acquire_owned()
                    .await
                    .map_err(|_| object_store::Error::Generic {
                        store: "object upload",
                        source: Box::new(io::Error::other("multipart part limiter closed")),
                    })?;
            part.await
        });
        Ok(())
    }

    async fn join_one(&mut self) -> anyhow::Result<()> {
        self.tasks
            .join_next()
            .await
            .expect("called only with an in-flight part")??;
        Ok(())
    }

    async fn complete(&mut self) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            let bytes = self.buffer.split().freeze();
            self.start_part(bytes).await?;
        }
        while !self.tasks.is_empty() {
            self.join_one().await?;
        }
        self.upload.complete().await?;
        Ok(())
    }

    async fn abort(&mut self) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        self.upload.abort().await?;
        Ok(())
    }
}

fn primary_with_cleanup(primary: anyhow::Error, cleanup: Option<anyhow::Error>) -> anyhow::Error {
    match cleanup {
        Some(cleanup) => anyhow::Error::new(PrimaryWithCleanup { primary, cleanup }),
        None => primary,
    }
}

#[derive(Debug)]
struct PrimaryWithCleanup {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}

impl fmt::Display for PrimaryWithCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; multipart cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}

impl std::error::Error for PrimaryWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary.source()
    }
}

fn parse_part_size(input: &str) -> Result<NonZeroUsize, String> {
    let bytes = input
        .parse::<bytesize::ByteSize>()
        .map_err(|error| error.to_string())?
        .as_u64();
    let size =
        usize::try_from(bytes).map_err(|_| format!("upload part size is too large: {input}"))?;
    NonZeroUsize::new(size).ok_or_else(|| "upload part size must be at least 1 byte".to_owned())
}
