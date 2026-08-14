//! Command-scoped Vortex output binding and per-object sinks.
//!
//! Vortex owns array coalescing and encoding. Storage owns target claims,
//! adaptive uploads, durability, and cleanup. The bounded adapter below is the
//! narrow bridge between those lifecycles.

use std::{fmt, io, sync::Arc};

use anyhow::{Error, Result, anyhow};
use arrow::{array::RecordBatch, compute::BatchCoalescer, datatypes::SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Sink as FuturesSink, SinkExt, stream};
use silk_chiffon_core::{
    DataSink, OpenSinkMode, SinkBinding, SinkBindingConfig, SinkCompletion, validate_batch_schema,
};
use silk_chiffon_storage::{ObjectUpload, ObjectUploadTask, StorageHandle};
use tokio::sync::mpsc;
use vortex::{
    array::{ArrayRef, stream::ArrayStreamAdapter},
    arrow::{FromArrowArray, FromArrowType},
    dtype::DType,
    file::WriteOptionsSessionExt,
    io::{IoBuf, VortexWrite},
    session::VortexSession,
};

use crate::args::TransformState;

const DEFAULT_RECORD_BATCH_SIZE: usize = 122_880;
const SINGLE_SINK_QUEUE_DEPTH: usize = 16;
const MULTIPLE_SINK_QUEUE_DEPTH: usize = 1;

pub(crate) async fn bind(
    config: &SinkBindingConfig,
    state: &TransformState,
) -> Result<Box<dyn SinkBinding>> {
    Ok(Box::new(output_binding(config, state)))
}

fn output_binding(config: &SinkBindingConfig, state: &TransformState) -> OutputBinding {
    let queue_depth = match config.open_sink_mode() {
        OpenSinkMode::OneAtATime => SINGLE_SINK_QUEUE_DEPTH,
        OpenSinkMode::Multiple => MULTIPLE_SINK_QUEUE_DEPTH,
    };
    OutputBinding {
        record_batch_size: state
            .vortex_record_batch_size
            .unwrap_or(DEFAULT_RECORD_BATCH_SIZE),
        queue_depth,
        session: state.session().clone(),
    }
}

struct OutputBinding {
    record_batch_size: usize,
    queue_depth: usize,
    session: VortexSession,
}

#[async_trait]
impl SinkBinding for OutputBinding {
    async fn open_sink(
        &self,
        handle: StorageHandle,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        Ok(Box::new(Sink::create(
            handle,
            schema,
            self.record_batch_size,
            self.queue_depth,
            self.session.clone(),
        )?))
    }
}

struct Sink {
    schema: SchemaRef,
    rows_written: u64,
    coalescer: BatchCoalescer,
    sender: Option<mpsc::Sender<ArrayRef>>,
    task: Option<ObjectUploadTask<()>>,
}

impl Sink {
    fn create(
        handle: StorageHandle,
        schema: SchemaRef,
        record_batch_size: usize,
        queue_depth: usize,
        session: VortexSession,
    ) -> Result<Self> {
        let coalescer = BatchCoalescer::new(Arc::clone(&schema), record_batch_size);
        let (sender, receiver) = mpsc::channel(queue_depth);
        let mut upload = ObjectUpload::new(handle);
        let writer = UploadWriter::new(upload.writer()?, upload.part_size().get());
        let writer_schema = Arc::clone(&schema);
        let task = ObjectUploadTask::spawn("Vortex writer", upload, move |cancellation| {
            tokio::spawn(async move {
                cancellation
                    .run_until_cancelled(write_file(writer, writer_schema, receiver, session))
                    .await
                    .ok_or_else(|| anyhow!("Vortex writer was cancelled"))?
            })
        });

        Ok(Self {
            schema,
            rows_written: 0,
            coalescer,
            sender: Some(sender),
            task: Some(task),
        })
    }

    async fn flush_completed_batches(&mut self) -> Result<()> {
        while let Some(completed_batch) = self.coalescer.next_completed_batch() {
            let rows = u64::try_from(completed_batch.num_rows())?;
            let vortex_array = ArrayRef::from_arrow(completed_batch, false)?;
            let sender = self
                .sender
                .as_ref()
                .ok_or_else(|| anyhow!("Vortex writer input is closed"))?;
            let cancellation = self
                .task
                .as_ref()
                .ok_or_else(|| anyhow!("Vortex writer task is finished"))?
                .cancellation()
                .clone();
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return Err(anyhow!("Vortex writer stopped before accepting a batch"));
                }
                result = sender.send(vortex_array) => {
                    result.map_err(|_| anyhow!("Vortex writer stopped before accepting a batch"))?;
                }
            }
            self.rows_written += rows;
        }
        Ok(())
    }

    fn cancel_writer(&mut self) {
        // Closing the channel is valid end-of-file to the codec, so cancellation
        // must become observable first when the sink did not finish normally.
        if let Some(task) = &self.task {
            task.cancellation().cancel();
        }
        self.sender.take();
    }

    async fn abort_unfinished(&mut self) -> Result<()> {
        self.cancel_writer();
        match self.task.take() {
            Some(task) => task.abort().await,
            None => Ok(()),
        }
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        self.cancel_writer();
    }
}

async fn write_file<W>(
    writer: UploadWriter<W>,
    schema: SchemaRef,
    mut receiver: mpsc::Receiver<ArrayRef>,
    session: VortexSession,
) -> Result<()>
where
    W: FuturesSink<Bytes, Error = futures::channel::mpsc::SendError> + Send + Unpin,
{
    let dtype = DType::from_arrow(schema);
    let arrays = ArrayStreamAdapter::new(
        dtype,
        stream::poll_fn(move |context| receiver.poll_recv(context).map(|item| item.map(Ok))),
    );
    session
        .write_options()
        .write(writer, arrays)
        .await
        .map_err(|error| anyhow!("failed to write Vortex file: {error}"))?;
    Ok(())
}

/// Adapts Vortex's async byte writer to one storage-owned object upload.
struct UploadWriter<W> {
    writer: W,
    part_size: usize,
}

impl<W> UploadWriter<W> {
    fn new(writer: W, part_size: usize) -> Self {
        Self { writer, part_size }
    }
}

impl<W> VortexWrite for UploadWriter<W>
where
    W: FuturesSink<Bytes, Error = futures::channel::mpsc::SendError> + Unpin,
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
impl DataSink for Sink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        validate_batch_schema(&self.schema, batch.schema_ref())?;
        self.coalescer.push_batch(batch)?;
        self.flush_completed_batches().await
    }

    async fn finish(mut self: Box<Self>) -> Result<SinkCompletion> {
        let result = async {
            self.coalescer
                .finish_buffered_batch()
                .map_err(|error| anyhow!("failed to finish buffered batch: {error}"))?;
            self.flush_completed_batches().await?;
            self.sender.take();
            Ok::<_, Error>(self.rows_written)
        }
        .await;
        let rows_written = match result {
            Ok(rows_written) => rows_written,
            Err(primary) => {
                return match self.abort_unfinished().await {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(with_cleanup_error(primary, cleanup)),
                };
            }
        };

        let (_, url) = self
            .task
            .take()
            .ok_or_else(|| anyhow!("Vortex writer task is finished"))?
            .finish()
            .await?;
        Ok(SinkCompletion::new(url, [], rows_written))
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.abort_unfinished().await
    }
}

fn with_cleanup_error(primary: Error, cleanup: Error) -> Error {
    Error::new(PrimaryWithCleanup { primary, cleanup })
}

#[derive(Debug)]
struct PrimaryWithCleanup {
    primary: Error,
    cleanup: Error,
}

impl fmt::Display for PrimaryWithCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}

impl std::error::Error for PrimaryWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary.source()
    }
}

#[cfg(test)]
mod tests {
    use std::{any::Any, sync::Arc};

    use clap::{Args, Command, FromArgMatches};
    use object_store::ObjectStoreExt;
    use silk_chiffon_storage::{ExistingOutput, LocationInput, OutputPreparation, StorageSession};
    use silk_chiffon_test_support::{TestBatch, prepared_local_output};
    use vortex::{
        file::OpenOptionsSessionExt,
        session::{SessionExt, SessionVar},
    };

    use super::*;

    fn state(arguments: &[&str]) -> TransformState {
        let command = TransformState::augment_args(Command::new("test"));
        let matches = command
            .try_get_matches_from(std::iter::once("test").chain(arguments.iter().copied()))
            .unwrap();
        TransformState::from_arg_matches(&matches).unwrap()
    }

    fn config(mode: OpenSinkMode) -> SinkBindingConfig {
        SinkBindingConfig::new(std::num::NonZeroUsize::new(1).unwrap(), mode, Vec::new())
    }

    #[derive(Clone, Debug)]
    struct SessionMarker(Arc<()>);

    impl SessionVar for SessionMarker {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn binding_shares_the_command_session_and_bounds_each_sink_mode() {
        let state = state(&[]);
        let marker = Arc::new(());
        state.session().register(SessionMarker(Arc::clone(&marker)));

        let one = output_binding(&config(OpenSinkMode::OneAtATime), &state);
        let many = output_binding(&config(OpenSinkMode::Multiple), &state);

        assert_eq!(one.queue_depth, SINGLE_SINK_QUEUE_DEPTH);
        assert_eq!(many.queue_depth, MULTIPLE_SINK_QUEUE_DEPTH);
        assert!(Arc::ptr_eq(
            &one.session.get_opt::<SessionMarker>().unwrap().0,
            &marker
        ));
        assert!(Arc::ptr_eq(
            &many.session.get_opt::<SessionMarker>().unwrap().0,
            &marker
        ));
    }

    #[tokio::test]
    async fn sink_coalesces_batches_and_finishes_one_durable_vortex_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("output.vortex");
        let state = state(&["--vortex-record-batch-size", "4"]);
        let binding = output_binding(&config(OpenSinkMode::OneAtATime), &state);
        let first = TestBatch::simple_with(&[1, 2], &["a", "b"]);
        let second = TestBatch::simple_with(&[3, 4, 5], &["c", "d", "e"]);
        let mut sink = binding
            .open_sink(prepared_local_output(&path), first.schema())
            .await
            .unwrap();

        sink.write_batch(first).await.unwrap();
        sink.write_batch(second).await.unwrap();
        let completion = sink.finish().await.unwrap();

        assert_eq!(completion.rows_written(), 5);
        assert_eq!(completion.durable_locations().len(), 1);
        let file = state
            .session()
            .open_options()
            .open_path(&path)
            .await
            .unwrap();
        assert_eq!(file.row_count(), 5);
    }

    async fn controlled_handle(storage: &StorageSession, name: &str) -> StorageHandle {
        storage
            .prepare_output_target(
                &LocationInput::parse(format!("tracking://bucket/{name}")).unwrap(),
                &OutputPreparation::new(ExistingOutput::Allow, false),
            )
            .await
            .unwrap()
    }

    async fn drive_to_active_part(
        sink: &mut dyn DataSink,
        handle: &StorageHandle,
        store: &silk_chiffon_test_support::controlled_upload::ControlledUploadStore,
    ) {
        let active_before = store.active_parts();
        let batch = TestBatch::simple();
        for _ in 0..64 {
            if store.active_parts() > active_before {
                break;
            }

            let write = sink.write_batch(batch.clone());
            tokio::pin!(write);
            tokio::select! {
                result = &mut write => result.unwrap(),
                result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    store.wait_for_more_active_parts(active_before),
                ) => {
                    result.unwrap_or_else(|_| {
                        panic!(
                            "Vortex did not start a multipart upload for {}",
                            handle.url()
                        )
                    });
                    break;
                }
            }
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.wait_for_more_active_parts(active_before),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Vortex did not start a multipart upload for {}",
                handle.url()
            )
        });
    }

    #[tokio::test]
    async fn abort_releases_a_backpressured_multipart_upload() {
        use silk_chiffon_test_support::controlled_upload::{
            controlled_upload_lock, controlled_upload_storage, controlled_upload_store,
        };

        let _guard = controlled_upload_lock().await;
        let storage = controlled_upload_storage();
        let store = controlled_upload_store();
        let aborts = store.aborts();
        let _blocked = store.block_parts();
        let batch = TestBatch::simple();
        let state = state(&["--vortex-record-batch-size", "1"]);
        let binding = output_binding(&config(OpenSinkMode::Multiple), &state);
        let handle = controlled_handle(&storage, "vortex-abort").await;
        let mut sink = binding
            .open_sink(handle.clone(), batch.schema())
            .await
            .unwrap();

        drive_to_active_part(sink.as_mut(), &handle, &store).await;

        sink.abort().await.unwrap();

        assert_eq!(store.active_parts(), 0);
        assert_eq!(store.aborts(), aborts + 1);
        assert!(matches!(
            store.head(handle.object_path()).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}
