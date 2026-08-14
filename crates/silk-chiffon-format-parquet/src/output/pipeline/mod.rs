//! Concurrent Parquet encoding pipeline.

mod analysis;
mod config;
pub(crate) mod encoding;
mod stages;

use std::{future::Future, sync::Arc};

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use parquet::file::properties::WriterProperties;
use silk_chiffon_storage::{ObjectUpload, ObjectUploadTask, PreparedOutputTarget};
use tokio::{runtime::Handle, sync::mpsc, task::JoinSet};
use tokio_util::{
    sync::CancellationToken,
    task::{AbortOnDropHandle, TaskTracker},
};

pub use config::PipelineConfig;

use crate::output::OutputRuntimes;
use stages::{PipelineSetup, run_pipeline};

#[derive(Clone)]
struct PipelineTaskScope {
    cancellation: CancellationToken,
    tracker: TaskTracker,
}

impl PipelineTaskScope {
    fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            tracker: TaskTracker::new(),
        }
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    async fn wait(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }

    fn spawn_stage<F>(&self, tasks: &mut JoinSet<Result<()>>, future: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let cancellation = self.cancellation.clone();
        tasks.spawn(self.tracker.track_future(async move {
            cancellation
                .run_until_cancelled(future)
                .await
                .unwrap_or(Ok(()))
        }));
    }

    fn spawn<F>(&self, future: F) -> AbortOnDropHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        AbortOnDropHandle::new(self.tracker.spawn(future))
    }

    fn spawn_on<F>(&self, future: F, handle: &Handle) -> AbortOnDropHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        AbortOnDropHandle::new(self.tracker.spawn_on(future, handle))
    }

    fn spawn_in_on<F>(&self, tasks: &mut JoinSet<F::Output>, future: F, handle: &Handle)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tasks.spawn_on(self.tracker.track_future(future), handle);
    }
}

pub(super) fn start_pipeline(
    target: PreparedOutputTarget,
    schema: &SchemaRef,
    base_properties: WriterProperties,
    runtimes: Arc<OutputRuntimes>,
    config: PipelineConfig,
) -> (mpsc::Sender<RecordBatch>, ObjectUploadTask<u64>) {
    let (ingestion_sender, ingestion_receiver) = mpsc::channel(config.ingestion_queue_size);
    let mut upload = ObjectUpload::new(target);
    let writer = upload
        .blocking_writer()
        .expect("a new object upload accepts one byte writer");
    let task = ObjectUploadTask::spawn("Parquet writer", upload, {
        let schema = Arc::clone(schema);
        move |cancellation| {
            tokio::spawn(run_pipeline(PipelineSetup {
                writer,
                schema,
                base_props: base_properties,
                runtimes,
                config,
                ingestion_rx: ingestion_receiver,
                scope: PipelineTaskScope::new(cancellation),
            }))
        }
    });
    (ingestion_sender, task)
}
