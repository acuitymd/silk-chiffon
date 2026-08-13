//! Producer task and upload lifecycle for one encoded object.

use std::fmt;

use anyhow::{Context, Error, Result};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::ObjectUpload;

fn with_cleanup_error(primary: Error, cleanup: Option<Error>) -> Error {
    match cleanup {
        Some(cleanup) => Error::new(PrimaryWithCleanup { primary, cleanup }),
        None => primary,
    }
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

fn task_error_after_cancel<T>(
    was_finished: bool,
    result: Result<Result<T>, JoinError>,
) -> Option<anyhow::Error> {
    match result {
        Err(error) if error.is_cancelled() => None,
        Err(error) => Some(anyhow::Error::new(error)),
        Ok(Err(error)) if was_finished => Some(error),
        // A running producer may report its own error after observing cancellation.
        Ok(_) => None,
    }
}

/// Owns one format producer task and the object upload fed by that task.
///
/// Formats use this owner after creating their bounded channel or writer bridge. The format keeps
/// responsibility for encoding and its producer result, while this type couples producer shutdown
/// to the upload's exactly-once completion or abort. Explicit terminal operations always join the
/// producer and settle the upload; dropping the owner can only request nonblocking cleanup.
pub struct ObjectUploadTask<T> {
    task_name: &'static str,
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<T>>>,
    upload: Option<ObjectUpload>,
}

impl<T> ObjectUploadTask<T>
where
    T: Send + 'static,
{
    /// Starts a producer with the cancellation token paired to its upload.
    pub fn spawn<F>(task_name: &'static str, upload: ObjectUpload, spawn_task: F) -> Self
    where
        F: FnOnce(CancellationToken) -> JoinHandle<Result<T>>,
    {
        let cancellation = CancellationToken::new();
        let task = spawn_task(cancellation.clone());
        Self {
            task_name,
            cancellation,
            task: Some(task),
            upload: Some(upload),
        }
    }

    /// Returns the token that coordinates producer and caller cancellation.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Joins the producer, then completes its upload and returns both results.
    pub async fn finish(mut self) -> Result<(T, Url)> {
        let task = self.task.take().expect("task exists until finish");
        let upload = self.upload.take().expect("upload exists until finish");
        let task_result = task
            .await
            .with_context(|| format!("{} task panicked", self.task_name))
            .and_then(|result| result);
        let value = match task_result {
            Ok(value) => value,
            Err(primary) => {
                let cleanup = upload.abort().await.err().map(anyhow::Error::new);
                return Err(with_cleanup_error(primary, cleanup));
            }
        };
        let url = upload.complete().await?;
        Ok((value, url))
    }

    /// Cancels and joins the producer while aborting its upload.
    pub async fn abort(mut self) -> Result<()> {
        let task = self.task.take().expect("task exists until abort");
        let was_finished = task.is_finished();
        self.cancellation.cancel();
        let upload = self.upload.take().expect("upload exists until abort");
        let (task_result, upload_result) = tokio::join!(task, upload.abort());
        let task_error = task_error_after_cancel(was_finished, task_result);
        let upload_error = upload_result.err().map(anyhow::Error::new);
        match task_error {
            Some(primary) => Err(with_cleanup_error(primary, upload_error)),
            None => upload_error.map_or(Ok(()), Err),
        }
    }
}

impl<T> Drop for ObjectUploadTask<T> {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.take();
        self.upload.take();
    }
}
