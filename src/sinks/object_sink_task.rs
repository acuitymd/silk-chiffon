use anyhow::{Context, Result};
use silk_chiffon_storage::ObjectUpload;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::with_cleanup_error;

fn task_error_after_cancel<T>(
    was_finished: bool,
    result: Result<anyhow::Result<T>, JoinError>,
) -> Option<anyhow::Error> {
    match result {
        Err(error) if error.is_cancelled() => None,
        Err(error) => Some(anyhow::Error::new(error)),
        Ok(Err(error)) if was_finished => Some(error),
        // Cancellation can make a running codec return an error.
        Ok(_) => None,
    }
}

/// Owns one format task and the object upload fed by that task.
///
/// Explicit completion and cancellation always join the format task and settle
/// the upload. Dropping the owner is only a best-effort fallback because async
/// cleanup cannot be awaited from `Drop`.
pub(crate) struct ObjectSinkTask<T> {
    task_name: &'static str,
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<T>>>,
    upload: Option<ObjectUpload>,
}

impl<T> ObjectSinkTask<T>
where
    T: Send + 'static,
{
    pub(crate) fn spawn<F>(task_name: &'static str, upload: ObjectUpload, spawn_task: F) -> Self
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

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) async fn finish(mut self) -> Result<(T, Url)> {
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

    pub(crate) async fn abort(mut self) -> Result<()> {
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

impl<T> Drop for ObjectSinkTask<T> {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.take();
        self.upload.take();
    }
}
