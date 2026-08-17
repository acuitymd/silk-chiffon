//! Dedicated runtimes for Parquet encoding and writing.
//!
//! Encoding is CPU-heavy, while the synchronous writer may wait on bounded
//! upload backpressure. Separate runtimes prevent either role from starving the
//! other or consuming the host's DataFusion workers.

use std::sync::Arc;

use anyhow::Result;
use tokio::{runtime::Handle, sync::Notify};

struct DedicatedRuntime {
    handle: Handle,
    shutdown: Arc<Notify>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl DedicatedRuntime {
    fn try_new(threads: usize, thread_name: &'static str) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_time()
            .thread_name(thread_name)
            .build()?;
        let handle = runtime.handle().clone();
        let shutdown = Arc::new(Notify::new());
        let wait_for_shutdown = Arc::clone(&shutdown);
        let thread = std::thread::spawn(move || {
            runtime.block_on(wait_for_shutdown.notified());
        });
        Ok(Self {
            handle,
            shutdown,
            thread: Some(thread),
        })
    }

    fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl Drop for DedicatedRuntime {
    fn drop(&mut self) {
        self.shutdown.notify_one();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(super) struct OutputRuntimes {
    encoding: DedicatedRuntime,
    writing: DedicatedRuntime,
}

impl OutputRuntimes {
    pub(super) fn try_new(encoding_threads: usize, writing_threads: usize) -> Result<Self> {
        Ok(Self {
            encoding: DedicatedRuntime::try_new(encoding_threads, "parquet-encoding")?,
            writing: DedicatedRuntime::try_new(writing_threads, "parquet-writing")?,
        })
    }

    pub(super) fn encoding(&self) -> &Handle {
        self.encoding.handle()
    }

    pub(super) fn writing(&self) -> &Handle {
        self.writing.handle()
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::JoinSet;

    use super::*;

    #[tokio::test]
    async fn roles_run_on_independent_runtimes() {
        let runtimes = OutputRuntimes::try_new(2, 1).unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn_on(async { 42 }, runtimes.encoding());
        assert_eq!(tasks.join_next().await.unwrap().unwrap(), 42);
        tasks.spawn_on(async { 7 }, runtimes.writing());
        assert_eq!(tasks.join_next().await.unwrap().unwrap(), 7);
    }

    #[tokio::test]
    async fn spawned_work_can_be_cancelled() {
        let runtimes = OutputRuntimes::try_new(1, 1).unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn_on(std::future::pending::<()>(), runtimes.encoding());
        tasks.abort_all();
        assert!(tasks.join_next().await.unwrap().is_err());
    }
}
