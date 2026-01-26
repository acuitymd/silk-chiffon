//! Dedicated runtimes for parquet encoding and I/O.
//!
//! Provides separate tokio runtimes for CPU-intensive encoding work and
//! blocking I/O operations. Based on DataFusion's thread pool separation pattern.
//! This keeps the main async runtime free for coordination.

use std::sync::Arc;

use anyhow::Result;
use tokio::runtime::Handle;
use tokio::sync::Notify;

/// Dedicated tokio runtime for CPU-bound parquet encoding.
///
/// Tasks spawned via `handle()` are tokio tasks - cancellable via JoinSet
/// and can use async internally. This is preferable to rayon for work
/// that needs async coordination or cancellation.
pub struct CpuRuntime {
    handle: Handle,
    notify_shutdown: Arc<Notify>,
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for CpuRuntime {
    fn drop(&mut self) {
        self.notify_shutdown.notify_one();
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            let _ = thread_join_handle.join();
        }
    }
}

impl CpuRuntime {
    /// Create a new runtime with the specified number of worker threads.
    pub fn try_new(num_threads: usize) -> Result<Self> {
        let cpu_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .enable_time()
            .thread_name("parquet-encode")
            .build()?;

        let handle = cpu_runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        let thread_join_handle = std::thread::spawn(move || {
            cpu_runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
        });

        Ok(Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }

    /// Get a handle for spawning tasks on this runtime.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

/// Dedicated tokio runtime for blocking I/O operations.
///
/// Uses a smaller thread pool (typically 1 thread) for file I/O operations
/// that would otherwise block the main runtime.
pub struct IoRuntime {
    handle: Handle,
    notify_shutdown: Arc<Notify>,
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for IoRuntime {
    fn drop(&mut self) {
        self.notify_shutdown.notify_one();
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            let _ = thread_join_handle.join();
        }
    }
}

impl IoRuntime {
    /// Create a new runtime with the specified number of worker threads.
    pub fn try_new(num_threads: usize) -> Result<Self> {
        let io_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .enable_time()
            .thread_name("parquet-io")
            .build()?;

        let handle = io_runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        let thread_join_handle = std::thread::spawn(move || {
            io_runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
        });

        Ok(Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }

    /// Get a handle for spawning tasks on this runtime.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

/// Combined runtimes for the parquet pipeline.
///
/// - `cpu`: for CPU-bound encoding work (default: num_cpus threads)
/// - `io`: for blocking file I/O (default: 1 thread)
pub struct ParquetRuntimes {
    pub cpu: CpuRuntime,
    pub io: IoRuntime,
}

impl ParquetRuntimes {
    /// Create runtimes with specified thread counts.
    pub fn try_new(cpu_threads: usize, io_threads: usize) -> Result<Self> {
        Ok(Self {
            cpu: CpuRuntime::try_new(cpu_threads)?,
            io: IoRuntime::try_new(io_threads)?,
        })
    }

    /// Create with default thread counts (num_cpus for CPU, 1 for I/O).
    pub fn try_default() -> Result<Self> {
        let cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        Self::try_new(cpus, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_cpu_runtime_basic() {
        let rt = CpuRuntime::try_new(2).unwrap();

        let mut tasks = JoinSet::new();
        tasks.spawn_on(
            async {
                let sum: u64 = (0..1000).sum();
                sum
            },
            rt.handle(),
        );

        let result = tasks.join_next().await.unwrap().unwrap();
        assert_eq!(result, 499500);
    }

    #[tokio::test]
    async fn test_cpu_runtime_parallel() {
        let rt = CpuRuntime::try_new(4).unwrap();

        let mut tasks = JoinSet::new();
        for i in 0..10 {
            tasks.spawn_on(
                async move {
                    let sum: u64 = (0..1000).map(|x| x * i).sum();
                    sum
                },
                rt.handle(),
            );
        }

        let mut results = vec![];
        while let Some(result) = tasks.join_next().await {
            results.push(result.unwrap());
        }
        assert_eq!(results.len(), 10);
    }

    #[tokio::test]
    async fn test_cpu_runtime_cancellation() {
        let rt = CpuRuntime::try_new(2).unwrap();

        let mut tasks = JoinSet::new();
        tasks.spawn_on(
            async {
                loop {
                    tokio::task::yield_now().await;
                }
            },
            rt.handle(),
        );

        tasks.abort_all();

        let result = tasks.join_next().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_io_runtime_basic() {
        let rt = IoRuntime::try_new(1).unwrap();

        let mut tasks = JoinSet::new();
        tasks.spawn_on(async { "io_done".to_string() }, rt.handle());

        let result = tasks.join_next().await.unwrap().unwrap();
        assert_eq!(result, "io_done");
    }

    #[tokio::test]
    async fn test_parquet_runtimes() {
        let runtimes = ParquetRuntimes::try_new(2, 1).unwrap();

        let mut cpu_tasks = JoinSet::new();
        cpu_tasks.spawn_on(
            async {
                let sum: u64 = (0..1000).sum();
                sum
            },
            runtimes.cpu.handle(),
        );

        let mut io_tasks = JoinSet::new();
        io_tasks.spawn_on(async { "io_done" }, runtimes.io.handle());

        assert_eq!(cpu_tasks.join_next().await.unwrap().unwrap(), 499500);
        assert_eq!(io_tasks.join_next().await.unwrap().unwrap(), "io_done");
    }

    #[test]
    fn test_default_runtimes() {
        let runtimes = ParquetRuntimes::try_default().unwrap();
        drop(runtimes);
    }
}
