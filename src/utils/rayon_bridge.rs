//! Bridge between rayon threadpools and tokio async code.
//!
//! Provides `spawn_on_pool` to run blocking work on a rayon threadpool
//! and await the result from async code via oneshot channel.

use anyhow::{Context, Result};
use rayon::ThreadPool;

/// Spawn blocking work on a rayon pool, return result via oneshot.
///
/// The closure runs on the given rayon threadpool. The result is sent
/// back through a tokio oneshot channel that the caller can await.
///
/// If the closure panics, the oneshot sender is dropped without sending,
/// causing this function to return an error ("rayon task panicked").
pub async fn spawn_on_pool<T, F>(pool: &ThreadPool, f: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    pool.spawn(move || {
        let _ = tx.send(f());
    });
    rx.await.context("rayon task panicked")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> ThreadPool {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_spawn_on_pool_success() {
        let pool = test_pool();
        let result = spawn_on_pool(&pool, || 42).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_spawn_on_pool_returns_result() {
        let pool = test_pool();

        let result: Result<i32> = spawn_on_pool(&pool, || Ok(42)).await.unwrap();
        assert_eq!(result.unwrap(), 42);

        let result: Result<i32> = spawn_on_pool(&pool, || Err(anyhow::anyhow!("inner error")))
            .await
            .unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_spawn_on_pool_captures_values() {
        let pool = test_pool();
        let data = [1, 2, 3, 4, 5];
        let result = spawn_on_pool(&pool, move || data.iter().sum::<i32>())
            .await
            .unwrap();
        assert_eq!(result, 15);
    }
}
