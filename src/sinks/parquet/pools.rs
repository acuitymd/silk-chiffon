//! Rayon threadpools for parquet encoding and I/O.
//!
//! Provides bounded threadpools instead of tokio's unbounded spawn_blocking,
//! giving us control over the number of threads used for CPU-bound encoding
//! work and blocking I/O operations.

use anyhow::{Context, Result};
use rayon::ThreadPool;

pub struct ParquetPools {
    pub encoding: ThreadPool,
    pub io: ThreadPool,
}

impl ParquetPools {
    pub fn new(encoding_threads: usize, io_threads: usize) -> Result<Self> {
        assert!(encoding_threads > 0, "encoding_threads must be > 0");
        assert!(io_threads > 0, "io_threads must be > 0");
        Ok(Self {
            encoding: rayon::ThreadPoolBuilder::new()
                .num_threads(encoding_threads)
                .thread_name(|i| format!("silk-encode-{i}"))
                .build()
                .context("failed to create encoding pool")?,
            io: rayon::ThreadPoolBuilder::new()
                .num_threads(io_threads)
                .thread_name(|i| format!("silk-io-{i}"))
                .build()
                .context("failed to create I/O pool")?,
        })
    }

    pub fn default_pools() -> Result<Self> {
        let cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        Self::new(cpus, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::rayon_bridge::spawn_on_pool;

    #[test]
    fn test_pools_creation() {
        let pools = ParquetPools::new(2, 2).unwrap();
        assert_eq!(pools.encoding.current_num_threads(), 2);
        assert_eq!(pools.io.current_num_threads(), 2);
    }

    #[test]
    fn test_default_pools() {
        let pools = ParquetPools::default_pools().unwrap();
        let cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        assert_eq!(pools.encoding.current_num_threads(), cpus);
        assert_eq!(pools.io.current_num_threads(), 1);
    }

    #[tokio::test]
    async fn test_pools_work_independently() {
        let pools = ParquetPools::new(2, 2).unwrap();

        let encoding_result = spawn_on_pool(&pools.encoding, || {
            let mut sum = 0u64;
            for i in 0..1000 {
                sum += i;
            }
            sum
        })
        .await
        .unwrap();
        assert_eq!(encoding_result, 499500);

        let io_result = spawn_on_pool(&pools.io, || "io_work".to_string())
            .await
            .unwrap();
        assert_eq!(io_result, "io_work");
    }
}
