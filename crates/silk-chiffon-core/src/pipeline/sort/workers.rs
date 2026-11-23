//! The threads that the sort's CPU work runs on.
//!
//! The sort has jobs that run for tens to hundreds of milliseconds without a
//! place to yield: sorting a range, merging a slice, writing a spill file.
//! They run on a thread pool of the sort's own, so that they do not hold up
//! the async runtime that reads the input and writes the output.

use datafusion::error::{DataFusionError, Result};
use rayon::prelude::*;

pub(super) struct Workers {
    pool: rayon::ThreadPool,
}

impl Workers {
    pub(super) fn try_new(parallelism: usize) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism.max(1))
            .thread_name(|index| format!("silk-sort-{index}"))
            .build()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Self { pool })
    }

    /// How many jobs run at once.
    pub(super) fn parallelism(&self) -> usize {
        self.pool.current_num_threads()
    }

    /// Starts a job and does not wait for it.
    pub(super) fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        self.pool.spawn(job);
    }

    /// Calls `work` with every index below `count`, on every thread that is
    /// free, and returns when all are done.
    pub(super) fn each(&self, count: usize, work: &(dyn Fn(usize) + Sync)) {
        // installed, so that a caller outside the pool does not fan out on
        // rayon's global pool
        self.pool
            .install(|| (0..count).into_par_iter().for_each(work));
    }

    /// [`Self::each`], collecting what `work` returns in index order.
    pub(super) fn map<T: Send>(&self, count: usize, work: &(dyn Fn(usize) -> T + Sync)) -> Vec<T> {
        self.pool
            .install(|| (0..count).into_par_iter().map(work).collect())
    }
}
