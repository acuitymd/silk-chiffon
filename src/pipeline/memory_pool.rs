//! A memory pool that reserves capacity for non-spillable consumers.
//!
//! DataFusion's built-in `FairSpillPool` divides memory evenly among spillable consumers,
//! but when spillable consumers have already consumed most of the pool, non-spillable
//! consumers (like sort merge phases) can fail to allocate even small amounts. This pool
//! solves that by maintaining a reserve that only non-spillable consumers can use.

use datafusion::common::{Result, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use parking_lot::Mutex;

/// A [`MemoryPool`] that guarantees a portion of capacity for non-spillable consumers.
///
/// Memory is partitioned into two regions:
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────┐
/// │                        pool_size                             │
/// │                                                              │
/// │  ┌─────────────────────────────┐  ┌────────────────────────┐ │
/// │  │   spillable region          │  │  non-spillable reserve │ │
/// │  │   (pool_size - reserve)     │  │  (reserve_for_         │ │
/// │  │                             │  │   non_spillable)       │ │
/// │  │  spillable consumers share  │  │                        │ │
/// │  │  this region evenly         │  │  non-spillable only    │ │
/// │  └─────────────────────────────┘  └────────────────────────┘ │
/// └──────────────────────────────────────────────────────────────┘
/// ```
///
/// - **Spillable consumers** can only use `pool_size - reserve_for_non_spillable`,
///   divided evenly among all registered spillable consumers (like `FairSpillPool`).
/// - **Non-spillable consumers** can use the entire pool (spillable region + reserve),
///   allocated first-come first-served.
#[derive(Debug)]
pub struct ReservedSpillPool {
    pool_size: usize,
    /// memory that spillable consumers cannot touch
    reserve_for_non_spillable: usize,
    state: Mutex<PoolState>,
}

#[derive(Debug)]
struct PoolState {
    num_spill: usize,
    spillable: usize,
    non_spillable: usize,
}

impl ReservedSpillPool {
    /// Create a new pool with `pool_size` total bytes and `reserve_for_non_spillable` bytes
    /// reserved exclusively for non-spillable consumers.
    ///
    /// # Panics
    ///
    /// Panics if `pool_size` is 0 or `reserve_for_non_spillable >= pool_size`.
    pub fn new(pool_size: usize, reserve_for_non_spillable: usize) -> Self {
        assert!(pool_size > 0, "pool size must be greater than 0");
        assert!(
            reserve_for_non_spillable < pool_size,
            "non-spillable reserve ({}) must be less than pool size ({})",
            human_readable_size(reserve_for_non_spillable),
            human_readable_size(pool_size),
        );
        Self {
            pool_size,
            reserve_for_non_spillable,
            state: Mutex::new(PoolState {
                num_spill: 0,
                spillable: 0,
                non_spillable: 0,
            }),
        }
    }
}

impl MemoryPool for ReservedSpillPool {
    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            self.state.lock().num_spill += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            let mut state = self.state.lock();
            state.num_spill = state.num_spill.checked_sub(1).unwrap();
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        match reservation.consumer().can_spill() {
            true => state.spillable += additional,
            false => state.non_spillable += additional,
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        match reservation.consumer().can_spill() {
            true => {
                state.spillable = state
                    .spillable
                    .checked_sub(shrink)
                    .expect("spillable underflow: shrink exceeds tracked spillable usage");
            }
            false => {
                state.non_spillable = state
                    .non_spillable
                    .checked_sub(shrink)
                    .expect("non-spillable underflow: shrink exceeds tracked non-spillable usage");
            }
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();

        match reservation.consumer().can_spill() {
            true => {
                // spillable consumers share the spillable region evenly,
                // dynamically reduced when non-spillable usage exceeds the reserve
                let spill_capacity = self
                    .pool_size
                    .saturating_sub(state.non_spillable.max(self.reserve_for_non_spillable));
                let per_consumer = spill_capacity
                    .checked_div(state.num_spill)
                    .unwrap_or(spill_capacity);

                if reservation.size() + additional > per_consumer {
                    return Err(resources_datafusion_err!(
                        "Failed to allocate additional {} for {} with {} already allocated \
                         for this reservation - {} remain available for this consumer's \
                         fair share ({} per consumer, {} spillable capacity)",
                        human_readable_size(additional),
                        reservation.consumer().name(),
                        human_readable_size(reservation.size()),
                        human_readable_size(per_consumer.saturating_sub(reservation.size())),
                        human_readable_size(per_consumer),
                        human_readable_size(spill_capacity)
                    ));
                }
                state.spillable += additional;
            }
            false => {
                // non-spillable consumers can use the full pool
                let available = self
                    .pool_size
                    .saturating_sub(state.non_spillable + state.spillable);

                if available < additional {
                    return Err(resources_datafusion_err!(
                        "Failed to allocate additional {} for {} with {} already allocated \
                         for this reservation - {} remain available for the total pool",
                        human_readable_size(additional),
                        reservation.consumer().name(),
                        human_readable_size(reservation.size()),
                        human_readable_size(available)
                    ));
                }
                state.non_spillable += additional;
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock();
        state.spillable + state.non_spillable
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn non_spillable_can_use_reserve() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.try_grow(80).unwrap();

        let mut ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(20).unwrap();

        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn spillable_cannot_use_reserve() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);

        s1.try_grow(80).unwrap();

        let err = s1.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn fair_sharing_among_spillable() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("s1")
            .with_can_spill(true)
            .register(&pool);
        let mut s2 = MemoryConsumer::new("s2")
            .with_can_spill(true)
            .register(&pool);

        // each spillable consumer gets 80/2 = 40
        s1.try_grow(40).unwrap();
        s2.try_grow(40).unwrap();

        let err = s1.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn non_spillable_uses_freed_spillable_memory() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.try_grow(60).unwrap();

        // non-spillable can use reserve + whatever spillable hasn't claimed
        let mut ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(40).unwrap(); // 100 - 60 = 40 available

        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn grow_and_shrink_tracking() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.grow(50);
        assert_eq!(pool.reserved(), 50);

        s1.shrink(30);
        assert_eq!(pool.reserved(), 20);

        let mut ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.grow(10);
        assert_eq!(pool.reserved(), 30);

        ns1.shrink(5);
        assert_eq!(pool.reserved(), 25);
    }

    #[test]
    fn spillable_share_adjusts_on_drop() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("s1")
            .with_can_spill(true)
            .register(&pool);
        let s2 = MemoryConsumer::new("s2")
            .with_can_spill(true)
            .register(&pool);

        // 2 spillable consumers: each gets 40
        s1.try_grow(40).unwrap();
        let err = s1.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");

        // drop s2, now s1 gets the full 80
        drop(s2);
        s1.try_grow(40).unwrap();
        assert_eq!(s1.size(), 80);
    }

    #[test]
    fn non_spillable_denied_when_pool_full() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let mut s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.try_grow(80).unwrap();

        let mut ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(20).unwrap();

        // pool is full (100/100), second non-spillable allocation fails
        let mut ns2 = MemoryConsumer::new("merger2").register(&pool);
        let err = ns2.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn spillable_adjusts_when_non_spillable_exceeds_reserve() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        // non-spillable uses 40 (more than the 20 reserve)
        let mut ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(40).unwrap();

        // spillable should only get (100 - 40) / 1 = 60, not (100 - 20) / 1 = 80
        let mut s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.try_grow(60).unwrap();

        let err = s1.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");

        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    #[should_panic(expected = "non-spillable reserve")]
    fn panics_on_invalid_reserve() {
        ReservedSpillPool::new(100, 100);
    }

    #[test]
    #[should_panic(expected = "pool size must be greater than 0")]
    fn panics_on_zero_pool_size() {
        ReservedSpillPool::new(0, 0);
    }
}
