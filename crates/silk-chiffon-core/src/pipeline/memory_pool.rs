//! A memory pool that serves consumers that cannot spill before those that can.
//!
//! DataFusion's built-in `FairSpillPool` divides memory evenly among spillable
//! consumers, and refuses a non-spillable consumer whenever the pool is full.
//! A sort that has filled the pool with data it could write to disk therefore
//! makes an input reader, or the sort's own merge, fail for lack of a few
//! megabytes. This pool grants those requests instead and takes the memory
//! back from the spillable consumers, which are refused their next request and
//! spill.

use datafusion::common::{Result, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use parking_lot::Mutex;
use std::fmt::{self, Display, Formatter};

/// Non-spillable consumers may push the pool this far over its size, as a
/// divisor of the size, while spillable consumers still hold memory they were
/// granted earlier. The transform sizes the pool below its memory budget, which
/// leaves room for this.
const OVERDRAFT_DIVISOR: usize = 4;

/// A [`MemoryPool`] that guarantees a portion of capacity for non-spillable consumers.
///
/// Memory is partitioned into two regions:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │                        pool_size                                    │
/// │                                                                     │
/// │  ┌─────────────────────────────┐  ┌───────────────────────────────┐ │
/// │  │   spillable region          │  │  non-spillable reserve        │ │
/// │  │   (pool_size - reserve)     │  │  (reserve_for_non_spillable)  │ │
/// │  │                             │  │                               │ │
/// │  │  spillable consumers share  │  │                               │ │
/// │  │  this region evenly         │  │  non-spillable only           │ │
/// │  └─────────────────────────────┘  └───────────────────────────────┘ │
/// └─────────────────────────────────────────────────────────────────────┘
/// ```
///
/// - **Spillable consumers** can only use `pool_size - reserve_for_non_spillable`,
///   divided evenly among all registered spillable consumers (like `FairSpillPool`).
///   The region shrinks while non-spillable consumers hold more than the reserve.
/// - **Non-spillable consumers** can use the entire pool, first-come first-served.
///   When spillable consumers already hold part of it, a non-spillable request is
///   still granted, up to a quarter of the pool over its size. The spillable
///   consumers are then over their share, so their next request is refused and
///   they spill.
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
    /// Create a new pool with `pool_size` total bytes and
    /// `reserve_for_non_spillable` bytes reserved exclusively for non-spillable
    /// consumers.
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
    fn name(&self) -> &str {
        "ReservedSpillPool"
    }

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
                // non-spillable consumers can use the full pool, and may
                // overdraw it against memory spillable consumers will give back
                let overdraft = if state.spillable > 0 {
                    self.pool_size / OVERDRAFT_DIVISOR
                } else {
                    0
                };
                let available = (self.pool_size + overdraft)
                    .saturating_sub(state.non_spillable + state.spillable)
                    .min(self.pool_size.saturating_sub(state.non_spillable));

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

impl Display for ReservedSpillPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({} total, {} non-spillable reserve)",
            self.name(),
            human_readable_size(self.pool_size),
            human_readable_size(self.reserve_for_non_spillable)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn non_spillable_can_use_reserve() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.try_grow(80).unwrap();

        let ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(20).unwrap();

        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn spillable_cannot_use_reserve() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);

        s1.try_grow(80).unwrap();

        let err = s1.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn fair_sharing_among_spillable() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let s1 = MemoryConsumer::new("s1")
            .with_can_spill(true)
            .register(&pool);
        let s2 = MemoryConsumer::new("s2")
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

        let s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.try_grow(60).unwrap();

        // non-spillable can use reserve + whatever spillable hasn't claimed
        let ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(40).unwrap(); // 100 - 60 = 40 available

        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn grow_and_shrink_tracking() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let s1 = MemoryConsumer::new("spiller")
            .with_can_spill(true)
            .register(&pool);
        s1.grow(50);
        assert_eq!(pool.reserved(), 50);

        s1.shrink(30);
        assert_eq!(pool.reserved(), 20);

        let ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.grow(10);
        assert_eq!(pool.reserved(), 30);

        ns1.shrink(5);
        assert_eq!(pool.reserved(), 25);
    }

    #[test]
    fn spillable_share_adjusts_on_drop() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let s1 = MemoryConsumer::new("s1")
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
    fn non_spillable_denied_when_nothing_can_spill() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(100).unwrap();

        // no spillable consumer holds memory it could give back
        let ns2 = MemoryConsumer::new("merger2").register(&pool);
        let err = ns2.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn non_spillable_overdraws_a_pool_that_spillable_consumers_filled() {
        let pool = Arc::new(ReservedSpillPool::new(100, 0)) as Arc<dyn MemoryPool>;

        let sorter = MemoryConsumer::new("sorter")
            .with_can_spill(true)
            .register(&pool);
        sorter.try_grow(100).unwrap();

        // the reader is served although the pool is full
        let reader = MemoryConsumer::new("reader").register(&pool);
        reader.try_grow(20).unwrap();
        assert_eq!(pool.reserved(), 120);

        // which puts the sorter over its share, so it is refused until it spills
        let err = sorter.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
        sorter.shrink(100);
        sorter.try_grow(80).unwrap();
        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn the_overdraft_is_a_quarter_of_the_pool() {
        let pool = Arc::new(ReservedSpillPool::new(100, 0)) as Arc<dyn MemoryPool>;

        let sorter = MemoryConsumer::new("sorter")
            .with_can_spill(true)
            .register(&pool);
        sorter.try_grow(100).unwrap();

        let reader = MemoryConsumer::new("reader").register(&pool);
        reader.try_grow(25).unwrap();
        let err = reader.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn spillable_adjusts_when_non_spillable_exceeds_reserve() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        // non-spillable uses 40 (more than the 20 reserve)
        let ns1 = MemoryConsumer::new("merger").register(&pool);
        ns1.try_grow(40).unwrap();

        // spillable should only get (100 - 40) / 1 = 60, not (100 - 20) / 1 = 80
        let s1 = MemoryConsumer::new("spiller")
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
