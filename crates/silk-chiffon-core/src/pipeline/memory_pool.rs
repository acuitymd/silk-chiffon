//! A memory pool that keeps part of itself free for consumers that cannot spill.
//!
//! DataFusion's built-in `FairSpillPool` divides memory evenly among spillable
//! consumers, and refuses a non-spillable consumer whenever the pool is full.
//! A sort that has filled the pool with data it could write to disk therefore
//! makes an input reader, or the sort's own merge, fail for lack of a few
//! megabytes. This pool keeps a reserve free beside what non-spillable
//! consumers already hold, so those requests find room.
//!
//! The pool's size is a limit: what it grants through `try_grow` never adds up
//! to more. A consumer that cannot spill is refused when the pool is full, and
//! should be given a reserve large enough that it is not. A consumer that can
//! spill is refused past its share, and spills. One that holds nothing has
//! nothing to spill, so it may take what is free, the reserve included, up to
//! its share. `grow`, which some DataFusion operators call where they cannot
//! take a refusal, is counted and never refused.

use datafusion::common::{Result, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

/// A [`MemoryPool`] that guarantees a portion of capacity for non-spillable consumers.
///
/// Memory is partitioned into two regions:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │                              pool_size                              │
/// │                                                                     │
/// │  ┌───────────────────────┐ ┌───────────────────┐ ┌───────────────┐  │
/// │  │ spillable region      │ │ held by           │ │ reserve       │  │
/// │  │                       │ │ non-spillable     │ │ (kept free)   │  │
/// │  │ spillable consumers   │ │ consumers         │ │               │  │
/// │  │ share it evenly       │ │                   │ │               │  │
/// │  └───────────────────────┘ └───────────────────┘ └───────────────┘  │
/// └─────────────────────────────────────────────────────────────────────┘
/// ```
///
/// - **Spillable consumers** share what is left of the pool after the reserve
///   and what non-spillable consumers hold, divided evenly among all
///   registered spillable consumers (like `FairSpillPool`). A consumer's share
///   counts every reservation it has, including ones split off another, and
///   the region is never granted past its size, even when a consumer that
///   registered later finds the others over their new shares. The exception is
///   a consumer that holds nothing: it has nothing to spill, so a refusal would
///   fail it, and it may take what is free anywhere in the pool, up to its share.
/// - **Non-spillable consumers** can use whatever of the pool is free,
///   first-come first-served. What they take shrinks the spillable region, so
///   the reserve is free again once the spillable consumers give back what is
///   over their shares. `try_grow` gives a spillable consumer none of the
///   reserve unless it holds nothing, and `grow`, which is never refused, can
///   take any of it.
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
    /// What each spillable consumer holds, by consumer id, across all of its
    /// reservations.
    held: HashMap<usize, usize>,
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
                held: HashMap::new(),
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
            let mut state = self.state.lock();
            state.num_spill += 1;
            state.held.insert(consumer.id(), 0);
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            let mut state = self.state.lock();
            state.num_spill = state.num_spill.checked_sub(1).unwrap();
            state.held.remove(&consumer.id());
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        let consumer = reservation.consumer();
        if consumer.can_spill() {
            state.spillable += additional;
            *state.held.entry(consumer.id()).or_default() += additional;
        } else {
            state.non_spillable += additional;
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        let consumer = reservation.consumer();
        if consumer.can_spill() {
            state.spillable = state
                .spillable
                .checked_sub(shrink)
                .expect("spillable underflow: shrink exceeds tracked spillable usage");
            let held = state.held.entry(consumer.id()).or_default();
            *held = held
                .checked_sub(shrink)
                .expect("spillable underflow: shrink exceeds what the consumer holds");
        } else {
            state.non_spillable = state
                .non_spillable
                .checked_sub(shrink)
                .expect("non-spillable underflow: shrink exceeds tracked non-spillable usage");
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();
        let consumer = reservation.consumer();

        if consumer.can_spill() {
            // the reserve stays free beside what non-spillable consumers
            // already hold, so a reader that asks for more finds room while
            // the sorts give back what is now over their shares
            let spill_capacity = self
                .pool_size
                .saturating_sub(state.non_spillable + self.reserve_for_non_spillable);
            let per_consumer = spill_capacity
                .checked_div(state.num_spill)
                .unwrap_or(spill_capacity);
            let held = state.held.get(&consumer.id()).copied().unwrap_or(0);
            // a consumer that holds nothing has nothing to spill, so a refusal
            // fails it: it may take what is free anywhere in the pool. One
            // that registered while the others filled the region would
            // otherwise fail before they give back what is over their shares
            let free = if held == 0 {
                self.pool_size
                    .saturating_sub(state.spillable + state.non_spillable)
            } else {
                spill_capacity.saturating_sub(state.spillable)
            };
            let available = per_consumer.saturating_sub(held).min(free);

            if additional > available {
                return Err(resources_datafusion_err!(
                    "Failed to allocate additional {} for {} with {} already allocated \
                     for this consumer - {} remain available for this consumer's \
                     fair share ({} per consumer, {} spillable capacity)",
                    human_readable_size(additional),
                    consumer.name(),
                    human_readable_size(held),
                    human_readable_size(available),
                    human_readable_size(per_consumer),
                    human_readable_size(spill_capacity)
                ));
            }
            state.spillable += additional;
            *state.held.entry(consumer.id()).or_default() += additional;
        } else {
            let available = self
                .pool_size
                .saturating_sub(state.non_spillable + state.spillable);

            if available < additional {
                return Err(resources_datafusion_err!(
                    "Failed to allocate additional {} for {} with {} already allocated \
                     for this reservation - {} remain available for the total pool",
                    human_readable_size(additional),
                    consumer.name(),
                    human_readable_size(reservation.size()),
                    human_readable_size(available)
                ));
            }
            state.non_spillable += additional;
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

        // the pool is full
        let ns2 = MemoryConsumer::new("merger2").register(&pool);
        let err = ns2.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
    }

    #[test]
    fn non_spillable_is_refused_when_spillable_consumers_filled_the_pool() {
        let pool = Arc::new(ReservedSpillPool::new(100, 0)) as Arc<dyn MemoryPool>;

        let sorter = MemoryConsumer::new("sorter")
            .with_can_spill(true)
            .register(&pool);
        sorter.try_grow(100).unwrap();

        let reader = MemoryConsumer::new("reader").register(&pool);
        let err = reader.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
        assert_eq!(pool.reserved(), 100);
    }

    #[test]
    fn a_share_counts_every_reservation_of_a_consumer() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let sorter = MemoryConsumer::new("sorter")
            .with_can_spill(true)
            .register(&pool);
        sorter.try_grow(60).unwrap();
        let lent = sorter.split(60);
        assert_eq!(sorter.size(), 0);

        // the 60 handed to a job still counts against the sorter's 80
        sorter.try_grow(20).unwrap();
        let err = sorter.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");

        drop(lent);
        sorter.try_grow(60).unwrap();
        assert_eq!(pool.reserved(), 80);
    }

    #[test]
    fn spillable_consumers_never_take_the_pool_past_its_size() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        let first = MemoryConsumer::new("first")
            .with_can_spill(true)
            .register(&pool);
        first.try_grow(80).unwrap();

        // the second's share is 40 and it holds nothing, so it may start in
        // the reserve, but no further than the pool's size
        let second = MemoryConsumer::new("second")
            .with_can_spill(true)
            .register(&pool);
        let err = second.try_grow(21).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
        second.try_grow(20).unwrap();
        assert_eq!(pool.reserved(), 100);

        // holding something, it waits for the first to give back what is
        // over its share
        let err = second.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");
        first.shrink(40);
        second.try_grow(20).unwrap();
        assert_eq!(pool.reserved(), 80);
    }

    #[test]
    fn the_reserve_stays_free_beside_what_non_spillable_consumers_hold() {
        let pool = Arc::new(ReservedSpillPool::new(100, 20)) as Arc<dyn MemoryPool>;

        // merges that hold more than the reserve
        let merges = MemoryConsumer::new("merges").register(&pool);
        merges.try_grow(40).unwrap();

        let sorter = MemoryConsumer::new("sorter")
            .with_can_spill(true)
            .register(&pool);
        sorter.try_grow(40).unwrap();
        let err = sorter.try_grow(1).unwrap_err().strip_backtrace();
        assert!(err.contains("Failed to allocate"), "{err}");

        let reader = MemoryConsumer::new("reader").register(&pool);
        reader.try_grow(20).unwrap();
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
