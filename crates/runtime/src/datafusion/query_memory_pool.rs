/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Query-operator memory pool: greedy, with a reserved slice that spillable
//! consumers (`ExternalSorter`, grouped hash aggregate) cannot take.
//!
//! `DataFusion`'s `GreedyMemoryPool` lets the first `try_grow` win. An
//! `ExternalSorter` (`can_spill: true`) only spills when that grow *fails*, so
//! a coalesced TPC-DS Q97 sort-merge held 103.6 GiB of a 107.50 GiB pool and
//! `cayenne_scan[store_sales, partition=2]` could not get 1 MiB (lab SF-100
//! `--validate`, 3d692f7d21). The cap is on *spillable* bytes, not total
//! used: capping total used let Q78 `HashJoinInput`s (unspillable) fill
//! `pool - headroom` and then starve `GroupedHashAggregateStream` (b51504b3bc).

use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::common::{Result, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryLimit, MemoryPool, MemoryReservation, TrackConsumersPool, human_readable_size,
};
use parking_lot::Mutex;

/// Spillable operators cannot consume this fraction of the pool, so a
/// `cayenne_scan` batch (1 MiB) can still allocate after a large sort.
const SPILLABLE_HEADROOM_DIVISOR: usize = 16;

/// Query pool used by `RuntimeEnv`: tracked consumers, spillable headroom.
#[must_use]
pub(crate) fn tracked_query_memory_pool(
    pool_size: usize,
    topn: NonZeroUsize,
) -> Arc<dyn MemoryPool> {
    Arc::new(TrackConsumersPool::new(
        GreedyPoolWithSpillHeadroom::new(pool_size),
        topn,
    ))
}

#[derive(Debug)]
struct PoolState {
    used: usize,
    spillable_used: usize,
}

#[derive(Debug)]
struct GreedyPoolWithSpillHeadroom {
    pool_size: usize,
    spillable_headroom: usize,
    state: Mutex<PoolState>,
}

impl GreedyPoolWithSpillHeadroom {
    fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            spillable_headroom: spillable_headroom_bytes(pool_size),
            state: Mutex::new(PoolState {
                used: 0,
                spillable_used: 0,
            }),
        }
    }

    fn spillable_cap(&self) -> usize {
        self.pool_size.saturating_sub(self.spillable_headroom)
    }
}

fn spillable_headroom_bytes(pool_size: usize) -> usize {
    let headroom = pool_size / SPILLABLE_HEADROOM_DIVISOR;
    if headroom == 0 && pool_size > 1 {
        1
    } else {
        headroom.min(pool_size.saturating_sub(1))
    }
}

impl MemoryPool for GreedyPoolWithSpillHeadroom {
    fn name(&self) -> &'static str {
        "greedy_spill_headroom"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        state.used = state.used.saturating_add(additional);
        if reservation.consumer().can_spill() {
            state.spillable_used = state.spillable_used.saturating_add(additional);
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        state.used = state.used.saturating_sub(shrink);
        if reservation.consumer().can_spill() {
            state.spillable_used = state.spillable_used.saturating_sub(shrink);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let remaining = {
            let mut state = self.state.lock();
            let new_used = state.used.saturating_add(additional);
            if new_used > self.pool_size {
                Some(self.pool_size.saturating_sub(state.used))
            } else if reservation.consumer().can_spill() {
                let new_spillable = state.spillable_used.saturating_add(additional);
                if new_spillable > self.spillable_cap() {
                    Some(self.spillable_cap().saturating_sub(state.spillable_used))
                } else {
                    state.spillable_used = new_spillable;
                    state.used = new_used;
                    None
                }
            } else {
                state.used = new_used;
                None
            }
        };
        // Format `self` only after dropping the state lock (`Display` re-locks).
        let Some(remaining) = remaining else {
            return Ok(());
        };
        Err(resources_datafusion_err!(
            "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}",
            human_readable_size(additional),
            reservation.consumer().name(),
            human_readable_size(reservation.size()),
            human_readable_size(remaining),
            self
        ))
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

impl Display for GreedyPoolWithSpillHeadroom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        write!(
            f,
            "{}(used: {}, spillable_used: {}, pool_size: {}, spillable_cap: {})",
            self.name(),
            human_readable_size(state.used),
            human_readable_size(state.spillable_used),
            human_readable_size(self.pool_size),
            human_readable_size(self.spillable_cap()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    fn pool(size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyPoolWithSpillHeadroom::new(size))
    }

    #[test]
    fn spillable_cannot_consume_headroom() {
        let pool_size = 16 * 1024;
        let memory = pool(pool_size);
        let headroom = spillable_headroom_bytes(pool_size);
        let cap = pool_size - headroom;
        let spillable = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&memory);

        spillable
            .try_grow(cap)
            .expect("spillable should fill up to the cap");
        spillable
            .try_grow(1)
            .expect_err("spillable must not take the headroom (TPC-DS Q97 ExternalSorter)");
        assert_eq!(memory.reserved(), cap);
    }

    #[test]
    fn unspillable_can_use_headroom_after_spillable_cap() {
        // Lab SF-100 Q97: ExternalSorter at ~cap, cayenne_scan needs 1 MiB.
        let pool_size = 16 * 1024;
        let memory = pool(pool_size);
        let cap = pool_size - spillable_headroom_bytes(pool_size);
        let sorter = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&memory);
        sorter
            .try_grow(cap)
            .expect("sorter should take the spillable cap");

        let scan = MemoryConsumer::new("cayenne_scan[store_sales, partition=2]")
            .with_can_spill(false)
            .register(&memory);
        scan.try_grow(1024)
            .expect("cayenne scan must allocate from the spillable headroom");
        assert_eq!(memory.reserved(), cap + 1024);
    }

    #[test]
    fn unspillable_can_fill_the_pool() {
        let pool_size = 4096;
        let memory = pool(pool_size);
        let scan = MemoryConsumer::new("cayenne_scan")
            .with_can_spill(false)
            .register(&memory);
        scan.try_grow(pool_size)
            .expect("unspillable consumers use the full pool");
        scan.try_grow(1)
            .expect_err("unspillable still cannot exceed the pool");
    }

    #[test]
    fn unspillable_hash_joins_do_not_starve_spillable_aggregates() {
        // Lab SF-100 Q78 on b51504b3bc: HashJoinInput (unspillable) filled
        // total used to the spillable cap, then GroupedHashAggregateStream
        // could not get 3.8 MiB. The cap is on spillable bytes only.
        let pool_size = 16 * 1024;
        let memory = pool(pool_size);
        let cap = pool_size - spillable_headroom_bytes(pool_size);
        let hash_join = MemoryConsumer::new("HashJoinInput[8]")
            .with_can_spill(false)
            .register(&memory);
        hash_join
            .try_grow(cap)
            .expect("unspillable hash join may fill up to the spillable cap of total used");

        let aggregate = MemoryConsumer::new("GroupedHashAggregateStream[12]")
            .with_can_spill(true)
            .register(&memory);
        aggregate
            .try_grow(1024)
            .expect("spillable aggregate must still grow while unspillable holds the cap");
        assert_eq!(memory.reserved(), cap + 1024);
    }
}
