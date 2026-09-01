/*
Copyright 2026 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Memory-pool accounting for Cayenne resident state that lives outside query execution.

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use parking_lot::Mutex as ParkingMutex;

/// Per-table accounting of Cayenne's long-lived resident state (outside query-
/// operator reservations): the PK keyset and deletion indexes, registered
/// against the `DataFusion` [`MemoryPool`] that
/// `runtime.query.memory_limit` controls. Before this reservation, that state
/// was invisible to the pool, so queries planned against the full budget while
/// the process drifted toward OOM. With the state registered, the pool reflects
/// real Cayenne usage, so the query path sees the *actually available* budget
/// and fails fast (`ResourcesExhausted`) instead of over-committing the host.
///
/// Accounting is intentionally **infallible** (`resize`, which over-commits the
/// `GreedyMemoryPool` rather than erroring): a deletion index can never be
/// silently dropped to fit a budget (that would resurrect deleted rows), so the
/// real bound on deletions is compaction, and the real bound on the keyset is
/// its memory-derived `pk_keyset_cache_max_bytes` cap with bloom fallback. The
/// reservation's job here is visibility + correct cross-consumer back-pressure,
/// not to gate Cayenne's own growth.
pub(crate) struct CayenneMemoryAccount {
    /// The reservation and the three component figures it is computed from, under
    /// one lock.
    ///
    /// The components are not independent of the reservation — the reservation
    /// *is* their sum, and their agreement is the whole diagnosis (see
    /// [`Self::snapshot`]). Published from separate atomics, a sampler could pair
    /// one write's components with another write's total and report an accounting
    /// gap that does not exist, and a mutation that computed its total before a
    /// concurrent one could resize the pool back down to a stale figure. Every
    /// mutation already had to take this lock to resize, so holding the
    /// components here costs no extra acquisition.
    state: ParkingMutex<AccountState>,
}

struct AccountState {
    reservation: MemoryReservation,
    keyset_bytes: usize,
    deletion_bytes: usize,
    cold_existence_bytes: usize,
}

impl AccountState {
    fn resize_to_total(&mut self) {
        let total = self
            .keyset_bytes
            .saturating_add(self.deletion_bytes)
            .saturating_add(self.cold_existence_bytes);
        // `resize` is infallible (over-commits the greedy pool). See the
        // `CayenneMemoryAccount` docstring for why deletions must never
        // fail-to-fit.
        self.reservation.resize(total);
    }
}

/// A coherent read of one table's accounting: the three components Cayenne
/// computed, and the total that reached the `DataFusion` pool. Every figure is
/// bytes.
pub(crate) struct MemoryAccountSnapshot {
    /// The PK keyset — an exact keyset or the bloom that replaced it.
    pub keyset: usize,
    /// Deletion and insert-record indexes.
    pub deletion_index: usize,
    /// The cold-tier PK existence view.
    pub cold_existence: usize,
    /// What the `DataFusion` pool reservation actually holds — the sum of the
    /// three above as of this same read.
    pub reserved: usize,
}

impl CayenneMemoryAccount {
    #[must_use]
    pub(crate) fn new(table_id: &str, pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            state: ParkingMutex::new(AccountState {
                reservation: MemoryConsumer::new(format!("cayenne:{table_id}")).register(pool),
                keyset_bytes: 0,
                deletion_bytes: 0,
                cold_existence_bytes: 0,
            }),
        }
    }

    /// Account the resident bytes of the PK keyset (exact keyset or bloom).
    pub(crate) fn set_keyset_bytes(&self, bytes: usize) {
        let previous = {
            let mut state = self.state.lock();
            let previous = std::mem::replace(&mut state.keyset_bytes, bytes);
            state.resize_to_total();
            previous
        };
        // The fleet delta is applied off this lock: it is a saturating add/sub on
        // a process-global counter, so concurrent transitions of this table
        // compose in any order, and taking a second lock inside the first buys
        // nothing.
        //
        // Keep this table's share of the process-global keyset ceiling in step,
        // as a delta. Residency is recomputed and republished here on every
        // change, so one place can hold the fleet figure honest.
        if bytes > previous {
            let growth = (bytes - previous) as u64;
            // Record it even if it does not fit: the clamp in
            // `effective_pk_keyset_budget` is what prevents growth, and once the
            // bytes EXIST, hiding them would let siblings over-commit against
            // headroom that is not there.
            //
            // This is a publication, not an admission — the caller already grew.
            // Two tables can read the same headroom, both grow into it, and the
            // second land here with nothing left, so the aggregate can exceed
            // the ceiling by what was in flight between the two. See
            // `try_reserve_keyset_bytes` for why that is the intended trade and
            // what bounds the overshoot.
            if !super::pk_keyset_budget::try_reserve_keyset_bytes(growth) {
                super::pk_keyset_budget::force_reserve_keyset_bytes(growth);
            }
        } else if previous > bytes {
            super::pk_keyset_budget::release_keyset_bytes((previous - bytes) as u64);
        }
    }

    /// Account the resident bytes of deletion + insert-record indexes (deleted
    /// keys, insert records, and position deletion vectors). Reset to 0 at
    /// compaction.
    pub(crate) fn set_deletion_bytes(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.deletion_bytes = bytes;
        state.resize_to_total();
    }

    /// Account the resident bytes of the cold-tier PK existence view (the
    /// per-cold-file bloom union). Reset to 0 whenever the view is cleared.
    pub(crate) fn set_cold_existence_bytes(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.cold_existence_bytes = bytes;
        state.resize_to_total();
    }

    /// Current total reserved bytes (keyset + deletions + cold existence). For
    /// observability and tests.
    #[must_use]
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.state.lock().reservation.size()
    }

    /// The components and the reservation, read together.
    ///
    /// Both halves are published because their relationship is the diagnosis,
    /// not either one alone. The components are what Cayenne *computed*;
    /// `reserved_bytes` is what actually reached the `DataFusion` pool. Equal
    /// means the accounting lands, and a resident-memory figure far above the
    /// pool is then off-pool structures. Components far above the reservation
    /// means the accounting itself is not landing — and no single gauge can tell
    /// those two apart, which is why they must come from one read rather than
    /// two.
    #[must_use]
    pub(crate) fn snapshot(&self) -> MemoryAccountSnapshot {
        let state = self.state.lock();
        MemoryAccountSnapshot {
            keyset: state.keyset_bytes,
            deletion_index: state.deletion_bytes,
            cold_existence: state.cold_existence_bytes,
            reserved: state.reservation.size(),
        }
    }
}

impl Drop for CayenneMemoryAccount {
    /// Return this table's share of the process-global keyset ceiling.
    ///
    /// Without this a dropped table's share is never released, so a pod that
    /// creates and drops tables — a re-registration, a schema evolution, a test
    /// harness building providers in a loop — walks the fleet budget down until
    /// every surviving table is refused and falls back to a bloom, with no
    /// memory actually in use. The `MemoryReservation` beside it already frees
    /// itself on drop; this gives the fleet budget the same property.
    fn drop(&mut self) {
        let held = self.state.get_mut().keyset_bytes;
        if held > 0 {
            super::pk_keyset_budget::release_keyset_bytes(held as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::execution::memory_pool::GreedyMemoryPool;

    #[test]
    fn snapshot_components_always_sum_to_the_reservation() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let account = CayenneMemoryAccount::new("coherence", &pool);

        for (keyset, deletion, cold) in [(0, 0, 0), (400, 0, 0), (400, 600, 0), (7, 600, 90)] {
            account.set_keyset_bytes(keyset);
            account.set_deletion_bytes(deletion);
            account.set_cold_existence_bytes(cold);

            let snapshot = account.snapshot();
            assert_eq!(
                snapshot.keyset + snapshot.deletion_index + snapshot.cold_existence,
                snapshot.reserved,
                "a snapshot's components and its reservation must agree, or the \
                 gap between them reads as an accounting bug that is not there"
            );
        }
    }

    #[test]
    fn cayenne_memory_account_tracks_keyset_and_deletions() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let account = CayenneMemoryAccount::new("test_table", &pool);
        assert_eq!(account.reserved_bytes(), 0);

        account.set_keyset_bytes(400);
        assert_eq!(account.reserved_bytes(), 400);
        assert_eq!(
            pool.reserved(),
            400,
            "the keyset reservation reaches the pool"
        );

        account.set_deletion_bytes(600);
        assert_eq!(
            account.reserved_bytes(),
            1000,
            "the reservation is keyset + deletion bytes"
        );
        assert_eq!(pool.reserved(), 1000);

        // A keyset shrink (e.g. an exact->bloom downgrade) leaves the deletion
        // accounting intact.
        account.set_keyset_bytes(50);
        assert_eq!(account.reserved_bytes(), 650);

        // The cold-tier existence view adds on top of both.
        account.set_cold_existence_bytes(100);
        assert_eq!(account.reserved_bytes(), 750);

        // Compaction clears the deletions; clearing the rest releases all.
        account.set_deletion_bytes(0);
        assert_eq!(account.reserved_bytes(), 150);
        account.set_cold_existence_bytes(0);
        assert_eq!(account.reserved_bytes(), 50);
        account.set_keyset_bytes(0);
        assert_eq!(account.reserved_bytes(), 0);
        assert_eq!(pool.reserved(), 0, "all reservation is released");
    }

    #[test]
    fn cayenne_memory_account_overcommits_a_tight_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let account = CayenneMemoryAccount::new("tight", &pool);
        account.set_deletion_bytes(10_000);
        assert_eq!(account.reserved_bytes(), 10_000);
        assert!(
            pool.reserved() >= 10_000,
            "Cayenne over-commit must be visible in the pool's reserved total"
        );
    }
}
