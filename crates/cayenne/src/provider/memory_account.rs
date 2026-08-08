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
use std::sync::atomic::{AtomicUsize, Ordering};

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
    reservation: ParkingMutex<MemoryReservation>,
    keyset_bytes: AtomicUsize,
    deletion_bytes: AtomicUsize,
    cold_existence_bytes: AtomicUsize,
}

impl CayenneMemoryAccount {
    #[must_use]
    pub(crate) fn new(table_id: &str, pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            reservation: ParkingMutex::new(
                MemoryConsumer::new(format!("cayenne:{table_id}")).register(pool),
            ),
            keyset_bytes: AtomicUsize::new(0),
            deletion_bytes: AtomicUsize::new(0),
            cold_existence_bytes: AtomicUsize::new(0),
        }
    }

    fn resize_to_total(&self) {
        let total = self
            .keyset_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.deletion_bytes.load(Ordering::Relaxed))
            .saturating_add(self.cold_existence_bytes.load(Ordering::Relaxed));
        // `resize` is infallible (over-commits the greedy pool). See the type
        // docstring for why deletions must never fail-to-fit.
        self.reservation.lock().resize(total);
    }

    /// Account the resident bytes of the PK keyset (exact keyset or bloom).
    pub(crate) fn set_keyset_bytes(&self, bytes: usize) {
        let previous = self.keyset_bytes.swap(bytes, Ordering::Relaxed);
        // Keep this table's share of the process-global keyset ceiling in step,
        // as a delta. Residency is recomputed and republished here on every
        // change, so one place can hold the fleet figure honest.
        if bytes > previous {
            let growth = (bytes - previous) as u64;
            // Record it even if it does not fit: the clamp in
            // `effective_pk_keyset_budget` is what prevents growth, and once the
            // bytes EXIST, hiding them would let siblings over-commit against
            // headroom that is not there.
            if !super::pk_keyset_budget::try_reserve_keyset_bytes(growth) {
                super::pk_keyset_budget::force_reserve_keyset_bytes(growth);
            }
        } else if previous > bytes {
            super::pk_keyset_budget::release_keyset_bytes((previous - bytes) as u64);
        }
        self.resize_to_total();
    }

    /// Account the resident bytes of deletion + insert-record indexes (deleted
    /// keys, insert records, and position deletion vectors). Reset to 0 at
    /// compaction.
    pub(crate) fn set_deletion_bytes(&self, bytes: usize) {
        self.deletion_bytes.store(bytes, Ordering::Relaxed);
        self.resize_to_total();
    }

    /// Account the resident bytes of the cold-tier PK existence view (the
    /// per-cold-file bloom union). Reset to 0 whenever the view is cleared.
    pub(crate) fn set_cold_existence_bytes(&self, bytes: usize) {
        self.cold_existence_bytes.store(bytes, Ordering::Relaxed);
        self.resize_to_total();
    }

    /// Current total reserved bytes (keyset + deletions + cold existence). For
    /// observability and tests.
    #[must_use]
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reservation.lock().size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::execution::memory_pool::GreedyMemoryPool;

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
        let held = self.keyset_bytes.load(Ordering::Relaxed);
        if held > 0 {
            super::pk_keyset_budget::release_keyset_bytes(held as u64);
        }
    }
}
