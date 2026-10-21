/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Primary key deletion strategy types and caches.
//!
//! Defines [`PkDeletionStrategy`] (the strategy kind) and [`PkDeletionStrategyWithCache`]
//! (the strategy with its associated in-memory caches).
//!
//! Caches are held in [`ArcSwap`] cells so that scans probe a wait-free, immutable
//! snapshot. Writers build a new index off the hot path and publish it via a single
//! atomic swap; readers never block on a write lock.

use super::deletion_index::{DeletionIndex, KeyDeletionIndex};
use super::{Error, Result};
use arc_swap::ArcSwap;
use roaring::{RoaringBitmap, RoaringTreemap};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use vortex_datafusion::VortexAccessPlan;
use vortex_scan::selection::Selection;

/// Position-based deletion state for a single data file.
///
/// Keeps the compact `RoaringBitmap` for write-side set operations and a
/// prebuilt `VortexAccessPlan` for scan planning. Building the access plan
/// converts the u32 bitmap into Vortex's u64 `RoaringTreemap`, so doing it once
/// when a deletion snapshot is published avoids rebuilding the treemap for
/// every file on every scan.
pub(crate) struct PositionDeletionVector {
    row_ids: RoaringBitmap,
    access_plan: Arc<VortexAccessPlan>,
}

impl PositionDeletionVector {
    #[must_use]
    pub(crate) fn new(row_ids: RoaringBitmap) -> Self {
        let exclude: RoaringTreemap = row_ids.iter().map(u64::from).collect();
        let access_plan = Arc::new(
            VortexAccessPlan::default().with_selection(Selection::ExcludeRoaring(exclude)),
        );

        Self {
            row_ids,
            access_plan,
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    #[must_use]
    pub(crate) fn len(&self) -> u64 {
        self.row_ids.len()
    }

    #[must_use]
    pub(crate) fn contains(&self, row_id: u32) -> bool {
        self.row_ids.contains(row_id)
    }

    #[must_use]
    pub(crate) fn count_before_row(&self, row_count: usize) -> usize {
        let deleted_rows = u32::try_from(row_count).map_or_else(
            |_| self.row_ids.len(),
            |row_count| self.row_ids.range_cardinality(0..row_count),
        );

        usize::try_from(deleted_rows).unwrap_or(usize::MAX)
    }

    #[must_use]
    pub(crate) fn to_bitmap(&self) -> RoaringBitmap {
        self.row_ids.clone()
    }

    #[must_use]
    pub(crate) fn access_plan(&self) -> Arc<VortexAccessPlan> {
        Arc::clone(&self.access_plan)
    }

    #[must_use]
    pub(crate) fn approx_bytes(&self) -> usize {
        // The resident state keeps both the original u32 bitmap and the u64
        // access-plan treemap built from it. Serialized size is a compact,
        // container-aware estimate that tracks bitmap growth without walking
        // every row id.
        std::mem::size_of::<Self>()
            .saturating_add(self.row_ids.serialized_size())
            .saturating_add(self.row_ids.serialized_size())
    }
}

impl fmt::Debug for PositionDeletionVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PositionDeletionVector")
            .field("deleted_rows", &self.row_ids.len())
            .finish_non_exhaustive()
    }
}

/// Position-based deletion cache keyed by data file path.
///
/// The per-file deletion state is `Arc`-wrapped so that publishing a fresh
/// snapshot through `ArcSwap` only clones a `HashMap<String, Arc<…>>`
/// (cheap — small string keys + 8-byte Arc pointers), not the bitmap/access-plan
/// data itself. Without the inner `Arc`, every per-batch delete on a
/// position-based table cloned every file's full bitmap on each commit,
/// turning the write into O(total deleted rows) per call. The shared inner type
/// lets readers and writers share unchanged entries for free; only entries that
/// the writer actually updates allocate a new `Arc`.
pub(crate) type PositionBitmap = HashMap<String, Arc<PositionDeletionVector>>;

fn approx_position_bitmap_bytes(bitmap: &PositionBitmap) -> usize {
    const POSITION_BITMAP_ENTRY_OVERHEAD_BYTES: usize = 64;

    bitmap.iter().fold(0, |total, (file_path, deletions)| {
        total
            .saturating_add(file_path.len())
            .saturating_add(POSITION_BITMAP_ENTRY_OVERHEAD_BYTES)
            .saturating_add(deletions.approx_bytes())
    })
}

/// Atomically-published deletion state for single-column `Int64` primary keys.
///
/// Holds one fused [`DeletionIndex`] whose entries carry both the delete and
/// (for upsert conflicts) re-insert sequence numbers, so the scan hot path
/// resolves visibility with a single probe. Previously a (deleted,
/// insert-records) index pair published together; the fused index preserves
/// the same atomicity with one cell.
#[derive(Debug, Clone)]
pub struct Int64PkDeletionSnapshot {
    pub(crate) tombstones: Arc<DeletionIndex>,
}

impl Int64PkDeletionSnapshot {
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            tombstones: Arc::new(DeletionIndex::empty()),
        }
    }

    #[must_use]
    pub(crate) fn from_index(tombstones: DeletionIndex) -> Self {
        Self {
            tombstones: Arc::new(tombstones),
        }
    }
}

/// Atomically-published deletion state for row-converter primary keys.
///
/// See [`Int64PkDeletionSnapshot`] for the fused-index rationale.
#[derive(Debug, Clone)]
pub struct RowConverterDeletionSnapshot {
    pub(crate) tombstones: Arc<KeyDeletionIndex>,
}

impl RowConverterDeletionSnapshot {
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            tombstones: Arc::new(KeyDeletionIndex::empty()),
        }
    }

    #[must_use]
    pub(crate) fn from_index(tombstones: KeyDeletionIndex) -> Self {
        Self {
            tombstones: Arc::new(tombstones),
        }
    }
}

/// Strategy for primary key-based deletion filtering.
///
/// Determines which cache and filter execution plan to use at query time.
/// Chosen based on the table's primary key configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PkDeletionStrategy {
    /// No primary key - use position-based deletion with `RoaringBitmap`.
    /// Requires `CoalescePartitionsExec` to ensure consistent ordering.
    PositionBased,
    /// Single-column Int64 primary key - use direct `HashSet<i64>` lookup.
    /// Most efficient: no serialization, 8 bytes per key, parallel reads.
    Int64Pk,
    /// Composite or non-integer primary key - use `RowConverter` + `HashSet<Box<[u8]>>`.
    /// Handles all PK types but has serialization overhead.
    RowConverterBased,
}

/// Runtime caches for deletion and insert tracking, organized by [`PkDeletionStrategy`].
///
/// Each variant holds the in-memory caches required for its corresponding strategy:
/// - Deletion caches track which rows should be filtered out during scans
/// - Insert caches (for PK-based strategies) track inserted rows to prevent
///   resurrection of previously deleted keys during upserts
///
/// All caches are `Arc<ArcSwap<…>>`. Read paths take a wait-free `load_full()`; writers
/// build a fresh snapshot and `store` it.
#[derive(Debug, Clone)]
pub(crate) enum PkDeletionStrategyWithCache {
    /// Position-based deletion tracking using `RoaringBitmap` per file.
    PositionBased {
        /// Maps data file path -> `RoaringBitmap` of file-local row positions.
        cached_deleted_row_ids: Arc<ArcSwap<PositionBitmap>>,
    },
    /// Int64 primary key deletion tracking with bloom-prefiltered hash index.
    Int64Pk {
        /// Atomically-published deleted PK and insert-record indexes.
        deletion_snapshot: Arc<ArcSwap<Int64PkDeletionSnapshot>>,
        /// Per-file position deletes for rows whose `(file, position)` is known
        /// (`deletion_mode: position`). Pushed into the Vortex scan alongside the
        /// above-scan key filter for the remaining (unlocated) rows.
        ///
        /// A key-mode table never WRITES one of these, but this cache is not
        /// therefore always empty there: a table that ran under `position` before
        /// its mode resolved to `key` keeps its durable vectors, which load here
        /// on reopen and are still applied. That is a compatibility path, not
        /// dead state — dropping it resurrects every row they mask.
        position_deletions: Arc<ArcSwap<PositionBitmap>>,
    },
    /// Composite/non-integer primary key deletion tracking using serialized row keys.
    RowConverterBased {
        /// Atomically-published deleted row-key and insert-record indexes.
        deletion_snapshot: Arc<ArcSwap<RowConverterDeletionSnapshot>>,
        /// Per-file position deletes for located rows (`deletion_mode: position`).
        /// See [`Self::Int64Pk`]'s `position_deletions`.
        position_deletions: Arc<ArcSwap<PositionBitmap>>,
    },
}

impl PkDeletionStrategyWithCache {
    /// Construct a position-based strategy with an empty cache.
    #[must_use]
    pub fn empty_position_based() -> Self {
        Self::PositionBased {
            cached_deleted_row_ids: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
        }
    }

    /// Construct an Int64 PK strategy with empty caches.
    #[must_use]
    pub fn empty_int64_pk() -> Self {
        Self::Int64Pk {
            deletion_snapshot: Arc::new(ArcSwap::from_pointee(Int64PkDeletionSnapshot::empty())),
            position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
        }
    }

    /// Construct a row-converter-based strategy with empty caches.
    #[must_use]
    pub fn empty_row_converter() -> Self {
        Self::RowConverterBased {
            deletion_snapshot: Arc::new(ArcSwap::from_pointee(
                RowConverterDeletionSnapshot::empty(),
            )),
            position_deletions: Arc::new(ArcSwap::from_pointee(PositionBitmap::new())),
        }
    }

    /// Construct an empty cache matching the given [`PkDeletionStrategy`].
    #[must_use]
    pub fn empty_for(strategy: PkDeletionStrategy) -> Self {
        match strategy {
            PkDeletionStrategy::PositionBased => Self::empty_position_based(),
            PkDeletionStrategy::Int64Pk => Self::empty_int64_pk(),
            PkDeletionStrategy::RowConverterBased => Self::empty_row_converter(),
        }
    }

    /// Returns the `PkDeletionStrategy` variant for this cache.
    #[must_use]
    pub const fn strategy(&self) -> PkDeletionStrategy {
        match self {
            Self::PositionBased { .. } => PkDeletionStrategy::PositionBased,
            Self::Int64Pk { .. } => PkDeletionStrategy::Int64Pk,
            Self::RowConverterBased { .. } => PkDeletionStrategy::RowConverterBased,
        }
    }

    /// Returns `true` if this is the `PositionBased` strategy.
    #[must_use]
    pub const fn is_position_based(&self) -> bool {
        matches!(self, Self::PositionBased { .. })
    }

    /// Returns `true` if this is the `Int64Pk` strategy.
    #[must_use]
    pub const fn is_int64_pk(&self) -> bool {
        matches!(self, Self::Int64Pk { .. })
    }

    /// Returns the position-based deletion cache, if this is a `PositionBased` strategy.
    #[must_use]
    pub(crate) fn position_based_cache(&self) -> Option<&Arc<ArcSwap<PositionBitmap>>> {
        match self {
            Self::PositionBased {
                cached_deleted_row_ids,
            } => Some(cached_deleted_row_ids),
            Self::Int64Pk { .. } | Self::RowConverterBased { .. } => None,
        }
    }

    /// Returns the Int64 PK deletion snapshot, if this is an `Int64Pk` strategy.
    #[must_use]
    pub fn int64_pk_snapshot(&self) -> Option<&Arc<ArcSwap<Int64PkDeletionSnapshot>>> {
        match self {
            Self::Int64Pk {
                deletion_snapshot, ..
            } => Some(deletion_snapshot),
            _ => None,
        }
    }

    /// Returns the row keys deletion snapshot, if this is a `RowConverterBased` strategy.
    #[must_use]
    pub fn row_keys_snapshot(&self) -> Option<&Arc<ArcSwap<RowConverterDeletionSnapshot>>> {
        match self {
            Self::RowConverterBased {
                deletion_snapshot, ..
            } => Some(deletion_snapshot),
            _ => None,
        }
    }

    /// Returns the per-file position-delete cache for **any** strategy: the
    /// `PositionBased` cache for PK-less tables, or the `position_deletions`
    /// cache for PK tables (`deletion_mode: position`). This is the unified
    /// handle the position-vector write/read paths key by file path.
    #[must_use]
    pub(crate) fn position_cache(&self) -> &Arc<ArcSwap<PositionBitmap>> {
        match self {
            Self::PositionBased {
                cached_deleted_row_ids: position_cache,
            }
            | Self::Int64Pk {
                position_deletions: position_cache,
                ..
            }
            | Self::RowConverterBased {
                position_deletions: position_cache,
                ..
            } => position_cache,
        }
    }

    /// Approximate resident bytes held by deletion and insert-record caches.
    /// Includes key-based delete/insert indexes and per-file position deletes.
    #[must_use]
    pub(crate) fn approx_resident_bytes(&self) -> usize {
        match self {
            Self::PositionBased {
                cached_deleted_row_ids,
            } => {
                let position_snapshot = cached_deleted_row_ids.load_full();
                approx_position_bitmap_bytes(&position_snapshot)
            }
            Self::Int64Pk {
                deletion_snapshot,
                position_deletions,
            } => {
                let snapshot = deletion_snapshot.load();
                let position_snapshot = position_deletions.load_full();
                snapshot
                    .tombstones
                    .approx_bytes()
                    .saturating_add(approx_position_bitmap_bytes(&position_snapshot))
            }
            Self::RowConverterBased {
                deletion_snapshot,
                position_deletions,
            } => {
                let snapshot = deletion_snapshot.load();
                let position_snapshot = position_deletions.load_full();
                snapshot
                    .tombstones
                    .approx_bytes()
                    .saturating_add(approx_position_bitmap_bytes(&position_snapshot))
            }
        }
    }

    /// Refresh this cache from a freshly-loaded source.
    ///
    /// Atomically swaps each `ArcSwap` cell to the snapshot held by `source`. Concurrent
    /// readers see either the old or the new value but never an in-between state.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategies don't match (e.g., `Int64Pk` vs `RowConverterBased`).
    pub fn refresh_from(&self, source: &Self, table_name: &str) -> Result<()> {
        match (self, source) {
            (
                Self::PositionBased {
                    cached_deleted_row_ids: existing,
                },
                Self::PositionBased {
                    cached_deleted_row_ids: fresh,
                },
            ) => {
                existing.store(fresh.load_full());
                Ok(())
            }
            (
                Self::Int64Pk {
                    deletion_snapshot: existing,
                    position_deletions: existing_positions,
                },
                Self::Int64Pk {
                    deletion_snapshot: fresh,
                    position_deletions: fresh_positions,
                },
            ) => {
                existing.store(fresh.load_full());
                existing_positions.store(fresh_positions.load_full());
                Ok(())
            }
            (
                Self::RowConverterBased {
                    deletion_snapshot: existing,
                    position_deletions: existing_positions,
                },
                Self::RowConverterBased {
                    deletion_snapshot: fresh,
                    position_deletions: fresh_positions,
                },
            ) => {
                existing.store(fresh.load_full());
                existing_positions.store(fresh_positions.load_full());
                Ok(())
            }
            _ => Err(Error::Internal {
                table: table_name.to_string(),
                message: format!(
                    "Strategy mismatch during cache refresh: existing={:?}, fresh={:?}",
                    self.strategy(),
                    source.strategy()
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Regression test for the `deletion_snapshot` lost-update race that the
    /// `rcu`-based publishing fixes. The old pattern (`load_full()` + `store()`)
    /// could clobber a concurrent prune/add that landed between the load and the
    /// store, resurrecting tombstoned rows.
    ///
    /// We make the race DETERMINISTIC via `ArcSwap::rcu`'s contract: the closure
    /// re-runs whenever the cell changed under it. On the first closure call we
    /// inject a concurrent prune (simulating a compaction that lands between the
    /// rcu load and its compare-and-swap), forcing `rcu` to retry against the
    /// PRUNED index. The published result must reflect BOTH the prune and the
    /// add — never the stale pre-prune view.
    ///
    /// With the old non-atomic `load_full()` + `store()`, the add (built off the
    /// pre-prune load) would overwrite the prune and resurrect the deleted key —
    /// `delete_len()` would be 3 and the deleted set would include pk 1.
    #[test]
    fn rcu_publish_does_not_lose_a_concurrent_prune() {
        // Live index: pk 1 deleted at seq 10, pk 2 deleted at seq 20.
        let initial = DeletionIndex::empty().extend_max_deletes([(1_i64, 10_i64), (2, 20)]);
        assert_eq!(initial.delete_len(), 2);
        let cell = ArcSwap::from_pointee(Int64PkDeletionSnapshot::from_index(initial));

        // A writer adds a pure delete for pk 3 at seq 30 via `rcu`. On the first
        // closure call we inject a concurrent prune at cutoff=10 (drops pk 1),
        // forcing `rcu` to retry against the pruned index.
        let mut injected = false;
        cell.rcu(|current| {
            if !injected {
                injected = true;
                let pruned = current.tombstones.prune_deletes_at_or_below(10);
                cell.store(Arc::new(Int64PkDeletionSnapshot::from_index(pruned)));
            }
            let updated = current
                .tombstones
                .extend_max(std::iter::once((3_i64, 30_i64)), std::iter::empty());
            Arc::new(Int64PkDeletionSnapshot::from_index(updated))
        });

        assert!(injected, "the injected concurrent prune should have run");

        let guard = cell.load();
        let final_index = &guard.tombstones;
        let deleted: BTreeSet<i64> = final_index
            .iter_entries()
            .filter(|(_, entry)| entry.delete_sequence().is_some())
            .map(|(pk, _)| pk)
            .collect();

        // pk 1 stayed pruned (no resurrection), pk 2 untouched, pk 3 added.
        assert_eq!(
            deleted,
            [2_i64, 3].into_iter().collect::<BTreeSet<_>>(),
            "rcu lost the concurrent prune (pk 1 resurrected) or dropped the add"
        );
        assert_eq!(final_index.delete_len(), 2);
    }
}
