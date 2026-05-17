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
use vortex_scan::Selection;

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

    pub(crate) fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.row_ids.iter()
    }

    #[must_use]
    pub(crate) fn to_bitmap(&self) -> RoaringBitmap {
        self.row_ids.clone()
    }

    #[must_use]
    pub(crate) fn access_plan(&self) -> Arc<VortexAccessPlan> {
        Arc::clone(&self.access_plan)
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
pub type PositionBitmap = HashMap<String, Arc<PositionDeletionVector>>;

/// Atomically-published deletion state for single-column `Int64` primary keys.
#[derive(Debug, Clone)]
pub struct Int64PkDeletionSnapshot {
    pub(crate) deleted_pk: Arc<DeletionIndex>,
    pub(crate) insert_records: Arc<DeletionIndex>,
}

impl Int64PkDeletionSnapshot {
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            deleted_pk: Arc::new(DeletionIndex::empty()),
            insert_records: Arc::new(DeletionIndex::empty()),
        }
    }

    #[must_use]
    pub(crate) const fn from_arcs(
        deleted_pk: Arc<DeletionIndex>,
        insert_records: Arc<DeletionIndex>,
    ) -> Self {
        Self {
            deleted_pk,
            insert_records,
        }
    }

    #[must_use]
    pub(crate) fn from_indices(deleted_pk: DeletionIndex, insert_records: DeletionIndex) -> Self {
        Self {
            deleted_pk: Arc::new(deleted_pk),
            insert_records: Arc::new(insert_records),
        }
    }
}

/// Atomically-published deletion state for row-converter primary keys.
#[derive(Debug, Clone)]
pub struct RowConverterDeletionSnapshot {
    pub(crate) deleted_row_keys: Arc<KeyDeletionIndex>,
    pub(crate) insert_records: Arc<KeyDeletionIndex>,
}

impl RowConverterDeletionSnapshot {
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            deleted_row_keys: Arc::new(KeyDeletionIndex::empty()),
            insert_records: Arc::new(KeyDeletionIndex::empty()),
        }
    }

    #[must_use]
    pub(crate) const fn from_arcs(
        deleted_row_keys: Arc<KeyDeletionIndex>,
        insert_records: Arc<KeyDeletionIndex>,
    ) -> Self {
        Self {
            deleted_row_keys,
            insert_records,
        }
    }

    #[must_use]
    pub(crate) fn from_indices(
        deleted_row_keys: KeyDeletionIndex,
        insert_records: KeyDeletionIndex,
    ) -> Self {
        Self {
            deleted_row_keys: Arc::new(deleted_row_keys),
            insert_records: Arc::new(insert_records),
        }
    }
}

/// Strategy for primary key-based deletion filtering.
///
/// Determines which cache and filter execution plan to use at query time.
/// Chosen based on the table's primary key configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkDeletionStrategy {
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
pub enum PkDeletionStrategyWithCache {
    /// Position-based deletion tracking using `RoaringBitmap` per file.
    PositionBased {
        /// Maps data file path -> `RoaringBitmap` of file-local row positions.
        cached_deleted_row_ids: Arc<ArcSwap<PositionBitmap>>,
    },
    /// Int64 primary key deletion tracking with bloom-prefiltered hash index.
    Int64Pk {
        /// Atomically-published deleted PK and insert-record indexes.
        deletion_snapshot: Arc<ArcSwap<Int64PkDeletionSnapshot>>,
    },
    /// Composite/non-integer primary key deletion tracking using serialized row keys.
    RowConverterBased {
        /// Atomically-published deleted row-key and insert-record indexes.
        deletion_snapshot: Arc<ArcSwap<RowConverterDeletionSnapshot>>,
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
        }
    }

    /// Construct a row-converter-based strategy with empty caches.
    #[must_use]
    pub fn empty_row_converter() -> Self {
        Self::RowConverterBased {
            deletion_snapshot: Arc::new(ArcSwap::from_pointee(
                RowConverterDeletionSnapshot::empty(),
            )),
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
    pub fn position_based_cache(&self) -> Option<&Arc<ArcSwap<PositionBitmap>>> {
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
            Self::Int64Pk { deletion_snapshot } => Some(deletion_snapshot),
            _ => None,
        }
    }

    /// Returns the row keys deletion snapshot, if this is a `RowConverterBased` strategy.
    #[must_use]
    pub fn row_keys_snapshot(&self) -> Option<&Arc<ArcSwap<RowConverterDeletionSnapshot>>> {
        match self {
            Self::RowConverterBased { deletion_snapshot } => Some(deletion_snapshot),
            _ => None,
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
                },
                Self::Int64Pk {
                    deletion_snapshot: fresh,
                },
            ) => {
                existing.store(fresh.load_full());
                Ok(())
            }
            (
                Self::RowConverterBased {
                    deletion_snapshot: existing,
                },
                Self::RowConverterBased {
                    deletion_snapshot: fresh,
                },
            ) => {
                existing.store(fresh.load_full());
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
