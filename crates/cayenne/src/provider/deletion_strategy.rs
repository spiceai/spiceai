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
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::Arc;

/// Position-based deletion bitmap keyed by data file path.
///
/// The per-file `RoaringBitmap` is `Arc`-wrapped so that publishing a fresh
/// snapshot through `ArcSwap` only clones a `HashMap<String, Arc<…>>`
/// (cheap — small string keys + 8-byte Arc pointers), not the bitmap data
/// itself. Without the inner `Arc`, every per-batch delete on a
/// position-based table cloned every file's full bitmap on each commit,
/// turning the write into O(total deleted rows) per call. The shared inner
/// type lets readers and writers share unchanged bitmaps for free; only
/// entries that the writer actually updates allocate a new `Arc`.
pub type PositionBitmap = HashMap<String, Arc<RoaringBitmap>>;

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
        /// Maps PK (i64) -> `delete_sequence_number` for sequence-based ordering.
        cached_deleted_pk: Arc<ArcSwap<DeletionIndex>>,
        /// Maps PK (i64) -> `insert_sequence_number` for upsert tracking.
        cached_insert_records: Arc<ArcSwap<DeletionIndex>>,
    },
    /// Composite/non-integer primary key deletion tracking using serialized row keys.
    RowConverterBased {
        /// Maps PK bytes -> `delete_sequence_number` for sequence-based ordering.
        cached_deleted_row_keys: Arc<ArcSwap<KeyDeletionIndex>>,
        /// Maps PK bytes -> `insert_sequence_number` for upsert tracking.
        cached_insert_records: Arc<ArcSwap<KeyDeletionIndex>>,
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
            cached_deleted_pk: Arc::new(ArcSwap::from_pointee(DeletionIndex::empty())),
            cached_insert_records: Arc::new(ArcSwap::from_pointee(DeletionIndex::empty())),
        }
    }

    /// Construct a row-converter-based strategy with empty caches.
    #[must_use]
    pub fn empty_row_converter() -> Self {
        Self::RowConverterBased {
            cached_deleted_row_keys: Arc::new(ArcSwap::from_pointee(KeyDeletionIndex::empty())),
            cached_insert_records: Arc::new(ArcSwap::from_pointee(KeyDeletionIndex::empty())),
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

    /// Returns the Int64 PK deletion cache, if this is an `Int64Pk` strategy.
    #[must_use]
    pub fn int64_pk_cache(&self) -> Option<&Arc<ArcSwap<DeletionIndex>>> {
        match self {
            Self::Int64Pk {
                cached_deleted_pk, ..
            } => Some(cached_deleted_pk),
            _ => None,
        }
    }

    /// Returns the row keys deletion cache, if this is a `RowConverterBased` strategy.
    #[must_use]
    pub fn row_keys_cache(&self) -> Option<&Arc<ArcSwap<KeyDeletionIndex>>> {
        match self {
            Self::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => Some(cached_deleted_row_keys),
            _ => None,
        }
    }

    /// Returns the Int64 insert records cache, if this is an `Int64Pk` strategy.
    #[must_use]
    pub fn int64_insert_records_cache(&self) -> Option<&Arc<ArcSwap<DeletionIndex>>> {
        match self {
            Self::Int64Pk {
                cached_insert_records,
                ..
            } => Some(cached_insert_records),
            _ => None,
        }
    }

    /// Returns the row keys insert records cache, if this is a `RowConverterBased` strategy.
    #[must_use]
    pub fn row_keys_insert_records_cache(&self) -> Option<&Arc<ArcSwap<KeyDeletionIndex>>> {
        match self {
            Self::RowConverterBased {
                cached_insert_records,
                ..
            } => Some(cached_insert_records),
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
                    cached_deleted_pk: existing_pk,
                    cached_insert_records: existing_insert,
                },
                Self::Int64Pk {
                    cached_deleted_pk: fresh_pk,
                    cached_insert_records: fresh_insert,
                },
            ) => {
                existing_pk.store(fresh_pk.load_full());
                existing_insert.store(fresh_insert.load_full());
                Ok(())
            }
            (
                Self::RowConverterBased {
                    cached_deleted_row_keys: existing_keys,
                    cached_insert_records: existing_insert,
                },
                Self::RowConverterBased {
                    cached_deleted_row_keys: fresh_keys,
                    cached_insert_records: fresh_insert,
                },
            ) => {
                existing_keys.store(fresh_keys.load_full());
                existing_insert.store(fresh_insert.load_full());
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
