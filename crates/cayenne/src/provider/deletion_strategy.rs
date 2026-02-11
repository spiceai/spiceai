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

use crate::catalog::{CatalogError, CatalogResult};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
#[derive(Debug, Clone)]
#[expect(clippy::type_complexity)]
pub enum PkDeletionStrategyWithCache {
    /// Position-based deletion tracking using `RoaringBitmap` per file.
    PositionBased {
        /// Maps data file path -> `RoaringBitmap` of file-local row positions.
        /// Uses Arc-wrapped `HashMap` for zero-copy sharing across concurrent operations.
        cached_deleted_row_ids: Arc<RwLock<Arc<HashMap<String, RoaringBitmap>>>>,
    },
    /// Int64 primary key deletion tracking with direct `HashMap<i64, i64>` lookup.
    Int64Pk {
        /// Maps PK (i64) -> `delete_sequence_number` for sequence-based ordering.
        cached_deleted_pk: Arc<RwLock<Arc<HashMap<i64, i64>>>>,
        /// Maps PK (i64) -> `insert_sequence_number` for upsert tracking.
        cached_insert_records: Arc<RwLock<Arc<HashMap<i64, i64>>>>,
    },
    /// Composite/non-integer primary key deletion tracking using serialized row keys.
    RowConverterBased {
        /// Maps PK bytes -> `delete_sequence_number` for sequence-based ordering.
        cached_deleted_row_keys: Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>,
        /// Maps PK bytes -> `insert_sequence_number` for upsert tracking.
        cached_insert_records: Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>,
    },
}

#[expect(clippy::type_complexity)]
impl PkDeletionStrategyWithCache {
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
    pub fn position_based_cache(
        &self,
    ) -> Option<&Arc<RwLock<Arc<HashMap<String, RoaringBitmap>>>>> {
        match self {
            Self::PositionBased {
                cached_deleted_row_ids,
            } => Some(cached_deleted_row_ids),
            Self::Int64Pk { .. } | Self::RowConverterBased { .. } => None,
        }
    }

    /// Returns the Int64 PK deletion cache, if this is an `Int64Pk` strategy.
    #[must_use]
    pub fn int64_pk_cache(&self) -> Option<&Arc<RwLock<Arc<HashMap<i64, i64>>>>> {
        match self {
            Self::Int64Pk {
                cached_deleted_pk, ..
            } => Some(cached_deleted_pk),
            _ => None,
        }
    }

    /// Returns the row keys deletion cache, if this is a `RowConverterBased` strategy.
    #[must_use]
    pub fn row_keys_cache(&self) -> Option<&Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>> {
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
    pub fn int64_insert_records_cache(&self) -> Option<&Arc<RwLock<Arc<HashMap<i64, i64>>>>> {
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
    pub fn row_keys_insert_records_cache(
        &self,
    ) -> Option<&Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>> {
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
    /// Copies data from `source` into `self`, updating the inner `Arc` values so that
    /// shared references (e.g., held by `CayenneDeletionSink`) see the updated data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The strategies don't match (e.g., `Int64Pk` vs `RowConverterBased`)
    /// - Any lock is poisoned
    pub fn refresh_from(&self, source: &Self) -> CatalogResult<()> {
        match (self, source) {
            (
                Self::PositionBased {
                    cached_deleted_row_ids: existing,
                },
                Self::PositionBased {
                    cached_deleted_row_ids: fresh,
                },
            ) => copy_cache(
                existing,
                fresh,
                "read fresh position data",
                "position write",
            ),
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
                copy_cache(
                    existing_pk,
                    fresh_pk,
                    "read fresh pk_i64 data",
                    "pk_i64 write",
                )?;
                copy_cache(
                    existing_insert,
                    fresh_insert,
                    "read fresh insert_pk_i64 data",
                    "insert_pk_i64 write",
                )
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
                copy_cache(
                    existing_keys,
                    fresh_keys,
                    "read fresh row_keys data",
                    "row_keys write",
                )?;
                copy_cache(
                    existing_insert,
                    fresh_insert,
                    "read fresh insert_row_keys data",
                    "insert_row_keys write",
                )
            }
            _ => Err(CatalogError::InvalidOperation {
                message: format!(
                    "Strategy mismatch during cache refresh: existing={:?}, fresh={:?}",
                    self.strategy(),
                    source.strategy()
                ),
                source: Box::<dyn std::error::Error + Send + Sync>::from(
                    "PkDeletionStrategy variant mismatch",
                ),
            }),
        }
    }
}

/// Helper to copy data between two `Arc<RwLock<Arc<T>>>` caches.
///
/// Used for refreshing deletion/insert caches from freshly-loaded data.
fn copy_cache<T: Clone>(
    existing: &Arc<RwLock<Arc<T>>>,
    fresh: &Arc<RwLock<Arc<T>>>,
    read_op: &str,
    write_op: &str,
) -> CatalogResult<()> {
    let fresh_data = {
        let guard = fresh.read().map_err(|_| CatalogError::LockPoisoned {
            operation: format!("refresh deletion cache ({read_op})"),
        })?;
        Arc::clone(&*guard)
    };
    let mut guard = existing.write().map_err(|_| CatalogError::LockPoisoned {
        operation: format!("refresh deletion cache ({write_op})"),
    })?;
    *guard = fresh_data;
    Ok(())
}
