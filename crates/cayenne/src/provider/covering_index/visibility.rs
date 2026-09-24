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

//! Source-role visibility adapters for covered physical rows.
//!
//! A covering run stores physical row ordinals. It must therefore apply the
//! same source-specific rule as the ordinary scan before any residual predicate
//! is evaluated. In particular, this is not a global PK deduplication pass:
//! protected and cold files ignore re-insert records, while the current warm
//! source may apply them, and memory/inline sources use their own sequence
//! tombstones.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;

use crate::provider::delete::{InsertRecordHandling, is_pk_visible_i64, is_pk_visible_row_key};
use crate::provider::deletion_strategy::PositionDeletionVector;
use crate::provider::mem_tier::InMemTombstones;
use crate::provider::on_conflict::PkDeletionSnapshot;
use crate::row_converter::RowConverter;

use super::{Error, Result};

/// The scan branch that owns a captured source.
///
/// The role is intentionally retained beside a `SourceId`: the same immutable
/// Vortex file cannot be interpreted correctly without knowing whether it was
/// scanned as current warm data or as an old protected/cold copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceRole {
    /// The current warm snapshot. `InsertRecordHandling` is captured separately
    /// because its value depends on whether protected snapshots are present.
    Warm,
    /// A protected snapshot whose creation sequence is the deletion cutoff.
    Protected {
        /// Only deletes newer than this sequence apply.
        min_delete_sequence: i64,
    },
    /// A cold-manifest source. Its old rows always ignore re-insert records.
    Cold,
    /// An inline metastore batch, interpreted at its durable data sequence.
    Inline {
        /// Sequence assigned when the entry became visible.
        data_sequence: i64,
    },
    /// A memory-tier batch, interpreted at its segment data sequence.
    Memory {
        /// Sequence assigned when the segment became visible.
        data_sequence: i64,
    },
}

/// PK columns and their byte-key encoder retained with a captured scan view.
#[derive(Clone, Debug)]
pub(crate) struct PrimaryKeyLayout {
    columns: Arc<[usize]>,
    row_converter: Option<Arc<RowConverter>>,
}

impl PrimaryKeyLayout {
    /// Construct an Int64 or row-key layout. Empty columns represent a
    /// position-only table and are valid only with position visibility.
    #[must_use]
    pub(crate) fn new(columns: Vec<usize>, row_converter: Option<Arc<RowConverter>>) -> Self {
        Self {
            columns: columns.into(),
            row_converter,
        }
    }

    /// Physical PK column positions in the stored payload schema.
    #[must_use]
    pub(crate) fn columns(&self) -> &[usize] {
        &self.columns
    }
}

/// Complete visibility state for a source in one captured scan view.
#[derive(Clone)]
pub(crate) enum VisibilityAdapter {
    /// Vortex-backed warm, protected, or cold data. Position deletes are
    /// applied after the source role's PK rule, exactly as the ordinary path
    /// applies the Vortex access plan plus its deletion filter.
    File {
        /// Source branch role.
        role: SourceRole,
        /// Frozen key-deletion state, including the captured mem-tier union.
        deletions: PkDeletionSnapshot,
        /// Whether a re-insert can make a tombstoned row visible.
        insert_record_handling: InsertRecordHandling,
        /// Protected-snapshot deletion cutoff, if any.
        min_delete_sequence: Option<i64>,
        /// Physical PK layout required by key-based variants.
        primary_key: PrimaryKeyLayout,
        /// Per-file position vector captured with the deletion snapshot.
        position_deletions: Option<Arc<PositionDeletionVector>>,
    },
    /// A decoded inline entry. Its durable deletion filtering has already been
    /// applied by the captured inline view; this adapter applies the captured
    /// in-memory tombstone union that the ordinary inline scan applies next.
    Inline {
        /// Source branch role.
        role: SourceRole,
        /// Captured cross-shard tombstone union.
        tombstones: InMemTombstones,
        /// Physical PK layout.
        primary_key: PrimaryKeyLayout,
    },
    /// A batch in one captured memory-tier shard. A shard's own tombstones,
    /// rather than the global union, define its ordinary scan visibility.
    Memory {
        /// Source branch role.
        role: SourceRole,
        /// Captured shard tombstones.
        tombstones: InMemTombstones,
        /// Physical PK layout.
        primary_key: PrimaryKeyLayout,
    },
    /// An explicitly unavailable source rule. It prevents `try_cover` from
    /// treating a source as visible-by-default while a producer is not wired.
    Unavailable,
}

impl std::fmt::Debug for VisibilityAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File {
                role,
                min_delete_sequence,
                position_deletions,
                ..
            } => formatter
                .debug_struct("File")
                .field("role", role)
                .field("min_delete_sequence", min_delete_sequence)
                .field("has_position_deletions", &position_deletions.is_some())
                .finish_non_exhaustive(),
            Self::Inline { role, .. } => formatter
                .debug_struct("Inline")
                .field("role", role)
                .finish_non_exhaustive(),
            Self::Memory { role, .. } => formatter
                .debug_struct("Memory")
                .field("role", role)
                .finish_non_exhaustive(),
            Self::Unavailable => formatter.write_str("Unavailable"),
        }
    }
}

impl VisibilityAdapter {
    /// Build the no-PK, no-position-delete file rule. This is useful for a
    /// provably position-only empty-delete source and for page-store tests; PK
    /// tables must retain their captured key-deletion state instead.
    #[must_use]
    pub(crate) fn position_only_file() -> Self {
        Self::File {
            role: SourceRole::Warm,
            deletions: PkDeletionSnapshot::PositionBased,
            insert_record_handling: InsertRecordHandling::Apply,
            min_delete_sequence: None,
            primary_key: PrimaryKeyLayout::new(Vec::new(), None),
            position_deletions: None,
        }
    }

    /// Build a file-source adapter. `role` must be warm, protected, or cold.
    pub(crate) fn file(
        role: SourceRole,
        deletions: PkDeletionSnapshot,
        insert_record_handling: InsertRecordHandling,
        primary_key: PrimaryKeyLayout,
        position_deletions: Option<Arc<PositionDeletionVector>>,
    ) -> Result<Self> {
        let min_delete_sequence = match role {
            SourceRole::Warm | SourceRole::Cold => None,
            SourceRole::Protected {
                min_delete_sequence,
            } => Some(min_delete_sequence),
            SourceRole::Inline { .. } | SourceRole::Memory { .. } => {
                return Err(Error::InvalidContract {
                    message: "inline or memory source role cannot use file visibility".to_string(),
                });
            }
        };
        Ok(Self::File {
            role,
            deletions,
            insert_record_handling,
            min_delete_sequence,
            primary_key,
            position_deletions,
        })
    }

    /// Build an inline-source adapter over the captured cross-tier tombstones.
    pub(crate) fn inline(
        data_sequence: i64,
        tombstones: InMemTombstones,
        primary_key: PrimaryKeyLayout,
    ) -> Self {
        Self::Inline {
            role: SourceRole::Inline { data_sequence },
            tombstones,
            primary_key,
        }
    }

    /// Build a memory-source adapter over its captured shard tombstones.
    pub(crate) fn memory(
        data_sequence: i64,
        tombstones: InMemTombstones,
        primary_key: PrimaryKeyLayout,
    ) -> Self {
        Self::Memory {
            role: SourceRole::Memory { data_sequence },
            tombstones,
            primary_key,
        }
    }

    /// Source role retained for diagnostics and future probe execution.
    #[must_use]
    pub(crate) fn role(&self) -> Option<SourceRole> {
        match self {
            Self::File { role, .. } | Self::Inline { role, .. } | Self::Memory { role, .. } => {
                Some(*role)
            }
            Self::Unavailable => None,
        }
    }

    /// Whether this adapter can reproduce ordinary-source visibility.
    #[must_use]
    pub(crate) const fn is_complete(&self) -> bool {
        !matches!(self, Self::Unavailable)
    }

    /// Return input batch row indexes that remain visible for this source.
    ///
    /// `physical_ordinals` are original source positions, never positions in a
    /// probe candidate batch. A caller must pass one ordinal per batch row even
    /// when a preceding key lookup narrowed the candidates; otherwise a
    /// position-delete vector could remove the wrong record.
    pub(crate) fn select_visible_rows(
        &self,
        batch: &RecordBatch,
        physical_ordinals: &[u64],
    ) -> Result<Vec<usize>> {
        if batch.num_rows() != physical_ordinals.len() {
            return Err(Error::InvalidContract {
                message: format!(
                    "visibility received {} physical ordinals for a {}-row batch",
                    physical_ordinals.len(),
                    batch.num_rows()
                ),
            });
        }

        match self {
            Self::File {
                deletions,
                insert_record_handling,
                min_delete_sequence,
                primary_key,
                position_deletions,
                ..
            } => {
                let visible = select_file_pk_rows(
                    batch,
                    deletions,
                    *insert_record_handling,
                    *min_delete_sequence,
                    primary_key,
                )?;
                apply_position_deletions(visible, physical_ordinals, position_deletions.as_deref())
            }
            Self::Inline {
                role,
                tombstones,
                primary_key,
            } => {
                let SourceRole::Inline { data_sequence } = role else {
                    return Err(Error::InvalidContract {
                        message: "inline visibility has a non-inline source role".to_string(),
                    });
                };
                select_sequence_tombstone_rows(batch, *data_sequence, tombstones, primary_key)
            }
            Self::Memory {
                role,
                tombstones,
                primary_key,
            } => {
                let SourceRole::Memory { data_sequence } = role else {
                    return Err(Error::InvalidContract {
                        message: "memory visibility has a non-memory source role".to_string(),
                    });
                };
                select_sequence_tombstone_rows(batch, *data_sequence, tombstones, primary_key)
            }
            Self::Unavailable => Err(Error::Unavailable {
                operation: "captured source has no visibility adapter".to_string(),
            }),
        }
    }
}

fn select_file_pk_rows(
    batch: &RecordBatch,
    deletions: &PkDeletionSnapshot,
    insert_record_handling: InsertRecordHandling,
    min_delete_sequence: Option<i64>,
    primary_key: &PrimaryKeyLayout,
) -> Result<Vec<usize>> {
    match deletions {
        PkDeletionSnapshot::PositionBased => Ok(all_rows(batch)),
        PkDeletionSnapshot::Int64Pk { tombstones } => {
            let pk = int64_primary_key(batch, primary_key)?;
            Ok(pk
                .values()
                .iter()
                .enumerate()
                .filter_map(|(row, &value)| {
                    is_pk_visible_i64(
                        value,
                        tombstones,
                        insert_record_handling,
                        min_delete_sequence,
                    )
                    .then_some(row)
                })
                .collect())
        }
        PkDeletionSnapshot::RowConverterBased { tombstones } => {
            let rows = encoded_primary_keys(batch, primary_key)?;
            Ok(rows
                .iter()
                .enumerate()
                .filter_map(|(row, key)| {
                    is_pk_visible_row_key(
                        key.as_ref(),
                        tombstones,
                        insert_record_handling,
                        min_delete_sequence,
                    )
                    .then_some(row)
                })
                .collect())
        }
    }
}

fn select_sequence_tombstone_rows(
    batch: &RecordBatch,
    data_sequence: i64,
    tombstones: &InMemTombstones,
    primary_key: &PrimaryKeyLayout,
) -> Result<Vec<usize>> {
    if primary_key.columns().is_empty() {
        return Ok(all_rows(batch));
    }
    if primary_key.columns().len() == 1
        && batch.column(primary_key.columns()[0]).data_type() == &arrow_schema::DataType::Int64
    {
        let pk = int64_primary_key(batch, primary_key)?;
        return Ok(pk
            .values()
            .iter()
            .enumerate()
            .filter_map(|(row, value)| {
                tombstones
                    .int64_pk
                    .get(value)
                    .is_none_or(|delete_sequence| data_sequence > *delete_sequence)
                    .then_some(row)
            })
            .collect());
    }

    let rows = encoded_primary_keys(batch, primary_key)?;
    Ok(rows
        .iter()
        .enumerate()
        .filter_map(|(row, key)| {
            tombstones
                .row_keys
                .get(key.as_ref())
                .is_none_or(|delete_sequence| data_sequence > *delete_sequence)
                .then_some(row)
        })
        .collect())
}

fn apply_position_deletions(
    rows: Vec<usize>,
    physical_ordinals: &[u64],
    position_deletions: Option<&PositionDeletionVector>,
) -> Result<Vec<usize>> {
    let Some(position_deletions) = position_deletions else {
        return Ok(rows);
    };
    let mut visible = Vec::with_capacity(rows.len());
    for row in rows {
        let ordinal = *physical_ordinals
            .get(row)
            .ok_or_else(|| Error::InvalidContract {
                message: format!("visibility row {row} has no physical ordinal"),
            })?;
        let ordinal = u32::try_from(ordinal).map_err(|_| Error::InvalidContract {
            message: format!(
                "position-delete visibility cannot represent physical ordinal {ordinal}"
            ),
        })?;
        if !position_deletions.contains(ordinal) {
            visible.push(row);
        }
    }
    Ok(visible)
}

fn int64_primary_key<'a>(
    batch: &'a RecordBatch,
    primary_key: &PrimaryKeyLayout,
) -> Result<&'a Int64Array> {
    let [column] = primary_key.columns() else {
        return Err(Error::InvalidContract {
            message: "Int64 deletion visibility requires exactly one primary-key column"
                .to_string(),
        });
    };
    let array = batch
        .column(*column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::InvalidContract {
            message: format!(
                "Int64 deletion visibility expected an Int64 primary key at column {column}"
            ),
        })?;
    if array.null_count() != 0 {
        return Err(Error::InvalidContract {
            message: "captured primary-key batch contains NULL values".to_string(),
        });
    }
    Ok(array)
}

fn encoded_primary_keys(
    batch: &RecordBatch,
    primary_key: &PrimaryKeyLayout,
) -> Result<crate::row_converter::Rows> {
    if primary_key.columns().is_empty() {
        return Err(Error::InvalidContract {
            message: "row-key deletion visibility requires primary-key columns".to_string(),
        });
    }
    let converter = primary_key
        .row_converter
        .as_ref()
        .ok_or_else(|| Error::InvalidContract {
            message: "row-key deletion visibility is missing its captured RowConverter".to_string(),
        })?;
    let columns = primary_key
        .columns()
        .iter()
        .map(|column| {
            let array = batch.column(*column);
            if array.null_count() != 0 {
                return Err(Error::InvalidContract {
                    message: "captured primary-key batch contains NULL values".to_string(),
                });
            }
            Ok(Arc::clone(array))
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    converter
        .convert_columns(&columns)
        .map_err(|error| Error::InvalidContract {
            message: format!("failed to encode captured row-key values for visibility: {error}"),
        })
}

fn all_rows(batch: &RecordBatch) -> Vec<usize> {
    (0..batch.num_rows()).collect()
}
