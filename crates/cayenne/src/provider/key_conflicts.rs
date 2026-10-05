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

//! Primary keys repeated inside the incoming data of one write.
//!
//! Each incoming record batch is one upsert statement:
//!
//! | policy                   | repeat within a batch        | repeat across batches |
//! |--------------------------|------------------------------|-----------------------|
//! | `drop`                   | first copy kept              | first copy kept       |
//! | `upsert`                 | last copy wins               | last copy wins        |
//! | `upsert_dedup`           | last copy wins               | last copy wins        |
//! | `upsert_dedup_by_row_id` | last copy wins               | last copy wins        |
//!
//! A refresh never fails on a key its data repeats, whatever the policy and
//! wherever the copies fall: every upsert policy keeps the last copy, as conflict
//! validation does for a statement's batch (`UpsertOptions::last_write_wins`).
//!
//! [`KeyResolver::resolve_batch`] applies the within-batch column, and
//! [`KeyResolver::collapse_write`] applies both columns to a write whose batches
//! are all in memory.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::Schema;
use datafusion_table_providers::util::on_conflict::OnConflict;
use hash_index::PrehashedBuildHasher;

use super::pk_index::pk_digest_bytes;
use super::pk_validation::null_primary_key_message;
use super::{Error, Result};

/// Seeds the hash [`KeyResolver::may_repeat_within`] checks a batch's keys by.
const REPEAT_CHECK_SEED: u64 = 0x6361_7965_6e6e_6502;
use crate::row_converter::{RowConverter, SortField};

/// The `upsert` refinement a dataset's `on_conflict` selects, which the table's
/// stored `OnConflict` cannot express. A refresh resolves every key it repeats
/// whichever is selected (see [`ConflictPolicy::new`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UpsertDedup {
    /// Plain `upsert` (or `drop`, which has no refinement).
    #[default]
    None,
    /// `upsert_dedup`: identical rows within a batch collapse to one.
    DropIdentical,
    /// `upsert_dedup_by_row_id`: the last row for a key within a batch wins.
    KeepLast,
}

/// How a write resolves an incoming key that it has already seen; see the module
/// documentation for the full table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictPolicy {
    /// `drop`: the first copy of a key is kept, within and across batches.
    KeepFirst,
    /// `upsert`, `upsert_dedup` and `upsert_dedup_by_row_id`: the last copy wins,
    /// within and across batches.
    UpsertKeepLast,
}

impl ConflictPolicy {
    /// The policy for a table's `on_conflict`, or `None` when it has none.
    pub(crate) fn new(on_conflict: Option<&OnConflict>, dedup: UpsertDedup) -> Option<Self> {
        Some(match on_conflict? {
            OnConflict::DoNothing(_) | OnConflict::DoNothingAll => Self::KeepFirst,
            // A refresh resolves every repeat, so the refinements that only
            // decide which within-batch repeats a statement tolerates do not apply.
            OnConflict::Upsert(_) => match dedup {
                UpsertDedup::None | UpsertDedup::DropIdentical | UpsertDedup::KeepLast => {
                    Self::UpsertKeepLast
                }
            },
        })
    }

    /// Whether a later batch's copy of a key replaces an earlier batch's copy
    /// (rather than being dropped).
    pub(crate) fn last_batch_wins(self) -> bool {
        !matches!(self, Self::KeepFirst)
    }
}

/// Which copy of a repeated key a write keeps: the last (the upsert policies)
/// or the first (`drop`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Survivor {
    Latest,
    Earliest,
}

impl Survivor {
    pub(crate) fn for_policy(policy: ConflictPolicy) -> Self {
        match policy {
            ConflictPolicy::KeepFirst => Self::Earliest,
            ConflictPolicy::UpsertKeepLast => Self::Latest,
        }
    }
}

/// A batch with its repeated keys resolved.
#[derive(Debug)]
pub(crate) struct ResolvedBatch {
    pub(crate) batch: RecordBatch,
    /// The key digest of each row of `batch`, in row order; all distinct.
    pub(crate) digests: Vec<u128>,
}

/// Resolves repeated primary keys for one table under one [`ConflictPolicy`].
pub(crate) struct KeyResolver {
    table_name: Arc<str>,
    primary_key: Arc<[usize]>,
    policy: ConflictPolicy,
    keys: RowConverter,
}

impl std::fmt::Debug for KeyResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyResolver")
            .field("table_name", &self.table_name)
            .field("primary_key", &self.primary_key)
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl KeyResolver {
    /// # Errors
    ///
    /// Returns an error if a primary key column's type cannot be encoded.
    pub(crate) fn new(
        table_name: &str,
        schema: &Schema,
        primary_key: &[usize],
        policy: ConflictPolicy,
    ) -> Result<Self> {
        let keys = RowConverter::new(
            primary_key
                .iter()
                .map(|&index| SortField::new(schema.field(index).data_type().clone()))
                .collect(),
        )?;
        Ok(Self {
            table_name: Arc::from(table_name),
            primary_key: primary_key.into(),
            policy,
            keys,
        })
    }


    pub(crate) fn policy(&self) -> ConflictPolicy {
        self.policy
    }

    /// Whether a primary key column of `batch` holds a null.
    pub(crate) fn has_null_key(&self, batch: &RecordBatch) -> bool {
        self.primary_key
            .iter()
            .any(|&index| batch.column(index).null_count() > 0)
    }

    /// Whether `batch` may hold a key more than once: two of its rows share a
    /// 64-bit hash of the key. When it returns `false`, every key of the batch
    /// is distinct, so [`Self::resolve_batch`] would return it unchanged. A
    /// shared hash is only a candidate; [`Self::resolve_batch`] decides exactly.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key column cannot be hashed.
    pub(crate) fn may_repeat_within(&self, batch: &RecordBatch) -> Result<bool> {
        let state = datafusion_common::hash_utils::RandomState::with_seed(REPEAT_CHECK_SEED);
        Ok(datafusion_common::hash_utils::with_hashes(
            self.primary_key.iter().map(|&index| batch.column(index)),
            &state,
            |hashes| {
                let mut seen: std::collections::HashSet<u64, PrehashedBuildHasher> =
                    std::collections::HashSet::with_capacity_and_hasher(
                        hashes.len(),
                        PrehashedBuildHasher,
                    );
                Ok(!hashes.iter().all(|hash| seen.insert(*hash)))
            },
        )?)
    }

    /// Resolve the keys `batch` repeats within itself, per the policy's
    /// within-batch rule.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null, or the policy rejects a repeat.
    pub(crate) fn resolve_batch(&self, batch: &RecordBatch) -> Result<ResolvedBatch> {
        if self
            .primary_key
            .iter()
            .any(|&index| batch.column(index).null_count() > 0)
        {
            return Err(Error::DataValidation {
                table: self.table_name.to_string(),
                message: null_primary_key_message(batch, &self.primary_key),
            });
        }
        let digests = self.digests(batch)?;
        // The row each key keeps: its first copy under `drop`, else its last.
        let mut survivor: HashMap<u128, usize, PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(digests.len(), PrehashedBuildHasher);
        let mut repeated = false;
        for (row, &digest) in digests.iter().enumerate() {
            match survivor.entry(digest) {
                Entry::Vacant(entry) => {
                    entry.insert(row);
                }
                Entry::Occupied(mut entry) => {
                    repeated = true;
                    match self.policy {
                        ConflictPolicy::KeepFirst => {}
                        ConflictPolicy::UpsertKeepLast => {
                            entry.insert(row);
                        }
                    }
                }
            }
        }
        if !repeated {
            return Ok(ResolvedBatch {
                batch: batch.clone(),
                digests,
            });
        }
        let keep: BooleanArray = digests
            .iter()
            .enumerate()
            .map(|(row, digest)| Some(survivor.get(digest) == Some(&row)))
            .collect();
        let digests = digests
            .into_iter()
            .zip(keep.values().iter())
            .filter_map(|(digest, kept)| kept.then_some(digest))
            .collect();
        Ok(ResolvedBatch {
            batch: filter_record_batch(batch, &keep)?,
            digests,
        })
    }

    /// Resolve every repeated key of a write whose batches are all in memory:
    /// within each batch per the policy, then across batches (the first copy
    /// under `drop`, the last otherwise). Row order is preserved.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null, or the policy rejects a repeat.
    pub(crate) fn collapse_write(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let resolved = batches
            .into_iter()
            .map(|batch| self.resolve_batch(&batch))
            .collect::<Result<Vec<_>>>()?;
        // The (batch, row) each key keeps across the write.
        let rows: usize = resolved.iter().map(|batch| batch.digests.len()).sum();
        let mut survivor: HashMap<u128, (usize, usize), PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(rows, PrehashedBuildHasher);
        let last_batch_wins = self.policy.last_batch_wins();
        let mut repeated = false;
        for (index, batch) in resolved.iter().enumerate() {
            for (row, &digest) in batch.digests.iter().enumerate() {
                match survivor.entry(digest) {
                    Entry::Vacant(entry) => {
                        entry.insert((index, row));
                    }
                    Entry::Occupied(mut entry) => {
                        repeated = true;
                        if last_batch_wins {
                            entry.insert((index, row));
                        }
                    }
                }
            }
        }
        if !repeated {
            return Ok(resolved.into_iter().map(|batch| batch.batch).collect());
        }
        resolved
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                let keep: BooleanArray = batch
                    .digests
                    .iter()
                    .enumerate()
                    .map(|(row, digest)| Some(survivor.get(digest) == Some(&(index, row))))
                    .collect();
                if keep.true_count() == keep.len() {
                    Ok(batch.batch)
                } else {
                    Ok(filter_record_batch(&batch.batch, &keep)?)
                }
            })
            .filter(|batch| !matches!(batch, Ok(batch) if batch.num_rows() == 0))
            .collect()
    }

    /// The table's encoding of each row's primary key (the `RowConverter` bytes
    /// key-based tombstones store).
    fn encode_keys(&self, batch: &RecordBatch) -> Result<crate::row_converter::Rows> {
        let columns: Vec<ArrayRef> = self
            .primary_key
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        Ok(self.keys.convert_columns(&columns)?)
    }

    pub(crate) fn digests(&self, batch: &RecordBatch) -> Result<Vec<u128>> {
        let keys = self.encode_keys(batch)?;
        Ok(keys
            .iter()
            .map(|key| pk_digest_bytes(key.as_ref()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int64Type};
    use datafusion_table_providers::util::column_reference::ColumnReference;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|(_, v)| *v))),
            ],
        )
        .expect("batch")
    }

    fn resolver(policy: ConflictPolicy) -> KeyResolver {
        KeyResolver::new("t", &schema(), &[0], policy).expect("resolver")
    }

    fn rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
        batches
            .iter()
            .flat_map(|batch| {
                let ids = batch.column(0).as_primitive::<Int64Type>();
                let values = batch.column(1).as_string::<i32>();
                (0..batch.num_rows())
                    .map(|row| (ids.value(row), values.value(row).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn owned(expected: &[(i64, &str)]) -> Vec<(i64, String)> {
        expected
            .iter()
            .map(|(id, v)| (*id, (*v).to_string()))
            .collect()
    }

    #[test]
    fn policy_follows_on_conflict_and_dedup() {
        let upsert = OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]));
        let drop = OnConflict::DoNothing(ColumnReference::new(vec!["id".to_string()]));
        assert_eq!(ConflictPolicy::new(None, UpsertDedup::KeepLast), None);
        assert_eq!(
            ConflictPolicy::new(Some(&drop), UpsertDedup::None),
            Some(ConflictPolicy::KeepFirst)
        );
        assert_eq!(
            ConflictPolicy::new(Some(&upsert), UpsertDedup::None),
            Some(ConflictPolicy::UpsertKeepLast)
        );
        assert_eq!(
            ConflictPolicy::new(Some(&upsert), UpsertDedup::DropIdentical),
            Some(ConflictPolicy::UpsertKeepLast)
        );
        assert_eq!(
            ConflictPolicy::new(Some(&upsert), UpsertDedup::KeepLast),
            Some(ConflictPolicy::UpsertKeepLast)
        );
    }

    /// The documented `on_conflict` table, one write of two batches:
    /// `[(1,a),(2,b),(1,a)]` then `[(1,c)]`.
    #[test]
    fn collapse_write_follows_the_documented_table() {
        let write = || vec![batch(&[(1, "a"), (2, "b"), (1, "a")]), batch(&[(1, "c")])];
        assert_eq!(
            rows(
                &resolver(ConflictPolicy::KeepFirst)
                    .collapse_write(write())
                    .expect("drop")
            ),
            owned(&[(1, "a"), (2, "b")])
        );
        assert_eq!(
            rows(
                &resolver(ConflictPolicy::UpsertKeepLast)
                    .collapse_write(write())
                    .expect("keep last")
            ),
            owned(&[(2, "b"), (1, "c")])
        );
    }

    #[test]
    fn keep_last_resolves_within_a_batch() {
        let resolved = resolver(ConflictPolicy::UpsertKeepLast)
            .resolve_batch(&batch(&[(1, "a"), (2, "b"), (1, "c")]))
            .expect("resolved");
        assert_eq!(rows(&[resolved.batch]), owned(&[(2, "b"), (1, "c")]));
        assert_eq!(resolved.digests.len(), 2);
    }

    #[test]
    fn a_null_key_fails_every_policy() {
        let nulls = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("batch");
        for policy in [
            ConflictPolicy::KeepFirst,
            ConflictPolicy::UpsertKeepLast,
        ] {
            let error = resolver(policy)
                .resolve_batch(&nulls)
                .expect_err("null key");
            assert!(
                error.to_string().contains("'id' has null values"),
                "{policy:?}: {error}"
            );
        }
    }

    #[test]
    fn a_write_without_repeats_is_returned_unchanged() {
        let write = vec![batch(&[(1, "a")]), batch(&[(2, "b")])];
        let resolved = resolver(ConflictPolicy::UpsertKeepLast)
            .collapse_write(write.clone())
            .expect("resolved");
        assert_eq!(resolved, write);
    }
}
