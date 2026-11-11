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
//! A table with a primary key keeps one row per key. A refresh or user
//! statement resolves the keys it repeats across all its batches: the copy that
//! arrived last is kept, or, when the writer supplies row versions, the copy
//! with the greatest version (the later arrival on an equal one). Change
//! streams apply versions in arrival order.
//!
//! [`KeyResolver::resolve_batch`] resolves one batch, and
//! [`KeyResolver::collapse_write`] resolves a buffered statement. A streamed
//! statement resolves the keys it repeats across batches after it is written
//! ([`super::overwrite_postpass`]).

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::Schema;
use hash_index::PrehashedBuildHasher;

use super::pk_index::pk_digest_bytes;
use super::pk_validation::null_primary_key_message;
use super::{Error, Result};
use crate::row_converter::{RowConverter, SortField};
use util::session_state::{SupersededReason, SupersededRows};

/// Seeds the hash [`KeyResolver::may_repeat_within`] checks a batch's keys by.
const REPEAT_CHECK_SEED: u64 = 0x6361_7965_6e6e_6502;

/// A batch with each row's time (UTC nanoseconds).
pub(crate) struct VersionedBatch {
    pub(crate) batch: RecordBatch,
    pub(crate) times: arrow::array::Int64Array,
}

impl VersionedBatch {
    /// This batch and its versions, keeping the rows `keep` selects.
    fn filter(self, keep: &BooleanArray) -> Result<Self> {
        if keep.true_count() == keep.len() {
            return Ok(self);
        }
        let filter = |array: &dyn arrow::array::Array| arrow::compute::filter(array, keep);
        Ok(Self {
            batch: filter_record_batch(&self.batch, keep)?,
            times: arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(
                filter(&self.times)?.as_ref(),
            )
            .clone(),
        })
    }
}

/// Resolves repeated primary keys for one table, keeping the last arrival.
pub(crate) struct KeyResolver {
    table_name: Arc<str>,
    primary_key: Arc<[usize]>,
    keys: RowConverter,
    superseded: Option<Arc<SupersededRows>>,
}

impl std::fmt::Debug for KeyResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyResolver")
            .field("table_name", &self.table_name)
            .field("primary_key", &self.primary_key)
            .finish_non_exhaustive()
    }
}

impl KeyResolver {
    /// # Errors
    ///
    /// Returns an error if a primary key column's type cannot be encoded.
    pub(crate) fn new(table_name: &str, schema: &Schema, primary_key: &[usize]) -> Result<Self> {
        let keys = RowConverter::new(
            primary_key
                .iter()
                .map(|&index| SortField::new(schema.field(index).data_type().clone()))
                .collect(),
        )?;
        Ok(Self {
            table_name: Arc::from(table_name),
            primary_key: primary_key.into(),
            keys,
            superseded: None,
        })
    }

    /// Count the rows this resolver supersedes into `superseded`.
    #[must_use]
    pub(crate) fn counting(mut self, superseded: Option<Arc<SupersededRows>>) -> Self {
        self.superseded = superseded;
        self
    }

    fn count(&self, reason: SupersededReason, rows: u64) {
        if let Some(superseded) = &self.superseded {
            superseded.add(reason, rows);
        }
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

    /// Resolve the keys `batch` repeats within itself, keeping each key's last
    /// copy.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null.
    pub(crate) fn resolve_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut resolved = self.collapse_write(vec![batch.clone()])?;
        Ok(match resolved.pop() {
            Some(resolved) => resolved,
            None => batch.slice(0, 0),
        })
    }

    /// Resolve every repeated key of a write whose batches are all in memory,
    /// keeping each key's last copy and counting every other one. Row order is
    /// preserved.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null.
    pub(crate) fn collapse_write(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut encoded = Vec::with_capacity(batches.len());
        for batch in &batches {
            self.ensure_no_null_key(batch)?;
            encoded.push(self.digests(batch)?);
        }
        // The (batch, row) each key keeps across the write: its last copy.
        let rows: usize = encoded.iter().map(Vec::len).sum();
        let mut survivor: HashMap<u128, (usize, usize), PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(rows, PrehashedBuildHasher);
        for (index, digests) in encoded.iter().enumerate() {
            for (row, &digest) in digests.iter().enumerate() {
                survivor.insert(digest, (index, row));
            }
        }
        let superseded = rows - survivor.len();
        if superseded == 0 {
            return Ok(batches);
        }
        self.count(SupersededReason::Arrival, superseded as u64);
        batches
            .into_iter()
            .zip(&encoded)
            .enumerate()
            .map(|(index, (batch, digests))| {
                let keep: BooleanArray = digests
                    .iter()
                    .enumerate()
                    .map(|(row, digest)| Some(survivor.get(digest) == Some(&(index, row))))
                    .collect();
                if keep.true_count() == keep.len() {
                    Ok(batch)
                } else {
                    Ok(filter_record_batch(&batch, &keep)?)
                }
            })
            .filter(|batch| !matches!(batch, Ok(batch) if batch.num_rows() == 0))
            .collect()
    }

    fn ensure_no_null_key(&self, batch: &RecordBatch) -> Result<()> {
        if self.has_null_key(batch) {
            return Err(Error::DataValidation {
                table: self.table_name.to_string(),
                message: null_primary_key_message(batch, &self.primary_key),
            });
        }
        Ok(())
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

    /// Resolve the keys `batches` repeat by row time rather than arrival alone: each
    /// key keeps its copy with the greatest time (each batch's own `times`), the later
    /// copy on an equal one, and every other copy is counted.
    /// Every batch comes back, filtered to its kept rows in their order, with its
    /// versions filtered alike.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null.
    pub(crate) fn resolve_by_version(
        &self,
        batches: Vec<VersionedBatch>,
    ) -> Result<Vec<VersionedBatch>> {
        let mut digests = Vec::with_capacity(batches.len());
        for versioned in &batches {
            self.ensure_no_null_key(&versioned.batch)?;
            digests.push(self.digests(&versioned.batch)?);
        }
        let rows: usize = digests.iter().map(Vec::len).sum();
        // A NULL time is `None`, older than any time.
        let version = |(index, row): (usize, usize)| {
            let times = &batches[index].times;
            (!times.is_null(row)).then(|| times.value(row))
        };
        let mut kept: HashMap<u128, (usize, usize), PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(rows, PrehashedBuildHasher);
        let mut counts = [0_u64; SupersededReason::ALL.len()];
        let mut count =
            |loser, winner| counts[SupersededReason::of_version(loser, winner) as usize] += 1;
        for (index, batch_digests) in digests.iter().enumerate() {
            for (row, &digest) in batch_digests.iter().enumerate() {
                match kept.entry(digest) {
                    Entry::Vacant(entry) => {
                        entry.insert((index, row));
                    }
                    Entry::Occupied(mut entry) => {
                        let (copy, current) = (version((index, row)), version(*entry.get()));
                        if copy >= current {
                            count(current, copy);
                            entry.insert((index, row));
                        } else {
                            count(copy, current);
                        }
                    }
                }
            }
        }
        if counts.iter().all(|&rows| rows == 0) {
            return Ok(batches);
        }
        for reason in SupersededReason::ALL {
            self.count(reason, counts[reason as usize]);
        }
        let resolved = batches
            .into_iter()
            .zip(&digests)
            .enumerate()
            .map(|(index, (versioned, batch_digests))| {
                let keep: BooleanArray = batch_digests
                    .iter()
                    .enumerate()
                    .map(|(row, digest)| Some(kept.get(digest) == Some(&(index, row))))
                    .collect();
                versioned.filter(&keep)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(resolved)
    }

    fn digests(&self, batch: &RecordBatch) -> Result<Vec<u128>> {
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

    fn resolver() -> KeyResolver {
        KeyResolver::new("t", &schema(), &[0]).expect("resolver")
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

    /// One write of two batches, `[(1,a),(2,b),(1,a)]` then `[(1,c)]`, keeps
    /// each key's last copy and counts the two it drops.
    #[test]
    fn collapse_write_keeps_each_keys_last_copy() {
        let superseded = Arc::new(SupersededRows::default());
        let resolved = resolver()
            .counting(Some(Arc::clone(&superseded)))
            .collapse_write(vec![
                batch(&[(1, "a"), (2, "b"), (1, "a")]),
                batch(&[(1, "c")]),
            ])
            .expect("keep last");
        assert_eq!(rows(&resolved), owned(&[(2, "b"), (1, "c")]));
        assert_eq!(superseded.get(SupersededReason::Arrival), 2);
    }

    #[test]
    fn keep_last_resolves_within_a_batch() {
        let resolved = resolver()
            .resolve_batch(&batch(&[(1, "a"), (2, "b"), (1, "c")]))
            .expect("resolved");
        assert_eq!(rows(&[resolved]), owned(&[(2, "b"), (1, "c")]));
    }

    /// A NULL time is older than [`i64::MIN`], the earliest time a version can hold,
    /// so a later copy with a NULL time does not replace it.
    #[test]
    fn a_null_time_is_older_than_the_minimum_time() {
        let versioned = |rows: &[(i64, &str)], time: Option<i64>| VersionedBatch {
            batch: batch(rows),
            times: std::iter::repeat_n(time, rows.len()).collect::<Int64Array>(),
        };
        let kept = resolver()
            .resolve_by_version(vec![
                versioned(&[(1, "min")], Some(i64::MIN)),
                versioned(&[(1, "null")], None),
            ])
            .expect("resolves");
        let batches: Vec<RecordBatch> = kept.into_iter().map(|v| v.batch).collect();
        assert_eq!(rows(&batches), owned(&[(1, "min")]));
    }

    #[test]
    fn a_null_key_fails_the_write() {
        let nulls = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("batch");
        let error = resolver().resolve_batch(&nulls).expect_err("null key");
        assert!(
            error.to_string().contains("'id' has null values"),
            "{error}"
        );
    }

    #[test]
    fn a_write_without_repeats_is_returned_unchanged() {
        let write = vec![batch(&[(1, "a")]), batch(&[(2, "b")])];
        let resolved = resolver().collapse_write(write.clone()).expect("resolved");
        assert_eq!(resolved, write);
    }
}
