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
//! A refresh or user statement resolves repeated keys across all its batches.
//! Identical rows collapse under every policy. Different versions fail under
//! `upsert`, keep the last arrival under `upsert_by_arrival`, and keep the first
//! under `drop`. Change streams apply versions in arrival order.
//!
//! [`KeyResolver::resolve_batch`] resolves one batch, and
//! [`KeyResolver::collapse_write`] resolves a buffered statement. A streamed
//! statement resolves the keys it repeats across batches after it is written
//! ([`super::overwrite_postpass`]).

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeListArray, GenericListArray,
    GenericListViewArray, OffsetSizeTrait, RecordBatch, StructArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use arrow::error::ArrowError;
use datafusion_table_providers::util::on_conflict::OnConflict;
use hash_index::PrehashedBuildHasher;

use super::pk_index::pk_digest_bytes;
use super::pk_validation::null_primary_key_message;
use super::{Error, Result};
use crate::row_converter::{RowConverter, SortField};
use util::session_state::{SupersededReason, SupersededRows};

/// Seeds the hash [`KeyResolver::may_repeat_within`] checks a batch's keys by.
const REPEAT_CHECK_SEED: u64 = 0x6361_7965_6e6e_6502;

/// The upsert policy carried separately from the catalog's `OnConflict`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum UpsertDedup {
    /// Plain `upsert` (or `drop`, which has no refinement).
    #[default]
    None,
    /// `upsert_dedup`: the deprecated alias of strict `upsert`.
    DropIdentical,
    /// `upsert_by_arrival` and its deprecated alias `upsert_dedup_by_row_id`.
    KeepLast,
}

/// How a write resolves an incoming key that it has already seen; see the module
/// documentation for the full table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictPolicy {
    /// `drop`: the first copy of a key is kept, within and across batches.
    KeepFirst,
    /// `upsert`: identical rows collapse; different versions are ambiguous.
    UpsertIdentical,
    /// `upsert_by_arrival`: the last copy wins, within and across batches.
    UpsertKeepLast,
}

impl ConflictPolicy {
    /// The policy for a table's `on_conflict`, or `None` when it has none.
    pub(crate) fn new(on_conflict: Option<&OnConflict>, dedup: UpsertDedup) -> Option<Self> {
        Some(match on_conflict? {
            OnConflict::DoNothing(_) | OnConflict::DoNothingAll => Self::KeepFirst,
            OnConflict::Upsert(_) => match dedup {
                UpsertDedup::None | UpsertDedup::DropIdentical => Self::UpsertIdentical,
                UpsertDedup::KeepLast => Self::UpsertKeepLast,
            },
        })
    }

    /// Whether a later batch's copy of a key replaces an earlier batch's copy
    /// (rather than being dropped).
    pub(crate) fn last_batch_wins(self) -> bool {
        !matches!(self, Self::KeepFirst)
    }
}

/// Which incoming version a write keeps, including strict equality validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Survivor {
    Latest,
    Earliest,
    Identical,
}

impl Survivor {
    pub(crate) fn for_policy(policy: ConflictPolicy) -> Self {
        match policy {
            ConflictPolicy::KeepFirst => Self::Earliest,
            ConflictPolicy::UpsertIdentical => Self::Identical,
            ConflictPolicy::UpsertKeepLast => Self::Latest,
        }
    }
}

/// Resolves repeated primary keys for one table under one [`ConflictPolicy`].
pub(crate) struct KeyResolver {
    table_name: Arc<str>,
    key_names: Arc<str>,
    primary_key: Arc<[usize]>,
    policy: ConflictPolicy,
    keys: RowConverter,
    contents: ContentEncoder,
    superseded: Option<Arc<SupersededRows>>,
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
            key_names: primary_key
                .iter()
                .map(|&index| schema.field(index).name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
                .into(),
            primary_key: primary_key.into(),
            policy,
            keys,
            contents: ContentEncoder::new(schema)?,
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

    pub(crate) fn policy(&self) -> ConflictPolicy {
        self.policy
    }

    /// Change streams apply each successive version rather than rejecting it.
    pub(crate) fn for_changes(mut self) -> Self {
        if self.policy == ConflictPolicy::UpsertIdentical {
            self.policy = ConflictPolicy::UpsertKeepLast;
        }
        self
    }

    pub(crate) fn conflicting_versions(&self, count: usize) -> Error {
        conflicting_versions(&self.table_name, &self.key_names, count)
    }

    /// Each row's content identity over the acceleration schema; two rows share
    /// one exactly when every column `IS NOT DISTINCT FROM` its counterpart
    /// (up to a 128-bit digest collision).
    pub(crate) fn content_digests(&self, batch: &RecordBatch) -> Result<Vec<u128>> {
        self.contents.digests(batch)
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
    pub(crate) fn resolve_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut resolved = self.collapse_write(vec![batch.clone()])?;
        Ok(match resolved.pop() {
            Some(resolved) => resolved,
            None => batch.slice(0, 0),
        })
    }

    /// Resolve every repeated key of a write whose batches are all in memory:
    /// the first copy under `drop`, the last otherwise. Under `upsert` the write
    /// fails when any key holds different versions, counting every such key.
    /// Row order is preserved.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null, or the policy rejects a repeat.
    pub(crate) fn collapse_write(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut encoded = Vec::with_capacity(batches.len());
        for batch in &batches {
            self.ensure_no_null_key(batch)?;
            encoded.push((self.digests(batch)?, self.content_digests(batch)?));
        }
        // The (batch, row) each key keeps across the write.
        let rows: usize = encoded.iter().map(|(digests, _)| digests.len()).sum();
        let mut survivor: HashMap<u128, (usize, usize), PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(rows, PrehashedBuildHasher);
        let last_wins = self.policy.last_batch_wins();
        let mut conflicts = HashSet::with_hasher(PrehashedBuildHasher);
        let mut repeated = false;
        for (index, (digests, contents)) in encoded.iter().enumerate() {
            for (row, &digest) in digests.iter().enumerate() {
                match survivor.entry(digest) {
                    Entry::Vacant(entry) => {
                        entry.insert((index, row));
                    }
                    Entry::Occupied(mut entry) => {
                        repeated = true;
                        let (previous_batch, previous_row) = *entry.get();
                        if self.policy == ConflictPolicy::UpsertIdentical
                            && encoded[previous_batch].1[previous_row] != contents[row]
                        {
                            conflicts.insert(digest);
                        }
                        if last_wins {
                            entry.insert((index, row));
                        }
                    }
                }
            }
        }
        if !conflicts.is_empty() {
            return Err(self.conflicting_versions(conflicts.len()));
        }
        if !repeated {
            return Ok(batches);
        }
        let (mut unchanged, mut arrival) = (0_u64, 0_u64);
        for (index, (digests, contents)) in encoded.iter().enumerate() {
            for (row, digest) in digests.iter().enumerate() {
                let Some(&(kept_batch, kept_row)) = survivor.get(digest) else {
                    continue;
                };
                if (kept_batch, kept_row) == (index, row) {
                    continue;
                }
                if encoded[kept_batch].1[kept_row] == contents[row] {
                    unchanged += 1;
                } else {
                    arrival += 1;
                }
            }
        }
        self.count(SupersededReason::Unchanged, unchanged);
        self.count(SupersededReason::Arrival, arrival);
        batches
            .into_iter()
            .zip(&encoded)
            .enumerate()
            .map(|(index, (batch, (digests, _)))| {
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

    /// One batch of a strict `upsert` resolved after its write, as batches that
    /// each hold a key once: identical copies collapse, and each further version
    /// of a key goes to a batch of its own, so the post-write resolution finds,
    /// and counts once, every key with different versions anywhere in the write.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary key is null.
    pub(crate) fn split_versions(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        self.ensure_no_null_key(batch)?;
        let digests = self.digests(batch)?;
        let contents = self.content_digests(batch)?;
        // Each key's distinct contents, in arrival order; a row's level is the
        // position of its content there, or `None` for a repeated copy.
        let mut versions: HashMap<u128, Vec<u128>, PrehashedBuildHasher> =
            HashMap::with_capacity_and_hasher(digests.len(), PrehashedBuildHasher);
        let levels: Vec<Option<usize>> = digests
            .iter()
            .zip(&contents)
            .map(|(digest, content)| {
                let seen = versions.entry(*digest).or_default();
                if seen.contains(content) {
                    None
                } else {
                    seen.push(*content);
                    Some(seen.len() - 1)
                }
            })
            .collect();
        self.count(
            SupersededReason::Unchanged,
            levels.iter().filter(|level| level.is_none()).count() as u64,
        );
        let deepest = levels.iter().flatten().copied().max().unwrap_or(0);
        if deepest == 0 && levels.iter().all(Option::is_some) {
            return Ok(vec![batch.clone()]);
        }
        (0..=deepest)
            .map(|level| {
                let keep: BooleanArray = levels
                    .iter()
                    .map(|row| Some(*row == Some(level)))
                    .collect();
                Ok(filter_record_batch(batch, &keep)?)
            })
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

    pub(crate) fn digests(&self, batch: &RecordBatch) -> Result<Vec<u128>> {
        let keys = self.encode_keys(batch)?;
        Ok(keys
            .iter()
            .map(|key| pk_digest_bytes(key.as_ref()))
            .collect())
    }
}

pub(crate) fn conflicting_versions(table: &str, key_names: &str, count: usize) -> Error {
    let digits = count.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    let noun = if count == 1 { "value" } else { "values" };
    Error::DataValidation {
        table: table.to_string(),
        message: format!(
            "its data holds different versions of {formatted} {noun} of '{key_names}', and `on_conflict: upsert` does not choose between versions. Set `on_conflict` to `upsert_by_arrival` to keep the version that arrived last. See: https://spiceai.org/docs/features/data-acceleration/constraints"
        ),
    }
}

/// Encodes each row's content as Arrow's row format over every column, hashed
/// to Cayenne's 128-bit digest. The row format orders floats by IEEE 754
/// `totalOrder` and nested values element by element, as `IS NOT DISTINCT FROM`
/// compares them. It cannot encode a `Map` or a dictionary of nested values, so
/// those are first rewritten as the value the comparison sees: a map as the list
/// of its entries, a dictionary as its values.
struct ContentEncoder {
    /// The type each column is encoded as, where it is not its own.
    rewrites: Vec<Option<DataType>>,
    rows: arrow::row::RowConverter,
}

impl ContentEncoder {
    fn new(schema: &Schema) -> Result<Self> {
        let rewrites: Vec<Option<DataType>> = schema
            .fields()
            .iter()
            .map(|field| encodable_type(field.data_type()))
            .collect();
        let rows = arrow::row::RowConverter::new(
            schema
                .fields()
                .iter()
                .zip(&rewrites)
                .map(|(field, rewrite)| {
                    arrow::row::SortField::new(
                        rewrite.clone().unwrap_or_else(|| field.data_type().clone()),
                    )
                })
                .collect(),
        )?;
        Ok(Self { rewrites, rows })
    }

    fn digests(&self, batch: &RecordBatch) -> Result<Vec<u128>> {
        if batch.num_columns() != self.rewrites.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "content identity expects {} columns, the batch has {}",
                self.rewrites.len(),
                batch.num_columns()
            ))
            .into());
        }
        let columns = batch
            .columns()
            .iter()
            .zip(&self.rewrites)
            .map(|(column, rewrite)| match rewrite {
                Some(target) => encodable_array(column, target),
                None => Ok(Arc::clone(column)),
            })
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
        let rows = self.rows.convert_columns(&columns)?;
        Ok(rows
            .iter()
            .map(|row| pk_digest_bytes(row.as_ref()))
            .collect())
    }
}

/// The type the row format encodes a value of `data_type` as, when it cannot
/// encode `data_type` itself.
fn encodable_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Map(entries, _) => Some(DataType::List(
            encodable_field(entries).unwrap_or_else(|| Arc::clone(entries)),
        )),
        DataType::Dictionary(_, values) if values.is_nested() => {
            Some(encodable_type(values).unwrap_or_else(|| values.as_ref().clone()))
        }
        DataType::List(field) => encodable_field(field).map(DataType::List),
        DataType::LargeList(field) => encodable_field(field).map(DataType::LargeList),
        DataType::ListView(field) => encodable_field(field).map(DataType::ListView),
        DataType::LargeListView(field) => encodable_field(field).map(DataType::LargeListView),
        DataType::FixedSizeList(field, size) => {
            encodable_field(field).map(|field| DataType::FixedSizeList(field, *size))
        }
        DataType::Struct(fields) => {
            let rewritten: Vec<Option<FieldRef>> = fields.iter().map(encodable_field).collect();
            rewritten.iter().any(Option::is_some).then(|| {
                DataType::Struct(
                    fields
                        .iter()
                        .zip(rewritten)
                        .map(|(field, rewrite)| rewrite.unwrap_or_else(|| Arc::clone(field)))
                        .collect::<Fields>(),
                )
            })
        }
        _ => None,
    }
}

fn encodable_field(field: &FieldRef) -> Option<FieldRef> {
    encodable_type(field.data_type())
        .map(|data_type| Arc::new(Field::clone(field).with_data_type(data_type)))
}

/// `array` as `target`, the type [`encodable_type`] gave for it or for a
/// column it nests.
fn encodable_array(array: &ArrayRef, target: &DataType) -> std::result::Result<ArrayRef, ArrowError> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    Ok(match (array.data_type(), target) {
        (DataType::Dictionary(_, values), _) => {
            return encodable_array(&arrow::compute::cast(array, values)?, target);
        }
        (DataType::Map(..), DataType::List(field)) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            Arc::new(GenericListArray::<i32>::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                encodable_array(&entries, field.data_type())?,
                map.nulls().cloned(),
            )?)
        }
        (DataType::List(_), DataType::List(field)) => encodable_list::<i32>(array, field)?,
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            encodable_list::<i64>(array, field)?
        }
        (DataType::ListView(_), DataType::ListView(field)) => {
            encodable_list_view::<i32>(array, field)?
        }
        (DataType::LargeListView(_), DataType::LargeListView(field)) => {
            encodable_list_view::<i64>(array, field)?
        }
        (DataType::FixedSizeList(..), DataType::FixedSizeList(field, size)) => {
            let list = array.as_fixed_size_list();
            Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                encodable_array(list.values(), field.data_type())?,
                list.nulls().cloned(),
            )?)
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| encodable_array(column, field.data_type()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                structs.nulls().cloned(),
            )?)
        }
        (from, to) => {
            return Err(ArrowError::NotYetImplemented(format!(
                "content identity of {from} encoded as {to}"
            )));
        }
    })
}

fn encodable_list<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &FieldRef,
) -> std::result::Result<ArrayRef, ArrowError> {
    let list = array.as_list::<O>();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        list.offsets().clone(),
        encodable_array(list.values(), field.data_type())?,
        list.nulls().cloned(),
    )?))
}

fn encodable_list_view<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &FieldRef,
) -> std::result::Result<ArrayRef, ArrowError> {
    let list = array.as_list_view::<O>();
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        Arc::clone(field),
        list.offsets().clone(),
        list.sizes().clone(),
        encodable_array(list.values(), field.data_type())?,
        list.nulls().cloned(),
    )?))
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
            Some(ConflictPolicy::UpsertIdentical)
        );
        assert_eq!(
            ConflictPolicy::new(Some(&upsert), UpsertDedup::DropIdentical),
            Some(ConflictPolicy::UpsertIdentical)
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
        assert_eq!(rows(&[resolved]), owned(&[(2, "b"), (1, "c")]));
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
        for policy in [ConflictPolicy::KeepFirst, ConflictPolicy::UpsertKeepLast] {
            let error = resolver(policy)
                .resolve_batch(&nulls)
                .expect_err("null key");
            assert!(
                error.to_string().contains("'id' has null values"),
                "{policy:?}: {error}"
            );
        }
    }

    /// One-row arrays of a column, several of them the same value in another
    /// physical form: a NULL over different child data, a dictionary key into a
    /// repeated value.
    fn content_variants() -> Vec<(&'static str, Vec<ArrayRef>)> {
        use arrow::array::{
            DictionaryArray, Float64Array, Int16Array, Int32Array, ListArray, MapArray,
        };
        use arrow::buffer::{NullBuffer, OffsetBuffer};
        use arrow::datatypes::Int32Type;

        let int32 = |values: &[Option<i32>]| Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
        let utf8 = |values: &[Option<&str>]| Arc::new(StringArray::from(values.to_vec())) as ArrayRef;
        let null = |valid: bool| (!valid).then(|| NullBuffer::new_null(1));
        let item = Arc::new(Field::new("item", DataType::Int32, true));
        let list = |values: &[Option<i32>], valid: bool| {
            Arc::new(ListArray::new(
                Arc::clone(&item),
                OffsetBuffer::from_lengths([values.len()]),
                int32(values),
                null(valid),
            )) as ArrayRef
        };
        let entry_fields = Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Int32, true),
        ]);
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entry_fields.clone()),
            false,
        ));
        let map = |entries: &[(&str, Option<i32>)], valid: bool| {
            let keys: Vec<Option<&str>> = entries.iter().map(|(key, _)| Some(*key)).collect();
            let values: Vec<Option<i32>> = entries.iter().map(|(_, value)| *value).collect();
            Arc::new(MapArray::new(
                Arc::clone(&entries_field),
                OffsetBuffer::from_lengths([entries.len()]),
                StructArray::new(entry_fields.clone(), vec![utf8(&keys), int32(&values)], None),
                null(valid),
                false,
            )) as ArrayRef
        };
        let pair_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let pair = |a: Option<i32>, b: Option<&str>, valid: bool| {
            Arc::new(StructArray::new(
                pair_fields.clone(),
                vec![int32(&[a]), utf8(&[b])],
                null(valid),
            )) as ArrayRef
        };
        let float = |value: Option<f64>| Arc::new(Float64Array::from(vec![value])) as ArrayRef;
        let dictionary = |key: Option<i32>| {
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![key]),
                    Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("x"), None])),
                )
                .expect("dictionary"),
            ) as ArrayRef
        };
        let list_dictionary = |key: Option<i16>| {
            Arc::new(
                DictionaryArray::<arrow::datatypes::Int16Type>::try_new(
                    Int16Array::from(vec![key]),
                    Arc::new(ListArray::new(
                        Arc::clone(&item),
                        OffsetBuffer::from_lengths([1, 2, 1]),
                        int32(&[Some(1), Some(1), None, Some(1)]),
                        None,
                    )),
                )
                .expect("dictionary of lists"),
            ) as ArrayRef
        };
        let map_in_struct_fields = Fields::from(vec![Field::new(
            "m",
            map(&[], true).data_type().clone(),
            true,
        )]);
        let map_in_struct = |entries: &[(&str, Option<i32>)], valid: bool| {
            Arc::new(StructArray::new(
                map_in_struct_fields.clone(),
                vec![map(entries, valid)],
                None,
            )) as ArrayRef
        };
        vec![
            (
                "float",
                [
                    None,
                    Some(0.0),
                    Some(-0.0),
                    Some(f64::NAN),
                    Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                    Some(1.5),
                ]
                .into_iter()
                .map(float)
                .collect(),
            ),
            (
                "utf8",
                [None, Some(""), Some("a"), Some("ab")]
                    .map(|value| utf8(&[value]))
                    .to_vec(),
            ),
            (
                "dictionary",
                [None, Some(0), Some(1), Some(2), Some(3)]
                    .map(dictionary)
                    .to_vec(),
            ),
            (
                "struct",
                vec![
                    pair(None, None, false),
                    pair(Some(7), Some("junk"), false),
                    pair(None, None, true),
                    pair(Some(1), Some("x"), true),
                    pair(Some(1), None, true),
                    pair(Some(2), Some("x"), true),
                ],
            ),
            (
                "list",
                vec![
                    list(&[], false),
                    list(&[Some(5), Some(6)], false),
                    list(&[], true),
                    list(&[Some(1)], true),
                    list(&[Some(1), None], true),
                    list(&[None, Some(1)], true),
                ],
            ),
            (
                "map",
                vec![
                    map(&[], false),
                    map(&[("z", Some(9))], false),
                    map(&[], true),
                    map(&[("a", Some(1))], true),
                    map(&[("a", None)], true),
                    map(&[("a", Some(1)), ("b", Some(2))], true),
                    map(&[("b", Some(2)), ("a", Some(1))], true),
                ],
            ),
            (
                "list_dictionary",
                [None, Some(0), Some(1), Some(2)]
                    .map(list_dictionary)
                    .to_vec(),
            ),
            (
                "map_in_struct",
                vec![
                    map_in_struct(&[], false),
                    map_in_struct(&[("z", Some(9))], false),
                    map_in_struct(&[("a", Some(1))], true),
                    map_in_struct(&[("a", Some(2))], true),
                ],
            ),
        ]
    }

    /// Content identity, which strict `upsert` collapses identical copies by,
    /// agrees with DataFusion's `IS NOT DISTINCT FROM` on every pair of rows,
    /// column by column and over whole rows.
    #[tokio::test]
    async fn content_identity_agrees_with_is_not_distinct_from() {
        use arrow::array::UInt32Array;
        use datafusion::prelude::SessionContext;

        const SEED: u64 = 0x5eed_c047_e470_1005;
        const ROWS: usize = 48;
        let mut state = SEED;
        let mut next = move |bound: usize| {
            // xorshift64*
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            usize::try_from(state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 33).expect("fits")
                % bound
        };
        let variants = content_variants();
        // Rows built from a few templates, so whole rows repeat too.
        let templates: Vec<Vec<usize>> = (0..4)
            .map(|_| variants.iter().map(|(_, column)| next(column.len())).collect())
            .collect();
        let mut chosen: Vec<Vec<usize>> = vec![Vec::with_capacity(ROWS); variants.len()];
        for _ in 0..ROWS {
            let template = &templates[next(templates.len())];
            for (column, (_, values)) in variants.iter().enumerate() {
                let variant = if next(4) == 0 {
                    next(values.len())
                } else {
                    template[column]
                };
                chosen[column].push(variant);
            }
        }
        let columns: Vec<ArrayRef> = variants
            .iter()
            .zip(&chosen)
            .map(|((_, values), picks)| {
                let parts: Vec<&dyn Array> = picks.iter().map(|&v| values[v].as_ref()).collect();
                arrow::compute::concat(&parts).expect("concat column")
            })
            .collect();
        let fields: Vec<Field> = variants
            .iter()
            .zip(&columns)
            .map(|((name, _), column)| Field::new(*name, column.data_type().clone(), true))
            .collect();
        let rows = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("rows");

        let (left, right): (Vec<u32>, Vec<u32>) = (0..ROWS)
            .flat_map(|i| (i + 1..ROWS).map(move |j| (i, j)))
            .map(|(i, j)| (u32::try_from(i).expect("row"), u32::try_from(j).expect("row")))
            .unzip();
        let (left, right) = (UInt32Array::from(left), UInt32Array::from(right));
        let mut pair_fields = Vec::new();
        let mut pair_columns = Vec::new();
        for (index, field) in rows.schema().fields().iter().enumerate() {
            for (side, take) in [("l", &left), ("r", &right)] {
                pair_fields.push(Field::new(
                    format!("{side}{index}"),
                    field.data_type().clone(),
                    true,
                ));
                pair_columns.push(
                    arrow::compute::take(rows.column(index), take, None).expect("take"),
                );
            }
        }
        let pairs =
            RecordBatch::try_new(Arc::new(Schema::new(pair_fields)), pair_columns).expect("pairs");
        let ctx = SessionContext::new();
        ctx.register_batch("pairs", pairs).expect("register pairs");
        let comparisons = (0..rows.num_columns())
            .map(|index| format!("l{index} IS NOT DISTINCT FROM r{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let answers = ctx
            .sql(&format!("SELECT {comparisons} FROM pairs"))
            .await
            .expect("plan the oracle")
            .collect()
            .await
            .expect("run the oracle");
        let oracle: Vec<Vec<bool>> = (0..rows.num_columns())
            .map(|index| {
                answers
                    .iter()
                    .flat_map(|batch| {
                        let column = batch.column(index).as_boolean();
                        assert_eq!(column.null_count(), 0, "IS NOT DISTINCT FROM is never NULL");
                        column.values().iter().collect::<Vec<_>>()
                    })
                    .collect()
            })
            .collect();

        let agree = |label: &str, digests: &[u128], same: &dyn Fn(usize) -> bool| {
            let mut equal_pairs = 0;
            for (pair, (&i, &j)) in left.values().iter().zip(right.values()).enumerate() {
                let (i, j) = (i as usize, j as usize);
                assert_eq!(
                    digests[i] == digests[j],
                    same(pair),
                    "seed {SEED:#x}, {label}: rows {i} and {j} ({:?} and {:?})",
                    rows.slice(i, 1),
                    rows.slice(j, 1),
                );
                equal_pairs += usize::from(same(pair));
            }
            equal_pairs
        };
        for (index, (name, _)) in variants.iter().enumerate() {
            let column = rows.project(&[index]).expect("project");
            let digests = ContentEncoder::new(&column.schema())
                .expect("encoder")
                .digests(&column)
                .expect("digests");
            let equal = agree(name, &digests, &|pair| oracle[index][pair]);
            // Non-vacuous: equal values in different physical forms were compared.
            let distinct_forms_equal = left
                .values()
                .iter()
                .zip(right.values())
                .enumerate()
                .filter(|&(pair, (&i, &j))| {
                    oracle[index][pair] && chosen[index][i as usize] != chosen[index][j as usize]
                })
                .count();
            assert!(
                equal > 0 && equal < left.len(),
                "seed {SEED:#x}, {name}: {equal} of {} pairs equal",
                left.len()
            );
            if !matches!(*name, "utf8" | "float") {
                assert!(
                    distinct_forms_equal > 0,
                    "seed {SEED:#x}, {name}: no equal pair in different physical forms"
                );
            }
        }
        let digests = ContentEncoder::new(&rows.schema())
            .expect("encoder")
            .digests(&rows)
            .expect("digests");
        let equal = agree("whole row", &digests, &|pair| {
            oracle.iter().all(|column| column[pair])
        });
        assert!(equal > 0, "seed {SEED:#x}: no whole row repeats");
    }

    /// A key resolver over a table with a `Map` column resolves its copies.
    #[test]
    fn strict_upsert_resolves_map_payloads() {
        let variants = content_variants();
        let (_, maps) = variants
            .iter()
            .find(|(name, _)| *name == "map")
            .expect("map variants");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("m", maps[3].data_type().clone(), true),
        ]));
        let batch = |ids: &[i64], picks: &[usize]| {
            let parts: Vec<&dyn Array> = picks.iter().map(|&v| maps[v].as_ref()).collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())),
                    arrow::compute::concat(&parts).expect("maps"),
                ],
            )
            .expect("batch")
        };
        let resolver = KeyResolver::new("t", &schema, &[0], ConflictPolicy::UpsertIdentical)
            .expect("a Map payload is supported");
        // NULL maps over different entries are one value.
        let resolved = resolver
            .collapse_write(vec![batch(&[1, 2], &[3, 0]), batch(&[1, 2], &[3, 1])])
            .expect("identical copies collapse");
        assert_eq!(
            resolved.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2
        );
        let error = resolver
            .collapse_write(vec![batch(&[1], &[5]), batch(&[1], &[6])])
            .expect_err("entry order makes a different map");
        assert!(
            error
                .to_string()
                .contains("different versions of 1 value of 'id'"),
            "{error}"
        );
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
