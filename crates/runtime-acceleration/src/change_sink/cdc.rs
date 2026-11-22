/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! CDC operation grouping and zero-copy row selection for accelerator backends.
//!
//! A backend splits one CDC batch into ordered upsert, delete and truncate
//! groups with `group_into_sub_batches`, then applies each group. The
//! provider and Cayenne backends both use these helpers.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow::datatypes::{ArrowNativeType, DataType};
use data_components::cdc::{ChangeBatch, ChangeOperation};
use datafusion::common::TableReference;
use datafusion::common::hash_utils::{RandomState, create_hashes};
use datafusion::error::Result;
use opentelemetry::KeyValue;
use runtime_metrics::acceleration as metrics;

/// Reuses the dataset label across the backend's hot-path measurements.
pub struct CdcMetrics {
    dataset: [KeyValue; 1],
}

impl CdcMetrics {
    #[must_use]
    pub fn new(dataset: &TableReference) -> Self {
        let name: Arc<str> = Arc::from(dataset.to_string());
        Self {
            dataset: [KeyValue::new("dataset", name)],
        }
    }

    fn tagged(&self, key: &'static str, value: &'static str) -> [KeyValue; 2] {
        [self.dataset[0].clone(), KeyValue::new(key, value)]
    }

    pub fn path(&self, path: &'static str) {
        metrics::CDC_APPLY_PATH_TOTAL.add(1, &self.tagged("path", path));
    }

    pub fn delete_keys(&self, batch: &ChangeBatch, rows: &[usize]) {
        let count = rows
            .iter()
            .filter(|&&row| batch.has_primary_keys(row))
            .count();
        metrics::CDC_KEYS_PER_DELETE_BURST
            .record(u64::try_from(count).unwrap_or(u64::MAX), &self.dataset);
    }

    pub fn delete_fallthrough(&self, reason: &'static str) {
        metrics::CDC_DELETE_ABSORB_FALLTHROUGH.add(1, &self.tagged("reason", reason));
    }

    pub fn fixed_cost(&self, phase: &'static str, start: Instant) {
        metrics::CDC_APPLY_FIXED_COST_MS.record(
            start.elapsed().as_secs_f64() * 1000.0,
            &self.tagged("phase", phase),
        );
    }
}

/// Select rows without changing their order or values.
///
/// # Errors
/// Returns an error if a take index exceeds `UInt32`, Arrow selection fails,
/// or the selected columns cannot form a record batch.
pub fn select_rows(data_batch: &RecordBatch, row_indices: &[usize]) -> Result<RecordBatch> {
    if let Some((offset, length)) = contiguous_row_span(row_indices) {
        return Ok(data_batch.slice(offset, length));
    }

    let indices = row_indices
        .iter()
        .map(|&i| {
            u32::try_from(i).map_err(|e| {
                arrow::error::ArrowError::InvalidArgumentError(format!(
                    "CDC row index {i} exceeds UInt32 take index range: {e}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let indices_array = UInt32Array::from(indices);

    let selected_columns: Vec<ArrayRef> = data_batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices_array, None))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(data_batch.schema(), selected_columns)?)
}

#[must_use]
pub fn contiguous_row_span(row_indices: &[usize]) -> Option<(usize, usize)> {
    let first = *row_indices.first()?;
    if row_indices
        .iter()
        .enumerate()
        .all(|(offset, &row)| row == first + offset)
    {
        Some((first, row_indices.len()))
    } else {
        None
    }
}

/// Tracks primary keys so that same-PK collisions within the bucket apply
/// last-write-wins deduplication (the newer row replaces the older one)
struct OpBatchAccumulator {
    rows: Vec<usize>,
    needs_sort: bool,
    /// Maps encoded PK to index into `rows`, enabling replacement on same-bucket PK collision.
    pk_to_pos: HashMap<Vec<u8>, usize, BuildHasherDefault<twox_hash::XxHash3_64>>,
}

impl OpBatchAccumulator {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            needs_sort: false,
            pk_to_pos: HashMap::default(),
        }
    }

    /// Returns `true` if `pk` is already tracked in this bucket.
    fn contains_pk(&self, pk: &[u8]) -> bool {
        self.pk_to_pos.contains_key(pk)
    }

    /// Insert `row_id` under `pk`. If the PK already exists in this bucket,
    /// the previous row index is replaced in-place (last-write-wins).
    /// See [`group_into_sub_batches`] for the rationale.
    fn insert_or_replace(&mut self, pk: Vec<u8>, row_id: usize) {
        if let Some(&pos) = self.pk_to_pos.get(&pk) {
            // Same-bucket collision: replace the earlier row with the newer
            // one. The old row is superseded because CDC rows carry the
            // full row state.
            if pos + 1 < self.rows.len() {
                self.needs_sort = true;
            }
            self.rows[pos] = row_id;
        } else {
            let pos = self.rows.len();
            self.rows.push(row_id);
            self.pk_to_pos.insert(pk, pos);
        }
    }

    /// Drain accumulated rows into `out` under the given operation type and
    /// reset PK tracking.
    fn flush_into(
        &mut self,
        op: ChangeOperationType,
        out: &mut Vec<(ChangeOperationType, Vec<usize>)>,
    ) {
        if !self.rows.is_empty() {
            if self.needs_sort {
                self.rows.sort_unstable();
                self.needs_sort = false;
            }
            out.push((op, std::mem::take(&mut self.rows)));
            self.pk_to_pos.clear();
        }
    }
}

fn primary_key_lists_are_uniform(change_batch: &ChangeBatch) -> bool {
    let Some(keys) = change_batch
        .record
        .column_by_name("primary_keys")
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
    else {
        return false;
    };
    let Some(names) = keys.values().as_any().downcast_ref::<StringArray>() else {
        return false;
    };
    if names.null_count() > 0 {
        return false;
    }
    // A null key list is a row without keys, the same as an empty one, when it
    // spans no names: `ChangeBatch::primary_keys` and `has_primary_keys` read it
    // as empty. A keyless producer (a `DoExchange` subscription of a table with
    // no primary key) sends every row that way, and treating it as nonuniform
    // would write each row on its own.
    if keys.null_count() > 0
        && (0..keys.len()).any(|row| keys.is_null(row) && keys.value_length(row) != 0)
    {
        return false;
    }
    let offsets = keys.value_offsets();
    let Some(first) = offsets.windows(2).next() else {
        return true;
    };
    let first = first[0].as_usize()..first[1].as_usize();
    offsets.windows(2).all(|pair| {
        let row = pair[0].as_usize()..pair[1].as_usize();
        row.len() == first.len()
            && row
                .zip(first.clone())
                .all(|(row, first)| names.value(row) == names.value(first))
    })
}

/// Groups rows into sub-batches based on operation type and primary key
/// conflicts across active operation buckets.
///
/// Uses a streaming conflict-window algorithm with **last-write-wins
/// deduplication**: two active buckets (upsert, delete) accumulate rows
/// concurrently. When an incoming row's PK already exists in the *other*
/// bucket, that bucket is flushed to preserve cross-operation ordering.
/// When the PK collides within the *same* bucket the earlier row index is
/// replaced in-place — CDC rows are full-state snapshots, so only the
/// latest row per PK is required and intermediate states can be safely dropped.
///
/// For deletes a same-bucket PK collision is unexpected in practice (a
/// source would have to emit two consecutive deletes for the same key
/// without an intervening upsert), but is still safe — deleting the same
/// PK twice is idempotent. We use the same replace path for both operation
/// types to keep the logic simple.
///
/// Truncate and Unknown act as barriers that flush everything.
#[must_use]
pub fn group_into_sub_batches(
    change_batch: &ChangeBatch,
) -> Vec<(ChangeOperationType, Vec<usize>)> {
    if distinct_upserts(change_batch) {
        // Distinct keys only append to the upsert bucket, never replace a slot
        // or set needs_sort. With no deletes or barriers, its final flush emits
        // every row in input order.
        return vec![(
            ChangeOperationType::Upsert,
            (0..change_batch.record.num_rows()).collect(),
        )];
    }
    group_by_scan(change_batch)
}

/// Proves that the scan would emit all rows as a single ordered upsert group.
/// Hash collisions reject the optimization; they never decide key equality.
fn distinct_upserts(change_batch: &ChangeBatch) -> bool {
    let num_rows = change_batch.record.num_rows();
    if num_rows == 0 || !primary_key_lists_are_uniform(change_batch) {
        return false;
    }

    let Some(ops) = change_batch
        .record
        .column_by_name("op")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
    else {
        return false;
    };
    // Read value(row), just as ChangeBatch::op does, including at null slots.
    if !(0..num_rows).all(|row| matches!(ops.value(row), "c" | "u" | "r")) {
        return false;
    }

    let pk_column_names = change_batch.primary_keys(0);
    let data_schema = change_batch.data_schema();
    let pk_col_indices: Vec<usize> = pk_column_names
        .iter()
        .filter_map(|name| data_schema.index_of(name).ok())
        .collect();
    if pk_col_indices.is_empty() || pk_col_indices.len() != pk_column_names.len() {
        return false;
    }
    let data_batch = change_batch.data_batch();
    if pk_col_indices.iter().any(|&index| {
        let column = data_batch.column(index);
        // The encoder writes a null marker independent of the underlying value.
        // Keep nulls on the exact scan rather than depend on hash null semantics.
        // Only byte-encoded types qualify: display-based encoding can equate
        // values that Arrow hashes differently (for example nested values).
        column.null_count() != 0
            || !matches!(
                column.data_type(),
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(_, _)
                    | DataType::Decimal128(_, _)
            )
    }) {
        return false;
    }

    let mut hashes = vec![0; num_rows];
    if create_hashes(
        pk_col_indices.iter().map(|&index| data_batch.column(index)),
        &RandomState::with_seed(0),
        &mut hashes,
    )
    .is_err()
    {
        // An unavailable hash implementation must preserve the scan's behavior.
        return false;
    }
    // Sort in place: contiguous u64 comparisons avoid a second hash-table
    // allocation and per-row probing. No copy or collision resolution is needed.
    hashes.sort_unstable();
    hashes.windows(2).all(|pair| pair[0] != pair[1])
}

fn group_by_scan(change_batch: &ChangeBatch) -> Vec<(ChangeOperationType, Vec<usize>)> {
    let num_rows = change_batch.record.num_rows();
    if num_rows == 0 {
        return vec![];
    }

    // Different key definitions cannot share conflict buckets or delete filters.
    if !primary_key_lists_are_uniform(change_batch) {
        return (0..num_rows)
            .map(|row| {
                (
                    ChangeOperationType::from_operation(&change_batch.op(row)),
                    vec![row],
                )
            })
            .collect();
    }

    // Find the PK columns once, instead of per-row, from the data schema. The
    // data is read as a batch only when there are keys to encode: a row
    // without data, such as the truncate that opens a `DoExchange` snapshot,
    // carries a null data struct, which cannot be read as a batch when a
    // column is non-nullable, and a keyless batch never needs one.
    let pk_column_names = change_batch.primary_keys(0);
    let data_schema = change_batch.data_schema();
    let pk_col_indices: Vec<usize> = pk_column_names
        .iter()
        .filter_map(|name| data_schema.index_of(name).ok())
        .collect();
    let data_batch = (!pk_col_indices.is_empty()).then(|| change_batch.data_batch());

    let mut upserts = OpBatchAccumulator::new();
    let mut deletes = OpBatchAccumulator::new();
    let mut out: Vec<(ChangeOperationType, Vec<usize>)> = Vec::new();

    for row_id in 0..num_rows {
        let op = change_batch.op(row_id);
        let op_type = ChangeOperationType::from_operation(&op);

        // Truncate and Unknown are barriers — flush everything, emit the
        // barrier row, and continue.
        if op_type == ChangeOperationType::Truncate || op_type == ChangeOperationType::Unknown {
            upserts.flush_into(ChangeOperationType::Upsert, &mut out);
            deletes.flush_into(ChangeOperationType::Delete, &mut out);
            out.push((op_type, vec![row_id]));
            continue;
        }

        // When PKs are available, use last-write-wins within the same
        // bucket (CDC rows are full-state snapshots so only the latest
        // row per PK matters) and flush only on *cross-bucket* conflicts
        // to preserve inter-operation ordering.
        if let Some(data_batch) = &data_batch {
            let primary_key = encode_primary_key(data_batch, &pk_col_indices, row_id);

            // Cross-bucket conflict: the *other* bucket already has this
            // PK, so flush it to preserve operation ordering.
            match op_type {
                ChangeOperationType::Upsert => {
                    if deletes.contains_pk(&primary_key) {
                        deletes.flush_into(ChangeOperationType::Delete, &mut out);
                    }
                }
                ChangeOperationType::Delete => {
                    if upserts.contains_pk(&primary_key) {
                        upserts.flush_into(ChangeOperationType::Upsert, &mut out);
                    }
                }
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => {
                    unreachable!("unexpected op type {op_type:?} after barrier check")
                }
            }

            // Same-bucket collision: replace the old row (last-write-wins).
            let batch = match op_type {
                ChangeOperationType::Upsert => &mut upserts,
                ChangeOperationType::Delete => &mut deletes,
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => {
                    unreachable!("unexpected op type {op_type:?} after barrier check")
                }
            };
            batch.insert_or_replace(primary_key, row_id);
        } else {
            // No PKs — fall back to grouping consecutive same-op rows
            // (can't detect conflicts without keys).
            match op_type {
                ChangeOperationType::Upsert => {
                    deletes.flush_into(ChangeOperationType::Delete, &mut out);
                    upserts.rows.push(row_id);
                }
                ChangeOperationType::Delete => {
                    upserts.flush_into(ChangeOperationType::Upsert, &mut out);
                    deletes.rows.push(row_id);
                }
                ChangeOperationType::Truncate | ChangeOperationType::Unknown => {
                    unreachable!("unexpected op type {op_type:?} after barrier check")
                }
            }
        }
    }

    // Flush remaining active batches.
    upserts.flush_into(ChangeOperationType::Upsert, &mut out);
    deletes.flush_into(ChangeOperationType::Delete, &mut out);

    out
}

#[must_use]
pub fn encode_primary_key(
    data_batch: &RecordBatch,
    pk_col_indices: &[usize],
    row_id: usize,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(pk_col_indices.len().saturating_mul(16));
    for &col_idx in pk_col_indices {
        key.extend_from_slice(&col_idx.to_le_bytes());
        encode_array_value(data_batch.column(col_idx).as_ref(), row_id, &mut key);
    }
    key
}

macro_rules! encode_primitive_value {
    ($array:expr, $row_id:expr, $array_type:ty, $key:expr) => {{
        if let Some(array) = $array.as_any().downcast_ref::<$array_type>() {
            $key.extend_from_slice(&array.value($row_id).to_le_bytes());
            return;
        }
    }};
}

fn encode_bytes(bytes: &[u8], key: &mut Vec<u8>) {
    key.extend_from_slice(&bytes.len().to_le_bytes());
    key.extend_from_slice(bytes);
}

fn encode_array_value(array: &dyn Array, row_id: usize, key: &mut Vec<u8>) {
    if array.is_null(row_id) {
        key.push(0);
        return;
    }
    key.push(1);

    match array.data_type() {
        DataType::Boolean => {
            if let Some(array) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                key.push(u8::from(array.value(row_id)));
                return;
            }
        }
        DataType::Int8 => {
            encode_primitive_value!(array, row_id, arrow::array::Int8Array, key);
        }
        DataType::Int16 => {
            encode_primitive_value!(array, row_id, arrow::array::Int16Array, key);
        }
        DataType::Int32 => {
            encode_primitive_value!(array, row_id, Int32Array, key);
        }
        DataType::Int64 => {
            encode_primitive_value!(array, row_id, Int64Array, key);
        }
        DataType::UInt8 => {
            encode_primitive_value!(array, row_id, arrow::array::UInt8Array, key);
        }
        DataType::UInt16 => {
            encode_primitive_value!(array, row_id, arrow::array::UInt16Array, key);
        }
        DataType::UInt32 => {
            encode_primitive_value!(array, row_id, UInt32Array, key);
        }
        DataType::UInt64 => {
            encode_primitive_value!(array, row_id, arrow::array::UInt64Array, key);
        }
        DataType::Float32 => {
            if let Some(array) = array.as_any().downcast_ref::<arrow::array::Float32Array>() {
                key.extend_from_slice(&array.value(row_id).to_bits().to_le_bytes());
                return;
            }
        }
        DataType::Float64 => {
            if let Some(array) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                key.extend_from_slice(&array.value(row_id).to_bits().to_le_bytes());
                return;
            }
        }
        DataType::Utf8 => {
            if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                encode_bytes(array.value(row_id).as_bytes(), key);
                return;
            }
        }
        DataType::LargeUtf8 => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
            {
                encode_bytes(array.value(row_id).as_bytes(), key);
                return;
            }
        }
        DataType::Date32 => {
            encode_primitive_value!(array, row_id, arrow::array::Date32Array, key);
        }
        DataType::Date64 => {
            encode_primitive_value!(array, row_id, arrow::array::Date64Array, key);
        }
        DataType::Time32(_) => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::Time32SecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            encode_primitive_value!(array, row_id, arrow::array::Time32MillisecondArray, key);
        }
        DataType::Time64(_) => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::Time64MicrosecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            encode_primitive_value!(array, row_id, arrow::array::Time64NanosecondArray, key);
        }
        DataType::Timestamp(_, _) => {
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            if let Some(array) = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            {
                key.extend_from_slice(&array.value(row_id).to_le_bytes());
                return;
            }
            encode_primitive_value!(array, row_id, arrow::array::TimestampNanosecondArray, key);
        }
        DataType::Decimal128(_, _) => {
            encode_primitive_value!(array, row_id, arrow::array::Decimal128Array, key);
        }
        _ => {}
    }

    if let Ok(value) = arrow::util::display::array_value_to_string(array, row_id) {
        key.push(0xfe);
        encode_bytes(value.as_bytes(), key);
    } else {
        key.push(0xff);
    }
}

// Used to group batch changes into sub-batches
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperationType {
    Upsert, // Create, Update, or Read
    Delete,
    Truncate,
    Unknown,
}

impl ChangeOperationType {
    #[must_use]
    pub fn from_operation(op: &ChangeOperation) -> Self {
        match op {
            ChangeOperation::Create | ChangeOperation::Update | ChangeOperation::Read => {
                Self::Upsert
            }
            ChangeOperation::Delete => Self::Delete,
            ChangeOperation::Truncate => Self::Truncate,
            ChangeOperation::Unknown(_) => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StructArray;
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Schema};
    use data_components::cdc::changes_schema;

    fn change_batch(ops: &[&str], keys: &[Vec<&str>], columns: Vec<ArrayRef>) -> ChangeBatch {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    Field::new(format!("key{index}"), column.data_type().clone(), true)
                })
                .collect::<Vec<_>>(),
        ));
        let names = keys.iter().flatten().copied().collect::<Vec<_>>();
        let keys = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            OffsetBuffer::from_lengths(keys.iter().map(Vec::len)),
            Arc::new(StringArray::from(names)),
            None,
        );
        let data = StructArray::new(schema.fields().clone(), columns, None);
        ChangeBatch::try_new(
            RecordBatch::try_new(
                Arc::new(changes_schema(&schema)),
                vec![
                    Arc::new(StringArray::from(ops.to_vec())),
                    Arc::new(keys),
                    Arc::new(data),
                ],
            )
            .expect("CDC record batch"),
        )
        .expect("valid CDC batch")
    }

    #[test]
    fn distinct_upserts_preserve_input_order() {
        let batch = change_batch(
            &["c", "u", "r", "c"],
            &vec![vec!["key0"]; 4],
            vec![Arc::new(Int64Array::from(vec![30, 10, 40, 20]))],
        );
        assert!(distinct_upserts(&batch));
        let expected = vec![(ChangeOperationType::Upsert, vec![0, 1, 2, 3])];
        assert_eq!(group_into_sub_batches(&batch), expected);
        assert_eq!(group_by_scan(&batch), expected);
    }

    #[test]
    fn duplicate_upserts_use_last_write_in_row_order() {
        let batch = change_batch(
            &["c", "u", "r", "c"],
            &vec![vec!["key0"]; 4],
            vec![Arc::new(Int64Array::from(vec![30, 10, 40, 30]))],
        );
        assert!(!distinct_upserts(&batch));
        let expected = vec![(ChangeOperationType::Upsert, vec![1, 2, 3])];
        assert_eq!(group_into_sub_batches(&batch), expected);
        assert_eq!(group_into_sub_batches(&batch), group_by_scan(&batch));
    }

    #[test]
    fn one_delete_uses_scan_buckets() {
        let batch = change_batch(
            &["c", "d", "r", "c"],
            &vec![vec!["key0"]; 4],
            vec![Arc::new(Int64Array::from(vec![30, 10, 40, 20]))],
        );
        assert!(!distinct_upserts(&batch));
        assert_eq!(
            group_into_sub_batches(&batch),
            vec![
                (ChangeOperationType::Upsert, vec![0, 2, 3]),
                (ChangeOperationType::Delete, vec![1]),
            ]
        );
        assert_eq!(group_into_sub_batches(&batch), group_by_scan(&batch));
    }

    /// A seeded xorshift generator keeps this pure-function sweep reproducible
    /// without adding a test dependency. The seed is printed before generation.
    struct Random(u64);

    impl Random {
        fn below(&mut self, limit: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % u64::try_from(limit).expect("small limit"))
                .expect("value is below a usize limit")
        }
    }

    #[test]
    fn grouping_matches_scan_for_seeded_batches() {
        const SEED: u64 = 0x1a9e_427b_d530_6fc1;
        eprintln!("grouping differential seed: {SEED:#x}");
        let mut random = Random(SEED);
        let mut eligible = 0;
        let key_names = ["key0", "key1", "key2"];
        for iteration in 0..720 {
            let rows = if iteration == 0 { 0 } else { random.below(301) };
            let key_count = 1 + random.below(3);
            let scenario = iteration % 12;
            let mut ids = (0..rows).collect::<Vec<_>>();
            for row in (1..rows).rev() {
                ids.swap(row, random.below(row + 1));
            }
            if scenario == 3 && rows > 1 {
                ids[rows - 1] = ids[0];
            } else if scenario >= 5 {
                for id in &mut ids {
                    *id = random.below(20);
                }
            }
            let mut columns = Vec::new();
            for _ in 0..key_count {
                let values = ids
                    .iter()
                    .enumerate()
                    .map(|(row, &id)| {
                        if scenario == 4 && (row == 0 || random.below(5) == 0) {
                            None
                        } else {
                            Some(i32::try_from(id).expect("at most 300 rows") - 150)
                        }
                    })
                    .collect::<Vec<_>>();
                let column: ArrayRef = match random.below(3) {
                    0 => Arc::new(Int32Array::from(values)),
                    1 => Arc::new(Int64Array::from(
                        values
                            .into_iter()
                            .map(|v| v.map(i64::from))
                            .collect::<Vec<_>>(),
                    )),
                    _ => Arc::new(StringArray::from(
                        values
                            .into_iter()
                            .map(|v| v.map(|v| format!("κ\0/{v}")))
                            .collect::<Vec<_>>(),
                    )),
                };
                columns.push(column);
            }
            let mut ops = (0..rows)
                .map(|_| ["c", "u", "r"][random.below(3)])
                .collect::<Vec<_>>();
            if scenario == 5 {
                for op in &mut ops {
                    *op = ["c", "u", "r", "d", "t", "unknown"][random.below(6)];
                }
            } else if rows > 0 {
                match scenario {
                    6 => ops[random.below(rows)] = "d",
                    7 => ops[random.below(rows)] = "t",
                    8 => ops[random.below(rows)] = "unknown",
                    _ => {}
                }
            }
            let mut keys = vec![key_names[..key_count].to_vec(); rows];
            match scenario {
                9 => {
                    for row in (1..rows).step_by(2) {
                        keys[row] = vec!["key0", "missing"];
                    }
                }
                10 => keys.fill(vec![]),
                11 => keys.fill(vec!["key0", "missing"]),
                _ => {}
            }
            let batch = change_batch(&ops, &keys, columns);
            eligible += usize::from(distinct_upserts(&batch));
            assert_eq!(
                group_into_sub_batches(&batch),
                group_by_scan(&batch),
                "seed={SEED:#x}, iteration={iteration}, scenario={scenario}, rows={rows}"
            );
        }
        assert!(
            eligible > 0,
            "seed={SEED:#x}: no batch exercised the fast path"
        );
    }
}
