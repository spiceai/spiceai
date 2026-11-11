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
    if keys.null_count() > 0 || names.null_count() > 0 {
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

    // Extract data batch and PK column indices once, instead of per-row.
    let data_batch = change_batch.data_batch();
    let pk_column_names = change_batch.primary_keys(0);
    let pk_col_indices: Vec<usize> = pk_column_names
        .iter()
        .filter_map(|name| data_batch.schema().index_of(name).ok())
        .collect();
    let has_pks = !pk_col_indices.is_empty();

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
        if has_pks {
            let primary_key = encode_primary_key(&data_batch, &pk_col_indices, row_id);

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
