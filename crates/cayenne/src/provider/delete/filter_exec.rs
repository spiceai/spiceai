/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

//! Deletion filter execution plans for Cayenne tables.
//!
//! This module provides execution plans that filter out deleted rows during query execution:
//!
//! - **`Int64PkDeletionFilterExec`**: Optimized for tables with single-column Int64 primary keys.
//!   Probes a [`DeletionIndex`] (bloom filter + `HashMap<i64, i64>`) once per row.
//!
//! - **`KeyBasedDeletionFilterExec`**: For tables with composite or non-integer primary keys.
//!   Uses Arrow's `RowConverter` to create deterministic byte keys, then probes a
//!   [`KeyDeletionIndex`] for each row.
//!
//! # Position-Based Deletion (No Filter Exec)
//!
//! For tables **without** a primary key, position-based deletion is used. This strategy
//! does NOT use a filter execution plan. Instead, deletions are pushed down directly to
//! the Vortex scan layer via `Selection::ExcludeRoaring`, which efficiently skips deleted
//! row positions at the storage level.
//!
//! # Sequence-Based Ordering
//!
//! Both filter execs support Iceberg-style sequence-based ordering for upsert semantics:
//! - `delete_sequence` records when a PK was marked for deletion
//! - `insert_sequence` records when a PK was re-inserted (upsert)
//! - If `insert_sequence > delete_sequence`, the row is visible (re-inserted after delete)
//!
//! # Vectorised probe
//!
//! Each batch is filtered in two passes:
//! 1. Build a `BooleanArray` keep-mask by probing the deletion index per row, with a
//!    bloom-filter prefilter that early-rejects keys that are definitely not deleted.
//! 2. Apply the mask in one shot via [`arrow::compute::filter_record_batch`].

use crate::provider::deletion_index::{DeletionIndex, KeyDeletionIndex};
use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder};
use arrow_row::RowConverter;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

// ============================================================================
// PK Visibility Helpers
// ============================================================================
//
// A row is visible (kept) if either:
// - Its PK is not in the deletion index, OR
// - Its PK is in the deletion index but was re-inserted with a higher sequence
//   number (upsert).
//
// Both helpers run inside the per-row loop, so they avoid second probes when
// the bloom filter on the deletion index already rejects the key.

/// Check if a row with the given Int64 PK is visible (not deleted, or re-inserted after deletion).
///
/// `min_delete_seq_to_apply` is the protected-snapshot cutoff: when `Some(min)`,
/// only deletions whose `delete_seq > min` apply. This lets the protected
/// snapshot scan path share the full `deleted_pks` index across snapshots
/// instead of rebuilding a filtered [`DeletionIndex`] per snapshot — the
/// `min_seq` is a single integer compared against the deletion sequence
/// number returned by the existing bloom-prefiltered `HashMap` probe, so it
/// adds at most one comparison per confirmed match (which the bloom
/// rejects most non-matching probes from reaching). `None` means apply every
/// deletion in `deleted_pks` (main scan path).
#[inline]
pub(crate) fn is_pk_visible_i64(
    pk: i64,
    deleted_pks: &DeletionIndex,
    insert_records: &DeletionIndex,
    min_delete_seq_to_apply: Option<i64>,
) -> bool {
    match min_delete_seq_to_apply {
        Some(min_delete_seq_to_apply) => {
            is_pk_visible_i64_after_min(pk, deleted_pks, insert_records, min_delete_seq_to_apply)
        }
        None => is_pk_visible_i64_without_min(pk, deleted_pks, insert_records),
    }
}

#[inline]
fn is_pk_visible_i64_without_min(
    pk: i64,
    deleted_pks: &DeletionIndex,
    insert_records: &DeletionIndex,
) -> bool {
    match deleted_pks.get(pk) {
        None => true,
        Some(delete_seq) => insert_records
            .get(pk)
            .is_some_and(|insert_seq| insert_seq > delete_seq),
    }
}

#[inline]
fn is_pk_visible_i64_after_min(
    pk: i64,
    deleted_pks: &DeletionIndex,
    insert_records: &DeletionIndex,
    min_delete_seq_to_apply: i64,
) -> bool {
    match deleted_pks.get(pk) {
        None => true,
        Some(delete_seq) => {
            if delete_seq <= min_delete_seq_to_apply {
                // Deletion pre-dates the protected snapshot's creation —
                // skip it. The full deletion index is reused here instead
                // of being rebuilt with these entries filtered out.
                return true;
            }
            insert_records
                .get(pk)
                .is_some_and(|insert_seq| insert_seq > delete_seq)
        }
    }
}

/// Check if a row with the given byte key is visible (not deleted, or re-inserted after deletion).
///
/// `min_delete_seq_to_apply` is the protected-snapshot cutoff. See
/// [`is_pk_visible_i64`] for the rationale.
#[inline]
pub(crate) fn is_pk_visible_row_key(
    key: &[u8],
    deleted_keys: &KeyDeletionIndex,
    insert_records: &KeyDeletionIndex,
    min_delete_seq_to_apply: Option<i64>,
) -> bool {
    match min_delete_seq_to_apply {
        Some(min_delete_seq_to_apply) => is_pk_visible_row_key_after_min(
            key,
            deleted_keys,
            insert_records,
            min_delete_seq_to_apply,
        ),
        None => is_pk_visible_row_key_without_min(key, deleted_keys, insert_records),
    }
}

#[inline]
fn is_pk_visible_row_key_without_min(
    key: &[u8],
    deleted_keys: &KeyDeletionIndex,
    insert_records: &KeyDeletionIndex,
) -> bool {
    match deleted_keys.get(key) {
        None => true,
        Some(delete_seq) => insert_records
            .get(key)
            .is_some_and(|insert_seq| insert_seq > delete_seq),
    }
}

#[inline]
fn is_pk_visible_row_key_after_min(
    key: &[u8],
    deleted_keys: &KeyDeletionIndex,
    insert_records: &KeyDeletionIndex,
    min_delete_seq_to_apply: i64,
) -> bool {
    match deleted_keys.get(key) {
        None => true,
        Some(delete_seq) => {
            if delete_seq <= min_delete_seq_to_apply {
                return true;
            }
            insert_records
                .get(key)
                .is_some_and(|insert_seq| insert_seq > delete_seq)
        }
    }
}

// ============================================================================
// Key-based deletion filter (for tables WITH primary key)
// ============================================================================

/// Execution plan that filters out deleted rows based on primary key matching.
///
/// This wraps another execution plan and removes rows whose primary key values
/// match the deleted row keys loaded from deletion vector files.
///
/// # Key-Based Semantics
///
/// Unlike position-based deletion (which uses per-file `RoaringBitmap` pushed down
/// to Vortex scan), this uses Arrow's `RowConverter` to create deterministic byte
/// representations of primary key columns. This approach:
///
/// - **Position-independent**: Works regardless of partition ordering
/// - **Survives reorganization**: Row keys are based on content, not position
/// - **Parallel-friendly**: No need to coalesce partitions
///
/// # Sequence-Based Ordering
///
/// Insert records track PKs that were deleted and then re-inserted (upserted).
/// A row is only filtered out if its key is in the deletion index AND either:
/// - It's not in the insert-records index, OR
/// - Its `insert_sequence < delete_sequence` for that key
///
/// This allows upsert semantics without full table compaction.
pub struct KeyBasedDeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Deletion index of PK bytes -> delete sequence number.
    deleted_row_keys: Arc<KeyDeletionIndex>,
    /// Deletion index of PK bytes -> insert sequence number for upserted PKs.
    insert_records: Arc<KeyDeletionIndex>,
    /// Indices of primary key columns in the schema
    pk_column_indices: Vec<usize>,
    /// `RowConverter` for converting PK columns to bytes
    row_converter: Arc<RowConverter>,
    /// Optional minimum sequence number for protected-snapshot filtering.
    /// See [`Int64PkDeletionFilterExec::min_delete_seq_to_apply`].
    min_delete_seq_to_apply: Option<i64>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl KeyBasedDeletionFilterExec {
    /// Create a new key-based deletion filter execution plan.
    ///
    /// # Arguments
    /// * `input` - The input execution plan to filter
    /// * `deleted_row_keys` - Bloom-prefiltered index of deleted PK byte keys
    /// * `insert_records` - Bloom-prefiltered index of upserted PK byte keys
    /// * `pk_column_indices` - Indices of primary key columns in the schema
    /// * `row_converter` - `RowConverter` configured for the PK columns
    /// * `min_delete_seq_to_apply` - Optional protected-snapshot cutoff
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_row_keys: Arc<KeyDeletionIndex>,
        insert_records: Arc<KeyDeletionIndex>,
        pk_column_indices: Vec<usize>,
        row_converter: Arc<RowConverter>,
        min_delete_seq_to_apply: Option<i64>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            deleted_row_keys,
            insert_records,
            pk_column_indices,
            row_converter,
            min_delete_seq_to_apply,
            properties,
        }
    }
}

impl std::fmt::Debug for KeyBasedDeletionFilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyBasedDeletionFilterExec")
    }
}

impl DisplayAs for KeyBasedDeletionFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "KeyBasedDeletionFilterExec: filtered_keys={}",
            self.deleted_row_keys.len()
        )
    }
}

impl ExecutionPlan for KeyBasedDeletionFilterExec {
    fn name(&self) -> &'static str {
        "KeyBasedDeletionFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Plan(
                "KeyBasedDeletionFilterExec requires exactly 1 child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.deleted_row_keys),
            Arc::clone(&self.insert_records),
            self.pk_column_indices.clone(),
            Arc::clone(&self.row_converter),
            self.min_delete_seq_to_apply,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let deleted_row_keys = Arc::clone(&self.deleted_row_keys);
        let insert_records = Arc::clone(&self.insert_records);
        let pk_column_indices = self.pk_column_indices.clone();
        let row_converter = Arc::clone(&self.row_converter);
        let min_delete_seq_to_apply = self.min_delete_seq_to_apply;
        let schema = input_stream.schema();

        Ok(Box::pin(KeyBasedDeletionFilterStream {
            input: input_stream,
            deleted_row_keys,
            insert_records,
            pk_column_indices,
            row_converter,
            min_delete_seq_to_apply,
            schema,
        }))
    }
}

/// Stream that filters out deleted rows based on primary key matching.
pub struct KeyBasedDeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_row_keys: Arc<KeyDeletionIndex>,
    insert_records: Arc<KeyDeletionIndex>,
    pk_column_indices: Vec<usize>,
    row_converter: Arc<RowConverter>,
    /// See [`Int64PkDeletionFilterStream::min_delete_seq_to_apply`].
    min_delete_seq_to_apply: Option<i64>,
    schema: arrow_schema::SchemaRef,
}

impl futures::Stream for KeyBasedDeletionFilterStream {
    type Item = datafusion_common::Result<arrow::array::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match std::pin::Pin::new(&mut self.input).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    let batch_size = batch.num_rows();

                    if batch_size == 0 {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Fast path: empty deleted keys index
                    if self.deleted_row_keys.is_empty() {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    if self.pk_column_indices.is_empty() {
                        return std::task::Poll::Ready(Some(Err(
                            datafusion_common::DataFusionError::Internal(
                                "KeyBasedDeletionFilterExec requires at least one primary key column index".to_string(),
                            ),
                        )));
                    }

                    // Extract PK columns from the batch
                    let mut pk_columns: Vec<ArrayRef> =
                        Vec::with_capacity(self.pk_column_indices.len());
                    for &idx in &self.pk_column_indices {
                        let Some(column) = batch.columns().get(idx) else {
                            return std::task::Poll::Ready(Some(Err(
                                datafusion_common::DataFusionError::Internal(format!(
                                    "KeyBasedDeletionFilterExec primary key column index {idx} is out of bounds for a batch with {} columns",
                                    batch.num_columns()
                                )),
                            )));
                        };
                        pk_columns.push(Arc::clone(column));
                    }

                    // Convert PK columns to row bytes (single batched conversion).
                    let rows = match self.row_converter.convert_columns(&pk_columns) {
                        Ok(rows) => rows,
                        Err(e) => {
                            return std::task::Poll::Ready(Some(Err(
                                datafusion_common::DataFusionError::ArrowError(Box::new(e), None),
                            )));
                        }
                    };

                    // Build keep mask: bloom-prefiltered probe per row + visibility check.
                    // Use `BooleanBufferBuilder` so the mask lives as a packed bitmap
                    // (1 bit per row instead of 1 byte) and skips the `Vec<bool>` →
                    // `BooleanArray` conversion pass.
                    let mut keep_mask = BooleanBufferBuilder::new(batch_size);
                    let mut keep_count: usize = 0;
                    for row in &rows {
                        let key: &[u8] = row.as_ref();
                        let visible = is_pk_visible_row_key(
                            key,
                            &self.deleted_row_keys,
                            &self.insert_records,
                            self.min_delete_seq_to_apply,
                        );
                        keep_mask.append(visible);
                        keep_count += usize::from(visible);
                    }

                    tracing::debug!(
                        "KeyBasedDeletionFilterStream: keeping {} of {} rows",
                        keep_count,
                        batch_size
                    );

                    // If all rows are deleted, skip this batch and continue to next
                    if keep_count == 0 {
                        continue;
                    }

                    // If no rows are deleted, return the batch as-is (fast path)
                    if keep_count == batch_size {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Apply mask in one shot via Arrow's filter kernel.
                    let filter_array = BooleanArray::new(keep_mask.finish(), None);
                    let filtered_batch =
                        match arrow::compute::filter_record_batch(&batch, &filter_array) {
                            Ok(filtered) => filtered,
                            Err(e) => {
                                return std::task::Poll::Ready(Some(Err(
                                    datafusion_common::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    ),
                                )));
                            }
                        };

                    return std::task::Poll::Ready(Some(Ok(filtered_batch)));
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)));
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

impl datafusion_execution::RecordBatchStream for KeyBasedDeletionFilterStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ============================================================================
// Int64 Primary Key Deletion Filter
// ============================================================================

/// Execution plan that filters out deleted rows based on Int64 primary key values.
///
/// Optimised for the common case of tables with a single-column Int64 primary key.
/// Avoids `RowConverter` overhead and probes a [`DeletionIndex`] (bloom filter +
/// `HashMap<i64, i64>`) directly with native i64 comparisons.
pub struct Int64PkDeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Bloom-prefiltered index of deleted PK -> delete sequence number.
    deleted_pk_values: Arc<DeletionIndex>,
    /// Bloom-prefiltered index of upserted PK -> insert sequence number.
    insert_records: Arc<DeletionIndex>,
    /// Index of the primary key column in the schema
    pk_column_index: usize,
    /// Optional minimum sequence number — only deletions with
    /// `delete_seq > min_delete_seq_to_apply` are honoured. Used by the
    /// protected-snapshot scan path to skip deletions that pre-date the
    /// protected snapshot's creation without rebuilding the deletion
    /// index. `None` means apply every deletion in `deleted_pk_values`.
    min_delete_seq_to_apply: Option<i64>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl Int64PkDeletionFilterExec {
    /// Create a new Int64 PK-based deletion filter execution plan.
    ///
    /// # Arguments
    /// * `input` - The input execution plan to filter
    /// * `deleted_pk_values` - Bloom-prefiltered index of deleted PK values
    /// * `insert_records` - Bloom-prefiltered index of upserted PK values
    /// * `pk_column_index` - Index of the primary key column in the schema
    /// * `min_delete_seq_to_apply` - Optional protected-snapshot cutoff
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_pk_values: Arc<DeletionIndex>,
        insert_records: Arc<DeletionIndex>,
        pk_column_index: usize,
        min_delete_seq_to_apply: Option<i64>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            deleted_pk_values,
            insert_records,
            pk_column_index,
            min_delete_seq_to_apply,
            properties,
        }
    }
}

impl std::fmt::Debug for Int64PkDeletionFilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int64PkDeletionFilterExec")
    }
}

impl DisplayAs for Int64PkDeletionFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Int64PkDeletionFilterExec: filtered_keys={}, pk_col_idx={}",
            self.deleted_pk_values.len(),
            self.pk_column_index
        )
    }
}

impl ExecutionPlan for Int64PkDeletionFilterExec {
    fn name(&self) -> &'static str {
        "Int64PkDeletionFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Plan(
                "Int64PkDeletionFilterExec requires exactly 1 child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.deleted_pk_values),
            Arc::clone(&self.insert_records),
            self.pk_column_index,
            self.min_delete_seq_to_apply,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let deleted_pk_values = Arc::clone(&self.deleted_pk_values);
        let insert_records = Arc::clone(&self.insert_records);
        let pk_column_index = self.pk_column_index;
        let min_delete_seq_to_apply = self.min_delete_seq_to_apply;
        let schema = input_stream.schema();

        Ok(Box::pin(Int64PkDeletionFilterStream {
            input: input_stream,
            deleted_pk_values,
            insert_records,
            pk_column_index,
            min_delete_seq_to_apply,
            schema,
        }))
    }
}

/// Stream that filters out deleted rows based on Int64 primary key matching.
struct Int64PkDeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_pk_values: Arc<DeletionIndex>,
    insert_records: Arc<DeletionIndex>,
    pk_column_index: usize,
    /// If `Some(min)`, only deletions with `delete_seq > min` apply. Lets
    /// protected snapshots share one `deleted_pk_values` index instead of
    /// each snapshot owning a per-snapshot rebuilt copy.
    min_delete_seq_to_apply: Option<i64>,
    schema: arrow_schema::SchemaRef,
}

impl futures::Stream for Int64PkDeletionFilterStream {
    type Item = datafusion_common::Result<arrow::array::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use arrow::array::Int64Array;

        loop {
            match std::pin::Pin::new(&mut self.input).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    let batch_size = batch.num_rows();

                    if batch_size == 0 {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Fast path: empty deletion index
                    if self.deleted_pk_values.is_empty() {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Get the PK column and downcast to Int64Array
                    let Some(pk_column) = batch.columns().get(self.pk_column_index) else {
                        return std::task::Poll::Ready(Some(Err(
                            datafusion_common::DataFusionError::Internal(format!(
                                "Int64PkDeletionFilterExec primary key column index {} is out of bounds for a batch with {} columns",
                                self.pk_column_index,
                                batch.num_columns()
                            )),
                        )));
                    };
                    let pk_array =
                        pk_column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                datafusion_common::DataFusionError::Internal(format!(
                                    "Expected Int64Array for PK column at index {}, got {:?}",
                                    self.pk_column_index,
                                    pk_column.data_type()
                                ))
                            })?;

                    // Build keep mask: bloom-prefiltered probe per row + visibility check.
                    // Iterate over `pk_array.values()` (a contiguous &[i64] slice) so the
                    // hot loop stays branchless on column access. The mask uses a packed
                    // bitmap (`BooleanBufferBuilder`, 1 bit per row) instead of a
                    // `Vec<bool>` to save 8× the per-batch heap footprint and skip the
                    // `BooleanArray` re-pack pass.
                    let pk_slice = pk_array.values();
                    let mut keep_mask = BooleanBufferBuilder::new(batch_size);
                    let mut keep_count: usize = 0;
                    for &pk_value in pk_slice {
                        let visible = is_pk_visible_i64(
                            pk_value,
                            &self.deleted_pk_values,
                            &self.insert_records,
                            self.min_delete_seq_to_apply,
                        );
                        keep_mask.append(visible);
                        keep_count += usize::from(visible);
                    }

                    tracing::debug!(
                        "Int64PkDeletionFilterStream: keeping {} of {} rows",
                        keep_count,
                        batch_size
                    );

                    // If all rows are deleted, skip this batch and continue to next
                    if keep_count == 0 {
                        continue;
                    }

                    // If no rows are deleted, return the batch as-is (fast path)
                    if keep_count == batch_size {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Apply mask in one shot via Arrow's filter kernel.
                    let filter_array = BooleanArray::new(keep_mask.finish(), None);
                    let filtered_batch =
                        match arrow::compute::filter_record_batch(&batch, &filter_array) {
                            Ok(filtered) => filtered,
                            Err(e) => {
                                return std::task::Poll::Ready(Some(Err(
                                    datafusion_common::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    ),
                                )));
                            }
                        };

                    return std::task::Poll::Ready(Some(Ok(filtered_batch)));
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)));
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => {
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

impl datafusion_execution::RecordBatchStream for Int64PkDeletionFilterStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::RecordBatch, datatypes::DataType};
    use arrow_row::SortField;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use std::collections::HashMap;

    /// Regression for the iter-13 `apply_partial_deletion_filter` fix:
    /// probing the full deletion index with `min_delete_seq_to_apply` set
    /// must return identical visibility decisions to probing a freshly
    /// rebuilt index that contains only `delete_seq > min` entries.
    /// This is what lets the protected-snapshot scan path stop rebuilding
    /// the deletion index per snapshot (the O(N·M) cost measured by
    /// `apply_partial_deletion_filter_per_scan` bench).
    #[test]
    fn is_pk_visible_with_min_seq_matches_filtered_rebuild() {
        // Build a deletion cache with delete sequences 1..=10. The
        // protected snapshot's min cutoff retains seqs 6..=10 only.
        let mut full_entries: HashMap<i64, i64> = HashMap::new();
        for pk in 0..10_i64 {
            full_entries.insert(pk, pk + 1);
        }
        let full_index = DeletionIndex::from_map(full_entries.clone());
        let min_seq = 5_i64;

        let filtered_entries: HashMap<i64, i64> = full_entries
            .iter()
            .filter(|(_, seq)| **seq > min_seq)
            .map(|(&pk, &seq)| (pk, seq))
            .collect();
        let filtered_index = DeletionIndex::from_map(filtered_entries);
        let empty_inserts = DeletionIndex::empty();

        // Probe every key, including some that aren't in either index.
        for pk in -2..12_i64 {
            let probe_time_filter =
                is_pk_visible_i64(pk, &full_index, &empty_inserts, Some(min_seq));
            let rebuilt_filter = is_pk_visible_i64(pk, &filtered_index, &empty_inserts, None);
            assert_eq!(
                probe_time_filter, rebuilt_filter,
                "pk={pk}: probe-time min_seq filter must match a rebuilt index"
            );
        }

        // Byte-keyed variant: same property must hold for KeyDeletionIndex.
        let mut full_key_entries: HashMap<Box<[u8]>, i64> = HashMap::new();
        for pk in 0..10_i64 {
            full_key_entries.insert(Box::<[u8]>::from(pk.to_be_bytes().as_slice()), pk + 1);
        }
        let full_key_index = KeyDeletionIndex::from_map(full_key_entries.clone());
        let filtered_key_entries: HashMap<Box<[u8]>, i64> = full_key_entries
            .iter()
            .filter(|(_, seq)| **seq > min_seq)
            .map(|(k, &seq)| (k.clone(), seq))
            .collect();
        let filtered_key_index = KeyDeletionIndex::from_map(filtered_key_entries);
        let empty_key_inserts = KeyDeletionIndex::empty();

        for pk in -2..12_i64 {
            let key = pk.to_be_bytes();
            let probe_time =
                is_pk_visible_row_key(&key, &full_key_index, &empty_key_inserts, Some(min_seq));
            let rebuilt =
                is_pk_visible_row_key(&key, &filtered_key_index, &empty_key_inserts, None);
            assert_eq!(
                probe_time, rebuilt,
                "byte-key pk={pk}: probe-time min_seq filter must match a rebuilt KeyDeletionIndex"
            );
        }
    }

    #[tokio::test]
    async fn key_based_deletion_filter_passes_empty_batches_without_pk_columns()
    -> datafusion_common::Result<()> {
        let schema = Arc::new(arrow_schema::Schema::empty());
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter([Ok(empty_batch)]),
        ));
        let deleted_row_keys = Arc::new(KeyDeletionIndex::from_map(HashMap::from([(
            Box::<[u8]>::from([42_u8].as_slice()),
            1_i64,
        )])));
        let row_converter = Arc::new(RowConverter::new(vec![
            SortField::new(DataType::Int64),
            SortField::new(DataType::Int64),
            SortField::new(DataType::Int64),
        ])?);

        let mut stream = KeyBasedDeletionFilterStream {
            input,
            deleted_row_keys,
            insert_records: Arc::new(KeyDeletionIndex::empty()),
            pk_column_indices: Vec::new(),
            row_converter,
            min_delete_seq_to_apply: None,
            schema,
        };

        let Some(batch) = stream.next().await.transpose()? else {
            return Err(datafusion_common::DataFusionError::Internal(
                "Expected an empty batch from KeyBasedDeletionFilterStream".to_string(),
            ));
        };
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 0);

        Ok(())
    }
}
