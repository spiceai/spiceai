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
//!   Uses direct `HashMap<i64, i64>` lookup with no serialization overhead.
//!
//! - **`KeyBasedDeletionFilterExec`**: For tables with composite or non-integer primary keys.
//!   Uses Arrow's `RowConverter` to create deterministic byte keys for lookup.
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

use arrow::array::ArrayRef;
use arrow_row::RowConverter;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// PK Visibility Helpers
// ============================================================================
//
// These helper functions determine whether a row is visible (not deleted) based on
// the deletion and insert caches. A row is visible if:
// - It was never deleted (not in deletion cache), OR
// - It was deleted but re-inserted with a higher sequence number (upsert)
//
// The sequence-based ordering follows Iceberg semantics where:
// - `delete_sequence` records when a PK was marked for deletion
// - `insert_sequence` records when a PK was re-inserted (upsert)
// - If `insert_sequence > delete_sequence`, the row is visible (re-inserted after delete)

/// Check if a row with the given Int64 PK is visible (not deleted or re-inserted after deletion).
///
/// Returns `true` if the row should be visible in queries.
#[inline]
pub(crate) fn is_pk_visible_i64(
    pk: i64,
    deleted_pks: &HashMap<i64, i64>,
    insert_records: Option<&HashMap<i64, i64>>,
) -> bool {
    match deleted_pks.get(&pk) {
        None => true, // Not deleted, row is visible
        Some(&delete_seq) => {
            // Deleted - check if re-inserted with higher sequence
            insert_records
                .and_then(|cache| cache.get(&pk))
                .is_some_and(|&insert_seq| insert_seq > delete_seq)
        }
    }
}

/// Check if a row with the given byte key is visible (not deleted or re-inserted after deletion).
///
/// Returns `true` if the row should be visible in queries.
#[inline]
pub(crate) fn is_pk_visible_row_key(
    key: &[u8],
    deleted_keys: &HashMap<Box<[u8]>, i64>,
    insert_records: Option<&HashMap<Box<[u8]>, i64>>,
) -> bool {
    match deleted_keys.get(key) {
        None => true, // Not deleted, row is visible
        Some(&delete_seq) => {
            // Deleted - check if re-inserted with higher sequence
            insert_records
                .and_then(|cache| cache.get(key))
                .is_some_and(|&insert_seq| insert_seq > delete_seq)
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
/// A row is only filtered out if its key is in `deleted_row_keys` AND either:
/// - It's not in `insert_records`, OR
/// - Its `insert_sequence < delete_sequence` for that key
///
/// This allows upsert semantics without full table compaction.
///
/// # Zero-Copy Design
///
/// The deleted row keys are wrapped in `Arc` to enable zero-copy sharing across
/// concurrent scans.
pub struct KeyBasedDeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Map of deleted row keys (primary key bytes from `RowConverter`) to delete sequence
    deleted_row_keys: Arc<HashMap<Box<[u8]>, i64>>,
    /// Map of insert records: PK bytes -> insert sequence number (for upserted PKs)
    insert_records: Arc<HashMap<Box<[u8]>, i64>>,
    /// Indices of primary key columns in the schema
    pk_column_indices: Vec<usize>,
    /// `RowConverter` for converting PK columns to bytes
    row_converter: Arc<RowConverter>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl KeyBasedDeletionFilterExec {
    /// Create a new key-based deletion filter execution plan.
    ///
    /// # Arguments
    /// * `input` - The input execution plan to filter
    /// * `deleted_row_keys` - Map of deleted row keys (PK bytes) to delete sequence
    /// * `insert_records` - Map of insert records (PK bytes -> insert sequence)
    /// * `pk_column_indices` - Indices of primary key columns in the schema
    /// * `row_converter` - `RowConverter` configured for the PK columns
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_row_keys: Arc<HashMap<Box<[u8]>, i64>>,
        insert_records: Arc<HashMap<Box<[u8]>, i64>>,
        pk_column_indices: Vec<usize>,
        row_converter: Arc<RowConverter>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            deleted_row_keys,
            insert_records,
            pk_column_indices,
            row_converter,
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
        let schema = input_stream.schema();

        Ok(Box::pin(KeyBasedDeletionFilterStream {
            input: input_stream,
            deleted_row_keys,
            insert_records,
            pk_column_indices,
            row_converter,
            schema,
        }))
    }
}

/// Stream that filters out deleted rows based on primary key matching.
///
/// A row is deleted only if its key is in `deleted_row_keys` AND either:
/// - It's not in `insert_records`, OR
/// - Its `insert_sequence < delete_sequence` for that key
pub struct KeyBasedDeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_row_keys: Arc<HashMap<Box<[u8]>, i64>>,
    insert_records: Arc<HashMap<Box<[u8]>, i64>>,
    pk_column_indices: Vec<usize>,
    row_converter: Arc<RowConverter>,
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

                    // Fast path: empty deleted keys map
                    if self.deleted_row_keys.is_empty() {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Extract PK columns from the batch
                    let pk_columns: Vec<ArrayRef> = self
                        .pk_column_indices
                        .iter()
                        .map(|&idx| Arc::clone(batch.column(idx)))
                        .collect();

                    // Convert PK columns to row format
                    let rows = match self.row_converter.convert_columns(&pk_columns) {
                        Ok(rows) => rows,
                        Err(e) => {
                            return std::task::Poll::Ready(Some(Err(
                                datafusion_common::DataFusionError::ArrowError(Box::new(e), None),
                            )));
                        }
                    };

                    // Build keep mask by checking each row's key against deleted map
                    let mut keep_mask = Vec::with_capacity(batch_size);
                    for row in &rows {
                        let key: &[u8] = row.as_ref();
                        keep_mask.push(is_pk_visible_row_key(
                            key,
                            &self.deleted_row_keys,
                            Some(&self.insert_records),
                        ));
                    }

                    // Count how many rows we're keeping
                    let keep_count = keep_mask.iter().filter(|&&v| v).count();

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

                    // Use Arrow's filter kernel with boolean array
                    let filter_array = arrow::array::BooleanArray::from(keep_mask);
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
/// This is an optimized deletion filter for the common case of tables with a
/// single-column Int64 primary key. It avoids `RowConverter` overhead by working
/// directly with native Int64 values.
///
/// # Advantages over `KeyBasedDeletionFilterExec`
///
/// - **No serialization**: Direct i64 comparison vs byte array conversion
/// - **Smaller memory footprint**: 8 bytes per deleted key vs variable-length bytes
/// - **Faster lookup**: Native `HashMap<i64, i64>` vs `HashMap<Box<[u8]>, i64>`
/// - **Zero-copy**: Uses Arrow `Int64Array` directly
///
/// # Sequence-Based Ordering
///
/// Insert records track PKs that were deleted and then re-inserted (upserted).
/// A row is only filtered out if its PK is in `deleted_pk_values` AND either:
/// - It's not in `insert_records`, OR
/// - Its `insert_sequence < delete_sequence` for that PK
///
/// This allows upsert semantics without full table compaction.
pub struct Int64PkDeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Map of deleted primary key values to their delete sequence number
    deleted_pk_values: Arc<HashMap<i64, i64>>,
    /// Map of insert records: PK -> insert sequence number (for upserted PKs)
    insert_records: Arc<HashMap<i64, i64>>,
    /// Index of the primary key column in the schema
    pk_column_index: usize,
    properties: datafusion_physical_plan::PlanProperties,
}

impl Int64PkDeletionFilterExec {
    /// Create a new Int64 PK-based deletion filter execution plan.
    ///
    /// # Arguments
    /// * `input` - The input execution plan to filter
    /// * `deleted_pk_values` - Map of deleted primary key values to delete sequence
    /// * `insert_records` - Map of insert records (PK -> insert sequence)
    /// * `pk_column_index` - Index of the primary key column in the schema
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_pk_values: Arc<HashMap<i64, i64>>,
        insert_records: Arc<HashMap<i64, i64>>,
        pk_column_index: usize,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            deleted_pk_values,
            insert_records,
            pk_column_index,
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
        let schema = input_stream.schema();

        Ok(Box::pin(Int64PkDeletionFilterStream {
            input: input_stream,
            deleted_pk_values,
            insert_records,
            pk_column_index,
            schema,
        }))
    }
}

/// Stream that filters out deleted rows based on Int64 primary key matching.
///
/// A row is deleted only if its PK is in `deleted_pk_values` AND either:
/// - It's not in `insert_records`, OR
/// - Its `insert_sequence < delete_sequence` for that PK
struct Int64PkDeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_pk_values: Arc<HashMap<i64, i64>>,
    insert_records: Arc<HashMap<i64, i64>>,
    pk_column_index: usize,
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

                    // Fast path: empty deleted keys map
                    if self.deleted_pk_values.is_empty() {
                        return std::task::Poll::Ready(Some(Ok(batch)));
                    }

                    // Get the PK column and downcast to Int64Array
                    let pk_column = batch.column(self.pk_column_index);
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

                    // Build keep mask by checking each row's PK value against deleted map
                    let mut keep_mask = Vec::with_capacity(batch_size);
                    for i in 0..batch_size {
                        let pk_value = pk_array.value(i);
                        keep_mask.push(is_pk_visible_i64(
                            pk_value,
                            &self.deleted_pk_values,
                            Some(&self.insert_records),
                        ));
                    }

                    // Count how many rows we're keeping
                    let keep_count = keep_mask.iter().filter(|&&v| v).count();

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

                    // Use Arrow's filter kernel with boolean array
                    let filter_array = arrow::array::BooleanArray::from(keep_mask);
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
