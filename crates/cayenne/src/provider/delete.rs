/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Deletion logic for Cayenne tables.
//!
//! This module provides three deletion filtering strategies:
//!
//! - **Position-based (`DeletionFilterExec`)**: For tables WITHOUT a primary key.
//!   Uses row position within the table. Requires `CoalescePartitionsExec` to ensure
//!   consistent ordering between delete and read operations.
//!
//! - **Int64 PK-based (`Int64PkDeletionFilterExec`)**: For tables with a single-column
//!   Int64 primary key. Uses direct `HashSet<i64>` lookup - no serialization overhead.
//!   This is the most efficient deletion strategy for the common case.
//!
//! - **RowConverter-based (`KeyBasedDeletionFilterExec`)**: For tables with composite
//!   or non-integer primary keys. Uses Arrow's `RowConverter` to create deterministic
//!   byte keys. More overhead but handles all PK types.
//!
//! Also provides:
//! - `CayenneDeletionSink`: Handles writing deletion vectors to storage

use super::constants::DELETION_CACHE_LOCK_POISONED;
use super::utils::{convert_to_i64, convert_to_i64_box, convert_to_u64_box};
use crate::catalog::{CatalogError, MetadataCatalog};
use crate::deletion::{DeletionIdentifier, DeletionVectorWriteSpec, DeletionVectorWriter};
use crate::metadata::TableMetadata;
use arrow::array::ArrayRef;
use arrow_row::RowConverter;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::TableProvider;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::Expr;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use roaring::RoaringBitmap;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

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

/// Execution plan that filters out deleted rows based on deletion vectors.
///
/// This wraps another execution plan and removes rows whose positions
/// match the deleted row IDs loaded from deletion vector files.
///
/// # Row ID Semantics
///
/// Deletion vectors store position-based row IDs that are assigned during the
/// delete operation's table scan. To ensure consistent row ID assignment during
/// reads, this exec coalesces all input partitions into a single stream before
/// applying the deletion filter. This guarantees that row IDs match regardless
/// of how many files or partitions the underlying data is spread across.
///
/// # Zero-Copy Design
///
/// The deleted row IDs are wrapped in `Arc` to enable zero-copy sharing across
/// concurrent scans. This avoids cloning potentially large bitmaps on every scan,
/// aligning with the project's zero-copy principles for Arrow data.
pub struct DeletionFilterExec {
    /// The input execution plan (possibly wrapped in `CoalescePartitionsExec`)
    input: Arc<dyn ExecutionPlan>,
    deleted_row_ids: Arc<RoaringBitmap>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl DeletionFilterExec {
    /// Create a new deletion filter execution plan.
    ///
    /// If the input has multiple partitions, it will be wrapped in a
    /// `CoalescePartitionsExec` to ensure consistent row ID assignment.
    /// Deletion vectors use position-based IDs, so all data must be processed
    /// in a single partition to match the order used during deletion.
    pub fn new(input: Arc<dyn ExecutionPlan>, deleted_row_ids: Arc<RoaringBitmap>) -> Self {
        // If the input has multiple partitions, wrap it in CoalescePartitionsExec
        // to ensure consistent row ordering for position-based deletion vectors.
        let (coalesced_input, properties) = if input.properties().partitioning.partition_count() > 1
        {
            let coalesced = Arc::new(CoalescePartitionsExec::new(input));
            let props = coalesced.properties().clone();
            (coalesced as Arc<dyn ExecutionPlan>, props)
        } else {
            let props = input.properties().clone();
            (input, props)
        };

        Self {
            input: coalesced_input,
            deleted_row_ids,
            properties,
        }
    }
}

impl std::fmt::Debug for DeletionFilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeletionFilterExec")
    }
}

impl DisplayAs for DeletionFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DeletionFilterExec: filtered_rows={}",
            self.deleted_row_ids.len()
        )
    }
}

impl ExecutionPlan for DeletionFilterExec {
    fn name(&self) -> &'static str {
        "DeletionFilterExec"
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
                "DeletionFilterExec requires exactly 1 child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.deleted_row_ids),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        // Zero-copy Arc clone - just increments reference count
        let deleted_row_ids = Arc::clone(&self.deleted_row_ids);
        let schema = input_stream.schema();

        Ok(Box::pin(DeletionFilterStream {
            input: input_stream,
            deleted_row_ids,
            schema,
            global_row_offset: 0,
        }))
    }
}

/// Stream that filters out deleted rows from an input stream.
pub struct DeletionFilterStream {
    input: SendableRecordBatchStream,
    deleted_row_ids: Arc<RoaringBitmap>,
    schema: arrow_schema::SchemaRef,
    global_row_offset: i64,
}

impl futures::Stream for DeletionFilterStream {
    type Item = datafusion_common::Result<arrow::array::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match std::pin::Pin::new(&mut self.input).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    let current_offset = self.global_row_offset;
                    let batch_size = batch.num_rows();

                    // Validate batch size upfront to avoid redundant error handling in hot path
                    let batch_size_i64 = match convert_to_i64(batch_size, "batch size") {
                        Ok(value) => value,
                        Err(err) => return std::task::Poll::Ready(Some(Err(err))),
                    };

                    self.global_row_offset =
                        match self.global_row_offset.checked_add(batch_size_i64) {
                            Some(value) => value,
                            None => {
                                return std::task::Poll::Ready(Some(Err(
                                    datafusion_common::DataFusionError::Execution(
                                        "Row ID overflow while updating global offset".to_string(),
                                    ),
                                )))
                            }
                        };

                    tracing::debug!(
                        "DeletionFilterStream: processing batch with {} rows, offset {}, deleted_row_ids count: {}",
                        batch_size, current_offset, self.deleted_row_ids.len()
                    );

                    // Optimization: Build a boolean mask for vectorized filtering using Arrow compute kernels
                    // This is more efficient than building indices and using the take kernel
                    // Pre-allocate with capacity for the entire batch
                    let mut keep_mask = Vec::with_capacity(batch_size);

                    // Vectorized row filtering: batch the contains() checks for better performance
                    for row_idx in 0..batch_size {
                        // Convert once - we've already validated batch_size fits in i64
                        let row_offset = match convert_to_i64(row_idx, "row index") {
                            Ok(value) => value,
                            Err(err) => {
                                return std::task::Poll::Ready(Some(Err(err)));
                            }
                        };
                        let Some(global_row_id) = current_offset.checked_add(row_offset) else {
                            return std::task::Poll::Ready(Some(Err(
                                datafusion_common::DataFusionError::Execution(
                                    "Row ID overflow while calculating global row id".to_string(),
                                ),
                            )));
                        };

                        // Check if row is deleted using RoaringBitmap's u32 API
                        // RoaringBitmap uses u32 internally. Row IDs >= 2^32 should trigger compaction
                        // rather than being supported directly.
                        let is_deleted = if let Ok(row_id_u32) = u32::try_from(global_row_id) {
                            self.deleted_row_ids.contains(row_id_u32)
                        } else {
                            // Row ID exceeds u32::MAX - this indicates table needs compaction
                            tracing::warn!(
                                "Row ID {} exceeds u32::MAX - table should be compacted to clear deletion vectors",
                                global_row_id
                            );
                            false
                        };

                        // Build boolean mask: true = keep, false = delete
                        keep_mask.push(!is_deleted);
                    }

                    // Count how many rows we're keeping
                    let keep_count = keep_mask.iter().filter(|&&v| v).count();

                    tracing::debug!(
                        "DeletionFilterStream: keeping {} of {} rows",
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

                    // Use Arrow's filter kernel with boolean array for SIMD-optimized filtering
                    // This is faster than the take kernel with indices for this use case
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

impl datafusion_execution::RecordBatchStream for DeletionFilterStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
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
/// Unlike position-based deletion (`DeletionFilterExec`), this uses Arrow's
/// `RowConverter` to create deterministic byte representations of primary key
/// columns. This approach:
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

/// Deletion sink for Cayenne tables.
///
/// This sink handles the process of marking rows as deleted by writing
/// deletion vectors to storage. Supports three deletion strategies:
/// - Position-based deletion (for tables without primary key)
/// - Int64 PK deletion (for tables with single-column Int64 primary key)
/// - Key-based deletion (for tables with composite/non-integer primary key)
pub struct CayenneDeletionSink {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    /// Reference to the cached position-based deletion vectors.
    /// Uses Arc-wrapped `RoaringBitmap` for zero-copy sharing across concurrent operations.
    cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
    /// Reference to the cached Int64 PK deletion vectors.
    /// Uses Arc-wrapped `HashMap<i64, i64>` for direct PK lookup (PK -> delete sequence).
    cached_deleted_pk_i64: Arc<RwLock<Arc<HashMap<i64, i64>>>>,
    /// Reference to the cached key-based deletion vectors.
    /// Uses Arc-wrapped `HashMap<Box<[u8]>, i64>` for zero-copy sharing (PK bytes -> delete sequence).
    #[expect(clippy::type_complexity)]
    cached_deleted_row_keys: Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>,
    /// Deletion strategy for this table.
    pk_deletion_strategy: super::table::PkDeletionStrategy,
    /// `RowConverter` for converting primary key columns to byte representation.
    /// Only set for tables with composite or non-integer primary keys.
    pk_row_converter: Option<Arc<RowConverter>>,
    /// Indices of primary key columns in the table schema.
    pk_column_indices: Vec<usize>,
    /// Additional listing tables from protected snapshots that should also be scanned for deletions.
    protected_snapshot_tables: Vec<Arc<ListingTable>>,
}

impl CayenneDeletionSink {
    /// Create a new deletion sink.
    #[expect(clippy::too_many_arguments)]
    #[expect(clippy::type_complexity)]
    pub fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        schema: SchemaRef,
        filters: &[Expr],
        cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
        cached_deleted_pk_i64: Arc<RwLock<Arc<HashMap<i64, i64>>>>,
        cached_deleted_row_keys: Arc<RwLock<Arc<HashMap<Box<[u8]>, i64>>>>,
        pk_deletion_strategy: super::table::PkDeletionStrategy,
        pk_row_converter: Option<Arc<RowConverter>>,
        pk_column_indices: Vec<usize>,
        protected_snapshot_tables: Vec<Arc<ListingTable>>,
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
            cached_deleted_row_ids,
            cached_deleted_pk_i64,
            cached_deleted_row_keys,
            pk_deletion_strategy,
            pk_row_converter,
            pk_column_indices,
            protected_snapshot_tables,
        }
    }

    async fn delete_all_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Collect batches from all tables
        let mut all_batches = Vec::new();
        for table in tables {
            let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
            let batches = collect(scan_plan, ctx.task_ctx()).await?;
            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            return Ok(0);
        }

        // Use the appropriate deletion strategy based on table configuration
        match self.pk_deletion_strategy {
            super::table::PkDeletionStrategy::Int64Pk => {
                // Int64 PK deletion - extract PK values directly
                let concatenated_batch =
                    arrow::compute::concat_batches(&self.schema, &all_batches)?;
                let pk_values = self.extract_int64_pk_values(&concatenated_batch)?;
                self.persist_int64_pk_deletions(pk_values).await
            }
            super::table::PkDeletionStrategy::RowConverterBased => {
                // RowConverter-based deletion for composite/non-integer PKs
                if let Some(ref row_converter) = self.pk_row_converter {
                    let concatenated_batch =
                        arrow::compute::concat_batches(&self.schema, &all_batches)?;
                    let row_keys = self.extract_row_keys(&concatenated_batch, row_converter)?;
                    self.persist_key_based_deletions(row_keys).await
                } else {
                    Err("RowConverter not available for RowConverterBased strategy".into())
                }
            }
            super::table::PkDeletionStrategy::PositionBased => {
                // Position-based deletion for tables without primary key
                let total_rows: usize = all_batches
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum();
                let total_rows_i64 = convert_to_i64_box(total_rows, "total row count")?;
                let row_ids: Vec<i64> = (0..total_rows_i64).collect();
                self.persist_position_based_deletions(row_ids).await
            }
        }
    }

    #[expect(dead_code)]
    async fn delete_all_rows(
        &self,
        ctx: &SessionContext,
        listing_table: Arc<ListingTable>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        self.delete_all_rows_from_tables(ctx, &[listing_table])
            .await
    }

    /// Extract Int64 primary key values from a batch.
    fn extract_int64_pk_values(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::Int64Array;

        // For Int64 PK strategy, we only have one PK column
        let pk_column_index = self
            .pk_column_indices
            .first()
            .ok_or("Int64 PK strategy requires exactly one PK column index")?;

        let pk_column = batch.column(*pk_column_index);
        let pk_array = pk_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!(
                    "Expected Int64Array for PK column at index {pk_column_index}, got {:?}",
                    pk_column.data_type()
                )
            })?;

        let pk_values: Vec<i64> = pk_array.values().iter().copied().collect();
        Ok(pk_values)
    }

    /// Extract row keys from a batch using the `RowConverter`.
    fn extract_row_keys(
        &self,
        batch: &arrow::array::RecordBatch,
        row_converter: &RowConverter,
    ) -> Result<Vec<Box<[u8]>>, Box<dyn std::error::Error + Send + Sync>> {
        let pk_columns: Vec<ArrayRef> = self
            .pk_column_indices
            .iter()
            .map(|&idx| Arc::clone(batch.column(idx)))
            .collect();

        let rows = row_converter.convert_columns(&pk_columns)?;

        let row_keys: Vec<Box<[u8]>> = rows
            .iter()
            .map(|row| row.as_ref().to_vec().into_boxed_slice())
            .collect();

        Ok(row_keys)
    }

    async fn delete_filtered_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::{Array, AsArray, Int64Array};
        use arrow::datatypes::{DataType, Field};

        // Collect batches from all tables
        let mut all_batches = Vec::new();
        for table in tables {
            let scan_plan = table.scan(&ctx.state(), None, &[], None).await?;
            let batches = collect(scan_plan, ctx.task_ctx()).await?;
            all_batches.extend(batches);
        }

        // If no data, nothing to delete
        if all_batches.is_empty() {
            return Ok(0);
        }

        // Flatten all batches into one for simpler processing
        let concatenated_batch = arrow::compute::concat_batches(&self.schema, &all_batches)?;
        let total_rows = concatenated_batch.num_rows();

        // Create a batch with row_id column added (needed for filtering)
        let row_id_array =
            Int64Array::from_iter_values(0..convert_to_i64_box(total_rows, "total rows")?);

        let mut fields = vec![Field::new("__row_id", DataType::Int64, false)];
        for field in self.schema.fields() {
            fields.push((**field).clone());
        }
        let schema_with_rowid = Arc::new(arrow::datatypes::Schema::new(fields));

        let mut columns_with_rowid = vec![Arc::new(row_id_array) as Arc<dyn Array>];
        columns_with_rowid.extend_from_slice(concatenated_batch.columns());

        let batch_with_rowid =
            arrow::array::RecordBatch::try_new(Arc::clone(&schema_with_rowid), columns_with_rowid)?;

        // Create a new session context with the row_id data
        let ctx_new = SessionContext::new();
        let mem_table_with_rowid = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema_with_rowid),
            vec![vec![batch_with_rowid]],
        )?;
        ctx_new.register_table("data_with_rowid", Arc::new(mem_table_with_rowid))?;

        // Start with selecting all columns so filters can reference them
        let mut filtered_df = ctx_new.sql("SELECT * FROM data_with_rowid").await?;

        // Apply all filters
        for filter in &self.filters {
            filtered_df = filtered_df.filter(filter.clone())?;
        }

        // Collect filtered results (includes __row_id and all original columns)
        let filtered_batches = filtered_df.collect().await?;

        if filtered_batches.is_empty() {
            return Ok(0);
        }

        // Use the appropriate deletion strategy based on table configuration
        match self.pk_deletion_strategy {
            super::table::PkDeletionStrategy::Int64Pk => {
                // Int64 PK deletion - extract PK values directly
                // Note: column indices are offset by 1 because __row_id is at index 0
                let first_batch = filtered_batches
                    .first()
                    .ok_or("Expected at least one batch after filtering (checked above)")?;
                let filtered_concat =
                    arrow::compute::concat_batches(&first_batch.schema(), &filtered_batches)?;

                let pk_column_index = self
                    .pk_column_indices
                    .first()
                    .ok_or("Int64 PK strategy requires exactly one PK column index")?;

                let pk_column = filtered_concat.column(*pk_column_index + 1); // +1 for __row_id offset
                let pk_array =
                    pk_column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            format!(
                        "Expected Int64Array for PK column at index {pk_column_index}, got {:?}",
                        pk_column.data_type()
                    )
                        })?;

                let pk_values: Vec<i64> = pk_array.values().iter().copied().collect();
                self.persist_int64_pk_deletions(pk_values).await
            }
            super::table::PkDeletionStrategy::RowConverterBased => {
                // RowConverter-based deletion for composite/non-integer PKs
                if let Some(ref row_converter) = self.pk_row_converter {
                    let filtered_concat = arrow::compute::concat_batches(
                        &filtered_batches[0].schema(),
                        &filtered_batches,
                    )?;

                    let pk_columns: Vec<ArrayRef> = self
                        .pk_column_indices
                        .iter()
                        .map(|&idx| Arc::clone(filtered_concat.column(idx + 1))) // +1 for __row_id offset
                        .collect();

                    let rows = row_converter.convert_columns(&pk_columns)?;
                    let row_keys: Vec<Box<[u8]>> = rows
                        .iter()
                        .map(|row| row.as_ref().to_vec().into_boxed_slice())
                        .collect();

                    self.persist_key_based_deletions(row_keys).await
                } else {
                    Err("RowConverter not available for RowConverterBased strategy".into())
                }
            }
            super::table::PkDeletionStrategy::PositionBased => {
                // Position-based deletion for tables without primary key
                let mut row_ids = Vec::new();
                for batch in filtered_batches {
                    let row_id_column = batch
                        .column(0)
                        .as_primitive::<arrow::datatypes::Int64Type>();
                    for i in 0..row_id_column.len() {
                        if !row_id_column.is_null(i) {
                            row_ids.push(row_id_column.value(i));
                        }
                    }
                }

                self.persist_position_based_deletions(row_ids).await
            }
        }
    }

    #[expect(dead_code)]
    async fn delete_filtered_rows(
        &self,
        ctx: &SessionContext,
        listing_table: Arc<ListingTable>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        self.delete_filtered_rows_from_tables(ctx, &[listing_table])
            .await
    }

    async fn persist_position_based_deletions(
        &self,
        row_ids: Vec<i64>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_row_ids = self.filter_existing_position_deletions(row_ids).await?;

        if filtered_row_ids.is_empty() {
            return Ok(0);
        }

        let writer = DeletionVectorWriter::new(&self.table_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new(filtered_row_ids)])
            .await
            .map_err(catalog_error_to_box)?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog
            .add_delete_file(result.delete_file)
            .await
            .map_err(catalog_error_to_box)?;

        // Extract row IDs from the result
        let written_row_ids = match &result.identifiers {
            DeletionIdentifier::PositionBased(ids) => ids,
            DeletionIdentifier::KeyBased(_) => {
                // This code path uses position-based deletion
                return Err("Unexpected key-based deletion in position-based sink".into());
            }
        };

        // Update the cached deletion vectors with the newly deleted row IDs
        // This avoids needing to reload from SQLite on the next scan.
        //
        // We create a new Arc with the updated HashSet to maintain zero-copy semantics
        // for concurrent readers who still hold references to the old Arc. This requires
        // cloning the entire HashSet, which is acceptable because:
        //
        // 1. **Write infrequency**: Deletions happen much less frequently than reads
        // 2. **Concurrent reader safety**: Cloning prevents disrupting ongoing scans that
        //    hold Arc references to the old deletion set
        // 3. **Cache coherence**: The alternative (in-place mutation) would require either:
        //    - Taking write locks during scans (blocks all concurrent queries)
        //    - Complex lock-free data structures (higher complexity, potential performance issues)
        //
        // For tables with very large deletion sets (millions of deleted rows), consider
        // running compaction to physically remove deleted rows and reset the deletion vectors.
        {
            let mut guard = self
                .cached_deleted_row_ids
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            // Clone the entire RoaringBitmap and add new deletions.
            // Cost: O(n) where n = existing deleted rows, but this is write path (infrequent).
            // Benefit: Zero-copy Arc clones for concurrent readers (frequent).
            let mut updated_set = (**guard).clone();
            // Convert i64 row IDs to u32 for RoaringBitmap
            for &row_id in written_row_ids {
                if let Ok(row_id_u32) = u32::try_from(row_id) {
                    updated_set.insert(row_id_u32);
                } else {
                    tracing::warn!(
                        "Skipping row ID {} that exceeds u32::MAX - table should be compacted",
                        row_id
                    );
                }
            }

            // Replace with new Arc - concurrent readers still have old Arc
            *guard = Arc::new(updated_set);
        }

        let deleted_count = convert_to_u64_box(written_row_ids.len(), "deleted row count")?;

        tracing::debug!(
            "Position-based deletion vector written and cache updated: {} row(s) at {:?}",
            deleted_count,
            result.path
        );

        Ok(deleted_count)
    }

    async fn persist_key_based_deletions(
        &self,
        row_keys: Vec<Box<[u8]>>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_row_keys = Self::filter_existing_key_deletions(row_keys);

        if filtered_row_keys.is_empty() {
            return Ok(0);
        }

        // Count how many keys are NEW deletions (not already in the cache).
        // This gives an accurate count of newly deleted rows for the return value.
        let new_deletion_count = {
            let guard = self
                .cached_deleted_row_keys
                .read()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;
            filtered_row_keys
                .iter()
                .filter(|key| !guard.contains_key(key.as_ref()))
                .count()
        };

        // Get a fresh sequence number from the catalog for this deletion operation.
        // This ensures each delete has a unique, monotonically increasing sequence number
        // that's higher than any previous operation.
        let delete_sequence = self
            .catalog
            .increment_sequence_number(self.table_metadata.table_id)
            .await
            .map_err(catalog_error_to_box)?;

        // Create a temporary metadata with the fresh sequence number
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;

        let writer = DeletionVectorWriter::new(&temp_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new_key_based(
                filtered_row_keys,
            )])
            .await
            .map_err(catalog_error_to_box)?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog
            .add_delete_file(result.delete_file)
            .await
            .map_err(catalog_error_to_box)?;

        // Extract row keys from the result
        let written_row_keys = match &result.identifiers {
            DeletionIdentifier::KeyBased(keys) => keys,
            DeletionIdentifier::PositionBased(_) => {
                return Err("Unexpected position-based deletion in key-based sink".into());
            }
        };

        // Update the cached deletion keys with sequence number
        {
            let mut guard = self
                .cached_deleted_row_keys
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            let mut updated_map = (**guard).clone();
            for key in written_row_keys {
                // Update with max sequence if key already exists
                updated_map
                    .entry(key.clone())
                    .and_modify(|seq| *seq = (*seq).max(delete_sequence))
                    .or_insert(delete_sequence);
            }

            *guard = Arc::new(updated_map);
        }

        let deleted_count = convert_to_u64_box(new_deletion_count, "deleted row count")?;

        tracing::debug!(
            "Key-based deletion vector written and cache updated: {} key(s) (seq={}) at {:?}",
            deleted_count,
            delete_sequence,
            result.path
        );

        Ok(deleted_count)
    }

    async fn persist_int64_pk_deletions(
        &self,
        pk_values: Vec<i64>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_pk_values = Self::filter_existing_int64_pk_deletions(pk_values);

        if filtered_pk_values.is_empty() {
            return Ok(0);
        }

        // Count how many PKs are NEW deletions (not already in the cache).
        // This gives an accurate count of newly deleted rows for the return value.
        let new_deletion_count = {
            let guard = self
                .cached_deleted_pk_i64
                .read()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;
            filtered_pk_values
                .iter()
                .filter(|pk| !guard.contains_key(*pk))
                .count()
        };

        // Get a fresh sequence number from the catalog for this deletion operation.
        // This ensures each delete has a unique, monotonically increasing sequence number
        // that's higher than any previous operation.
        let delete_sequence = self
            .catalog
            .increment_sequence_number(self.table_metadata.table_id)
            .await
            .map_err(catalog_error_to_box)?;

        // For Int64 PK deletions, we store them as key-based deletions
        // where each key is the 8-byte big-endian representation of the i64 value.
        // This allows efficient storage and lookup.
        let row_keys: Vec<Box<[u8]>> = filtered_pk_values
            .iter()
            .map(|&pk| pk.to_be_bytes().to_vec().into_boxed_slice())
            .collect();

        // Create a temporary metadata with the fresh sequence number
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;

        let writer = DeletionVectorWriter::new(&temp_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new_key_based(row_keys)])
            .await
            .map_err(catalog_error_to_box)?;

        let Some(result) = results.pop() else {
            return Ok(0);
        };

        self.catalog
            .add_delete_file(result.delete_file)
            .await
            .map_err(catalog_error_to_box)?;

        // Update the cached Int64 PK deletion map with sequence number
        {
            let mut guard = self
                .cached_deleted_pk_i64
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            let mut updated_map = (**guard).clone();
            for &pk_value in &filtered_pk_values {
                // Update with max sequence if key already exists
                updated_map
                    .entry(pk_value)
                    .and_modify(|seq| *seq = (*seq).max(delete_sequence))
                    .or_insert(delete_sequence);
            }

            *guard = Arc::new(updated_map);
        }

        let deleted_count = convert_to_u64_box(new_deletion_count, "deleted row count")?;

        tracing::debug!(
            "Int64 PK deletion vector written and cache updated: {} key(s) (seq={}) at {:?}",
            deleted_count,
            delete_sequence,
            result.path
        );

        Ok(deleted_count)
    }

    fn filter_existing_int64_pk_deletions(pk_values: Vec<i64>) -> Vec<i64> {
        // For sequence-based ordering, we MUST write new deletion files even for
        // PKs that were already deleted, because the new deletion has a higher
        // sequence number. This ensures proper ordering: data written after the
        // first delete but before the second delete will be properly filtered.
        //
        // We only deduplicate within the current batch (in DeletionVectorWriter).
        pk_values
    }

    async fn filter_existing_position_deletions(
        &self,
        row_ids: Vec<i64>,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }

        let delete_files = self
            .catalog
            .get_table_delete_files(self.table_metadata.table_id)
            .await
            .map_err(catalog_error_to_box)?;

        if delete_files.is_empty() {
            return Ok(row_ids);
        }

        let delete_files_for_read = delete_files.clone();
        let existing_row_ids =
            tokio::task::spawn_blocking(move || read_deletion_vectors(delete_files_for_read))
                .await
                .map_err(|source| catalog_error_to_box(CatalogError::TaskJoin { source }))?
                .map_err(|err| {
                    catalog_error_to_box(CatalogError::InvalidOperation {
                        message: "Failed to read existing deletion vectors".to_string(),
                        source: Box::new(err),
                    })
                })?;

        // Filter out row_ids that already exist in deletion vectors
        Ok(row_ids
            .into_iter()
            .filter(|&row_id| {
                // Convert i64 to u32 for RoaringBitmap lookup
                if let Ok(row_id_u32) = u32::try_from(row_id) {
                    !existing_row_ids.contains(row_id_u32)
                } else {
                    // Row ID exceeds u32::MAX - keep it (not in bitmap)
                    true
                }
            })
            .collect())
    }

    fn filter_existing_key_deletions(row_keys: Vec<Box<[u8]>>) -> Vec<Box<[u8]>> {
        // For sequence-based ordering, we MUST write new deletion files even for
        // PKs that were already deleted, because the new deletion has a higher
        // sequence number. This ensures proper ordering: data written after the
        // first delete but before the second delete will be properly filtered.
        //
        // We only deduplicate within the current batch (in DeletionVectorWriter).
        row_keys
    }
}

fn catalog_error_to_box(err: CatalogError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(err)
}

#[async_trait]
impl DeletionSink for CayenneDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();

        let listing_table = {
            let guard = self
                .listing_table
                .read()
                .map_err(|_| super::constants::LISTING_TABLE_LOCK_POISONED.to_string())?;
            Arc::clone(&guard)
        };

        // Collect all tables to scan: main listing table + protected snapshots
        let mut all_tables = vec![Arc::clone(&listing_table)];
        for protected_table in &self.protected_snapshot_tables {
            all_tables.push(Arc::clone(protected_table));
        }

        if self.filters.is_empty() {
            return self.delete_all_rows_from_tables(&ctx, &all_tables).await;
        }

        self.delete_filtered_rows_from_tables(&ctx, &all_tables)
            .await
    }
}

/// Read deletion vectors from files and return a `RoaringBitmap` of deleted row IDs.
///
/// # Blocking I/O Warning
///
/// This function performs **blocking file system I/O** operations using `std::fs::File::open`
/// and must be called from within `tokio::task::spawn_blocking` to avoid blocking the async
/// runtime. The caller is responsible for offloading this to a blocking task context.
///
/// See: Project coding guidelines on Async/Blocking Patterns
///
/// # Design Constraints
///
/// `RoaringBitmap` uses u32 internally, supporting row IDs 0 to ~4 billion. Row IDs beyond
/// `u32::MAX` are logged as warnings and skipped. Tables with excessive deleted rows should
/// trigger compaction to remove deleted rows and clear deletion vectors. Large deletion vector
/// sets indicate poor table health and severely degrade query performance.
///
/// # Performance Optimizations
///
/// Uses `RoaringBitmap` for:
/// - SIMD-optimized contains operations (used in hot read path)
/// - 50-90% memory savings vs `HashSet` for sparse deletions
/// - Efficient bulk insertion using Arrow's contiguous arrays
///
/// # Errors
///
/// Returns an error if any deletion vector file cannot be read or parsed.
pub fn read_deletion_vectors(
    delete_files: Vec<crate::metadata::DeleteFile>,
) -> datafusion_common::Result<RoaringBitmap> {
    use arrow::array::Array;
    use arrow::ipc::reader::FileReader;

    let mut deleted_row_ids = RoaringBitmap::new();
    let file_count = delete_files.len();

    tracing::debug!(
        "read_deletion_vectors: processing {} delete files",
        file_count
    );

    // Track overflow occurrences to log once at the end instead of per-row
    let mut overflow_count: u64 = 0;
    let mut first_overflow_id: Option<i64> = None;

    for delete_file in delete_files {
        let path = std::path::Path::new(&delete_file.path);
        tracing::debug!("read_deletion_vectors: reading file {:?}", path);

        // Read deletion vector file
        let file = std::fs::File::open(path).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to open deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        let reader = FileReader::try_new(file, None).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        // Read all batches and extract row IDs
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read batch from deletion vector: {e}"
                ))
            })?;

            // Get row_id column (first column)
            let row_id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "Expected Int64Array for row_id column".to_string(),
                    )
                })?;

            // Optimized bulk insertion using Arrow's contiguous values slice
            // This is SIMD-friendly and avoids per-element overhead
            let values = row_id_array.values(); // &[i64] - contiguous memory

            if row_id_array.null_count() == 0 {
                // Fast path: no nulls, bulk insert entire slice
                for &row_id in values {
                    // Convert i64 to u32 for RoaringBitmap
                    // Row IDs >= 4 billion should have triggered compaction
                    if let Ok(row_id_u32) = u32::try_from(row_id) {
                        deleted_row_ids.insert(row_id_u32);
                    } else {
                        // Track overflow for single warning at end
                        if first_overflow_id.is_none() {
                            first_overflow_id = Some(row_id);
                        }
                        overflow_count += 1;
                    }
                }
            } else {
                // Slow path: check validity bitmap for nulls
                for i in 0..row_id_array.len() {
                    if row_id_array.is_valid(i) {
                        let row_id = values[i];
                        if let Ok(row_id_u32) = u32::try_from(row_id) {
                            deleted_row_ids.insert(row_id_u32);
                        } else {
                            // Track overflow for single warning at end
                            if first_overflow_id.is_none() {
                                first_overflow_id = Some(row_id);
                            }
                            overflow_count += 1;
                        }
                    }
                }
            }
        }
    }

    // Log once if any overflows occurred, avoiding hot path log spam
    if overflow_count > 0 {
        tracing::warn!(
            "Skipped {} row ID(s) that exceed u32::MAX (first: {}) - table should be compacted to remove deleted rows",
            overflow_count,
            first_overflow_id.unwrap_or(0)
        );
    }

    tracing::debug!(
        "Loaded {} deleted row IDs from {} deletion vector files",
        deleted_row_ids.len(),
        file_count
    );

    Ok(deleted_row_ids)
}

/// Read key-based deletion vectors from files and return a `HashSet` of deleted row keys.
///
/// # Blocking I/O Warning
///
/// This function performs **blocking file system I/O** operations and must be called
/// from within `tokio::task::spawn_blocking`.
///
/// # Errors
///
/// Returns an error if any deletion vector file cannot be read or parsed.
#[expect(dead_code)]
pub fn read_key_based_deletion_vectors(
    delete_files: Vec<crate::metadata::DeleteFile>,
) -> datafusion_common::Result<HashSet<Box<[u8]>>> {
    use arrow::array::{Array, BinaryArray};
    use arrow::ipc::reader::FileReader;

    let mut deleted_row_keys = HashSet::new();
    let file_count = delete_files.len();

    tracing::debug!(
        "read_key_based_deletion_vectors: processing {} delete files",
        file_count
    );

    for delete_file in delete_files {
        let path = std::path::Path::new(&delete_file.path);
        tracing::debug!("read_key_based_deletion_vectors: reading file {:?}", path);

        let file = std::fs::File::open(path).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to open deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        let reader = FileReader::try_new(file, None).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read batch from deletion vector: {e}"
                ))
            })?;

            // Get row_key column (first column) - should be Binary
            let row_key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "Expected BinaryArray for row_key column".to_string(),
                    )
                })?;

            for i in 0..row_key_array.len() {
                if !row_key_array.is_null(i) {
                    let key = row_key_array.value(i);
                    deleted_row_keys.insert(key.to_vec().into_boxed_slice());
                }
            }
        }
    }

    tracing::debug!(
        "Loaded {} deleted row keys from {} deletion vector files",
        deleted_row_keys.len(),
        file_count
    );

    Ok(deleted_row_keys)
}

/// Read deletion vectors from files, detecting whether each file is position-based or key-based
/// from its schema, and return separate collections for each type.
///
/// # Blocking I/O Warning
///
/// This function performs **blocking file system I/O** operations and must be called
/// from within `tokio::task::spawn_blocking`.
///
/// # Returns
///
/// A tuple of `(position_based_row_ids, key_based_row_keys_with_sequence)`.
/// The key-based map contains PK bytes -> max delete sequence number for that PK.
///
/// # Errors
///
/// Returns an error if any deletion vector file cannot be read or parsed.
#[expect(clippy::type_complexity)]
pub fn detect_deletion_type_and_read(
    delete_files: Vec<crate::metadata::DeleteFile>,
) -> datafusion_common::Result<(RoaringBitmap, HashMap<Box<[u8]>, i64>)> {
    use arrow::array::{Array, BinaryArray};
    use arrow::datatypes::DataType;
    use arrow::ipc::reader::FileReader;

    let mut deleted_row_ids = RoaringBitmap::new();
    let mut deleted_row_keys: HashMap<Box<[u8]>, i64> = HashMap::new();
    let file_count = delete_files.len();

    tracing::debug!(
        "detect_deletion_type_and_read: processing {} delete files",
        file_count
    );

    // Track overflow occurrences to log once at the end
    let mut overflow_count: u64 = 0;
    let mut first_overflow_id: Option<i64> = None;

    for delete_file in delete_files {
        let path = std::path::Path::new(&delete_file.path);
        tracing::debug!("detect_deletion_type_and_read: reading file {:?}", path);

        let file = std::fs::File::open(path).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to open deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        let reader = FileReader::try_new(file, None).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        // Detect type from schema: first column name determines type
        // "row_id" (Int64) = position-based, "row_key" (Binary) = key-based
        let schema = reader.schema();
        let first_field = schema.field(0);
        let is_key_based = matches!(first_field.data_type(), DataType::Binary);

        // Get the sequence number for this delete file (for sequence-based ordering)
        let file_sequence = delete_file.sequence_number;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read batch from deletion vector: {e}"
                ))
            })?;

            if is_key_based {
                // Key-based: extract Binary row_key column
                let row_key_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected BinaryArray for row_key column".to_string(),
                        )
                    })?;

                for i in 0..row_key_array.len() {
                    if !row_key_array.is_null(i) {
                        let key = row_key_array.value(i).to_vec().into_boxed_slice();
                        // Track max delete sequence for each PK
                        deleted_row_keys
                            .entry(key)
                            .and_modify(|seq| *seq = (*seq).max(file_sequence))
                            .or_insert(file_sequence);
                    }
                }
            } else {
                // Position-based: extract Int64 row_id column
                let row_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected Int64Array for row_id column".to_string(),
                        )
                    })?;

                let values = row_id_array.values();
                if row_id_array.null_count() == 0 {
                    for &row_id in values {
                        if let Ok(row_id_u32) = u32::try_from(row_id) {
                            deleted_row_ids.insert(row_id_u32);
                        } else {
                            if first_overflow_id.is_none() {
                                first_overflow_id = Some(row_id);
                            }
                            overflow_count += 1;
                        }
                    }
                } else {
                    for i in 0..row_id_array.len() {
                        if row_id_array.is_valid(i) {
                            let row_id = values[i];
                            if let Ok(row_id_u32) = u32::try_from(row_id) {
                                deleted_row_ids.insert(row_id_u32);
                            } else if first_overflow_id.is_none() {
                                first_overflow_id = Some(row_id);
                                overflow_count += 1;
                            } else {
                                overflow_count += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    if overflow_count > 0 {
        tracing::warn!(
            "Skipped {} row ID(s) that exceed u32::MAX (first: {}) - table should be compacted",
            overflow_count,
            first_overflow_id.unwrap_or(0)
        );
    }

    tracing::debug!(
        "Loaded {} position-based + {} key-based deleted rows from {} deletion vector files",
        deleted_row_ids.len(),
        deleted_row_keys.len(),
        file_count
    );

    Ok((deleted_row_ids, deleted_row_keys))
}
