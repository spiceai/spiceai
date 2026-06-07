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
//!
//! # Pushdown transparency
//!
//! Both filter execs forward parent predicates to the child Vortex scan via
//! `gather_filters_for_pushdown`, so the query's predicate prunes pages/files
//! *below* the deletion mask even when key-deletes are pending. This is sound
//! because a deletion filter only **removes** rows (never changes column values,
//! never adds rows): any predicate true of the filter's output is sound to
//! evaluate on the child, and `[min,max]` zone-map pruning of that predicate
//! stays valid post-delete (the true post-delete range is a subset of the stored
//! bounds). The deletion mask is applied positionally on the surviving rows and
//! is never folded into pruning. Projections and limits are deliberately not
//! pushed through (the execs need their PK columns, and being row-reducing a
//! child limit could under-produce). See the OLAP-under-load audit, finding R1,
//! and the SOTA scan (`ClickHouse` PREWHERE / Iceberg-V3 deletion-vector layering).

use crate::provider::deletion_index::{DeletionIndex, KeyDeletionIndex, Tombstone};
use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder};
use arrow_row::RowConverter;
use datafusion::config::ConfigOptions;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use std::any::Any;
use std::sync::Arc;

/// Per-partition metrics for a deletion-filter exec.
///
/// `EXPLAIN ANALYZE` shows nothing for an exec without metrics, which made the
/// merge-on-read deletion read-tax invisible (its only signal was an OS-level
/// CPU sample of `KeyDeletionIndex::get`). These surface, per partition:
/// - `output_rows` / `elapsed_compute` (`DataFusion` baseline) — the per-row probe cost.
/// - `rows_deleted` — rows removed by the filter; scanned rows can be inferred as
///   `rows_deleted + output_rows`.
///
/// The deletion-set size is shown in the exec's `DisplayAs` label (`filtered_keys=`),
/// not as a metric: it is a per-table constant, and `DataFusion` sums metric values
/// across partitions, which would report `partitions × keys`.
#[derive(Clone)]
struct DeletionFilterMetrics {
    baseline: BaselineMetrics,
    /// Rows removed because their key was an applicable (visible) deletion.
    rows_deleted: Count,
}

impl DeletionFilterMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            rows_deleted: MetricBuilder::new(metrics).counter("rows_deleted", partition),
        }
    }
}

// ============================================================================
// PK Visibility Helpers
// ============================================================================
//
// A row is visible (kept) if either:
// - Its PK has no tombstone in the deletion index, OR
// - Its tombstone's deletion pre-dates the protected-snapshot cutoff, OR
// - Insert records are honored and the PK was re-inserted with a higher
//   sequence number (upsert).
//
// The fused index answers all of this with ONE bloom-prefiltered probe per
// row; the previous two-index design paid a second bloom + HAMT walk on every
// confirmed deletion (the dominant per-row cost in changes-mode profiles).

/// Whether the visibility check honors insert records (upsert re-insertions).
///
/// The main scan path applies them. Protected-snapshot paths ignore them: a
/// re-inserted row is scanned from the snapshot's own newer files, so honoring
/// the re-insert in the base scan would resurrect the old row version.
/// (Previously expressed by passing the shared-static empty index as
/// `insert_records`; the fused index makes the mode explicit.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertRecordHandling {
    /// A deleted PK is visible again if re-inserted after the delete.
    Apply,
    /// Re-inserts never override a deletion.
    Ignore,
}

/// Shared visibility verdict for a probed tombstone.
#[inline]
fn tombstone_visible(
    tombstone: Tombstone,
    insert_record_handling: InsertRecordHandling,
    min_delete_seq_to_apply: Option<i64>,
) -> bool {
    if min_delete_seq_to_apply
        .is_some_and(|min_delete_seq| tombstone.delete_sequence <= min_delete_seq)
    {
        // Deletion pre-dates the protected snapshot's creation — skip it. The
        // full deletion index is reused here instead of being rebuilt with
        // these entries filtered out.
        return true;
    }
    match insert_record_handling {
        InsertRecordHandling::Apply => tombstone
            .insert_sequence
            .is_some_and(|insert_seq| insert_seq > tombstone.delete_sequence),
        InsertRecordHandling::Ignore => false,
    }
}

/// Check if a row with the given Int64 PK is visible (not deleted, or re-inserted after deletion).
///
/// `min_delete_seq_to_apply` is the protected-snapshot cutoff: when `Some(min)`,
/// only deletions whose `delete_seq > min` apply. This lets the protected
/// snapshot scan path share the full deletion index across snapshots
/// instead of rebuilding a filtered [`DeletionIndex`] per snapshot — the
/// `min_seq` is a single integer compared against the deletion sequence
/// number returned by the existing bloom-prefiltered probe, so it
/// adds at most one comparison per confirmed match (which the bloom
/// rejects most non-matching probes from reaching). `None` means apply every
/// deletion in the index (main scan path).
#[inline]
pub(crate) fn is_pk_visible_i64(
    pk: i64,
    tombstones: &DeletionIndex,
    insert_record_handling: InsertRecordHandling,
    min_delete_seq_to_apply: Option<i64>,
) -> bool {
    match tombstones.get(pk) {
        None => true,
        Some(tombstone) => {
            tombstone_visible(tombstone, insert_record_handling, min_delete_seq_to_apply)
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
    tombstones: &KeyDeletionIndex,
    insert_record_handling: InsertRecordHandling,
    min_delete_seq_to_apply: Option<i64>,
) -> bool {
    match tombstones.get(key) {
        None => true,
        Some(tombstone) => {
            tombstone_visible(tombstone, insert_record_handling, min_delete_seq_to_apply)
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
///
/// # Pushdown Transparency Contract
///
/// This exec is **filter-pushdown-transparent**: it forwards parent predicates
/// through to the child Vortex scan (`gather_filters_for_pushdown`) so zone-map
/// / page pruning runs *below* the deletion mask even while key-deletes are
/// pending. This is sound because the exec only **removes** rows — it never
/// changes a surviving row's column values and never adds rows — so any
/// predicate true of its output is sound to evaluate on the child, and a
/// granule the child prunes by `[min,max]` could only have contained
/// non-matching rows (deleted or not). The deletion mask composes positionally
/// and is never folded into pruning. Projections and limits are **not** pushed
/// through: the exec needs its PK columns, and because it can reduce row count a
/// child limit could under-produce. See audit finding R1.
pub struct KeyBasedDeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Fused tombstone index of PK bytes -> (delete, insert) sequence numbers.
    tombstones: Arc<KeyDeletionIndex>,
    /// Whether re-inserted PKs override their deletion (main scan) or not
    /// (protected-snapshot paths).
    insert_record_handling: InsertRecordHandling,
    /// Indices of primary key columns in the schema
    pk_column_indices: Vec<usize>,
    /// `RowConverter` for converting PK columns to bytes
    row_converter: Arc<RowConverter>,
    /// Optional minimum sequence number for protected-snapshot filtering.
    /// See [`Int64PkDeletionFilterExec::min_delete_seq_to_apply`].
    min_delete_seq_to_apply: Option<i64>,
    properties: datafusion_physical_plan::PlanProperties,
    /// Execution metrics surfaced via `EXPLAIN ANALYZE` (see [`DeletionFilterMetrics`]).
    metrics: ExecutionPlanMetricsSet,
}

impl KeyBasedDeletionFilterExec {
    /// Create a new key-based deletion filter execution plan.
    ///
    /// # Arguments
    /// * `input` - The input execution plan to filter
    /// * `tombstones` - Bloom-prefiltered fused index of deleted/upserted PK byte keys
    /// * `insert_record_handling` - Whether upsert re-insertions override deletions
    /// * `pk_column_indices` - Indices of primary key columns in the schema
    /// * `row_converter` - `RowConverter` configured for the PK columns
    /// * `min_delete_seq_to_apply` - Optional protected-snapshot cutoff
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        tombstones: Arc<KeyDeletionIndex>,
        insert_record_handling: InsertRecordHandling,
        pk_column_indices: Vec<usize>,
        row_converter: Arc<RowConverter>,
        min_delete_seq_to_apply: Option<i64>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            tombstones,
            insert_record_handling,
            pk_column_indices,
            row_converter,
            min_delete_seq_to_apply,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
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
            self.tombstones.delete_len()
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
            Arc::clone(&self.tombstones),
            self.insert_record_handling,
            self.pk_column_indices.clone(),
            Arc::clone(&self.row_converter),
            self.min_delete_seq_to_apply,
        )))
    }

    /// Forward parent filters to the child scan (filter-pushdown transparency).
    ///
    /// This exec only *removes* rows (deleted PKs); it never changes a surviving
    /// row's column values and never adds rows. Therefore any predicate that is
    /// true of this exec's output is sound to evaluate on the child *below* it:
    /// a granule/file the child prunes (because its `[min,max]` cannot match the
    /// predicate) could only have held non-matching rows, deleted or not. So
    /// pushing the query predicate through to the Vortex scan restores
    /// zone-map / page pruning while the positional deletion mask is still
    /// applied here, on top, against the (already-pruned) surviving rows.
    ///
    /// Without this override the default bars all parent filters, stranding the
    /// query predicate above the scan whenever key-deletes are pending — every
    /// prunable scan degrades to a full-column decode under CDC load (see
    /// audit finding R1). Mirrors `CayenneAccelerationExec::gather_filters_for_pushdown`.
    ///
    /// Filters are not folded into the deletion mask (the mask composes
    /// positionally, never as a min/max constraint), so pruning stays correct.
    /// `from_children` routes each parent filter to the child by column analysis;
    /// the child's output schema and this exec's `pk_column_indices` are
    /// unchanged by late-materialization pushdown. Limits are intentionally NOT
    /// forwarded (this exec can reduce row count, so a child limit could
    /// under-produce); the default `handle_child_pushdown_result` (`if_all`)
    /// marks a parent filter supported only if the child fully absorbed it.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let metrics = DeletionFilterMetrics::new(&self.metrics, partition);
        let input_stream = self.input.execute(partition, context)?;
        let tombstones = Arc::clone(&self.tombstones);
        let insert_record_handling = self.insert_record_handling;
        let pk_column_indices = self.pk_column_indices.clone();
        let row_converter = Arc::clone(&self.row_converter);
        let min_delete_seq_to_apply = self.min_delete_seq_to_apply;
        let schema = input_stream.schema();

        Ok(Box::pin(KeyBasedDeletionFilterStream {
            input: input_stream,
            tombstones,
            insert_record_handling,
            pk_column_indices,
            row_converter,
            min_delete_seq_to_apply,
            schema,
            metrics,
        }))
    }
}

/// Stream that filters out deleted rows based on primary key matching.
pub struct KeyBasedDeletionFilterStream {
    input: SendableRecordBatchStream,
    tombstones: Arc<KeyDeletionIndex>,
    insert_record_handling: InsertRecordHandling,
    pk_column_indices: Vec<usize>,
    row_converter: Arc<RowConverter>,
    /// See [`Int64PkDeletionFilterStream::min_delete_seq_to_apply`].
    min_delete_seq_to_apply: Option<i64>,
    schema: arrow_schema::SchemaRef,
    metrics: DeletionFilterMetrics,
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

                    // Time the probe + filter kernel so the merge-on-read read-tax is
                    // visible in EXPLAIN ANALYZE. Dropped on every return/continue below.
                    let _timer = self.metrics.baseline.elapsed_compute().timer();

                    // Fast path: no deletions to apply (insert-only entries
                    // never affect visibility)
                    if !self.tombstones.has_deletions() {
                        self.metrics.baseline.record_output(batch_size);
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
                            &self.tombstones,
                            self.insert_record_handling,
                            self.min_delete_seq_to_apply,
                        );
                        keep_mask.append(visible);
                        keep_count += usize::from(visible);
                    }

                    tracing::trace!(
                        "KeyBasedDeletionFilterStream: keeping {} of {} rows",
                        keep_count,
                        batch_size
                    );

                    // If all rows are deleted, skip this batch and continue to next
                    if keep_count == 0 {
                        self.metrics.rows_deleted.add(batch_size);
                        continue;
                    }

                    // If no rows are deleted, return the batch as-is (fast path)
                    if keep_count == batch_size {
                        self.metrics.baseline.record_output(batch_size);
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

                    let filtered_row_count = filtered_batch.num_rows();
                    self.metrics
                        .rows_deleted
                        .add(batch_size - filtered_row_count);
                    self.metrics.baseline.record_output(filtered_row_count);

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
///
/// Like [`KeyBasedDeletionFilterExec`], this exec is **filter-pushdown-transparent**
/// (forwards parent predicates to the child Vortex scan so zone-map pruning runs
/// below the deletion mask). Sound because deletes only remove rows; see that
/// type's "Pushdown Transparency Contract" and audit finding R1. Projections and
/// limits are not pushed through.
pub struct Int64PkDeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// Bloom-prefiltered fused index of PK -> (delete, insert) sequence numbers.
    tombstones: Arc<DeletionIndex>,
    /// Whether re-inserted PKs override their deletion (main scan) or not
    /// (protected-snapshot paths).
    insert_record_handling: InsertRecordHandling,
    /// Index of the primary key column in the schema
    pk_column_index: usize,
    /// Optional minimum sequence number — only deletions with
    /// `delete_seq > min_delete_seq_to_apply` are honoured. Used by the
    /// protected-snapshot scan path to skip deletions that pre-date the
    /// protected snapshot's creation without rebuilding the deletion
    /// index. `None` means apply every deletion in the index.
    min_delete_seq_to_apply: Option<i64>,
    properties: datafusion_physical_plan::PlanProperties,
    /// Execution metrics surfaced via `EXPLAIN ANALYZE` (see [`DeletionFilterMetrics`]).
    metrics: ExecutionPlanMetricsSet,
}

impl Int64PkDeletionFilterExec {
    /// Create a new Int64 PK-based deletion filter execution plan.
    ///
    /// # Arguments
    /// * `input` - The input execution plan to filter
    /// * `tombstones` - Bloom-prefiltered fused index of deleted/upserted PK values
    /// * `insert_record_handling` - Whether upsert re-insertions override deletions
    /// * `pk_column_index` - Index of the primary key column in the schema
    /// * `min_delete_seq_to_apply` - Optional protected-snapshot cutoff
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        tombstones: Arc<DeletionIndex>,
        insert_record_handling: InsertRecordHandling,
        pk_column_index: usize,
        min_delete_seq_to_apply: Option<i64>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
            tombstones,
            insert_record_handling,
            pk_column_index,
            min_delete_seq_to_apply,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
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
            self.tombstones.delete_len(),
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
            Arc::clone(&self.tombstones),
            self.insert_record_handling,
            self.pk_column_index,
            self.min_delete_seq_to_apply,
        )))
    }

    /// Forward parent filters to the child scan (filter-pushdown transparency).
    ///
    /// Same contract as [`KeyBasedDeletionFilterExec::gather_filters_for_pushdown`]:
    /// this exec only removes rows (deleted Int64 PKs) and never changes a
    /// surviving row's values, so the query predicate is sound to evaluate on
    /// the child below it and `[min,max]` zone-map pruning of that predicate
    /// stays valid. Forwarding it restores Vortex page/file pruning while the
    /// per-row deletion probe is still applied here on the survivors. Limits are
    /// not forwarded (row-reducing exec); `if_all` keeps a parent filter
    /// "supported" only when the child fully absorbed it.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let metrics = DeletionFilterMetrics::new(&self.metrics, partition);
        let input_stream = self.input.execute(partition, context)?;
        let tombstones = Arc::clone(&self.tombstones);
        let insert_record_handling = self.insert_record_handling;
        let pk_column_index = self.pk_column_index;
        let min_delete_seq_to_apply = self.min_delete_seq_to_apply;
        let schema = input_stream.schema();

        Ok(Box::pin(Int64PkDeletionFilterStream {
            input: input_stream,
            tombstones,
            insert_record_handling,
            pk_column_index,
            min_delete_seq_to_apply,
            schema,
            metrics,
        }))
    }
}

/// Stream that filters out deleted rows based on Int64 primary key matching.
struct Int64PkDeletionFilterStream {
    input: SendableRecordBatchStream,
    tombstones: Arc<DeletionIndex>,
    insert_record_handling: InsertRecordHandling,
    pk_column_index: usize,
    /// If `Some(min)`, only deletions with `delete_seq > min` apply. Lets
    /// protected snapshots share one tombstone index instead of
    /// each snapshot owning a per-snapshot rebuilt copy.
    min_delete_seq_to_apply: Option<i64>,
    schema: arrow_schema::SchemaRef,
    metrics: DeletionFilterMetrics,
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

                    // Time the probe + filter kernel so the merge-on-read read-tax is
                    // visible in EXPLAIN ANALYZE. Dropped on every return/continue below.
                    let _timer = self.metrics.baseline.elapsed_compute().timer();

                    // Fast path: no deletions to apply (insert-only entries
                    // never affect visibility)
                    if !self.tombstones.has_deletions() {
                        self.metrics.baseline.record_output(batch_size);
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
                            &self.tombstones,
                            self.insert_record_handling,
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
                        self.metrics.rows_deleted.add(batch_size);
                        continue;
                    }

                    // If no rows are deleted, return the batch as-is (fast path)
                    if keep_count == batch_size {
                        self.metrics.baseline.record_output(batch_size);
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

                    let filtered_row_count = filtered_batch.num_rows();
                    self.metrics
                        .rows_deleted
                        .add(batch_size - filtered_row_count);
                    self.metrics.baseline.record_output(filtered_row_count);

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

        // Probe every key, including some that aren't in either index. The
        // protected-snapshot paths ignore insert records (see
        // `InsertRecordHandling::Ignore`).
        for pk in -2..12_i64 {
            let probe_time_filter =
                is_pk_visible_i64(pk, &full_index, InsertRecordHandling::Ignore, Some(min_seq));
            let rebuilt_filter =
                is_pk_visible_i64(pk, &filtered_index, InsertRecordHandling::Ignore, None);
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

        for pk in -2..12_i64 {
            let key = pk.to_be_bytes();
            let probe_time = is_pk_visible_row_key(
                &key,
                &full_key_index,
                InsertRecordHandling::Ignore,
                Some(min_seq),
            );
            let rebuilt = is_pk_visible_row_key(
                &key,
                &filtered_key_index,
                InsertRecordHandling::Ignore,
                None,
            );
            assert_eq!(
                probe_time, rebuilt,
                "byte-key pk={pk}: probe-time min_seq filter must match a rebuilt KeyDeletionIndex"
            );
        }
    }

    /// Upsert semantics through the fused index: a deleted PK re-inserted at a
    /// higher sequence is visible on the main scan path (`Apply`) and stays
    /// hidden on protected-snapshot paths (`Ignore`).
    #[test]
    fn fused_tombstone_visibility_honors_handling_mode() {
        let deleted: HashMap<i64, i64> = HashMap::from([(1, 10), (2, 20)]);
        let inserts: HashMap<i64, i64> = HashMap::from([(1, 11), (2, 5)]);
        let index = DeletionIndex::from_maps(deleted, inserts);

        // pk=1: re-inserted after delete (11 > 10) — visible only under Apply.
        assert!(is_pk_visible_i64(
            1,
            &index,
            InsertRecordHandling::Apply,
            None
        ));
        assert!(!is_pk_visible_i64(
            1,
            &index,
            InsertRecordHandling::Ignore,
            None
        ));

        // pk=2: stale insert record (5 < 20) — deleted under both modes.
        assert!(!is_pk_visible_i64(
            2,
            &index,
            InsertRecordHandling::Apply,
            None
        ));
        assert!(!is_pk_visible_i64(
            2,
            &index,
            InsertRecordHandling::Ignore,
            None
        ));

        // pk=3: never deleted — visible under both modes.
        assert!(is_pk_visible_i64(
            3,
            &index,
            InsertRecordHandling::Apply,
            None
        ));
        assert!(is_pk_visible_i64(
            3,
            &index,
            InsertRecordHandling::Ignore,
            None
        ));

        // The min-seq cutoff overrides everything: deletions at or below the
        // protected snapshot's creation sequence are skipped.
        assert!(is_pk_visible_i64(
            2,
            &index,
            InsertRecordHandling::Ignore,
            Some(20)
        ));
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
            tombstones: deleted_row_keys,
            insert_record_handling: InsertRecordHandling::Apply,
            pk_column_indices: Vec::new(),
            row_converter,
            min_delete_seq_to_apply: None,
            schema,
            metrics: DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
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

    /// Regression for the R1 pushdown-barrier fix: `gather_filters_for_pushdown`
    /// on `Int64PkDeletionFilterExec` must forward a parent predicate to the
    /// child scan (marked `PushedDown::Yes`) rather than bar it. The default
    /// `ExecutionPlan` impl returns `all_unsupported`, which would strand the
    /// query predicate above the scan and disable zone-map pruning while
    /// key-deletes are pending. Soundness: a deletion filter only removes rows,
    /// so any predicate true of its output is sound to evaluate on the child
    /// below it.
    #[test]
    fn int64_deletion_filter_forwards_parent_filter_to_child() -> datafusion_common::Result<()> {
        use arrow::array::Int64Array;
        use arrow_schema::{Field, Schema};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::expressions::col;
        use datafusion_physical_plan::filter_pushdown::PushedDown;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
            ],
        )?;
        let child: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;

        // PK column "id" is at index 0; one deletion present.
        let tombstones = Arc::new(DeletionIndex::from_map(HashMap::from([(2_i64, 1_i64)])));
        let exec =
            Int64PkDeletionFilterExec::new(child, tombstones, InsertRecordHandling::Apply, 0, None);

        // A real boolean predicate on a child column ("val" > 0) must be
        // reported pushable (a bare column reference is not a valid predicate).
        let parent_filter: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion_physical_expr::expressions::BinaryExpr::new(
                col("val", &schema)?,
                datafusion::logical_expr::Operator::Gt,
                datafusion_physical_expr::expressions::lit(0_i64),
            ));
        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Pre,
            vec![Arc::clone(&parent_filter)],
            &ConfigOptions::default(),
        )?;

        let per_child = description.parent_filters();
        assert_eq!(per_child.len(), 1, "exec has exactly one child");
        assert_eq!(
            per_child[0].len(),
            1,
            "the single parent filter is tracked for the child"
        );
        assert!(
            matches!(per_child[0][0].discriminant, PushedDown::Yes),
            "deletion filter must forward the parent predicate to the child scan (pushdown transparency), got {:?}",
            per_child[0][0].discriminant
        );

        Ok(())
    }
}
