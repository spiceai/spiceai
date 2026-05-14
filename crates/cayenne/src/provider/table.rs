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

//! Cayenne `TableProvider` implementation.
//!
//! This module contains the main `CayenneTableProvider` struct which implements
//! `DataFusion`'s `TableProvider` trait for Cayenne tables.

use super::constants::{
    DEFAULT_DATA_FILE_ID, LISTING_TABLE_LOCK_POISONED, PROTECTED_SNAPSHOTS_LOCK_POISONED,
    STAGING_DIR_NAME, STAGING_WAL_FILENAME,
};
use super::delete::{
    CayenneDeletionSink, DeletionIdentifier, DeletionVectorWriteSpec, DeletionVectorWriter,
    FileBasedDeletionSink, Int64PkDeletionFilterExec, KeyBasedDeletionFilterExec,
};
use super::streaming::StreamingExec;
use crate::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use crate::metadata::{
    CreateTableOptions, InlinedData, InlinedDataStats, TableMetadata, TableStatistics,
};
use crate::provider::scan::{CayenneAccelerationExec, round_robin_repartition_if_needed};
use crate::provider::sink::CayenneDataSink;
use crate::provider::{Error, Result};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{Constraints, DFSchema};
use datafusion_execution::cache::TableScopedPath;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, Operator, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_table_providers::util::constraints::UpsertOptions;
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectStorePath;
use roaring::RoaringBitmap;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::task;
use vortex::dtype::arrow::FromArrowType;
use vortex_datafusion::VortexFormat;

use super::context::CayenneContext;
use super::deletion_index::{DeletionIndex, KeyDeletionIndex};
use super::deletion_strategy::{PkDeletionStrategy, PkDeletionStrategyWithCache};
use super::vortex_format::DeletionFilteringVortexFormat;
use arc_swap::ArcSwap;

/// Accumulates per-column statistics across multiple `RecordBatch`es during a write.
///
/// Builds Vortex [`StatsSet`] objects per column (min, max, null count) and tracks
/// the total row count. After the write completes, call [`to_file_statistics_blob`] to
/// produce a serialized Vortex `FileStatistics` blob for metastore persistence.
///
/// Thread-safe: guarded by `Mutex` when shared across stream tasks.
///
/// [`StatsSet`]: vortex::array::stats::StatsSet
#[derive(Debug)]
pub(crate) struct ColumnStatsAccumulator {
    /// Per-column accumulated stats as Vortex `StatsSet`
    columns: std::sync::Mutex<Vec<vortex::array::stats::StatsSet>>,
    /// Per-column "has any batch been merged yet" flag. The first batch is
    /// assigned directly (not merged) because `StatsSet::default()` represents
    /// "unknown" — and `merge_unordered(unknown, known) == unknown`, which
    /// would silently drop the first batch's stats.
    columns_seeded: std::sync::Mutex<Vec<bool>>,
    /// Column dtypes (Vortex types, derived from Arrow schema)
    dtypes: Vec<vortex::dtype::DType>,
    /// Total accumulated row count across all batches
    row_count: std::sync::atomic::AtomicI64,
    /// Arrow schema for serialization
    schema: arrow_schema::Schema,
}

impl ColumnStatsAccumulator {
    /// Create a new accumulator for the given schema.
    pub(crate) fn new(schema: &arrow_schema::Schema) -> Self {
        let num_cols = schema.fields().len();
        let columns = vec![vortex::array::stats::StatsSet::default(); num_cols];
        let dtypes: Vec<vortex::dtype::DType> = schema
            .fields()
            .iter()
            .map(|f| {
                vortex::dtype::DType::from_arrow((
                    f.data_type(),
                    if f.is_nullable() {
                        vortex::dtype::Nullability::Nullable
                    } else {
                        vortex::dtype::Nullability::NonNullable
                    },
                ))
            })
            .collect();
        Self {
            columns: std::sync::Mutex::new(columns),
            columns_seeded: std::sync::Mutex::new(vec![false; num_cols]),
            dtypes,
            row_count: std::sync::atomic::AtomicI64::new(0),
            schema: schema.clone(),
        }
    }

    /// Update accumulated stats from a `RecordBatch`.
    pub(crate) fn update(&self, batch: &RecordBatch) {
        let Ok(mut cols) = self.columns.lock() else {
            tracing::warn!("ColumnStatsAccumulator: mutex poisoned in update(), skipping");
            return;
        };
        let Ok(mut seeded) = self.columns_seeded.lock() else {
            tracing::warn!("ColumnStatsAccumulator: seeded-mutex poisoned in update(), skipping");
            return;
        };

        let num_rows = batch.num_rows();
        // Use saturating addition at i64::MAX so overflow on extremely long-lived
        // accumulators surfaces as a clamped row count rather than wrapping to
        // a negative value that would get persisted as a bogus `num_rows`.
        let delta = i64::try_from(num_rows).unwrap_or(i64::MAX);
        let _ = self.row_count.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_add(delta)),
        );

        for (i, col) in batch.columns().iter().enumerate() {
            if i >= cols.len() || i >= self.dtypes.len() || i >= seeded.len() {
                continue;
            }

            // Build a StatsSet for this batch's column
            let batch_stats =
                crate::stats::column_stats_to_stats_set(&Self::compute_column_stats(col));

            // For the first batch, seed directly. `StatsSet::default()` is
            // treated by Vortex as "unknown" — and `merge_unordered(unknown,
            // known) == unknown`, which would otherwise silently drop the
            // first batch's stats. On subsequent batches, merge using the
            // commutative unordered merge so statistics stay correct
            // regardless of the order batches arrive in.
            if seeded[i] {
                let existing = std::mem::take(&mut cols[i]);
                cols[i] = existing.merge_unordered(&batch_stats, &self.dtypes[i]);
            } else {
                cols[i] = batch_stats;
                seeded[i] = true;
            }
        }
    }

    /// Compute `DataFusion` `ColumnStatistics` from a single Arrow column.
    fn compute_column_stats(col: &dyn arrow::array::Array) -> datafusion_common::ColumnStatistics {
        use datafusion_common::stats::Precision;

        let null_count = Precision::Exact(col.null_count());

        if col.is_empty() || col.null_count() == col.len() {
            return datafusion_common::ColumnStatistics {
                null_count,
                min_value: Precision::Absent,
                max_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            };
        }

        // O(n) linear scan to find min/max using `ScalarValue` comparison.
        // NaN values are skipped entirely so stats remain deterministic.
        let mut batch_min: Option<datafusion_common::ScalarValue> = None;
        let mut batch_max: Option<datafusion_common::ScalarValue> = None;

        for row_idx in 0..col.len() {
            if col.is_null(row_idx) {
                continue;
            }
            let Ok(value) = datafusion_common::ScalarValue::try_from_array(col, row_idx) else {
                continue;
            };

            // Skip NaN: partial_cmp(NaN, x) always returns None
            if value.partial_cmp(&value) != Some(std::cmp::Ordering::Equal) {
                continue;
            }

            batch_min = Some(match batch_min {
                None => value.clone(),
                Some(existing) => {
                    if value.partial_cmp(&existing) == Some(std::cmp::Ordering::Less) {
                        value.clone()
                    } else {
                        existing
                    }
                }
            });
            batch_max = Some(match batch_max {
                None => value,
                Some(existing) => {
                    if value.partial_cmp(&existing) == Some(std::cmp::Ordering::Greater) {
                        value
                    } else {
                        existing
                    }
                }
            });
        }

        datafusion_common::ColumnStatistics {
            null_count,
            min_value: batch_min.map_or(Precision::Absent, Precision::Exact),
            max_value: batch_max.map_or(Precision::Absent, Precision::Exact),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }
    }

    /// Get the total accumulated row count.
    pub(crate) fn row_count(&self) -> i64 {
        self.row_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Convert accumulated stats to a serialized Vortex `FileStatistics` blob.
    ///
    /// Returns `None` if no rows were accumulated or serialization fails.
    pub(crate) fn to_file_statistics_blob(&self) -> Option<Vec<u8>> {
        let (blob, _row_count) = self.to_file_statistics_blob_with_row_count()?;
        Some(blob)
    }

    pub(crate) fn to_file_statistics_blob_with_row_count(&self) -> Option<(Vec<u8>, i64)> {
        let row_count = self.row_count();
        if row_count == 0 {
            return None;
        }
        let Ok(cols) = self.columns.lock() else {
            tracing::warn!(
                "ColumnStatsAccumulator: mutex poisoned in to_file_statistics_blob(), returning None"
            );
            return None;
        };

        let file_stats = crate::stats::build_file_statistics(cols.clone(), &self.schema);
        match crate::stats::serialize_file_statistics(&file_stats) {
            Ok(bytes) => Some((bytes, row_count)),
            Err(e) => {
                tracing::warn!("Failed to serialize file statistics: {e}");
                None
            }
        }
    }

    fn merged_file_statistics_blob(&self, existing_blob: &[u8]) -> Option<Vec<u8>> {
        let Ok(cols) = self.columns.lock() else {
            tracing::warn!(
                "ColumnStatsAccumulator: mutex poisoned in merged_file_statistics_blob(), returning None"
            );
            return None;
        };

        crate::stats::merge_serialized_stats(existing_blob, &cols, &self.dtypes, &self.schema)
    }
}

/// Maximum number of rows to inline in the metastore instead of writing a Vortex file.
pub(crate) const INLINE_MAX_ROWS: usize = 1024;

/// Maximum serialized IPC size (bytes) to inline in the metastore.
const INLINE_MAX_BYTES: usize = 1_048_576; // 1 MB

/// Maximum rows to keep in the inline level-0 memtable before flushing to Vortex.
pub(crate) const INLINE_MEMTABLE_MAX_ROWS: i64 = 10_000;

/// Maximum inline level-0 entries before flushing to Vortex.
pub(crate) const INLINE_MEMTABLE_MAX_SEGMENTS: i64 = 64;

/// Maximum serialized IPC bytes to keep inline before flushing to Vortex.
pub(crate) const INLINE_MEMTABLE_MAX_BYTES: i64 = 8 * 1_048_576;

/// Maximum in-memory byte budget while buffering the inline fast-path stream.
///
/// `INLINE_MAX_ROWS` alone does not bound memory usage — a pathological batch
/// with few rows but very large string / binary values can still consume a lot
/// of RAM. Once the cumulative array memory size of buffered batches exceeds
/// this budget the fast-path bails out and falls through to the normal Vortex
/// write path, where the stream is consumed incrementally. Held slightly above
/// `INLINE_MAX_BYTES` (the serialized IPC cap) to account for in-memory Arrow
/// overhead vs. the compact IPC representation.
pub(crate) const INLINE_MAX_BUFFER_BYTES: usize = 4 * 1_048_576; // 4 MB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InlineMemtablePressure {
    Rows,
    Segments,
    IpcBytes,
}

impl InlineMemtablePressure {
    #[must_use]
    fn as_str(self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::Segments => "segments",
            Self::IpcBytes => "ipc_bytes",
        }
    }
}

#[must_use]
pub(crate) fn inline_memtable_pressure(stats: InlinedDataStats) -> Option<InlineMemtablePressure> {
    if stats.record_count >= INLINE_MEMTABLE_MAX_ROWS {
        return Some(InlineMemtablePressure::Rows);
    }
    if stats.entry_count > INLINE_MEMTABLE_MAX_SEGMENTS {
        return Some(InlineMemtablePressure::Segments);
    }
    if stats.ipc_bytes >= INLINE_MEMTABLE_MAX_BYTES {
        return Some(InlineMemtablePressure::IpcBytes);
    }
    None
}

/// Serialize one or more `RecordBatch`es to Arrow IPC stream bytes.
fn serialize_batches_to_ipc(
    batches: &[RecordBatch],
) -> std::result::Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    if let Some(first) = batches.first() {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, first.schema_ref())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserialize Arrow IPC bytes back to a `RecordBatch`.
fn deserialize_ipc_to_batch(
    ipc_bytes: &[u8],
) -> std::result::Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)?;
    reader.collect()
}

fn deserialize_delete_keys_from_ipc(
    ipc_bytes: &[u8],
) -> std::result::Result<Vec<Box<[u8]>>, arrow::error::ArrowError> {
    let batches = deserialize_ipc_to_batch(ipc_bytes)?;
    let mut row_keys = Vec::new();

    for batch in batches {
        let Some(row_key_array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
        else {
            return Err(arrow::error::ArrowError::CastError(
                "Expected BinaryArray for inlined delete row_key column".to_string(),
            ));
        };

        row_keys.reserve(row_key_array.len());
        for row_index in 0..row_key_array.len() {
            if !row_key_array.is_null(row_index) {
                row_keys.push(row_key_array.value(row_index).to_vec().into_boxed_slice());
            }
        }
    }

    Ok(row_keys)
}

/// Extension trait to extract `UpsertOptions` from `OnConflict`.
///
/// The upstream `OnConflict` enum only contains `ColumnReference`, but our on-conflict
/// logic requires `UpsertOptions`. This trait provides a compatibility shim.
trait OnConflictExt {
    /// Returns `UpsertOptions` for this `OnConflict` variant.
    /// Currently returns default options; future versions may store options in `OnConflict`.
    fn get_upsert_options(&self) -> UpsertOptions;
}

impl OnConflictExt for OnConflict {
    fn get_upsert_options(&self) -> UpsertOptions {
        UpsertOptions::default()
    }
}

/// Cayenne table provider that reads from Vortex virtual files.
///
/// This provider manages a table composed of multiple "virtual files", where each file
/// is a Vortex `ListingTable` at its own directory.
///
/// Currently, the implementation uses a single `ListingTable` that scans the entire table
/// directory. In a future optimization, this could be enhanced to manage multiple
/// `ListingTables` (one per virtual file) and union their results for better control
/// over file-level operations.
pub struct CayenneTableProvider {
    /// Table metadata from the catalog
    table_metadata: TableMetadata,
    /// Reference to the metadata catalog for file operations
    catalog: Arc<dyn MetadataCatalog>,
    /// Underlying Vortex `ListingTable` that scans all virtual files in the table directory.
    /// Note: Each `DataFile` in the catalog represents a subdirectory (virtual file),
    /// but this `ListingTable` currently scans all of them together.
    /// Wrapped in `RwLock` to allow updating the listing table on overwrite operations.
    /// Uses `std::sync::RwLock` instead of `tokio::sync::RwLock` because we need
    /// synchronous access in `TableProvider` trait methods (`supports_filters_pushdown`
    /// and `statistics`), and the lock is held for very short durations (just Arc clones).
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    /// Optional retention filters that should be applied immediately after writes.
    retention_filters: Vec<Expr>,
    /// Optional builder to construct time-based retention filter.
    ///
    /// Used for period-based retention (e.g. `retention_period: 30d`).
    time_retention_filter_builder: Option<super::retention::TimeRetentionFilterBuilder>,
    /// Context containing Vortex format with caches and configuration.
    /// If the same context is reused across multiple instances, all internal operations
    /// share the same footer and segment caches, enabling shared memory management.
    context: Arc<CayenneContext>,
    /// Strategy for primary key-based deletion filtering.
    /// Contains the deletion caches specific to each strategy variant.
    pk_deletion_strategy: PkDeletionStrategyWithCache,
    /// `RowConverter` for converting primary key columns to byte representation.
    /// Only set for tables with composite or non-integer primary keys.
    pk_row_converter: Option<Arc<RowConverter>>,
    /// Indices of primary key columns in the table schema.
    pk_column_indices: Vec<usize>,
    /// Write lock to serialize insert operations and prevent concurrent write races.
    /// This ensures that:
    /// - Only one `insert()` runs at a time per table
    /// - Parallel chunk writes complete before listing table refresh
    /// - Retention filters are applied atomically after writes
    /// - Statistics are consistent and up-to-date
    ///
    /// Uses `tokio::sync::Mutex` because the lock is held across `.await` points during insert operations.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Optional object store configuration for remote storage (e.g., S3 Express One Zone).
    /// When set, this object store is registered with `SessionContext` for data file operations.
    object_store_config: Option<crate::metadata::ObjectStoreConfig>,
    /// Current snapshot ID, updated after compaction operations.
    ///
    /// This is separate from `table_metadata.current_snapshot_id` because compaction
    /// creates a new snapshot but we don't want to modify the original `TableMetadata`.
    /// Uses `RwLock` for concurrent reads during normal operations with occasional
    /// writes on compaction. The lock is held briefly for string operations.
    current_snapshot_id: Arc<RwLock<String>>,
    /// Protected snapshot IDs that should skip deletion filtering.
    ///
    /// When data is inserted while pending deletions exist, the new data is written
    /// to a new snapshot that is "protected" - deletions that existed at the time
    /// of insert should not apply to this snapshot's data.
    ///
    /// Maps `snapshot_id` -> `minimum_sequence` (all deletes with seq <= `min_seq` don't apply).
    /// At scan time, data from these snapshots is scanned without deletion filtering.
    protected_snapshots: Arc<RwLock<HashMap<String, i64>>>,
    /// Cached inlined row count. Maintained while the process is running so
    /// append-heavy inline CDC writes don't query the metastore after every
    /// burst just to decide whether to checkpoint.
    inlined_row_count: Arc<AtomicI64>,
}

/// Builder for constructing a `CayenneTableProvider` with optional configuration.
///
/// Use this builder to configure optional parameters before opening an existing table
/// or creating a new one.
///
/// # Example
///
/// ```ignore
/// // Open an existing table
/// let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
///     .with_retention_filters(filters)
///     .with_object_store(config)
///     .open("my_table").await?;
///
/// // Create a new table
/// let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
///     .with_retention_filters(filters)
///     .create(options).await?;
/// ```
#[derive(Clone)]
pub struct CayenneTableProviderBuilder {
    catalog: Arc<dyn MetadataCatalog>,
    runtime_env: Arc<RuntimeEnv>,
    retention_filters: Vec<Expr>,
    time_retention_filter_builder: Option<super::retention::TimeRetentionFilterBuilder>,
    object_store_config: Option<crate::metadata::ObjectStoreConfig>,
    context: Option<Arc<CayenneContext>>,
}

impl CayenneTableProviderBuilder {
    /// Create a new builder with the required catalog and shared `RuntimeEnv`.
    #[must_use]
    pub fn new(catalog: Arc<dyn MetadataCatalog>, runtime_env: Arc<RuntimeEnv>) -> Self {
        Self {
            catalog,
            runtime_env,
            retention_filters: Vec::new(),
            time_retention_filter_builder: None,
            object_store_config: None,
            context: None,
        }
    }

    /// Set retention filters that will be applied after writes.
    ///
    /// These filters cause automatic deletion of rows matching the filter criteria
    /// after each write operation.
    #[must_use]
    pub fn with_retention_filters(mut self, filters: Vec<Expr>) -> Self {
        self.retention_filters = filters;
        self
    }

    /// Set a time-based retention filter builder.
    ///
    /// When set, this builder is used to apply time-based retention filter at scan time.
    #[must_use]
    pub fn with_time_retention_filter_builder(
        mut self,
        builder: super::retention::TimeRetentionFilterBuilder,
    ) -> Self {
        self.time_retention_filter_builder = Some(builder);
        self
    }

    /// Set the object store configuration for remote storage.
    ///
    /// Used for S3 Express One Zone storage where data files are stored remotely
    /// while metadata remains on local disk.
    #[must_use]
    pub fn with_object_store(mut self, config: crate::metadata::ObjectStoreConfig) -> Self {
        self.object_store_config = Some(config);
        self
    }

    /// Set a shared [`CayenneContext`] for this table provider.
    ///
    /// Use this to share a single context (with caches) across multiple table providers
    /// This avoids creating separate caches per partition
    #[must_use]
    pub fn with_context(mut self, context: Arc<CayenneContext>) -> Self {
        self.context = Some(context);
        self
    }

    /// Open an existing table by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn open(self, table_name: &str) -> Result<CayenneTableProvider> {
        CayenneTableProvider::new_internal(
            table_name,
            self.catalog,
            self.retention_filters,
            self.time_retention_filter_builder,
            self.object_store_config,
            self.runtime_env,
            self.context,
        )
        .await
        .map_err(Into::into)
    }

    /// Create a new table with the given options.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create(self, options: CreateTableOptions) -> CatalogResult<CayenneTableProvider> {
        let table_name = options.table_name.clone();
        let _table_id = self.catalog.create_table(options).await?;

        CayenneTableProvider::new_internal(
            &table_name,
            self.catalog,
            self.retention_filters,
            self.time_retention_filter_builder,
            self.object_store_config,
            self.runtime_env,
            self.context,
        )
        .await
    }
}

#[derive(Debug, Clone, Copy)]
struct RowLocation {
    source: RowSource,
    data_file_id: i64,
    row_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowSource {
    File,
    Inlined,
}

#[derive(Default)]
struct InlinedDeletionMaps {
    int64_pk: HashMap<i64, i64>,
    row_keys: HashMap<Box<[u8]>, i64>,
}

#[derive(Default)]
struct ExtractedPrimaryKeys {
    int64_pk: Vec<i64>,
    row_keys: Vec<Box<[u8]>>,
}

#[derive(Default)]
struct InlinedDataRewrite {
    updated_data: Vec<InlinedData>,
    deleted_inlined_ids: Vec<String>,
    removed_rows: usize,
}

impl InlinedDataRewrite {
    #[must_use]
    fn is_empty(&self) -> bool {
        self.updated_data.is_empty() && self.deleted_inlined_ids.is_empty()
    }
}

struct InlineAwareDeletionSink {
    table: CayenneTableProvider,
    file_sink: CayenneDeletionSink,
    filters: Vec<Expr>,
}

#[async_trait]
impl DeletionSink for InlineAwareDeletionSink {
    async fn delete_from(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let _write_guard = self.table.write_lock.lock().await;

        let inlined_deleted = self
            .table
            .delete_inlined_rows_matching_filters(&self.filters)
            .await?;
        let file_deleted = self.file_sink.delete_from().await?;

        inlined_deleted.checked_add(file_deleted).ok_or_else(|| {
            Box::new(datafusion_common::DataFusionError::Execution(
                "Deleted row count overflowed u64".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })
    }
}

struct BatchValidationResult {
    filtered_batch: Option<RecordBatch>,
    delete_specs: Vec<(i64, Vec<i64>)>,
    kept_keys: HashSet<OwnedRow>,
    /// File-backed Int64 PK values being deleted (for `Int64Pk` strategy).
    deleted_pk_i64: Vec<i64>,
    /// File-backed row key bytes being deleted (for `RowConverterBased` strategy).
    deleted_row_keys: Vec<Box<[u8]>>,
    /// Inlined Int64 PK values being deleted.
    deleted_inlined_pk_i64: Vec<i64>,
    /// Inlined row key bytes being deleted.
    deleted_inlined_row_keys: Vec<Box<[u8]>>,
}

pub(crate) struct PreparedInsertStream {
    pub(crate) stream: SendableRecordBatchStream,
    pub(crate) on_conflict_deletions: OnConflictDeletions,
}

#[derive(Default)]
pub(crate) struct OnConflictDeletions {
    pub(crate) delete_specs: HashMap<i64, Vec<i64>>,
    /// Deleted file-backed Int64 PK values (for `Int64Pk` strategy).
    pub(crate) deleted_pk_i64: Vec<i64>,
    /// Deleted file-backed row keys (for `RowConverterBased` strategy).
    pub(crate) deleted_row_keys: Vec<Box<[u8]>>,
    /// Deleted inlined Int64 PK values.
    pub(crate) deleted_inlined_pk_i64: Vec<i64>,
    /// Deleted inlined row keys.
    pub(crate) deleted_inlined_row_keys: Vec<Box<[u8]>>,
}

impl OnConflictDeletions {
    #[must_use]
    pub(crate) fn has_file_deletions(&self) -> bool {
        !self.delete_specs.is_empty()
    }

    #[must_use]
    pub(crate) fn has_inlined_deletions(&self) -> bool {
        !self.deleted_inlined_pk_i64.is_empty() || !self.deleted_inlined_row_keys.is_empty()
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        !self.has_file_deletions() && !self.has_inlined_deletions()
    }

    #[must_use]
    pub(crate) fn file_delete_specs_count(&self) -> usize {
        self.delete_specs.len()
    }

    #[must_use]
    pub(crate) fn deleted_key_count(&self) -> usize {
        self.deleted_pk_i64.len()
            + self.deleted_row_keys.len()
            + self.deleted_inlined_pk_i64.len()
            + self.deleted_inlined_row_keys.len()
    }
}

/// Result of on-conflict validation containing deleted PK information.
struct OnConflictValidationResult {
    filtered_batches: Vec<RecordBatch>,
    on_conflict_deletions: OnConflictDeletions,
}

struct OnConflictContext<'a> {
    pk_indices: &'a [usize],
    converter: &'a RowConverter,
    on_conflict: &'a OnConflict,
    upsert_options: &'a UpsertOptions,
    existing_keys: &'a mut HashMap<OwnedRow, RowLocation>,
    incoming_keys: &'a HashSet<OwnedRow>,
}

impl std::fmt::Debug for CayenneTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneTableProvider")
            .field("table_metadata", &self.table_metadata)
            .finish_non_exhaustive()
    }
}

impl CayenneTableProvider {
    /// Returns the name of this table.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_metadata.table_name
    }

    /// Returns the base path for this table's data.
    #[must_use]
    pub(crate) fn table_path(&self) -> &str {
        &self.table_metadata.path
    }

    /// Returns the table ID from the catalog.
    #[must_use]
    pub(crate) fn table_id(&self) -> &str {
        &self.table_metadata.table_id
    }

    /// Returns a reference to the write lock for serializing insert operations.
    #[must_use]
    pub(crate) fn write_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.write_lock
    }

    #[must_use]
    pub(crate) fn write_lock_arc(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.write_lock)
    }

    #[must_use]
    pub(crate) fn target_file_size_bytes(&self) -> usize {
        self.context.target_file_size_bytes()
    }

    /// Returns a cheap clone that shares the underlying table state for write operations.
    #[must_use]
    pub fn clone_for_write_operations(&self) -> Self {
        self.clone_for_write()
    }

    /// Returns whether retention filters are configured for this table.
    #[must_use]
    pub(crate) fn has_retention_filters(&self) -> bool {
        !self.retention_filters.is_empty() || self.time_retention_filter_builder.is_some()
    }

    /// Returns the path to a snapshot directory for this table.
    #[must_use]
    pub(crate) fn snapshot_dir_path_for(&self, snapshot_id: &str) -> std::path::PathBuf {
        Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        )
    }

    /// Atomically commit an overwrite operation to the catalog.
    ///
    /// This clears any existing delete files since overwrite replaces all data.
    pub(crate) async fn commit_overwrite(&self, new_snapshot_id: &str) -> CatalogResult<()> {
        self.catalog
            .commit_compaction(&self.table_metadata.table_id, new_snapshot_id)
            .await
    }

    /// Update the listing table to point to a new snapshot directory.
    ///
    /// This ensures subsequent queries in the same context will read from the new data.
    pub(crate) fn update_listing_table_for_snapshot(&self, new_snapshot_id: &str) -> Result<()> {
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            new_snapshot_id,
        );

        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        let mut listing_table_guard =
            self.listing_table
                .write()
                .map_err(|_| Error::LockPoisoned {
                    table: self.table_metadata.table_name.clone(),
                    lock: LISTING_TABLE_LOCK_POISONED,
                })?;
        *listing_table_guard = new_listing_table;
        Ok(())
    }

    /// Trigger cleanup of old snapshot directories in the background.
    ///
    /// This is a non-blocking operation that logs warnings on failure but doesn't
    /// propagate errors, as cleanup failures shouldn't fail the write operation.
    ///
    /// Protected snapshots (those containing data written after deletions) are preserved
    /// alongside the current snapshot to prevent data loss for queries that reference them.
    pub(crate) async fn trigger_old_snapshot_cleanup(&self, current_snapshot: &str) {
        // Collect protected snapshot IDs to preserve during cleanup
        let protected_snapshot_ids: HashSet<String> = {
            let Ok(guard) = self.protected_snapshots.read() else {
                tracing::warn!("Failed to read protected snapshots for cleanup");
                return;
            };
            guard.keys().cloned().collect()
        };

        if self.table_metadata.path.starts_with("s3://") {
            if let Err(err) = self
                .cleanup_old_snapshots_s3(current_snapshot, &protected_snapshot_ids)
                .await
            {
                tracing::warn!(
                    "Failed to cleanup old S3 snapshots for table {}: {err}",
                    &self.table_metadata.table_id
                );
            }
        } else {
            let table_path = self.table_metadata.path.clone();
            let table_id = self.table_metadata.table_id.clone();
            let current_snapshot = current_snapshot.to_string();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = Self::cleanup_old_snapshots_blocking(
                    &table_path,
                    &table_id,
                    &current_snapshot,
                    &protected_snapshot_ids,
                ) {
                    tracing::warn!(
                        "Failed to cleanup old snapshots for table {}: {e}",
                        table_id
                    );
                }
            });
        }
    }

    /// Construct the path to a snapshot directory.
    ///
    /// Directory structure: `[table_path]/[table_id]/[snapshot_id]/`
    ///
    /// # Arguments
    ///
    /// * `table_path` - The base path for the table
    /// * `table_id` - The unique identifier for the table
    /// * `snapshot_id` - The snapshot identifier
    pub(super) fn snapshot_dir_path(
        table_path: &str,
        table_id: &str,
        snapshot_id: &str,
    ) -> std::path::PathBuf {
        std::path::PathBuf::from(table_path)
            .join(table_id)
            .join(snapshot_id)
    }

    /// Convert a directory path to a `DataFusion`-compatible URL string with trailing slash.
    ///
    /// `DataFusion` requires directory URLs to end with a trailing slash.
    fn dir_to_url_string(dir: &std::path::Path) -> String {
        let mut url_str = dir.to_string_lossy().to_string();
        if !url_str.ends_with('/') {
            url_str.push('/');
        }
        url_str
    }

    fn register_object_store_if_needed(
        runtime_env: &Arc<RuntimeEnv>,
        config: &crate::metadata::ObjectStoreConfig,
    ) {
        // Use the object store registry to check if already registered
        let already_registered = runtime_env
            .object_store_registry
            .get_store(&config.url)
            .map(|existing| Arc::ptr_eq(&existing, &config.store))
            .unwrap_or(false);

        if !already_registered {
            runtime_env.register_object_store(&config.url, Arc::clone(&config.store));
            tracing::debug!("Registered object store for {}", config.url.as_str());
        }
    }

    pub(super) fn require_object_store(&self) -> Result<&crate::metadata::ObjectStoreConfig> {
        self.object_store_config
            .as_ref()
            .ok_or_else(|| Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: "S3 storage requires an object_store_config".to_string(),
            })
    }

    pub(super) fn snapshot_object_store_prefix(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<ObjectStorePath>> {
        if !self.table_metadata.path.starts_with("s3://") {
            return Ok(None);
        }

        let snapshot_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        );

        let url = url::Url::parse(&snapshot_url).map_err(|source| Error::UrlParse {
            url: snapshot_url.clone(),
            source,
        })?;

        let host = url.host_str().unwrap_or_default();
        let config = self.require_object_store()?;
        let config_host = config.url.host_str().unwrap_or_default();

        if !config_host.is_empty() && !host.is_empty() && config_host != host {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!(
                    "Snapshot host {host} does not match configured object store host {config_host}"
                ),
            });
        }

        let path = url.path().trim_start_matches('/');
        Ok(Some(ObjectStorePath::from(path)))
    }

    async fn delete_prefix_with_object_store(&self, prefix: &ObjectStorePath) -> Result<()> {
        let config = self.require_object_store()?;
        let objects: Vec<_> = config
            .store
            .list(Some(prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list objects for snapshot cleanup",
                table: self.table_metadata.table_name.clone(),
                source: e,
            })?;

        for meta in objects {
            config
                .store
                .delete(&meta.location)
                .await
                .map_err(|e| Error::ObjectStore {
                    operation: "delete object from snapshot cleanup",
                    table: self.table_metadata.table_name.clone(),
                    source: e,
                })?;
        }

        Ok(())
    }

    async fn cleanup_old_snapshots_s3(
        &self,
        current_snapshot: &str,
        protected_snapshot_ids: &HashSet<String>,
    ) -> Result<()> {
        let config = self.require_object_store()?;

        let base_url =
            url::Url::parse(&self.table_metadata.path).map_err(|source| Error::UrlParse {
                url: self.table_metadata.path.clone(),
                source,
            })?;

        let mut base_prefix = base_url.path().trim_start_matches('/').to_string();
        if !base_prefix.ends_with('/') {
            base_prefix.push('/');
        }

        let prefix =
            ObjectStorePath::from(format!("{base_prefix}{}/", self.table_metadata.table_id));

        let list_result = config
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list snapshots for cleanup",
                table: self.table_metadata.table_name.clone(),
                source: e,
            })?;

        for common_prefix in list_result.common_prefixes {
            if let Some(snapshot_id) = common_prefix.parts().last() {
                let snapshot_id_str = snapshot_id.as_ref();
                // Skip current snapshot, protected snapshots, and the staging directory
                if snapshot_id_str == current_snapshot
                    || protected_snapshot_ids.contains(snapshot_id_str)
                    || snapshot_id_str == STAGING_DIR_NAME
                {
                    tracing::debug!(
                        "Keeping snapshot: {snapshot_id_str} (current, protected, or staging)"
                    );
                    continue;
                }
                self.delete_prefix_with_object_store(&common_prefix).await?;
            }
        }

        Ok(())
    }

    /// Create a new `ListingTable` for a snapshot directory.
    ///
    /// # Arguments
    ///
    /// * `snapshot_dir_url` - URL string for the snapshot directory (local path or S3 URL)
    /// * `schema` - Arrow schema for the table
    /// * `vortex_format` - Vortex format
    /// * `strategy` - The deletion strategy for this table (contains embedded caches)
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be created.
    fn create_listing_table(
        snapshot_dir_url: &str,
        schema: SchemaRef,
        vortex_format: &Arc<VortexFormat>,
        strategy: &PkDeletionStrategyWithCache,
    ) -> Result<Arc<ListingTable>> {
        Self::create_listing_table_with_config(
            snapshot_dir_url,
            schema,
            vortex_format,
            strategy,
            &SessionConfig::default(),
        )
    }

    fn create_listing_table_with_config(
        snapshot_dir_url: &str,
        schema: SchemaRef,
        vortex_format: &Arc<VortexFormat>,
        strategy: &PkDeletionStrategyWithCache,
        session_config: &SessionConfig,
    ) -> Result<Arc<ListingTable>> {
        let table_url = ListingTableUrl::parse(snapshot_dir_url)?;

        let listing_options = Self::create_listing_options(vortex_format, strategy, session_config);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let listing_table = ListingTable::try_new(config)?;

        Ok(Arc::new(listing_table))
    }

    // Create listing options for Vortex format.
    ///
    /// Only wraps the `VortexFormat` with `DeletionFilteringVortexFormat` for
    /// `PositionBased` strategy. PK-based strategies (`Int64Pk`, `RowConverterBased`)
    /// filter at the `ExecutionPlan` level, not during file reading.
    fn create_listing_options(
        vortex_format: &Arc<VortexFormat>,
        strategy: &PkDeletionStrategyWithCache,
        session_config: &SessionConfig,
    ) -> ListingOptions {
        let file_format: Arc<dyn FileFormat> = match strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => Arc::new(DeletionFilteringVortexFormat::new(
                Arc::clone(vortex_format),
                Arc::clone(cached_deleted_row_ids),
            )),
            PkDeletionStrategyWithCache::Int64Pk { .. }
            | PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                Arc::clone(vortex_format) as Arc<dyn FileFormat>
            }
        };
        ListingOptions::new(file_format).with_session_config_options(session_config)
    }

    /// Construct the snapshot directory URL string.
    ///
    /// For local paths, returns a file:// URL or path string.
    /// For S3 paths, returns the S3 URL with proper path components.
    ///
    /// # Arguments
    ///
    /// * `table_path` - The base path for the table (local path or S3 URL)
    /// * `table_id` - The unique identifier for the table
    /// * `snapshot_id` - The snapshot identifier
    fn snapshot_dir_url(table_path: &str, table_id: &str, snapshot_id: &str) -> String {
        if table_path.starts_with("s3://") {
            // S3 URL: join path components with /
            let base = table_path.trim_end_matches('/');
            format!("{base}/{table_id}/{snapshot_id}/")
        } else {
            // Local path: use PathBuf and convert to URL string
            let path = Self::snapshot_dir_path(table_path, table_id, snapshot_id);
            Self::dir_to_url_string(&path)
        }
    }

    /// Ensure a snapshot directory exists, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub(crate) async fn ensure_snapshot_dir_exists(
        snapshot_dir: &std::path::Path,
    ) -> std::io::Result<()> {
        if !snapshot_dir.exists() {
            tokio::fs::create_dir_all(snapshot_dir).await?;
        }
        Ok(())
    }

    /// Clear the staging directory, removing any leftover files.
    ///
    /// Called at the start of each staged append to guarantee a clean slate.
    /// If the directory does not exist it is created.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be cleaned or created.
    pub(crate) async fn clear_staging_dir(&self) -> Result<()> {
        if self.table_metadata.path.starts_with("s3://") {
            // S3: delete all objects under the staging prefix
            if let Some(prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? {
                self.delete_prefix_with_object_store(&prefix).await?;
            }
        } else {
            // Local FS: remove and recreate the directory
            let staging_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                STAGING_DIR_NAME,
            );
            match tokio::fs::remove_dir_all(&staging_dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            tokio::fs::create_dir_all(&staging_dir).await?;
        }
        Ok(())
    }

    /// Move all files from the staging directory into the current snapshot directory.
    ///
    /// On local filesystems `rename()` is used, which is atomic on the same filesystem
    /// (staging and snapshot dirs share `{table_path}/{table_id}/`).
    ///
    /// On S3, files are copied to the current snapshot prefix first, then the staging
    /// originals are deleted (copy-all-then-delete-all ordering to avoid data loss if
    /// the operation is interrupted).
    ///
    /// # Errors
    ///
    /// Returns an error if any file move/copy fails.
    pub(crate) async fn move_files_to_current_snapshot(&self) -> Result<()> {
        let current_snapshot = self.get_current_snapshot_id()?;

        if self.table_metadata.path.starts_with("s3://") {
            self.move_staging_files_s3(&current_snapshot).await
        } else {
            self.move_staging_files_local(&current_snapshot).await
        }
    }

    /// Move staging files to the current snapshot on local filesystem.
    async fn move_staging_files_local(&self, current_snapshot: &str) -> Result<()> {
        let staging_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            STAGING_DIR_NAME,
        );
        let target_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            current_snapshot,
        );

        // Ensure target directory exists
        Self::ensure_snapshot_dir_exists(&target_dir).await?;

        let mut entries = tokio::fs::read_dir(&staging_dir).await?;
        let mut moved_count = 0usize;

        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }

            let file_name = entry.file_name();

            // Skip the WAL file — it is managed separately (removed after
            // all data files have been successfully moved).
            if file_name == STAGING_WAL_FILENAME {
                continue;
            }

            let src = staging_dir.join(&file_name);
            let dst = target_dir.join(&file_name);

            tokio::fs::rename(&src, &dst).await?;
            moved_count += 1;
        }

        tracing::debug!(
            "Moved {moved_count} file(s) from staging to snapshot {current_snapshot} for table {table_name}",
            table_name = self.table_metadata.table_name,
        );

        Ok(())
    }

    /// Move staging files to the current snapshot on S3.
    ///
    /// Uses copy-all-then-delete-all ordering: all files are copied to the target
    /// prefix first, then staging originals are deleted. If interrupted after copies
    /// but before deletes, data exists in both locations (safe — deduplicated by PK
    /// or idempotent for append-only tables).
    async fn move_staging_files_s3(&self, current_snapshot: &str) -> Result<()> {
        let config = self.require_object_store()?;

        let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? else {
            return Ok(());
        };
        let Some(target_prefix) = self.snapshot_object_store_prefix(current_snapshot)? else {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!("Cannot compute S3 prefix for snapshot '{current_snapshot}'"),
            });
        };

        // List all objects in staging
        let objects: Vec<_> = config
            .store
            .list(Some(&staging_prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list staging objects for move",
                table: self.table_metadata.table_name.clone(),
                source: e,
            })?;

        if objects.is_empty() {
            return Ok(());
        }

        // Phase 1: copy data objects to target prefix (skip WAL file)
        let mut copied_locations = Vec::with_capacity(objects.len());
        for meta in &objects {
            let relative = meta
                .location
                .as_ref()
                .strip_prefix(staging_prefix.as_ref())
                .ok_or_else(|| Error::Internal {
                    table: self.table_metadata.table_name.clone(),
                    message: format!(
                        "Staging object '{}' does not have expected prefix '{}'",
                        meta.location,
                        staging_prefix.as_ref(),
                    ),
                })?;

            // Skip the WAL file — it is managed separately (removed after
            // all data files have been successfully copied/deleted).
            if relative == STAGING_WAL_FILENAME {
                continue;
            }
            let target_path =
                ObjectStorePath::from(format!("{}{relative}", target_prefix.as_ref()));

            config
                .store
                .copy(&meta.location, &target_path)
                .await
                .map_err(|e| Error::ObjectStore {
                    operation: "copy staging file to snapshot",
                    table: self.table_metadata.table_name.clone(),
                    source: e,
                })?;
            copied_locations.push(meta.location.clone());
        }

        // Phase 2: delete staging originals
        for location in &copied_locations {
            config
                .store
                .delete(location)
                .await
                .map_err(|e| Error::ObjectStore {
                    operation: "delete staging file after copy",
                    table: self.table_metadata.table_name.clone(),
                    source: e,
                })?;
        }

        tracing::debug!(
            "Moved {} file(s) from staging to snapshot {current_snapshot} (S3) for table {}",
            copied_locations.len(),
            self.table_metadata.table_name,
        );

        Ok(())
    }

    /// Sync a directory to ensure all files are durably written to disk.
    ///
    /// This is critical for crash safety: we must ensure all data files are
    /// persisted before updating the catalog metadata. Otherwise, a crash
    /// after catalog update but before data flush could result in a catalog
    /// pointing to incomplete/missing data files.
    ///
    /// # ACID Durability
    ///
    /// This function is part of the durability guarantee:
    /// 1. Write data files to new snapshot directory
    /// 2. Sync directory (this function) - ensures data is on disk
    /// 3. Update catalog atomically - commits the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be synced.
    pub(crate) async fn sync_snapshot_dir(snapshot_dir: &std::path::Path) -> CatalogResult<()> {
        let snapshot_dir = snapshot_dir.to_path_buf();
        let dir_display = snapshot_dir.display().to_string();
        tokio::task::spawn_blocking(move || {
            // Open the directory and call sync_all to flush metadata
            let dir = std::fs::File::open(&snapshot_dir)
                .map_err(|source| CatalogError::IoError { source })?;
            dir.sync_all()
                .map_err(|source| CatalogError::IoError { source })?;
            Ok::<(), CatalogError>(())
        })
        .await
        .map_err(|e| Error::TaskPanicked {
            table: dir_display,
            source: e,
        })?
    }

    /// Cleanup old snapshot directories after a full refresh.
    ///
    /// For full refresh mode, after the new snapshot is written and the catalog is updated,
    /// old snapshot directories are no longer needed and can be physically deleted.
    ///
    /// This function performs blocking filesystem I/O and should be called from within
    /// `tokio::task::spawn_blocking` to avoid blocking the async runtime thread pool.
    ///
    /// # Arguments
    ///
    /// * `table_path` - Base path for the table
    /// * `table_id` - Table identifier
    /// * `current_snapshot_id` - The current (active) snapshot ID that should be kept
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot directories cannot be listed or deleted.
    ///
    /// # Blocking I/O Warning
    ///
    /// This function uses `std::fs` for filesystem operations and will block the calling thread.
    /// It must be called from within `tokio::task::spawn_blocking`.
    fn cleanup_old_snapshots_blocking(
        table_path: &str,
        table_id: &str,
        current_snapshot_id: &str,
        protected_snapshot_ids: &HashSet<String>,
    ) -> CatalogResult<()> {
        let table_dir = std::path::PathBuf::from(table_path).join(table_id);

        // Check if table directory exists
        if !table_dir.exists() {
            return Ok(());
        }

        tracing::debug!(
            "Cleaning up old snapshots for table {} (keeping current={}, protected={})",
            table_id,
            current_snapshot_id,
            protected_snapshot_ids.len()
        );

        // Read all entries in the table directory using blocking I/O
        let entries =
            std::fs::read_dir(&table_dir).map_err(|source| CatalogError::IoError { source })?;

        let mut deleted_count = 0;
        for entry_result in entries {
            let entry = entry_result.map_err(|source| CatalogError::IoError { source })?;
            let path = entry.path();

            // Only process directories (snapshots)
            if !path.is_dir() {
                continue;
            }

            // Get the snapshot ID (directory name)
            let Some(snapshot_id) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            // Skip the current snapshot, protected snapshots, and the staging directory
            if snapshot_id == current_snapshot_id
                || protected_snapshot_ids.contains(snapshot_id)
                || snapshot_id == STAGING_DIR_NAME
            {
                tracing::debug!(
                    "Keeping snapshot: {} (current, protected, or staging)",
                    snapshot_id
                );
                continue;
            }

            // Delete the old snapshot directory using blocking I/O
            tracing::info!(
                "Deleting old snapshot directory for table {}: {}",
                table_id,
                snapshot_id
            );

            std::fs::remove_dir_all(&path).map_err(|source| CatalogError::IoError { source })?;

            deleted_count += 1;
        }

        if deleted_count > 0 {
            tracing::info!(
                "Cleaned up {} old snapshot(s) for table {}",
                deleted_count,
                table_id
            );
        } else {
            tracing::debug!("No old snapshots to cleanup for table {}", table_id);
        }

        Ok(())
    }

    /// Create a new Cayenne table provider.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .open(table_name)
            .await
    }

    /// Create a new table provider with explicit retention filters.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new_with_retention(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        retention_filters: Vec<Expr>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_retention_filters(retention_filters)
            .open(table_name)
            .await
    }

    /// Internal constructor used by the builder.
    async fn new_internal(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        retention_filters: Vec<Expr>,
        time_retention_filter_builder: Option<super::retention::TimeRetentionFilterBuilder>,
        object_store_config: Option<crate::metadata::ObjectStoreConfig>,
        runtime_env: Arc<RuntimeEnv>,
        context: Option<Arc<CayenneContext>>,
    ) -> CatalogResult<Self> {
        let table_metadata = catalog.get_table(table_name).await?;

        // Use the provided context (for partition cache sharing) or build a
        // fresh one from this table's VortexConfig and the shared RuntimeEnv.
        let context = context
            .unwrap_or_else(|| CayenneContext::new(&table_metadata.vortex_config, runtime_env));

        if table_metadata.path.starts_with("s3://") && object_store_config.is_none() {
            return Err(Error::Internal {
                table: table_name.to_string(),
                message: "Table uses S3 storage but no object_store_config was provided"
                    .to_string(),
            }
            .into());
        }

        // Construct URL to current snapshot
        // Directory structure: [table_path]/[table_id]/[snapshot_id]/
        // All tables have a snapshot ID (created on table initialization)
        let snapshot_dir_url = Self::snapshot_dir_url(
            &table_metadata.path,
            &table_metadata.table_id,
            &table_metadata.current_snapshot_id,
        );

        // Determine if this table has a primary key for key-based deletion
        let has_primary_key = !table_metadata.primary_key.is_empty();

        // Determine PK deletion strategy kind and build RowConverter if needed
        let (pk_deletion_strategy_kind, pk_row_converter, pk_column_indices) = if has_primary_key {
            let schema = &table_metadata.schema;
            let mut indices = Vec::with_capacity(table_metadata.primary_key.len());
            let mut pk_fields = Vec::with_capacity(table_metadata.primary_key.len());

            for pk_col in &table_metadata.primary_key {
                let (idx, field) =
                    schema
                        .column_with_name(pk_col)
                        .ok_or_else(|| Error::DataValidation {
                            table: table_name.to_string(),
                            message: format!("Primary key column '{pk_col}' not found in schema"),
                        })?;
                indices.push(idx);
                pk_fields.push(field.clone());
            }

            // Check if we can use the optimized Int64 PK strategy:
            // - Single column primary key
            // - Column type is Int64
            if pk_fields.len() == 1
                && *pk_fields[0].data_type() == arrow::datatypes::DataType::Int64
            {
                // Optimized path: single Int64 PK - no RowConverter needed
                (PkDeletionStrategy::Int64Pk, None, indices)
            } else {
                // General path: composite or non-integer PK - use RowConverter
                let sort_fields: Vec<SortField> = pk_fields
                    .iter()
                    .map(|f| SortField::new(f.data_type().clone()))
                    .collect();

                let row_converter = RowConverter::new(sort_fields).map_err(Error::from)?;

                (
                    PkDeletionStrategy::RowConverterBased,
                    Some(Arc::new(row_converter)),
                    indices,
                )
            }
        } else {
            (PkDeletionStrategy::PositionBased, None, Vec::new())
        };

        // Load deletion vectors and insert records once at initialization
        // to avoid repeated SQLite queries on every scan.
        // Returns the fully constructed PkDeletionStrategy with embedded caches.
        let table_id = table_metadata.table_id.clone();
        let catalog_for_load = Arc::clone(&catalog);
        let pk_deletion_strategy =
            Self::load_deletion_vectors_all(&table_id, catalog_for_load, pk_deletion_strategy_kind)
                .await?;

        let listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::<arrow_schema::Schema>::clone(&table_metadata.schema),
            context.file_format(),
            &pk_deletion_strategy,
        )?;

        // Load protected snapshots from catalog.
        // Protected snapshots are those with sequence > max_delete_sequence.
        // They contain data written after deletions and should skip deletion filtering.
        let protected_snapshots =
            Self::load_protected_snapshots(Arc::clone(&catalog), &table_id, &pk_deletion_strategy)
                .await?;
        let inlined_row_count = catalog.get_inlined_data_count(&table_id).await?;

        // Register the S3 object store in the shared RuntimeEnv once during
        // construction. Every code path that creates a SessionContext from
        // `self.context.runtime_env()` (e.g. `create_session_context`, keyset
        // loading, deletion sinks) will automatically inherit the store.
        if let Some(ref config) = object_store_config {
            Self::register_object_store_if_needed(context.runtime_env(), config);
        }

        let provider = Self {
            current_snapshot_id: Arc::new(RwLock::new(table_metadata.current_snapshot_id.clone())),
            table_metadata,
            catalog,
            listing_table: Arc::new(RwLock::new(listing_table)),
            retention_filters,
            time_retention_filter_builder,
            context,
            pk_deletion_strategy,
            pk_row_converter,
            pk_column_indices,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            object_store_config,
            protected_snapshots: Arc::new(RwLock::new(protected_snapshots)),
            inlined_row_count: Arc::new(AtomicI64::new(inlined_row_count)),
        };

        // Fail construction if a staging WAL exists — the table may contain
        // partial data from an interrupted append and must be resolved first.
        provider.ensure_no_incomplete_write().await?;

        Ok(provider)
    }

    /// Create a new table in Cayenne.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .create(options)
            .await
            .map_err(Into::into)
    }

    /// Create a new table in Cayenne with retention filters applied to subsequent writes.
    ///
    /// For more configuration options, use [`CayenneTableProviderBuilder`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table_with_retention(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
        retention_filters: Vec<Expr>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_retention_filters(retention_filters)
            .create(options)
            .await
            .map_err(Into::into)
    }

    /// Get a reference to the catalog.
    ///
    /// This is useful for testing and advanced use cases that need direct catalog access.
    #[must_use]
    pub fn catalog(&self) -> &Arc<dyn MetadataCatalog> {
        &self.catalog
    }

    /// Get the table metadata.
    #[must_use]
    pub fn metadata(&self) -> &TableMetadata {
        &self.table_metadata
    }

    /// Insert data to a NEW snapshot with a specific sequence number.
    ///
    /// This is used when inserting while pending PK-based deletions exist.
    /// By writing to a new snapshot with a higher sequence number, we ensure:
    /// - Old data in previous snapshots is filtered by deletions (`delete_seq` >= `old_snapshot_seq`)
    /// - New data in this snapshot is visible (`new_snapshot_seq` > `delete_seq`)
    ///
    /// This achieves Iceberg-style sequence ordering without rewriting existing files.
    pub(crate) async fn insert_to_new_snapshot_with_sequence(
        &self,
        stream: SendableRecordBatchStream,
        sequence_number: i64,
    ) -> CatalogResult<(u64, Arc<ColumnStatsAccumulator>)> {
        let target_size_bytes = self.context.target_file_size_bytes();

        // Generate a new snapshot ID
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();

        // Write data to the new snapshot
        let (total_rows, chunk_count, stats_acc) = self
            .write_to_snapshot(stream, target_size_bytes, &new_snapshot_id)
            .await?;

        tracing::debug!(
            "Insert to new snapshot {} completed, wrote {} rows to Vortex in {} chunk(s)",
            new_snapshot_id,
            total_rows,
            chunk_count
        );

        // Record the snapshot's sequence number in the catalog
        self.catalog
            .set_snapshot_sequence(
                &self.table_metadata.table_id,
                &new_snapshot_id,
                sequence_number,
            )
            .await?;

        // Get the maximum delete sequence from current deletions.
        // This snapshot is protected from deletions with seq <= max_delete_seq.
        let max_delete_seq = self.get_max_delete_sequence();

        // Add to protected snapshots so scan applies only NEWER deletions (seq > max_delete_seq)
        // We do NOT clear old protected snapshots because they may contain data that's still valid.
        // Each protected snapshot applies its own partial deletion filter based on when it was created.
        {
            let mut guard = self
                .protected_snapshots
                .write()
                .map_err(|_| Error::LockPoisoned {
                    table: self.table_metadata.table_name.clone(),
                    lock: PROTECTED_SNAPSHOTS_LOCK_POISONED,
                })?;
            guard.insert(new_snapshot_id.clone(), max_delete_seq);
        }

        // The listing table stays as-is. Protected snapshots are handled at scan time.
        // See the doc comment above for why we do NOT update current_snapshot.

        Ok((total_rows, stats_acc))
    }

    /// Get the maximum delete sequence number from the cached deletions.
    fn get_max_delete_sequence(&self) -> i64 {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => cached_deleted_pk
                .load()
                .entries()
                .values()
                .max()
                .copied()
                .unwrap_or(0),
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => cached_deleted_row_keys
                .load()
                .entries()
                .values()
                .max()
                .copied()
                .unwrap_or(0),
            PkDeletionStrategyWithCache::PositionBased { .. } => 0,
        }
    }

    /// Write a stream of record batches to a specific snapshot directory.
    ///
    /// This is used during compaction operations where data needs to be persisted
    /// to a new snapshot.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream of record batches to write
    /// * `target_size_bytes` - Configured writer target file size (for write behavior/logging)
    /// * `snapshot_id` - The snapshot ID to write to
    ///
    /// # Returns
    ///
    /// A tuple of (total rows written, number of writer operations)
    ///
    /// # Errors
    ///
    /// Returns an error if the write operation fails.
    pub(crate) async fn write_to_snapshot(
        &self,
        stream: SendableRecordBatchStream,
        target_size_bytes: usize,
        snapshot_id: &str,
    ) -> Result<(u64, usize, Arc<ColumnStatsAccumulator>)> {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::Instant;

        // Construct snapshot directory URL
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        );

        // Create a new ListingTable pointing to the snapshot directory
        let snapshot_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        // Create session context once with object store registered (if S3).
        let session_state = Arc::new(self.create_session_context().state());

        // Progress tracking for S3 Express uploads
        let is_s3_storage = self.table_metadata.path.starts_with("s3://");
        let start_time = Instant::now();
        let last_progress_ms = Arc::new(AtomicU64::new(0));
        let total_bytes_written = Arc::new(AtomicUsize::new(0));
        let total_rows_written = Arc::new(AtomicU64::new(0));

        // Column stats accumulator — updated per batch during writes
        let stats_accumulator = Arc::new(ColumnStatsAccumulator::new(&self.table_metadata.schema));

        // Log when starting S3 upload process
        if is_s3_storage {
            tracing::info!(
                "Starting S3 upload to snapshot {} for table {} (writer target file size: {})",
                snapshot_id,
                self.table_metadata.table_name,
                format_bytes(target_size_bytes)
            );
        }

        let tracked_schema = Arc::clone(&self.table_metadata.schema);
        let tracked_stream = {
            let total_bytes_written = Arc::clone(&total_bytes_written);
            let total_rows_written = Arc::clone(&total_rows_written);
            let last_progress_ms = Arc::clone(&last_progress_ms);
            let stats_acc = Arc::clone(&stats_accumulator);
            let table_name = self.table_metadata.table_name.clone();
            let start = start_time;

            stream.map(move |batch_result| {
                if let Ok(batch) = &batch_result {
                    total_bytes_written.fetch_add(batch.get_array_memory_size(), Ordering::Relaxed);
                    total_rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    stats_acc.update(batch);

                    if is_s3_storage {
                        let elapsed = start.elapsed();
                        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                        let last_logged = last_progress_ms.load(Ordering::Relaxed);
                        if elapsed_ms.saturating_sub(last_logged) >= 10_000 {
                            let bytes_so_far = total_bytes_written.load(Ordering::Relaxed);
                            let throughput = if elapsed.as_secs_f64() > 0.0 {
                                #[expect(clippy::cast_precision_loss)]
                                let bytes_per_sec = bytes_so_far as f64 / elapsed.as_secs_f64();
                                format_bytes_per_sec(bytes_per_sec)
                            } else {
                                "calculating...".to_string()
                            };
                            tracing::info!(
                                "S3 upload for {}: streamed {} in {:.1}s, {}",
                                table_name,
                                format_bytes(bytes_so_far),
                                elapsed.as_secs_f64(),
                                throughput
                            );
                            last_progress_ms.store(elapsed_ms, Ordering::Relaxed);
                        }
                    }
                }
                batch_result
            })
        };

        let tracked_stream =
            RecordBatchStreamAdapter::new(Arc::clone(&tracked_schema), tracked_stream);
        let stream_exec = Arc::new(StreamingExec::new(tracked_schema, Box::pin(tracked_stream)));

        let insert_plan = snapshot_listing_table
            .insert_into(session_state.as_ref(), stream_exec, InsertOp::Append)
            .await?;

        collect(insert_plan, session_state.task_ctx()).await?;

        let total_rows = total_rows_written.load(Ordering::Relaxed);
        let writer_ops = usize::from(total_rows > 0); // 1 writer op if we wrote any rows, otherwise 0

        // Log final summary for S3 Express uploads
        if is_s3_storage {
            let elapsed = start_time.elapsed();
            let total_bytes = total_bytes_written.load(Ordering::Relaxed);
            let throughput = if elapsed.as_secs_f64() > 0.0 {
                #[expect(clippy::cast_precision_loss)]
                let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
                format_bytes_per_sec(bytes_per_sec)
            } else {
                "N/A".to_string()
            };
            tracing::info!(
                "Completed S3 upload for {} to snapshot {}: {} rows across {} writer operation(s) ({}) in {:.1}s, {}",
                self.table_metadata.table_name,
                snapshot_id,
                total_rows,
                writer_ops,
                format_bytes(total_bytes),
                elapsed.as_secs_f64(),
                throughput
            );
        }

        Ok((total_rows, writer_ops, stats_accumulator))
    }

    /// Create a clone of necessary fields for parallel write tasks.
    ///
    /// This method clones only the Arc references needed for writing,
    /// which is cheap (just atomic reference count increments).
    ///
    /// # Note on Retention Filters
    ///
    /// Retention filters are preserved in the clone because they need to be applied
    /// by `insert()` at the end of each write operation. The `insert()` method holds
    /// the write lock and applies retention atomically after all parallel chunk writes
    /// complete.
    ///
    /// This design provides ACID semantics:
    /// - Retention filters are table-wide predicates (e.g., "delete rows older than 30 days")
    /// - They must scan all table data, not just the newly written chunks
    /// - The write lock ensures atomicity: all writes + retention happen as one operation
    pub(crate) fn clone_for_write(&self) -> Self {
        Self {
            table_metadata: self.table_metadata.clone(),
            catalog: Arc::clone(&self.catalog),
            listing_table: Arc::clone(&self.listing_table),
            context: Arc::clone(&self.context),
            retention_filters: self.retention_filters.clone(),
            time_retention_filter_builder: self.time_retention_filter_builder.clone(),
            pk_deletion_strategy: self.pk_deletion_strategy.clone(),
            pk_row_converter: self.pk_row_converter.as_ref().map(Arc::clone),
            pk_column_indices: self.pk_column_indices.clone(),
            write_lock: Arc::clone(&self.write_lock), // Shared across all clones for same table
            object_store_config: self.object_store_config.clone(),
            current_snapshot_id: Arc::clone(&self.current_snapshot_id),
            protected_snapshots: Arc::clone(&self.protected_snapshots),
            inlined_row_count: Arc::clone(&self.inlined_row_count),
        }
    }

    /// Returns the column indices for the configured primary key, if any.
    fn primary_key_indices(&self) -> Result<Option<Vec<usize>>> {
        if self.table_metadata.primary_key.is_empty() {
            return Ok(None);
        }

        let mut indices = Vec::with_capacity(self.table_metadata.primary_key.len());
        for pk_col in &self.table_metadata.primary_key {
            let idx =
                self.table_metadata
                    .schema
                    .index_of(pk_col)
                    .map_err(|_| Error::DataValidation {
                        table: self.table_metadata.table_name.clone(),
                        message: format!("Primary key column '{pk_col}' not found in schema"),
                    })?;
            indices.push(idx);
        }

        Ok(Some(indices))
    }

    /// Build a `RowConverter` for the primary key columns.
    fn build_pk_converter(&self, pk_indices: &[usize]) -> Result<RowConverter> {
        let mut sort_fields = Vec::with_capacity(pk_indices.len());
        for idx in pk_indices {
            let field = self.table_metadata.schema.field(*idx);
            sort_fields.push(SortField::new(field.data_type().clone()));
        }

        Ok(RowConverter::new(sort_fields)?)
    }

    /// Build the existing keyset (primary key bytes -> row location) for append-mode inserts.
    ///
    /// This method scans BOTH the main listing table AND any protected snapshots to build
    /// a complete keyset of all existing primary keys.
    ///
    /// This method respects ALL deletion caches based on `pk_deletion_strategy`:
    /// - `Int64Pk`: Uses `cached_deleted_pk_i64` and `cached_insert_records_pk_i64`
    /// - `RowConverterBased`: Uses `cached_deleted_row_keys` and `cached_insert_records_row_keys`
    /// - `PositionBased`: Uses `cached_deleted_row_ids` (no primary key)
    ///
    /// Rows marked as deleted are excluded unless they were re-inserted with a higher
    /// sequence number (upsert semantics).
    async fn load_existing_keyset(
        &self,
        pk_indices: &[usize],
        converter: &RowConverter,
    ) -> Result<HashMap<OwnedRow, RowLocation>> {
        // Clone listing table to avoid holding locks across await points
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| Error::LockPoisoned {
                table: self.table_metadata.table_name.clone(),
                lock: LISTING_TABLE_LOCK_POISONED,
            })?;
            Arc::clone(&guard)
        };

        // Clone protected snapshots to avoid holding locks across await points
        let protected_snapshots = {
            let guard = self
                .protected_snapshots
                .read()
                .map_err(|_| Error::LockPoisoned {
                    table: self.table_metadata.table_name.clone(),
                    lock: PROTECTED_SNAPSHOTS_LOCK_POISONED,
                })?;
            guard.clone()
        };

        let ctx = self.create_session_context();
        // Only read PK columns - no need to load all columns for keyset building
        let pk_projection = pk_indices.to_vec();

        // Scan main listing table
        let scan_plan = listing_table
            .scan(&ctx.state(), Some(&pk_projection), &[], None)
            .await?;

        // Load the deletion caches based on pk_deletion_strategy.
        // Note: PositionBased strategy is never used here since it implies no primary key,
        // and this function is only called for tables with primary keys.
        // ArcSwap loads are wait-free; the resulting `Arc<...Index>` is an immutable
        // snapshot of the deletion state at this instant.
        let deleted_pk_i64: Option<Arc<DeletionIndex>> = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => Some(cached_deleted_pk.load_full()),
            _ => None,
        };

        let deleted_row_keys: Option<Arc<KeyDeletionIndex>> = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => Some(cached_deleted_row_keys.load_full()),
            _ => None,
        };

        let mut keyset = HashMap::with_capacity(1024);
        let mut row_id_base: i64 = 0;

        // After projection, batch columns are at indices 0..pk_indices.len()
        let projected_pk_indices: Vec<usize> = (0..pk_indices.len()).collect();

        // Process main listing table batches with the FULL deletion filter (no insert_records).
        // This mirrors scan()'s apply_deletion_filter() which uses all deletions without
        // insert_records when protected snapshots exist.
        // min_delete_seq_threshold=None means ALL deletions apply.
        let main_stream = datafusion_physical_plan::execute_stream(scan_plan, ctx.task_ctx())?;
        Self::process_stream_into_keyset(
            main_stream,
            &self.pk_deletion_strategy,
            pk_indices,
            converter,
            &projected_pk_indices,
            deleted_pk_i64.as_deref(),
            deleted_row_keys.as_deref(),
            None, // all deletions apply to main listing table
            &self.table_metadata.table_name,
            &mut keyset,
            &mut row_id_base,
        )
        .await?;

        // Process each protected snapshot with a PARTIAL deletion filter.
        // Only deletions with seq > max_delete_seq_at_creation apply, mirroring
        // scan()'s apply_partial_deletion_filter().
        for (snapshot_id, max_delete_seq_at_creation) in &protected_snapshots {
            let snapshot_url = Self::snapshot_dir_url(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                snapshot_id,
            );

            let snapshot_listing_table = Self::create_listing_table(
                &snapshot_url,
                Arc::clone(&self.table_metadata.schema),
                self.context.file_format(),
                &self.pk_deletion_strategy,
            )?;

            let snapshot_plan = snapshot_listing_table
                .scan(&ctx.state(), Some(&pk_projection), &[], None)
                .await?;

            let snapshot_stream =
                datafusion_physical_plan::execute_stream(snapshot_plan, ctx.task_ctx())?;

            Self::process_stream_into_keyset(
                snapshot_stream,
                &self.pk_deletion_strategy,
                pk_indices,
                converter,
                &projected_pk_indices,
                deleted_pk_i64.as_deref(),
                deleted_row_keys.as_deref(),
                Some(*max_delete_seq_at_creation), // only deletions with seq > threshold apply
                &self.table_metadata.table_name,
                &mut keyset,
                &mut row_id_base,
            )
            .await?;
        }

        let inlined_batches = self.read_inlined_batches().await?;
        self.process_visible_inlined_batches_into_keyset(
            &inlined_batches,
            pk_indices,
            converter,
            &mut keyset,
        )?;

        Ok(keyset)
    }

    fn process_visible_inlined_batches_into_keyset(
        &self,
        batches: &[RecordBatch],
        pk_indices: &[usize],
        converter: &RowConverter,
        keyset: &mut HashMap<OwnedRow, RowLocation>,
    ) -> Result<()> {
        for batch in batches {
            let pk_columns: Vec<_> = pk_indices
                .iter()
                .map(|idx| Arc::clone(batch.column(*idx)))
                .collect();
            let rows = converter.convert_columns(&pk_columns)?;

            for row_index in 0..batch.num_rows() {
                if pk_columns.iter().any(|column| column.is_null(row_index)) {
                    return Err(Error::DataValidation {
                        table: self.table_metadata.table_name.clone(),
                        message: format!(
                            "Null primary key encountered in inlined data for table {}",
                            self.table_metadata.table_name,
                        ),
                    });
                }

                keyset.insert(
                    rows.row(row_index).owned(),
                    RowLocation {
                        source: RowSource::Inlined,
                        data_file_id: DEFAULT_DATA_FILE_ID,
                        row_id: -1,
                    },
                );
            }
        }

        Ok(())
    }

    /// Process a record batch stream and add visible keys to the keyset.
    ///
    /// Filters out deleted rows using the provided deletion maps. No `insert_records` are
    /// used — visibility is determined solely by whether a deletion exists for the key.
    ///
    /// `min_delete_seq_threshold`: When `Some(threshold)`, only deletions with
    /// `seq > threshold` are considered (for protected snapshots). When `None`, all
    /// deletions apply (for the main listing table). This avoids building filtered
    /// `HashMap` copies per snapshot — each row is checked with a single O(1) lookup.
    ///
    /// Keys from later batches override earlier ones in the keyset, which is correct
    /// because protected snapshots contain data inserted at higher sequence numbers.
    #[expect(clippy::too_many_arguments)]
    async fn process_stream_into_keyset(
        mut stream: SendableRecordBatchStream,
        pk_deletion_strategy: &PkDeletionStrategyWithCache,
        pk_indices: &[usize],
        converter: &RowConverter,
        projected_pk_indices: &[usize],
        deleted_pk_i64: Option<&DeletionIndex>,
        deleted_row_keys: Option<&KeyDeletionIndex>,
        min_delete_seq_threshold: Option<i64>,
        table_name: &str,
        keyset: &mut HashMap<OwnedRow, RowLocation>,
        row_id_base: &mut i64,
    ) -> Result<()> {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let pk_columns: Vec<_> = projected_pk_indices
                .iter()
                .map(|idx| Arc::clone(batch.column(*idx)))
                .collect();

            let rows = converter.convert_columns(&pk_columns)?;

            // For Int64Pk strategy, get the PK column as Int64Array for efficient lookup
            let int64_pk_array: Option<&arrow::array::Int64Array> =
                if pk_deletion_strategy.is_int64_pk() && pk_indices.len() == 1 {
                    batch.column(0).as_any().downcast_ref()
                } else {
                    None
                };

            for row_idx in 0..batch.num_rows() {
                let row_id = *row_id_base
                    + i64::try_from(row_idx).map_err(|_| Error::Internal {
                        table: table_name.to_string(),
                        message: "Row index exceeds i64::MAX; cannot compute row_id".to_string(),
                    })?;

                // Check if row is deleted based on pk_deletion_strategy.
                // For main batches (threshold=None): all deletions apply.
                // For protected snapshots (threshold=Some(T)): only deletions with seq > T apply.
                let is_deleted = match pk_deletion_strategy {
                    PkDeletionStrategyWithCache::Int64Pk { .. } => {
                        if let (Some(pk_array), Some(deleted_pks)) =
                            (int64_pk_array, deleted_pk_i64)
                        {
                            let pk_value = pk_array.value(row_idx);
                            match deleted_pks.get(pk_value) {
                                None => false, // not deleted (bloom-prefiltered)
                                Some(del_seq) => match min_delete_seq_threshold {
                                    None => true, // all deletions apply
                                    Some(threshold) => del_seq > threshold,
                                },
                            }
                        } else {
                            false
                        }
                    }
                    PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                        if let Some(deleted_keys) = deleted_row_keys {
                            let key = rows.row(row_idx);
                            match deleted_keys.get(key.as_ref()) {
                                None => false, // not deleted (bloom-prefiltered)
                                Some(del_seq) => match min_delete_seq_threshold {
                                    None => true, // all deletions apply
                                    Some(threshold) => del_seq > threshold,
                                },
                            }
                        } else {
                            false
                        }
                    }
                    PkDeletionStrategyWithCache::PositionBased { .. } => {
                        unreachable!("PositionBased strategy should not reach load_existing_keyset")
                    }
                };

                if is_deleted {
                    continue;
                }

                // Enforce non-null primary key values
                let has_null = pk_columns.iter().any(|col| col.is_null(row_idx));
                if has_null {
                    return Err(Error::DataValidation {
                        table: table_name.to_string(),
                        message: format!(
                            "Null primary key encountered in existing data for table {table_name}",
                        ),
                    });
                }

                let key = rows.row(row_idx).owned();

                // Insert or update the key in the keyset.
                // Keys from protected snapshots may override keys from the main listing table
                // because protected snapshots contain data inserted at higher sequence numbers.
                // This is expected behavior for upserts.
                keyset.insert(
                    key,
                    RowLocation {
                        source: RowSource::File,
                        data_file_id: DEFAULT_DATA_FILE_ID,
                        row_id,
                    },
                );
            }

            *row_id_base += i64::try_from(batch.num_rows()).map_err(|_| Error::Internal {
                table: table_name.to_string(),
                message: "Batch row count exceeds i64::MAX; cannot compute row_id_base".to_string(),
            })?;
        }

        Ok(())
    }

    /// Prepare an incoming stream for insert by validating `on_conflict` constraints.
    ///
    /// If a primary key is configured, this method:
    /// 1. Loads existing keys from the table (respecting deletion visibility)
    /// 2. Validates incoming rows against `on_conflict` behavior (drop/upsert)
    /// 3. Returns a prepared stream with conflicts resolved and deletion specs
    ///
    /// If no primary key is configured, returns the stream unchanged with empty deletion specs.
    pub(crate) async fn prepare_stream_for_insert(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<PreparedInsertStream> {
        let Some(pk_indices) = self.primary_key_indices()? else {
            return Ok(PreparedInsertStream {
                stream,
                on_conflict_deletions: OnConflictDeletions::default(),
            });
        };

        let converter = self.build_pk_converter(&pk_indices)?;
        let mut existing_keys = self.load_existing_keyset(&pk_indices, &converter).await?;
        tracing::debug!(
            "prepare_stream_for_insert: loaded {} existing keys for table {}",
            existing_keys.len(),
            self.table_metadata.table_name
        );

        let validation_result = self
            .validate_on_conflict(stream, &pk_indices, &converter, &mut existing_keys)
            .await?;

        // Build a new stream from the validated batches.
        let schema = validation_result.filtered_batches.first().map_or_else(
            || Arc::clone(&self.table_metadata.schema),
            RecordBatch::schema,
        );
        let validated_stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(validation_result.filtered_batches.into_iter().map(Ok)),
        );

        Ok(PreparedInsertStream {
            stream: Box::pin(validated_stream) as SendableRecordBatchStream,
            on_conflict_deletions: validation_result.on_conflict_deletions,
        })
    }

    /// Validate incoming batches against primary key uniqueness and configured on-conflict behavior.
    ///
    /// Returns filtered batches (with dropped rows removed) and a map of deletion vector specs
    /// keyed by `data_file_id`.
    async fn validate_on_conflict(
        &self,
        mut stream: SendableRecordBatchStream,
        pk_indices: &[usize],
        converter: &RowConverter,
        existing_keys: &mut HashMap<OwnedRow, RowLocation>,
    ) -> Result<OnConflictValidationResult> {
        let mut incoming_keys: HashSet<OwnedRow> = HashSet::with_capacity(1024);
        let mut filtered_batches = Vec::new();
        let mut delete_specs: HashMap<i64, Vec<i64>> = HashMap::new();
        let mut all_deleted_pk_i64: Vec<i64> = Vec::new();
        let mut all_deleted_row_keys: Vec<Box<[u8]>> = Vec::new();
        let mut all_deleted_inlined_pk_i64: Vec<i64> = Vec::new();
        let mut all_deleted_inlined_row_keys: Vec<Box<[u8]>> = Vec::new();

        // Use configured on_conflict or default to DoNothingAll (silently drops duplicates).
        // When a primary key is configured without explicit on_conflict, this ensures
        // inserts succeed without unique constraint errors.
        let on_conflict = self
            .table_metadata
            .on_conflict
            .clone()
            .unwrap_or(OnConflict::DoNothingAll);
        let upsert_options = on_conflict.get_upsert_options();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;

            if batch.num_rows() == 0 {
                continue;
            }

            let mut ctx = OnConflictContext {
                pk_indices,
                converter,
                on_conflict: &on_conflict,
                upsert_options: &upsert_options,
                existing_keys,
                incoming_keys: &incoming_keys,
            };

            let BatchValidationResult {
                filtered_batch,
                delete_specs: batch_delete_specs,
                kept_keys,
                deleted_pk_i64,
                deleted_row_keys,
                deleted_inlined_pk_i64,
                deleted_inlined_row_keys,
            } = self.apply_on_conflict_to_batch(batch, &mut ctx)?;

            for (data_file_id, rows) in batch_delete_specs {
                delete_specs.entry(data_file_id).or_default().extend(rows);
            }

            all_deleted_pk_i64.extend(deleted_pk_i64);
            all_deleted_row_keys.extend(deleted_row_keys);
            all_deleted_inlined_pk_i64.extend(deleted_inlined_pk_i64);
            all_deleted_inlined_row_keys.extend(deleted_inlined_row_keys);

            incoming_keys.extend(kept_keys);

            if let Some(batch) = filtered_batch {
                filtered_batches.push(batch);
            }
        }

        Ok(OnConflictValidationResult {
            filtered_batches,
            on_conflict_deletions: OnConflictDeletions {
                delete_specs,
                deleted_pk_i64: all_deleted_pk_i64,
                deleted_row_keys: all_deleted_row_keys,
                deleted_inlined_pk_i64: all_deleted_inlined_pk_i64,
                deleted_inlined_row_keys: all_deleted_inlined_row_keys,
            },
        })
    }

    fn apply_on_conflict_to_batch(
        &self,
        batch: RecordBatch,
        ctx: &mut OnConflictContext<'_>,
    ) -> Result<BatchValidationResult> {
        use arrow::array::Int64Array;

        let pk_columns: Vec<_> = ctx
            .pk_indices
            .iter()
            .map(|idx| Arc::clone(batch.column(*idx)))
            .collect();

        let rows = ctx.converter.convert_columns(&pk_columns)?;

        // For Int64Pk strategy, get direct access to the PK column for value extraction
        let int64_pk_array: Option<&Int64Array> =
            if self.pk_deletion_strategy.is_int64_pk() && pk_columns.len() == 1 {
                pk_columns[0].as_any().downcast_ref::<Int64Array>()
            } else {
                None
            };

        let mut keep_mask = Vec::with_capacity(batch.num_rows());
        let mut row_keys: Vec<OwnedRow> = Vec::with_capacity(batch.num_rows());
        let mut delete_specs: HashMap<i64, Vec<i64>> = HashMap::new();
        let mut deleted_pk_i64: Vec<i64> = Vec::new();
        let mut deleted_row_keys: Vec<Box<[u8]>> = Vec::new();
        let mut deleted_inlined_pk_i64: Vec<i64> = Vec::new();
        let mut deleted_inlined_row_keys: Vec<Box<[u8]>> = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let has_null = pk_columns.iter().any(|col| col.is_null(row_idx));
            if has_null {
                return Err(Error::DataValidation {
                    table: self.table_metadata.table_name.clone(),
                    message: "Primary key values must be non-null".to_string(),
                });
            }

            let key = rows.row(row_idx).owned();
            if ctx.incoming_keys.contains(&key) {
                return Err(Error::DataValidation {
                    table: self.table_metadata.table_name.clone(),
                    message: "Incoming data contains duplicate primary key across batches"
                        .to_string(),
                });
            }

            if let Some(existing) = ctx.existing_keys.get(&key) {
                match ctx.on_conflict {
                    OnConflict::DoNothingAll | OnConflict::DoNothing(_) => {
                        keep_mask.push(false);
                    }
                    OnConflict::Upsert(_) => {
                        let is_inlined_conflict = existing.source == RowSource::Inlined;
                        match &self.pk_deletion_strategy {
                            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                                if let Some(arr) = int64_pk_array {
                                    if is_inlined_conflict {
                                        deleted_inlined_pk_i64.push(arr.value(row_idx));
                                    } else {
                                        deleted_pk_i64.push(arr.value(row_idx));
                                    }
                                }
                            }
                            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                                let row_key = key.as_ref().to_vec().into_boxed_slice();
                                if is_inlined_conflict {
                                    deleted_inlined_row_keys.push(row_key);
                                } else {
                                    deleted_row_keys.push(row_key);
                                }
                            }
                            PkDeletionStrategyWithCache::PositionBased { .. } => {
                                // Position-based doesn't need PK values
                            }
                        }

                        if !is_inlined_conflict {
                            delete_specs
                                .entry(existing.data_file_id)
                                .or_default()
                                .push(existing.row_id);
                        }

                        ctx.existing_keys.insert(
                            key.clone(),
                            RowLocation {
                                source: RowSource::Inlined,
                                data_file_id: DEFAULT_DATA_FILE_ID,
                                row_id: -1,
                            },
                        );
                        keep_mask.push(true);
                    }
                }
            } else {
                keep_mask.push(true);
            }

            row_keys.push(key);
        }

        if !ctx.upsert_options.is_default() {
            let mut seen: HashMap<OwnedRow, usize> = HashMap::new();
            for (row_idx, key) in row_keys.iter().enumerate() {
                if !keep_mask[row_idx] {
                    continue;
                }

                if let Some(existing_idx) = seen.get(key) {
                    if ctx.upsert_options.last_write_wins {
                        keep_mask[*existing_idx] = false;
                        seen.insert(key.clone(), row_idx);
                    } else if ctx.upsert_options.remove_duplicates {
                        keep_mask[row_idx] = false;
                    } else {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Duplicate primary key found in batch".to_string(),
                        });
                    }
                } else {
                    seen.insert(key.clone(), row_idx);
                }
            }
        }

        let (filtered_batch, kept_keys) =
            Self::filter_validated_batch(batch, keep_mask, &row_keys)?;

        Ok(BatchValidationResult {
            filtered_batch,
            delete_specs: delete_specs.into_iter().collect(),
            kept_keys,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        })
    }

    fn filter_validated_batch(
        batch: RecordBatch,
        keep_mask: Vec<bool>,
        row_keys: &[OwnedRow],
    ) -> Result<(Option<RecordBatch>, HashSet<OwnedRow>)> {
        if keep_mask.iter().all(|v| !*v) {
            return Ok((None, HashSet::new()));
        }

        let kept_keys: HashSet<OwnedRow> = row_keys
            .iter()
            .zip(&keep_mask)
            .filter(|(_, keep)| **keep)
            .map(|(key, _)| key.clone())
            .collect();

        if keep_mask.iter().all(|v| *v) {
            return Ok((Some(batch), kept_keys));
        }

        let filter_array = arrow::array::BooleanArray::from(keep_mask);
        let filtered_batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;

        Ok((Some(filtered_batch), kept_keys))
    }

    fn adjust_cached_inlined_row_count(&self, delta: i64) {
        let _ =
            self.inlined_row_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(if delta >= 0 {
                        current.saturating_add(delta)
                    } else {
                        current.saturating_sub(delta.saturating_abs())
                    })
                });
    }

    fn rewritten_inlined_data_entry(
        source: &InlinedData,
        batches: &[RecordBatch],
        record_count: usize,
    ) -> Result<InlinedData> {
        let data_ipc = serialize_batches_to_ipc(batches)?;

        Ok(InlinedData {
            inlined_id: source.inlined_id.clone(),
            table_id: source.table_id.clone(),
            partition_key: source.partition_key.clone(),
            data_ipc,
            record_count: i64::try_from(record_count).unwrap_or(i64::MAX),
            sequence_number: source.sequence_number,
            created_at: source.created_at.clone(),
        })
    }

    fn filter_inlined_batch_for_pk_deletions(
        &self,
        batch: RecordBatch,
        deleted_pk_i64: &HashSet<i64>,
        deleted_row_keys: &HashSet<Box<[u8]>>,
    ) -> Result<(Option<RecordBatch>, usize)> {
        if batch.num_rows() == 0 {
            return Ok((None, 0));
        }

        let pk_indices = &self.pk_column_indices;
        if pk_indices.is_empty() {
            return Ok((Some(batch), 0));
        }

        let mut keep_mask = Vec::with_capacity(batch.num_rows());
        let mut removed_rows = 0_usize;

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                if deleted_pk_i64.is_empty() {
                    return Ok((Some(batch), 0));
                }

                let pk_array = batch
                    .column(pk_indices[0])
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| Error::DataValidation {
                        table: self.table_metadata.table_name.clone(),
                        message: "Int64 primary key column has unexpected type".to_string(),
                    })?;

                for row_index in 0..batch.num_rows() {
                    if pk_array.is_null(row_index) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let should_delete = deleted_pk_i64.contains(&pk_array.value(row_index));
                    keep_mask.push(!should_delete);
                    removed_rows += usize::from(should_delete);
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                if deleted_row_keys.is_empty() {
                    return Ok((Some(batch), 0));
                }

                let converter = self.build_pk_converter(pk_indices)?;
                let pk_columns: Vec<_> = pk_indices
                    .iter()
                    .map(|idx| Arc::clone(batch.column(*idx)))
                    .collect();
                let rows = converter.convert_columns(&pk_columns)?;

                for row_index in 0..batch.num_rows() {
                    if pk_columns.iter().any(|column| column.is_null(row_index)) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let should_delete = deleted_row_keys.contains(rows.row(row_index).as_ref());
                    keep_mask.push(!should_delete);
                    removed_rows += usize::from(should_delete);
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => return Ok((Some(batch), 0)),
        }

        if removed_rows == 0 {
            return Ok((Some(batch), 0));
        }
        if removed_rows == batch.num_rows() {
            return Ok((None, removed_rows));
        }

        let filter_array = arrow::array::BooleanArray::from(keep_mask);
        let filtered_batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;
        Ok((Some(filtered_batch), removed_rows))
    }

    async fn build_inlined_data_rewrite_for_pk_keys(
        &self,
        deleted_pk_i64: &[i64],
        deleted_row_keys: &[Box<[u8]>],
    ) -> Result<InlinedDataRewrite> {
        let deleted_pk_i64: HashSet<i64> = deleted_pk_i64.iter().copied().collect();
        let deleted_row_keys: HashSet<Box<[u8]>> = deleted_row_keys.iter().cloned().collect();
        if deleted_pk_i64.is_empty() && deleted_row_keys.is_empty() {
            return Ok(InlinedDataRewrite::default());
        }

        let inlined_data = self
            .catalog
            .get_inlined_data(&self.table_metadata.table_id)
            .await?;
        if inlined_data.is_empty() {
            return Ok(InlinedDataRewrite::default());
        }

        let legacy_inlined_deletions = self.load_inlined_deletion_maps().await?;
        let mut rewrite = InlinedDataRewrite::default();

        for entry in inlined_data {
            let batches = deserialize_ipc_to_batch(&entry.data_ipc)?;
            let mut rewritten_batches = Vec::with_capacity(batches.len());
            let mut original_rows = 0_usize;
            let mut remaining_rows = 0_usize;
            let mut entry_removed_rows = 0_usize;

            for batch in batches {
                original_rows += batch.num_rows();
                let Some(visible_batch) = self.filter_inlined_batch_for_deletions(
                    batch,
                    entry.sequence_number,
                    &legacy_inlined_deletions,
                )?
                else {
                    continue;
                };
                let (filtered_batch, removed_rows) = self.filter_inlined_batch_for_pk_deletions(
                    visible_batch,
                    &deleted_pk_i64,
                    &deleted_row_keys,
                )?;
                entry_removed_rows += removed_rows;
                if let Some(batch) = filtered_batch {
                    remaining_rows += batch.num_rows();
                    rewritten_batches.push(batch);
                }
            }

            if entry_removed_rows == 0 {
                continue;
            }

            rewrite.removed_rows += original_rows.saturating_sub(remaining_rows);
            if remaining_rows == 0 {
                rewrite.deleted_inlined_ids.push(entry.inlined_id);
            } else {
                rewrite
                    .updated_data
                    .push(Self::rewritten_inlined_data_entry(
                        &entry,
                        &rewritten_batches,
                        remaining_rows,
                    )?);
            }
        }

        Ok(rewrite)
    }

    async fn commit_inlined_data_mutation(
        &self,
        rewrite: InlinedDataRewrite,
        data: Vec<InlinedData>,
        appended_rows: usize,
    ) -> CatalogResult<()> {
        if rewrite.is_empty() && data.is_empty() {
            return Ok(());
        }

        let removed_rows = rewrite.removed_rows;
        self.catalog
            .commit_inlined_mutation(
                &self.table_metadata.table_id,
                rewrite.updated_data,
                rewrite.deleted_inlined_ids,
                data,
            )
            .await?;

        let appended_rows = i64::try_from(appended_rows).unwrap_or(i64::MAX);
        let removed_rows = i64::try_from(removed_rows).unwrap_or(i64::MAX);
        self.adjust_cached_inlined_row_count(appended_rows.saturating_sub(removed_rows));

        Ok(())
    }

    /// Apply deletion vectors generated by on-conflict (upsert) handling.
    ///
    /// Not supported for Position-based tables (no PK) that doesn't support upserts
    ///
    /// This function:
    /// 1. Writes deletion vectors for the deleted PKs
    /// 2. Updates the appropriate in-memory cache based on `pk_deletion_strategy`:
    ///    - `Int64Pk`: Updates `cached_deleted_pk_i64` AND `cached_insert_records_pk_i64`
    ///    - `RowConverterBased`: Updates `cached_deleted_row_keys` AND `cached_insert_records_row_keys`
    ///
    /// For upsert operations, we track both the deletion (with `delete_sequence`) and the
    /// re-insertion (with `insert_sequence` = `delete_sequence` + 1) so that the new row
    /// isn't filtered out by the deletion filter during scans.
    ///
    /// Following Iceberg's sequence-based ordering model where deletes are tracked by
    /// PK value + sequence number for proper ordering of concurrent operations.
    pub(crate) async fn apply_on_conflict_deletions(
        &self,
        on_conflict_deletions: OnConflictDeletions,
    ) -> CatalogResult<()> {
        let OnConflictDeletions {
            delete_specs,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        } = on_conflict_deletions;

        let has_file_deletions = !delete_specs.is_empty();
        let has_inlined_deletions =
            !deleted_inlined_pk_i64.is_empty() || !deleted_inlined_row_keys.is_empty();

        if !has_file_deletions && !has_inlined_deletions {
            return Ok(());
        }

        let inlined_rewrite = if has_inlined_deletions {
            self.build_inlined_data_rewrite_for_pk_keys(
                &deleted_inlined_pk_i64,
                &deleted_inlined_row_keys,
            )
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Failed to rewrite inlined data for upserted PKs: {err}"),
            })?
        } else {
            InlinedDataRewrite::default()
        };

        if !inlined_rewrite.is_empty() {
            let removed_rows = inlined_rewrite.removed_rows;
            self.commit_inlined_data_mutation(inlined_rewrite, vec![], 0)
                .await?;

            tracing::debug!(
                "Removed {} inlined row(s) for table {} during upsert rewrite",
                removed_rows,
                self.table_metadata.table_name,
            );
        }

        if !has_file_deletions {
            return Ok(());
        }

        // Get a fresh sequence number for this deletion operation.
        // This ensures proper ordering: data written after this delete but before
        // the next delete will be properly filtered.
        let delete_sequence = self
            .catalog
            .increment_sequence_number(&self.table_metadata.table_id)
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Failed to get delete sequence number: {err}"),
            })?;

        // The insert sequence must be higher than delete sequence so the new row
        // isn't filtered out. We use delete_sequence + 1 for the re-insertion.
        let insert_sequence = delete_sequence + 1;

        // Create a temporary metadata with the fresh delete sequence number.
        // The table_metadata's current_sequence_number is stale (set at table open time),
        // so we must use the actual delete_sequence from increment_sequence_number().
        let mut temp_metadata = self.table_metadata.clone();
        temp_metadata.current_sequence_number = delete_sequence;
        let writer = DeletionVectorWriter::new(&temp_metadata);

        // For on-conflict (upsert) handling, use key-based deletion vectors.
        // Position-based tables don't support upserts, so we always use row keys here.
        // Build the row keys based on the deletion strategy:
        // - Int64Pk: Convert i64 values to 8-byte big-endian representations
        // - RowConverterBased: Use the provided row keys directly
        let row_keys_for_deletion: Vec<Box<[u8]>> = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => deleted_pk_i64
                .iter()
                .map(|&pk| pk.to_be_bytes().to_vec().into_boxed_slice())
                .collect(),
            PkDeletionStrategyWithCache::RowConverterBased { .. } => deleted_row_keys.clone(),
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based tables don't support upserts
                vec![]
            }
        };

        let pk_bytes_list_for_insert_records: Vec<Vec<u8>> = row_keys_for_deletion
            .iter()
            .map(|key| key.as_ref().to_vec())
            .collect();

        let specs = if row_keys_for_deletion.is_empty() {
            vec![]
        } else {
            vec![DeletionVectorWriteSpec::new_key_based(
                row_keys_for_deletion,
            )]
        };

        let results = writer.write(specs).await?;

        if results.is_empty() {
            return Ok(());
        }

        let mut new_deleted_rows = RoaringBitmap::new();
        // Register new delete files
        for result in &results {
            self.catalog
                .add_delete_file(result.delete_file.clone())
                .await
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!("Failed to register delete file: {err}"),
                })?;

            if let DeletionIdentifier::PositionBased { row_ids, .. } = &result.identifiers {
                for &row_id in row_ids {
                    if let Ok(row_id_u32) = u32::try_from(row_id) {
                        new_deleted_rows.insert(row_id_u32);
                    }
                }
            }
        }

        // Persist insert records for reinserted PKs so sequence-based visibility survives restart.
        // Without this, caches may allow reinserted rows in-process, but after restart the
        // catalog reload would miss insert sequences and incorrectly hide rows as deleted.
        if !pk_bytes_list_for_insert_records.is_empty() {
            self.catalog
                .add_insert_records_batch(
                    &self.table_metadata.table_id,
                    pk_bytes_list_for_insert_records,
                    insert_sequence,
                )
                .await
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!("Failed to persist insert records for upserted PKs: {err}"),
                })?;
        }

        // For PK-based strategies, keep old delete files to preserve deletion history.
        // Each upsert round may affect a different subset of PKs, so removing old files
        // would lose deletion records for PKs not in the current round.

        // Update the appropriate cache based on deletion strategy.
        // This follows Iceberg's pattern where deletes are tracked by PK + sequence number.
        // For upserts, we also update insert records so the new row isn't filtered out.
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk,
                cached_insert_records,
            } => {
                // Build new deletion + insert snapshots and publish atomically.
                // Writers are serialised by the per-table write lock so the load+rebuild+store
                // sequence is race-free.
                let updated_deleted = cached_deleted_pk
                    .load()
                    .extend_max(deleted_pk_i64.iter().map(|&pk| (pk, delete_sequence)));
                let deleted_count = updated_deleted.len();
                cached_deleted_pk.store(Arc::new(updated_deleted));

                tracing::debug!(
                    "Updated Int64 PK deletion cache with {} keys (seq={}) for table {}",
                    deleted_count,
                    delete_sequence,
                    self.table_metadata.table_name
                );

                let updated_inserts = cached_insert_records
                    .load()
                    .extend_max(deleted_pk_i64.into_iter().map(|pk| (pk, insert_sequence)));
                let insert_count = updated_inserts.len();
                cached_insert_records.store(Arc::new(updated_inserts));

                tracing::debug!(
                    "Updated Int64 PK insert records cache with {} keys (seq={}) for table {}",
                    insert_count,
                    insert_sequence,
                    self.table_metadata.table_name
                );
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                let updated_deleted = cached_deleted_row_keys.load().extend_max(
                    deleted_row_keys
                        .iter()
                        .map(|key| (key.clone(), delete_sequence)),
                );
                let deleted_count = updated_deleted.len();
                cached_deleted_row_keys.store(Arc::new(updated_deleted));

                tracing::debug!(
                    "Updated RowConverter deletion cache with {} keys (seq={}) for table {}",
                    deleted_count,
                    delete_sequence,
                    self.table_metadata.table_name
                );

                let updated_inserts = cached_insert_records.load().extend_max(
                    deleted_row_keys
                        .into_iter()
                        .map(|key| (key, insert_sequence)),
                );
                let insert_count = updated_inserts.len();
                cached_insert_records.store(Arc::new(updated_inserts));

                tracing::debug!(
                    "Updated RowConverter insert records cache with {} keys (seq={}) for table {}",
                    insert_count,
                    insert_sequence,
                    self.table_metadata.table_name
                );
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // This branch should never be reached - position-based tables don't have PKs
                // and don't support upserts.
                unreachable!(
                    "apply_on_conflict_deletions called for position-based strategy on table {}",
                    self.table_metadata.table_name
                );
            }
        }

        Ok(())
    }

    /// Sort a record batch stream using `DataFusion`'s `SortExec` for optimal performance.
    ///
    /// This is used during refresh operations to sort the **entire refresh corpus** before it's
    /// chunked and written to files, ensuring optimal zone map statistics across all Vortex files.
    ///
    /// # External Sort with Disk Spilling
    ///
    /// Uses `DataFusion`'s `SortExec` which provides:
    /// - **Automatic disk spilling**: Handles datasets larger than available memory
    /// - **Streaming external merge sort**: Processes data incrementally without loading all into RAM
    /// - **SIMD-optimized kernels**: Hardware-accelerated sorting (NEON on arm64, AVX2/AVX-512 on amd64)
    /// - **Configurable spill compression**: Supports zstd, `lz4_frame`, or uncompressed spill files
    /// - **Memory management**: Integrates with `DataFusion`'s memory pool and reservation system
    ///
    /// # Configuration
    ///
    /// Spill behavior is controlled by runtime configuration:
    /// - `sort_spill_reservation_bytes`: Memory reserved for merge operations (default: 10MB)
    /// - `sort_in_place_threshold_bytes`: Size below which data is sorted in-place (default: 1MB)
    /// - `spill_compression`: Compression codec for spill files (uncompressed, `lz4_frame`, zstd)
    /// - `temp_directory`: Directory for spill files (configured in runtime)
    ///
    /// # Performance
    ///
    /// - Small datasets (<1MB): Sorted in-place in memory, no allocations
    /// - Medium datasets (1MB-available memory): In-memory sort with single merge
    /// - Large datasets (>available memory): External merge sort with disk spilling
    /// - All cases use SIMD-optimized Arrow kernels and parallel sorting via rayon
    ///
    /// # Errors
    ///
    /// Returns an error if sorting fails or if configured sort columns don't exist.
    fn sort_stream(&self, stream: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        use datafusion_execution::TaskContext;

        // Create a task context with default memory pool and runtime settings
        // This will use the configured spill directory and compression settings
        let task_ctx = Arc::new(TaskContext::default());

        tracing::debug!(
            "Sorting refresh data by columns {:?} for table {} using DataFusion SortExec with disk spilling support",
            self.context.sort_columns(),
            self.table_metadata.table_name
        );

        // Use the common stream sorting utility
        let sorted_stream =
            util::stream_utils::sort_stream(stream, self.context.sort_columns(), &task_ctx)?;

        Ok(sorted_stream)
    }

    /// Sort and rewrite data by reading from the current listing table, writing
    /// sorted data to a new snapshot, and atomically swapping.
    ///
    /// This method:
    /// 1. Reads all data from the current listing table
    /// 2. Sorts the data using `DataFusion`'s `SortExec` (with disk spilling)
    /// 3. Writes sorted data to a **new** snapshot directory (avoids deleting
    ///    files that the lazy `SortExec` stream still needs to read)
    /// 4. Atomically commits the new snapshot in the catalog
    /// 5. Updates in-memory state and triggers old snapshot cleanup
    ///
    /// This ensures zone maps have non-overlapping min/max ranges for optimal pruning.
    ///
    /// # Errors
    ///
    /// Returns an error if reading, sorting, or rewriting fails.
    pub async fn sort_and_rewrite_data(&self, target_size_bytes: usize) -> Result<()> {
        tracing::info!(
            "Sorting and rewriting data for table {} by columns {:?}",
            self.table_metadata.table_name,
            self.context.sort_columns()
        );

        // Read all data from the current listing table
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| Error::LockPoisoned {
                table: self.table_metadata.table_name.clone(),
                lock: LISTING_TABLE_LOCK_POISONED,
            })?;
            Arc::clone(&*guard)
        };

        // Create a session context and scan the listing table to get all data
        let ctx = self.create_session_context();
        let df = ctx.read_table(listing_table)?;

        // Get the data as a stream
        let stream = df.execute_stream().await?;

        // Sort the stream using our existing sort logic
        let sorted_stream = self.sort_stream(stream)?;

        // Write sorted data to a new snapshot directory. Because SortExec lazily
        // reads input files via DataSourceExec, writing to a separate directory
        // avoids the need to either:
        //  - delete old files first (which would break the lazy read), or
        //  - collect all sorted data into memory before writing
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        let cleanup_failed_snapshot = async {
            if is_s3 {
                let snapshot_url = Self::snapshot_dir_url(
                    &self.table_metadata.path,
                    &self.table_metadata.table_id,
                    &new_snapshot_id,
                );

                match url::Url::parse(&snapshot_url) {
                    Ok(url) => {
                        let Some(config) = self.object_store_config.as_ref() else {
                            tracing::warn!(
                                "Skipping failed sort-rewrite S3 cleanup for table {} because object_store_config is missing",
                                self.table_metadata.table_name
                            );
                            return;
                        };

                        let snapshot_host = url.host_str().unwrap_or_default();
                        let config_host = config.url.host_str().unwrap_or_default();
                        if !snapshot_host.is_empty()
                            && !config_host.is_empty()
                            && snapshot_host != config_host
                        {
                            tracing::warn!(
                                "Skipping failed sort-rewrite S3 cleanup for table {} because snapshot host {} does not match configured object store host {}",
                                self.table_metadata.table_name,
                                snapshot_host,
                                config_host
                            );
                            return;
                        }

                        let path = url.path().trim_start_matches('/');
                        let prefix = ObjectStorePath::from(path);
                        if let Err(e) = self.delete_prefix_with_object_store(&prefix).await {
                            tracing::warn!(
                                "Failed to clean up failed sort-rewrite snapshot {} for table {}: {e}",
                                new_snapshot_id,
                                self.table_metadata.table_name
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to parse snapshot URL for failed sort-rewrite cleanup {} on table {}: {e}",
                            snapshot_url,
                            self.table_metadata.table_name
                        );
                    }
                }
            } else {
                let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
                if let Err(e) = tokio::fs::remove_dir_all(&snapshot_dir).await {
                    tracing::warn!(
                        "Failed to clean up failed sort-rewrite snapshot dir {} for table {}: {e}",
                        snapshot_dir.display(),
                        self.table_metadata.table_name
                    );
                }
            }
        };

        // For local paths, ensure the snapshot directory exists.
        // S3 doesn't require directory creation (object storage creates paths on write).
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        let (total_rows, chunk_count, _stats_acc) = self
            .write_to_snapshot(sorted_stream, target_size_bytes, &new_snapshot_id)
            .await?;

        if total_rows == 0 {
            tracing::debug!(
                "No data to sort-rewrite for table {}",
                self.table_metadata.table_name
            );
            // Clean up empty snapshot directory for local paths
            if !is_s3 {
                let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
                let _ = tokio::fs::remove_dir(&snapshot_dir).await;
            }
            return Ok(());
        }

        // Sync the snapshot directory for durability before committing metadata.
        if !is_s3 {
            let snapshot_dir = self.snapshot_dir_path_for(&new_snapshot_id);
            if let Err(e) = Self::sync_snapshot_dir(&snapshot_dir).await {
                cleanup_failed_snapshot.await;
                return Err(Error::Catalog { source: e });
            }
        }

        // Pre-create the listing table before committing to catalog.
        // This ensures that if listing table creation fails, we haven't committed
        // the catalog yet, avoiding an inconsistent state.
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &new_snapshot_id,
        );
        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        // Atomically update the catalog to point to the new sorted snapshot.
        // commit_compaction clears delete files and insert records, which is
        // correct here since the sort rewrites all live data into the new snapshot.
        if let Err(e) = self.commit_overwrite(&new_snapshot_id).await {
            cleanup_failed_snapshot.await;
            return Err(Error::Catalog { source: e });
        }

        // Now that catalog is committed, update the in-memory listing table.
        {
            let mut listing_table_guard =
                self.listing_table
                    .write()
                    .map_err(|_| Error::LockPoisoned {
                        table: self.table_metadata.table_name.clone(),
                        lock: LISTING_TABLE_LOCK_POISONED,
                    })?;
            *listing_table_guard = new_listing_table;
        }

        // Update in-memory state to match the new catalog
        self.update_current_snapshot_id(&new_snapshot_id)?;

        if let Err(e) = self.clear_all_deletion_caches() {
            tracing::warn!(
                "Failed to clear deletion caches after sort rewrite for table {}: {e}",
                self.table_metadata.table_name
            );
        }

        // Old snapshot directories are cleaned up in the background
        self.trigger_old_snapshot_cleanup(&new_snapshot_id).await;

        tracing::info!(
            "Rewrote {} rows in {} sorted chunk(s) for table {}",
            total_rows,
            chunk_count,
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Create a `SessionContext` for data operations using the shared `RuntimeEnv`.
    ///
    /// The shared `RuntimeEnv` (from [`CayenneContext`]) already has the S3 object
    /// store registered during construction, so all sessions created here inherit
    /// it automatically. This also shares the `list_files` cache and other
    /// runtime-level caches with the main Spice query engine.
    fn create_session_context(&self) -> SessionContext {
        SessionContext::new_with_config_rt(
            SessionConfig::default(),
            Arc::clone(self.context.runtime_env()),
        )
    }

    /// Wrap a plan with a `FilterExec` that enforces the retention filter.
    ///
    /// `ListingTable::scan()` drops non-partition filters — they only influence
    /// the file-limit heuristic, not the actual scan. Adding a `FilterExec`
    /// above `DataSourceExec` allows `DataFusion`'s physical optimizer to push
    /// the predicate into `VortexSource::try_pushdown_filters`, enabling
    /// file-level pruning via min/max stats and row-level filtering.
    fn wrap_plan_with_retention_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        retention_filter: &Expr,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, datafusion_common::DataFusionError> {
        let arrow_schema = plan.schema();
        let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone())?;
        let execution_props = ExecutionProps::new();

        let physical_filter = datafusion_physical_expr::create_physical_expr(
            retention_filter,
            &df_schema,
            &execution_props,
        )?;

        let filter_exec = FilterExec::try_new(physical_filter, plan)?;

        tracing::trace!(
            table = %self.table_metadata.table_name,
            filter = %retention_filter,
            "Applied retention_filter FilterExec at scan time"
        );

        Ok(Arc::new(filter_exec))
    }

    pub(crate) async fn apply_retention_filters(&self) -> CatalogResult<u64> {
        use data_components::delete::DeletionSink;

        if self.retention_filters.is_empty() {
            return Ok(0);
        }

        let filters = self.retention_filters.clone();
        let sink = CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            &filters,
            self.pk_deletion_strategy.clone(),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            Vec::new(), // Retention filters don't need to scan protected snapshots
            Arc::clone(self.context.runtime_env()),
            None, // Already under write_lock from write_all_append
        );

        let deleted_count =
            sink.delete_from()
                .await
                .map_err(|err| CatalogError::InvalidOperation {
                    message: "Failed to execute retention filters.".to_string(),
                    source: err,
                })?;

        // Refresh deletion cache after applying retention filters
        if deleted_count > 0 {
            self.refresh_deletion_cache().await?;
        }

        Ok(deleted_count)
    }

    /// Refresh the cached deletion vectors by reloading from the catalog.
    ///
    /// This should be called after operations that modify deletion vectors:
    /// - After applying retention filters
    /// - After manual delete operations
    /// - After compaction that removes deleted rows
    ///
    /// # Errors
    ///
    /// Returns an error if deletion vectors cannot be loaded from the catalog.
    async fn refresh_deletion_cache(&self) -> CatalogResult<()> {
        let fresh_strategy = Self::load_deletion_vectors_all(
            &self.table_metadata.table_id,
            Arc::clone(&self.catalog),
            self.pk_deletion_strategy.strategy(),
        )
        .await?;

        self.pk_deletion_strategy
            .refresh_from(&fresh_strategy, &self.table_metadata.table_name)?;

        tracing::debug!(
            "Refreshed deletion cache for table {} (strategy: {:?})",
            self.table_metadata.table_name,
            self.pk_deletion_strategy.strategy(),
        );

        Ok(())
    }

    /// Check if there are pending deletions based on the current deletion strategy.
    ///
    /// This is used to determine if inserts need special handling:
    /// - Position-based deletions use per-file deletion vectors (no special handling needed)
    /// - PK-based deletions use anti-deletions (write to new snapshot with higher sequence)
    ///
    pub(crate) fn has_pending_deletions(&self) -> bool {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => !cached_deleted_row_ids.load().is_empty(),
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => !cached_deleted_pk.load().is_empty(),
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => !cached_deleted_row_keys.load().is_empty(),
        }
    }

    /// Returns a reference to the primary key deletion strategy and its caches.
    #[must_use]
    pub(crate) fn pk_deletion_strategy(&self) -> &PkDeletionStrategyWithCache {
        &self.pk_deletion_strategy
    }

    /// Clear all cached deletion vectors and insert records.
    ///
    /// This should be called after compaction operations that have applied all deletions
    /// and written a clean snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the protected snapshots lock is poisoned.
    pub(crate) fn clear_all_deletion_caches(&self) -> Result<()> {
        // Clear caches based on the current strategy.
        // ArcSwap stores publish a fresh empty snapshot atomically; readers see either
        // the old or new state and never block.
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => {
                cached_deleted_row_ids.store(Arc::new(HashMap::new()));
            }
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk,
                cached_insert_records,
            } => {
                cached_deleted_pk.store(Arc::new(DeletionIndex::empty()));
                cached_insert_records.store(Arc::new(DeletionIndex::empty()));
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                cached_deleted_row_keys.store(Arc::new(KeyDeletionIndex::empty()));
                cached_insert_records.store(Arc::new(KeyDeletionIndex::empty()));
            }
        }

        // Clear protected snapshots - after compaction all data is in the main snapshot
        {
            let mut guard = self
                .protected_snapshots
                .write()
                .map_err(|_| Error::LockPoisoned {
                    table: self.table_metadata.table_name.clone(),
                    lock: LISTING_TABLE_LOCK_POISONED,
                })?;
            guard.clear();
        }

        tracing::debug!(
            "Cleared all deletion and insert records caches for table {}",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Get the current snapshot ID.
    ///
    /// This returns the live snapshot ID which may differ from `table_metadata.current_snapshot_id`
    /// after compaction operations.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub(super) fn get_current_snapshot_id(&self) -> Result<String> {
        let guard = self
            .current_snapshot_id
            .read()
            .map_err(|_| Error::LockPoisoned {
                table: self.table_metadata.table_name.clone(),
                lock: LISTING_TABLE_LOCK_POISONED,
            })?;
        Ok(guard.clone())
    }

    /// Update the current snapshot ID after a compaction operation.
    ///
    /// This must be called after `commit_compaction` to keep the in-memory snapshot ID
    /// in sync with the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub(crate) fn update_current_snapshot_id(&self, new_snapshot_id: &str) -> Result<()> {
        let mut guard = self
            .current_snapshot_id
            .write()
            .map_err(|_| Error::LockPoisoned {
                table: self.table_metadata.table_name.clone(),
                lock: LISTING_TABLE_LOCK_POISONED,
            })?;
        *guard = new_snapshot_id.to_string();
        tracing::debug!(
            "Updated current snapshot ID for table {} to {}",
            self.table_metadata.table_name,
            new_snapshot_id
        );
        Ok(())
    }

    /// Refresh in-memory query state by reloading from the catalog (source of truth).
    ///
    /// This keeps existing `Arc<CayenneTableProvider>` handles usable after catalog refreshes
    /// by updating mutable state in place instead of swapping provider objects.
    ///
    /// Acquires the write lock to prevent racing with in-progress writes/deletes. While holding
    /// the lock, reloads deletion vectors, protected snapshots, and the listing table
    /// directly from the catalog — NOT from the `source` provider, which may contain
    /// stale state captured before the lock was acquired.
    ///
    /// The `source` parameter is used to validate that the table ID matches.
    ///
    /// # Errors
    ///
    /// Returns an error if `source` refers to a different table (mismatched table IDs)
    /// or if reloading from the catalog fails.
    pub async fn refresh(&self, source: &Self) -> Result<()> {
        if self.table_metadata.table_id != source.table_metadata.table_id {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!(
                    "Cannot refresh table {} from different table {}",
                    self.table_metadata.table_id, source.table_metadata.table_id,
                ),
            });
        }

        // Acquire the write lock so no insert/delete is in-flight while we reload state.
        let _write_guard = self.write_lock.lock().await;

        // Reload deletion vectors from the catalog (SQLite) — the source of truth.
        // This picks up any deletions committed by writes that completed after the
        // source provider was opened.
        let fresh_strategy = Self::load_deletion_vectors_all(
            &self.table_metadata.table_id,
            Arc::clone(&self.catalog),
            self.pk_deletion_strategy.strategy(),
        )
        .await
        .map_err(|e| Error::Internal {
            table: self.table_metadata.table_name.clone(),
            message: format!("Failed to reload deletion vectors during refresh: {e}"),
        })?;

        self.pk_deletion_strategy
            .refresh_from(&fresh_strategy, &self.table_metadata.table_name)?;

        // Reload protected snapshots from the catalog.
        let fresh_protected_snapshots = Self::load_protected_snapshots(
            Arc::clone(&self.catalog),
            &self.table_metadata.table_id,
            &self.pk_deletion_strategy,
        )
        .await
        .map_err(|e| Error::Internal {
            table: self.table_metadata.table_name.clone(),
            message: format!("Failed to reload protected snapshots during refresh: {e}"),
        })?;

        {
            let mut guard = self
                .protected_snapshots
                .write()
                .map_err(|_| Error::LockPoisoned {
                    table: self.table_metadata.table_name.clone(),
                    lock: PROTECTED_SNAPSHOTS_LOCK_POISONED,
                })?;
            *guard = fresh_protected_snapshots;
        }

        // Reload the current snapshot ID from the catalog.
        let fresh_metadata = self
            .catalog
            .get_table(&self.table_metadata.table_name)
            .await
            .map_err(|e| Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!("Failed to reload table metadata during refresh: {e}"),
            })?;
        self.update_current_snapshot_id(&fresh_metadata.current_snapshot_id)?;

        // Rebuild the listing table from the fresh snapshot ID on disk.
        self.refresh_listing_table()?;

        tracing::debug!(
            "Refreshed in-memory state for table {} from catalog",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Delete rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn delete_by_primary_key(&self, _key_values: Vec<Vec<u8>>) -> Result<u64> {
        // Implementation would:
        // 1. Scan data files for matching primary keys
        // 2. Create/update deletion vectors
        // 3. Write deletion vector files
        // 4. Add delete file entries to catalog
        // 5. Return number of rows deleted
        Err(Error::Unsupported {
            operation: "delete_by_primary_key",
        })
    }

    /// Update rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn update_by_primary_key(
        &self,
        _key_values: Vec<Vec<u8>>,
        _new_values: Vec<arrow::array::RecordBatch>,
    ) -> Result<u64> {
        // Implementation would:
        // 1. Delete old rows using deletion vectors
        // 2. Insert new rows
        // 3. Return number of rows updated
        Err(Error::Unsupported {
            operation: "update_by_primary_key",
        })
    }

    /// Refresh the underlying `ListingTable` to pick up new files and update statistics.
    ///
    /// This method should be called after insert operations to ensure that:
    /// - The `ListingTable` discovers newly written Vortex files
    /// - Table statistics (row counts, column stats) are updated and aggregated across all files
    /// - Query plans can use fresh statistics for optimization (partition pruning, filter pushdown)
    ///
    /// # Statistics Handling
    ///
    /// Vortex automatically computes column statistics (min, max, `null_count`, `distinct_count`) when
    /// writing files. These statistics are embedded in Vortex file footers. The `ListingTable`
    /// aggregates these statistics across all files to provide table-level statistics to `DataFusion`'s
    /// query optimizer.
    ///
    /// When `sort_columns` is configured, sorted data produces tighter min/max bounds, making
    /// zone map pruning more effective for range queries.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be refreshed.
    pub(crate) fn refresh_listing_table(&self) -> Result<()> {
        // Construct URL to current snapshot using the live snapshot ID
        // (which may differ from table_metadata after compaction)
        let current_snapshot = self.get_current_snapshot_id()?;
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &current_snapshot,
        );

        // Invalidate the list-files cache for the snapshot directory so the next
        // scan discovers newly written files
        Self::invalidate_list_files_cache(self.context.runtime_env(), &snapshot_dir_url);

        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        // Update the listing table with write lock
        let mut guard = self
            .listing_table
            .write()
            .map_err(|_| Error::LockPoisoned {
                table: self.table_metadata.table_name.clone(),
                lock: LISTING_TABLE_LOCK_POISONED,
            })?;
        *guard = new_listing_table;

        tracing::debug!(
            "Refreshed listing table for {} to pick up new files and update statistics",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Invalidate the `list_files_cache` entry for the given snapshot directory URL.
    ///
    /// `DataFusion`'s `ListingTableUrl` caches directory listings in the `RuntimeEnv`'s
    /// `CacheManager` with infinite TTL. After files are added or removed from a
    /// snapshot directory, the stale cache entry must be evicted so the next scan
    /// lists files fresh from the filesystem / object store.
    pub(crate) fn invalidate_list_files_cache(
        runtime_env: &Arc<RuntimeEnv>,
        snapshot_dir_url: &str,
    ) {
        let Some(cache) = runtime_env.cache_manager.get_list_files_cache() else {
            return;
        };

        // Parse the URL the same way `ListingTableUrl::parse` does to derive
        // the `object_store::path::Path` prefix used as the cache key.
        let Ok(table_url) = ListingTableUrl::parse(snapshot_dir_url) else {
            tracing::warn!(
                "Failed to parse snapshot URL for cache invalidation: {snapshot_dir_url}"
            );
            return;
        };

        let key = TableScopedPath {
            table: None,
            path: table_url.prefix().clone(),
        };

        if cache.remove(&key).is_some() {
            tracing::debug!("Invalidated list-files cache for {snapshot_dir_url}");
        }
    }

    /// Persist table-level statistics by merging the current write with the
    /// existing metastore aggregate when possible.
    ///
    /// Best-effort: logs a warning and continues if stats persistence fails,
    /// since stats are an optimization and not critical for correctness.
    pub(crate) async fn persist_table_stats(&self, accumulator: &ColumnStatsAccumulator) {
        let Some((new_blob, new_rows)) = accumulator.to_file_statistics_blob_with_row_count()
        else {
            return;
        };

        let existing_stats = match self
            .catalog
            .get_table_statistics(&self.table_metadata.table_id)
            .await
        {
            Ok(stats) => stats,
            Err(e) => {
                tracing::warn!(
                    "Failed to load existing table stats for {} before merge: {e}",
                    self.table_metadata.table_name
                );
                None
            }
        };

        let (statistics_blob, num_rows) = if let Some(existing) = existing_stats {
            let Some(merged_blob) =
                accumulator.merged_file_statistics_blob(&existing.statistics_blob)
            else {
                tracing::warn!(
                    "Failed to merge table stats for {}; leaving previous aggregate stats unchanged",
                    self.table_metadata.table_name
                );
                return;
            };
            (merged_blob, existing.num_rows.saturating_add(new_rows))
        } else {
            (new_blob, new_rows)
        };

        let stats = TableStatistics {
            table_id: self.table_metadata.table_id.clone(),
            statistics_blob,
            num_rows,
        };

        if let Err(e) = self.catalog.upsert_table_statistics(&stats).await {
            tracing::warn!(
                "Failed to persist table stats for {}: {e}",
                self.table_metadata.table_name
            );
        }
    }

    /// Write small batches directly to the metastore, optionally atomically
    /// rewriting inline rows they replace.
    pub(crate) async fn try_inline_batches_with_inlined_deletions(
        &self,
        batches: &[RecordBatch],
        deleted_inlined_pk_i64: &[i64],
        deleted_inlined_row_keys: &[Box<[u8]>],
    ) -> Result<bool> {
        let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if total_rows == 0 {
            return Ok(true); // nothing to write
        }
        if total_rows > INLINE_MAX_ROWS {
            return Ok(false);
        }
        let ipc_bytes =
            serialize_batches_to_ipc(batches).map_err(|e| Error::Arrow { source: e })?;
        if ipc_bytes.len() > INLINE_MAX_BYTES {
            return Ok(false);
        }

        let rewrite = self
            .build_inlined_data_rewrite_for_pk_keys(
                deleted_inlined_pk_i64,
                deleted_inlined_row_keys,
            )
            .await?;
        let removed_rows = rewrite.removed_rows;

        self.commit_inlined_data_mutation(
            rewrite,
            vec![InlinedData::pending_catalog_insert(
                self.table_metadata.table_id.clone(),
                None,
                ipc_bytes,
                i64::try_from(total_rows).unwrap_or(i64::MAX),
            )],
            total_rows,
        )
        .await?;

        tracing::debug!(
            "Inlined {} rows for table {} after removing {} replaced inline row(s)",
            total_rows,
            self.table_metadata.table_name,
            removed_rows,
        );

        Ok(true)
    }

    #[must_use]
    pub(crate) fn cached_inlined_row_count(&self) -> i64 {
        self.inlined_row_count.load(Ordering::Relaxed)
    }

    /// Read visible inlined data for this table and return as `RecordBatch`es.
    ///
    /// Used at scan time to union inlined data with the file-based data. For
    /// primary-key tables this still honors legacy metastore-inlined delete
    /// markers, while new inline mutations rewrite `cayenne_inlined_data` rows
    /// directly.
    pub(crate) async fn read_inlined_batches(&self) -> Result<Vec<RecordBatch>> {
        let inlined = self
            .catalog
            .get_inlined_data(&self.table_metadata.table_id)
            .await?;

        if inlined.is_empty() {
            return Ok(Vec::new());
        }

        let inlined_deletions = self.load_inlined_deletion_maps().await?;
        let mut batches = Vec::new();
        for entry in &inlined {
            let entry_batches = deserialize_ipc_to_batch(&entry.data_ipc)
                .map_err(|e| super::Error::Arrow { source: e })?;
            for batch in entry_batches {
                if let Some(filtered) = self.filter_inlined_batch_for_deletions(
                    batch,
                    entry.sequence_number,
                    &inlined_deletions,
                )? {
                    batches.push(filtered);
                }
            }
        }

        Ok(batches)
    }

    async fn load_inlined_deletion_maps(&self) -> Result<InlinedDeletionMaps> {
        if self.pk_deletion_strategy.is_position_based() {
            return Ok(InlinedDeletionMaps::default());
        }

        let inlined_deletes = self
            .catalog
            .get_inlined_deletes(&self.table_metadata.table_id)
            .await?;

        let mut maps = InlinedDeletionMaps::default();
        for delete in inlined_deletes {
            let row_keys = deserialize_delete_keys_from_ipc(&delete.delete_ipc)
                .map_err(|e| super::Error::Arrow { source: e })?;
            for row_key in row_keys {
                if self.pk_deletion_strategy.is_int64_pk() {
                    let pk = Self::row_key_to_i64(&row_key, &self.table_metadata.table_name)?;
                    maps.int64_pk
                        .entry(pk)
                        .and_modify(|sequence| {
                            *sequence = (*sequence).max(delete.sequence_number);
                        })
                        .or_insert(delete.sequence_number);
                } else {
                    maps.row_keys
                        .entry(row_key)
                        .and_modify(|sequence| *sequence = (*sequence).max(delete.sequence_number))
                        .or_insert(delete.sequence_number);
                }
            }
        }

        Ok(maps)
    }

    fn row_key_to_i64(row_key: &[u8], table_name: &str) -> Result<i64> {
        if row_key.len() != 8 {
            return Err(Error::DataValidation {
                table: table_name.to_string(),
                message: format!(
                    "Invalid inlined Int64 delete key length {}; expected 8 bytes",
                    row_key.len()
                ),
            });
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(row_key);
        Ok(i64::from_be_bytes(bytes))
    }

    fn filter_inlined_batch_for_deletions(
        &self,
        batch: RecordBatch,
        data_sequence: i64,
        inlined_deletions: &InlinedDeletionMaps,
    ) -> Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 || self.pk_deletion_strategy.is_position_based() {
            return Ok((batch.num_rows() > 0).then_some(batch));
        }

        let Some(pk_indices) = self.primary_key_indices()? else {
            return Ok(Some(batch));
        };

        let mut keep_mask = Vec::with_capacity(batch.num_rows());
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => {
                let pk_index = *pk_indices.first().ok_or_else(|| Error::Internal {
                    table: self.table_metadata.table_name.clone(),
                    message: "Int64 PK strategy requires a primary key column".to_string(),
                })?;
                let pk_array = batch
                    .column(pk_index)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| Error::Internal {
                        table: self.table_metadata.table_name.clone(),
                        message: format!(
                            "Expected Int64Array for PK column at index {pk_index}, got {:?}",
                            batch.column(pk_index).data_type()
                        ),
                    })?;
                let deleted_pk = cached_deleted_pk.load_full();

                for row_index in 0..batch.num_rows() {
                    if pk_array.is_null(row_index) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let pk = pk_array.value(row_index);
                    let max_delete_sequence = deleted_pk
                        .get(pk)
                        .into_iter()
                        .chain(inlined_deletions.int64_pk.get(&pk).copied())
                        .max();
                    keep_mask.push(
                        max_delete_sequence
                            .is_none_or(|delete_sequence| data_sequence > delete_sequence),
                    );
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => {
                let converter = self.build_pk_converter(&pk_indices)?;
                let pk_columns: Vec<_> = pk_indices
                    .iter()
                    .map(|idx| Arc::clone(batch.column(*idx)))
                    .collect();
                let rows = converter.convert_columns(&pk_columns)?;
                let deleted_row_keys = cached_deleted_row_keys.load_full();

                for row_index in 0..batch.num_rows() {
                    if pk_columns.iter().any(|column| column.is_null(row_index)) {
                        return Err(Error::DataValidation {
                            table: self.table_metadata.table_name.clone(),
                            message: "Primary key values must be non-null".to_string(),
                        });
                    }
                    let row_key = rows.row(row_index);
                    let max_delete_sequence = deleted_row_keys
                        .get(row_key.as_ref())
                        .into_iter()
                        .chain(inlined_deletions.row_keys.get(row_key.as_ref()).copied())
                        .max();
                    keep_mask.push(
                        max_delete_sequence
                            .is_none_or(|delete_sequence| data_sequence > delete_sequence),
                    );
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => unreachable!(
                "Position-based inlined deletion filtering returned before PK handling"
            ),
        }

        if keep_mask.iter().all(|keep| *keep) {
            return Ok(Some(batch));
        }
        if keep_mask.iter().all(|keep| !*keep) {
            return Ok(None);
        }

        let filter = arrow::array::BooleanArray::from(keep_mask);
        Ok(Some(arrow::compute::filter_record_batch(&batch, &filter)?))
    }

    /// Checkpoint: flush all inlined data to a Vortex file and clear from metastore.
    ///
    /// Reads all inlined data entries, concatenates them into a single stream,
    /// writes to Vortex, and clears the inlined data in the metastore.
    pub(crate) async fn checkpoint_inlined_data(&self) -> Result<u64> {
        let batches = self.read_inlined_batches().await?;
        if batches.is_empty() {
            let stats = self
                .catalog
                .get_inlined_data_stats(&self.table_metadata.table_id)
                .await?;
            self.inlined_row_count
                .store(stats.record_count, Ordering::Relaxed);

            if stats.entry_count > 0 {
                tracing::info!(
                    table = %self.table_metadata.table_name,
                    rows = stats.record_count,
                    segments = stats.entry_count,
                    ipc_bytes = stats.ipc_bytes,
                    "Clearing fully-deleted inline memtable"
                );
                self.clear_inlined_metadata_after_checkpoint().await?;
            }

            return Ok(0);
        }

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        tracing::info!(
            "Checkpointing {} inlined rows ({} batches) for table {}",
            total_rows,
            batches.len(),
            self.table_metadata.table_name,
        );

        // Write inlined data through the normal staging path
        let schema = Arc::clone(&self.table_metadata.schema);
        let mem_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[batches],
            Arc::clone(&schema),
            None,
        )?;

        let ctx = self.create_session_context();
        let stream = datafusion_physical_plan::execute_stream(mem_exec, ctx.task_ctx())?;

        let stats = if self.pk_deletion_strategy.is_position_based() {
            let target_size_bytes = self.context.target_file_size_bytes();
            let (_rows, _ops, stats) = self
                .write_to_snapshot(stream, target_size_bytes, &self.get_current_snapshot_id()?)
                .await?;
            stats
        } else {
            let sequence_number = self
                .catalog
                .increment_sequence_number(&self.table_metadata.table_id)
                .await?;
            let (_rows, stats) = self
                .insert_to_new_snapshot_with_sequence(stream, sequence_number)
                .await?;
            stats
        };

        // Persist table stats from the checkpoint write (best-effort; logs on error).
        self.persist_table_stats(&stats).await;

        self.clear_inlined_metadata_after_checkpoint().await?;

        self.refresh_listing_table()?;

        Ok(u64::try_from(total_rows).unwrap_or(u64::MAX))
    }

    async fn clear_inlined_metadata_after_checkpoint(&self) -> Result<()> {
        self.catalog
            .clear_inlined_data(&self.table_metadata.table_id)
            .await?;
        self.catalog
            .clear_inlined_deletes(&self.table_metadata.table_id)
            .await?;
        self.inlined_row_count.store(0, Ordering::Relaxed);
        Ok(())
    }

    /// Flush the inline level-0 memtable when accumulated entries would make reads or
    /// rewrites too expensive.
    pub(crate) async fn checkpoint_inlined_data_if_memtable_pressure_exceeded(&self) -> Result<()> {
        let stats = self
            .catalog
            .get_inlined_data_stats(&self.table_metadata.table_id)
            .await?;
        self.inlined_row_count
            .store(stats.record_count, Ordering::Relaxed);

        let Some(pressure) = inline_memtable_pressure(stats) else {
            return Ok(());
        };

        tracing::info!(
            table = %self.table_metadata.table_name,
            rows = stats.record_count,
            segments = stats.entry_count,
            ipc_bytes = stats.ipc_bytes,
            reason = pressure.as_str(),
            "Checkpointing inline memtable to Vortex"
        );
        self.checkpoint_inlined_data().await?;
        Ok(())
    }

    /// Flush inlined rows to Vortex files when pending inline data exists.
    ///
    /// Callers must hold `write_lock` while calling this helper.
    async fn checkpoint_inlined_data_if_present_for_delete(&self) -> datafusion_common::Result<()> {
        let inlined_count = self.cached_inlined_row_count();

        if inlined_count > 0 {
            self.checkpoint_inlined_data().await.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to checkpoint inlined data before delete: {e}"
                ))
            })?;
        }

        Ok(())
    }

    async fn delete_inlined_rows_matching_filters(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<u64> {
        if self.pk_deletion_strategy.is_position_based() {
            return Ok(0);
        }

        let inlined_data = self
            .catalog
            .get_inlined_data(&self.table_metadata.table_id)
            .await
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read inlined data for delete on table {}: {e}",
                    self.table_metadata.table_name
                ))
            })?;
        if inlined_data.is_empty() {
            return Ok(0);
        }

        let legacy_inlined_deletions = self.load_inlined_deletion_maps().await.map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read inlined delete metadata for delete on table {}: {e}",
                self.table_metadata.table_name
            ))
        })?;

        let coerced_filters = self.coerce_filters_for_inlined_delete(filters)?;
        let physical_filters = self.build_physical_filters_for_inlined_delete(&coerced_filters)?;
        let mut rewrite = InlinedDataRewrite::default();
        let mut matched_deleted_rows = 0_usize;

        for entry in inlined_data {
            let batches = deserialize_ipc_to_batch(&entry.data_ipc)?;
            let mut rewritten_batches = Vec::with_capacity(batches.len());
            let mut original_rows = 0_usize;
            let mut remaining_rows = 0_usize;
            let mut entry_matched_rows = 0_usize;

            for batch in batches {
                original_rows += batch.num_rows();
                let Some(visible_batch) = self
                    .filter_inlined_batch_for_deletions(
                        batch,
                        entry.sequence_number,
                        &legacy_inlined_deletions,
                    )
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to apply inlined delete visibility for table {}: {e}",
                            self.table_metadata.table_name
                        ))
                    })?
                else {
                    continue;
                };

                let filtered_batch =
                    self.apply_inlined_delete_filters(visible_batch.clone(), &physical_filters)?;
                if filtered_batch.num_rows() == 0 {
                    remaining_rows += visible_batch.num_rows();
                    rewritten_batches.push(visible_batch);
                    continue;
                }

                let keys = self.extract_primary_keys_from_batch(&filtered_batch)?;
                let deleted_pk_i64: HashSet<i64> = keys.int64_pk.into_iter().collect();
                let deleted_row_keys: HashSet<Box<[u8]>> = keys.row_keys.into_iter().collect();
                let (filtered_batch, removed_rows) = self
                    .filter_inlined_batch_for_pk_deletions(
                        visible_batch,
                        &deleted_pk_i64,
                        &deleted_row_keys,
                    )
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to rewrite inlined data for delete on table {}: {e}",
                            self.table_metadata.table_name
                        ))
                    })?;
                entry_matched_rows += removed_rows;
                if let Some(batch) = filtered_batch {
                    remaining_rows += batch.num_rows();
                    rewritten_batches.push(batch);
                }
            }

            if entry_matched_rows == 0 {
                continue;
            }

            matched_deleted_rows += entry_matched_rows;
            rewrite.removed_rows += original_rows.saturating_sub(remaining_rows);
            if remaining_rows == 0 {
                rewrite.deleted_inlined_ids.push(entry.inlined_id);
            } else {
                rewrite.updated_data.push(
                    Self::rewritten_inlined_data_entry(&entry, &rewritten_batches, remaining_rows)
                        .map_err(|e| {
                            datafusion_common::DataFusionError::Execution(format!(
                                "Failed to serialize rewritten inlined data for table {}: {e}",
                                self.table_metadata.table_name
                            ))
                        })?,
                );
            }
        }

        if rewrite.is_empty() {
            return Ok(0);
        }

        let deleted_rows = u64::try_from(matched_deleted_rows).map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "Inlined delete row count exceeds u64::MAX".to_string(),
            )
        })?;

        self.commit_inlined_data_mutation(rewrite, vec![], 0)
            .await
            .map_err(|err| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to rewrite inlined data for table {}: {err}",
                    self.table_metadata.table_name
                ))
            })?;

        Ok(deleted_rows)
    }

    fn coerce_filters_for_inlined_delete(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Vec<Expr>> {
        let df_schema = DFSchema::try_from(self.table_metadata.schema.as_ref().clone())?;
        let mut coerced_filters = Vec::with_capacity(filters.len());

        for filter in filters {
            let mut rewriter = TypeCoercionRewriter::new(&df_schema);
            coerced_filters.push(filter.clone().rewrite(&mut rewriter)?.data);
        }

        Ok(coerced_filters)
    }

    fn build_physical_filters_for_inlined_delete(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let df_schema = DFSchema::try_from(self.table_metadata.schema.as_ref().clone())?;
        let execution_props = ExecutionProps::new();

        filters
            .iter()
            .map(|filter| create_physical_expr(filter, &df_schema, &execution_props))
            .collect()
    }

    fn apply_inlined_delete_filters(
        &self,
        mut batch: RecordBatch,
        physical_filters: &[Arc<dyn PhysicalExpr>],
    ) -> datafusion_common::Result<RecordBatch> {
        for filter in physical_filters {
            if batch.num_rows() == 0 {
                break;
            }

            let filter_value = filter.evaluate(&batch)?;
            let filter_array = filter_value.into_array(batch.num_rows())?;
            let filter_array = filter_array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Delete filter for table {} did not evaluate to BooleanArray, got {:?}",
                        self.table_metadata.table_name,
                        filter_array.data_type()
                    ))
                })?;

            batch = arrow::compute::filter_record_batch(&batch, filter_array)?;
        }

        Ok(batch)
    }

    fn extract_primary_keys_from_batch(
        &self,
        batch: &RecordBatch,
    ) -> datafusion_common::Result<ExtractedPrimaryKeys> {
        let Some(pk_indices) = self
            .primary_key_indices()
            .map_err(datafusion_common::DataFusionError::from)?
        else {
            return Ok(ExtractedPrimaryKeys::default());
        };

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                let pk_index = *pk_indices.first().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Int64 PK strategy requires a primary key column".to_string(),
                    )
                })?;
                let pk_array = batch
                    .column(pk_index)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Expected Int64Array for PK column at index {pk_index}, got {:?}",
                            batch.column(pk_index).data_type()
                        ))
                    })?;
                let mut values = Vec::with_capacity(batch.num_rows());
                for row_index in 0..batch.num_rows() {
                    if pk_array.is_null(row_index) {
                        return Err(datafusion_common::DataFusionError::Execution(format!(
                            "Primary key values must be non-null for table {}",
                            self.table_metadata.table_name
                        )));
                    }
                    values.push(pk_array.value(row_index));
                }
                Ok(ExtractedPrimaryKeys {
                    int64_pk: values,
                    row_keys: Vec::new(),
                })
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                let converter = self
                    .pk_row_converter
                    .as_ref()
                    .map_or_else(
                        || self.build_pk_converter(&pk_indices).map(Arc::new),
                        |converter| Ok(Arc::clone(converter)),
                    )
                    .map_err(datafusion_common::DataFusionError::from)?;
                let pk_columns: Vec<_> = pk_indices
                    .iter()
                    .map(|idx| Arc::clone(batch.column(*idx)))
                    .collect();
                let rows = converter.convert_columns(&pk_columns)?;
                let mut row_keys = Vec::with_capacity(batch.num_rows());
                for row_index in 0..batch.num_rows() {
                    if pk_columns.iter().any(|column| column.is_null(row_index)) {
                        return Err(datafusion_common::DataFusionError::Execution(format!(
                            "Primary key values must be non-null for table {}",
                            self.table_metadata.table_name
                        )));
                    }
                    row_keys.push(rows.row(row_index).as_ref().to_vec().into_boxed_slice());
                }
                Ok(ExtractedPrimaryKeys {
                    int64_pk: Vec::new(),
                    row_keys,
                })
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                Ok(ExtractedPrimaryKeys::default())
            }
        }
    }

    /// Load both position-based and key-based deletion vectors from the catalog.
    ///
    /// This method queries the catalog for delete files and loads them into memory,
    /// constructing the appropriate `PkDeletionStrategy` variant with embedded caches:
    /// - `PositionBased`: Cache of `HashMap<String, RoaringBitmap>` (file path -> row positions)
    /// - `Int64Pk`: Cache of `HashMap<i64, i64>` (PK -> max delete sequence) + insert records
    /// - `RowConverterBased`: Cache of `HashMap<Box<[u8]>, i64>` (serialized PK bytes -> max delete sequence) + insert records
    ///
    /// # Returns
    ///
    /// The fully constructed `PkDeletionStrategy` with all caches populated.
    async fn load_deletion_vectors_all(
        table_id: &str,
        catalog: Arc<dyn MetadataCatalog>,
        strategy: PkDeletionStrategy,
    ) -> CatalogResult<PkDeletionStrategyWithCache> {
        use super::delete::detect_deletion_type_and_read;

        // Query catalog for delete files
        let delete_files = catalog
            .get_table_delete_files(table_id)
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to load deletion vectors from catalog.".to_string(),
                source: Box::new(e),
            })?;

        // Load insert records from catalog (only for PK-based strategies)
        let insert_records_bytes = if strategy == PkDeletionStrategy::PositionBased {
            HashMap::new()
        } else {
            catalog.get_insert_records(table_id).await.map_err(|e| {
                CatalogError::InvalidOperation {
                    message: "Failed to load insert records from catalog.".to_string(),
                    source: Box::new(e),
                }
            })?
        };

        // Early return for empty case - construct strategy with empty caches
        if delete_files.is_empty() && insert_records_bytes.is_empty() {
            return Ok(PkDeletionStrategyWithCache::empty_for(strategy));
        }

        // Parse insert records based on strategy
        let (insert_records_pk_i64, insert_records_row_keys) = match strategy {
            PkDeletionStrategy::PositionBased => (HashMap::new(), HashMap::new()),
            PkDeletionStrategy::Int64Pk => {
                // Convert insert record bytes to i64
                let int64_pks: HashMap<i64, i64> = insert_records_bytes
                    .iter()
                    .filter_map(|(bytes, &seq)| {
                        if bytes.len() >= 8 {
                            let mut arr = [0_u8; 8];
                            arr.copy_from_slice(&bytes[..8]);
                            Some((i64::from_be_bytes(arr), seq))
                        } else {
                            tracing::warn!(
                                "Skipping invalid Int64 insert record key with length {} (expected at least 8 bytes)",
                                bytes.len()
                            );
                            None
                        }
                    })
                    .collect();
                (int64_pks, HashMap::new())
            }
            PkDeletionStrategy::RowConverterBased => {
                // Use the byte keys directly
                (HashMap::new(), insert_records_bytes)
            }
        };

        // Early return if only insert records exist (no delete files)
        if delete_files.is_empty() {
            return Ok(match strategy {
                PkDeletionStrategy::PositionBased => {
                    PkDeletionStrategyWithCache::empty_position_based()
                }
                PkDeletionStrategy::Int64Pk => PkDeletionStrategyWithCache::Int64Pk {
                    cached_deleted_pk: Arc::new(ArcSwap::from_pointee(DeletionIndex::empty())),
                    cached_insert_records: Arc::new(ArcSwap::from_pointee(
                        DeletionIndex::from_map(insert_records_pk_i64),
                    )),
                },
                PkDeletionStrategy::RowConverterBased => {
                    PkDeletionStrategyWithCache::RowConverterBased {
                        cached_deleted_row_keys: Arc::new(ArcSwap::from_pointee(
                            KeyDeletionIndex::empty(),
                        )),
                        cached_insert_records: Arc::new(ArcSwap::from_pointee(
                            KeyDeletionIndex::from_map(insert_records_row_keys),
                        )),
                    }
                }
            });
        }

        // Read deletion vector files in a blocking task, detecting type from schema
        // Returns (HashMap<String, RoaringBitmap>, HashMap<Box<[u8]>, i64>) where:
        // - per_file_row_ids: file path -> bitmap of deleted row positions
        // - deleted_row_keys: PK bytes -> max delete sequence
        let (per_file_row_ids, deleted_row_keys) =
            task::spawn_blocking(move || detect_deletion_type_and_read(delete_files))
                .await
                .map_err(|err| CatalogError::InvalidOperation {
                    message: "Deletion vector reader task panicked or was cancelled.".to_string(),
                    source: Box::new(err),
                })
                .and_then(|result| {
                    result.map_err(|err| CatalogError::InvalidOperation {
                        message: "Failed to read deletion vectors.".to_string(),
                        source: Box::new(err),
                    })
                })?;

        // Construct the appropriate cache variant with populated caches
        let cache = match strategy {
            PkDeletionStrategy::PositionBased => {
                let total_deletions: u64 = per_file_row_ids.values().map(RoaringBitmap::len).sum();
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} position-based deletions across {} files",
                    total_deletions,
                    per_file_row_ids.len(),
                );
                PkDeletionStrategyWithCache::PositionBased {
                    cached_deleted_row_ids: Arc::new(ArcSwap::from_pointee(per_file_row_ids)),
                }
            }
            PkDeletionStrategy::Int64Pk => {
                // Int64 PK - convert row_keys (which contain Int64 bytes) to i64
                // TODO: Optimize to store Int64 PK values directly in deletion files
                let int64_pks: HashMap<i64, i64> = deleted_row_keys
                    .iter()
                    .filter_map(|(bytes, &seq)| {
                        if bytes.len() >= 8 {
                            // RowConverter uses big-endian for i64 with sign bit flipped
                            let mut arr = [0_u8; 8];
                            arr.copy_from_slice(&bytes[..8]);
                            Some((i64::from_be_bytes(arr), seq))
                        } else {
                            tracing::warn!(
                                "Skipping invalid Int64 deletion key with length {} (expected at least 8 bytes)",
                                bytes.len()
                            );
                            None
                        }
                    })
                    .collect();
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} int64-pk, {} int64-insert",
                    int64_pks.len(),
                    insert_records_pk_i64.len(),
                );
                PkDeletionStrategyWithCache::Int64Pk {
                    cached_deleted_pk: Arc::new(ArcSwap::from_pointee(DeletionIndex::from_map(
                        int64_pks,
                    ))),
                    cached_insert_records: Arc::new(ArcSwap::from_pointee(
                        DeletionIndex::from_map(insert_records_pk_i64),
                    )),
                }
            }
            PkDeletionStrategy::RowConverterBased => {
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} key-based, {} key-insert",
                    deleted_row_keys.len(),
                    insert_records_row_keys.len(),
                );
                PkDeletionStrategyWithCache::RowConverterBased {
                    cached_deleted_row_keys: Arc::new(ArcSwap::from_pointee(
                        KeyDeletionIndex::from_map(deleted_row_keys),
                    )),
                    cached_insert_records: Arc::new(ArcSwap::from_pointee(
                        KeyDeletionIndex::from_map(insert_records_row_keys),
                    )),
                }
            }
        };

        Ok(cache)
    }

    /// Load protected snapshots from the catalog.
    ///
    /// Protected snapshots are those with sequence > `max_delete_sequence`.
    /// They contain data written after deletions and should skip deletion filtering.
    async fn load_protected_snapshots(
        catalog: Arc<dyn MetadataCatalog>,
        table_id: &str,
        strategy: &PkDeletionStrategyWithCache,
    ) -> CatalogResult<HashMap<String, i64>> {
        // Only PK-based strategies support sequence-ordered snapshot protection.
        // Position-based deletion vectors are per-file and don't need protected snapshots.
        if strategy.is_position_based() {
            return Ok(HashMap::new());
        }

        let snapshot_sequences = catalog.get_all_snapshot_sequences(table_id).await?;

        if snapshot_sequences.is_empty() {
            return Ok(HashMap::new());
        }

        // Treat ALL snapshots as protected, using each snapshot's own persisted
        // `sequence_number` as its deletion threshold.
        //
        // Each snapshot's `sequence_number` was allocated (via `increment_sequence_number`)
        // BEFORE the same round's deletions were created. Therefore:
        // - All deletions from PRIOR rounds have `delete_seq < sequence_number`
        // - All deletions from the SAME or LATER rounds have `delete_seq > sequence_number`
        //
        // The partial deletion filter uses `delete_seq > threshold`, so setting the
        // threshold to `sequence_number` correctly:
        // - Skips deletions from prior rounds (already accounted for at snapshot creation)
        // - Applies deletions from the same or later rounds
        //
        // Previously, this function computed a single global `max_delete_seq` from ALL
        // deletions and filtered out snapshots where `seq <= max_delete_seq`. This was
        // incorrect because later rounds' deletions raised the global max, causing earlier
        // snapshots to be incorrectly dropped and their data lost on restart.

        tracing::debug!(
            "Loaded {} protected snapshot(s) for table_id {table_id}",
            snapshot_sequences.len(),
        );

        Ok(snapshot_sequences)
    }

    /// Creates a projection that strips additional columns added for deletion filtering.
    ///
    /// Extend the projection to include columns referenced by `filter` that aren't
    /// already present. Returns the (possibly extended) projection and whether any
    /// columns were added (meaning a projection strip is needed later).
    fn extend_projection_for_retention_filter(
        &self,
        projection: Option<Vec<usize>>,
        filter: &Expr,
        already_extended: bool,
    ) -> (Option<Vec<usize>>, bool) {
        let Some(mut proj) = projection else {
            return (None, already_extended);
        };
        let mut added = already_extended;
        for col_ref in filter.column_refs() {
            if let Some((idx, _)) = self.table_metadata.schema.column_with_name(col_ref.name())
                && !proj.contains(&idx)
            {
                proj.push(idx);
                added = true;
            }
        }
        (Some(proj), added)
    }

    /// When filtering by PK, we may have added PK columns to the scan that weren't in the
    /// original projection. This creates a `ProjectionExec` that only outputs the originally
    /// requested columns.
    #[expect(clippy::unused_self)]
    fn create_projection_strip(
        &self,
        input: Arc<dyn ExecutionPlan>,
        num_columns_to_keep: usize,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let mut projection_expr: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(num_columns_to_keep);

        for idx in 0..num_columns_to_keep {
            let field = input_schema.field(idx);
            let col_name = field.name().clone();
            projection_expr.push((
                Arc::new(Column::new(&col_name, idx)) as Arc<dyn PhysicalExpr>,
                col_name,
            ));
        }

        let projection = ProjectionExec::try_new(projection_expr, input)?;
        Ok(Arc::new(CayenneAccelerationExec::new(Arc::new(projection))))
    }

    /// Scan protected snapshots with partial deletion filtering.
    ///
    /// Protected snapshots skip deletions that existed when they were created
    /// (deletions with seq <= `max_delete_seq_at_creation`), but newer deletions
    /// (seq > `max_delete_seq_at_creation`) are still applied.
    async fn scan_protected_snapshots(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        pk_indices_in_projection: &[usize],
    ) -> datafusion_common::Result<Vec<Arc<dyn ExecutionPlan>>> {
        let protected_snapshots = {
            let guard = self.protected_snapshots.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "Protected snapshots lock poisoned".to_string(),
                )
            })?;
            guard.clone()
        };

        if protected_snapshots.is_empty() {
            return Ok(Vec::new());
        }

        let mut plans = Vec::with_capacity(protected_snapshots.len());

        for (snapshot_id, max_delete_seq_at_creation) in protected_snapshots {
            // Create listing table for this snapshot
            let snapshot_url = Self::snapshot_dir_url(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                &snapshot_id,
            );

            let listing_table = Self::create_listing_table_with_config(
                &snapshot_url,
                Arc::clone(&self.table_metadata.schema),
                self.context.file_format(),
                &self.pk_deletion_strategy,
                state.config(),
            )
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to create listing table for protected snapshot {snapshot_id}: {e}"
                ))
            })?;

            let plan = listing_table
                .scan(state, projection, filters, limit)
                .await?;

            // Apply partial deletion filter - only deletions with seq > max_delete_seq_at_creation
            let filtered_plan = self.apply_partial_deletion_filter(
                plan,
                pk_indices_in_projection,
                max_delete_seq_at_creation,
            )?;

            plans.push(filtered_plan);
        }

        Ok(plans)
    }

    /// Apply partial deletion filter - only deletions with seq > threshold are applied.
    ///
    /// This is used for protected snapshots which should skip deletions that existed
    /// when they were created, but still honor newer deletions.
    fn apply_partial_deletion_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
        min_delete_seq_to_apply: i64,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => {
                let all_deleted_pks = cached_deleted_pk.load_full();

                // Filter to only include deletions with seq > min_delete_seq_to_apply.
                let filtered_deletions: HashMap<i64, i64> = all_deleted_pks
                    .entries()
                    .iter()
                    .filter(|(_, seq)| **seq > min_delete_seq_to_apply)
                    .map(|(&pk, &seq)| (pk, seq))
                    .collect();

                if filtered_deletions.is_empty() {
                    // No deletions to apply, return plan as-is
                    return Ok(Arc::new(CayenneAccelerationExec::new(plan)));
                }

                let pk_column_index =
                    pk_indices_in_projection.first().copied().ok_or_else(|| {
                        datafusion_common::DataFusionError::Internal(
                            "Int64 PK strategy requires exactly one PK column index".to_string(),
                        )
                    })?;

                let empty_insert_records = Arc::new(DeletionIndex::empty());
                Ok(Arc::new(Int64PkDeletionFilterExec::new(
                    plan,
                    Arc::new(DeletionIndex::from_map(filtered_deletions)),
                    empty_insert_records,
                    pk_column_index,
                )))
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => {
                // Similar logic for RowConverter-based strategy
                if let Some(ref row_converter) = self.pk_row_converter {
                    let all_deleted_keys = cached_deleted_row_keys.load_full();

                    // Filter to only include deletions with seq > min_delete_seq_to_apply.
                    let filtered_deletions: HashMap<Box<[u8]>, i64> = all_deleted_keys
                        .entries()
                        .iter()
                        .filter(|(_, seq)| **seq > min_delete_seq_to_apply)
                        .map(|(key, &seq)| (key.clone(), seq))
                        .collect();

                    if filtered_deletions.is_empty() {
                        return Ok(Arc::new(CayenneAccelerationExec::new(plan)));
                    }

                    let empty_insert_records = Arc::new(KeyDeletionIndex::empty());
                    Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                        plan,
                        Arc::new(KeyDeletionIndex::from_map(filtered_deletions)),
                        empty_insert_records,
                        pk_indices_in_projection.to_vec(),
                        Arc::clone(row_converter),
                    )))
                } else {
                    Ok(Arc::new(CayenneAccelerationExec::new(plan)))
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based doesn't use protected snapshots
                Ok(Arc::new(CayenneAccelerationExec::new(plan)))
            }
        }
    }

    /// Apply deletion filter to a plan based on the current deletion strategy.
    fn apply_deletion_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => {
                let deleted_pk_values = cached_deleted_pk.load_full();
                // Don't use insert_records for protected snapshot approach
                // The protected snapshots already handle new data without filtering
                let empty_insert_records = Arc::new(DeletionIndex::empty());

                if !deleted_pk_values.is_empty() {
                    let pk_column_index =
                        pk_indices_in_projection.first().copied().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "Int64 PK strategy requires exactly one PK column index"
                                    .to_string(),
                            )
                        })?;

                    return Ok(Arc::new(Int64PkDeletionFilterExec::new(
                        plan,
                        deleted_pk_values,
                        empty_insert_records,
                        pk_column_index,
                    )));
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => {
                if let Some(ref row_converter) = self.pk_row_converter {
                    let deleted_row_keys = cached_deleted_row_keys.load_full();
                    // Don't use insert_records for protected snapshot approach
                    let empty_insert_records = Arc::new(KeyDeletionIndex::empty());

                    if !deleted_row_keys.is_empty() {
                        return Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                            plan,
                            deleted_row_keys,
                            empty_insert_records,
                            pk_indices_in_projection.to_vec(),
                            Arc::clone(row_converter),
                        )));
                    }
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based deletions are handled at the Vortex scan level; no manual filtering is needed
            }
        }

        // No deletions to apply (position-based deletions are handled at Vortex scan level).
        Ok(plan)
    }

    /// Apply deletion filter including insert records (for main scan path, not protected snapshots).
    /// Unlike `apply_deletion_filter` which uses empty insert records, this passes the full
    /// cached insert records needed for the main plan.
    fn apply_deletion_filter_with_insert_records(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        pk_indices_in_projection: &[usize],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk,
                cached_insert_records,
            } => {
                let deleted_pk_values = cached_deleted_pk.load_full();
                let insert_records_pk_values = cached_insert_records.load_full();

                if !deleted_pk_values.is_empty() {
                    tracing::debug!(
                        "Applying Int64 PK deletion filter ({} deleted keys, {} insert records) to scan of table {}",
                        deleted_pk_values.len(),
                        insert_records_pk_values.len(),
                        self.table_metadata.table_name
                    );

                    let pk_column_index =
                        pk_indices_in_projection.first().copied().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "Int64 PK strategy requires exactly one PK column index"
                                    .to_string(),
                            )
                        })?;

                    return Ok(Arc::new(Int64PkDeletionFilterExec::new(
                        plan,
                        deleted_pk_values,
                        insert_records_pk_values,
                        pk_column_index,
                    )));
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                if let Some(ref row_converter) = self.pk_row_converter {
                    let deleted_row_keys = cached_deleted_row_keys.load_full();
                    let insert_records_row_keys = cached_insert_records.load_full();

                    if !deleted_row_keys.is_empty() {
                        tracing::debug!(
                            "Applying RowConverter-based deletion filter ({} deleted keys, {} insert records) to scan of table {}",
                            deleted_row_keys.len(),
                            insert_records_row_keys.len(),
                            self.table_metadata.table_name
                        );

                        return Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                            plan,
                            deleted_row_keys,
                            insert_records_row_keys,
                            pk_indices_in_projection.to_vec(),
                            Arc::clone(row_converter),
                        )));
                    }
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based deletions are handled at the Vortex scan level
            }
        }

        Ok(plan)
    }
}

#[async_trait]
impl TableProvider for CayenneTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Register object store with the session's runtime env if configured for S3 Express One Zone.
        // This ensures the session can access S3 when the underlying ListingTable reads data.
        if let Some(ref config) = self.object_store_config {
            Self::register_object_store_if_needed(state.runtime_env(), config);
        }

        // Determine if we need PK-based deletion (Int64 or RowConverter based)
        let need_pk_deletion = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => !cached_deleted_pk.load().is_empty(),
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => !cached_deleted_row_keys.load().is_empty(),
            PkDeletionStrategyWithCache::PositionBased { .. } => false,
        };

        // For PK-based deletion, we need to ensure PK columns are included in the projection
        // so we can filter by key. We may need to strip them out afterward if they weren't
        // originally requested.
        let (effective_projection, pk_indices_in_projection, need_projection_strip) =
            if need_pk_deletion {
                if let Some(proj) = projection {
                    // Check which PK columns are missing from the projection
                    let mut extended_proj: Vec<usize> = proj.clone();
                    let mut pk_indices: Vec<usize> =
                        Vec::with_capacity(self.pk_column_indices.len());
                    let mut added_columns = false;

                    for &pk_idx in &self.pk_column_indices {
                        if let Some(pos) = extended_proj.iter().position(|&p| p == pk_idx) {
                            // PK column already in projection
                            pk_indices.push(pos);
                        } else {
                            // PK column not in projection - add it at the end
                            pk_indices.push(extended_proj.len());
                            extended_proj.push(pk_idx);
                            added_columns = true;
                        }
                    }

                    (Some(extended_proj), pk_indices, added_columns)
                } else {
                    // No projection means all columns are selected
                    (None, self.pk_column_indices.clone(), false)
                }
            } else {
                // No PK-based deletion needed, use original projection
                let pk_indices = if let Some(proj) = projection {
                    self.pk_column_indices
                        .iter()
                        .filter_map(|&orig_idx| {
                            proj.iter().position(|&proj_idx| proj_idx == orig_idx)
                        })
                        .collect()
                } else {
                    self.pk_column_indices.clone()
                };
                (projection.cloned(), pk_indices, false)
            };

        // Time-based retention: build a keep filter at scan time.
        // Prefer the builder (produces correctly-typed timestamps matching the
        // column's timezone) over the legacy Expr+simplify path.
        // Injected at two layers:
        // 1. Appended to scan filters for file-level statistics pruning (Vortex should_prune)
        // 2. Wrapped as a physical FilterExec for row-level filtering
        let retention_keep_filter = if let Some(ref builder) = self.time_retention_filter_builder {
            let filter = builder.keep_filter();
            let filter = util::expr::simplify_expr(filter, &self.table_metadata.schema)?;
            Some(filter)
        } else {
            None
        };

        // Ensure columns referenced by the retention filter are in the projection.
        // Similar to PK column handling: if the user's query doesn't SELECT the time
        // column, we add it for FilterExec and strip it afterward.
        let (effective_projection, need_projection_strip) =
            if let Some(ref keep_filter) = retention_keep_filter {
                self.extend_projection_for_retention_filter(
                    effective_projection,
                    keep_filter,
                    need_projection_strip,
                )
            } else {
                (effective_projection, need_projection_strip)
            };

        // Build effective scan filters: user filters + optional retention filter.
        let effective_filters: Vec<Expr>;
        let scan_filters = if let Some(ref keep_filter) = retention_keep_filter {
            effective_filters = filters
                .iter()
                .cloned()
                .chain(std::iter::once(keep_filter.clone()))
                .collect();
            tracing::trace!(
                table = %self.table_metadata.table_name,
                total_filters = effective_filters.len(),
                "Injected time_retention keep-filter into scan filters"
            );
            &effective_filters
        } else {
            filters
        };

        let target_partitions = state.config().target_partitions();

        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            &self.get_current_snapshot_id()?,
        );
        let listing_table = Self::create_listing_table_with_config(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
            state.config(),
        )?;
        let main_plan = listing_table
            .scan(state, effective_projection.as_ref(), scan_filters, limit)
            .await?;

        // Check for protected snapshots that need to be scanned with partial deletion filter.
        let protected_snapshot_plans = self
            .scan_protected_snapshots(
                state,
                effective_projection.as_ref(),
                scan_filters,
                limit,
                &pk_indices_in_projection,
            )
            .await?;

        // Read any inlined data and create a MemoryExec plan for it.
        let inlined_batches = self.read_inlined_batches().await.map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read inlined data for table {}: {e}",
                self.table_metadata.table_name
            ))
        })?;
        let inlined_plan: Option<Arc<dyn ExecutionPlan>> = if inlined_batches.is_empty() {
            None
        } else {
            // Apply projection to inlined batches if needed
            let proj_schema = if let Some(ref proj) = effective_projection {
                let fields: Vec<arrow_schema::FieldRef> = proj
                    .iter()
                    .map(|&i| self.table_metadata.schema.field(i).clone().into())
                    .collect();
                Arc::new(arrow_schema::Schema::new(fields))
            } else {
                Arc::clone(&self.table_metadata.schema)
            };

            let projected_batches: Vec<RecordBatch> = inlined_batches
                .into_iter()
                .map(|batch| {
                    if let Some(ref proj) = effective_projection {
                        batch.project(proj).map_err(|e| {
                            datafusion_common::DataFusionError::Execution(format!(
                                "Failed to project inlined batch for table {}: {e}",
                                self.table_metadata.table_name
                            ))
                        })
                    } else {
                        Ok(batch)
                    }
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;

            if projected_batches.is_empty() {
                None
            } else {
                Some(
                    datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                        &[projected_batches],
                        proj_schema,
                        None,
                    )?,
                )
            }
        };

        // Build the final plan:
        // - If protected snapshots exist: deletion filter on main, UNION with snapshots
        // - Otherwise: apply deletion filter directly to main plan
        // - If inlined data exists: UNION with inlined data plan
        let plan = if protected_snapshot_plans.is_empty() {
            self.apply_deletion_filter_with_insert_records(main_plan, &pk_indices_in_projection)?
        } else {
            let filtered_main_plan =
                self.apply_deletion_filter(main_plan, &pk_indices_in_projection)?;

            let mut all_plans = vec![filtered_main_plan];
            all_plans.extend(protected_snapshot_plans);
            UnionExec::try_new(all_plans)?
        };

        // Union inlined data if present
        let plan: Arc<dyn ExecutionPlan> = if let Some(inline_exec) = inlined_plan {
            UnionExec::try_new(vec![plan, inline_exec])?
        } else {
            plan
        };

        // Wrap with FilterExec for time retention. DataFusion's physical optimizer
        // pushes FilterExec predicates down through the plan tree (including through
        // UnionExec) into each child's VortexSource via `try_pushdown_filters`,
        // enabling file-level pruning via min/max stats and row-level filtering.
        let plan: Arc<dyn ExecutionPlan> = if let Some(ref keep_filter) = retention_keep_filter {
            self.wrap_plan_with_retention_filter(plan, keep_filter)?
        } else {
            plan
        };

        let mut plan: Arc<dyn ExecutionPlan> = if scan_filters.is_empty() && limit.is_none() {
            round_robin_repartition_if_needed(Arc::clone(&plan), target_partitions)?.unwrap_or(plan)
        } else {
            plan
        };

        plan = if let Some(limit) = limit {
            let local_limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(plan, limit));
            let single_partition: Arc<dyn ExecutionPlan> =
                Arc::new(CoalescePartitionsExec::new(local_limit));
            Arc::new(GlobalLimitExec::new(single_partition, 0, Some(limit)))
        } else {
            plan
        };

        // Strip extra columns (PK or retention time column) added to the projection
        // but not originally requested by the query.
        if need_projection_strip && let Some(orig_proj) = projection {
            return self.create_projection_strip(plan, orig_proj.len());
        }

        Ok(Arc::new(CayenneAccelerationExec::new(plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        // Synchronous method - clone Arc quickly and release lock immediately
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    LISTING_TABLE_LOCK_POISONED.to_string(),
                )
            })?;
            Arc::clone(&guard)
        };
        listing_table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion_common::Statistics> {
        // Delegate statistics tracking to the underlying Vortex ListingTable.
        // The ListingTable aggregates statistics from all Vortex files in the table directory,
        // providing metrics such as:
        // - Total number of rows across all files
        // - Total size in bytes
        // - Column-level statistics (min, max, null count, distinct count if available)
        //
        // This allows the query optimizer to make informed decisions about:
        // - Partition pruning
        // - Join ordering
        // - Aggregation strategies
        //
        // Note: Statistics are cached by the ListingTable and may not reflect
        // very recent writes until the table metadata is refreshed.
        //
        // Clone Arc quickly and release lock immediately to minimize contention
        let listing_table = {
            let guard = self.listing_table.read().ok()?;
            Arc::clone(&guard)
        };
        listing_table.statistics()
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        if is_s3 {
            tracing::info!(
                "Cayenne insert_into called for S3 table {} (overwrite: {:?})",
                self.table_metadata.table_name,
                overwrite
            );
        }

        // Register object store with the session's runtime env if configured for S3 Express One Zone.
        // This ensures the session can access S3 when the underlying ListingTable writes data.
        if let Some(ref config) = self.object_store_config {
            Self::register_object_store_if_needed(state.runtime_env(), config);
        } else if is_s3 {
            tracing::warn!(
                "S3 table {} has no object_store_config! Writes will fail.",
                self.table_metadata.table_name
            );
        }

        // For appends on local paths, ensure the snapshot directory exists before writing.
        // S3 creates paths on write automatically so this is only needed for local storage.
        if overwrite != InsertOp::Overwrite && !is_s3 {
            let current_snapshot = self.get_current_snapshot_id().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to get current snapshot ID: {e}"
                ))
            })?;
            let snapshot_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                &current_snapshot,
            );
            Self::ensure_snapshot_dir_exists(&snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        }

        // Delegate entirely to CayenneDataSink which handles:
        // - Overwrite: new snapshot creation, catalog commit, state updates, cleanup
        // - Append: write lock, PK validation, on-conflict deletions, new snapshot
        //   when needed, retention filters, sort-and-rewrite, listing table refresh
        let sink = Arc::new(CayenneDataSink::new(
            self.clone_for_write(),
            overwrite,
            Arc::clone(&self.table_metadata.schema),
            Arc::clone(&self.context),
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if self.file_based_deletes_preferred(&filters) {
            // File-based retention operates on listing table files. Materialize
            // pending inline rows first so retention can reason about file stats.
            {
                let _guard = self.write_lock.lock().await;
                self.checkpoint_inlined_data_if_present_for_delete().await?;
            }

            tracing::debug!(
                "Table '{}': using file-based retention delete path",
                self.table_metadata.table_name,
            );
            return self.delete_using_files(&filters);
        }

        if self.pk_deletion_strategy.is_position_based() {
            // Position-based deletion vectors target file-local row positions,
            // so no-PK inline rows must still be materialized before deletion.
            {
                let _guard = self.write_lock.lock().await;
                self.checkpoint_inlined_data_if_present_for_delete().await?;
            }

            return self.delete_using_deletion_vectors(&filters);
        }

        let file_sink = self.build_deletion_vector_sink(&filters, None)?;
        Ok(Arc::new(DeletionExec::new(Arc::new(
            InlineAwareDeletionSink {
                table: self.clone_for_write(),
                file_sink,
                filters,
            },
        ))))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let table_source = Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
            self.clone_for_write(),
        )));
        let mut plan =
            datafusion_expr::LogicalPlanBuilder::scan("__update_source", table_source, None)?
                .build()?;

        if let Some(combined) = filters.clone().into_iter().reduce(Expr::and) {
            plan = datafusion_expr::LogicalPlanBuilder::from(plan)
                .filter(combined)?
                .build()?;
        }

        let mut proj_exprs = Vec::new();
        for field in schema.fields() {
            let col_name = field.name();
            if let Some((_, expr)) = assignments.iter().find(|(name, _)| name == col_name) {
                proj_exprs.push(expr.clone().alias(col_name));
            } else {
                proj_exprs.push(datafusion_expr::col(col_name));
            }
        }
        plan = datafusion_expr::LogicalPlanBuilder::from(plan)
            .project(proj_exprs)?
            .build()?;

        let source_plan = state.create_physical_plan(&plan).await?;
        let session_state = state
            .as_any()
            .downcast_ref::<datafusion::execution::SessionState>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Session is not a SessionState".to_string(),
                )
            })?
            .clone();

        Ok(Arc::new(data_components::update::UpdateExec::new(
            source_plan,
            Arc::new(self.clone_for_write()),
            session_state,
            filters,
        )))
    }
}

impl CayenneTableProvider {
    /// File-level delete path.
    ///
    /// Creates a [`FileBasedDeletionSink`] that discovers eligible files
    /// (where `max(col) < threshold_value`) and deletes them from the main
    /// snapshot and all protected snapshot directories.
    fn delete_using_files(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if filters.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "delete_using_files requires exactly one filter, got {}",
                filters.len(),
            )));
        }
        let filter = &filters[0];

        // Build protected snapshot listing tables for PK-based strategies only.
        // Position-based tables have no protected snapshots.
        let protected_snapshot_tables = if self.pk_deletion_strategy.is_position_based() {
            None
        } else {
            Some(self.build_protected_snapshot_listing_tables()?)
        };

        Ok(Arc::new(DeletionExec::new(Arc::new(
            FileBasedDeletionSink::new(
                Arc::clone(&self.listing_table),
                protected_snapshot_tables,
                filter.clone(),
                self.table_metadata.table_name.clone(),
                Arc::clone(&self.catalog),
                Arc::clone(&self.protected_snapshots),
                self.table_metadata.table_id.clone(),
                self.table_metadata.path.clone(),
                Arc::clone(self.context.runtime_env()),
                Arc::clone(&self.write_lock),
            ),
        ))))
    }

    /// Main deletion-vector path via [`CayenneDeletionSink`].
    fn delete_using_deletion_vectors(
        &self,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(Arc::new(
            self.build_deletion_vector_sink(filters, Some(Arc::clone(&self.write_lock)))?,
        ))))
    }

    fn build_deletion_vector_sink(
        &self,
        filters: &[Expr],
        write_lock: Option<Arc<tokio::sync::Mutex<()>>>,
    ) -> datafusion_common::Result<CayenneDeletionSink> {
        let snapshot_tables: Vec<Arc<ListingTable>> = self
            .build_protected_snapshot_listing_tables()?
            .into_iter()
            .map(|(_, table)| table)
            .collect();

        Ok(CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            filters,
            self.pk_deletion_strategy.clone(),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            snapshot_tables,
            Arc::clone(self.context.runtime_env()),
            write_lock,
        ))
    }

    /// Delete rows by hash-probing key columns against a set of matched keys.
    ///
    /// Fast path for `MERGE INTO` on `PositionBased` tables. Bypasses filter
    /// construction and the O(N) filter-per-file evaluation. Instead, scans
    /// each file and performs O(1) `HashSet` lookups per row.
    ///
    /// Acquires the write lock to prevent concurrent writes/refreshes.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table lock cannot be read or if the
    /// underlying position-based deletion scan/persist operation fails.
    pub async fn delete_matched_rows_by_key_probe(
        &self,
        matched_keys: std::collections::HashSet<Vec<datafusion_common::ScalarValue>>,
        key_columns: &[String],
    ) -> datafusion_common::Result<u64> {
        let _write_guard = self.write_lock.lock().await;

        // MERGE key-probe deletes operate on listing-table files only, so
        // pending inlined rows must be materialized first.
        self.checkpoint_inlined_data_if_present_for_delete().await?;

        let ctx = self.create_session_context();
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| {
                datafusion_common::DataFusionError::Internal(format!(
                    "Failed to read listing table lock for '{}'",
                    self.table_metadata.table_name
                ))
            })?;
            Arc::clone(&guard)
        };

        // PositionBased tables have no protected snapshots, so we only scan the main listing table.
        let all_tables = vec![listing_table];

        // Build the deletion sink with write_lock=None (we already hold it).
        let sink = CayenneDeletionSink::new(
            self.table_metadata.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.listing_table),
            Arc::clone(&self.table_metadata.schema),
            &[], // no filters — positions are resolved by key probe
            self.pk_deletion_strategy.clone(),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            Vec::new(), // no protected snapshots for PositionBased
            Arc::clone(self.context.runtime_env()),
            None, // write lock already held above
        );

        sink.delete_by_key_hash_probe(&ctx, &all_tables, matched_keys, key_columns)
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))
    }

    /// Returns `true` if this table uses the `PositionBased` deletion strategy.
    #[must_use]
    pub fn is_position_based(&self) -> bool {
        self.pk_deletion_strategy.is_position_based()
    }

    /// Build listing tables for all protected snapshots.
    ///
    /// Returns a vec of `(snapshot_id, listing_table)` pairs.
    fn build_protected_snapshot_listing_tables(
        &self,
    ) -> datafusion_common::Result<Vec<(String, Arc<ListingTable>)>> {
        let protected_snapshots = {
            let guard = self.protected_snapshots.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "Protected snapshots lock poisoned".to_string(),
                )
            })?;
            guard.clone()
        };

        let mut result = Vec::with_capacity(protected_snapshots.len());
        for (snapshot_id, _) in protected_snapshots {
            let snapshot_url = Self::snapshot_dir_url(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                &snapshot_id,
            );

            let listing_table = Self::create_listing_table(
                &snapshot_url,
                Arc::clone(&self.table_metadata.schema),
                self.context.file_format(),
                &self.pk_deletion_strategy,
            )
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to create listing table for protected snapshot {snapshot_id}: {e}"
                ))
            })?;
            result.push((snapshot_id, listing_table));
        }
        Ok(result)
    }

    /// Returns `true` if deletes can use whole-file deletion instead of per-row deletion vectors.
    ///
    /// Requirements:
    /// - Time-based retention is configured (`time_retention_filter_builder`).
    /// - The table is **not** backed by S3 storage.
    /// - The filter is a single `retention_col < threshold` expression matching
    ///   the configured retention column. Non-retention deletes (e.g. CDC
    ///   change-batch `DELETE WHERE pk = value`) fall through to the
    ///   deletion-vector path to preserve correct DELETE semantics.
    fn file_based_deletes_preferred(&self, filters: &[Expr]) -> bool {
        let Some(ref builder) = self.time_retention_filter_builder else {
            return false;
        };

        if self.table_metadata.path.starts_with("s3://") {
            return false;
        }

        // Only use file-based path when the filter is a retention-pattern delete
        // on the configured retention column: `col < threshold`.
        let is_retention_filter = filters.len() == 1
            && super::retention::extract_retention_column_and_threshold(&filters[0])
                .is_ok_and(|(col, op, _)| col == builder.column_name() && op == Operator::Lt);

        if !is_retention_filter {
            tracing::debug!(
                "Table '{}': delete filter does not match retention pattern (`{} < threshold`)",
                self.table_metadata.table_name,
                builder.column_name(),
            );
        }

        is_retention_filter
    }
}

/// Formats a byte count as a human-readable string (e.g., "1.23 GiB").
fn format_bytes(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    #[expect(clippy::cast_precision_loss)]
    let bytes_f64 = bytes as f64;

    if bytes_f64 >= GIB {
        format!("{:.2} GiB", bytes_f64 / GIB)
    } else if bytes_f64 >= MIB {
        format!("{:.2} MiB", bytes_f64 / MIB)
    } else if bytes_f64 >= KIB {
        format!("{:.2} KiB", bytes_f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Formats bytes per second as a human-readable throughput string.
fn format_bytes_per_sec(bytes_per_sec: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    if bytes_per_sec >= GIB {
        format!("{:.2} GiB/s", bytes_per_sec / GIB)
    } else if bytes_per_sec >= MIB {
        format!("{:.2} MiB/s", bytes_per_sec / MIB)
    } else if bytes_per_sec >= KIB {
        format!("{:.2} KiB/s", bytes_per_sec / KIB)
    } else {
        format!("{bytes_per_sec:.0} B/s")
    }
}

#[cfg(test)]
mod tests {
    use crate::CayenneCatalog;
    use crate::metadata::VortexConfig;

    use super::*;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{Constraints, ToDFSchema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;
    use datafusion_common::DataFusionError;
    use datafusion_federation::schema_cast::record_convert::try_cast_to;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::sync::Arc;
    use test_framework::arrow_record_batch_gen::*;

    #[test]
    fn inline_memtable_pressure_is_absent_below_thresholds() {
        let stats = InlinedDataStats {
            record_count: INLINE_MEMTABLE_MAX_ROWS - 1,
            entry_count: INLINE_MEMTABLE_MAX_SEGMENTS,
            ipc_bytes: INLINE_MEMTABLE_MAX_BYTES - 1,
        };

        assert_eq!(inline_memtable_pressure(stats), None);
    }

    #[test]
    fn inline_memtable_pressure_detects_thresholds() {
        assert_eq!(
            inline_memtable_pressure(InlinedDataStats {
                record_count: INLINE_MEMTABLE_MAX_ROWS,
                ..InlinedDataStats::default()
            }),
            Some(InlineMemtablePressure::Rows)
        );
        assert_eq!(
            inline_memtable_pressure(InlinedDataStats {
                entry_count: INLINE_MEMTABLE_MAX_SEGMENTS + 1,
                ..InlinedDataStats::default()
            }),
            Some(InlineMemtablePressure::Segments)
        );
        assert_eq!(
            inline_memtable_pressure(InlinedDataStats {
                ipc_bytes: INLINE_MEMTABLE_MAX_BYTES,
                ..InlinedDataStats::default()
            }),
            Some(InlineMemtablePressure::IpcBytes)
        );
    }

    /// A `TableProviderFactory` implementation to create new instances of `CayenneTableProvider`.
    // Not used outside of tests until https://github.com/spiceai/spiceai/issues/8534 is resolved
    #[derive(Debug)]
    pub struct CayenneTableProviderFactory {}

    #[async_trait]
    impl TableProviderFactory for CayenneTableProviderFactory {
        async fn create(
            &self,
            state: &dyn Session,
            cmd: &CreateExternalTable,
        ) -> std::result::Result<Arc<dyn TableProvider>, DataFusionError> {
            let metastore_type = cmd
                .options
                .get("cayenne_metastore")
                .map_or("sqlite", String::as_str);

            let metadata_dir = cmd.options.get("cayenne_metadata_dir").cloned().ok_or(
                DataFusionError::Execution("cayenne_metadata_dir option is required".to_string()),
            )?;

            // Ensure metadata directory exists
            std::fs::create_dir_all(&metadata_dir).map_err(DataFusionError::IoError)?;

            let connection_string = match metastore_type {
                "turso" => format!("libsql://{metadata_dir}/cayenne.db"),
                "sqlite" => format!("sqlite://{metadata_dir}/cayenne.db"),
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Unsupported cayenne_metastore type: {metastore_type}"
                    )));
                }
            };

            let catalog = async move {
                let catalog = Arc::new(
                    CayenneCatalog::new(connection_string)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                ) as Arc<dyn MetadataCatalog>;

                catalog
                    .init()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok::<Arc<dyn MetadataCatalog>, DataFusionError>(catalog)
            }
            .await?;

            // Support vortex configuration via options: https://github.com/spiceai/spiceai/issues/8533
            let vortex_config = VortexConfig::default();

            // Use file_path if provided as base, otherwise use default: spice_data_base_path() + dataset_name
            let dir_path =
                cmd.options
                    .get("cayenne_data_dir")
                    .cloned()
                    .ok_or(DataFusionError::Execution(
                        "cayenne_metadata_dir option is required".to_string(),
                    ))?;

            let table_options = CreateTableOptions {
                table_name: cmd.name.to_string(),
                schema: Arc::clone(cmd.schema.inner()),
                primary_key: vec![], // No PK by default, can be set by caller
                on_conflict: None,   // No on-conflict behavior by default
                base_path: dir_path,
                partition_column: None, // Non-partitioned table
                vortex_config,
            };

            let retention_filters = Vec::new();

            // Create CayenneTableProvider
            let cayenne_table = CayenneTableProvider::create_table_with_retention(
                catalog,
                table_options,
                retention_filters,
                Arc::clone(state.runtime_env()),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(Arc::new(cayenne_table) as Arc<dyn TableProvider>)
        }
    }

    async fn arrow_cayenne_round_trip(
        arrow_record: RecordBatch,
        source_schema: SchemaRef,
        table_name: &str,
    ) {
        let factory = CayenneTableProviderFactory {};

        let temp_dir = tempfile::tempdir().expect("temp dir created");

        let cmd_options = HashMap::from([
            (
                "cayenne_metadata_dir".to_string(),
                format!(
                    "{}/metadata",
                    temp_dir.path().to_str().expect("should be str")
                ),
            ),
            (
                "cayenne_data_dir".to_string(),
                format!("{}/data", temp_dir.path().to_str().expect("should be str")),
            ),
        ]);

        let ctx = SessionContext::new();
        let cmd = CreateExternalTable {
            schema: Arc::new(arrow_record.schema().to_dfschema().expect("to df schema")),
            name: table_name.into(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: false,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: cmd_options,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };
        let table_provider = factory
            .create(&ctx.state(), &cmd)
            .await
            .expect("table provider created");

        let ctx = SessionContext::new();

        let mem_exec = MemorySourceConfig::try_new_exec(
            &[vec![arrow_record.clone()]],
            arrow_record.schema(),
            None,
        )
        .expect("memory exec created");
        let insert_plan = table_provider
            .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
            .await
            .expect("insert plan created");

        let _ = collect(insert_plan, ctx.task_ctx())
            .await
            .expect("insert done");

        ctx.register_table(table_name, table_provider)
            .expect("Table should be registered");
        let sql = format!("SELECT * FROM {table_name}");
        let df = ctx
            .sql(&sql)
            .await
            .expect("DataFrame should be created from query");

        let record_batch = df.collect().await.expect("RecordBatch should be collected");
        let casted_record =
            try_cast_to(record_batch[0].clone(), source_schema).expect("should cast record batch");

        tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
        tracing::debug!(
            "Cayenne returned Record Batch: {:?}",
            record_batch[0].columns()
        );

        // Check results
        assert_eq!(record_batch.len(), 1);
        assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
        assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
        assert_eq!(casted_record, arrow_record);
    }

    #[rstest]
    #[case::binary(get_arrow_binary_record_batch(), "binary")]
    #[case::large_binary(get_arrow_large_binary_record_batch(), "large_binary")]
    #[ignore = "Vortex does not support FixedSizeBinary yet. Planned: https://github.com/vortex-data/vortex/issues/2116"]
    #[case::fixed_size_binary(get_arrow_fixed_sized_binary_record_batch(), "fixed_size_binary")]
    #[case::int(get_arrow_int_record_batch(), "int")]
    #[case::float(get_arrow_float_record_batch(), "float")]
    #[case::float16(get_arrow_float16_record_batch(), "float16")]
    #[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
    #[case::utf8_view(get_arrow_utf8_view_record_batch(), "utf8_view")]
    #[case::binary_view(get_arrow_binary_view_record_batch(), "binary_view")]
    #[case::time(get_arrow_time_record_batch(), "time")]
    #[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
    #[case::date(get_arrow_date_record_batch(), "date")]
    #[case::struct_type(get_arrow_struct_record_batch(), "struct")]
    #[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
    #[ignore = "Vortex does not support Interval yet. See: https://github.com/vortex-data/vortex/issues/2116"]
    #[case::interval(get_arrow_interval_record_batch(), "interval")]
    #[ignore = "Vortex does not support Duration yet. Not on roadmap: https://github.com/vortex-data/vortex/issues/2116"]
    #[case::duration(get_arrow_duration_record_batch(), "duration")]
    #[case::list(get_arrow_list_record_batch(), "list")]
    #[case::null(get_arrow_null_record_batch(), "null")]
    #[case::list_of_structs(get_arrow_list_of_structs_record_batch(), "list_of_structs")]
    #[case::list_of_fixed_size_lists(
        get_arrow_list_of_fixed_size_lists_record_batch(),
        "list_of_fixed_size_lists"
    )]
    #[case::list_of_lists(get_arrow_list_of_lists_record_batch(), "list_of_lists")]
    #[ignore = "Vortex does not support Map yet. Not on roadmap: https://github.com/vortex-data/vortex/issues/2116"]
    #[case::map(get_arrow_map_record_batch(), "map")]
    #[case::dictionary(get_arrow_dictionary_array_record_batch(), "dictionary")]
    #[test_log::test(tokio::test)]
    async fn test_arrow_cayenne_roundtrip(
        #[case] arrow_result: (RecordBatch, SchemaRef),
        #[case] table_name: &str,
    ) {
        arrow_cayenne_round_trip(
            arrow_result.0,
            arrow_result.1,
            &format!("{table_name}_types"),
        )
        .await;
    }

    /// Helper: build a single-column Int64 `RecordBatch` and the matching `RowConverter`.
    fn make_int64_pk_batch(values: &[i64]) -> (RecordBatch, RowConverter) {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)]));
        let col = Arc::new(Int64Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema, vec![col]).expect("valid batch");
        let converter =
            RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("valid converter");
        (batch, converter)
    }

    fn single_batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
        let schema = batch.schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter([Ok(batch)]),
        ))
    }

    #[tokio::test]
    async fn test_process_stream_into_keyset_int64pk_filters_deleted() {
        let (batch, converter) = make_int64_pk_batch(&[1, 2, 3]);

        // Delete pk=2 with del_seq=1
        let deleted_index = DeletionIndex::from_map(HashMap::from([(2_i64, 1_i64)]));
        let strategy = PkDeletionStrategyWithCache::Int64Pk {
            cached_deleted_pk: Arc::new(ArcSwap::from_pointee(deleted_index.clone())),
            cached_insert_records: Arc::new(ArcSwap::from_pointee(DeletionIndex::empty())),
        };

        let mut keyset = HashMap::new();
        let mut row_id_base: i64 = 0;

        CayenneTableProvider::process_stream_into_keyset(
            single_batch_stream(batch),
            &strategy,
            &[0],
            &converter,
            &[0],
            Some(&deleted_index),
            None,
            None, // all deletions apply
            "test_table",
            &mut keyset,
            &mut row_id_base,
        )
        .await
        .expect("process_stream_into_keyset should succeed");

        assert_eq!(keyset.len(), 2, "pk=2 should be filtered out");
        assert_eq!(row_id_base, 3);
    }

    #[tokio::test]
    async fn test_process_stream_into_keyset_threshold_filters_partial() {
        let (batch, converter) = make_int64_pk_batch(&[1, 2, 3]);

        // pk=1 deleted at seq 5, pk=2 deleted at seq 15
        let deleted_index =
            DeletionIndex::from_map(HashMap::from([(1_i64, 5_i64), (2_i64, 15_i64)]));
        let strategy = PkDeletionStrategyWithCache::Int64Pk {
            cached_deleted_pk: Arc::new(ArcSwap::from_pointee(deleted_index.clone())),
            cached_insert_records: Arc::new(ArcSwap::from_pointee(DeletionIndex::empty())),
        };

        let mut keyset = HashMap::new();
        let mut row_id_base: i64 = 0;

        // threshold=10: only deletions with del_seq > 10 apply
        CayenneTableProvider::process_stream_into_keyset(
            single_batch_stream(batch),
            &strategy,
            &[0],
            &converter,
            &[0],
            Some(&deleted_index),
            None,
            Some(10),
            "test_table",
            &mut keyset,
            &mut row_id_base,
        )
        .await
        .expect("process_stream_into_keyset should succeed");

        // pk=1 (del_seq=5 <= 10) => visible, pk=2 (del_seq=15 > 10) => filtered, pk=3 => visible
        assert_eq!(
            keyset.len(),
            2,
            "only pk=2 should be filtered (del_seq 15 > threshold 10)"
        );
        assert_eq!(row_id_base, 3);
    }

    #[tokio::test]
    async fn test_process_stream_into_keyset_no_deletions() {
        let (batch, converter) = make_int64_pk_batch(&[10, 20, 30]);

        let strategy = PkDeletionStrategyWithCache::empty_int64_pk();

        let mut keyset = HashMap::new();
        let mut row_id_base: i64 = 0;

        CayenneTableProvider::process_stream_into_keyset(
            single_batch_stream(batch),
            &strategy,
            &[0],
            &converter,
            &[0],
            None,
            None,
            None,
            "test_table",
            &mut keyset,
            &mut row_id_base,
        )
        .await
        .expect("process_stream_into_keyset should succeed");

        assert_eq!(keyset.len(), 3, "all rows should be in keyset");
        assert_eq!(row_id_base, 3, "row_id_base should advance by batch size");
    }

    #[test]
    fn test_row_key_to_i64_rejects_invalid_length() {
        let err = CayenneTableProvider::row_key_to_i64(&[1, 2, 3], "test_table")
            .expect_err("invalid inlined Int64 key should fail");

        assert!(
            err.to_string().contains("expected 8 bytes"),
            "unexpected error: {err}"
        );
    }

    /// Helper to create a `CayenneTableProvider` with sort columns configured.
    ///
    /// Returns the provider and the temp dir (must be kept alive for the test duration).
    async fn create_sorted_cayenne_table(
        table_name: &str,
        schema: SchemaRef,
        sort_columns: Vec<String>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> (CayenneTableProvider, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().expect("temp dir created");
        let metadata_dir = format!(
            "{}/metadata",
            temp_dir.path().to_str().expect("should be str")
        );
        let data_dir = format!("{}/data", temp_dir.path().to_str().expect("should be str"));

        std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

        let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
        let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
            as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog initialized");

        let vortex_config = VortexConfig {
            sort_columns,
            ..VortexConfig::default()
        };

        let options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: data_dir,
            partition_column: None,
            vortex_config,
        };

        let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
            .create(options)
            .await
            .expect("table created");

        (provider, temp_dir)
    }

    /// Helper to insert a `RecordBatch` into a `CayenneTableProvider`.
    async fn insert_batch(provider: &CayenneTableProvider, batch: RecordBatch) {
        let ctx = SessionContext::new();
        let schema = batch.schema();

        let mem_exec = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
            .expect("memory exec created");

        let insert_plan = provider
            .insert_into(&ctx.state(), mem_exec, InsertOp::Append)
            .await
            .expect("insert plan created");

        let _ = collect(insert_plan, ctx.task_ctx())
            .await
            .expect("insert done");
    }

    /// Helper to read all data from a `CayenneTableProvider` as `RecordBatch`es.
    async fn read_all(
        ctx: &SessionContext,
        provider: &CayenneTableProvider,
        table_name: &str,
    ) -> Vec<RecordBatch> {
        ctx.deregister_table(table_name).ok();
        ctx.register_table(table_name, Arc::new(provider.clone_for_write()))
            .expect("table registered");
        let df = ctx
            .sql(&format!("SELECT * FROM {table_name}"))
            .await
            .expect("query created");
        df.collect().await.expect("collect succeeded")
    }

    #[tokio::test]
    async fn test_sort_and_rewrite_data_sorts_by_column() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let ctx = SessionContext::new();
        let (provider, _temp_dir) = create_sorted_cayenne_table(
            "sort_rewrite_test",
            Arc::clone(&schema),
            vec!["id".to_string()],
            ctx.runtime_env(),
        )
        .await;

        // Insert data in deliberately unsorted order across multiple batches
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![30, 10, 50])),
                Arc::new(Int64Array::from(vec![300, 100, 500])),
            ],
        )
        .expect("valid batch");
        insert_batch(&provider, batch1).await;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![20, 40])),
                Arc::new(Int64Array::from(vec![200, 400])),
            ],
        )
        .expect("valid batch");
        insert_batch(&provider, batch2).await;

        // Verify data is present but unsorted before rewrite
        let before = read_all(&ctx, &provider, "sort_rewrite_test").await;
        let total_rows_before: usize = before.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows_before, 5, "should have 5 rows before sort");

        // Sort and rewrite
        provider
            .sort_and_rewrite_data(128 * 1024 * 1024)
            .await
            .expect("sort_and_rewrite_data should succeed");

        // Read back and verify data is sorted by "id" ascending
        let after = read_all(&ctx, &provider, "sort_rewrite_test").await;
        let total_rows_after: usize = after.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows_after, 5, "should still have 5 rows after sort");

        // Collect all id values in order
        let mut all_ids: Vec<i64> = Vec::new();
        let mut all_values: Vec<i64> = Vec::new();
        for batch in &after {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column should be Int64");
            let val_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column should be Int64");
            for i in 0..batch.num_rows() {
                all_ids.push(id_col.value(i));
                all_values.push(val_col.value(i));
            }
        }

        assert_eq!(
            all_ids,
            vec![10, 20, 30, 40, 50],
            "ids should be sorted ascending"
        );
        assert_eq!(
            all_values,
            vec![100, 200, 300, 400, 500],
            "values should follow their corresponding ids"
        );
    }

    #[tokio::test]
    async fn test_sort_and_rewrite_data_empty_table() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let ctx = SessionContext::new();
        let (provider, _temp_dir) = create_sorted_cayenne_table(
            "sort_empty_test",
            Arc::clone(&schema),
            vec!["id".to_string()],
            ctx.runtime_env(),
        )
        .await;

        // Sort and rewrite on empty table should succeed without error
        provider
            .sort_and_rewrite_data(128 * 1024 * 1024)
            .await
            .expect("sort_and_rewrite_data on empty table should succeed");

        let after = read_all(&ctx, &provider, "sort_empty_test").await;
        let total_rows: usize = after.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 0, "empty table should remain empty after sort");
    }

    #[tokio::test]
    async fn test_sort_and_rewrite_data_preserves_all_rows() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ctx = SessionContext::new();
        let (provider, _temp_dir) = create_sorted_cayenne_table(
            "sort_preserve_test",
            Arc::clone(&schema),
            vec!["ts".to_string()],
            ctx.runtime_env(),
        )
        .await;

        // Insert multiple batches with overlapping timestamp ranges
        for i in (0..5).rev() {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![i * 10 + 5, i * 10])),
                    Arc::new(StringArray::from(vec![
                        format!("row_{}", i * 10 + 5),
                        format!("row_{}", i * 10),
                    ])),
                ],
            )
            .expect("valid batch");
            insert_batch(&provider, batch).await;
        }

        // Sort and rewrite
        provider
            .sort_and_rewrite_data(128 * 1024 * 1024)
            .await
            .expect("sort_and_rewrite_data should succeed");

        // Read back and verify all 10 rows are present and sorted
        let after = read_all(&ctx, &provider, "sort_preserve_test").await;
        let total_rows: usize = after.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 10, "all 10 rows should be preserved");

        let mut all_ts: Vec<i64> = Vec::new();
        let mut all_names: Vec<String> = Vec::new();
        for batch in &after {
            let ts_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts column should be Int64");
            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column should be Utf8");
            for i in 0..batch.num_rows() {
                all_ts.push(ts_col.value(i));
                all_names.push(name_col.value(i).to_string());
            }
        }

        // Verify sorted by ts ascending
        let expected_ts: Vec<i64> = (0..10).map(|i| i * 5).collect();
        assert_eq!(all_ts, expected_ts, "timestamps should be sorted ascending");

        // Verify each name corresponds to its timestamp
        for (ts, name) in all_ts.iter().zip(all_names.iter()) {
            assert_eq!(
                name,
                &format!("row_{ts}"),
                "name should match its timestamp"
            );
        }
    }
}
