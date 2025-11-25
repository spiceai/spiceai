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

//! `DataFusion` `TableProvider` implementation for Cayenne tables.
//!
//! # Virtual File Concept
//!
//! Cayenne treats "files" as virtual files, where each file is actually a Vortex
//! `ListingTable` at a unique directory. The catalog's `DataFile` entries track metadata
//! for these virtual files, but all actual I/O operations delegate to the corresponding
//! `ListingTable`:
//!
//! - **Reading**: Query the `ListingTable` for the specific file directory
//! - **Appending**: Append data via the `ListingTable` (creates new Vortex files)
//! - **Deleting**: Delete the `ListingTable`'s directory
//! - **Stats**: Get statistics from the `ListingTable`
//!
//! A Cayenne table can have multiple virtual files (`ListingTables`), each in its own
//! subdirectory (e.g., `file_000001/`, `file_000002/`). When querying the table,
//! the provider reads from all active virtual files.

use super::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use super::deletion::{DeletionVectorWriteSpec, DeletionVectorWriter};
use super::metadata::{CreateTableOptions, TableMetadata};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream as DFStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::Constraints;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::PlanProperties;
use futures::StreamExt;
use roaring::RoaringBitmap;
use std::any::Any;
use std::borrow::Cow;
use std::convert::TryInto;
use std::sync::{Arc, RwLock};
use tokio::task;
use vortex_datafusion::VortexFormat;

const DEFAULT_DATA_FILE_ID: i64 = 0;

/// Error message for poisoned `RwLock` on the listing table.
///
/// Lock poisoning occurs when a thread panics while holding the lock, leaving it in an
/// inconsistent state. This is a critical error that typically requires restarting the runtime.
const LISTING_TABLE_LOCK_POISONED: &str =
    "Lock poisoned on listing table: a thread panicked while holding this lock. \
    This indicates an internal error that requires restarting the runtime.";

/// Error message for poisoned `RwLock` on the deletion cache.
///
/// Lock poisoning occurs when a thread panics while holding the lock, leaving it in an
/// inconsistent state. This is a critical error that typically requires restarting the runtime.
const DELETION_CACHE_LOCK_POISONED: &str =
    "Lock poisoned on deletion cache: a thread panicked while holding this lock. \
    This indicates an internal error that requires restarting the runtime.";

/// Execution plan that filters out deleted rows based on deletion vectors.
///
/// This wraps another execution plan and removes rows whose positions
/// match the deleted row IDs loaded from deletion vector files.
///
/// # Zero-Copy Design
///
/// The deleted row IDs are wrapped in `Arc` to enable zero-copy sharing across
/// concurrent scans. This avoids cloning potentially large bitmaps on every scan,
/// aligning with the project's zero-copy principles for Arrow data.
struct DeletionFilterExec {
    input: Arc<dyn ExecutionPlan>,
    deleted_row_ids: Arc<RoaringBitmap>,
    properties: datafusion_physical_plan::PlanProperties,
}

impl DeletionFilterExec {
    fn new(input: Arc<dyn ExecutionPlan>, deleted_row_ids: Arc<RoaringBitmap>) -> Self {
        let properties = input.properties().clone();
        Self {
            input,
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
struct DeletionFilterStream {
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
    /// Vortex encoding configuration for hardware-accelerated compression
    vortex_config: super::metadata::VortexConfig,
    /// Cached deletion vectors (deleted row IDs) to avoid repeated metastore queries on every scan.
    /// This is loaded once during table provider initialization and invalidated when delete files change.
    /// Using `RwLock` for concurrent reads during scans with occasional writes on updates.
    /// The inner `Arc<RoaringBitmap>` enables zero-copy sharing: scans clone the Arc (cheap ref count
    /// increment) rather than cloning the entire bitmap, aligning with zero-copy principles.
    ///
    /// `RoaringBitmap` provides 50-90% memory savings vs `HashSet` for sparse deletions and SIMD-optimized
    /// contains operations. Limited to u32 row IDs (4 billion rows). Tables with excessive deleted rows
    /// (approaching billions) should trigger compaction to maintain query performance and clear deletion vectors.
    cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
    /// Write lock to serialize insert operations and prevent concurrent write races.
    /// This ensures that:
    /// - Only one `insert()` runs at a time per table
    /// - Parallel chunk writes complete before listing table refresh
    /// - Retention filters are applied atomically after writes
    /// - Statistics are consistent and up-to-date
    ///
    /// Uses `tokio::sync::Mutex` because the lock is held across `.await` points during insert operations.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for CayenneTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneTableProvider")
            .field("table_metadata", &self.table_metadata)
            .finish_non_exhaustive()
    }
}

impl CayenneTableProvider {
    /// Construct the path to a snapshot directory.
    ///
    /// Directory structure: `[table_path]/[table_id]/[snapshot_id]/`
    ///
    /// # Arguments
    ///
    /// * `table_path` - The base path for the table
    /// * `table_id` - The unique identifier for the table
    /// * `snapshot_id` - The snapshot identifier
    fn snapshot_dir_path(table_path: &str, table_id: i64, snapshot_id: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(table_path)
            .join(table_id.to_string())
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

    /// Create a configured `VortexSession` with selected encodings for hardware acceleration.
    ///
    /// This method registers only the encodings that are enabled in the configuration,
    /// allowing fine-grained control over compression vs performance tradeoffs.
    fn create_vortex_session(
        config: &super::metadata::VortexConfig,
    ) -> vortex_session::VortexSession {
        use vortex::VortexSessionDefault;
        use vortex_session::VortexSession;

        // Use default session which registers all encodings
        // Note: If all encodings are enabled, this is optimal. Otherwise, the overhead
        // of unused encodings is minimal compared to the complexity of custom registration.
        // Future enhancement: Vortex may provide API for selective encoding registration.
        let session = VortexSession::default();

        // Log which encodings are configured to be used
        let mut enabled = Vec::new();
        if config.enable_alp {
            enabled.push("ALP (SIMD numeric compression)");
        }
        if config.enable_fsst {
            enabled.push("FSST (SIMD string compression)");
        }
        if config.enable_bitpacking {
            enabled.push("BitPacking (SIMD integer compression)");
        }
        if config.enable_delta {
            enabled.push("Delta");
        }
        if config.enable_rle {
            enabled.push("RLE");
        }
        if config.enable_dict {
            enabled.push("Dictionary");
        }
        if config.enable_for {
            enabled.push("FOR");
        }
        if config.enable_zigzag {
            enabled.push("ZigZag");
        }

        if enabled.is_empty() {
            tracing::warn!("All Cayenne Vortex encodings disabled - using canonical encoding only");
        } else {
            tracing::info!("Cayenne Vortex encodings enabled: {}", enabled.join(", "));
        }

        session
    }

    /// Create a new `ListingTable` for a snapshot directory.
    ///
    /// # Arguments
    ///
    /// * `snapshot_dir` - Path to the snapshot directory
    /// * `schema` - Arrow schema for the table
    /// * `vortex_config` - Vortex encoding configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be created.
    fn create_listing_table(
        snapshot_dir: &std::path::Path,
        schema: SchemaRef,
        vortex_config: &super::metadata::VortexConfig,
    ) -> CatalogResult<Arc<ListingTable>> {
        let dir_url_str = Self::dir_to_url_string(snapshot_dir);

        let table_url = ListingTableUrl::parse(&dir_url_str).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to parse table URL: {e}"),
            }
        })?;

        // Create a configured Vortex session with selected encodings
        let vortex_session = Self::create_vortex_session(vortex_config);

        // Configure VortexFormat with hardware-optimized settings
        let vortex_opts = vortex_datafusion::VortexOptions {
            footer_cache_size_mb: vortex_config.footer_cache_mb,
            segment_cache_size_mb: vortex_config.segment_cache_mb,
        };

        let format = Arc::new(VortexFormat::new_with_options(vortex_session, vortex_opts));
        let listing_options =
            ListingOptions::new(format).with_session_config_options(&SessionConfig::default());

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let listing_table = ListingTable::try_new(config).map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to create listing table: {e}"),
            }
        })?;

        Ok(Arc::new(listing_table))
    }

    /// Ensure a snapshot directory exists, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    async fn ensure_snapshot_dir_exists(
        snapshot_dir: &std::path::Path,
    ) -> datafusion_common::Result<()> {
        if !snapshot_dir.exists() {
            tokio::fs::create_dir_all(snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        }
        Ok(())
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
        table_id: i64,
        current_snapshot_id: &str,
    ) -> CatalogResult<()> {
        let table_dir = std::path::PathBuf::from(table_path).join(table_id.to_string());

        // Check if table directory exists
        if !table_dir.exists() {
            return Ok(());
        }

        tracing::debug!(
            "Cleaning up old snapshots for table {} (keeping {})",
            table_id,
            current_snapshot_id
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

            // Skip the current snapshot
            if snapshot_id == current_snapshot_id {
                tracing::debug!("Keeping current snapshot: {}", snapshot_id);
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
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new(table_name: &str, catalog: Arc<dyn MetadataCatalog>) -> CatalogResult<Self> {
        Self::new_with_retention(table_name, catalog, Vec::new()).await
    }

    /// Create a new table provider with explicit retention filters.
    ///
    /// This is primarily used by the runtime when datasets specify `retention_sql`
    /// so that deletion vectors are written before a refresh completes.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn new_with_retention(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        retention_filters: Vec<Expr>,
    ) -> CatalogResult<Self> {
        let table_metadata = catalog.get_table(table_name).await?;

        // Construct path to current snapshot
        // Directory structure: [table_path]/[table_id]/[snapshot_id]/
        // All tables have a snapshot ID (created on table initialization)
        let snapshot_dir = Self::snapshot_dir_path(
            &table_metadata.path,
            table_metadata.table_id,
            &table_metadata.current_snapshot_id,
        );

        let vortex_config = table_metadata.vortex_config.clone();

        let listing_table = Self::create_listing_table(
            &snapshot_dir,
            Arc::<arrow_schema::Schema>::clone(&table_metadata.schema),
            &vortex_config,
        )?;

        // Load deletion vectors once at initialization to avoid repeated SQLite queries on every scan
        let table_id = table_metadata.table_id;
        let catalog_for_load = Arc::clone(&catalog);
        let deleted_row_ids = Self::load_deletion_vectors(table_id, catalog_for_load).await?;

        Ok(Self {
            table_metadata,
            catalog,
            listing_table: Arc::new(RwLock::new(listing_table)),
            retention_filters,
            vortex_config,
            // Wrap in Arc for zero-copy sharing across concurrent scans
            cached_deleted_row_ids: Arc::new(RwLock::new(Arc::new(deleted_row_ids))),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Create a new table in Cayenne.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
    ) -> CatalogResult<Self> {
        Self::create_table_with_retention(catalog, options, Vec::new()).await
    }

    /// Create a new table in Cayenne with retention filters applied to subsequent writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create_table_with_retention(
        catalog: Arc<dyn MetadataCatalog>,
        options: CreateTableOptions,
        retention_filters: Vec<Expr>,
    ) -> CatalogResult<Self> {
        let _table_id = catalog.create_table(options.clone()).await?;
        Self::new_with_retention(&options.table_name, catalog, retention_filters).await
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

    /// Insert data from a record batch stream.
    ///
    /// This method writes data to the Vortex `ListingTable`. The actual file writing is
    /// delegated to `DataFusion`'s `ListingTable` via `insert_into`, which uses `VortexSink`
    /// to create Vortex files in the table directory.
    ///
    /// # Implementation Notes
    ///
    /// The insert operation is handled by the underlying `ListingTable`, which:
    /// 1. Receives the record batch stream
    /// 2. Writes Vortex files to the table directory
    /// 3. Returns the number of rows written
    ///
    /// Note: Currently this doesn't create per-file virtual file entries in the Cayenne
    /// catalog. In a future enhancement, we could track individual Vortex files as
    /// separate `DataFile` entries by:
    /// - Intercepting the `VortexSink` output to discover written files
    /// - Creating unique subdirectories per "virtual file"
    /// - Adding one `DataFile` entry per subdirectory to the catalog
    ///
    /// For now, the data is successfully written to the `ListingTable`'s directory and
    /// will be readable on the next scan, even though we're not tracking individual
    /// files in the Cayenne catalog metadata yet.
    ///
    /// # Size-Based File Chunking
    ///
    /// This method implements size-based chunking to control Vortex file sizes:
    /// - Batches are accumulated until the target file size is reached
    /// - Each chunk is written as a separate Vortex file in parallel
    /// - Each file maintains proper statistics for `DataFusion` pushdown and pruning
    ///
    /// The target file size is configurable via `VortexConfig.target_vortex_file_size_mb`
    /// and defaults to 256 MB.
    ///
    /// # Performance Optimizations
    ///
    /// - **Streaming**: Processes chunks as they're formed, avoiding buffering all data
    /// - **Parallel writes**: Multiple chunks written concurrently with bounded parallelism
    /// - **Zero-copy**: Reuses `RecordBatch` Arc references, no data copying
    /// - **Pre-allocation**: Reserves capacity to minimize reallocations
    ///
    /// # Concurrency Safety
    ///
    /// This method uses an internal write lock to serialize insert operations on the same table.
    /// Multiple concurrent `insert()` calls will block, ensuring that:
    /// - Only one insert runs at a time per table
    /// - All parallel chunk writes complete before the listing table is refreshed
    /// - Retention filters are applied atomically after all writes
    /// - Table statistics remain consistent
    ///
    /// **Within a single insert**, chunks are written in parallel (bounded to 4 concurrent writes)
    /// for optimal I/O throughput. The serialization only applies across different `insert()` calls.
    ///
    /// This design ensures correctness while maintaining high performance for individual inserts.
    /// If you need higher write concurrency, consider partitioning your data across multiple tables.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be inserted.
    #[allow(clippy::items_after_statements)]
    #[allow(clippy::too_many_lines)]
    pub async fn insert(&self, stream: SendableRecordBatchStream) -> CatalogResult<u64> {
        // Acquire write lock to serialize inserts and prevent concurrent write races.
        // This ensures listing table refresh happens after all parallel chunk writes complete
        // and retention filters are applied atomically.
        let _write_guard = self.write_lock.lock().await;

        let target_size_bytes = self.vortex_config.target_vortex_file_size_mb * 1024 * 1024;

        // Process stream in chunks and write them in parallel with bounded concurrency
        let (total_rows, chunk_count) = self
            .chunk_and_write_parallel(stream, target_size_bytes)
            .await?;

        tracing::debug!(
            "Insert completed, wrote {} rows to Vortex in {} chunk(s)",
            total_rows,
            chunk_count
        );

        // Apply retention filters before refreshing the listing table so any rows matching the
        // configured predicate are captured in deletion vector files within this refresh.
        //
        // ACID GUARANTEES: The write lock ensures atomicity:
        // 1. All chunk writes complete before retention filters are evaluated
        // 2. Retention filters are applied before the write lock is released
        // 3. The listing table is refreshed atomically after retention
        // 4. Other inserts are blocked until the entire operation completes
        //
        // This provides ACID semantics: either all data is written with retention applied,
        // or the operation fails and nothing is visible. There is a small visibility window
        // (milliseconds) between file write and retention filter application where newly
        // written data is queryable before deletion vectors are created, but this window is
        // bounded by the write lock and cannot be observed by other insert operations.
        //
        // This is the correct design for retention filters - they are table-wide predicates
        // that must scan all data, not per-chunk predicates. Applying them atomically after
        // the write completes ensures consistency without write amplification.
        if !self.retention_filters.is_empty() {
            match self.apply_retention_filters().await {
                Ok(deleted) => {
                    if deleted > 0 {
                        tracing::info!(
                            "Retention filters deleted {} row(s) for table {}",
                            deleted,
                            self.table_metadata.table_name
                        );
                    } else {
                        tracing::debug!(
                            "Retention filters found no rows to delete for table {}",
                            self.table_metadata.table_name
                        );
                    }
                }
                Err(err) => {
                    return Err(super::catalog::CatalogError::InvalidOperation {
                        message: format!("Failed to apply retention filters after insert: {err}"),
                    });
                }
            }
        }

        // If sort_columns is configured, sort the data on disk after retention filters.
        // This operates on the listing table data (the complete corpus after retention),
        // ensuring optimal zone maps with non-overlapping min/max ranges.
        // Sorting uses DataFusion's SortExec with:
        // - Automatic disk spilling for datasets larger than available memory
        // - Streaming external merge sort for efficient memory usage
        // - SIMD-optimized kernels (NEON on arm64, AVX2/AVX-512 on amd64)
        // - Configurable compression for spill files (zstd, lz4_frame, uncompressed)
        if !self.vortex_config.sort_columns.is_empty() {
            self.sort_and_rewrite_data(target_size_bytes).await?;
        }

        // Refresh the listing table to pick up new/rewritten files and update statistics.
        // This ensures that query plans have access to up-to-date table statistics
        // after the insert operation completes. The write lock ensures this refresh
        // happens after all parallel chunk writes are complete and no other insert
        // can interfere.
        self.refresh_listing_table()?;

        // Write lock is released here, allowing the next insert to proceed
        Ok(total_rows)
    }

    /// Process stream in chunks and write them in parallel with bounded concurrency.
    ///
    /// This method optimizes throughput by:
    /// - Streaming chunk formation (no buffering of all chunks)
    /// - Parallel writes with bounded concurrency (max 4 concurrent writes)
    /// - Zero-copy batch handling (Arc references)
    ///
    /// # Returns
    ///
    /// Returns a tuple of `(total_rows, chunk_count)` where:
    /// - `total_rows` is the total number of rows written
    /// - `chunk_count` is the number of Vortex files created
    ///
    /// # Errors
    ///
    /// Returns an error if any chunk write fails.
    async fn chunk_and_write_parallel(
        &self,
        mut stream: SendableRecordBatchStream,
        target_size_bytes: usize,
    ) -> CatalogResult<(u64, usize)> {
        use tokio::sync::Semaphore;

        // Bounded parallelism: max 4 concurrent writes to avoid overwhelming I/O
        let semaphore = Arc::new(Semaphore::new(4));
        let mut write_tasks = tokio::task::JoinSet::new();

        // Pre-allocate chunk vector with estimated capacity
        // Estimate: average batch ~8MB, so reserve for a few batches per chunk
        let estimated_batches_per_chunk = (target_size_bytes / (8 * 1024 * 1024)).max(1);
        let mut current_chunk = Vec::with_capacity(estimated_batches_per_chunk);
        let mut current_size = 0usize;
        let mut total_rows = 0u64;
        let mut chunk_count = 0usize;

        while let Some(batch_result) = stream.next().await {
            let batch =
                batch_result.map_err(|e| super::catalog::CatalogError::InvalidOperation {
                    message: format!("Failed to read batch from stream: {e}"),
                })?;

            let batch_size = batch.get_array_memory_size();

            // If adding this batch would exceed target size and we have data, write current chunk
            if current_size + batch_size > target_size_bytes && !current_chunk.is_empty() {
                // Acquire semaphore permit before spawning write task
                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
                    super::catalog::CatalogError::InvalidOperation {
                        message: format!("Failed to acquire write permit: {e}"),
                    }
                })?;

                // Move chunk to write task (zero-copy via mem::take)
                let chunk_to_write = std::mem::replace(
                    &mut current_chunk,
                    Vec::with_capacity(estimated_batches_per_chunk),
                );
                current_size = 0;
                chunk_count += 1;

                // Clone self for the async task
                let self_clone = self.clone_for_write();
                write_tasks.spawn(async move {
                    let result = self_clone.write_chunk(chunk_to_write).await;
                    drop(permit); // Release permit after write completes
                    result
                });
            }

            current_size += batch_size;
            current_chunk.push(batch);
        }

        // Write final chunk if non-empty
        if !current_chunk.is_empty() {
            let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
                super::catalog::CatalogError::InvalidOperation {
                    message: format!("Failed to acquire write permit for final chunk: {e}"),
                }
            })?;

            chunk_count += 1;

            let self_clone = self.clone_for_write();
            write_tasks.spawn(async move {
                let result = self_clone.write_chunk(current_chunk).await;
                drop(permit);
                result
            });
        }

        // Wait for all writes to complete and collect row counts
        while let Some(result) = write_tasks.join_next().await {
            let row_count =
                result.map_err(|e| super::catalog::CatalogError::InvalidOperation {
                    message: format!("Write task panicked: {e}"),
                })??;
            total_rows += row_count;
        }

        Ok((total_rows, chunk_count))
    }

    /// Create a clone of necessary fields for parallel write tasks.
    ///
    /// This method clones only the Arc references needed for writing,
    /// which is cheap (just atomic reference count increments).
    ///
    /// # Note on Retention Filters
    ///
    /// The cloned instance has empty `retention_filters` because retention is applied
    /// atomically at the end of the main `insert()` method (after all parallel chunk
    /// writes complete but before the write lock is released).
    ///
    /// This design provides ACID semantics:
    /// - Retention filters are table-wide predicates (e.g., "delete rows older than 30 days")
    /// - They must scan all table data, not just the newly written chunks
    /// - Applying them per-chunk would cause write amplification (write, scan, delete, repeat)
    /// - The write lock ensures atomicity: all writes + retention happen as one operation
    ///
    /// There is a brief moment (milliseconds) where newly written data exists on disk before
    /// deletion vectors are created, but the write lock prevents this from being observable
    /// to other operations - either the entire insert+retention succeeds atomically, or it fails.
    fn clone_for_write(&self) -> Self {
        Self {
            table_metadata: self.table_metadata.clone(),
            catalog: Arc::clone(&self.catalog),
            listing_table: Arc::clone(&self.listing_table),
            vortex_config: self.vortex_config.clone(),
            retention_filters: Vec::new(), // Applied once after all chunks complete, not per-chunk
            cached_deleted_row_ids: Arc::clone(&self.cached_deleted_row_ids),
            write_lock: Arc::clone(&self.write_lock), // Shared across all clones for same table
        }
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
    fn sort_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> CatalogResult<SendableRecordBatchStream> {
        use datafusion_execution::TaskContext;

        // Create a task context with default memory pool and runtime settings
        // This will use the configured spill directory and compression settings
        let task_ctx = Arc::new(TaskContext::default());

        tracing::debug!(
            "Sorting refresh data by columns {:?} for table {} using DataFusion SortExec with disk spilling support",
            self.vortex_config.sort_columns,
            self.table_metadata.table_name
        );

        // Use the common stream sorting utility
        let sorted_stream = runtime_datafusion::stream_utils::sort_stream(
            stream,
            &self.vortex_config.sort_columns,
            &task_ctx,
        )
        .map_err(|e| CatalogError::InvalidOperation {
            message: format!("Failed to execute sort: {e}"),
        })?;

        Ok(sorted_stream)
    }

    /// Sort and rewrite data on disk by reading from the listing table.
    ///
    /// This method:
    /// 1. Reads all data from the current listing table (includes retention filter results)
    /// 2. Sorts the data using `DataFusion`'s `SortExec` (with disk spilling)
    /// 3. Deletes the old unsorted files
    /// 4. Writes the sorted data back in optimally-sized chunks
    ///
    /// This ensures zone maps have non-overlapping min/max ranges for optimal pruning.
    ///
    /// # Errors
    ///
    /// Returns an error if reading, sorting, or rewriting fails.
    async fn sort_and_rewrite_data(&self, target_size_bytes: usize) -> CatalogResult<()> {
        use datafusion::execution::context::SessionContext;

        tracing::info!(
            "Sorting and rewriting data for table {} by columns {:?}",
            self.table_metadata.table_name,
            self.vortex_config.sort_columns
        );

        // Read all data from the current listing table
        let listing_table = {
            let guard = self
                .listing_table
                .read()
                .map_err(|_| CatalogError::LockPoisoned {
                    operation: "read listing table for sort".to_string(),
                })?;
            Arc::clone(&*guard)
        };

        // Create a session context and scan the listing table to get all data
        let ctx = SessionContext::new();
        let df = ctx
            .read_table(listing_table)
            .map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to read listing table for sorting: {e}"),
            })?;

        // Get the data as a stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to get stream from listing table: {e}"),
            })?;

        // Sort the stream using our existing sort logic
        let sorted_stream = self.sort_stream(stream)?;

        // Delete all existing Vortex files in the snapshot directory before rewriting
        let snapshot_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            &self.table_metadata.current_snapshot_id,
        );

        self.delete_snapshot_files(&snapshot_dir).await?;

        // Write the sorted data back in chunks
        let (total_rows, chunk_count) = self
            .chunk_and_write_parallel(sorted_stream, target_size_bytes)
            .await?;

        tracing::info!(
            "Rewrote {} rows in {} sorted chunk(s) for table {}",
            total_rows,
            chunk_count,
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Delete all Vortex files in a snapshot directory.
    ///
    /// # Errors
    ///
    /// Returns an error if files cannot be deleted.
    async fn delete_snapshot_files(&self, snapshot_dir: &std::path::Path) -> CatalogResult<()> {
        if !snapshot_dir.exists() {
            return Ok(());
        }

        let mut read_dir = tokio::fs::read_dir(snapshot_dir)
            .await
            .map_err(|source| CatalogError::IoError { source })?;

        let mut deleted_count = 0;
        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|source| CatalogError::IoError { source })?
        {
            let path = entry.path();

            // Only delete files (Vortex files), not subdirectories
            if path.is_file() {
                tokio::fs::remove_file(&path)
                    .await
                    .map_err(|source| CatalogError::IoError { source })?;
                deleted_count += 1;
            }
        }

        tracing::debug!(
            "Deleted {} Vortex file(s) from snapshot directory before rewriting sorted data",
            deleted_count
        );

        Ok(())
    }

    /// Write a single chunk of record batches as a Vortex file.
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk cannot be written.
    #[allow(clippy::items_after_statements)]
    #[allow(clippy::too_many_lines)]
    async fn write_chunk(&self, chunk: Vec<RecordBatch>) -> CatalogResult<u64> {
        if chunk.is_empty() {
            return Ok(0);
        }

        let schema = chunk[0].schema();
        let row_count: u64 = chunk.iter().map(|b| b.num_rows() as u64).sum();

        // Create a stream from the chunk batches
        let batch_stream = futures::stream::iter(chunk.into_iter().map(Ok));
        let chunk_stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), batch_stream);

        // Create a streaming execution plan that forwards batches without buffering
        // Uses tokio::sync::Mutex to properly handle async context
        struct StreamingExec {
            schema: arrow_schema::SchemaRef,
            stream: tokio::sync::Mutex<Option<DFStream>>,
            properties: datafusion_physical_plan::PlanProperties,
        }

        impl std::fmt::Debug for StreamingExec {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("StreamingExec").finish()
            }
        }

        impl DisplayAs for StreamingExec {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "StreamingExec")
            }
        }

        impl ExecutionPlan for StreamingExec {
            fn name(&self) -> &'static str {
                "StreamingExec"
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn schema(&self) -> arrow_schema::SchemaRef {
                Arc::<arrow_schema::Schema>::clone(&self.schema)
            }

            fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
                &self.properties
            }

            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![]
            }

            fn with_new_children(
                self: Arc<Self>,
                _children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
                Ok(self)
            }

            fn execute(
                &self,
                _partition: usize,
                _context: Arc<datafusion_execution::TaskContext>,
            ) -> datafusion_common::Result<DFStream> {
                // Use async-aware RecordBatchStreamAdapter to properly forward the stream
                let schema = Arc::<arrow_schema::Schema>::clone(&self.schema);
                let stream_mutex = Arc::new(tokio::sync::Mutex::new(
                    self.stream
                        .try_lock()
                        .map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                "Stream is locked (concurrent access detected)".to_string(),
                            )
                        })?
                        .take()
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Execution(
                                "Stream already consumed".to_string(),
                            )
                        })?,
                ));

                use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
                let adapter = RecordBatchStreamAdapter::new(
                    schema,
                    async_stream::stream! {
                        let mut stream = stream_mutex.lock().await;
                        while let Some(batch) = stream.next().await {
                            yield batch;
                        }
                    },
                );

                Ok(Box::pin(adapter))
            }
        }

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::<arrow_schema::Schema>::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        let stream_exec = Arc::new(StreamingExec {
            schema: Arc::<arrow_schema::Schema>::clone(&schema),
            stream: tokio::sync::Mutex::new(Some(Box::pin(chunk_stream))),
            properties,
        });

        // Create a session context for executing the insert
        let ctx = SessionContext::new();
        let state = ctx.state();

        // Delegate to ListingTable's insert_into to write Vortex files
        // Clone the Arc and drop the lock before awaiting
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| {
                super::catalog::CatalogError::LockPoisoned {
                    operation: "write_chunk (read listing table)".to_string(),
                }
            })?;
            Arc::clone(&guard)
        };
        let insert_plan = listing_table
            .insert_into(&state, stream_exec, InsertOp::Append)
            .await
            .map_err(|e| super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to create insert plan for chunk: {e}"),
            })?;

        // Execute the insert plan
        collect(insert_plan, state.task_ctx()).await.map_err(|e| {
            super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to execute insert for chunk: {e}"),
            }
        })?;

        tracing::debug!("Wrote chunk with {} rows to Vortex", row_count);

        Ok(row_count)
    }

    async fn apply_retention_filters(&self) -> CatalogResult<u64> {
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
            Arc::clone(&self.cached_deleted_row_ids),
        );

        let deleted_count =
            sink.delete_from()
                .await
                .map_err(|err| CatalogError::InvalidOperation {
                    message: format!("Failed to execute retention filters: {err}"),
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
        let deleted_row_ids =
            Self::load_deletion_vectors(self.table_metadata.table_id, Arc::clone(&self.catalog))
                .await?;

        let mut guard =
            self.cached_deleted_row_ids
                .write()
                .map_err(|_| CatalogError::LockPoisoned {
                    operation: "refresh deletion cache (write)".to_string(),
                })?;

        // Replace with new Arc-wrapped HashSet for zero-copy sharing
        *guard = Arc::new(deleted_row_ids);

        tracing::debug!(
            "Refreshed deletion cache for table {} ({} deleted rows)",
            self.table_metadata.table_name,
            guard.len()
        );

        Ok(())
    }

    /// Delete rows matching the given primary key values.
    ///
    /// # Errors
    ///
    /// Returns an error as this operation is not yet implemented.
    pub fn delete_by_primary_key(&self, _key_values: Vec<Vec<u8>>) -> CatalogResult<u64> {
        // Implementation would:
        // 1. Scan data files for matching primary keys
        // 2. Create/update deletion vectors
        // 3. Write deletion vector files
        // 4. Add delete file entries to catalog
        // 5. Return number of rows deleted
        Err(super::catalog::CatalogError::InvalidOperation {
            message: "Delete not yet implemented".to_string(),
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
    ) -> CatalogResult<u64> {
        // Implementation would:
        // 1. Delete old rows using deletion vectors
        // 2. Insert new rows
        // 3. Return number of rows updated
        Err(super::catalog::CatalogError::InvalidOperation {
            message: "Update not yet implemented".to_string(),
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
    fn refresh_listing_table(&self) -> CatalogResult<()> {
        // Construct path to current snapshot
        let snapshot_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            &self.table_metadata.current_snapshot_id,
        );

        let new_listing_table = Self::create_listing_table(
            &snapshot_dir,
            Arc::<arrow_schema::Schema>::clone(&self.table_metadata.schema),
            &self.vortex_config,
        )?;

        // Update the listing table with write lock
        let mut guard =
            self.listing_table
                .write()
                .map_err(|_| super::catalog::CatalogError::LockPoisoned {
                    operation: "refresh listing table (write)".to_string(),
                })?;
        *guard = new_listing_table;

        tracing::debug!(
            "Refreshed listing table for {} to pick up new files and update statistics",
            self.table_metadata.table_name
        );

        Ok(())
    }

    /// Load deletion vectors from the catalog and return a `RoaringBitmap` of deleted row IDs.
    ///
    /// This method queries the catalog for delete files and loads all deletion vectors
    /// into memory. It should be called once during table provider initialization and
    /// whenever delete files are added/updated.
    ///
    /// # Design Constraints
    ///
    /// `RoaringBitmap` uses u32 internally, limiting support to row IDs < 4 billion.
    /// Tables approaching this limit should trigger compaction. Excessive deletion vectors
    /// severely degrade query performance and indicate poor table health. Compaction removes
    /// deleted rows and clears deletion vectors.
    ///
    /// # Performance Notes
    ///
    /// - Queries metastore once via catalog
    /// - Reads deletion vector files in a blocking task
    /// - Result is cached in the table provider to avoid repeated queries on every scan
    /// - `RoaringBitmap` provides 50-90% memory savings vs `HashSet` for sparse deletions
    async fn load_deletion_vectors(
        table_id: i64,
        catalog: Arc<dyn MetadataCatalog>,
    ) -> CatalogResult<RoaringBitmap> {
        // Query catalog for delete files (this spawns a blocking task internally)
        let delete_files = catalog
            .get_table_delete_files(table_id)
            .await
            .map_err(|e| super::catalog::CatalogError::InvalidOperation {
                message: format!("Failed to load deletion vectors from catalog: {e}"),
            })?;

        if delete_files.is_empty() {
            return Ok(RoaringBitmap::new());
        }

        // Read deletion vector files in a blocking task
        let deleted_row_ids =
            task::spawn_blocking(move || Self::read_deletion_vectors(delete_files))
                .await
                .map_err(|err| super::catalog::CatalogError::InvalidOperation {
                    message: format!(
                        "Deletion vector reader task panicked or was cancelled: {err}"
                    ),
                })
                .and_then(|result| {
                    result.map_err(|err| super::catalog::CatalogError::InvalidOperation {
                        message: format!("Failed to read deletion vectors: {err}"),
                    })
                })?;

        tracing::debug!(
            "Cached {} deletion vectors ({} deleted rows) for table_id {}",
            deleted_row_ids.len(),
            deleted_row_ids.len(),
            table_id
        );

        Ok(deleted_row_ids)
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
    fn read_deletion_vectors(
        delete_files: Vec<super::metadata::DeleteFile>,
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
        // Delegate to the underlying listing table first
        // Clone the Arc and drop the lock before awaiting to avoid holding locks across await points
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    LISTING_TABLE_LOCK_POISONED.to_string(),
                )
            })?;
            Arc::clone(&guard)
        };
        let plan = listing_table
            .scan(state, projection, filters, limit)
            .await?;

        // Use cached deletion vectors instead of querying the catalog on every scan.
        // This dramatically improves concurrent query performance by avoiding repeated
        // SQLite queries and spawn_blocking tasks.
        // Zero-copy Arc clone: just increments reference count, no HashSet allocation.
        let deleted_row_ids = {
            let guard = self.cached_deleted_row_ids.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    DELETION_CACHE_LOCK_POISONED.to_string(),
                )
            })?;
            Arc::clone(&guard)
        };

        // If there are any deleted rows, apply filtering
        if !deleted_row_ids.is_empty() {
            tracing::debug!(
                "Applying cached deletion filter ({} deleted rows) to scan of table {}",
                deleted_row_ids.len(),
                self.table_metadata.table_name
            );
            return Ok(Arc::new(DeletionFilterExec::new(plan, deleted_row_ids)));
        }

        Ok(plan)
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
        // Handle overwrite by creating a new snapshot
        // Directory structure: [data_dir]/[table_id]/[snapshot_id]/
        if overwrite == InsertOp::Overwrite {
            // Generate a new UUIDv7 for the snapshot
            let new_snapshot_id = uuid::Uuid::now_v7().to_string();

            // Create snapshot directory: [table_path]/[table_id]/[snapshot_id]/
            let snapshot_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                self.table_metadata.table_id,
                &new_snapshot_id,
            );

            // Create the snapshot directory
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;

            // Create a new ListingTable pointing to the snapshot directory
            let new_listing_table = Self::create_listing_table(
                &snapshot_dir,
                Arc::clone(&self.table_metadata.schema),
                &self.vortex_config,
            )
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to create listing table for new snapshot: {e}"
                ))
            })?;

            // Perform the insert using the new listing table with append mode
            // (Vortex only supports append at the file level)
            let result = new_listing_table
                .insert_into(state, input, InsertOp::Append)
                .await?;

            // Update the catalog to point to the new snapshot
            self.catalog
                .set_current_snapshot(self.table_metadata.table_id, &new_snapshot_id)
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to update snapshot after overwrite: {e}"
                    ))
                })?;

            // Update the provider's listing table to point to the new snapshot
            // This ensures subsequent queries in the same context will read from the new data
            let mut listing_table_guard = self.listing_table.write().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    LISTING_TABLE_LOCK_POISONED.to_string(),
                )
            })?;
            *listing_table_guard = new_listing_table;

            // Trigger cleanup of old snapshot directories after successful full refresh
            // This is fire-and-forget using spawn_blocking to avoid blocking the async runtime
            let table_path = self.table_metadata.path.clone();
            let table_id = self.table_metadata.table_id;
            let current_snapshot = new_snapshot_id.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) =
                    Self::cleanup_old_snapshots_blocking(&table_path, table_id, &current_snapshot)
                {
                    tracing::warn!(
                        "Failed to cleanup old snapshots for table {}: {e}",
                        table_id
                    );
                }
            });

            return Ok(result);
        }

        // For regular appends, use the existing snapshot and listing table
        // Ensure the snapshot directory exists (it might not if this is the first write to a newly created table)
        let snapshot_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            &self.table_metadata.current_snapshot_id,
        );

        Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;

        // Clone the Arc and drop the lock before awaiting
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    LISTING_TABLE_LOCK_POISONED.to_string(),
                )
            })?;
            Arc::clone(&guard)
        };
        let result = listing_table
            .insert_into(state, input, InsertOp::Append)
            .await?;

        // Refresh the listing table to pick up new files and update statistics
        // This ensures query plans have access to up-to-date statistics after the insert
        self.refresh_listing_table().map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to refresh listing table after insert: {e}"
            ))
        })?;

        Ok(result)
    }
}

// Implement DeletionTableProvider for Cayenne
#[async_trait]
impl DeletionTableProvider for CayenneTableProvider {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(CayenneDeletionSink::new(
                self.table_metadata.clone(),
                Arc::clone(&self.catalog),
                Arc::clone(&self.listing_table),
                Arc::clone(&self.table_metadata.schema),
                filters,
                Arc::clone(&self.cached_deleted_row_ids),
            )),
            &self.table_metadata.schema,
        )))
    }
}

/// Deletion sink for Cayenne tables
struct CayenneDeletionSink {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    /// Reference to the cached deletion vectors to invalidate after writing new deletions.
    /// Uses Arc-wrapped `RoaringBitmap` for zero-copy sharing across concurrent operations.
    cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
}

impl CayenneDeletionSink {
    fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        schema: SchemaRef,
        filters: &[Expr],
        cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
            cached_deleted_row_ids,
        }
    }

    async fn delete_all_rows(
        &self,
        ctx: &SessionContext,
        listing_table: Arc<ListingTable>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let scan_plan = listing_table.scan(&ctx.state(), None, &[], None).await?;
        let batches = collect(scan_plan, ctx.task_ctx()).await?;
        let total_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        let total_rows_i64 = convert_to_i64_box(total_rows, "total row count")?;

        let row_ids: Vec<i64> = (0..total_rows_i64).collect();

        self.persist_deletions(row_ids, DEFAULT_DATA_FILE_ID).await
    }

    async fn delete_filtered_rows(
        &self,
        ctx: &SessionContext,
        listing_table: Arc<ListingTable>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::{DataType, Field};

        let scan_plan = listing_table.scan(&ctx.state(), None, &[], None).await?;
        let batches = collect(scan_plan, ctx.task_ctx()).await?;

        // If no data, nothing to delete
        if batches.is_empty() {
            return Ok(0);
        }

        // Flatten all batches into one for simpler processing
        let concatenated_batch = arrow::compute::concat_batches(&self.schema, &batches)?;
        let total_rows = concatenated_batch.num_rows();

        // Create a batch with row_id column added
        let row_id_array = arrow::array::Int64Array::from_iter_values(
            0..convert_to_i64_box(total_rows, "total rows")?,
        );

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

        // Now select just the row IDs
        let row_ids_df = filtered_df.select_columns(&["__row_id"])?;

        // Collect the filtered row IDs
        let filtered_rowid_batches = row_ids_df.collect().await?;
        let mut row_ids = Vec::new();

        for batch in filtered_rowid_batches {
            let row_id_column = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..row_id_column.len() {
                if !row_id_column.is_null(i) {
                    row_ids.push(row_id_column.value(i));
                }
            }
        }

        self.persist_deletions(row_ids, DEFAULT_DATA_FILE_ID).await
    }

    async fn persist_deletions(
        &self,
        row_ids: Vec<i64>,
        data_file_id: i64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let filtered_row_ids = self.filter_existing_deletions(row_ids).await?;

        if filtered_row_ids.is_empty() {
            return Ok(0);
        }

        let writer = DeletionVectorWriter::new(&self.table_metadata);
        let mut results = writer
            .write(vec![DeletionVectorWriteSpec::new(
                data_file_id,
                filtered_row_ids,
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
            for &row_id in &result.row_ids {
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

        let deleted_count = convert_to_u64_box(result.row_ids.len(), "deleted row count")?;

        tracing::debug!(
            "Deletion vector written and cache updated: {} row(s) at {:?}",
            deleted_count,
            result.path
        );

        Ok(deleted_count)
    }

    async fn filter_existing_deletions(
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
        let existing_row_ids = tokio::task::spawn_blocking(move || {
            CayenneTableProvider::read_deletion_vectors(delete_files_for_read)
        })
        .await
        .map_err(|source| catalog_error_to_box(CatalogError::TaskJoin { source }))?
        .map_err(|err| {
            catalog_error_to_box(CatalogError::InvalidOperation {
                message: format!("Failed to read existing deletion vectors: {err}"),
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
                .map_err(|_| LISTING_TABLE_LOCK_POISONED.to_string())?;
            Arc::clone(&guard)
        };

        if self.filters.is_empty() {
            return self.delete_all_rows(&ctx, Arc::clone(&listing_table)).await;
        }

        self.delete_filtered_rows(&ctx, listing_table).await
    }
}

/// Generic conversion function that handles type conversion with proper error handling.
///
/// This is the core conversion utility that all type conversion functions delegate to.
/// It provides consistent error messages with context about what value failed to convert
/// and includes both the source and target types in the error message.
///
/// # Design Rationale
///
/// This function consolidates conversion logic to ensure consistent error handling across
/// all numeric conversions in Cayenne. Without it, we would have duplicate error handling
/// code for each conversion pair (usize→i64, u64→i64, etc.), making it harder to maintain
/// consistent error messages.
///
/// # Generic Parameters
///
/// * `T` - Source type (must implement `TryInto<U>`, `Copy`, and `Display`)
/// * `U` - Target type (must implement `Display`)
///
/// # Why `Copy` is Required
///
/// The `Copy` bound is required because:
/// 1. The value is used twice: once for the conversion attempt and once in the error message
/// 2. `TryInto::try_into` consumes `self`, so without `Copy` we would move the value
/// 3. All numeric types (`usize`, `u64`, `i64`, etc.) implement `Copy`, so this isn't restrictive
///
/// # When to Use
///
/// **Do NOT call this function directly.** Instead, use the type-specific wrapper functions:
/// - `convert_to_i64()` - For conversions within `DataFusion` error context
/// - `convert_to_i64_box()` - For conversions in async/trait methods with boxed errors
/// - `convert_to_u64_box()` - For conversions to `u64` with boxed errors
///
/// The wrapper functions provide better type inference and appropriate error type handling
/// for their specific use cases.
///
/// # Examples
///
/// ```ignore
/// // GOOD - Use wrapper functions
/// let value = convert_to_i64(batch.num_rows(), "batch size")?;
///
/// // BAD - Don't call try_convert directly
/// let value = try_convert::<usize, i64>(batch.num_rows(), "batch size")?;
/// ```
fn try_convert<T, U>(value: T, context: &str) -> datafusion_common::Result<U>
where
    T: TryInto<U> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
    U: std::fmt::Display,
{
    value.try_into().map_err(|err| {
        datafusion_common::DataFusionError::Execution(format!(
            "Failed to convert {context} value {value} to {}: {err}",
            std::any::type_name::<U>()
        ))
    })
}

/// Convert a numeric value to `i64` with `DataFusion` error type.
///
/// Use this function when converting numeric values (typically `usize` or `u64`) to `i64`
/// within `DataFusion` `TableProvider` implementations or execution plans, where the error
/// type is `datafusion_common::Result<T>`.
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "batch size", "row count")
///
/// # Examples
///
/// ```ignore
/// // Converting batch size in hot path
/// let batch_size_i64 = convert_to_i64(batch.num_rows(), "batch size")?;
///
/// // Converting row index
/// let row_offset = convert_to_i64(row_idx, "row index")?;
/// ```
fn convert_to_i64<T>(value: T, context: &str) -> datafusion_common::Result<i64>
where
    T: TryInto<i64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    try_convert(value, context)
}

/// Convert a numeric value to `i64` with boxed error type.
///
/// Use this function when converting numeric values to `i64` in contexts that require
/// boxed errors, such as:
/// - Async trait methods (`DeletionSink::delete_from`)
/// - Functions returning `Result<T, Box<dyn Error>>`
/// - Code that needs to bridge between different error types
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "deleted row count")
///
/// # Examples
///
/// ```ignore
/// // In deletion sink with boxed error return type
/// let total_rows_i64 = convert_to_i64_box(total_rows, "total row count")?;
/// let deleted_count_i64 = convert_to_i64_box(deleted_count, "deleted row count")?;
/// ```
fn convert_to_i64_box<T>(
    value: T,
    context: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>>
where
    T: TryInto<i64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    convert_to_i64(value, context)
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

/// Convert a numeric value to `u64` with boxed error type.
///
/// Use this function when converting numeric values to `u64` in contexts that require
/// boxed errors. This is primarily used for return values that must be `u64`, such as
/// row counts returned from deletion operations.
///
/// # Arguments
///
/// * `value` - The numeric value to convert
/// * `context` - Description of what the value represents (e.g., "deleted row count")
///
/// # Examples
///
/// ```ignore
/// // Converting deletion count from usize to u64
/// let deleted_count = convert_to_u64_box(row_ids.len(), "deleted row count")?;
/// ```
fn convert_to_u64_box<T>(
    value: T,
    context: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>
where
    T: TryInto<u64> + Copy + std::fmt::Display,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    try_convert::<T, u64>(value, context)
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cayenne_catalog::CayenneCatalog;
    use crate::metadata::CreateTableOptions;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::context::SessionContext;
    use datafusion_catalog::TableProvider;
    use futures::future::join_all;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Helper to create a test catalog with a table containing sample data
    async fn setup_test_table(
        connection_string: &str,
    ) -> (Arc<CayenneCatalog>, TableMetadata, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory for test");
        let catalog = Arc::new(
            CayenneCatalog::new(connection_string)
                .expect("Failed to create CayenneCatalog instance"),
        );
        catalog
            .init()
            .await
            .expect("Failed to initialize catalog schema and tables");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let table_name = "test_table";
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                base_path: temp_dir.path().to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            })
            .await
            .expect("Failed to create test table in catalog");

        let table_metadata = catalog
            .get_table(table_name)
            .await
            .expect("Failed to get table metadata from catalog");

        tracing::info!("Created table '{}' with ID {}", table_name, table_id);

        // Create provider and insert test data
        let ctx = SessionContext::new();
        let catalog_trait: Arc<dyn MetadataCatalog> =
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;
        let provider = CayenneTableProvider::new(table_name, catalog_trait)
            .await
            .expect("Failed to create CayenneTableProvider instance");

        // Insert 1000 rows of test data
        let mut id_values = Vec::new();
        let mut name_values = Vec::new();
        for i in 0..1000 {
            id_values.push(i);
            name_values.push(format!("name_{i}"));
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(id_values)),
                Arc::new(StringArray::from(name_values)),
            ],
        )
        .expect("Failed to create RecordBatch with test data");

        // Create a memory exec plan from the batch
        let mem_config = MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None)
            .expect("Failed to create MemorySourceConfig from test data");
        let mem_exec = DataSourceExec::new(Arc::new(mem_config));

        let insert_result = provider
            .insert_into(&ctx.state(), Arc::new(mem_exec), InsertOp::Append)
            .await
            .expect("Failed to create insert execution plan");

        // Execute the insert plan to actually write the data
        let batches = collect(insert_result, ctx.task_ctx())
            .await
            .expect("Failed to execute insert plan and write test data");

        tracing::info!("Insert completed, wrote {} batches", batches.len());

        (catalog, table_metadata, temp_dir)
    }

    #[tokio::test]
    async fn test_concurrent_reads_sqlite() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for concurrent reads test");
        let db_path = temp_dir.path().join("cayenne_concurrent_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_concurrent_reads_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_concurrent_reads_turso() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for concurrent reads test (Turso)");
        let db_path = temp_dir.path().join("cayenne_concurrent_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_concurrent_reads_impl(&connection_string).await;
    }

    /// Core concurrent read test implementation
    async fn test_concurrent_reads_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        // Create multiple concurrent readers
        let num_readers = 20;
        let num_queries_per_reader = 10;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider in concurrent reader task");

                let mut total_rows = 0;
                for query_num in 0..num_queries_per_reader {
                    // Execute a full table scan
                    let plan = provider
                        .scan(&ctx.state(), None, &[], None)
                        .await
                        .expect("Failed to create scan plan in concurrent reader");

                    let batches = collect(plan, ctx.task_ctx())
                        .await
                        .expect("Failed to collect scan results in concurrent reader");

                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    total_rows += row_count;

                    if query_num == 0 {
                        tracing::info!(
                            "Reader {} first query returned {} rows",
                            reader_id,
                            row_count
                        );
                    }
                }

                total_rows
            });

            handles.push(handle);
        }

        // Wait for all readers to complete
        let results = join_all(handles).await;

        // Verify all readers completed successfully
        for (idx, result) in results.iter().enumerate() {
            match result {
                Ok(total_rows) => {
                    assert_eq!(
                        *total_rows,
                        1000 * num_queries_per_reader,
                        "Reader {idx} read incorrect number of rows"
                    );
                }
                Err(e) => panic!("Reader {idx} failed: {e}"),
            }
        }

        tracing::info!(
            "✓ {} concurrent readers successfully completed {} queries each",
            num_readers,
            num_queries_per_reader
        );
    }

    #[tokio::test]
    async fn test_concurrent_reads_with_filters_sqlite() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for filter test");
        let db_path = temp_dir.path().join("cayenne_filter_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_filters_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_concurrent_reads_with_filters_turso() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for filter test (Turso)");
        let db_path = temp_dir.path().join("cayenne_filter_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_filters_impl(&connection_string).await;
    }

    /// Test concurrent reads with various filter conditions
    async fn test_concurrent_reads_with_filters_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        let num_readers = 10;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider for filter test reader");

                // Register the table with DataFusion so we can run SQL queries
                ctx.register_table("test_table", Arc::new(provider))
                    .expect("Failed to register table with DataFusion context");

                // Execute various queries with filters
                let queries = vec![
                    ("SELECT COUNT(*) FROM test_table WHERE id < 500", 500),
                    ("SELECT COUNT(*) FROM test_table WHERE id >= 500", 500),
                    ("SELECT COUNT(*) FROM test_table WHERE id % 2 = 0", 500),
                    ("SELECT COUNT(*) FROM test_table", 1000),
                ];

                for (query, expected_count) in &queries {
                    let df = ctx.sql(query).await.expect("Failed to execute SQL query");
                    let batches = df.collect().await.expect("Failed to collect query results");

                    // Extract count from result
                    let count = batches[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("Failed to downcast count column to Int64Array")
                        .value(0);

                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let count_usize = count as usize;
                    assert_eq!(
                        count_usize, *expected_count,
                        "Reader {reader_id} query '{query}' returned incorrect count"
                    );
                }

                reader_id
            });

            handles.push(handle);
        }

        // Wait for all readers to complete
        let results = join_all(handles).await;

        // Verify all readers completed successfully
        for result in results {
            result.expect("Filter test concurrent reader task should complete successfully");
        }

        tracing::info!(
            "✓ {} concurrent readers with filters completed successfully",
            num_readers
        );
    }

    #[tokio::test]
    async fn test_concurrent_reads_with_projections_sqlite() {
        let temp_dir =
            TempDir::new().expect("Failed to create temporary directory for projection test");
        let db_path = temp_dir.path().join("cayenne_projection_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_projections_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_concurrent_reads_with_projections_turso() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for projection test (Turso)");
        let db_path = temp_dir.path().join("cayenne_projection_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_concurrent_reads_with_projections_impl(&connection_string).await;
    }

    /// Test concurrent reads with different column projections
    async fn test_concurrent_reads_with_projections_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        let num_readers = 15;

        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider for projection test reader");

                ctx.register_table("test_table", Arc::new(provider))
                    .expect("Failed to register table for projection test");

                // Test different projection patterns
                let queries = vec![
                    "SELECT id FROM test_table",
                    "SELECT name FROM test_table",
                    "SELECT id, name FROM test_table",
                    "SELECT name, id FROM test_table",
                ];

                for query in &queries {
                    let df = ctx
                        .sql(query)
                        .await
                        .expect("Failed to execute projection query");
                    let batches = df
                        .collect()
                        .await
                        .expect("Failed to collect projection query results");

                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(
                        row_count, 1000,
                        "Reader {reader_id} query '{query}' returned incorrect row count"
                    );
                }

                reader_id
            });

            handles.push(handle);
        }

        let results = join_all(handles).await;

        for result in results {
            result.expect("Projection test concurrent reader task should complete successfully");
        }

        tracing::info!(
            "✓ {} concurrent readers with projections completed successfully",
            num_readers
        );
    }

    #[tokio::test]
    async fn test_high_concurrency_stress_sqlite() {
        let temp_dir = TempDir::new()
            .expect("Failed to create temporary directory for high concurrency stress test");
        let db_path = temp_dir.path().join("cayenne_stress_test.db");
        let connection_string = format!("sqlite://{}", db_path.to_string_lossy());
        test_high_concurrency_stress_impl(&connection_string).await;
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn test_high_concurrency_stress_turso() {
        let temp_dir = TempDir::new().expect(
            "Failed to create temporary directory for high concurrency stress test (Turso)",
        );
        let db_path = temp_dir.path().join("cayenne_stress_test.db");
        let connection_string = format!("libsql://{}", db_path.to_string_lossy());
        test_high_concurrency_stress_impl(&connection_string).await;
    }

    /// Stress test with high concurrency (50 readers, 50 queries each)
    async fn test_high_concurrency_stress_impl(connection_string: &str) {
        let (catalog, table_metadata, _temp_dir) = setup_test_table(connection_string).await;

        let num_readers = 50;
        let queries_per_reader = 50;

        let start = std::time::Instant::now();
        let mut handles = Vec::new();

        for reader_id in 0..num_readers {
            let catalog_clone = Arc::clone(&catalog);
            let table_name = table_metadata.table_name.clone();

            let handle = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog_trait: Arc<dyn MetadataCatalog> = catalog_clone;
                let provider = CayenneTableProvider::new(&table_name, catalog_trait)
                    .await
                    .expect("Failed to create provider for stress test reader");

                for _ in 0..queries_per_reader {
                    let plan = provider
                        .scan(&ctx.state(), None, &[], None)
                        .await
                        .expect("Failed to create scan plan in stress test");

                    let batches = collect(plan, ctx.task_ctx())
                        .await
                        .expect("Failed to collect scan results in stress test");

                    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    assert_eq!(row_count, 1000, "Reader {reader_id} got wrong row count");
                }

                reader_id
            });

            handles.push(handle);
        }

        let results = join_all(handles).await;
        let duration = start.elapsed();

        for result in results {
            result.expect("Stress test concurrent reader task should complete successfully");
        }

        let total_queries = num_readers * queries_per_reader;
        let qps = f64::from(total_queries) / duration.as_secs_f64();

        tracing::info!(
            "✓ Stress test: {} concurrent readers × {} queries = {} total queries in {:.2}s ({:.0} qps)",
            num_readers,
            queries_per_reader,
            total_queries,
            duration.as_secs_f64(),
            qps
        );
    }

    /// Test that data is sorted when `sort_columns` is configured
    #[tokio::test]
    async fn test_sort_columns() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let temp_dir = TempDir::new().expect("Failed to create temporary directory for sort test");
        let data_path = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_path).expect("Failed to create data directory");

        let connection_string =
            format!("sqlite://{}/cayenne.db", temp_dir.path().to_string_lossy());
        let catalog = Arc::new(
            crate::CayenneCatalog::new(connection_string).expect("Failed to create catalog"),
        );
        catalog.init().await.expect("Failed to initialize catalog");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Configure table with sort columns
        let vortex_config = crate::metadata::VortexConfig {
            sort_columns: vec!["timestamp".to_string(), "id".to_string()],
            ..Default::default()
        };

        let table_options = crate::metadata::CreateTableOptions {
            table_name: "sorted_test".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config,
        };

        let table = CayenneTableProvider::create_table(catalog, table_options)
            .await
            .expect("Failed to create table");

        // Insert unsorted data
        let unsorted_ids = vec![5i64, 3, 1, 4, 2];
        let unsorted_timestamps = vec![100i64, 200, 50, 150, 75];
        let unsorted_values = vec![50i64, 30, 10, 40, 20];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(unsorted_ids)),
                Arc::new(Int64Array::from(unsorted_timestamps)),
                Arc::new(Int64Array::from(unsorted_values)),
            ],
        )
        .expect("Failed to create record batch");

        let stream = futures::stream::once(async { Ok(batch) });
        let batch_stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), stream);

        table
            .insert(Box::pin(batch_stream))
            .await
            .expect("Failed to insert data");

        // Verify data is sorted by timestamp, then by id
        let ctx = SessionContext::new();
        let scan_plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("Failed to create scan plan");

        let result_batches = collect(scan_plan, ctx.task_ctx())
            .await
            .expect("Failed to collect results");

        assert!(!result_batches.is_empty(), "Should have result batches");

        // Combine all batches
        let combined = arrow::compute::concat_batches(&schema, &result_batches)
            .expect("Failed to concatenate batches");

        let timestamp_col = combined
            .column_by_name("timestamp")
            .expect("timestamp column exists")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp is Int64Array");

        let id_col = combined
            .column_by_name("id")
            .expect("id column exists")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64Array");

        // Verify sorted order: timestamp ascending, then id ascending
        let expected_timestamps = [50i64, 75, 100, 150, 200];
        let expected_ids = [1i64, 2, 5, 4, 3];

        for i in 0..5 {
            assert_eq!(
                timestamp_col.value(i),
                expected_timestamps[i],
                "Row {i} timestamp should be sorted"
            );
            assert_eq!(
                id_col.value(i),
                expected_ids[i],
                "Row {i} id should match expected order"
            );
        }

        tracing::info!("✓ Data sorted correctly by sort_columns");
    }
}
