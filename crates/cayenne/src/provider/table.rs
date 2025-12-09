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

//! Cayenne `TableProvider` implementation.
//!
//! This module contains the main `CayenneTableProvider` struct which implements
//! `DataFusion`'s `TableProvider` trait for Cayenne tables.

use super::constants::LISTING_TABLE_LOCK_POISONED;
use super::delete::{read_deletion_vectors, CayenneDeletionSink, DeletionFilterExec};
use super::streaming::StreamingExec;
use crate::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use crate::metadata::{CreateTableOptions, TableMetadata};
use crate::provider::scan::CayenneAccelerationExec;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionTableProvider};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::Constraints;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::collect;
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use roaring::RoaringBitmap;
use std::any::Any;
use std::borrow::Cow;
use std::sync::{Arc, RwLock};
use tokio::task;
use vortex::VortexSessionDefault;
use vortex_datafusion::VortexFormat;
use vortex_session::VortexSession;

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
    vortex_config: crate::metadata::VortexConfig,
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
        vortex_config: &crate::metadata::VortexConfig,
    ) -> CatalogResult<Arc<ListingTable>> {
        let dir_url_str = Self::dir_to_url_string(snapshot_dir);

        let table_url =
            ListingTableUrl::parse(&dir_url_str).map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to parse table URL: {e}"),
            })?;

        // Create a configured Vortex session with selected encodings
        let vortex_session = VortexSession::default();

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

        let listing_table =
            ListingTable::try_new(config).map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to create listing table: {e}"),
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
                    return Err(CatalogError::InvalidOperation {
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
            let batch = batch_result.map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to read batch from stream: {e}"),
            })?;

            let batch_size = batch.get_array_memory_size();

            // If adding this batch would exceed target size and we have data, write current chunk
            if current_size + batch_size > target_size_bytes && !current_chunk.is_empty() {
                // Acquire semaphore permit before spawning write task
                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
                    CatalogError::InvalidOperation {
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
                CatalogError::InvalidOperation {
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
            let row_count = result.map_err(|e| CatalogError::InvalidOperation {
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
        let sorted_stream =
            util::stream_utils::sort_stream(stream, &self.vortex_config.sort_columns, &task_ctx)
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
    async fn write_chunk(&self, chunk: Vec<RecordBatch>) -> CatalogResult<u64> {
        if chunk.is_empty() {
            return Ok(0);
        }

        let schema = chunk[0].schema();
        let row_count: u64 = chunk.iter().map(|b| b.num_rows() as u64).sum();

        // Create a stream from the chunk batches
        let batch_stream = futures::stream::iter(chunk.into_iter().map(Ok));
        let chunk_stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), batch_stream);

        let stream_exec = Arc::new(StreamingExec::new(
            Arc::clone(&schema),
            Box::pin(chunk_stream),
        ));

        // Create a session context for executing the insert
        let ctx = SessionContext::new();
        let state = ctx.state();

        // Delegate to ListingTable's insert_into to write Vortex files
        // Clone the Arc and drop the lock before awaiting
        let listing_table = {
            let guard = self
                .listing_table
                .read()
                .map_err(|_| CatalogError::LockPoisoned {
                    operation: "write_chunk (read listing table)".to_string(),
                })?;
            Arc::clone(&guard)
        };
        let insert_plan = listing_table
            .insert_into(&state, stream_exec, InsertOp::Append)
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to create insert plan for chunk: {e}"),
            })?;

        // Execute the insert plan
        collect(insert_plan, state.task_ctx()).await.map_err(|e| {
            CatalogError::InvalidOperation {
                message: format!("Failed to execute insert for chunk: {e}"),
            }
        })?;

        tracing::debug!("Wrote chunk with {} rows to Vortex", row_count);

        Ok(row_count)
    }

    async fn apply_retention_filters(&self) -> CatalogResult<u64> {
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
        Err(CatalogError::InvalidOperation {
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
        Err(CatalogError::InvalidOperation {
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
        let mut guard = self
            .listing_table
            .write()
            .map_err(|_| CatalogError::LockPoisoned {
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
            .map_err(|e| CatalogError::InvalidOperation {
                message: format!("Failed to load deletion vectors from catalog: {e}"),
            })?;

        if delete_files.is_empty() {
            return Ok(RoaringBitmap::new());
        }

        // Read deletion vector files in a blocking task
        let deleted_row_ids = task::spawn_blocking(move || read_deletion_vectors(delete_files))
            .await
            .map_err(|err| CatalogError::InvalidOperation {
                message: format!("Deletion vector reader task panicked or was cancelled: {err}"),
            })
            .and_then(|result| {
                result.map_err(|err| CatalogError::InvalidOperation {
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
                    super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
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
