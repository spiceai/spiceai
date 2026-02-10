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
    DEFAULT_DATA_FILE_ID, DELETION_CACHE_LOCK_POISONED, LISTING_TABLE_LOCK_POISONED,
};
use super::delete::{
    is_pk_visible_i64, is_pk_visible_row_key, CayenneDeletionSink, DeletionIdentifier,
    DeletionVectorWriteSpec, DeletionVectorWriter, Int64PkDeletionFilterExec,
    KeyBasedDeletionFilterExec,
};
use super::streaming::StreamingExec;
use crate::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use crate::metadata::{CreateTableOptions, TableMetadata};
use crate::provider::scan::CayenneAccelerationExec;
use crate::provider::sink::CayenneDataSink;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionTableProvider};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::Constraints;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_table_providers::util::constraints::UpsertOptions;
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectStorePath;
use roaring::RoaringBitmap;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::task;
use vortex_datafusion::VortexFormat;

use super::context::CayenneContext;
use super::deletion_strategy::{PkDeletionStrategy, PkDeletionStrategyWithCache};
use super::vortex_format::DeletionFilteringVortexFormat;

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
/// let provider = CayenneTableProviderBuilder::new(catalog)
///     .with_retention_filters(filters)
///     .with_object_store(config)
///     .open("my_table").await?;
///
/// // Create a new table
/// let provider = CayenneTableProviderBuilder::new(catalog)
///     .with_retention_filters(filters)
///     .create(options).await?;
/// ```
#[derive(Clone)]
pub struct CayenneTableProviderBuilder {
    catalog: Arc<dyn MetadataCatalog>,
    retention_filters: Vec<Expr>,
    object_store_config: Option<crate::metadata::ObjectStoreConfig>,
    context: Option<Arc<CayenneContext>>,
}

impl CayenneTableProviderBuilder {
    /// Create a new builder with the required catalog.
    #[must_use]
    pub fn new(catalog: Arc<dyn MetadataCatalog>) -> Self {
        Self {
            catalog,
            retention_filters: Vec::new(),
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
    pub async fn open(self, table_name: &str) -> CatalogResult<CayenneTableProvider> {
        CayenneTableProvider::new_internal(
            table_name,
            self.catalog,
            self.retention_filters,
            self.object_store_config,
            self.context,
        )
        .await
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
            self.object_store_config,
            self.context,
        )
        .await
    }
}

#[derive(Debug, Clone, Copy)]
struct RowLocation {
    data_file_id: i64,
    row_id: i64,
}

struct BatchValidationResult {
    filtered_batch: Option<RecordBatch>,
    delete_specs: Vec<(i64, Vec<i64>)>,
    kept_keys: HashSet<OwnedRow>,
    /// Int64 PK values being deleted (for `Int64Pk` strategy)
    deleted_pk_i64: Vec<i64>,
    /// Row key bytes being deleted (for `RowConverterBased` strategy)
    deleted_row_keys: Vec<Box<[u8]>>,
}

/// Result of on-conflict validation containing deleted PK information.
struct OnConflictValidationResult {
    filtered_batches: Vec<RecordBatch>,
    delete_specs: HashMap<i64, Vec<i64>>,
    /// Deleted Int64 PK values (for `Int64Pk` strategy)
    deleted_pk_i64: Vec<i64>,
    /// Deleted row keys (for `RowConverterBased` strategy)
    deleted_row_keys: Vec<Box<[u8]>>,
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

    /// Returns the path to a snapshot directory for this table.
    #[must_use]
    pub(crate) fn snapshot_dir_path_for(&self, snapshot_id: &str) -> std::path::PathBuf {
        Self::snapshot_dir_path(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            snapshot_id,
        )
    }

    /// Atomically commit an overwrite operation to the catalog.
    ///
    /// This clears any existing delete files since overwrite replaces all data.
    pub(crate) async fn commit_overwrite(&self, new_snapshot_id: &str) -> CatalogResult<()> {
        self.catalog
            .commit_compaction(self.table_metadata.table_id, new_snapshot_id)
            .await
    }

    /// Update the listing table to point to a new snapshot directory.
    ///
    /// This ensures subsequent queries in the same context will read from the new data.
    pub(crate) fn update_listing_table_for_snapshot(
        &self,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            self.table_metadata.table_id,
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
                .map_err(|_| CatalogError::LockPoisoned {
                    operation: "update listing table for snapshot".to_string(),
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
                    self.table_metadata.table_id
                );
            }
        } else {
            let table_path = self.table_metadata.path.clone();
            let table_id = self.table_metadata.table_id;
            let current_snapshot = current_snapshot.to_string();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = Self::cleanup_old_snapshots_blocking(
                    &table_path,
                    table_id,
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

    fn require_object_store(&self) -> CatalogResult<&crate::metadata::ObjectStoreConfig> {
        self.object_store_config
            .as_ref()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "S3 storage requires an object_store_config".to_string(),
                source: Box::new(std::io::Error::other("missing object store configuration")),
            })
    }

    fn snapshot_object_store_prefix(
        &self,
        snapshot_id: &str,
    ) -> CatalogResult<Option<ObjectStorePath>> {
        if !self.table_metadata.path.starts_with("s3://") {
            return Ok(None);
        }

        let snapshot_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            snapshot_id,
        );

        let url = url::Url::parse(&snapshot_url).map_err(|e| CatalogError::InvalidOperation {
            message: format!("Failed to parse snapshot URL {snapshot_url}"),
            source: Box::new(e),
        })?;

        let host = url.host_str().unwrap_or_default();
        let config = self.require_object_store()?;
        let config_host = config.url.host_str().unwrap_or_default();

        if !config_host.is_empty() && !host.is_empty() && config_host != host {
            return Err(CatalogError::InvalidOperation {
                message: format!(
                    "Snapshot host {host} does not match configured object store host {config_host}"
                ),
                source: Box::new(std::io::Error::other("host mismatch")),
            });
        }

        let path = url.path().trim_start_matches('/');
        Ok(Some(ObjectStorePath::from(path)))
    }

    async fn delete_prefix_with_object_store(&self, prefix: &ObjectStorePath) -> CatalogResult<()> {
        let config = self.require_object_store()?;
        let objects: Vec<_> = config
            .store
            .list(Some(prefix))
            .try_collect()
            .await
            .map_err(|source| CatalogError::InvalidOperation {
                message: "Failed to list objects for snapshot cleanup".to_string(),
                source: Box::new(source),
            })?;

        for meta in objects {
            config
                .store
                .delete(&meta.location)
                .await
                .map_err(|source| CatalogError::InvalidOperation {
                    message: format!(
                        "Failed to delete object {} from snapshot cleanup",
                        meta.location
                    ),
                    source: Box::new(source),
                })?;
        }

        Ok(())
    }

    async fn cleanup_old_snapshots_s3(
        &self,
        current_snapshot: &str,
        protected_snapshot_ids: &HashSet<String>,
    ) -> CatalogResult<()> {
        let config = self.require_object_store()?;

        let base_url = url::Url::parse(&self.table_metadata.path).map_err(|e| {
            CatalogError::InvalidOperation {
                message: format!(
                    "Failed to parse table path for snapshot cleanup: {}",
                    self.table_metadata.path
                ),
                source: Box::new(e),
            }
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
            .map_err(|source| CatalogError::InvalidOperation {
                message: "Failed to list snapshots for cleanup".to_string(),
                source: Box::new(source),
            })?;

        for common_prefix in list_result.common_prefixes {
            if let Some(snapshot_id) = common_prefix.parts().last() {
                let snapshot_id_str = snapshot_id.as_ref();
                // Skip current snapshot and protected snapshots
                if snapshot_id_str == current_snapshot
                    || protected_snapshot_ids.contains(snapshot_id_str)
                {
                    tracing::debug!("Keeping snapshot: {snapshot_id_str} (current or protected)");
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
    ) -> CatalogResult<Arc<ListingTable>> {
        let table_url = ListingTableUrl::parse(snapshot_dir_url).map_err(|e| {
            CatalogError::InvalidOperation {
                message: format!("Failed to parse table URL '{snapshot_dir_url}'."),
                source: Box::new(e),
            }
        })?;

        let listing_options = Self::create_listing_options(vortex_format, strategy);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let listing_table =
            ListingTable::try_new(config).map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to create listing table.".to_string(),
                source: Box::new(e),
            })?;

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
        ListingOptions::new(file_format).with_session_config_options(&SessionConfig::default())
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
    fn snapshot_dir_url(table_path: &str, table_id: i64, snapshot_id: &str) -> String {
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
    ) -> datafusion_common::Result<()> {
        if !snapshot_dir.exists() {
            tokio::fs::create_dir_all(snapshot_dir)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        }
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
        tokio::task::spawn_blocking(move || {
            // Open the directory and call sync_all to flush metadata
            let dir = std::fs::File::open(&snapshot_dir)
                .map_err(|source| CatalogError::IoError { source })?;
            dir.sync_all()
                .map_err(|source| CatalogError::IoError { source })?;
            Ok::<(), CatalogError>(())
        })
        .await
        .map_err(|e| CatalogError::InvalidOperation {
            message: "Directory sync task panicked".to_string(),
            source: Box::new(e),
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
        table_id: i64,
        current_snapshot_id: &str,
        protected_snapshot_ids: &HashSet<String>,
    ) -> CatalogResult<()> {
        let table_dir = std::path::PathBuf::from(table_path).join(table_id.to_string());

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

            // Skip the current snapshot and protected snapshots
            if snapshot_id == current_snapshot_id || protected_snapshot_ids.contains(snapshot_id) {
                tracing::debug!("Keeping snapshot: {} (current or protected)", snapshot_id);
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
    pub async fn new(table_name: &str, catalog: Arc<dyn MetadataCatalog>) -> CatalogResult<Self> {
        CayenneTableProviderBuilder::new(catalog)
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
    ) -> CatalogResult<Self> {
        CayenneTableProviderBuilder::new(catalog)
            .with_retention_filters(retention_filters)
            .open(table_name)
            .await
    }

    /// Internal constructor used by the builder.
    async fn new_internal(
        table_name: &str,
        catalog: Arc<dyn MetadataCatalog>,
        retention_filters: Vec<Expr>,
        object_store_config: Option<crate::metadata::ObjectStoreConfig>,
        context: Option<Arc<CayenneContext>>,
    ) -> CatalogResult<Self> {
        let table_metadata = catalog.get_table(table_name).await?;

        if table_metadata.path.starts_with("s3://") && object_store_config.is_none() {
            return Err(CatalogError::InvalidOperation {
                message: format!(
                    "Table {table_name} uses S3 storage but no object_store_config was provided"
                ),
                source: Box::new(std::io::Error::other("missing object store configuration")),
            });
        }

        // Construct URL to current snapshot
        // Directory structure: [table_path]/[table_id]/[snapshot_id]/
        // All tables have a snapshot ID (created on table initialization)
        let snapshot_dir_url = Self::snapshot_dir_url(
            &table_metadata.path,
            table_metadata.table_id,
            &table_metadata.current_snapshot_id,
        );

        // Use provided context or create a new one from table metadata config
        let context = context.unwrap_or_else(|| CayenneContext::new(&table_metadata.vortex_config));

        // Determine if this table has a primary key for key-based deletion
        let has_primary_key = !table_metadata.primary_key.is_empty();

        // Determine PK deletion strategy kind and build RowConverter if needed
        let (pk_deletion_strategy_kind, pk_row_converter, pk_column_indices) = if has_primary_key {
            let schema = &table_metadata.schema;
            let mut indices = Vec::with_capacity(table_metadata.primary_key.len());
            let mut pk_fields = Vec::with_capacity(table_metadata.primary_key.len());

            for pk_col in &table_metadata.primary_key {
                let (idx, field) = schema.column_with_name(pk_col).ok_or_else(|| {
                    CatalogError::InvalidOperation {
                        message: format!(
                            "Primary key column '{pk_col}' not found in schema for table {table_name}"
                        ),
                        source: Box::new(std::io::Error::other("missing primary key column")),
                    }
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

                let row_converter =
                    RowConverter::new(sort_fields).map_err(|e| CatalogError::InvalidOperation {
                        message: "Failed to create RowConverter for primary key columns"
                            .to_string(),
                        source: Box::new(e),
                    })?;

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
        let table_id = table_metadata.table_id;
        let catalog_for_load = Arc::clone(&catalog);
        let pk_deletion_strategy =
            Self::load_deletion_vectors_all(table_id, catalog_for_load, pk_deletion_strategy_kind)
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
            Self::load_protected_snapshots(Arc::clone(&catalog), table_id, &pk_deletion_strategy)
                .await?;

        Ok(Self {
            current_snapshot_id: Arc::new(RwLock::new(table_metadata.current_snapshot_id.clone())),
            table_metadata,
            catalog,
            listing_table: Arc::new(RwLock::new(listing_table)),
            retention_filters,
            context,
            pk_deletion_strategy,
            pk_row_converter,
            pk_column_indices,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            object_store_config,
            protected_snapshots: Arc::new(RwLock::new(protected_snapshots)),
        })
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
    ) -> CatalogResult<Self> {
        CayenneTableProviderBuilder::new(catalog)
            .create(options)
            .await
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
    ) -> CatalogResult<Self> {
        CayenneTableProviderBuilder::new(catalog)
            .with_retention_filters(retention_filters)
            .create(options)
            .await
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
    /// and defaults to 128 MB.
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
    /// **Within a single insert**, chunks are written in parallel with bounded concurrency
    /// (configurable via `VortexConfig.upload_concurrency`, default 4) for optimal I/O throughput.
    /// The serialization only applies across different `insert()` calls.
    ///
    /// This design ensures correctness while maintaining high performance for individual inserts.
    /// If you need higher write concurrency, consider partitioning your data across multiple tables.
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be inserted.
    pub(crate) async fn insert(&self, stream: SendableRecordBatchStream) -> CatalogResult<u64> {
        // Acquire write lock to serialize inserts and prevent concurrent write races.
        // This ensures listing table refresh happens after all parallel chunk writes complete
        // and retention filters are applied atomically.
        let _write_guard = self.write_lock.lock().await;

        // Check for pending deletions based on the deletion strategy.
        //
        // POSITION-BASED STRATEGY: No compaction needed on insert.
        // Deletion vectors are tracked per-file (HashMap<file_path, RoaringBitmap>), so each
        // file's deletion bitmap is independent. Adding new files doesn't affect existing
        // deletion vectors - the new files simply have no entries in the deletion cache.
        // Compaction would be wasteful here and can cause issues if retention filters
        // re-run on the compacted data.
        //
        // PK-BASED STRATEGIES (Int64Pk, RowConverterBased): Use anti-deletions to avoid compaction.
        // New data is written to a new snapshot with higher sequence number, ensuring proper
        // Iceberg-style ordering where deletions only apply to earlier snapshots.
        let has_pending_deletions = self.has_pending_deletions()?;

        // For PK-based strategies with pending deletions, we need to write to a NEW snapshot
        // with a higher sequence number. This ensures proper Iceberg-style ordering:
        // - Deletions apply to snapshots with sequence <= delete_sequence
        // - New data in snapshots with sequence > delete_sequence is visible
        //
        // We still need to run validate_on_conflict() on the incoming stream
        // to handle upserts for PKs that already exist in the table. Without this,
        // duplicate PKs would appear in query results.
        //
        // NOTE: This block only applies to PK-based strategies. PositionBased strategy
        // doesn't need special handling - new files are simply added to the current snapshot
        // and existing per-file deletion vectors remain valid.
        if has_pending_deletions && !self.pk_deletion_strategy.is_position_based() {
            let new_sequence = self
                .catalog
                .increment_sequence_number(self.table_metadata.table_id)
                .await?;
            tracing::info!(
                "Table {} has pending PK-based deletions, inserting to new snapshot with seq={}",
                self.table_metadata.table_name,
                new_sequence
            );

            let (prepared_stream, delete_specs, deleted_pk_i64, deleted_row_keys) =
                self.prepare_stream_for_insert(stream).await?;

            tracing::debug!(
                "insert() with pending deletions: delete_specs={} files, deleted_pk_i64={} keys",
                delete_specs.len(),
                deleted_pk_i64.len()
            );

            // Write to new snapshot with the prepared (deduplicated) stream
            let total_rows = self
                .insert_to_new_snapshot_with_sequence(prepared_stream, new_sequence)
                .await?;

            // Update deletion caches for the upserted PKs
            self.apply_on_conflict_deletions(delete_specs, deleted_pk_i64, deleted_row_keys)
                .await?;

            return Ok(total_rows);
        }

        let target_size_bytes = self.context.target_file_size_bytes();

        // If a primary key is configured, enforce on_conflict behavior by materializing
        // the incoming stream, validating keys, and preparing deletion vectors.
        let (prepared_stream, delete_specs, deleted_pk_i64, deleted_row_keys) =
            self.prepare_stream_for_insert(stream).await?;

        tracing::debug!(
            "insert(): delete_specs={} files, deleted_pk_i64={} keys",
            delete_specs.len(),
            deleted_pk_i64.len()
        );

        // Process stream in chunks and write them in parallel with bounded concurrency
        let (total_rows, chunk_count) = self
            .chunk_and_write_parallel(prepared_stream, target_size_bytes)
            .await?;

        tracing::debug!(
            "Insert completed, wrote {} rows to Vortex in {} chunk(s)",
            total_rows,
            chunk_count
        );

        // Apply any deletion vectors generated by on_conflict handling before retention.
        self.apply_on_conflict_deletions(delete_specs, deleted_pk_i64, deleted_row_keys)
            .await?;

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
                        message: "Failed to apply retention filters after insert.".to_string(),
                        source: Box::new(err),
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
        if self.context.has_sort_columns() {
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

    /// Insert data to a NEW snapshot with a specific sequence number.
    ///
    /// This is used when inserting while pending PK-based deletions exist.
    /// By writing to a new snapshot with a higher sequence number, we ensure:
    /// - Old data in previous snapshots is filtered by deletions (`delete_seq` >= `old_snapshot_seq`)
    /// - New data in this snapshot is visible (`new_snapshot_seq` > `delete_seq`)
    ///
    /// This achieves Iceberg-style sequence ordering without rewriting existing files.
    async fn insert_to_new_snapshot_with_sequence(
        &self,
        stream: SendableRecordBatchStream,
        sequence_number: i64,
    ) -> CatalogResult<u64> {
        let target_size_bytes = self.context.target_file_size_bytes();

        // Generate a new snapshot ID
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();

        // Write data to the new snapshot
        let (total_rows, chunk_count) = self
            .chunk_and_write_parallel_to_snapshot(stream, target_size_bytes, &new_snapshot_id)
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
                self.table_metadata.table_id,
                &new_snapshot_id,
                sequence_number,
            )
            .await?;

        // Get the maximum delete sequence from current deletions.
        // This snapshot is protected from deletions with seq <= max_delete_seq.
        let max_delete_seq = self.get_max_delete_sequence()?;

        // Add to protected snapshots so scan applies only NEWER deletions (seq > max_delete_seq)
        // We do NOT clear old protected snapshots because they may contain data that's still valid.
        // Each protected snapshot applies its own partial deletion filter based on when it was created.
        {
            let mut guard =
                self.protected_snapshots
                    .write()
                    .map_err(|_| CatalogError::LockPoisoned {
                        operation: "add protected snapshot".to_string(),
                    })?;
            guard.insert(new_snapshot_id.clone(), max_delete_seq);
        }

        // The listing table stays as-is. Protected snapshots are handled at scan time.
        // See the doc comment above for why we do NOT update current_snapshot.

        Ok(total_rows)
    }

    /// Get the maximum delete sequence number from the cached deletions.
    fn get_max_delete_sequence(&self) -> CatalogResult<i64> {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => {
                let guard = cached_deleted_pk
                    .read()
                    .map_err(|_| CatalogError::LockPoisoned {
                        operation: "read Int64 PK deletions for max sequence".to_string(),
                    })?;
                Ok(guard.values().max().copied().unwrap_or(0))
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => {
                let guard =
                    cached_deleted_row_keys
                        .read()
                        .map_err(|_| CatalogError::LockPoisoned {
                            operation: "read row key deletions for max sequence".to_string(),
                        })?;
                Ok(guard.values().max().copied().unwrap_or(0))
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => Ok(0),
        }
    }

    /// Process stream in chunks and write them in parallel with bounded concurrency.
    ///
    /// This method optimizes throughput by:
    /// - Streaming chunk formation (no buffering of all chunks)
    /// - Parallel writes with bounded concurrency (configurable via `VortexConfig.upload_concurrency`)
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
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::Instant;

        // Bounded parallelism: configurable concurrent writes to optimize I/O
        let semaphore = Arc::clone(self.context.upload_semaphore());

        // Progress tracking for S3 Express uploads
        let is_s3_storage = self.table_metadata.path.starts_with("s3://");
        let start_time = Instant::now();
        let last_progress_ms = Arc::new(AtomicU64::new(0));
        let total_bytes_written = Arc::new(AtomicUsize::new(0));
        let files_written = Arc::new(AtomicUsize::new(0));
        let mut write_tasks = tokio::task::JoinSet::new();

        // Log when starting S3 upload process
        if is_s3_storage {
            tracing::info!(
                "Starting S3 upload for table {} (target chunk size: {})",
                self.table_metadata.table_name,
                format_bytes(target_size_bytes)
            );
        }

        // Pre-allocate chunk vector with estimated capacity
        // Estimate: average batch ~8MB, so reserve for a few batches per chunk
        let estimated_batches_per_chunk = (target_size_bytes / (8 * 1024 * 1024)).max(1);
        let mut current_chunk = Vec::with_capacity(estimated_batches_per_chunk);
        let mut current_size = 0usize;
        let mut total_rows = 0u64;
        let mut chunk_count = 0usize;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to read batch from stream.".to_string(),
                source: Box::new(e),
            })?;

            let batch_size = batch.get_array_memory_size();

            // If adding this batch would exceed target size and we have data, write current chunk
            if current_size + batch_size > target_size_bytes && !current_chunk.is_empty() {
                // Acquire semaphore permit before spawning write task
                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: "Failed to acquire write permit.".to_string(),
                        source: Box::new(e),
                    }
                })?;

                // Move chunk to write task (zero-copy via mem::take)
                let chunk_to_write = std::mem::replace(
                    &mut current_chunk,
                    Vec::with_capacity(estimated_batches_per_chunk),
                );
                let chunk_size = current_size;
                current_size = 0;
                chunk_count += 1;

                // Clone self and progress trackers for the async task
                let self_clone = self.clone_for_write();
                let total_bytes = Arc::clone(&total_bytes_written);
                let files_count = Arc::clone(&files_written);
                let progress_time = Arc::clone(&last_progress_ms);
                let is_s3 = is_s3_storage;
                let table_name = self.table_metadata.table_name.clone();
                let start = start_time;
                let current_chunk_num = chunk_count;

                // Log when starting a chunk upload (before the slow I/O operation)
                if is_s3 {
                    tracing::info!(
                        "Starting S3 upload for {} chunk {} ({})...",
                        table_name,
                        current_chunk_num,
                        format_bytes(chunk_size)
                    );
                }

                write_tasks.spawn(async move {
                    let result = self_clone.write_chunk(chunk_to_write).await;

                    // Track progress for S3 uploads
                    if is_s3 {
                        total_bytes.fetch_add(chunk_size, Ordering::Relaxed);
                        let file_num = files_count.fetch_add(1, Ordering::Relaxed) + 1;

                        // Log progress every 10 seconds or when a file completes
                        let elapsed = start.elapsed();
                        // Use saturating conversion since elapsed time in real usage won't exceed u64::MAX milliseconds
                        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                        let last_logged = progress_time.load(Ordering::Relaxed);
                        let should_log =
                            elapsed_ms.saturating_sub(last_logged) >= 10_000 || result.is_ok();
                        if should_log {
                            let bytes_so_far = total_bytes.load(Ordering::Relaxed);
                            let throughput = if elapsed.as_secs_f64() > 0.0 {
                                #[expect(clippy::cast_precision_loss)]
                                let bytes_per_sec = bytes_so_far as f64 / elapsed.as_secs_f64();
                                format_bytes_per_sec(bytes_per_sec)
                            } else {
                                "calculating...".to_string()
                            };
                            tracing::info!(
                                "S3 upload for {}: {} files completed ({}) in {:.1}s, {}",
                                table_name,
                                file_num,
                                format_bytes(bytes_so_far),
                                elapsed.as_secs_f64(),
                                throughput
                            );
                            progress_time.store(elapsed_ms, Ordering::Relaxed);
                        }
                    }

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
                    message: "Failed to acquire write permit for final chunk.".to_string(),
                    source: Box::new(e),
                }
            })?;

            chunk_count += 1;
            let final_chunk_size = current_size;

            let self_clone = self.clone_for_write();
            let total_bytes = Arc::clone(&total_bytes_written);
            let files_count = Arc::clone(&files_written);
            let is_s3 = is_s3_storage;

            write_tasks.spawn(async move {
                let result = self_clone.write_chunk(current_chunk).await;

                // Track final chunk for S3 uploads
                if is_s3 {
                    total_bytes.fetch_add(final_chunk_size, Ordering::Relaxed);
                    files_count.fetch_add(1, Ordering::Relaxed);
                }

                drop(permit);
                result
            });
        }

        // Wait for all writes to complete and collect row counts
        while let Some(result) = write_tasks.join_next().await {
            let row_count = result.map_err(|e| CatalogError::InvalidOperation {
                message: "Write task panicked.".to_string(),
                source: Box::new(e),
            })??;
            total_rows += row_count;
        }

        // Log final summary for S3 Express uploads
        if is_s3_storage {
            let elapsed = start_time.elapsed();
            let total_bytes = total_bytes_written.load(Ordering::Relaxed);
            let files_count = files_written.load(Ordering::Relaxed);
            let throughput = if elapsed.as_secs_f64() > 0.0 {
                #[expect(clippy::cast_precision_loss)]
                let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
                format_bytes_per_sec(bytes_per_sec)
            } else {
                "N/A".to_string()
            };
            tracing::info!(
                "Completed S3 upload for {}: {} rows in {} files ({}) in {:.1}s, {}",
                self.table_metadata.table_name,
                total_rows,
                files_count,
                format_bytes(total_bytes),
                elapsed.as_secs_f64(),
                throughput
            );
        }

        Ok((total_rows, chunk_count))
    }

    /// Write a stream of record batches to a specific snapshot directory, chunking into
    /// parallel writes for efficiency.
    ///
    /// This is similar to `chunk_and_write_parallel` but writes to a specified snapshot
    /// directory rather than the current listing table's location. This is used during
    /// compaction operations where data needs to be written to a new snapshot.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream of record batches to write
    /// * `target_size_bytes` - Target size for each output file in bytes
    /// * `snapshot_id` - The snapshot ID to write to
    ///
    /// # Returns
    ///
    /// A tuple of (total rows written, number of files written)
    ///
    /// # Errors
    ///
    /// Returns an error if the write operation fails.
    pub(crate) async fn chunk_and_write_parallel_to_snapshot(
        &self,
        mut stream: SendableRecordBatchStream,
        target_size_bytes: usize,
        snapshot_id: &str,
    ) -> CatalogResult<(u64, usize)> {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::Instant;

        // Construct snapshot directory URL
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            snapshot_id,
        );

        // Create a new ListingTable pointing to the snapshot directory
        let snapshot_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        // Bounded parallelism: configurable concurrent writes to optimize I/O
        let semaphore = Arc::clone(self.context.upload_semaphore());

        // Progress tracking for S3 Express uploads
        let is_s3_storage = self.table_metadata.path.starts_with("s3://");
        let start_time = Instant::now();
        let last_progress_ms = Arc::new(AtomicU64::new(0));
        let total_bytes_written = Arc::new(AtomicUsize::new(0));
        let files_written = Arc::new(AtomicUsize::new(0));
        let mut write_tasks = tokio::task::JoinSet::new();

        // Log when starting S3 upload process
        if is_s3_storage {
            tracing::info!(
                "Starting S3 upload to snapshot {} for table {} (target chunk size: {})",
                snapshot_id,
                self.table_metadata.table_name,
                format_bytes(target_size_bytes)
            );
        }

        // Pre-allocate chunk vector with estimated capacity
        let estimated_batches_per_chunk = (target_size_bytes / (8 * 1024 * 1024)).max(1);
        let mut current_chunk = Vec::with_capacity(estimated_batches_per_chunk);
        let mut current_size = 0usize;
        let mut total_rows = 0u64;
        let mut chunk_count = 0usize;

        let snapshot_listing_table = Arc::new(snapshot_listing_table);

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to read batch from stream.".to_string(),
                source: Box::new(e),
            })?;

            let batch_size = batch.get_array_memory_size();

            // If adding this batch would exceed target size and we have data, write current chunk
            if current_size + batch_size > target_size_bytes && !current_chunk.is_empty() {
                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: "Failed to acquire write permit.".to_string(),
                        source: Box::new(e),
                    }
                })?;

                let chunk_to_write = std::mem::replace(
                    &mut current_chunk,
                    Vec::with_capacity(estimated_batches_per_chunk),
                );
                let chunk_size = current_size;
                current_size = 0;
                chunk_count += 1;

                let listing_table_clone = Arc::clone(&snapshot_listing_table);
                let total_bytes = Arc::clone(&total_bytes_written);
                let files_count = Arc::clone(&files_written);
                let progress_time = Arc::clone(&last_progress_ms);
                let is_s3 = is_s3_storage;
                let table_name = self.table_metadata.table_name.clone();
                let start = start_time;
                let current_chunk_num = chunk_count;

                if is_s3 {
                    tracing::info!(
                        "Starting S3 upload for {} chunk {} ({})...",
                        table_name,
                        current_chunk_num,
                        format_bytes(chunk_size)
                    );
                }

                write_tasks.spawn(async move {
                    let result =
                        Self::write_chunk_to_listing_table(&listing_table_clone, chunk_to_write)
                            .await;

                    if is_s3 {
                        total_bytes.fetch_add(chunk_size, Ordering::Relaxed);
                        let file_num = files_count.fetch_add(1, Ordering::Relaxed) + 1;

                        let elapsed = start.elapsed();
                        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
                        let last_logged = progress_time.load(Ordering::Relaxed);
                        let should_log =
                            elapsed_ms.saturating_sub(last_logged) >= 10_000 || result.is_ok();
                        if should_log {
                            let bytes_so_far = total_bytes.load(Ordering::Relaxed);
                            let throughput = if elapsed.as_secs_f64() > 0.0 {
                                #[expect(clippy::cast_precision_loss)]
                                let bytes_per_sec = bytes_so_far as f64 / elapsed.as_secs_f64();
                                format_bytes_per_sec(bytes_per_sec)
                            } else {
                                "calculating...".to_string()
                            };
                            tracing::info!(
                                "S3 upload for {}: {} files completed ({}) in {:.1}s, {}",
                                table_name,
                                file_num,
                                format_bytes(bytes_so_far),
                                elapsed.as_secs_f64(),
                                throughput
                            );
                            progress_time.store(elapsed_ms, Ordering::Relaxed);
                        }
                    }

                    drop(permit);
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
                    message: "Failed to acquire write permit for final chunk.".to_string(),
                    source: Box::new(e),
                }
            })?;

            chunk_count += 1;
            let final_chunk_size = current_size;

            let listing_table_clone = Arc::clone(&snapshot_listing_table);
            let total_bytes = Arc::clone(&total_bytes_written);
            let files_count = Arc::clone(&files_written);
            let is_s3 = is_s3_storage;

            write_tasks.spawn(async move {
                let result =
                    Self::write_chunk_to_listing_table(&listing_table_clone, current_chunk).await;

                if is_s3 {
                    total_bytes.fetch_add(final_chunk_size, Ordering::Relaxed);
                    files_count.fetch_add(1, Ordering::Relaxed);
                }

                drop(permit);
                result
            });
        }

        // Wait for all writes to complete and collect row counts
        while let Some(result) = write_tasks.join_next().await {
            let row_count = result.map_err(|e| CatalogError::InvalidOperation {
                message: "Write task panicked.".to_string(),
                source: Box::new(e),
            })??;
            total_rows += row_count;
        }

        // Log final summary for S3 Express uploads
        if is_s3_storage {
            let elapsed = start_time.elapsed();
            let total_bytes = total_bytes_written.load(Ordering::Relaxed);
            let files_count = files_written.load(Ordering::Relaxed);
            let throughput = if elapsed.as_secs_f64() > 0.0 {
                #[expect(clippy::cast_precision_loss)]
                let bytes_per_sec = total_bytes as f64 / elapsed.as_secs_f64();
                format_bytes_per_sec(bytes_per_sec)
            } else {
                "N/A".to_string()
            };
            tracing::info!(
                "Completed S3 upload for {} to snapshot {}: {} rows in {} files ({}) in {:.1}s, {}",
                self.table_metadata.table_name,
                snapshot_id,
                total_rows,
                files_count,
                format_bytes(total_bytes),
                elapsed.as_secs_f64(),
                throughput
            );
        }

        Ok((total_rows, chunk_count))
    }

    /// Write a chunk of record batches to a specific `ListingTable`.
    ///
    /// This is a static helper method for `chunk_and_write_parallel_to_snapshot`.
    async fn write_chunk_to_listing_table(
        listing_table: &ListingTable,
        chunk: Vec<RecordBatch>,
    ) -> CatalogResult<u64> {
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

        let ctx = SessionContext::new();
        let state = ctx.state();

        let insert_plan = listing_table
            .insert_into(&state, stream_exec, InsertOp::Append)
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to create insert plan for chunk.".to_string(),
                source: Box::new(e),
            })?;

        collect(insert_plan, state.task_ctx()).await.map_err(|e| {
            CatalogError::InvalidOperation {
                message: "Failed to execute insert for chunk.".to_string(),
                source: Box::new(e),
            }
        })?;

        Ok(row_count)
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
    fn clone_for_write(&self) -> Self {
        Self {
            table_metadata: self.table_metadata.clone(),
            catalog: Arc::clone(&self.catalog),
            listing_table: Arc::clone(&self.listing_table),
            context: Arc::clone(&self.context),
            retention_filters: self.retention_filters.clone(),
            pk_deletion_strategy: self.pk_deletion_strategy.clone(),
            pk_row_converter: self.pk_row_converter.as_ref().map(Arc::clone),
            pk_column_indices: self.pk_column_indices.clone(),
            write_lock: Arc::clone(&self.write_lock), // Shared across all clones for same table
            object_store_config: self.object_store_config.clone(),
            current_snapshot_id: Arc::clone(&self.current_snapshot_id),
            protected_snapshots: Arc::clone(&self.protected_snapshots),
        }
    }

    /// Returns the column indices for the configured primary key, if any.
    fn primary_key_indices(&self) -> CatalogResult<Option<Vec<usize>>> {
        if self.table_metadata.primary_key.is_empty() {
            return Ok(None);
        }

        let mut indices = Vec::with_capacity(self.table_metadata.primary_key.len());
        for pk_col in &self.table_metadata.primary_key {
            let idx = self.table_metadata.schema.index_of(pk_col).map_err(|_| {
                CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Primary key column '{pk_col}' not found in schema for table {}",
                        self.table_metadata.table_name
                    ),
                }
            })?;
            indices.push(idx);
        }

        Ok(Some(indices))
    }

    /// Build a `RowConverter` for the primary key columns.
    fn build_pk_converter(&self, pk_indices: &[usize]) -> CatalogResult<RowConverter> {
        let mut sort_fields = Vec::with_capacity(pk_indices.len());
        for idx in pk_indices {
            let field = self.table_metadata.schema.field(*idx);
            sort_fields.push(SortField::new(field.data_type().clone()));
        }

        RowConverter::new(sort_fields).map_err(|err| CatalogError::InvalidOperationNoSource {
            message: format!(
                "Failed to create row converter for primary key on table {}: {err}",
                self.table_metadata.table_name
            ),
        })
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
    ) -> CatalogResult<HashMap<OwnedRow, RowLocation>> {
        // Clone listing table to avoid holding locks across await points
        let listing_table = {
            let guard = self
                .listing_table
                .read()
                .map_err(|_| CatalogError::LockPoisoned {
                    operation: "load_existing_keyset (read listing table)".to_string(),
                })?;
            Arc::clone(&guard)
        };

        // Clone protected snapshots to avoid holding locks across await points
        let protected_snapshots = {
            let guard =
                self.protected_snapshots
                    .read()
                    .map_err(|_| CatalogError::LockPoisoned {
                        operation: "read protected snapshots in load_existing_keyset".to_string(),
                    })?;
            guard.clone()
        };

        let ctx = SessionContext::new();
        // Only read PK columns - no need to load all columns for keyset building
        let pk_projection = pk_indices.to_vec();
        let scan_plan = listing_table
            .scan(&ctx.state(), Some(&pk_projection), &[], None)
            .await
            .map_err(|err| CatalogError::InvalidOperationNoSource {
                message: format!("Failed to scan listing table for primary keys: {err}"),
            })?;

        let mut all_batches = collect(scan_plan, ctx.task_ctx()).await.map_err(|err| {
            CatalogError::InvalidOperationNoSource {
                message: format!("Failed to collect primary key scan: {err}"),
            }
        })?;

        // Also collect batches from each protected snapshot
        for snapshot_id in protected_snapshots.keys() {
            let snapshot_url = Self::snapshot_dir_url(
                &self.table_metadata.path,
                self.table_metadata.table_id,
                snapshot_id,
            );

            let snapshot_listing_table = Self::create_listing_table(
                &snapshot_url,
                Arc::clone(&self.table_metadata.schema),
                self.context.file_format(),
                &self.pk_deletion_strategy,
            )?;

            // Only read PK columns - no need to load all columns for keyset building
            let snapshot_plan = snapshot_listing_table
                .scan(&ctx.state(), Some(&pk_projection), &[], None)
                .await
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Failed to scan protected snapshot {snapshot_id} for primary keys: {err}"
                    ),
                })?;

            let snapshot_batches = collect(snapshot_plan, ctx.task_ctx())
                .await
                .map_err(|err| CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Failed to collect protected snapshot {snapshot_id} scan: {err}"
                    ),
                })?;

            all_batches.extend(snapshot_batches);
        }

        // Load the appropriate deletion cache based on pk_deletion_strategy.
        // This ensures keys that were previously deleted are not considered as conflicts.
        // Note: PositionBased strategy is never used here since it implies no primary key,
        // and this function is only called for tables with primary keys.
        let (deleted_pk_i64, insert_records_pk_i64) = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk,
                cached_insert_records,
            } => {
                let deleted_guard = cached_deleted_pk.read().map_err(|_| {
                    CatalogError::InvalidOperationNoSource {
                        message: DELETION_CACHE_LOCK_POISONED.to_string(),
                    }
                })?;
                let insert_guard = cached_insert_records.read().map_err(|_| {
                    CatalogError::InvalidOperationNoSource {
                        message: DELETION_CACHE_LOCK_POISONED.to_string(),
                    }
                })?;
                (
                    Some(Arc::clone(&deleted_guard)),
                    Some(Arc::clone(&insert_guard)),
                )
            }
            _ => (None, None),
        };

        let (deleted_row_keys, insert_records_row_keys) = match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                let deleted_guard = cached_deleted_row_keys.read().map_err(|_| {
                    CatalogError::InvalidOperationNoSource {
                        message: DELETION_CACHE_LOCK_POISONED.to_string(),
                    }
                })?;
                let insert_guard = cached_insert_records.read().map_err(|_| {
                    CatalogError::InvalidOperationNoSource {
                        message: DELETION_CACHE_LOCK_POISONED.to_string(),
                    }
                })?;
                (
                    Some(Arc::clone(&deleted_guard)),
                    Some(Arc::clone(&insert_guard)),
                )
            }
            _ => (None, None),
        };

        let mut keyset = HashMap::with_capacity(1024);
        let mut row_id_base: i64 = 0;

        // After projection, batch columns are at indices 0..pk_indices.len()
        let projected_pk_indices: Vec<usize> = (0..pk_indices.len()).collect();

        for batch in all_batches {
            let pk_columns: Vec<_> = projected_pk_indices
                .iter()
                .map(|idx| Arc::clone(batch.column(*idx)))
                .collect();

            let rows = converter.convert_columns(&pk_columns).map_err(|err| {
                CatalogError::InvalidOperationNoSource {
                    message: format!("Failed to convert primary key columns: {err}"),
                }
            })?;

            // For Int64Pk strategy, get the PK column as Int64Array for efficient lookup
            let int64_pk_array: Option<&arrow::array::Int64Array> =
                if self.pk_deletion_strategy.is_int64_pk() && pk_indices.len() == 1 {
                    batch.column(0).as_any().downcast_ref()
                } else {
                    None
                };

            for row_idx in 0..batch.num_rows() {
                let row_id = row_id_base
                    + i64::try_from(row_idx).map_err(|_| {
                        CatalogError::InvalidOperationNoSource {
                            message: "Row index exceeds i64::MAX; cannot compute row_id"
                                .to_string(),
                        }
                    })?;

                // Check if row is deleted based on pk_deletion_strategy
                let is_deleted = match &self.pk_deletion_strategy {
                    PkDeletionStrategyWithCache::Int64Pk { .. } => {
                        if let (Some(pk_array), Some(deleted_pks)) =
                            (int64_pk_array, &deleted_pk_i64)
                        {
                            let pk_value = pk_array.value(row_idx);
                            !is_pk_visible_i64(
                                pk_value,
                                deleted_pks,
                                insert_records_pk_i64.as_deref(),
                            )
                        } else {
                            false
                        }
                    }
                    PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                        if let Some(deleted_keys) = &deleted_row_keys {
                            let key = rows.row(row_idx);
                            !is_pk_visible_row_key(
                                key.as_ref(),
                                deleted_keys,
                                insert_records_row_keys.as_deref(),
                            )
                        } else {
                            false
                        }
                    }
                    // PositionBased implies no primary key, but this function requires PKs
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
                    return Err(CatalogError::InvalidOperationNoSource {
                        message: format!(
                            "Null primary key encountered in existing data for table {}",
                            self.table_metadata.table_name
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
                        data_file_id: DEFAULT_DATA_FILE_ID,
                        row_id,
                    },
                );
            }

            row_id_base += i64::try_from(batch.num_rows()).map_err(|_| {
                CatalogError::InvalidOperationNoSource {
                    message: "Batch row count exceeds i64::MAX; cannot compute row_id_base"
                        .to_string(),
                }
            })?;
        }

        Ok(keyset)
    }

    /// Prepare an incoming stream for insert by validating `on_conflict` constraints.
    ///
    /// If a primary key is configured, this method:
    /// 1. Loads existing keys from the table (respecting deletion visibility)
    /// 2. Validates incoming rows against `on_conflict` behavior (drop/upsert)
    /// 3. Returns a prepared stream with conflicts resolved and deletion specs
    ///
    /// If no primary key is configured, returns the stream unchanged with empty deletion specs.
    async fn prepare_stream_for_insert(
        &self,
        stream: SendableRecordBatchStream,
    ) -> CatalogResult<(
        SendableRecordBatchStream,
        HashMap<i64, Vec<i64>>,
        Vec<i64>,
        Vec<Box<[u8]>>,
    )> {
        let Some(pk_indices) = self.primary_key_indices()? else {
            return Ok((stream, HashMap::new(), Vec::new(), Vec::new()));
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

        Ok((
            Box::pin(validated_stream) as SendableRecordBatchStream,
            validation_result.delete_specs,
            validation_result.deleted_pk_i64,
            validation_result.deleted_row_keys,
        ))
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
    ) -> CatalogResult<OnConflictValidationResult> {
        let mut incoming_keys: HashSet<OwnedRow> = HashSet::with_capacity(1024);
        let mut filtered_batches = Vec::new();
        let mut delete_specs: HashMap<i64, Vec<i64>> = HashMap::new();
        let mut all_deleted_pk_i64: Vec<i64> = Vec::new();
        let mut all_deleted_row_keys: Vec<Box<[u8]>> = Vec::new();

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
            let batch = batch_result.map_err(|e| CatalogError::InvalidOperationNoSource {
                message: format!("Failed to read batch for on_conflict validation: {e}"),
            })?;

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
            } = self.apply_on_conflict_to_batch(batch, &mut ctx)?;

            for (data_file_id, rows) in batch_delete_specs {
                delete_specs.entry(data_file_id).or_default().extend(rows);
            }

            all_deleted_pk_i64.extend(deleted_pk_i64);
            all_deleted_row_keys.extend(deleted_row_keys);

            incoming_keys.extend(kept_keys);

            if let Some(batch) = filtered_batch {
                filtered_batches.push(batch);
            }
        }

        Ok(OnConflictValidationResult {
            filtered_batches,
            delete_specs,
            deleted_pk_i64: all_deleted_pk_i64,
            deleted_row_keys: all_deleted_row_keys,
        })
    }

    fn apply_on_conflict_to_batch(
        &self,
        batch: RecordBatch,
        ctx: &mut OnConflictContext<'_>,
    ) -> CatalogResult<BatchValidationResult> {
        use arrow::array::Int64Array;

        let pk_columns: Vec<_> = ctx
            .pk_indices
            .iter()
            .map(|idx| Arc::clone(batch.column(*idx)))
            .collect();

        let rows = ctx.converter.convert_columns(&pk_columns).map_err(|err| {
            CatalogError::InvalidOperationNoSource {
                message: format!("Failed to convert primary key columns: {err}"),
            }
        })?;

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

        for row_idx in 0..batch.num_rows() {
            let has_null = pk_columns.iter().any(|col| col.is_null(row_idx));
            if has_null {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Primary key values must be non-null for table {}",
                        self.table_metadata.table_name
                    ),
                });
            }

            let key = rows.row(row_idx).owned();
            if ctx.incoming_keys.contains(&key) {
                return Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "Incoming data for table {} contains duplicate primary key across batches",
                        self.table_metadata.table_name
                    ),
                });
            }

            if let Some(existing) = ctx.existing_keys.get(&key) {
                match ctx.on_conflict {
                    OnConflict::DoNothingAll | OnConflict::DoNothing(_) => {
                        keep_mask.push(false);
                    }
                    OnConflict::Upsert(_) => {
                        delete_specs
                            .entry(existing.data_file_id)
                            .or_default()
                            .push(existing.row_id);

                        // Track the PK value being deleted for cache updates
                        match &self.pk_deletion_strategy {
                            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                                if let Some(arr) = int64_pk_array {
                                    deleted_pk_i64.push(arr.value(row_idx));
                                }
                            }
                            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                                deleted_row_keys.push(key.as_ref().to_vec().into_boxed_slice());
                            }
                            PkDeletionStrategyWithCache::PositionBased { .. } => {
                                // Position-based doesn't need PK values
                            }
                        }

                        ctx.existing_keys.insert(
                            key.clone(),
                            RowLocation {
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
                        return Err(CatalogError::InvalidOperationNoSource {
                            message: format!(
                                "Duplicate primary key found in batch for table {}",
                                self.table_metadata.table_name
                            ),
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
        })
    }

    fn filter_validated_batch(
        batch: RecordBatch,
        keep_mask: Vec<bool>,
        row_keys: &[OwnedRow],
    ) -> CatalogResult<(Option<RecordBatch>, HashSet<OwnedRow>)> {
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
        let filtered_batch =
            arrow::compute::filter_record_batch(&batch, &filter_array).map_err(|err| {
                CatalogError::InvalidOperationNoSource {
                    message: format!("Failed to filter batch for on_conflict handling: {err}"),
                }
            })?;

        Ok((Some(filtered_batch), kept_keys))
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
    async fn apply_on_conflict_deletions(
        &self,
        delete_specs: HashMap<i64, Vec<i64>>,
        deleted_pk_i64: Vec<i64>,
        deleted_row_keys: Vec<Box<[u8]>>,
    ) -> CatalogResult<()> {
        if delete_specs.is_empty() {
            return Ok(());
        }

        // Get a fresh sequence number for this deletion operation.
        // This ensures proper ordering: data written after this delete but before
        // the next delete will be properly filtered.
        let delete_sequence = self
            .catalog
            .increment_sequence_number(self.table_metadata.table_id)
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
                // Update Int64 PK deletion cache with delete sequence
                {
                    let mut guard = cached_deleted_pk.write().map_err(|_| {
                        CatalogError::InvalidOperationNoSource {
                            message: DELETION_CACHE_LOCK_POISONED.to_string(),
                        }
                    })?;

                    let mut updated_map = (**guard).clone();
                    for &pk_value in &deleted_pk_i64 {
                        updated_map
                            .entry(pk_value)
                            .and_modify(|seq| *seq = (*seq).max(delete_sequence))
                            .or_insert(delete_sequence);
                    }
                    let updated_count = updated_map.len();
                    *guard = Arc::new(updated_map);

                    tracing::debug!(
                        "Updated Int64 PK deletion cache with {} keys (seq={}) for table {}",
                        updated_count,
                        delete_sequence,
                        self.table_metadata.table_name
                    );
                }

                // Update Int64 PK insert records cache with insert sequence (higher than delete)
                // This ensures the newly inserted row isn't filtered out by the deletion filter.
                {
                    let mut guard = cached_insert_records.write().map_err(|_| {
                        CatalogError::InvalidOperationNoSource {
                            message: DELETION_CACHE_LOCK_POISONED.to_string(),
                        }
                    })?;

                    let mut updated_map = (**guard).clone();
                    for pk_value in deleted_pk_i64 {
                        updated_map
                            .entry(pk_value)
                            .and_modify(|seq| *seq = (*seq).max(insert_sequence))
                            .or_insert(insert_sequence);
                    }
                    let updated_count = updated_map.len();
                    *guard = Arc::new(updated_map);

                    tracing::debug!(
                        "Updated Int64 PK insert records cache with {} keys (seq={}) for table {}",
                        updated_count,
                        insert_sequence,
                        self.table_metadata.table_name
                    );
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                // Update row key deletion cache with delete sequence
                {
                    let mut guard = cached_deleted_row_keys.write().map_err(|_| {
                        CatalogError::InvalidOperationNoSource {
                            message: DELETION_CACHE_LOCK_POISONED.to_string(),
                        }
                    })?;

                    let mut updated_map = (**guard).clone();
                    for row_key in &deleted_row_keys {
                        updated_map
                            .entry(row_key.clone())
                            .and_modify(|seq| *seq = (*seq).max(delete_sequence))
                            .or_insert(delete_sequence);
                    }
                    let updated_count = updated_map.len();
                    *guard = Arc::new(updated_map);

                    tracing::debug!(
                        "Updated RowConverter deletion cache with {} keys (seq={}) for table {}",
                        updated_count,
                        delete_sequence,
                        self.table_metadata.table_name
                    );
                }

                // Update row key insert records cache with insert sequence
                {
                    let mut guard = cached_insert_records.write().map_err(|_| {
                        CatalogError::InvalidOperationNoSource {
                            message: DELETION_CACHE_LOCK_POISONED.to_string(),
                        }
                    })?;

                    let mut updated_map = (**guard).clone();
                    for row_key in deleted_row_keys {
                        updated_map
                            .entry(row_key)
                            .and_modify(|seq| *seq = (*seq).max(insert_sequence))
                            .or_insert(insert_sequence);
                    }
                    let updated_count = updated_map.len();
                    *guard = Arc::new(updated_map);

                    tracing::debug!(
                        "Updated RowConverter insert records cache with {} keys (seq={}) for table {}",
                        updated_count,
                        insert_sequence,
                        self.table_metadata.table_name
                    );
                }
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
            self.context.sort_columns(),
            self.table_metadata.table_name
        );

        // Use the common stream sorting utility
        let sorted_stream =
            util::stream_utils::sort_stream(stream, self.context.sort_columns(), &task_ctx)
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to execute sort.".to_string(),
                    source: Box::new(e),
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
            self.context.sort_columns()
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
        let ctx = self.create_session_context();
        let df = ctx
            .read_table(listing_table)
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to read listing table for sorting.".to_string(),
                source: Box::new(e),
            })?;

        // Get the data as a stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to get stream from listing table.".to_string(),
                source: Box::new(e),
            })?;

        // Sort the stream using our existing sort logic
        let sorted_stream = self.sort_stream(stream)?;

        // Delete all existing Vortex files in the snapshot directory before rewriting
        // Note: For S3 paths, we skip deletion and let new files coexist (may need future cleanup)
        let is_s3_path = self.table_metadata.path.starts_with("s3://");
        let current_snapshot = self.get_current_snapshot_id()?;

        if is_s3_path {
            if let Some(prefix) = self.snapshot_object_store_prefix(&current_snapshot)? {
                self.delete_prefix_with_object_store(&prefix).await?;
            } else {
                tracing::warn!(
                    "S3 path detected but no object store prefix could be derived for sorted rewrite cleanup"
                );
            }
        } else {
            let snapshot_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                self.table_metadata.table_id,
                &current_snapshot,
            );
            self.delete_snapshot_files(&snapshot_dir).await?;
        }

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

    /// Create a `SessionContext` for data operations, registering object store if configured.
    fn create_session_context(&self) -> SessionContext {
        let ctx = SessionContext::new();
        let is_s3 = self.table_metadata.path.starts_with("s3://");

        // Register object store if configured for remote storage (e.g., S3 Express One Zone)
        if let Some(ref config) = self.object_store_config {
            Self::register_object_store_if_needed(&ctx.runtime_env(), config);
        } else if is_s3 {
            tracing::warn!(
                "Creating SessionContext for S3 table {} but no object_store_config!",
                self.table_metadata.table_name
            );
        }

        ctx
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

        // Create a session context for executing the insert (with object store if needed)
        let ctx = self.create_session_context();
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
                message: "Failed to create insert plan for chunk.".to_string(),
                source: Box::new(e),
            })?;

        // Execute the insert plan
        collect(insert_plan, state.task_ctx()).await.map_err(|e| {
            CatalogError::InvalidOperation {
                message: "Failed to execute insert for chunk.".to_string(),
                source: Box::new(e),
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
            self.pk_deletion_strategy.clone(),
            self.pk_row_converter.as_ref().map(Arc::clone),
            self.pk_column_indices.clone(),
            Vec::new(), // Retention filters don't need to scan protected snapshots
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
            self.table_metadata.table_id,
            Arc::clone(&self.catalog),
            self.pk_deletion_strategy.strategy(),
        )
        .await?;

        self.pk_deletion_strategy.refresh_from(&fresh_strategy)?;

        tracing::debug!(
            "Refreshed deletion cache for table {} (strategy: {:?})",
            self.table_metadata.table_name,
            self.pk_deletion_strategy.strategy(),
        );

        Ok(())
    }

    /// Process incoming batches and add insert records for PKs that are being re-inserted.
    ///
    /// This method implements upsert semantics using sequence-based ordering:
    /// 1. Collects all incoming batches
    /// 2. Gets a new sequence number from the catalog
    /// 3. Extracts PKs from the data
    /// 4. For PKs that are in the deletion set, adds insert records with the new sequence
    /// 5. Returns a stream of the batches for normal insert processing
    ///
    /// Insert records are stored in the catalog and cached in memory. During scan,
    /// a row is deleted only if its PK is in the deletion set AND (not in `insert_records`
    /// OR `insert_seq` < `delete_seq`).
    ///
    /// # Errors
    ///
    /// Returns an error if processing fails.
    ///
    /// NOTE: Currently unused because we use compaction for all strategies when there
    /// are pending deletions. Kept for potential future optimization with per-file
    /// sequence tracking.
    #[expect(dead_code)]
    async fn add_insert_records_for_incoming_pks(
        &self,
        stream: SendableRecordBatchStream,
    ) -> CatalogResult<SendableRecordBatchStream> {
        use futures::TryStreamExt;

        // Collect all batches from the stream
        let batches: Vec<RecordBatch> =
            stream
                .try_collect()
                .await
                .map_err(|e| CatalogError::InvalidOperation {
                    message: "Failed to collect batches for insert record processing".to_string(),
                    source: Box::new(e),
                })?;

        if batches.is_empty() {
            let schema = Arc::clone(&self.table_metadata.schema);
            let empty_stream: futures::stream::Iter<
                std::vec::IntoIter<datafusion_common::Result<RecordBatch>>,
            > = futures::stream::iter(Vec::new());
            return Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    empty_stream,
                ),
            ));
        }

        // Get a new sequence number for this insert operation
        let insert_sequence = self
            .catalog
            .increment_sequence_number(self.table_metadata.table_id)
            .await?;

        // Extract PKs and add insert records based on strategy
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                self.add_insert_records_int64(&batches, insert_sequence)
                    .await?;
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                self.add_insert_records_row_converter(&batches, insert_sequence)
                    .await?;
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based uses per-file deletion vectors and doesn't need insert records
                unreachable!("Position-based strategy doesn't track insert records");
            }
        }

        // Return the batches as a stream for normal insert processing
        let schema = Arc::clone(&self.table_metadata.schema);
        let batch_results: Vec<datafusion_common::Result<RecordBatch>> =
            batches.into_iter().map(Ok).collect();
        let batch_stream = futures::stream::iter(batch_results);
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, batch_stream),
        ))
    }

    /// Add insert records for Int64 PK strategy.
    ///
    /// Extracts ALL Int64 PKs from incoming batches and adds insert records with the current
    /// sequence number. This is required for sequence-based ordering where we need to know
    /// when each PK was inserted to compare against deletion sequences.
    async fn add_insert_records_int64(
        &self,
        batches: &[RecordBatch],
        insert_sequence: i64,
    ) -> CatalogResult<()> {
        use arrow::array::Int64Array;

        let pk_column_index =
            *self
                .pk_column_indices
                .first()
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: "Int64 PK strategy requires exactly one PK column index".to_string(),
                    source: Box::new(std::io::Error::other("missing pk column")),
                })?;

        // Extract ALL PKs from incoming batches
        let mut pks_to_record: Vec<i64> = Vec::new();

        for batch in batches {
            let pk_column = batch.column(pk_column_index);
            let pk_array = pk_column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: "Failed to downcast PK column to Int64Array".to_string(),
                    source: Box::new(std::io::Error::other("invalid pk type")),
                })?;

            for value in pk_array.values() {
                pks_to_record.push(*value);
            }
        }

        if pks_to_record.is_empty() {
            tracing::debug!(
                "No PKs in incoming data for table {}",
                self.table_metadata.table_name
            );
            return Ok(());
        }

        tracing::info!(
            "Adding {} insert records (seq={}) for table {} (Int64 PK strategy)",
            pks_to_record.len(),
            insert_sequence,
            self.table_metadata.table_name
        );

        // Convert to bytes for catalog storage
        let pk_bytes_list: Vec<Vec<u8>> = pks_to_record
            .iter()
            .map(|pk| pk.to_be_bytes().to_vec())
            .collect();

        // Add to catalog with sequence number
        self.catalog
            .add_insert_records_batch(self.table_metadata.table_id, pk_bytes_list, insert_sequence)
            .await?;

        // Update in-memory cache
        if let Some(cache) = self.pk_deletion_strategy.int64_insert_records_cache() {
            let mut guard = cache.write().map_err(|_| CatalogError::LockPoisoned {
                operation: "update Int64 insert records cache".to_string(),
            })?;
            let mut new_map = (**guard).clone();
            for pk in pks_to_record {
                new_map.insert(pk, insert_sequence);
            }
            *guard = Arc::new(new_map);
        }

        Ok(())
    }

    /// Add insert records for `RowConverter`-based PK strategy.
    ///
    /// Converts ALL PK columns to byte representation and adds insert records with the current
    /// sequence number. This is required for sequence-based ordering where we need to know
    /// when each PK was inserted to compare against deletion sequences.
    async fn add_insert_records_row_converter(
        &self,
        batches: &[RecordBatch],
        insert_sequence: i64,
    ) -> CatalogResult<()> {
        let row_converter =
            self.pk_row_converter
                .as_ref()
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: "RowConverter not available for RowConverterBased strategy"
                        .to_string(),
                    source: Box::new(std::io::Error::other("missing row converter")),
                })?;

        // Extract ALL PKs from incoming batches
        let mut keys_to_record: Vec<Box<[u8]>> = Vec::new();

        for batch in batches {
            // Extract PK columns
            let pk_columns: Vec<ArrayRef> = self
                .pk_column_indices
                .iter()
                .map(|&idx| Arc::clone(batch.column(idx)))
                .collect();

            // Convert to row format
            let rows = row_converter.convert_columns(&pk_columns).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: "Failed to convert PK columns to row format".to_string(),
                    source: Box::new(e),
                }
            })?;

            for row in &rows {
                let key: Box<[u8]> = row.as_ref().into();
                keys_to_record.push(key);
            }
        }

        if keys_to_record.is_empty() {
            tracing::debug!(
                "No PKs in incoming data for table {}",
                self.table_metadata.table_name
            );
            return Ok(());
        }

        tracing::info!(
            "Adding {} insert records (seq={}) for table {} (RowConverter strategy)",
            keys_to_record.len(),
            insert_sequence,
            self.table_metadata.table_name
        );

        // Convert to Vec<Vec<u8>> for catalog storage
        let pk_bytes_list: Vec<Vec<u8>> = keys_to_record.iter().map(|k| k.to_vec()).collect();

        // Add to catalog with sequence number
        self.catalog
            .add_insert_records_batch(self.table_metadata.table_id, pk_bytes_list, insert_sequence)
            .await?;

        // Update in-memory cache
        if let Some(cache) = self.pk_deletion_strategy.row_keys_insert_records_cache() {
            let mut guard = cache.write().map_err(|_| CatalogError::LockPoisoned {
                operation: "update key-based insert records cache".to_string(),
            })?;
            let mut new_map = (**guard).clone();
            for key in keys_to_record {
                new_map.insert(key, insert_sequence);
            }
            *guard = Arc::new(new_map);
        }

        Ok(())
    }

    /// Check if there are pending deletions based on the current deletion strategy.
    ///
    /// This is used to determine if inserts need special handling:
    /// - Position-based deletions use per-file deletion vectors (no special handling needed)
    /// - PK-based deletions use anti-deletions (write to new snapshot with higher sequence)
    ///
    /// # Errors
    ///
    /// Returns an error if the deletion cache lock is poisoned.
    fn has_pending_deletions(&self) -> CatalogResult<bool> {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => {
                let guard =
                    cached_deleted_row_ids
                        .read()
                        .map_err(|_| CatalogError::LockPoisoned {
                            operation: "check position-based deletion cache".to_string(),
                        })?;
                Ok(!guard.is_empty())
            }
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk, ..
            } => {
                let guard = cached_deleted_pk
                    .read()
                    .map_err(|_| CatalogError::LockPoisoned {
                        operation: "check Int64 PK deletion cache".to_string(),
                    })?;
                Ok(!guard.is_empty())
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => {
                let guard =
                    cached_deleted_row_keys
                        .read()
                        .map_err(|_| CatalogError::LockPoisoned {
                            operation: "check key-based deletion cache".to_string(),
                        })?;
                Ok(!guard.is_empty())
            }
        }
    }

    /// Clear all cached deletion vectors and insert records.
    ///
    /// This should be called after compaction operations that have applied all deletions
    /// and written a clean snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if any cache lock is poisoned.
    pub(crate) fn clear_all_deletion_caches(&self) -> CatalogResult<()> {
        // Clear caches based on the current strategy
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::PositionBased {
                cached_deleted_row_ids,
            } => {
                let mut guard =
                    cached_deleted_row_ids
                        .write()
                        .map_err(|_| CatalogError::LockPoisoned {
                            operation: "clear position-based deletion cache".to_string(),
                        })?;
                *guard = Arc::new(HashMap::new());
            }
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk,
                cached_insert_records,
            } => {
                {
                    let mut guard =
                        cached_deleted_pk
                            .write()
                            .map_err(|_| CatalogError::LockPoisoned {
                                operation: "clear Int64 PK deletion cache".to_string(),
                            })?;
                    *guard = Arc::new(HashMap::new());
                }
                {
                    let mut guard =
                        cached_insert_records
                            .write()
                            .map_err(|_| CatalogError::LockPoisoned {
                                operation: "clear Int64 insert records cache".to_string(),
                            })?;
                    *guard = Arc::new(HashMap::new());
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                {
                    let mut guard = cached_deleted_row_keys.write().map_err(|_| {
                        CatalogError::LockPoisoned {
                            operation: "clear key-based deletion cache".to_string(),
                        }
                    })?;
                    *guard = Arc::new(HashMap::new());
                }
                {
                    let mut guard =
                        cached_insert_records
                            .write()
                            .map_err(|_| CatalogError::LockPoisoned {
                                operation: "clear key-based insert records cache".to_string(),
                            })?;
                    *guard = Arc::new(HashMap::new());
                }
            }
        }

        // Clear protected snapshots - after compaction all data is in the main snapshot
        {
            let mut guard =
                self.protected_snapshots
                    .write()
                    .map_err(|_| CatalogError::LockPoisoned {
                        operation: "clear protected snapshots".to_string(),
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
    fn get_current_snapshot_id(&self) -> CatalogResult<String> {
        let guard = self
            .current_snapshot_id
            .read()
            .map_err(|_| CatalogError::LockPoisoned {
                operation: "read current snapshot id".to_string(),
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
    pub(crate) fn update_current_snapshot_id(&self, new_snapshot_id: &str) -> CatalogResult<()> {
        let mut guard =
            self.current_snapshot_id
                .write()
                .map_err(|_| CatalogError::LockPoisoned {
                    operation: "update current snapshot id".to_string(),
                })?;
        *guard = new_snapshot_id.to_string();
        tracing::debug!(
            "Updated current snapshot ID for table {} to {}",
            self.table_metadata.table_name,
            new_snapshot_id
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
        Err(CatalogError::NotImplemented {
            function: "delete_by_primary_key".to_string(),
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
        Err(CatalogError::NotImplemented {
            function: "update_by_primary_key".to_string(),
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
        // Construct URL to current snapshot using the live snapshot ID
        // (which may differ from table_metadata after compaction)
        let current_snapshot = self.get_current_snapshot_id()?;
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            self.table_metadata.table_id,
            &current_snapshot,
        );

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
        table_id: i64,
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
            return Ok(match strategy {
                PkDeletionStrategy::PositionBased => PkDeletionStrategyWithCache::PositionBased {
                    cached_deleted_row_ids: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                },
                PkDeletionStrategy::Int64Pk => PkDeletionStrategyWithCache::Int64Pk {
                    cached_deleted_pk: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                    cached_insert_records: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                },
                PkDeletionStrategy::RowConverterBased => {
                    PkDeletionStrategyWithCache::RowConverterBased {
                        cached_deleted_row_keys: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                        cached_insert_records: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                    }
                }
            });
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
                PkDeletionStrategy::PositionBased => PkDeletionStrategyWithCache::PositionBased {
                    cached_deleted_row_ids: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                },
                PkDeletionStrategy::Int64Pk => PkDeletionStrategyWithCache::Int64Pk {
                    cached_deleted_pk: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                    cached_insert_records: Arc::new(RwLock::new(Arc::new(insert_records_pk_i64))),
                },
                PkDeletionStrategy::RowConverterBased => {
                    PkDeletionStrategyWithCache::RowConverterBased {
                        cached_deleted_row_keys: Arc::new(RwLock::new(Arc::new(HashMap::new()))),
                        cached_insert_records: Arc::new(RwLock::new(Arc::new(
                            insert_records_row_keys,
                        ))),
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
                    cached_deleted_row_ids: Arc::new(RwLock::new(Arc::new(per_file_row_ids))),
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
                    cached_deleted_pk: Arc::new(RwLock::new(Arc::new(int64_pks))),
                    cached_insert_records: Arc::new(RwLock::new(Arc::new(insert_records_pk_i64))),
                }
            }
            PkDeletionStrategy::RowConverterBased => {
                tracing::debug!(
                    "Cached deletion vectors for table_id {table_id}: {} key-based, {} key-insert",
                    deleted_row_keys.len(),
                    insert_records_row_keys.len(),
                );
                PkDeletionStrategyWithCache::RowConverterBased {
                    cached_deleted_row_keys: Arc::new(RwLock::new(Arc::new(deleted_row_keys))),
                    cached_insert_records: Arc::new(RwLock::new(Arc::new(insert_records_row_keys))),
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
        table_id: i64,
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
                self.table_metadata.table_id,
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
                let all_deleted_pks = {
                    let guard = cached_deleted_pk.read().map_err(|_| {
                        datafusion_common::DataFusionError::Execution(
                            super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                        )
                    })?;
                    Arc::clone(&guard)
                };

                // Filter to only include deletions with seq > min_delete_seq_to_apply
                let filtered_deletions: HashMap<i64, i64> = all_deleted_pks
                    .iter()
                    .filter(|(_pk, &seq)| seq > min_delete_seq_to_apply)
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

                let empty_insert_records = Arc::new(HashMap::new());
                Ok(Arc::new(Int64PkDeletionFilterExec::new(
                    plan,
                    Arc::new(filtered_deletions),
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
                    let all_deleted_keys = {
                        let guard = cached_deleted_row_keys.read().map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                            )
                        })?;
                        Arc::clone(&guard)
                    };

                    // Filter to only include deletions with seq > min_delete_seq_to_apply
                    let filtered_deletions: HashMap<Box<[u8]>, i64> = all_deleted_keys
                        .iter()
                        .filter(|(_key, &seq)| seq > min_delete_seq_to_apply)
                        .map(|(key, &seq)| (key.clone(), seq))
                        .collect();

                    if filtered_deletions.is_empty() {
                        return Ok(Arc::new(CayenneAccelerationExec::new(plan)));
                    }

                    let empty_insert_records = Arc::new(HashMap::new());
                    Ok(Arc::new(KeyBasedDeletionFilterExec::new(
                        plan,
                        Arc::new(filtered_deletions),
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
                let deleted_pk_values = {
                    let guard = cached_deleted_pk.read().map_err(|_| {
                        datafusion_common::DataFusionError::Execution(
                            super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                        )
                    })?;
                    Arc::clone(&guard)
                };
                // Don't use insert_records for protected snapshot approach
                // The protected snapshots already handle new data without filtering
                let empty_insert_records = Arc::new(HashMap::new());

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
                    let deleted_row_keys = {
                        let guard = cached_deleted_row_keys.read().map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                            )
                        })?;
                        Arc::clone(&guard)
                    };
                    // Don't use insert_records for protected snapshot approach
                    let empty_insert_records: Arc<HashMap<Box<[u8]>, i64>> =
                        Arc::new(HashMap::new());

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
                // TODO: Implement Vortex-native deletion support by attaching per-file deletion
                // vectors via VortexAccessPlan in PartitionedFile.extensions. This allows Vortex
                // to skip decompressing deleted rows entirely using Selection::ExcludeRoaring.
                // See: vortex-datafusion VortexAccessPlan and VortexOpener::open()
            }
        }

        // No deletions to apply (position-based deletions are handled at Vortex scan level)
        Ok(Arc::new(CayenneAccelerationExec::new(plan)))
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
            } => {
                let guard = cached_deleted_pk.read().map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                    )
                })?;
                !guard.is_empty()
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                ..
            } => {
                let guard = cached_deleted_row_keys.read().map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                    )
                })?;
                !guard.is_empty()
            }
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

        // Delegate to the underlying listing table
        // Clone the Arc and drop the lock before awaiting to avoid holding locks across await points
        let listing_table = {
            let guard = self.listing_table.read().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    LISTING_TABLE_LOCK_POISONED.to_string(),
                )
            })?;
            Arc::clone(&guard)
        };
        let main_plan = listing_table
            .scan(state, effective_projection.as_ref(), filters, limit)
            .await?;

        // Check for protected snapshots that need to be scanned with partial deletion filter
        let protected_snapshot_plans = self
            .scan_protected_snapshots(
                state,
                effective_projection.as_ref(),
                filters,
                limit,
                &pk_indices_in_projection,
            )
            .await?;

        // If there are protected snapshots, we need to:
        // 1. Apply deletion filter to main plan
        // 2. UNION with unfiltered protected snapshot plans
        let plan = if protected_snapshot_plans.is_empty() {
            main_plan
        } else {
            use datafusion_physical_plan::union::UnionExec;

            // Apply deletion filter to main plan only
            let filtered_main_plan =
                self.apply_deletion_filter(main_plan, &pk_indices_in_projection)?;

            // UNION the filtered main plan with unfiltered protected snapshot plans
            let mut all_plans = vec![filtered_main_plan];
            all_plans.extend(protected_snapshot_plans);
            let union_plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(all_plans)?;

            // Strip extra PK columns if needed
            if need_projection_strip {
                if let Some(orig_proj) = projection {
                    return self.create_projection_strip(union_plan, orig_proj.len());
                }
            }

            return Ok(union_plan);
        };

        // Apply deletion filter based on strategy (original logic for when no protected snapshots)
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                cached_deleted_pk,
                cached_insert_records,
            } => {
                // Optimized Int64 PK deletion - direct HashSet<i64> lookup
                let deleted_pk_values = {
                    let guard = cached_deleted_pk.read().map_err(|_| {
                        datafusion_common::DataFusionError::Execution(
                            super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                        )
                    })?;
                    Arc::clone(&guard)
                };
                let insert_records_pk_values = {
                    let guard = cached_insert_records.read().map_err(|_| {
                        datafusion_common::DataFusionError::Execution(
                            super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                        )
                    })?;
                    Arc::clone(&guard)
                };

                if !deleted_pk_values.is_empty() {
                    tracing::debug!(
                        "Applying Int64 PK deletion filter ({} deleted keys, {} insert records) to scan of table {}",
                        deleted_pk_values.len(),
                        insert_records_pk_values.len(),
                        self.table_metadata.table_name
                    );

                    // For Int64 PK, we only have one PK column
                    let pk_column_index =
                        pk_indices_in_projection.first().copied().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(
                                "Int64 PK strategy requires exactly one PK column index"
                                    .to_string(),
                            )
                        })?;

                    let deletion_filter = Arc::new(Int64PkDeletionFilterExec::new(
                        plan,
                        deleted_pk_values,
                        insert_records_pk_values,
                        pk_column_index,
                    ));

                    // Strip extra PK columns if needed
                    if need_projection_strip {
                        if let Some(orig_proj) = projection {
                            return self.create_projection_strip(deletion_filter, orig_proj.len());
                        }
                    }

                    return Ok(deletion_filter);
                }
            }
            PkDeletionStrategyWithCache::RowConverterBased {
                cached_deleted_row_keys,
                cached_insert_records,
            } => {
                // RowConverter-based deletion for composite/non-integer PKs
                if let Some(ref row_converter) = self.pk_row_converter {
                    let deleted_row_keys = {
                        let guard = cached_deleted_row_keys.read().map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                            )
                        })?;
                        Arc::clone(&guard)
                    };
                    let insert_records_row_keys = {
                        let guard = cached_insert_records.read().map_err(|_| {
                            datafusion_common::DataFusionError::Execution(
                                super::constants::DELETION_CACHE_LOCK_POISONED.to_string(),
                            )
                        })?;
                        Arc::clone(&guard)
                    };

                    if !deleted_row_keys.is_empty() {
                        tracing::debug!(
                            "Applying RowConverter-based deletion filter ({} deleted keys, {} insert records) to scan of table {}",
                            deleted_row_keys.len(),
                            insert_records_row_keys.len(),
                            self.table_metadata.table_name
                        );

                        let deletion_filter = Arc::new(KeyBasedDeletionFilterExec::new(
                            plan,
                            deleted_row_keys,
                            insert_records_row_keys,
                            pk_indices_in_projection.clone(),
                            Arc::clone(row_converter),
                        ));

                        // Strip extra PK columns if needed
                        if need_projection_strip {
                            if let Some(orig_proj) = projection {
                                return self
                                    .create_projection_strip(deletion_filter, orig_proj.len());
                            }
                        }

                        return Ok(deletion_filter);
                    }
                }
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // TODO: Implement Vortex-native deletion support by attaching per-file deletion
                // vectors via VortexAccessPlan in PartitionedFile.extensions. This allows Vortex
                // to skip decompressing deleted rows entirely using Selection::ExcludeRoaring.
                // See: vortex-datafusion VortexAccessPlan and VortexOpener::open()
            }
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

        // For overwrite mode, delegate directly to CayenneDataSink which handles:
        // - Creating a new snapshot
        // - Memory-bounded writes via chunk_and_write_parallel_to_snapshot
        // - Catalog commit and state updates AFTER the data is written
        // - Old snapshot cleanup
        if overwrite == InsertOp::Overwrite {
            let sink = Arc::new(CayenneDataSink::new(
                self.clone_for_write(),
                InsertOp::Overwrite,
                Arc::clone(&self.table_metadata.schema),
                Arc::clone(&self.context),
            ));
            return Ok(Arc::new(DataSinkExec::new(input, sink, None)));
        }

        // For regular appends, use the existing snapshot and listing table
        // Ensure the snapshot directory exists for local paths (S3 creates paths on write)
        if !self.table_metadata.path.starts_with("s3://") {
            let current_snapshot = self.get_current_snapshot_id().map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to get current snapshot ID: {e}"
                ))
            })?;
            let snapshot_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                self.table_metadata.table_id,
                &current_snapshot,
            );
            Self::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        if is_s3 {
            tracing::info!(
                "Preparing CayenneDataSink for S3 write to {}",
                self.table_metadata.table_name
            );
        }

        // If a primary key is configured, materialize the input and apply on-conflict handling.
        let final_input = if let Some(pk_indices) = self.primary_key_indices().map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to get primary key indices: {e}"
            ))
        })? {
            // Execute the input plan to get the data stream
            let task_ctx = state.task_ctx();
            let input_stream = input.execute(0, Arc::clone(&task_ctx)).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to execute input plan for on-conflict handling: {e}"
                ))
            })?;

            // Build converter and load existing keys
            let converter = self.build_pk_converter(&pk_indices).map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to build PK converter: {e}"
                ))
            })?;
            let mut existing_keys = self
                .load_existing_keyset(&pk_indices, &converter)
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to load existing keyset: {e}"
                    ))
                })?;

            // Validate on-conflict and get filtered batches + deletion specs
            let validation_result = self
                .validate_on_conflict(input_stream, &pk_indices, &converter, &mut existing_keys)
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to validate on-conflict: {e}"
                    ))
                })?;

            // Apply deletion vectors for upserted rows
            let has_on_conflict_deletions = !validation_result.delete_specs.is_empty();
            if has_on_conflict_deletions {
                self.apply_on_conflict_deletions(
                    validation_result.delete_specs,
                    validation_result.deleted_pk_i64,
                    validation_result.deleted_row_keys,
                )
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to apply on-conflict deletions: {e}"
                    ))
                })?;
            }

            // Create new input from validated batches
            if validation_result.filtered_batches.is_empty() {
                // Nothing to insert after on-conflict filtering
                // Return a plan that returns 0 rows with the count schema expected by DataFusion
                let count_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new(
                        "count",
                        arrow::datatypes::DataType::UInt64,
                        false,
                    ),
                ]));
                return Ok(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
                    count_schema,
                )));
            }

            // If there were on-conflict deletions, write to a NEW snapshot that's protected
            // from those deletions. Otherwise, write to the main snapshot.
            if has_on_conflict_deletions {
                // Use the streaming insert to write to a new snapshot with proper sequence handling
                let schema = validation_result
                    .filtered_batches
                    .first()
                    .map(RecordBatch::schema)
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "No validated batches after applying on-conflict deletions".to_string(),
                        )
                    })?;
                let batch_stream =
                    futures::stream::iter(validation_result.filtered_batches.into_iter().map(Ok));
                let validated_stream =
                    RecordBatchStreamAdapter::new(Arc::clone(&schema), batch_stream);

                // Get a sequence number higher than the delete sequence
                let insert_sequence = self
                    .catalog
                    .increment_sequence_number(self.table_metadata.table_id)
                    .await
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to get insert sequence: {e}"
                        ))
                    })?;

                // Write to a new snapshot
                let _rows_written = self
                    .insert_to_new_snapshot_with_sequence(
                        Box::pin(validated_stream),
                        insert_sequence,
                    )
                    .await
                    .map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Failed to insert to new snapshot: {e}"
                        ))
                    })?;

                // Refresh the listing table to include the new snapshot
                self.refresh_listing_table().map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to refresh listing table: {e}"
                    ))
                })?;

                // Return an empty plan with the count schema expected by DataFusion
                // (we already did the insert, so return 0 as no more rows to insert)
                let count_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new(
                        "count",
                        arrow::datatypes::DataType::UInt64,
                        false,
                    ),
                ]));
                return Ok(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
                    count_schema,
                )));
            }

            let schema = validation_result
                .filtered_batches
                .first()
                .map(RecordBatch::schema)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "No validated batches for on-conflict handling".to_string(),
                    )
                })?;
            let batch_stream =
                futures::stream::iter(validation_result.filtered_batches.into_iter().map(Ok));
            let validated_stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), batch_stream);

            Arc::new(StreamingExec::new(schema, Box::pin(validated_stream)))
                as Arc<dyn ExecutionPlan>
        } else {
            // No primary key, use input as-is
            input
        };

        // Use CayenneDataSink with DataSinkExec for memory-bounded writes:
        // - Chunked writes via chunk_and_write_parallel
        // - Retention filter application
        // - Automatic listing table refresh
        let sink = Arc::new(CayenneDataSink::new(
            self.clone_for_write(),
            InsertOp::Append,
            Arc::clone(&self.table_metadata.schema),
            Arc::clone(&self.context),
        ));
        let result = Arc::new(DataSinkExec::new(final_input, sink, None));

        if is_s3 {
            tracing::info!(
                "CayenneDataSink created for {} (S3 write plan)",
                self.table_metadata.table_name
            );
        }

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
        // Collect protected snapshot listing tables for deletion scanning
        let protected_snapshot_tables = {
            let protected_snapshots = {
                let guard = self.protected_snapshots.read().map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        "Protected snapshots lock poisoned".to_string(),
                    )
                })?;
                guard.clone()
            };

            let mut tables = Vec::with_capacity(protected_snapshots.len());
            for (snapshot_id, _) in protected_snapshots {
                let snapshot_url = Self::snapshot_dir_url(
                    &self.table_metadata.path,
                    self.table_metadata.table_id,
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
                tables.push(listing_table);
            }
            tables
        };

        Ok(Arc::new(DeletionExec::new(
            Arc::new(CayenneDeletionSink::new(
                self.table_metadata.clone(),
                Arc::clone(&self.catalog),
                Arc::clone(&self.listing_table),
                Arc::clone(&self.table_metadata.schema),
                filters,
                self.pk_deletion_strategy.clone(),
                self.pk_row_converter.as_ref().map(Arc::clone),
                self.pk_column_indices.clone(),
                protected_snapshot_tables,
            )),
            &self.table_metadata.schema,
        )))
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
    use crate::metadata::VortexConfig;
    use crate::CayenneCatalog;

    use super::*;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{Constraints, ToDFSchema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::physical_plan::collect;
    use datafusion_common::DataFusionError;
    use datafusion_federation::schema_cast::record_convert::try_cast_to;
    use rstest::rstest;
    use std::collections::HashMap;
    use std::sync::Arc;
    use test_framework::arrow_record_batch_gen::*;

    /// A `TableProviderFactory` implementation to create new instances of `CayenneTableProvider`.
    // Not used outside of tests until https://github.com/spiceai/spiceai/issues/8534 is resolved
    #[derive(Debug)]
    pub struct CayenneTableProviderFactory {}

    #[async_trait]
    impl TableProviderFactory for CayenneTableProviderFactory {
        async fn create(
            &self,
            _state: &dyn Session,
            cmd: &CreateExternalTable,
        ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
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
                    )))
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
}
