/*
Copyright 2026 The Spice.ai OSS Authors

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

//! File-based deletion sink for time-based retention.
//!
//! Deletes entire Vortex files whose data is fully expired according to the
//! retention threshold. Files are eligible for deletion when their per-file
//! column statistics show `max(retention_col) < threshold`.
//!
//! Unlike [`CayenneDeletionSink`](super::CayenneDeletionSink), which marks
//! individual rows as deleted via deletion vectors, this sink physically
//! removes data files from the filesystem or object store.
//!
//! For PK-based tables with protected snapshots, the sink also scans protected
//! snapshot directories and removes eligible files from them. When a protected
//! snapshot directory is fully emptied, the sink performs additional cleanup:
//! it clears the snapshot sequence from the catalog, removes the entry
//! from the in-memory protected snapshots map, and deletes the empty snapshot
//! data directory from disk.

use crate::catalog::{CatalogError, CatalogResult, MetadataCatalog};
use crate::provider::retention::extract_retention_column_and_threshold;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::TableProvider;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use object_store::{ObjectMeta, ObjectStore};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Result from file-based deletion, including metadata for post-delete cleanup.
#[derive(Debug)]
pub(crate) struct FileBasedDeletionResult {
    /// Total number of rows deleted across all files.
    pub total_deleted_rows: u64,
    /// IDs of protected snapshots that were fully emptied
    /// (all files expired and deleted).
    pub emptied_snapshot_ids: Vec<String>,
}

/// Result of scanning a single listing table for retention-eligible files.
struct DeletionCheckScanResult {
    /// Files whose `max(retention_col) < threshold`.
    files: Vec<(ObjectMeta, Option<usize>)>,
    /// `true` when every file in the table matched — the table is fully expired.
    all_matched: bool,
}

/// File-based deletion sink for time-based retention.
///
/// Discovers files eligible for deletion based on per-file column statistics
/// and deletes them from the filesystem. Used for tables with `retention_period`
/// configured, for both position-based and PK-based deletion strategies.
///
/// # Workflow
///
/// 1. Parse the filter expression to extract column name and threshold scalar.
/// 2. Call [`list_files_for_scan`] to enumerate files with per-file statistics
///    for the main listing table and all protected snapshot tables.
/// 3. For each file, check if `max(retention_col) < threshold` — if so, the
///    file is eligible for deletion (all rows are expired).
/// 4. Delete eligible files from the filesystem.
/// 5. Identify protected snapshots that are fully emptied.
/// 6. Return the total number of deleted rows and cleanup metadata.
pub struct FileBasedDeletionSink {
    /// Main listing table to enumerate files and collect per-file statistics.
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    /// Protected snapshot listing tables keyed by snapshot ID (PK-based strategies only).
    /// `None` for position-based tables.
    protected_snapshot_tables: Option<Vec<(String, Arc<ListingTable>)>>,
    /// The retention filter expression (e.g., `event_time < literal`).
    filter: Expr,
    /// Table name for logging.
    table_name: String,

    /// Metadata catalog for clearing snapshot sequence records.
    catalog: Arc<dyn MetadataCatalog>,
    /// In-memory protected snapshots map (shared with `CayenneTableProvider`).
    protected_snapshots: Arc<RwLock<HashMap<String, i64>>>,
    /// Table ID for catalog operations.
    table_id: i64,
    /// Table base path for constructing snapshot directory paths.
    table_path: String,
}

impl FileBasedDeletionSink {
    /// Create a new file-based deletion sink.
    ///
    /// # Arguments
    ///
    /// * `listing_table` - Main listing table for the current snapshot.
    /// * `protected_snapshot_tables` - Protected snapshot listing tables keyed
    ///   by snapshot ID. `None` for position-based tables.
    /// * `filter` - Retention filter expression.
    /// * `table_name` - Table name for logging.
    /// * `catalog` - Metadata catalog for clearing snapshot sequence records.
    /// * `protected_snapshots` - In-memory protected snapshots map.
    /// * `table_id` - Table ID for catalog operations.
    /// * `table_path` - Table base path for snapshot directory construction.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        protected_snapshot_tables: Option<Vec<(String, Arc<ListingTable>)>>,
        filter: Expr,
        table_name: String,
        catalog: Arc<dyn MetadataCatalog>,
        protected_snapshots: Arc<RwLock<HashMap<String, i64>>>,
        table_id: i64,
        table_path: String,
    ) -> Self {
        Self {
            listing_table,
            protected_snapshot_tables,
            filter,
            table_name,
            catalog,
            protected_snapshots,
            table_id,
            table_path,
        }
    }

    /// Discover files eligible for retention-based deletion.
    ///
    /// Lists all files via [`ListingTable::list_files_for_scan`] and checks
    /// per-file column statistics. A file is eligible when its
    /// `max(retention_col) < threshold` — meaning all rows in the file are
    /// older than the retention threshold.
    ///
    /// Returns a [`DeletionScanResult`] containing the eligible files and
    /// whether *all* files in the table matched (i.e. the table is fully
    /// expired and can be cleaned up).
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table lock is poisoned, files cannot be
    /// listed, or the retention column is not found in the schema.
    async fn retention_eligible_files(
        &self,
        ctx: &SessionContext,
        listing_table: &ListingTable,
        retention_col: &str,
        retention_threshold: &ScalarValue,
    ) -> CatalogResult<DeletionCheckScanResult> {
        // Call list_files_for_scan — lists all files + collects per-file stats.
        // collect_stat is true by default via SessionConfig::default().
        let (file_groups, _aggregate_stats) = listing_table
            .list_files_for_scan(&ctx.state(), &[], None)
            .await
            .map_err(|e| CatalogError::InvalidOperation {
                message: "Failed to list files for retention scan".to_string(),
                source: Box::new(e),
            })?;

        // Find the column index for the retention column
        let col_idx = listing_table
            .schema()
            .index_of(retention_col)
            .map_err(|_| CatalogError::InvalidOperationNoSource {
                message: format!("Retention column '{retention_col}' not found in schema"),
            })?;

        // Filter: file is eligible when max(retention_col) < threshold
        let mut total_files: usize = 0;
        let mut eligible = Vec::new();
        for file_group in &file_groups {
            for file in file_group.iter() {
                total_files += 1;
                let stats = file.statistics.as_ref();

                let dominated = stats
                    .and_then(|s| s.column_statistics.get(col_idx))
                    .and_then(|col_stats| col_stats.max_value.get_value())
                    .is_some_and(|max_val| max_val < retention_threshold);
                // No stats → don't delete (safe default via is_some_and returning false)

                if dominated {
                    let num_rows = stats.and_then(|s| s.num_rows.get_value().copied());
                    eligible.push((file.object_meta.clone(), num_rows));
                }
            }
        }

        Ok(DeletionCheckScanResult {
            all_matched: !eligible.is_empty() && eligible.len() == total_files,
            files: eligible,
        })
    }

    /// Delete eligible files from an object store.
    ///
    /// Returns the total number of deleted rows.
    async fn delete_eligible_files(
        &self,
        eligible_files: &[(ObjectMeta, Option<usize>)],
        object_store: &dyn ObjectStore,
        source: &str,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut total_rows: u64 = 0;
        let mut deleted_count: u64 = 0;

        for (meta, num_rows) in eligible_files {
            match object_store.delete(&meta.location).await {
                Ok(()) => {
                    let row_count = num_rows.unwrap_or(0);
                    let Ok(rows) = u64::try_from(row_count) else {
                        return Err(format!(
                            "Retention: invalid row count {row_count} for file {} (cannot convert to u64)",
                            meta.location
                        )
                        .into());
                    };

                    total_rows = total_rows.saturating_add(rows);
                    deleted_count += 1;

                    tracing::debug!(
                        table = %self.table_name,
                        path = %meta.location,
                        size = meta.size,
                        num_rows = num_rows.unwrap_or(0),
                        "Retention: deleted expired file"
                    );
                }
                Err(object_store::Error::NotFound { .. }) => {
                    // File already deleted (race with another retention check) — safe to ignore
                    tracing::debug!(
                        table = %self.table_name,
                        path = %meta.location,
                        "Retention: file already deleted (NotFound)"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        table = %self.table_name,
                        path = %meta.location,
                        error = %e,
                        "Retention: failed to delete expired file"
                    );
                    return Err(Box::new(e));
                }
            }
        }

        tracing::debug!(
            table = %self.table_name,
            source,
            "Evicted {deleted_count} file(s), {total_rows} row(s)"
        );

        Ok(total_rows)
    }

    /// Internal implementation that returns structured deletion results
    /// including cleanup metadata for post-delete operations.
    pub(crate) async fn delete_from_internal(
        &self,
    ) -> Result<FileBasedDeletionResult, Box<dyn std::error::Error + Send + Sync>> {
        // Parse the filter expression to extract column name and threshold
        let (col_name, _op, threshold) = extract_retention_column_and_threshold(&self.filter)
            .map_err(|e| format!("Failed to extract retention column and threshold: {e}"))?;

        tracing::debug!(
            table = %self.table_name,
            column = %col_name,
            threshold = %threshold,
            "File-based retention: discovering eligible files"
        );

        // Clone main listing table once to avoid holding locks across await points
        let listing_table = {
            self.listing_table
                .read()
                .map_err(|_| "Listing table lock poisoned".to_string())?
                .clone()
        };

        // A single throwaway SessionContext for the entire operation. It only
        // provides the object-store registry.
        // Vortex footer/segment caches live inside the VortexFormat embedded in the
        // shared ListingTable and are unaffected by this SessionContext.
        let ctx = SessionContext::new();

        // Get the object store for file deletion
        let object_store_url = listing_table
            .table_paths()
            .first()
            .map(datafusion_datasource::ListingTableUrl::object_store)
            .ok_or("Table has no paths")?;

        let object_store: Arc<dyn ObjectStore> = ctx
            .runtime_env()
            .object_store_registry
            .get_store(object_store_url.as_ref())
            .map_err(|e| format!("Failed to get object store: {e}"))?;

        let mut total_deleted_rows: u64 = 0;

        // 1. Discover and delete eligible files from the main listing table
        let main_scan = self
            .retention_eligible_files(&ctx, &listing_table, &col_name, &threshold)
            .await
            .map_err(|e| format!("Failed to discover retention-eligible files: {e}"))?;

        if !main_scan.files.is_empty() {
            let rows = self
                .delete_eligible_files(&main_scan.files, object_store.as_ref(), "main")
                .await?;
            total_deleted_rows = total_deleted_rows.saturating_add(rows);
        }

        // 2. Discover and delete eligible files from protected snapshot tables
        let mut emptied_snapshot_ids = Vec::new();

        for (idx, (snapshot_id, snapshot_table)) in
            self.protected_snapshot_tables.iter().flatten().enumerate()
        {
            let scan_result = self
                .retention_eligible_files(&ctx, snapshot_table, &col_name, &threshold)
                .await
                .map_err(|e| {
                    format!(
                        "Failed to discover retention-eligible files in protected snapshot {snapshot_id}: {e}"
                    )
                })?;

            if scan_result.files.is_empty() {
                continue;
            }

            let source = format!("snapshot[{idx}]");
            let rows = self
                .delete_eligible_files(&scan_result.files, object_store.as_ref(), &source)
                .await?;
            total_deleted_rows = total_deleted_rows.saturating_add(rows);

            if scan_result.all_matched {
                tracing::debug!(
                    table = %self.table_name,
                    snapshot_id,
                    "Protected snapshot fully emptied by file-based retention"
                );
                emptied_snapshot_ids.push(snapshot_id.clone());
            }
        }

        if total_deleted_rows == 0 {
            tracing::debug!(
                table = %self.table_name,
                "File-based retention: no eligible files found"
            );
        }

        Ok(FileBasedDeletionResult {
            total_deleted_rows,
            emptied_snapshot_ids,
        })
    }
}

#[async_trait]
impl DeletionSink for FileBasedDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let result = self.delete_from_internal().await?;

        // Clean up emptied protected snapshots: catalog, in-memory map, and directory.
        if !result.emptied_snapshot_ids.is_empty() {
            self.cleanup_emptied_snapshots(&result.emptied_snapshot_ids)
                .await;
        }

        Ok(result.total_deleted_rows)
    }
}

impl FileBasedDeletionSink {
    /// Clean up protected snapshots that were fully emptied by file-based retention.
    ///
    /// For each emptied snapshot:
    /// 1. Remove the snapshot sequence from the catalog.
    /// 2. Remove the entry from the in-memory `protected_snapshots` map.
    /// 3. Delete the now-empty snapshot directory from disk.
    ///
    /// Errors are logged as warnings but do not fail the overall delete operation
    /// — the data files are already removed, so cleanup is best-effort.
    async fn cleanup_emptied_snapshots(&self, emptied_ids: &[String]) {
        for snapshot_id in emptied_ids {
            // 1. Remove snapshot sequence from catalog
            if let Err(e) = self
                .catalog
                .clear_snapshot_sequence(self.table_id, snapshot_id)
                .await
            {
                tracing::warn!(
                    "Failed to clear snapshot sequence for snapshot {snapshot_id} in table {}: {e}",
                    self.table_name
                );
                continue;
            }

            // 2. Remove from in-memory map
            if let Ok(mut guard) = self.protected_snapshots.write() {
                guard.remove(snapshot_id);
            } else {
                tracing::warn!(
                    "Protected snapshots lock poisoned while cleaning up snapshot {snapshot_id} in table {}",
                    self.table_name
                );
                continue;
            }

            // 3. Delete the empty snapshot directory
            let snapshot_dir = std::path::PathBuf::from(&self.table_path)
                .join(self.table_id.to_string())
                .join(snapshot_id);
            match tokio::fs::remove_dir_all(&snapshot_dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to remove empty snapshot directory {}: {e}",
                        snapshot_dir.display()
                    );
                }
            }

            tracing::debug!(
                "Cleaned up emptied protected snapshot {snapshot_id} for table {}",
                self.table_name
            );
        }
    }
}
