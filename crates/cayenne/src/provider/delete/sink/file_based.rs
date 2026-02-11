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

use crate::catalog::{CatalogError, CatalogResult};
use crate::provider::retention::extract_retention_column_and_threshold;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::TableProvider;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::{Arc, RwLock};

/// File-based deletion sink for time-based retention.
///
/// Discovers files eligible for deletion based on per-file column statistics
/// and deletes them from the filesystem. This is used for position-based tables
/// with `retention_period` configured.
///
/// # Workflow
///
/// 1. Parse the filter expression to extract column name and threshold scalar.
/// 2. Call [`list_files_for_scan`] to enumerate files with per-file statistics.
/// 3. For each file, check if `max(retention_col) < threshold` — if so, the
///    file is eligible for deletion (all rows are expired).
/// 4. Delete eligible files from the filesystem.
/// 5. Return the total number of deleted rows (from per-file stats).
///
/// # Notes
///
/// Unlike [`CayenneDeletionSink`](super::CayenneDeletionSink), this sink does
/// not currently scan protected snapshots. File-based deletes are only enabled for the
/// position-based strategy (no primary key), which does not use protected
/// snapshots
pub struct FileBasedDeletionSink {
    /// Listing table to enumerate files and collect per-file statistics.
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    /// The retention filter expression (e.g., `event_time < literal`).
    filter: Expr,
    /// Table name for logging.
    table_name: String,
}

impl FileBasedDeletionSink {
    /// Create a new file-based deletion sink.
    pub fn new(
        listing_table: Arc<RwLock<Arc<ListingTable>>>,
        filter: Expr,
        table_name: String,
    ) -> Self {
        Self {
            listing_table,
            filter,
            table_name,
        }
    }

    /// Discover files eligible for retention-based deletion.
    ///
    /// Lists all files via [`ListingTable::list_files_for_scan`] and checks
    /// per-file column statistics. A file is eligible when its
    /// `max(retention_col) < threshold` — meaning all rows in the file are
    /// older than the retention threshold.
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
    ) -> CatalogResult<Vec<(ObjectMeta, Option<usize>)>> {
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
        let mut eligible = Vec::new();
        for file_group in &file_groups {
            for file in file_group.iter() {
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

        Ok(eligible)
    }
}

#[async_trait]
impl DeletionSink for FileBasedDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // 1. Parse the filter expression to extract column name and threshold
        let (col_name, _op, threshold) = extract_retention_column_and_threshold(&self.filter)
            .map_err(|e| format!("Failed to extract retention column and threshold: {e}"))?;

        tracing::debug!(
            table = %self.table_name,
            column = %col_name,
            threshold = %threshold,
            "File-based retention: discovering eligible files"
        );

        // Clone listing table once to avoid holding locks across await points
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

        // 2. Discover eligible files
        let eligible_files = self
            .retention_eligible_files(&ctx, &listing_table, &col_name, &threshold)
            .await
            .map_err(|e| format!("Failed to discover retention-eligible files: {e}"))?;

        if eligible_files.is_empty() {
            tracing::debug!(
                table = %self.table_name,
                "File-based retention: no eligible files found"
            );
            return Ok(0);
        }

        // 3. Get the object store for file deletion
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

        // 4. Delete eligible files and count rows
        let mut total_rows: u64 = 0;
        let mut deleted_files: u64 = 0;

        for (meta, num_rows) in &eligible_files {
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
                    deleted_files += 1;

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

        tracing::debug!("Evicted {deleted_files} files for {}", self.table_name);

        Ok(total_rows)
    }
}
