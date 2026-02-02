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

//! Position-based deletion methods for `CayenneDeletionSink`.
//!
//! This module implements deletion logic for tables WITHOUT a primary key.
//! It uses file-local row positions tracked via `RoaringBitmap` for efficient
//! row exclusion during Vortex scans.

use super::super::vector_io::{DeletionIdentifier, DeletionVectorWriteSpec, DeletionVectorWriter};
use super::{catalog_error_to_box, CayenneDeletionSink};
use crate::provider::constants::DELETION_CACHE_LOCK_POISONED;
use crate::provider::utils::convert_to_u64_box;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::utils::get_available_parallelism;
use datafusion_common::DFSchema;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::Expr;
use datafusion_physical_expr::create_physical_expr;
use futures::StreamExt;
use object_store::ObjectStore;
use roaring::{RoaringBitmap, RoaringTreemap};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use vortex::array::arrow::IntoArrowArray;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
use vortex::VortexSessionDefault;
use vortex_datafusion::DefaultExpressionConvertor;
use vortex_datafusion::ExpressionConvertor;
use vortex_session::VortexSession;

static MAX_CONCURRENT_FILE_SCANS: LazyLock<usize> = LazyLock::new(get_available_parallelism);

impl CayenneDeletionSink {
    /// Delete filtered rows using Vortex-native streaming scan with per-file deletion tracking.
    ///
    /// Uses Vortex's `row_idx()` expression to project **only row indices** (no data columns read),
    /// with filters pushed directly to the Vortex scan for chunk-level pruning via statistics.
    /// Deletion vectors are tracked per-file using file-local positions (stable across appends).
    ///
    /// # Key optimizations
    ///
    /// - **Zero data I/O**: Projects only `row_idx()`, skips all data columns
    /// - **Filter pushdown**: Leverages Vortex scan statistics for chunk pruning
    /// - **Per-file deletion vectors**: File-local row IDs remain valid regardless of scan order
    /// - **Streaming**: No global materialization
    ///
    /// # Returns
    ///
    /// The total number of **newly** deleted rows across all files.
    pub(super) async fn delete_filtered_rows_position_based(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        if self.filters.is_empty() {
            return Err("Method requires a WHERE clause filter. No filter was specified.".into());
        }

        let mut per_file_row_ids: HashMap<String, Vec<u64>> = HashMap::new();

        // Build Vortex filter once - all tables share the same schema
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let vortex_filter = build_vortex_filter(&self.filters, &df_schema)?;

        tracing::debug!(
            vortex_filter = ?vortex_filter,
            "Converted filters to Vortex expression for position-based deletion"
        );

        for table in tables {
            let table_scan_start = std::time::Instant::now();

            // Future optimization: extract partition-only filters from self.filters when partition
            // pruning is supported for Cayenne. Pass them to list_files_for_scan for partition pruning,
            // reducing the number of files to scan. Note: self.filters may contain non-partition column
            // filters which would cause errors in pruned_partition_list, so filtering by partition
            // column names would be required.
            let partition_filters: &[Expr] = &[];

            // List files for this table with partition pruning applied (if partitioned)
            let (file_groups, _stats) = table
                .list_files_for_scan(&ctx.state(), partition_filters, None)
                .await?;

            // Get the object store URL for this table
            let object_store_url = table
                .table_paths()
                .first()
                .map(datafusion_datasource::ListingTableUrl::object_store)
                .ok_or("Table has no paths")?;

            // Get the object store from the runtime env
            let object_store = ctx
                .runtime_env()
                .object_store_registry
                .get_store(object_store_url.as_ref())
                .map_err(|e| format!("Failed to get object store: {e}"))?;

            // Scan files in parallel with bounded concurrency using buffer_unordered
            let vortex_session = VortexSession::default();

            // Build futures directly using a for loop to avoid iterator lifetime issues:
            // 1. FileGroup::iter() returns `impl Iterator` (opaque type) - flat_map can't unify these
            // 2. Async futures for buffer_unordered must be 'static, requiring owned data
            let mut scan_futures = Vec::new();
            for fg in &file_groups {
                for pf in fg.iter() {
                    let file_path = pf.path().to_string();
                    let vortex_session = vortex_session.clone();
                    let object_store = Arc::clone(&object_store);
                    let vortex_filter = vortex_filter.clone();
                    scan_futures.push(async move {
                        let result = self
                            .scan_file_for_new_deletions(
                                &file_path,
                                &vortex_session,
                                &object_store,
                                vortex_filter.as_ref(),
                            )
                            .await;
                        (file_path, result)
                    });
                }
            }

            let mut stream =
                futures::stream::iter(scan_futures).buffer_unordered(*MAX_CONCURRENT_FILE_SCANS);

            let mut table_rows_matched: usize = 0;
            let mut table_files_scanned: usize = 0;

            while let Some((file_path, result)) = stream.next().await {
                let row_ids = result?;

                table_files_scanned += 1;
                table_rows_matched += row_ids.len();

                if !row_ids.is_empty() {
                    tracing::trace!(
                        file_path = %file_path,
                        new_deletions = row_ids.len(),
                        "File has rows matching deletion filter"
                    );
                    per_file_row_ids.insert(file_path, row_ids);
                }
            }

            tracing::debug!(
                table_path = %table.table_paths().first().map_or("unknown", datafusion_datasource::ListingTableUrl::as_str),
                files_scanned = table_files_scanned,
                rows_matched = table_rows_matched,
                elapsed = ?table_scan_start.elapsed(),
                "Retention scan completed for table"
            );
        }

        if per_file_row_ids.is_empty() {
            tracing::debug!("No new deletions to persist");
            return Ok(0);
        }

        self.persist_position_based_deletions(per_file_row_ids)
            .await
    }

    /// Scan a single Vortex file for rows matching the deletion filter.
    ///
    /// Uses Vortex's `row_idx()` expression to project **only row indices** (no data columns),
    /// with the filter pushed to the scan for chunk-level pruning. Already-deleted rows
    /// (from the cache) are excluded via a selection, so only NEW deletions are returned.
    ///
    /// # Returns
    ///
    /// Vector of file-local row indices that match the filter (new deletions only).
    async fn scan_file_for_new_deletions(
        &self,
        file_path: &str,
        vortex_session: &VortexSession,
        object_store: &Arc<dyn ObjectStore>,
        vortex_filter: Option<&vortex::expr::Expression>,
    ) -> Result<Vec<u64>, Box<dyn std::error::Error + Send + Sync>> {
        // Get existing deletions for this file to exclude from scan
        let already_deleted = {
            let guard = self
                .cached_deleted_row_ids
                .read()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            if let Some(existing_bitmap) = guard.get(file_path) {
                // ExcludeRoaring is preferred over ExcludeByIndex: less memory (~2 bits vs 8 bytes/row)
                // and enables native bitmap operations in Vortex (intersection, is_disjoint) which is faster
                let excluded_indices: RoaringTreemap =
                    existing_bitmap.iter().map(u64::from).collect();
                Some(vortex::scan::Selection::ExcludeRoaring(excluded_indices))
            } else {
                None
            }
        };

        // Open the Vortex file directly using the session
        let vxf = vortex_session
            .open_options()
            .open_object_store(object_store, file_path)
            .await
            .map_err(|e| format!("Failed to open Vortex file {file_path}: {e}"))?;

        // Build the scan with row_idx() projection only - no data columns read.
        let mut scan_builder = vxf.scan()?.with_projection(row_idx());

        if let Some(selection) = already_deleted {
            scan_builder = scan_builder.with_selection(selection);
        }

        // Apply filter if we have one
        if let Some(filter) = vortex_filter {
            scan_builder = scan_builder.with_filter(filter.clone());
        }

        // Execute the scan and collect row indices
        // All returned rows are NEW deletions (already-deleted rows were excluded by selection)
        let mut stream = scan_builder.into_stream()?;
        let mut new_row_ids: Vec<u64> = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk =
                chunk_result.map_err(|e| format!("Failed to read chunk from {file_path}: {e}"))?;

            // The chunk contains row indices as U64 primitive array
            // Convert Vortex array directly to Arrow array (not RecordBatch)
            let arrow_array = chunk
                .into_arrow_preferred()
                .map_err(|e| format!("Failed to convert chunk to Arrow array: {e}"))?;

            if arrow_array.is_empty() {
                continue;
            }

            // row_idx() returns u64 values
            let row_indices = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| "row_idx() did not return UInt64Array".to_string())?;

            new_row_ids.extend_from_slice(row_indices.values());
        }

        Ok(new_row_ids)
    }

    /// Persist per-file position-based deletions.
    ///
    /// Each entry in `row_ids` maps a source data file path to the
    /// file-local row positions to delete. This method:
    ///
    /// 1. Merges new positions with existing deletions from the cache
    /// 2. Writes a combined deletion vector per source file
    /// 3. Updates the in-memory cache
    ///
    /// The deletion vector file contains ALL deleted positions (existing + new),
    /// enabling the scan to skip all deleted rows in a single lookup.
    ///
    /// # Arguments
    ///
    /// * `row_ids` - Map of source data file path to file-local row IDs
    ///
    /// # Returns
    ///
    /// The total number of **newly** deleted rows (not counting already-deleted).
    pub(super) async fn persist_position_based_deletions(
        &self,
        row_ids: HashMap<String, Vec<u64>>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        if row_ids.is_empty() {
            return Ok(0);
        }

        // Read existing deletions to merge with new ones
        let existing_deletions: Arc<HashMap<String, RoaringBitmap>> = {
            let guard = self
                .cached_deleted_row_ids
                .read()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;
            Arc::clone(&*guard)
        };

        let writer = DeletionVectorWriter::new(&self.table_metadata);

        // Track new deletion count for return value
        let new_deletion_count: usize = row_ids.values().map(Vec::len).sum();

        // Create one DeletionVectorWriteSpec per file with MERGED deletions
        // (existing from cache + new from this operation)
        let specs: Vec<DeletionVectorWriteSpec> = row_ids
            .iter()
            .filter(|(_, row_ids)| !row_ids.is_empty())
            .map(|(file_path, new_row_ids)| {
                // Start with existing deletions for this file (if any)
                let mut combined_ids: Vec<u64> =
                    if let Some(existing_bitmap) = existing_deletions.get(file_path) {
                        existing_bitmap.iter().map(u64::from).collect()
                    } else {
                        Vec::new()
                    };

                // Add new deletions
                combined_ids.extend(new_row_ids.iter().copied());

                DeletionVectorWriteSpec::new_position_based(file_path.clone(), combined_ids)
            })
            .collect();

        if specs.is_empty() {
            return Ok(0);
        }

        let results = writer.write(specs).await.map_err(catalog_error_to_box)?;

        for result in results {
            self.catalog
                .add_delete_file(result.delete_file)
                .await
                .map_err(catalog_error_to_box)?;

            // Validate we received position-based identifiers as expected
            if matches!(&result.identifiers, DeletionIdentifier::KeyBased(_)) {
                return Err("Unexpected key-based deletion in position-based sink".into());
            }
        }

        // Pre-build updated bitmaps OUTSIDE the write lock to minimize lock hold time.
        let cache_updates: HashMap<String, RoaringBitmap> = row_ids
            .iter()
            .map(|(file_path, row_ids)| {
                let mut bitmap = existing_deletions
                    .get(file_path)
                    .cloned()
                    .unwrap_or_default();
                bitmap.extend(row_ids.iter().filter_map(|&id| u32::try_from(id).ok()));
                (file_path.clone(), bitmap)
            })
            .collect();

        // Quick write lock - just insert pre-built entries
        {
            let mut guard = self
                .cached_deleted_row_ids
                .write()
                .map_err(|_| DELETION_CACHE_LOCK_POISONED.to_string())?;

            let map: &mut HashMap<String, RoaringBitmap> = Arc::make_mut(&mut *guard);
            map.extend(cache_updates);
        }

        // Return count of NEW deletions
        convert_to_u64_box(new_deletion_count, "new deletion count")
    }
}

/// Build a combined Vortex filter expression from `DataFusion` logical filters.
///
/// Converts each filter through: Logical Expr → Physical Expr → Vortex Expression,
/// then combines them with AND.
fn build_vortex_filter(
    filters: &[datafusion_expr::Expr],
    df_schema: &DFSchema,
) -> Result<Option<vortex::expr::Expression>, Box<dyn std::error::Error + Send + Sync>> {
    use vortex::expr::and;

    if filters.is_empty() {
        return Ok(None);
    }

    let execution_props = ExecutionProps::new();
    let expr_convertor = DefaultExpressionConvertor::default();

    // Convert logical filters to physical expressions
    let physical_filters: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> = filters
        .iter()
        .map(|f| {
            // Type coercion is required because logical filter expressions may have
            // mismatched types (e.g., comparing Int32 column to Int64 literal).
            // The rewriter applies SQL type coercion rules to ensure operands have
            // compatible types before conversion to physical expressions.
            let mut rewriter = TypeCoercionRewriter::new(df_schema);
            let coerced_filter = f.clone().rewrite(&mut rewriter)?.data;
            create_physical_expr(&coerced_filter, df_schema, &execution_props)
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?;

    // Convert to Vortex expressions and combine with AND
    let mut combined: Option<vortex::expr::Expression> = None;
    for phys_filter in &physical_filters {
        match expr_convertor.convert(phys_filter.as_ref()) {
            Ok(vortex_expr) => {
                combined = Some(match combined {
                    Some(existing) => and(existing, vortex_expr),
                    None => vortex_expr,
                });
            }
            Err(e) => {
                return Err(format!(
                    "Failed to convert filter to Vortex expression: {e}. Filter: {phys_filter}"
                )
                .into());
            }
        }
    }

    Ok(combined)
}
