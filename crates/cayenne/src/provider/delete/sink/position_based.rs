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
use super::CayenneDeletionSink;
use crate::provider::Error;
use crate::provider::deletion_strategy::PositionDeletionVector;
use crate::provider::utils::convert_to_u64_box;
use arrow::row::{OwnedRow, RowConverter};
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::context::SessionContext;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::DFSchema;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::utils::get_available_parallelism;
use datafusion_expr::Expr;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_expr::expressions as phys_expr;
use futures::StreamExt;
use object_store::ObjectStore;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use vortex::VortexSessionDefault;
use vortex::array::arrow::IntoArrowArray;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
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
    ) -> crate::provider::Result<u64> {
        let table_name = &self.table_metadata.table_name;

        if self.filters.is_empty() {
            return Err(Error::Internal {
                table: table_name.clone(),
                message: "Method requires a WHERE clause filter. No filter was specified."
                    .to_string(),
            });
        }

        let mut per_file_row_ids: HashMap<String, Vec<u64>> = HashMap::new();

        // Build Vortex filter once - all tables share the same schema
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let vortex_filter = build_vortex_filter(&self.filters, &df_schema)
            .map_err(datafusion_common::DataFusionError::External)?;

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
            let list_result = table
                .list_files_for_scan(&ctx.state(), partition_filters, None)
                .await?;
            let file_groups = list_result.file_groups;

            // Get the object store URL for this table
            let object_store_url = table
                .table_paths()
                .first()
                .map(datafusion_datasource::ListingTableUrl::object_store)
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: "Table has no paths".to_string(),
                })?;

            // Get the object store from the runtime env
            let object_store = ctx
                .runtime_env()
                .object_store_registry
                .get_store(object_store_url.as_ref())?;

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

        let total_new_deletions: usize = per_file_row_ids.values().map(std::vec::Vec::len).sum();
        tracing::debug!(
            table = %table_name,
            total_new_deletions,
            files_with_deletions = per_file_row_ids.len(),
            "Position-based delete: persisting deletions"
        );

        self.persist_position_based_deletions(per_file_row_ids)
            .await
    }

    /// Delete rows by hash-probing key columns against a set of matched keys.
    ///
    /// This is the fast path for `MERGE INTO` on `PositionBased` tables. Instead of
    /// building an O(N) filter expression and pushing it into every file scan, this
    /// method scans each file once and performs an O(1) `HashSet` lookup per row.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Session context for object store access
    /// * `tables` - Listing tables to scan (main + protected snapshots)
    /// * `matched_keys` - Key tuples from the MERGE join output
    /// * `key_columns` - Column names for the ON keys
    ///
    /// # Returns
    ///
    /// The total number of newly deleted rows.
    pub(crate) async fn delete_by_key_hash_probe(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
        matched_keys: std::collections::HashSet<Vec<datafusion_common::ScalarValue>>,
        key_columns: &[String],
    ) -> crate::provider::Result<u64> {
        let table_name = &self.table_metadata.table_name;

        if matched_keys.is_empty() {
            return Ok(0);
        }

        let mut per_file_row_ids: HashMap<String, Vec<u64>> = HashMap::new();

        for table in tables {
            let table_scan_start = std::time::Instant::now();

            let partition_filters: &[Expr] = &[];
            let list_result = table
                .list_files_for_scan(&ctx.state(), partition_filters, None)
                .await?;
            let file_groups = list_result.file_groups;

            let object_store_url = table
                .table_paths()
                .first()
                .map(datafusion_datasource::ListingTableUrl::object_store)
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: "Table has no paths".to_string(),
                })?;

            let object_store = ctx
                .runtime_env()
                .object_store_registry
                .get_store(object_store_url.as_ref())?;

            let vortex_session = VortexSession::default();

            // Build futures with owned values to avoid lifetime issues from
            // nested iterator adapters over `FileGroup::iter()`.
            let mut scan_futures = Vec::new();
            for fg in &file_groups {
                for pf in fg.iter() {
                    let file_path = pf.path().to_string();
                    let vortex_session = vortex_session.clone();
                    let object_store = Arc::clone(&object_store);
                    let matched_keys = &matched_keys;
                    scan_futures.push(async move {
                        let result = self
                            .scan_file_for_key_matches(
                                &file_path,
                                &vortex_session,
                                &object_store,
                                matched_keys,
                                key_columns,
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
                        "File has rows matching key-probe deletion"
                    );
                    per_file_row_ids
                        .entry(file_path)
                        .or_default()
                        .extend(row_ids);
                }
            }

            tracing::debug!(
                table_path = %table.table_paths().first().map_or("unknown", datafusion_datasource::ListingTableUrl::as_str),
                files_scanned = table_files_scanned,
                rows_matched = table_rows_matched,
                elapsed = ?table_scan_start.elapsed(),
                "Key-probe deletion scan completed for table"
            );
        }

        if per_file_row_ids.is_empty() {
            tracing::debug!("No deletions found via key-probe scan");
            return Ok(0);
        }

        let total_new_deletions: usize = per_file_row_ids.values().map(std::vec::Vec::len).sum();
        tracing::debug!(
            table = %table_name,
            total_new_deletions,
            files_with_deletions = per_file_row_ids.len(),
            "Key-probe delete: persisting deletions"
        );

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
    ) -> crate::provider::Result<Vec<u64>> {
        let table_name = &self.table_metadata.table_name;

        // Get existing deletions for this file to exclude from scan
        let cached_deleted_row_ids = self
            .pk_deletion_strategy
            .position_based_cache()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: "scan_file_for_deletions called with incompatible PkDeletionStrategy"
                    .to_string(),
            })?;
        // ArcSwap load is wait-free; the snapshot is immutable for the lifetime of `current`.
        let already_deleted = {
            let current = cached_deleted_row_ids.load();
            current
                .get(file_path)
                .map(|deletion_vector| deletion_vector.access_plan())
        };

        // Open the Vortex file directly using the session
        let vxf = vortex_session
            .open_options()
            .open_object_store(object_store, file_path)
            .await
            .map_err(|e| Error::Vortex {
                operation: "open vortex file for deletion scan",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

        // Build the scan with row_idx() projection only - no data columns read.
        let mut scan_builder = vxf
            .scan()
            .map_err(|e| Error::Vortex {
                operation: "build vortex scan for deletion",
                table: table_name.clone(),
                source: Box::new(e),
            })?
            .with_projection(row_idx());

        if let Some(access_plan) = already_deleted {
            scan_builder = access_plan.apply_to_builder(scan_builder);
        }

        // Apply filter if we have one
        if let Some(filter) = vortex_filter {
            scan_builder = scan_builder.with_filter(filter.clone());
        }

        // Execute the scan and collect row indices
        // All returned rows are NEW deletions (already-deleted rows were excluded by selection)
        let mut stream = scan_builder.into_stream().map_err(|e| Error::Vortex {
            operation: "start vortex scan stream for deletion",
            table: table_name.clone(),
            source: Box::new(e),
        })?;
        let mut new_row_ids: Vec<u64> = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| Error::Vortex {
                operation: "read vortex chunk for deletion",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

            // The chunk contains row indices as U64 primitive array
            // Convert Vortex array directly to Arrow array (not RecordBatch)
            let arrow_array = chunk.into_arrow_preferred().map_err(|e| Error::Vortex {
                operation: "convert vortex chunk to arrow array",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

            if arrow_array.is_empty() {
                continue;
            }

            // row_idx() returns u64 values
            let row_indices = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: "row_idx() did not return UInt64Array".to_string(),
                })?;

            new_row_ids.extend_from_slice(row_indices.values());
        }

        Ok(new_row_ids)
    }

    /// Scan a single Vortex file and find rows whose key columns match `matched_keys`.
    ///
    /// Unlike [`scan_file_for_new_deletions`] which pushes a Vortex filter expression,
    /// this method reads data and performs an O(1) `HashSet` probe per row. This is
    /// dramatically faster when the number of matched keys (N) is large, because the
    /// filter-based path evaluates O(N) comparisons per chunk.
    ///
    /// The scan projects *only* the key columns needed for the probe (using
    /// `vortex::expr::select`). File-local row positions are tracked with a manual
    /// row counter (`row_position`), and positions that are already deleted (from the
    /// position-based cache) are skipped so this method returns only candidates for
    /// NEW deletions.
    ///
    /// This projection is critical for wide tables — without it every MERGE key-probe
    /// would read every column of every file.
    ///
    /// # Returns
    ///
    /// Vector of file-local row indices whose key columns are in `matched_keys`.
    async fn scan_file_for_key_matches(
        &self,
        file_path: &str,
        vortex_session: &VortexSession,
        object_store: &Arc<dyn ObjectStore>,
        matched_keys: &std::collections::HashSet<Vec<datafusion_common::ScalarValue>>,
        key_columns: &[String],
    ) -> crate::provider::Result<Vec<u64>> {
        let table_name = &self.table_metadata.table_name;

        // Snapshot already-deleted positions for this file from the cache.
        let already_deleted: Option<Arc<PositionDeletionVector>> = {
            let cache = self
                .pk_deletion_strategy
                .position_based_cache()
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message:
                        "scan_file_for_key_matches called with incompatible PkDeletionStrategy"
                            .to_string(),
                })?;
            cache.load().get(file_path).cloned()
        };

        // Open the Vortex file directly.
        let vxf = vortex_session
            .open_options()
            .open_object_store(object_store, file_path)
            .await
            .map_err(|e| Error::Vortex {
                operation: "open vortex file for key-match scan",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

        // Project *only* the key columns required for the HashSet probe.
        // We maintain our own row_position counter, so we do not need the
        // Vortex row_idx column. This is the fix for the wide-table regression
        // (see bench `wide_table_key_probe_scan`).
        let mut scan_builder = vxf.scan().map_err(|e| Error::Vortex {
            operation: "build vortex scan for key-match",
            table: table_name.clone(),
            source: Box::new(e),
        })?;

        if !key_columns.is_empty() {
            use vortex::expr::{root, select};
            // `select` accepts Vec<&str> / Vec<Arc<str>>
            let cols: Vec<&str> = key_columns.iter().map(String::as_str).collect();
            let proj = select(cols, root());
            scan_builder = scan_builder.with_projection(proj);
        }

        let mut stream = scan_builder.into_stream().map_err(|e| Error::Vortex {
            operation: "start vortex scan stream for key-match",
            table: table_name.clone(),
            source: Box::new(e),
        })?;

        let mut matching_positions: Vec<u64> = Vec::new();
        let mut row_position: u64 = 0;
        // Resolved on the first chunk and reused across the stream — schema is
        // stable per file, so the per-chunk `index_of` lookups become wasted
        // work as files grow past one chunk.
        let mut key_indices: Option<Vec<usize>> = None;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| Error::Vortex {
                operation: "read vortex chunk for key-match",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

            let arrow_array = chunk.into_arrow_preferred().map_err(|e| Error::Vortex {
                operation: "convert vortex chunk to arrow for key-match",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

            if arrow_array.is_empty() {
                continue;
            }

            // Without projection, Vortex returns a StructArray representing the full row.
            let struct_array = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: "Vortex scan without projection did not return StructArray"
                        .to_string(),
                })?;
            let batch = arrow::record_batch::RecordBatch::from(struct_array);

            let key_indices: &[usize] = if let Some(indices) = &key_indices {
                indices.as_slice()
            } else {
                let resolved: Vec<usize> = key_columns
                    .iter()
                    .map(|col_name| {
                        batch
                            .schema()
                            .index_of(col_name)
                            .map_err(|_| Error::Internal {
                                table: table_name.clone(),
                                message: format!(
                                    "Key column '{col_name}' not found in Vortex file schema"
                                ),
                            })
                    })
                    .collect::<crate::provider::Result<Vec<_>>>()?;
                key_indices = Some(resolved);
                // SAFETY: we just assigned `Some` above
                key_indices.as_deref().unwrap_or(&[])
            };

            for row_idx in 0..batch.num_rows() {
                let key: Vec<datafusion_common::ScalarValue> = key_indices
                    .iter()
                    .map(|&idx| {
                        datafusion_common::ScalarValue::try_from_array(batch.column(idx), row_idx)
                            .map_err(|e| Error::Internal {
                                table: table_name.clone(),
                                message: format!("Failed to extract key value: {e}"),
                            })
                    })
                    .collect::<crate::provider::Result<Vec<_>>>()?;

                let is_already_deleted = u32::try_from(row_position).ok().is_some_and(|pos| {
                    already_deleted
                        .as_ref()
                        .is_some_and(|deletion_vector| deletion_vector.contains(pos))
                });

                if matched_keys.contains(&key) && !is_already_deleted {
                    matching_positions.push(row_position);
                }
                row_position += 1;
            }
        }

        Ok(matching_positions)
    }

    /// Scan a single (freshly written) data file and return `(primary-key,
    /// file-local position)` for **every** row, in physical scan order.
    ///
    /// Used by the `deletion_mode: position` write-time read-back to upgrade
    /// keyset entries from `FileUnlocated` to `FilePositioned`, so a later upsert
    /// of one of these keys can tombstone the prior version by position (pushed
    /// into the Vortex scan) instead of by key (applied above the scan).
    ///
    /// The keys are `RowConverter`-encoded into the SAME `OwnedRow` form the PK
    /// keyset uses, so they collide with the existing keyset entries. The
    /// position is a manual physical-order counter — identical to the row index
    /// `Selection::ExcludeRoaring` consumes (see `scan_file_for_key_matches`),
    /// so captured positions line up with how deletes are applied.
    pub(crate) async fn scan_file_for_all_positions(
        &self,
        file_path: &str,
        object_store: &Arc<dyn ObjectStore>,
        pk_column_names: &[String],
        converter: &RowConverter,
    ) -> crate::provider::Result<Vec<(OwnedRow, u64)>> {
        debug_assert!(
            !pk_column_names.is_empty(),
            "scan_file_for_all_positions requires at least one primary-key column"
        );
        let table_name = &self.table_metadata.table_name;
        let vortex_session = VortexSession::default();

        let vxf = vortex_session
            .open_options()
            .open_object_store(object_store, file_path)
            .await
            .map_err(|e| Error::Vortex {
                operation: "open vortex file for position read-back",
                table: table_name.clone(),
                source: Box::new(e),
            })?;

        // Project ONLY the primary-key columns — no data columns, no row_idx
        // column (positions are tracked with the manual counter below).
        let mut scan_builder = vxf.scan().map_err(|e| Error::Vortex {
            operation: "build vortex scan for position read-back",
            table: table_name.clone(),
            source: Box::new(e),
        })?;
        {
            use vortex::expr::{root, select};
            let cols: Vec<&str> = pk_column_names.iter().map(String::as_str).collect();
            scan_builder = scan_builder.with_projection(select(cols, root()));
        }

        let mut stream = scan_builder.into_stream().map_err(|e| Error::Vortex {
            operation: "start vortex scan stream for position read-back",
            table: table_name.clone(),
            source: Box::new(e),
        })?;

        let mut entries: Vec<(OwnedRow, u64)> = Vec::new();
        let mut row_position: u64 = 0;
        // PK column indices in the projected batch, in `converter` field order.
        // Resolved once on the first chunk (schema is stable per file).
        let mut pk_indices: Option<Vec<usize>> = None;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| Error::Vortex {
                operation: "read vortex chunk for position read-back",
                table: table_name.clone(),
                source: Box::new(e),
            })?;
            let arrow_array = chunk.into_arrow_preferred().map_err(|e| Error::Vortex {
                operation: "convert vortex chunk to arrow for position read-back",
                table: table_name.clone(),
                source: Box::new(e),
            })?;
            if arrow_array.is_empty() {
                continue;
            }
            let struct_array = arrow_array
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: "Vortex position read-back scan did not return StructArray"
                        .to_string(),
                })?;
            let batch = arrow::record_batch::RecordBatch::from(struct_array);

            let indices: &[usize] = if let Some(indices) = &pk_indices {
                indices.as_slice()
            } else {
                let resolved: Vec<usize> = pk_column_names
                    .iter()
                    .map(|name| {
                        batch.schema().index_of(name).map_err(|_| Error::Internal {
                            table: table_name.clone(),
                            message: format!(
                                "Primary-key column '{name}' not found in Vortex file schema"
                            ),
                        })
                    })
                    .collect::<crate::provider::Result<Vec<_>>>()?;
                pk_indices = Some(resolved);
                pk_indices.as_deref().unwrap_or(&[])
            };

            let pk_columns: Vec<arrow::array::ArrayRef> = indices
                .iter()
                .map(|&i| Arc::clone(batch.column(i)))
                .collect();
            let rows = converter
                .convert_columns(&pk_columns)
                .map_err(Error::from)?;

            for row_index in 0..batch.num_rows() {
                entries.push((rows.row(row_index).owned(), row_position));
                row_position += 1;
            }
        }

        Ok(entries)
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
    pub(crate) async fn persist_position_based_deletions(
        &self,
        row_ids: HashMap<String, Vec<u64>>,
    ) -> crate::provider::Result<u64> {
        let table_name = &self.table_metadata.table_name;

        if row_ids.is_empty() {
            return Ok(0);
        }

        // The per-file position-delete cache. For PK-less tables this is the
        // `PositionBased` cache; for PK tables under `deletion_mode: position`
        // it is the `position_deletions` cache that sits alongside the key
        // index (located rows are tombstoned by position, unlocated rows by key).
        let cached_deleted_row_ids = self.pk_deletion_strategy.position_cache();

        // Read existing deletions to merge with new ones (wait-free).
        // The cache value type is `Arc<PositionDeletionVector>` so a clone of the
        // outer map only clones small string keys + Arc pointers, not the
        // deleted-position bitmap/access-plan data itself.
        let existing_deletions = cached_deleted_row_ids.load_full();

        let writer = DeletionVectorWriter::new(&self.table_metadata);

        // Build write specs and precompute cache updates while counting TRUE new deletions
        // (set difference between incoming row_ids and existing cache per file).
        let mut new_deletion_count: usize = 0;
        let mut overflow_count: u64 = 0;
        let mut first_overflow_id: Option<u64> = None;
        let mut specs: Vec<DeletionVectorWriteSpec> = Vec::new();
        let mut cache_updates: HashMap<String, Arc<PositionDeletionVector>> = HashMap::new();

        for (file_path, incoming_row_ids) in row_ids.iter().filter(|(_, ids)| !ids.is_empty()) {
            // Take an immutable snapshot of the existing deletion vector so we
            // can read existing positions for the "is this new?" check and
            // combined-IDs build without cloning unchanged bitmap data.
            let existing_deletion = existing_deletions.get(file_path);

            // Deduplicate incoming row IDs first to avoid over-counting and redundant writes.
            let mut unique_new_row_ids: Vec<u32> = Vec::with_capacity(incoming_row_ids.len());
            for &id in incoming_row_ids {
                if let Ok(id32) = u32::try_from(id) {
                    unique_new_row_ids.push(id32);
                } else {
                    if first_overflow_id.is_none() {
                        first_overflow_id = Some(id);
                    }
                    overflow_count += 1;
                }
            }
            unique_new_row_ids.sort_unstable();
            unique_new_row_ids.dedup();

            if unique_new_row_ids.is_empty() {
                continue;
            }

            let newly_added_for_file = unique_new_row_ids
                .iter()
                .filter(|&&id| existing_deletion.is_none_or(|deletion| !deletion.contains(id)))
                .count();

            if newly_added_for_file == 0 {
                continue;
            }

            new_deletion_count += newly_added_for_file;

            // Union existing + new into one bitmap, then derive the writer-bound
            // `Vec<u64>` from its monotone iterator — saves a separate
            // `Vec<u64> + sort/dedup` pass. See `position_delete_redundant_walks`
            // bench. Only THIS file's bitmap is cloned; unchanged file bitmaps
            // stay shared through `Arc`s in the outer snapshot.
            let mut updated_bitmap = existing_deletion
                .map_or_else(RoaringBitmap::new, |deletion_vector| {
                    deletion_vector.to_bitmap()
                });
            updated_bitmap.extend(unique_new_row_ids.iter().copied());

            // RoaringBitmap::iter yields strictly-increasing values, so the writer
            // can skip its sort/dedup pass — see `position_delete_redundant_walks` bench.
            let combined_ids: Vec<u64> = updated_bitmap.iter().map(u64::from).collect();
            specs.push(DeletionVectorWriteSpec::new_position_based_sorted(
                file_path.clone(),
                combined_ids,
            ));

            cache_updates.insert(
                file_path.clone(),
                Arc::new(PositionDeletionVector::new(updated_bitmap)),
            );
        }

        if overflow_count > 0 {
            tracing::warn!(
                "Skipped {} row ID(s) that exceed u32::MAX (first: {}) - table should be compacted",
                overflow_count,
                first_overflow_id.unwrap_or(0)
            );
        }

        if specs.is_empty() {
            return Ok(0);
        }

        let results = writer.write(specs).await?;

        for result in results {
            self.catalog.add_delete_file(result.delete_file).await?;

            // Validate we received position-based identifiers as expected
            if matches!(&result.identifiers, DeletionIdentifier::KeyBased(_)) {
                return Err(Error::Internal {
                    table: table_name.clone(),
                    message: "Unexpected key-based deletion in position-based sink".to_string(),
                });
            }
        }

        // Build a fresh snapshot. Cloning the outer HashMap now only clones
        // small (String, Arc<PositionDeletionVector>) entries — unchanged files
        // share their bitmap/access-plan data with the previous snapshot through
        // the inner Arc. Then overlay the cache_updates entries for files that
        // changed in THIS commit. The pre-inner-Arc revision unconditionally
        // cloned every file's full bitmap on every commit, turning the write into
        // O(total deleted rows across all files) per call.
        let mut updated_map = (*cached_deleted_row_ids.load_full()).clone();
        updated_map.extend(cache_updates);
        cached_deleted_row_ids.store(Arc::new(updated_map));

        // Return count of NEW deletions
        convert_to_u64_box(new_deletion_count, "new deletion count").map_err(|e| Error::Internal {
            table: table_name.clone(),
            message: e.to_string(),
        })
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

    // Convert to Vortex expressions and combine with AND.
    // When direct conversion fails (e.g., struct() IN-list from composite-key
    // deletes), try decomposing the expression into Vortex-compatible form.
    let mut combined: Option<vortex::expr::Expression> = None;
    for phys_filter in &physical_filters {
        let vortex_expr = match expr_convertor.convert(phys_filter.as_ref()) {
            Ok(expr) => expr,
            Err(_) => {
                match try_decompose_struct_in_list(phys_filter.as_ref(), &expr_convertor, df_schema)
                    .or_else(|| {
                        try_decompose_struct_eq(phys_filter.as_ref(), &expr_convertor, df_schema)
                    }) {
                    Some(decomposed) => decomposed,
                    None => {
                        return Err(format!(
                            "Failed to convert filter to Vortex expression. Filter: {phys_filter}"
                        )
                        .into());
                    }
                }
            }
        };
        combined = Some(match combined {
            Some(existing) => and(existing, vortex_expr),
            None => vortex_expr,
        });
    }

    Ok(combined)
}

/// Decompose `struct(col1, col2, ...) IN (struct_lit1, struct_lit2, ...)` into a
/// balanced OR-tree of AND-equalities that Vortex can evaluate.
///
/// `DataFusion` converts `(k1, k2) IN ((v1, w1), (v2, w2))` into
/// `struct(k1, k2) IN (SET)` which Vortex's expression convertor doesn't support.
/// This function decomposes it into:
///   `(k1 = v1 AND k2 = w1) OR (k1 = v2 AND k2 = w2)`
/// using a balanced binary tree to keep expression depth at O(log N).
fn try_decompose_struct_in_list(
    expr: &dyn datafusion_physical_expr::PhysicalExpr,
    convertor: &DefaultExpressionConvertor,
    df_schema: &DFSchema,
) -> Option<vortex::expr::Expression> {
    use datafusion_common::ScalarValue;

    let in_list = expr.as_any().downcast_ref::<phys_expr::InListExpr>()?;

    // Check that the value expression is struct(col1, col2, ...),
    // possibly wrapped in a CAST (DataFusion may insert type coercion).
    let value_expr: &dyn datafusion_physical_expr::PhysicalExpr = in_list.expr().as_ref();
    let struct_fn = if let Some(sf) = value_expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::ScalarFunctionExpr>(
    ) {
        sf
    } else if let Some(cast_expr) = value_expr.as_any().downcast_ref::<phys_expr::CastExpr>() {
        cast_expr
            .expr()
            .as_any()
            .downcast_ref::<datafusion_physical_expr::ScalarFunctionExpr>()?
    } else {
        return None;
    };
    if struct_fn.name() != "struct" {
        return None;
    }

    // Convert each column in struct() args to a Vortex expression.
    // Also record each column's native data type for literal casting.
    let mut vortex_columns: Vec<vortex::expr::Expression> =
        Vec::with_capacity(struct_fn.args().len());
    let mut col_types: Vec<arrow::datatypes::DataType> = Vec::with_capacity(struct_fn.args().len());
    let arrow_schema: arrow::datatypes::Schema = df_schema.as_arrow().as_ref().clone();
    for arg in struct_fn.args() {
        let vx = convertor.convert(arg.as_ref()).ok()?;
        let dt = arg.data_type(&arrow_schema).ok()?;
        vortex_columns.push(vx);
        col_types.push(dt);
    }

    if vortex_columns.is_empty() {
        return None;
    }

    // Build one AND-conjunction per list element: (col1 = v1 AND col2 = w1)
    let row_predicates: Vec<vortex::expr::Expression> = in_list
        .list()
        .iter()
        .filter_map(|elem| {
            let literal = elem.as_any().downcast_ref::<phys_expr::Literal>()?;
            let ScalarValue::Struct(struct_arr) = literal.value() else {
                return None;
            };

            // Build (col1 = v1 AND col2 = w1) for this struct literal.
            // Cast each field scalar to the column's native type to avoid
            // Vortex DType mismatches (e.g., i64 literal vs i32 column).
            let field_eqs: Vec<vortex::expr::Expression> = (0..vortex_columns.len())
                .map(|i| {
                    let field_scalar = ScalarValue::try_from_array(struct_arr.column(i), 0).ok()?;
                    let cast_scalar = field_scalar
                        .cast_to(&col_types[i])
                        .ok()
                        .unwrap_or(field_scalar);
                    let phys_lit = phys_expr::Literal::new(cast_scalar);
                    let vortex_lit = convertor.convert(&phys_lit).ok()?;
                    Some(vortex::expr::eq(vortex_columns[i].clone(), vortex_lit))
                })
                .collect::<Option<Vec<_>>>()?;

            let mut conj = field_eqs.into_iter();
            let first = conj.next()?;
            Some(conj.fold(first, vortex::expr::and))
        })
        .collect();

    if row_predicates.is_empty() {
        return None;
    }

    // Build a balanced binary OR-tree to keep depth at O(log N)
    let result = balanced_or(row_predicates);

    if in_list.negated() {
        Some(vortex::expr::not(result))
    } else {
        Some(result)
    }
}

/// Decompose `struct(col1, col2, ...) = struct_literal` into `col1 = v1 AND col2 = v2 AND ...`.
///
/// `DataFusion` optimizes single-element `IN` lists to equality: `(k1, k2) IN ((v1, v2))` becomes
/// `struct(k1, k2) = {c0:v1, c1:v2}`. Vortex doesn't support struct equality, so we decompose
/// it the same way `try_decompose_struct_in_list` handles the multi-element case.
fn try_decompose_struct_eq(
    expr: &dyn datafusion_physical_expr::PhysicalExpr,
    convertor: &DefaultExpressionConvertor,
    df_schema: &DFSchema,
) -> Option<vortex::expr::Expression> {
    use datafusion_common::ScalarValue;

    let bin_expr = expr
        .as_any()
        .downcast_ref::<datafusion_physical_expr::expressions::BinaryExpr>()?;
    if *bin_expr.op() != datafusion_expr::Operator::Eq {
        return None;
    }

    // LHS must be struct(col1, col2, ...), possibly wrapped in CAST.
    let lhs: &dyn datafusion_physical_expr::PhysicalExpr = bin_expr.left().as_ref();
    let struct_fn = if let Some(sf) = lhs
        .as_any()
        .downcast_ref::<datafusion_physical_expr::ScalarFunctionExpr>()
    {
        sf
    } else if let Some(cast_expr) = lhs.as_any().downcast_ref::<phys_expr::CastExpr>() {
        cast_expr
            .expr()
            .as_any()
            .downcast_ref::<datafusion_physical_expr::ScalarFunctionExpr>()?
    } else {
        return None;
    };
    if struct_fn.name() != "struct" {
        return None;
    }

    // RHS must be a struct literal.
    let rhs_literal = bin_expr
        .right()
        .as_any()
        .downcast_ref::<phys_expr::Literal>()?;
    let ScalarValue::Struct(struct_arr) = rhs_literal.value() else {
        return None;
    };

    // Convert each column arg to a Vortex expression and collect native types.
    let arrow_schema: arrow::datatypes::Schema = df_schema.as_arrow().as_ref().clone();
    let mut vortex_columns: Vec<vortex::expr::Expression> =
        Vec::with_capacity(struct_fn.args().len());
    let mut col_types: Vec<arrow::datatypes::DataType> = Vec::with_capacity(struct_fn.args().len());
    for arg in struct_fn.args() {
        let vx = convertor.convert(arg.as_ref()).ok()?;
        let dt = arg.data_type(&arrow_schema).ok()?;
        vortex_columns.push(vx);
        col_types.push(dt);
    }
    if vortex_columns.is_empty() {
        return None;
    }

    // Build col1 = v1 AND col2 = v2 AND ...
    let field_eqs: Vec<vortex::expr::Expression> = (0..vortex_columns.len())
        .map(|i| {
            let field_scalar = ScalarValue::try_from_array(struct_arr.column(i), 0).ok()?;
            let cast_scalar = field_scalar
                .cast_to(&col_types[i])
                .ok()
                .unwrap_or(field_scalar);
            let phys_lit = phys_expr::Literal::new(cast_scalar);
            let vortex_lit = convertor.convert(&phys_lit).ok()?;
            Some(vortex::expr::eq(vortex_columns[i].clone(), vortex_lit))
        })
        .collect::<Option<Vec<_>>>()?;

    let mut conj = field_eqs.into_iter();
    let first = conj.next()?;
    Some(conj.fold(first, vortex::expr::and))
}
/// Combine expressions with OR using a balanced binary tree.
/// Depth is O(log N) instead of O(N) from a linear fold, avoiding stack overflow.
fn balanced_or(mut exprs: Vec<vortex::expr::Expression>) -> vortex::expr::Expression {
    use vortex::expr::or;
    debug_assert!(!exprs.is_empty());
    while exprs.len() > 1 {
        let mut next = Vec::with_capacity(exprs.len().div_ceil(2));
        let mut i = 0;
        while i + 1 < exprs.len() {
            // Take pairs — using swap_remove-style but preserving order
            let right = exprs[i + 1].clone();
            let left = exprs[i].clone();
            next.push(or(left, right));
            i += 2;
        }
        if i < exprs.len() {
            next.push(exprs[i].clone());
        }
        exprs = next;
    }
    match exprs.into_iter().next() {
        Some(expr) => expr,
        None => unreachable!("balanced_or called with empty exprs"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    use std::sync::Arc;
    use vortex_datafusion::DefaultExpressionConvertor;

    /// Build a `struct(col1, col2, ...)` [`ScalarFunctionExpr`] over the given columns.
    fn make_struct_fn(
        col_names: &[&str],
        schema: &Schema,
    ) -> Arc<dyn datafusion_physical_expr::PhysicalExpr> {
        let args: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> = col_names
            .iter()
            .map(|name| {
                Arc::new(Column::new_with_schema(name, schema).expect("column exists in schema"))
                    as Arc<dyn datafusion_physical_expr::PhysicalExpr>
            })
            .collect();

        let struct_fields: Vec<Field> = col_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                Field::new(
                    format!("c{i}"),
                    schema
                        .field_with_name(name)
                        .expect("field exists in schema")
                        .data_type()
                        .clone(),
                    true,
                )
            })
            .collect();
        let return_field = Arc::new(Field::new(
            "struct",
            DataType::Struct(struct_fields.into()),
            false,
        ));

        let struct_udf: Arc<datafusion_expr::ScalarUDF> =
            Arc::new(datafusion_functions::core::r#struct::StructFunc::new().into());
        Arc::new(ScalarFunctionExpr::new(
            "struct",
            struct_udf,
            args,
            return_field,
            Arc::default(),
        ))
    }

    /// Build a [`ScalarValue::Struct`] literal from a slice of scalar values.
    fn make_struct_literal(values: &[ScalarValue]) -> ScalarValue {
        let (fields, arrays): (Vec<_>, Vec<_>) = values
            .iter()
            .enumerate()
            .map(|(i, sv)| {
                let arr = sv.to_array().expect("scalar to array conversion");
                let field = Arc::new(Field::new(format!("c{i}"), arr.data_type().clone(), true));
                (field, arr)
            })
            .unzip();

        let struct_arr = arrow::array::StructArray::new(fields.into(), arrays, None);
        ScalarValue::Struct(Arc::new(struct_arr))
    }

    fn two_col_schema() -> (Schema, DFSchema) {
        let schema = Schema::new(vec![
            Field::new("k1", DataType::Int32, false),
            Field::new("k2", DataType::Utf8, false),
        ]);
        let df_schema = DFSchema::try_from(schema.clone()).expect("schema conversion");
        (schema, df_schema)
    }

    // ── try_decompose_struct_eq ──────────────────────────────────────────

    #[test]
    fn decompose_struct_eq_two_columns() {
        let (schema, df_schema) = two_col_schema();
        let convertor = DefaultExpressionConvertor::default();

        let expr = Arc::new(BinaryExpr::new(
            make_struct_fn(&["k1", "k2"], &schema),
            datafusion_expr::Operator::Eq,
            Arc::new(Literal::new(make_struct_literal(&[
                ScalarValue::Int32(Some(42)),
                ScalarValue::Utf8(Some("hello".into())),
            ]))),
        ));

        let result = try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema);
        let expected = vortex::expr::and(
            vortex::expr::eq(
                vortex::expr::col("k1"),
                vortex::expr::lit(vortex::scalar::Scalar::from(42_i32)),
            ),
            vortex::expr::eq(
                vortex::expr::col("k2"),
                vortex::expr::lit(vortex::scalar::Scalar::from("hello")),
            ),
        );
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn decompose_struct_eq_single_column() {
        let schema = Schema::new(vec![Field::new("k1", DataType::Int64, false)]);
        let df_schema = DFSchema::try_from(schema.clone()).expect("schema conversion");
        let convertor = DefaultExpressionConvertor::default();

        let expr = Arc::new(BinaryExpr::new(
            make_struct_fn(&["k1"], &schema),
            datafusion_expr::Operator::Eq,
            Arc::new(Literal::new(make_struct_literal(&[ScalarValue::Int64(
                Some(99),
            )]))),
        ));

        let result = try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema);
        let expected = vortex::expr::eq(
            vortex::expr::col("k1"),
            vortex::expr::lit(vortex::scalar::Scalar::from(99_i64)),
        );
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn decompose_struct_eq_three_columns() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]);
        let df_schema = DFSchema::try_from(schema.clone()).expect("schema conversion");
        let convertor = DefaultExpressionConvertor::default();

        let expr = Arc::new(BinaryExpr::new(
            make_struct_fn(&["a", "b", "c"], &schema),
            datafusion_expr::Operator::Eq,
            Arc::new(Literal::new(make_struct_literal(&[
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Utf8(Some("three".into())),
            ]))),
        ));

        let result = try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema);
        // fold produces: and(and(a=1, b=2), c='three')
        let expected = vortex::expr::and(
            vortex::expr::and(
                vortex::expr::eq(
                    vortex::expr::col("a"),
                    vortex::expr::lit(vortex::scalar::Scalar::from(1_i32)),
                ),
                vortex::expr::eq(
                    vortex::expr::col("b"),
                    vortex::expr::lit(vortex::scalar::Scalar::from(2_i64)),
                ),
            ),
            vortex::expr::eq(
                vortex::expr::col("c"),
                vortex::expr::lit(vortex::scalar::Scalar::from("three")),
            ),
        );
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn decompose_struct_eq_with_cast_wrapping() {
        let (schema, df_schema) = two_col_schema();
        let convertor = DefaultExpressionConvertor::default();

        // CAST(struct(k1, k2) AS Struct<c0:Int32, c1:Utf8>) = {c0:42, c1:"hello"}
        let cast_expr = Arc::new(phys_expr::CastExpr::new(
            make_struct_fn(&["k1", "k2"], &schema),
            DataType::Struct(
                vec![
                    Field::new("c0", DataType::Int32, true),
                    Field::new("c1", DataType::Utf8, true),
                ]
                .into(),
            ),
            None,
        ));
        let expr = Arc::new(BinaryExpr::new(
            cast_expr,
            datafusion_expr::Operator::Eq,
            Arc::new(Literal::new(make_struct_literal(&[
                ScalarValue::Int32(Some(42)),
                ScalarValue::Utf8(Some("hello".into())),
            ]))),
        ));

        let result = try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema);
        let expected = vortex::expr::and(
            vortex::expr::eq(
                vortex::expr::col("k1"),
                vortex::expr::lit(vortex::scalar::Scalar::from(42_i32)),
            ),
            vortex::expr::eq(
                vortex::expr::col("k2"),
                vortex::expr::lit(vortex::scalar::Scalar::from("hello")),
            ),
        );
        assert_eq!(result, Some(expected));
    }

    // ── Negative / rejection cases ──────────────────────────────────────

    #[test]
    fn decompose_struct_eq_rejects_not_eq_operator() {
        let (schema, df_schema) = two_col_schema();
        let convertor = DefaultExpressionConvertor::default();

        let expr = Arc::new(BinaryExpr::new(
            make_struct_fn(&["k1", "k2"], &schema),
            datafusion_expr::Operator::NotEq,
            Arc::new(Literal::new(make_struct_literal(&[
                ScalarValue::Int32(Some(1)),
                ScalarValue::Utf8(Some("x".into())),
            ]))),
        ));
        assert_eq!(
            try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema),
            None,
        );
    }

    #[test]
    fn decompose_struct_eq_rejects_non_struct_lhs() {
        let (_, df_schema) = two_col_schema();
        let convertor = DefaultExpressionConvertor::default();

        // k1 = 42  (plain column, not struct(...))
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("k1", 0)),
            datafusion_expr::Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
        ));
        assert_eq!(
            try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema),
            None,
        );
    }

    #[test]
    fn decompose_struct_eq_rejects_non_struct_rhs() {
        let (schema, df_schema) = two_col_schema();
        let convertor = DefaultExpressionConvertor::default();

        // struct(k1, k2) = 42  (RHS is scalar, not struct literal)
        let expr = Arc::new(BinaryExpr::new(
            make_struct_fn(&["k1", "k2"], &schema),
            datafusion_expr::Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
        ));
        assert_eq!(
            try_decompose_struct_eq(expr.as_ref(), &convertor, &df_schema),
            None,
        );
    }

    // ── build_vortex_filter integration ─────────────────────────────────

    #[test]
    fn build_filter_empty_returns_none() {
        let schema = Schema::new(vec![Field::new("k1", DataType::Int32, false)]);
        let df_schema = DFSchema::try_from(schema).expect("schema conversion");
        assert_eq!(
            build_vortex_filter(&[], &df_schema).expect("build filter"),
            None
        );
    }

    #[test]
    fn build_filter_simple_eq() {
        let schema = Schema::new(vec![Field::new("k1", DataType::Int32, false)]);
        let df_schema = DFSchema::try_from(schema).expect("schema conversion");

        let filters =
            vec![datafusion_expr::col("k1").eq(datafusion_expr::lit(ScalarValue::Int32(Some(7))))];
        let result = build_vortex_filter(&filters, &df_schema).expect("build filter");
        assert!(result.is_some());
    }

    #[test]
    fn build_filter_multiple_filters_anded() {
        let schema = Schema::new(vec![
            Field::new("k1", DataType::Int32, false),
            Field::new("k2", DataType::Utf8, false),
        ]);
        let df_schema = DFSchema::try_from(schema).expect("schema conversion");

        let filters = vec![
            datafusion_expr::col("k1").eq(datafusion_expr::lit(ScalarValue::Int32(Some(1)))),
            datafusion_expr::col("k2")
                .eq(datafusion_expr::lit(ScalarValue::Utf8(Some("a".into())))),
        ];
        let result = build_vortex_filter(&filters, &df_schema).expect("build filter");
        let expected = vortex::expr::and(
            vortex::expr::eq(
                vortex::expr::col("k1"),
                vortex::expr::lit(vortex::scalar::Scalar::from(1_i32)),
            ),
            vortex::expr::eq(
                vortex::expr::col("k2"),
                vortex::expr::lit(vortex::scalar::Scalar::from("a")),
            ),
        );
        assert_eq!(result.expect("filter should be Some"), expected);
    }
}
