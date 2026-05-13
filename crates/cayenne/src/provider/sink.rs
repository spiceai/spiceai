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

//! A [`DataSink`] implementation that writes data to a Cayenne table.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::sink::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::Result as DFResult;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use futures::StreamExt;

use super::constants::STAGING_DIR_NAME;
use super::context::CayenneContext;
use super::table::CayenneTableProvider;

/// A [`DataSink`] implementation that writes data to a Cayenne table.
///
/// Supports two write modes via [`InsertOp`]:
/// - **Append**: Adds data to the current (or a new) snapshot, with PK on-conflict
///   handling, retention filters, and optional sorting.
/// - **Overwrite**: Replaces all data by writing to a fresh snapshot and atomically
///   updating the catalog.
///
/// # File Sizing
///
/// File sizing is delegated to the downstream Vortex/DataFusion writer using
/// the configured target file size (`VortexConfig.target_vortex_file_size_mb`,
/// default 128 MB).
///
/// # Performance
///
/// - **Streaming**: Forwards the input stream directly to the writer
/// - **Writer-managed buffering**: Avoids duplicate chunking heuristics in this sink
/// - **Zero-copy**: Reuses `RecordBatch` Arc references, no data copying
///
/// # Concurrency
///
/// A per-table write lock (acquired in [`DataSink::write_all`]) serializes all write
/// operations. Multiple concurrent `insert_into()` calls on the same table will block,
/// ensuring only one write runs at a time.
pub struct CayenneDataSink {
    /// The Cayenne table provider to write to.
    table: CayenneTableProvider,

    /// The insert operation mode (Append, Overwrite).
    overwrite: InsertOp,

    /// Schema of the data being written.
    schema: SchemaRef,

    /// Shared context containing configuration (file size, concurrency, sort columns, etc.)
    /// and cached resources (upload semaphore, Vortex format).
    context: Arc<CayenneContext>,
}

impl CayenneDataSink {
    /// Creates a new `CayenneDataSink`.
    ///
    /// # Arguments
    ///
    /// * `table` - The Cayenne table provider to write to
    /// * `overwrite` - The insert operation mode
    /// * `schema` - Schema of the data being written
    /// * `context` - Shared context with configuration and resources
    #[must_use]
    pub fn new(
        table: CayenneTableProvider,
        overwrite: InsertOp,
        schema: SchemaRef,
        context: Arc<CayenneContext>,
    ) -> Self {
        Self {
            table,
            overwrite,
            schema,
            context,
        }
    }
}

impl fmt::Debug for CayenneDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDataSink")
            .field("table", &self.table.table_name())
            .field("overwrite", &self.overwrite)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayenneDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "CayenneDataSink(table={}, mode={:?})",
                    self.table.table_name(),
                    self.overwrite
                )
            }
        }
    }
}

#[async_trait]
impl DataSink for CayenneDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        // Acquire write lock to serialize all writes (append and overwrite) and
        // prevent concurrent races on catalog state, snapshot IDs, and listing table.
        let _write_guard = self.table.write_lock().lock().await;

        // Normalize incoming batches to the table schema (e.g. CDC nullability mismatches)
        // causing Vortex assertion failures.
        let target_schema = Arc::clone(&self.schema);
        let normalized = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&target_schema),
            data.map(move |batch_result| {
                batch_result.and_then(|batch| {
                    arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&target_schema))
                        .map_err(Into::into)
                })
            }),
        ));

        if self.overwrite == InsertOp::Overwrite {
            self.write_all_overwrite(normalized)
                .await
                .map_err(Into::into)
        } else {
            self.write_all_append(normalized, context)
                .await
                .map_err(Into::into)
        }
    }
}

impl CayenneDataSink {
    /// Append data from a record batch stream into the Cayenne table.
    ///
    /// Writes data to the current snapshot (via [`CayenneTableProvider::chunk_and_write_parallel`])
    /// or a new snapshot (via [`CayenneTableProvider::insert_to_new_snapshot_with_sequence`])
    /// when deletion isolation is needed.
    ///
    /// # Write Pipeline
    ///
    /// 1. Checks for pending PK-based deletions
    /// 2. Validates primary key on-conflict constraints via `prepare_stream_for_insert`
    /// 3. Writes data to a new snapshot (if PK-based deletions pending or on-conflict
    ///    deletions exist) or the current snapshot
    /// 4. Applies on-conflict deletion vectors
    /// 5. Applies retention filters
    /// 6. Sorts and rewrites data if configured
    /// 7. Refreshes the listing table
    ///
    /// # Errors
    ///
    /// Returns an error if the data cannot be inserted.
    async fn write_all_append(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> super::Result<u64> {
        // Ensure no incomplete write from a previous crash before proceeding.
        // A leftover staging WAL indicates an interrupted file-move operation,
        // meaning the table may contain partial data. Block all writes until resolved.
        self.table.ensure_no_incomplete_write().await?;

        // Check for pending PK-based deletions (from explicit DELETE operations).
        //
        // POSITION-BASED STRATEGY: No compaction/new snapshot needed on insert.
        // Deletion vectors are tracked per-file (HashMap<file_path, RoaringBitmap>), so each
        // file's deletion bitmap is independent. Adding new files doesn't affect existing
        // deletion vectors — the new files simply have no entries in the deletion cache.
        //
        // PK-BASED STRATEGIES (Int64Pk, RowConverterBased): Write to a new snapshot with a
        // higher sequence number, ensuring proper Iceberg-style ordering:
        // - Deletions apply to snapshots with sequence <= delete_sequence
        // - New data in snapshots with sequence > delete_sequence is visible
        // This avoids compaction by using "anti-deletions" — the new snapshot isolates
        // new data from existing deletion vectors.
        let needs_new_snapshot_for_pending_deletions =
            !self.table.pk_deletion_strategy().is_position_based()
                && self.table.has_pending_deletions();

        if needs_new_snapshot_for_pending_deletions {
            tracing::debug!(
                "Table {} has pending PK-based deletions, will write to new snapshot",
                self.table.table_name()
            );
        }

        // Validate primary key on-conflict constraints and prepare the stream.
        // For tables without PKs, this is a pass-through with empty deletion specs.
        //
        // NOTE: Even when pending PK-based deletions exist, we still need to run
        // validate_on_conflict() (via prepare_stream_for_insert) on the incoming stream
        // to handle upserts for PKs that already exist in the table. Without this,
        // duplicate PKs would appear in query results.
        let (
            mut prepared_stream,
            delete_specs,
            deleted_pk_i64,
            deleted_row_keys,
            deleted_inlined_pk_i64,
            deleted_inlined_row_keys,
        ) = self.table.prepare_stream_for_insert(data).await?;

        let has_file_on_conflict_deletions = !delete_specs.is_empty();
        let has_inlined_on_conflict_deletions =
            !deleted_inlined_pk_i64.is_empty() || !deleted_inlined_row_keys.is_empty();
        let has_on_conflict_deletions =
            has_file_on_conflict_deletions || has_inlined_on_conflict_deletions;

        tracing::debug!(
            "write_all_append: delete_specs={} files, deleted_pk_i64={} keys, \
             pending_deletions={}, on_conflict_deletions={}",
            delete_specs.len(),
            deleted_pk_i64.len() + deleted_inlined_pk_i64.len(),
            needs_new_snapshot_for_pending_deletions,
            has_on_conflict_deletions
        );

        // Determine write target: new snapshot (for deletion isolation) or current snapshot.
        //
        // New snapshot is required when:
        // - Pending PK-based deletions exist (from explicit DELETE operations) — ensures
        //   Iceberg-style sequence ordering so deletions don't affect new data.
        // - On-conflict deletions exist (from INSERT upserts) — the deletion vectors
        //   target rows in the current snapshot
        let needs_new_snapshot =
            needs_new_snapshot_for_pending_deletions || has_file_on_conflict_deletions;

        // Always clear staging dir first to self-heal from previous crashes,
        // even if we end up using the inline fast-path.
        self.table.clear_staging_dir().await?;
        // ── Data inlining fast-path ────────────────────────────────────
        // For small data, store directly in the metastore as Arrow IPC to avoid
        // Vortex file overhead. PK tables can use this path when conflicts are
        // inline-only and there are no pending file-backed PK deletions: those
        // paths still need a protected snapshot so old rows stay hidden while
        // replacement rows are visible.
        let has_sort_columns = self.context.has_sort_columns();
        let is_partitioned = self.table.metadata().partition_column.is_some();
        let can_inline = !needs_new_snapshot_for_pending_deletions
            && !has_file_on_conflict_deletions
            && !has_sort_columns
            && !is_partitioned
            && !self.table.has_retention_filters();
        if can_inline {
            // Collect the stream incrementally. Stop as soon as the inline row
            // threshold or an in-memory byte budget is exceeded so we don't
            // buffer the entire large insert. The byte budget guards against
            // pathological batches (e.g. a few rows with very large strings)
            // where row count alone does not bound memory usage.
            let schema = prepared_stream.schema();
            let mut batches = Vec::new();
            let mut total_rows = 0usize;
            let mut total_bytes = 0usize;
            let mut exceeded = false;
            while let Some(batch) = futures::StreamExt::next(&mut prepared_stream).await {
                let batch = batch?;
                total_rows += batch.num_rows();
                total_bytes = total_bytes.saturating_add(batch.get_array_memory_size());
                batches.push(batch);
                if total_rows > super::table::INLINE_MAX_ROWS
                    || total_bytes > super::table::INLINE_MAX_BUFFER_BYTES
                {
                    exceeded = true;
                    break;
                }
            }

            if !exceeded
                && total_rows > 0
                && self
                    .table
                    .try_inline_batches_with_inlined_deletions(
                        &batches,
                        &deleted_inlined_pk_i64,
                        &deleted_inlined_row_keys,
                    )
                    .await?
            {
                // Persist stats from the inlined batches
                let stats_acc = super::table::ColumnStatsAccumulator::new(&schema);
                for batch in &batches {
                    stats_acc.update(batch);
                }

                self.table.persist_table_stats(&stats_acc).await;

                // Auto-checkpoint when accumulated inline data reaches 10K rows
                let inlined_count = self.table.cached_inlined_row_count();
                if inlined_count >= 10_000
                    && let Err(e) = self.table.checkpoint_inlined_data().await
                {
                    tracing::warn!(
                        "Auto-checkpoint of inlined data failed for {}: {e}",
                        self.table.table_name(),
                    );
                }

                return Ok(u64::try_from(total_rows).unwrap_or(u64::MAX));
            }
            // Fell through — batch was too large after IPC serialization.

            // Exceeded threshold or IPC too large. Chain the already-read batches
            // with the remaining stream so nothing is lost or fully buffered.
            let remaining_stream = prepared_stream;
            let buffered_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                &[batches],
                Arc::clone(&schema),
                None,
            )?;
            let buffered_stream =
                datafusion_physical_plan::execute_stream(buffered_exec, Arc::clone(context))?;

            // Chain: yield buffered batches first, then the remaining un-read stream
            let chained_stream =
                Box::pin(futures::StreamExt::chain(buffered_stream, remaining_stream));
            let re_stream: datafusion_physical_plan::SendableRecordBatchStream = Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    chained_stream,
                ),
            );

            // Continue with the normal write path (staging dir already cleared above)
            let target_size_bytes = self.context.target_file_size_bytes();

            let (rows, _writer_ops, stats_acc) = match self
                .table
                .write_to_snapshot(re_stream, target_size_bytes, STAGING_DIR_NAME)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if let Err(cleanup_err) = self.table.clear_staging_dir().await {
                        tracing::warn!(
                            "Failed to clean staging dir after write error for table {}: {cleanup_err}",
                            self.table.table_name(),
                        );
                    }
                    return Err(e);
                }
            };

            let staged_append = self.table.staged_append_for_existing_staging();
            staged_append.finalize_staged_write().await?;

            self.table
                .apply_on_conflict_deletions(
                    delete_specs,
                    deleted_pk_i64,
                    deleted_row_keys,
                    deleted_inlined_pk_i64,
                    deleted_inlined_row_keys,
                )
                .await?;

            let retention_deleted_rows = self.apply_retention_if_configured().await?;
            let sorted = self.sort_if_configured().await?;
            if should_refresh_listing_table_after_post_write(retention_deleted_rows, sorted) {
                self.table.refresh_listing_table().await?;
            }
            self.table.persist_table_stats(&stats_acc).await;

            return Ok(rows);
        }

        let (total_rows, write_stats_acc) = if needs_new_snapshot {
            // Apply on-conflict deletion vectors BEFORE creating the protected snapshot.
            self.table
                .apply_on_conflict_deletions(
                    delete_specs,
                    deleted_pk_i64,
                    deleted_row_keys,
                    deleted_inlined_pk_i64,
                    deleted_inlined_row_keys,
                )
                .await?;

            // Write to a NEW snapshot with a higher sequence number so that:
            // - Old data filtered by deletions (delete_seq >= old_snapshot_seq)
            // - New data visible (new_snapshot_seq > delete_seq)
            let new_sequence = self
                .table
                .catalog()
                .increment_sequence_number(self.table.table_id())
                .await?;

            self.table
                .insert_to_new_snapshot_with_sequence(prepared_stream, new_sequence)
                .await?
        } else {
            // Write chunks to a staging directory, then move to the current snapshot.
            // This prevents partial files from appearing in the active snapshot on
            // stream errors, which would advance the watermark past lost data.
            let target_size_bytes = self.context.target_file_size_bytes();

            // Staging dir already cleared at the top of write_all_append.
            // Write to _staging/ directory
            let (rows, writer_ops, stats_acc) = match self
                .table
                .write_to_snapshot(prepared_stream, target_size_bytes, STAGING_DIR_NAME)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    // Best-effort cleanup — next append's clear_staging_dir() handles leftovers
                    if let Err(cleanup_err) = self.table.clear_staging_dir().await {
                        tracing::warn!(
                            "Failed to clean staging dir after write error for table {}: {cleanup_err}",
                            self.table.table_name(),
                        );
                    }
                    return Err(e);
                }
            };

            // Step 3: Write staging WAL (records intent before the non-atomic move)
            // Step 4-6: Execute WAL finalize sequence: write WAL, move files,
            // remove WAL, and refresh listing table.
            let staged_append = self.table.staged_append_for_existing_staging();
            staged_append.finalize_staged_write().await?;

            tracing::debug!(
                "Insert completed, wrote {} rows to Vortex in {} writer operation(s)",
                rows,
                writer_ops
            );

            // Apply deletion vectors generated by on-conflict handling (no-op if empty).
            self.table
                .apply_on_conflict_deletions(
                    delete_specs,
                    deleted_pk_i64,
                    deleted_row_keys,
                    deleted_inlined_pk_i64,
                    deleted_inlined_row_keys,
                )
                .await?;

            (rows, stats_acc)
        };

        // Listing table refresh is already part of the staged WAL finalize flow.
        // For snapshot-creation paths, we still need this explicit refresh.
        if needs_new_snapshot {
            self.table.refresh_listing_table().await?;
        }

        let retention_deleted_rows = self.apply_retention_if_configured().await?;

        // Sort operates on the listing table data (the complete corpus after retention),
        // ensuring optimal zone maps with non-overlapping min/max ranges.
        // Uses DataFusion's SortExec with automatic disk spilling for large datasets,
        // streaming external merge sort, and SIMD-optimized kernels.
        let sorted = self.sort_if_configured().await?;

        // Staged appends refresh the listing table during WAL finalization, and
        // new-snapshot writes refresh it immediately above. Refresh again only
        // when a post-write operation can change file visibility/statistics.
        if should_refresh_listing_table_after_post_write(retention_deleted_rows, sorted) {
            self.table.refresh_listing_table().await?;
        }

        // Persist table-level column statistics to the metastore (best-effort).
        self.table.persist_table_stats(&write_stats_acc).await;

        // Write lock is released when `_write_guard` drops (in write_all).

        Ok(total_rows)
    }

    /// Apply retention filters if configured on the table.
    async fn apply_retention_if_configured(&self) -> super::Result<u64> {
        if !self.table.has_retention_filters() {
            return Ok(0);
        }

        let deleted = self.table.apply_retention_filters().await?;
        if deleted > 0 {
            tracing::info!(
                "Retention filters deleted {} row(s) for table {}",
                deleted,
                self.table.table_name()
            );
        } else {
            tracing::debug!(
                "Retention filters found no rows to delete for table {}",
                self.table.table_name()
            );
        }
        Ok(deleted)
    }

    /// Sort and rewrite data if `sort_columns` is configured.
    async fn sort_if_configured(&self) -> super::Result<bool> {
        if !self.context.has_sort_columns() {
            return Ok(false);
        }

        let target_size_bytes = self.context.target_file_size_bytes();
        self.table.sort_and_rewrite_data(target_size_bytes).await?;
        Ok(true)
    }

    /// Handles overwrite mode writes by creating a new snapshot:
    /// 1. Generates a new `UUIDv7` snapshot ID
    /// 2. Writes data to the new snapshot directory with memory bounds
    /// 3. Syncs the directory for durability (local paths only)
    /// 4. Atomically updates the catalog to point to the new snapshot
    /// 5. Updates in-memory state (snapshot ID, listing table, deletion caches)
    /// 6. Triggers cleanup of old snapshots
    async fn write_all_overwrite(&self, data: SendableRecordBatchStream) -> super::Result<u64> {
        // Generate a new UUIDv7 for the snapshot
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();

        let is_s3 = self.table.table_path().starts_with("s3://");

        // For local paths, ensure the snapshot directory exists
        // S3 doesn't require directory creation (object storage creates paths on write)
        if !is_s3 {
            let snapshot_dir = self.table.snapshot_dir_path_for(&new_snapshot_id);
            CayenneTableProvider::ensure_snapshot_dir_exists(&snapshot_dir).await?;
        }

        // Write data to the new snapshot.
        let target_size = self.context.target_file_size_bytes();
        let (total_rows, _files_written, write_stats_acc) = self
            .table
            .write_to_snapshot(data, target_size, &new_snapshot_id)
            .await?;

        // Sync the snapshot directory to ensure all data is durably written.
        // This is critical for ACID durability - we must ensure data files are
        // on disk before updating the catalog metadata.
        if !is_s3 {
            let snapshot_dir = self.table.snapshot_dir_path_for(&new_snapshot_id);
            CayenneTableProvider::sync_snapshot_dir(&snapshot_dir).await?;
        }

        // Atomically update the catalog snapshot and clear any delete files.
        // For overwrite operations, any existing delete files are stale since
        // we're replacing all data. Using commit_compaction ensures atomicity.
        self.table.commit_overwrite(&new_snapshot_id).await?;

        // Update the in-memory snapshot ID to match the new catalog state
        self.table.update_current_snapshot_id(&new_snapshot_id)?;

        // Clear any in-memory deletion caches since all data was replaced
        if let Err(e) = self.table.clear_all_deletion_caches() {
            tracing::warn!(
                "Failed to clear deletion caches after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }

        // Update the provider's listing table to point to the new snapshot
        // This ensures subsequent queries in the same context will read from the new data
        self.table
            .update_listing_table_for_snapshot(&new_snapshot_id)
            .await?;

        // Trigger cleanup of old snapshot directories after successful full refresh
        self.table
            .trigger_old_snapshot_cleanup(&new_snapshot_id)
            .await;

        // Clear stale inlined data and file stats since all data was replaced.
        if let Err(e) = self
            .table
            .catalog()
            .clear_inlined_data(self.table.table_id())
            .await
        {
            tracing::warn!(
                "Failed to clear inlined data after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }
        if let Err(e) = self
            .table
            .catalog()
            .clear_inlined_deletes(self.table.table_id())
            .await
        {
            tracing::warn!(
                "Failed to clear inlined deletes after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }
        // Clear the prior statistics row before upserting so a zero-row
        // overwrite leaves no stats at all (rather than stale stats that
        // describe rows the overwrite just deleted). `persist_table_stats`
        // is a no-op when the accumulator is empty, so the clear is what
        // actually removes the stale row in that case.
        if let Err(e) = self
            .table
            .catalog()
            .clear_table_statistics(self.table.table_id())
            .await
        {
            tracing::warn!(
                "Failed to clear table statistics after overwrite for table {}: {e}",
                self.table.table_name()
            );
        }
        self.table.persist_table_stats(&write_stats_acc).await;

        Ok(total_rows)
    }
}

fn should_refresh_listing_table_after_post_write(
    retention_deleted_rows: u64,
    sorted: bool,
) -> bool {
    retention_deleted_rows > 0 || sorted
}

#[cfg(test)]
mod tests {
    #[test]
    fn refresh_listing_table_only_when_post_write_steps_changed_files() {
        assert!(!super::should_refresh_listing_table_after_post_write(
            0, false
        ));
        assert!(super::should_refresh_listing_table_after_post_write(
            1, false
        ));
        assert!(super::should_refresh_listing_table_after_post_write(
            0, true
        ));
        assert!(super::should_refresh_listing_table_after_post_write(
            1, true
        ));
    }
}
