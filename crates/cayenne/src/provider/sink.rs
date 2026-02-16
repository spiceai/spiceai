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
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_common::Result as DFResult;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;

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
/// # Size-Based File Chunking
///
/// Data is chunked into separate Vortex files based on a target file size
/// (configurable via `VortexConfig.target_vortex_file_size_mb`, default 128 MB):
/// - Batches are accumulated until the target file size is reached
/// - Each chunk is written as a separate Vortex file in parallel
/// - Each file maintains proper statistics for `DataFusion` pushdown and pruning
///
/// # Performance
///
/// - **Streaming**: Processes chunks as they're formed, avoiding buffering all data
/// - **Parallel writes**: Multiple chunks written concurrently with bounded parallelism
///   (configurable via `VortexConfig.upload_concurrency`, default 4)
/// - **Zero-copy**: Reuses `RecordBatch` Arc references, no data copying
///
/// # Concurrency
///
/// A per-table write lock (acquired in [`DataSink::write_all`]) serializes all write
/// operations. Multiple concurrent `insert_into()` calls on the same table will block,
/// ensuring only one write runs at a time. **Within** a single write, chunks are
/// written in parallel for I/O throughput.
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
        _context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        // Acquire write lock to serialize all writes (append and overwrite) and
        // prevent concurrent races on catalog state, snapshot IDs, and listing table.
        let _write_guard = self.table.write_lock().lock().await;

        if self.overwrite == InsertOp::Overwrite {
            self.write_all_overwrite(data).await.map_err(Into::into)
        } else {
            self.write_all_append(data).await.map_err(Into::into)
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
    async fn write_all_append(&self, data: SendableRecordBatchStream) -> super::Result<u64> {
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
                && self.table.has_pending_deletions()?;

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
        let (prepared_stream, delete_specs, deleted_pk_i64, deleted_row_keys) =
            self.table.prepare_stream_for_insert(data).await?;

        let has_on_conflict_deletions = !delete_specs.is_empty();

        tracing::debug!(
            "write_all_append: delete_specs={} files, deleted_pk_i64={} keys, \
             pending_deletions={}, on_conflict_deletions={}",
            delete_specs.len(),
            deleted_pk_i64.len(),
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
            needs_new_snapshot_for_pending_deletions || has_on_conflict_deletions;

        let total_rows = if needs_new_snapshot {
            // Apply on-conflict deletion vectors BEFORE creating the protected snapshot.
            self.table
                .apply_on_conflict_deletions(delete_specs, deleted_pk_i64, deleted_row_keys)
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
            // Write chunks to the current snapshot.
            let target_size_bytes = self.context.target_file_size_bytes();
            let (rows, chunk_count) = self
                .table
                .chunk_and_write_parallel(prepared_stream, target_size_bytes)
                .await?;

            tracing::debug!(
                "Insert completed, wrote {} rows to Vortex in {} chunk(s)",
                rows,
                chunk_count
            );

            // Apply deletion vectors generated by on-conflict handling (no-op if empty).
            self.table
                .apply_on_conflict_deletions(delete_specs, deleted_pk_i64, deleted_row_keys)
                .await?;

            rows
        };

        // Apply retention filters, sort, and refresh listing table.
        self.apply_retention_if_configured().await?;

        // Sort operates on the listing table data (the complete corpus after retention),
        // ensuring optimal zone maps with non-overlapping min/max ranges.
        // Uses DataFusion's SortExec with automatic disk spilling for large datasets,
        // streaming external merge sort, and SIMD-optimized kernels.
        self.sort_if_configured().await?;

        // Refresh the listing table to pick up new/rewritten files and update statistics,
        // so subsequent query plans see the latest data.
        self.table.refresh_listing_table()?;

        // Write lock is released when `_write_guard` drops (in write_all).

        Ok(total_rows)
    }

    /// Apply retention filters if configured on the table.
    async fn apply_retention_if_configured(&self) -> super::Result<()> {
        if !self.table.has_retention_filters() {
            return Ok(());
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
        Ok(())
    }

    /// Sort and rewrite data if `sort_columns` is configured.
    async fn sort_if_configured(&self) -> super::Result<()> {
        if !self.context.has_sort_columns() {
            return Ok(());
        }

        let target_size_bytes = self.context.target_file_size_bytes();
        self.table.sort_and_rewrite_data(target_size_bytes).await?;
        Ok(())
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

        // Write data to the new snapshot with memory-bounded parallel writes
        let target_size = self.context.target_file_size_bytes();
        let (total_rows, _files_written) = self
            .table
            .chunk_and_write_parallel_to_snapshot(data, target_size, &new_snapshot_id)
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
            .update_listing_table_for_snapshot(&new_snapshot_id)?;

        // Trigger cleanup of old snapshot directories after successful full refresh
        self.table
            .trigger_old_snapshot_cleanup(&new_snapshot_id)
            .await;

        Ok(total_rows)
    }
}

/// Convert a `CatalogError` to a `DataFusionError::External`.
fn to_df_exec_err<E: std::error::Error + Send + Sync + 'static>(
    err: E,
) -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::External(Box::new(err))
}
