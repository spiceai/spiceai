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

use super::context::CayenneContext;
use super::mutation_writer::AppendMutationWriter;
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
        AppendMutationWriter::new(&self.table, &self.context, context)
            .write(data)
            .await
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
            .update_listing_table_for_snapshot(&new_snapshot_id)?;

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
        } else {
            self.table.clear_cached_table_statistics();
        }
        self.table.persist_table_stats(&write_stats_acc).await;

        Ok(total_rows)
    }
}
