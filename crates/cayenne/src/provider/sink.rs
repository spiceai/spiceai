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
        if self.overwrite == InsertOp::Overwrite {
            self.write_all_overwrite(data).await
        } else {
            self.write_all_append(data).await
        }
    }
}

impl CayenneDataSink {
    /// Handles append mode writes by delegating to `CayenneTableProvider::insert()`.
    async fn write_all_append(&self, data: SendableRecordBatchStream) -> DFResult<u64> {
        self.table
            .insert(data)
            .await
            .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))
    }

    /// Handles overwrite mode writes by creating a new snapshot:
    /// 1. Generates a new `UUIDv7` snapshot ID
    /// 2. Writes data to the new snapshot directory with memory bounds
    /// 3. Syncs the directory for durability (local paths only)
    /// 4. Atomically updates the catalog to point to the new snapshot
    /// 5. Updates in-memory state (snapshot ID, listing table, deletion caches)
    /// 6. Triggers cleanup of old snapshots
    async fn write_all_overwrite(&self, data: SendableRecordBatchStream) -> DFResult<u64> {
        // Generate a new UUIDv7 for the snapshot
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();

        let is_s3 = self.table.table_path().starts_with("s3://");

        // For local paths, ensure the snapshot directory exists
        // S3 doesn't require directory creation (object storage creates paths on write)
        if !is_s3 {
            let snapshot_dir = self.table.snapshot_dir_path_for(&new_snapshot_id);
            CayenneTableProvider::ensure_snapshot_dir_exists(&snapshot_dir)
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to create snapshot directory: {e}"
                    ))
                })?;
        }

        // Write data to the new snapshot with memory-bounded parallel writes
        let target_size = self.context.target_file_size_bytes();
        let (total_rows, _files_written) = self
            .table
            .chunk_and_write_parallel_to_snapshot(data, target_size, &new_snapshot_id)
            .await
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to write to snapshot: {e}"
                ))
            })?;

        // Sync the snapshot directory to ensure all data is durably written.
        // This is critical for ACID durability - we must ensure data files are
        // on disk before updating the catalog metadata.
        if !is_s3 {
            let snapshot_dir = self.table.snapshot_dir_path_for(&new_snapshot_id);
            CayenneTableProvider::sync_snapshot_dir(&snapshot_dir)
                .await
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "Failed to sync snapshot directory for durability: {e}"
                    ))
                })?;
        }

        // Atomically update the catalog snapshot and clear any delete files.
        // For overwrite operations, any existing delete files are stale since
        // we're replacing all data. Using commit_compaction ensures atomicity.
        self.table
            .commit_overwrite(&new_snapshot_id)
            .await
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to commit overwrite: {e}"
                ))
            })?;

        // Update the in-memory snapshot ID to match the new catalog state
        self.table
            .update_current_snapshot_id(&new_snapshot_id)
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to update current snapshot ID: {e}"
                ))
            })?;

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
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to update listing table: {e}"
                ))
            })?;

        // Trigger cleanup of old snapshot directories after successful full refresh
        self.table
            .trigger_old_snapshot_cleanup(&new_snapshot_id)
            .await;

        Ok(total_rows)
    }
}
