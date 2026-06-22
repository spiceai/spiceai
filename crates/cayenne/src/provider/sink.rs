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
/// default 256 MB).
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
            // Overwrite path: `CayenneTableProvider::begin_overwrite` acquires the
            // table write lock internally and the lock is held inside the
            // returned `PreparedOverwrite` until `finish`/`rollback`. Acquiring
            // it again here would deadlock.
            self.write_all_overwrite(normalized, context)
                .await
                .map_err(Into::into)
        } else {
            // Append path: `write_all_append` uses the existing-staging helpers
            // that assume the caller already holds the write lock.
            let _write_guard = self.table.write_lock().lock().await;
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
    /// or stages a new snapshot when deletion isolation is needed.
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

    /// Handles overwrite-mode writes by staging the new snapshot via
    /// [`CayenneTableProvider::begin_overwrite`] and then committing it
    /// through a dedicated single-partition transaction.
    ///
    /// 1. [`begin_overwrite`] writes the input stream to a fresh
    ///    `<table_id>/<new_snapshot>/` directory and acquires the table write
    ///    lock. `target_partitions` is sourced from the session config so the
    ///    underlying parallel-writer fan-out matches the rest of the query.
    /// 2. [`PreparedOverwrite::apply_owned_txn`] flips the catalog pointer
    ///    via the trait-based `commit_compaction` (own transaction, with
    ///    retry-on-conflict), preserving exact pre-issue-#10125 retry
    ///    semantics for non-coordinated writes.
    /// 3. [`PreparedOverwrite::finish`] updates the in-memory snapshot id,
    ///    listing table, deletion caches, inlined-data caches, and triggers
    ///    old-snapshot GC.
    ///
    /// Cross-partition coordinators (issue #10125 step 4b) take the same
    /// `PreparedOverwrite` handle and call
    /// [`PreparedOverwrite::apply_in_txn`] inside one shared transaction
    /// so every participating partition's pointer flip is atomic.
    async fn write_all_overwrite(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> super::Result<u64> {
        let target_partitions = context.session_config().target_partitions();
        let prepared = self.table.begin_overwrite(data, target_partitions).await?;
        prepared
            .apply_owned_txn()
            .await
            .map_err(super::Error::from)?;
        prepared.finish().await
    }
}
