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

//! Cross-partition atomic commit coordinator for Cayenne.
//!
//! Implements [`InsertStrategy`] for partitioned Cayenne tables (issue
//! #10125). Both `InsertOp::Overwrite` and `InsertOp::Append`/`Replace` are
//! coordinated — the per-partition `PartitionerExec` fan-out is replaced by
//! a single sink per insert mode that holds every participating partition
//! to one shared commit boundary:
//!
//! - **Overwrite** ([`CayennePartitionedOverwriteSink`]): every participating
//!   partition's catalog mutation is batched into a single
//!   [`MetastoreTransaction`] so the `current_snapshot_id` pointer flips
//!   happen atomically — either every partition advances or none do.
//! - **Append / Replace** ([`CayennePartitionedAppendSink`]): every
//!   participating partition's `listing_fence.write()` is held for one
//!   shared barrier window while files move into the current snapshot dir
//!   and the in-memory `ListingTable` Arcs swap, anchored by a top-level
//!   [`cayenne::PartitionedWal`] for crash recovery (local-FS only —
//!   S3-backed tables skip the top-level WAL and rely on the per-partition
//!   staging WAL for crash safety).
//!
//! ## Overwrite coordination flow
//!
//! 1. **Stage** (parallel-safe per partition): partition the input stream by
//!    partition key. For each unique key seen, spawn a writer task that
//!    streams batches into [`CayenneTableProvider::begin_overwrite`], which
//!    writes data into a fresh `<table_id>/<new_snapshot>/` directory and
//!    returns a [`PreparedOverwrite`] receipt.
//! 2. **Apply** (single shared transaction): open one transaction on the
//!    shared [`CayenneCatalog`]. For every receipt, call
//!    [`PreparedOverwrite::apply_in_txn`] inside that transaction. Commit
//!    once. Retries on transient `SQLITE_BUSY` / Turso `BEGIN CONCURRENT`
//!    conflicts with bounded backoff.
//! 3. **Finish** (parallel-safe per partition): for every receipt, run
//!    [`PreparedOverwrite::finish`] to publish the new snapshot in-memory and
//!    trigger old-snapshot GC.
//!
//! Failure at step 2 rolls back every prepared receipt (best-effort cleanup
//! of the staged snapshot directories). Failure at step 3 is logged but does
//! not roll back — the catalog has already committed, so readers see the new
//! state via the next scan.
//!
//! ## Append coordination flow
//!
//! Mirrors the overwrite flow up to the commit boundary. After every
//! partition's writer task returns a [`PreparedStagedAppend`]:
//!
//! 1. Sort the receipts by `table_id` for deterministic fence-acquisition
//!    order across concurrent coordinators.
//! 2. Acquire every partition's `listing_fence.write()` (held until the
//!    barrier closes).
//! 3. On local FS, write a top-level [`cayenne::PartitionedWal`] anchor at
//!    `<table_root>/_partitioned_wal/<commit_id>.json` before any file move.
//!    On S3, skip the WAL — the per-partition staging WAL still anchors
//!    single-partition recovery.
//! 4. For each receipt, call `apply_under_held_barrier`: move staged files
//!    into the snapshot directory, remove the per-partition WAL, swap the
//!    in-memory `ListingTable`.
//! 5. Remove the top-level WAL.
//! 6. Release fences (drop guards together).
//! 7. Run [`PreparedStagedAppend::finish`] on each receipt.

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, PartitionedWal, PartitionedWalEntry, PreparedOverwrite,
    PreparedStagedAppend,
};
use datafusion::common::{Column, DFSchema};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, dml::InsertOp};
use futures::StreamExt;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::encode_composite_key;
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::insert::{
    InsertStrategy, PartitionContext, partition_batch_composite,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

/// Per-partition key type used by `PartitionTableProvider`. The
/// runtime-table-partition crate keeps the concrete alias `pub(crate)`; for
/// our local map we just use the underlying `String`.
type CompositePartitionKey = String;

/// Cross-partition atomic-commit strategy for Cayenne. See module-level
/// documentation for the overwrite coordination flow.
pub struct CayennePartitionedInsertStrategy {
    catalog: Arc<CayenneCatalog>,
    /// Serializes cross-partition coordinators on this table.
    ///
    /// One coordinator may have many concurrent per-partition writer tasks
    /// in flight (each holding its own partition's write lock via
    /// `begin_overwrite` / `begin_staged_append`), but two coordinators on
    /// the same table acquire per-partition locks in input-stream order —
    /// that order is not guaranteed consistent across coordinators, so
    /// concurrent coordinators on overlapping partition sets could deadlock
    /// under lock-ordering. Serializing the coordinators here eliminates
    /// that hazard. Within one coordinator, every partition's writer task
    /// runs in parallel, so the outer serialization does not bottleneck a
    /// single refresh.
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    /// Absolute filesystem path of the table's data root. The append-mode
    /// coordinator writes its top-level cross-partition WAL under
    /// `<table_root>/_partitioned_wal/` (issue #10125 §6.5). Unused by the
    /// overwrite-mode coordinator.
    table_root: PathBuf,
}

impl std::fmt::Debug for CayennePartitionedInsertStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedInsertStrategy")
            .finish_non_exhaustive()
    }
}

impl CayennePartitionedInsertStrategy {
    /// Construct a new strategy.
    ///
    /// `catalog` must be the same `CayenneCatalog` that the partition
    /// creator was built against. `table_root` is the absolute path of the
    /// partitioned table's data directory; it's used by the append
    /// coordinator to write top-level WAL files at
    /// `<table_root>/_partitioned_wal/`.
    #[must_use]
    pub fn new(catalog: Arc<CayenneCatalog>, table_root: PathBuf) -> Self {
        Self {
            catalog,
            coordinator_lock: Arc::new(tokio::sync::Mutex::new(())),
            table_root,
        }
    }
}

#[async_trait]
impl InsertStrategy for CayennePartitionedInsertStrategy {
    async fn execute_insert(
        &self,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
        context: &PartitionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match insert_op {
            InsertOp::Overwrite => {
                let physical_exprs = create_partition_physical_exprs(
                    &context.partition_by,
                    Arc::clone(&context.schema),
                )?;
                let sink = Arc::new(CayennePartitionedOverwriteSink {
                    catalog: Arc::clone(&self.catalog),
                    coordinator_lock: Arc::clone(&self.coordinator_lock),
                    creator: Arc::clone(&context.creator),
                    partitions: Arc::clone(&context.partitions),
                    schema: Arc::clone(&context.schema),
                    physical_exprs,
                });
                Ok(Arc::new(DataSinkExec::new(input, sink, None)))
            }
            InsertOp::Append | InsertOp::Replace => {
                let physical_exprs = create_partition_physical_exprs(
                    &context.partition_by,
                    Arc::clone(&context.schema),
                )?;
                let sink = Arc::new(CayennePartitionedAppendSink {
                    coordinator_lock: Arc::clone(&self.coordinator_lock),
                    creator: Arc::clone(&context.creator),
                    partitions: Arc::clone(&context.partitions),
                    schema: Arc::clone(&context.schema),
                    physical_exprs,
                    table_root: self.table_root.clone(),
                });
                Ok(Arc::new(DataSinkExec::new(input, sink, None)))
            }
        }
    }
}

/// `DataSink` that fans the input stream out by partition key, stages every
/// partition's overwrite, and commits all of them in one shared
/// `MetastoreTransaction`.
///
/// Streams batches to per-partition writer tasks via `mpsc` channels so
/// steady-state memory is bounded by channel capacity × partition count,
/// not by total input size.
struct CayennePartitionedOverwriteSink {
    catalog: Arc<CayenneCatalog>,
    /// See [`CayennePartitionedInsertStrategy::coordinator_lock`].
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

/// Per-partition writer-task channel depth. Bounds in-flight backpressure on
/// the slowest partition; larger values trade memory for throughput tolerance
/// when partitions write at different speeds. Matches the value used by
/// `accelerated_table::write::write_through.rs` for the same shape.
const PARTITION_WRITER_CHANNEL_DEPTH: usize = 8;

impl std::fmt::Debug for CayennePartitionedOverwriteSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedOverwriteSink")
            .field("partition_exprs", &self.physical_exprs.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayennePartitionedOverwriteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayennePartitionedOverwriteSink")
    }
}

#[async_trait]
impl DataSink for CayennePartitionedOverwriteSink {
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
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        // Serialize cross-partition coordinators on this table so concurrent
        // coordinators on overlapping partition sets can't deadlock on
        // per-partition lock-acquisition order. Within this coordinator,
        // per-partition writer tasks run in parallel.
        let _coordinator_guard = self.coordinator_lock.lock().await;

        // Each per-partition writer fan-outs across `target_partitions` Vortex
        // file writers; the session config drives that count to match the
        // rest of the query (see PR #10822).
        let target_partitions = context.session_config().target_partitions();

        // Step 1: route each input batch to its partition's writer task.
        // On first-seen partition, spawn a `tokio::task` that calls
        // `begin_overwrite` on a `RecordBatchStreamAdapter` around an
        // mpsc::Receiver. Subsequent batches for that partition are pushed
        // through the sender, providing natural backpressure (capacity
        // PARTITION_WRITER_CHANNEL_DEPTH). Steady-state memory is
        // O(channel_depth × partition_count), not O(input_size).
        let mut senders: HashMap<String, mpsc::Sender<datafusion::common::Result<RecordBatch>>> =
            HashMap::new();
        let mut handles: Vec<JoinHandle<cayenne::provider::Result<PreparedOverwrite>>> = Vec::new();
        let mut upstream_err: Option<DataFusionError> = None;

        'outer: while let Some(batch_result) = data.next().await {
            let batch = match batch_result {
                Ok(batch) => batch,
                Err(e) => {
                    upstream_err = Some(e);
                    break 'outer;
                }
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let partitioned = partition_batch_composite(&batch, &self.physical_exprs)?;
            for (key, (values, batch_part)) in partitioned {
                let sender = if let Some(s) = senders.get(&key) {
                    s.clone()
                } else {
                    // First batch for this partition: resolve the per-partition
                    // CayenneTableProvider and spawn its writer task.
                    let provider = self.get_or_create_partition_provider(values).await?;
                    let cayenne = downcast_to_cayenne(&provider).ok_or_else(|| {
                        DataFusionError::Execution(
                            "CayennePartitionedInsertStrategy expects every partition's table \
                             provider to be a CayenneTableProvider"
                                .to_string(),
                        )
                    })?;
                    let cayenne_owned = cayenne.clone_for_write_operations();
                    let (tx, rx) = mpsc::channel::<datafusion::common::Result<RecordBatch>>(
                        PARTITION_WRITER_CHANNEL_DEPTH,
                    );
                    let schema_clone = Arc::clone(&self.schema);
                    let handle = tokio::spawn(async move {
                        let stream: SendableRecordBatchStream = Box::pin(
                            RecordBatchStreamAdapter::new(schema_clone, ReceiverStream::new(rx)),
                        );
                        cayenne_owned.begin_overwrite(stream, target_partitions).await
                    });
                    senders.insert(key.clone(), tx.clone());
                    handles.push(handle);
                    tx
                };

                if sender.send(Ok(batch_part)).await.is_err() {
                    // Writer task terminated unexpectedly (channel closed).
                    // Stop sending; we'll surface the real error when we
                    // join the handle below.
                    upstream_err = Some(DataFusionError::Execution(
                        "partition writer task terminated before stream end".to_string(),
                    ));
                    break 'outer;
                }
            }
        }

        // If upstream errored mid-stream, propagate the error down every
        // open channel so each writer task observes the failure and returns
        // Err (rather than committing a truncated overwrite).
        if let Some(ref err) = upstream_err {
            let err_msg = err.to_string();
            for sender in senders.values() {
                let _ = sender
                    .send(Err(DataFusionError::Execution(format!(
                        "upstream stream terminated with error: {err_msg}"
                    ))))
                    .await;
            }
        }

        // Close all senders so writer streams terminate naturally.
        drop(senders);

        // Step 2: join every writer task, collecting prepared overwrites and
        // any task-side errors. We always await every task even if one fails,
        // so that no writer task leaks past `write_all`'s return.
        let mut prepared: Vec<PreparedOverwrite> = Vec::with_capacity(handles.len());
        let mut task_err: Option<DataFusionError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(prep)) => prepared.push(prep),
                Ok(Err(e)) => {
                    if task_err.is_none() {
                        task_err = Some(DataFusionError::from(e));
                    }
                }
                Err(panic_err) => {
                    if task_err.is_none() {
                        task_err = Some(DataFusionError::Execution(format!(
                            "partition writer task panicked: {panic_err}"
                        )));
                    }
                }
            }
        }

        if let Some(err) = upstream_err.or(task_err) {
            for prep in prepared {
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!("rollback after stream/writer error failed: {rollback_err}");
                }
            }
            return Err(err);
        }

        if prepared.is_empty() {
            return Ok(0);
        }

        // Step 3: catalog transaction. Open once, apply every partition's
        // mutation, commit once. If any apply fails, roll back the prepared
        // overwrites (cleanup of the staged snapshot directories) and return
        // the error; the txn is auto-rolled-back when its handle drops.
        if let Err(err) = self.commit_in_one_txn(&prepared).await {
            for prep in prepared {
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!(
                        "rollback of prepared overwrite failed after txn error: {rollback_err}"
                    );
                }
            }
            return Err(err);
        }

        // Step 4: per-partition in-memory finish (snapshot id, listing
        // table, deletion caches, GC trigger). Failures here are logged but
        // do not roll back — the catalog has already committed, so readers
        // see the new state via the next scan.
        let mut total_rows: u64 = 0;
        for prep in prepared {
            match prep.finish().await {
                Ok(rows) => total_rows = total_rows.saturating_add(rows),
                Err(e) => {
                    tracing::warn!(
                        "finish() for prepared overwrite failed after txn commit: {e}; \
                         in-memory state will reconcile on next scan"
                    );
                }
            }
        }
        Ok(total_rows)
    }
}

impl CayennePartitionedOverwriteSink {
    /// Open one transaction, apply every partition's `commit_compaction_in_txn`,
    /// commit. Mirrors the retry-on-conflict shape of
    /// [`cayenne::MetadataCatalog::commit_compaction`] but for the batched
    /// cross-partition case.
    ///
    /// Retries on `SQLITE_BUSY` / `SQLITE_LOCKED` (and the equivalent Turso
    /// `BEGIN CONCURRENT` write-conflict at commit time). Each retry opens a
    /// fresh transaction and re-runs every `PreparedOverwrite::apply_in_txn`
    /// — the prepared receipts are immutable (data already on disk in their
    /// new snapshot directories), so re-applying their catalog mutations is
    /// safe and idempotent.
    async fn commit_in_one_txn(
        &self,
        prepared: &[PreparedOverwrite],
    ) -> datafusion::common::Result<()> {
        let max_attempts = turso_shared::DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        for attempt in 1..=max_attempts {
            let mut txn = self
                .catalog
                .begin_transaction()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut apply_err: Option<cayenne::CatalogError> = None;
            for prep in prepared {
                if let Err(e) = prep.apply_in_txn(&self.catalog, &mut *txn).await {
                    apply_err = Some(e);
                    break;
                }
            }
            if let Some(e) = apply_err {
                // Drop the transaction (auto-rollback). Retry if the failure
                // looks transient.
                drop(txn);
                if attempt < max_attempts && cayenne::is_retryable_write_conflict(&e) {
                    let delay = turso_shared::retry_backoff_delay(attempt);
                    tracing::debug!(
                        attempt,
                        max_attempts,
                        ?delay,
                        "Retrying cross-partition commit after apply_in_txn conflict"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(DataFusionError::External(Box::new(e)));
            }

            match txn.commit().await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < max_attempts && cayenne::is_retryable_write_conflict(&e) => {
                    let delay = turso_shared::retry_backoff_delay(attempt);
                    tracing::debug!(
                        attempt,
                        max_attempts,
                        ?delay,
                        "Retrying cross-partition commit after txn commit conflict"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }
        }

        Err(DataFusionError::Execution(format!(
            "cross-partition commit exhausted {max_attempts} attempts without success"
        )))
    }

    /// Resolve or create the partition provider for the given partition
    /// values. Mirrors `PartitionTableProvider::get_or_create_partition_provider`
    /// but operates on the `PartitionContext` directly so the sink doesn't
    /// need a back-reference to the provider.
    async fn get_or_create_partition_provider(
        &self,
        partition_values: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, DataFusionError> {
        let partition_key = encode_composite_key(&partition_values).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode partition key: {e}"))
        })?;

        let mut partitions_lock = self.partitions.write().await;
        if let Some(partition) = partitions_lock.get(&partition_key) {
            return Ok(Arc::clone(&partition.table_provider));
        }
        let partition = self
            .creator
            .create_partition(partition_values)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let provider = Arc::clone(&partition.table_provider);
        partitions_lock.insert(partition_key, partition);
        Ok(provider)
    }
}

/// Compile every partition-by expression into a physical expression that can
/// be evaluated against incoming `RecordBatch`es.
fn create_partition_physical_exprs(
    partition_by: &[PartitionedBy],
    schema: SchemaRef,
) -> datafusion::common::Result<Vec<Arc<dyn PhysicalExpr>>> {
    let input_dfschema = DFSchema::try_from(schema)?;
    let execution_props = ExecutionProps::new();
    partition_by
        .iter()
        .map(|partitioned_by| {
            create_physical_expr(
                &Expr::Column(Column::new_unqualified(match &partitioned_by.expression {
                    Expr::Column(c) => c.name.clone(),
                    other => other.to_string(),
                })),
                &input_dfschema,
                &execution_props,
            )
            // Fall through with the actual expression if the simple Column
            // shortcut fails (e.g. a derived partition expression).
            .or_else(|_| {
                create_physical_expr(
                    &partitioned_by.expression,
                    &input_dfschema,
                    &execution_props,
                )
            })
        })
        .collect()
}

/// Try to downcast a partition's table provider to a `CayenneTableProvider`.
///
/// The provider returned by `CayennePartitionCreator` is always a
/// `CayenneTableProvider`, so the downcast succeeds in practice; the
/// `Option` return covers the case where a non-Cayenne creator is
/// accidentally wired into this strategy (which should fail loudly).
fn downcast_to_cayenne(
    provider: &Arc<dyn datafusion::catalog::TableProvider>,
) -> Option<&CayenneTableProvider> {
    provider.as_any().downcast_ref::<CayenneTableProvider>()
}

// ============================================================================
// Append-mode coordinator (issue #10125 §6.3 + step 6).
//
// Append cannot use the overwrite path's MetastoreTransaction batching
// because append commits don't mutate the catalog at all — visibility is
// filesystem state + the in-memory ListingTable. The cross-partition
// guarantee is delivered by a *barrier*: every participating partition's
// listing fence is held for write while file moves + ListingTable swaps
// happen, so any reader going through `CayenneTableProvider::scan()`
// resolves either before or after the whole barrier, never in the middle.
//
// Crash safety: on local-filesystem tables, the top-level `PartitionedWal`
// written at `<table_root>/_partitioned_wal/<commit_id>.json` records every
// partition participating in this barrier. If the writer crashes mid-barrier,
// the WAL survives and the per-partition staging WALs that exist correspond
// to partitions in the top-level WAL. Operator/recovery uses this to decide
// whether to replay or roll back the set. Auto-recovery is a follow-up; the
// MVP keeps the WAL as a diagnostic anchor + a clean removal on success.
//
// S3-backed tables: `PartitionedWal::write_to` uses `tokio::fs` and would
// fail on an `s3://...` `table_root`. Until the WAL grows an object-store IO
// path, the append coordinator skips the top-level WAL for S3 tables and
// relies on each partition's staging WAL for single-partition recovery. The
// cross-partition barrier (fence + ordered fence acquisition) still holds —
// what is lost is the *set anchor* needed to atomically replay or roll back
// a crash that interrupted the apply loop across partitions. For the MVP
// that gap is acceptable: the per-partition `ensure_no_incomplete_write`
// check still blocks any partition whose staging WAL survives, so no
// half-applied state becomes silently visible to readers.
// ============================================================================

/// `DataSink` that fans the input stream out by partition key, stages every
/// partition's append, and commits all of them under one shared
/// cross-partition barrier.
///
/// Streaming + backpressure work the same way as the overwrite sink (see
/// [`CayennePartitionedOverwriteSink`]); only the commit-side differs.
struct CayennePartitionedAppendSink {
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    table_root: PathBuf,
}

impl std::fmt::Debug for CayennePartitionedAppendSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionedAppendSink")
            .field("partition_exprs", &self.physical_exprs.len())
            .field("table_root", &self.table_root)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayennePartitionedAppendSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CayennePartitionedAppendSink")
    }
}

#[async_trait]
impl DataSink for CayennePartitionedAppendSink {
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
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let _coordinator_guard = self.coordinator_lock.lock().await;

        let target_partitions = context.session_config().target_partitions();

        // Phase 1: fan input out to per-partition writer tasks that each call
        // begin_staged_append → prepare (writes the per-partition staging
        // WAL). Same streaming + backpressure shape as the overwrite sink.
        let mut senders: HashMap<String, mpsc::Sender<datafusion::common::Result<RecordBatch>>> =
            HashMap::new();
        let mut handles: Vec<JoinHandle<cayenne::provider::Result<PreparedStagedAppend>>> =
            Vec::new();
        let mut upstream_err: Option<DataFusionError> = None;

        'outer: while let Some(batch_result) = data.next().await {
            let batch = match batch_result {
                Ok(batch) => batch,
                Err(e) => {
                    upstream_err = Some(e);
                    break 'outer;
                }
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let partitioned = partition_batch_composite(&batch, &self.physical_exprs)?;
            for (key, (values, batch_part)) in partitioned {
                let sender = if let Some(s) = senders.get(&key) {
                    s.clone()
                } else {
                    let provider = self.get_or_create_partition_provider(values).await?;
                    let cayenne = downcast_to_cayenne(&provider).ok_or_else(|| {
                        DataFusionError::Execution(
                            "CayennePartitionedInsertStrategy expects every partition's table \
                             provider to be a CayenneTableProvider"
                                .to_string(),
                        )
                    })?;
                    let cayenne_owned = cayenne.clone_for_write_operations();
                    let (tx, rx) = mpsc::channel::<datafusion::common::Result<RecordBatch>>(
                        PARTITION_WRITER_CHANNEL_DEPTH,
                    );
                    let schema_clone = Arc::clone(&self.schema);
                    let handle: JoinHandle<cayenne::provider::Result<PreparedStagedAppend>> =
                        tokio::spawn(async move {
                            let stream: SendableRecordBatchStream =
                                Box::pin(RecordBatchStreamAdapter::new(
                                    schema_clone,
                                    ReceiverStream::new(rx),
                                ));
                            let staged = cayenne_owned
                                .begin_staged_append(stream, target_partitions)
                                .await?;
                            staged.prepare().await
                        });
                    senders.insert(key.clone(), tx.clone());
                    handles.push(handle);
                    tx
                };

                if sender.send(Ok(batch_part)).await.is_err() {
                    upstream_err = Some(DataFusionError::Execution(
                        "partition writer task terminated before stream end".to_string(),
                    ));
                    break 'outer;
                }
            }
        }

        if let Some(ref err) = upstream_err {
            let err_msg = err.to_string();
            for sender in senders.values() {
                let _ = sender
                    .send(Err(DataFusionError::Execution(format!(
                        "upstream stream terminated with error: {err_msg}"
                    ))))
                    .await;
            }
        }
        drop(senders);

        // Join all writer tasks.
        let mut prepared: Vec<PreparedStagedAppend> = Vec::with_capacity(handles.len());
        let mut task_err: Option<DataFusionError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(prep)) => prepared.push(prep),
                Ok(Err(e)) => {
                    if task_err.is_none() {
                        task_err = Some(DataFusionError::from(e));
                    }
                }
                Err(panic_err) => {
                    if task_err.is_none() {
                        task_err = Some(DataFusionError::Execution(format!(
                            "partition writer task panicked: {panic_err}"
                        )));
                    }
                }
            }
        }

        if let Some(err) = upstream_err.or(task_err) {
            for prep in prepared {
                if let Err(rollback_err) = prep.rollback().await {
                    tracing::warn!("rollback after stream/writer error failed: {rollback_err}");
                }
            }
            return Err(err);
        }

        if prepared.is_empty() {
            return Ok(0);
        }

        // Phase 2: cross-partition barrier.
        //
        // (a) Sort prepared by table_id so all coordinators acquire fences in
        //     the same total order. (We already serialize coordinators via
        //     coordinator_lock, but sorting keeps the invariant defensible if
        //     the outer lock is ever relaxed.)
        // (b) Acquire every participating partition's listing fence for write.
        //     Held for the duration of the file-move + listing-swap loop.
        // (c) Write the top-level partitioned WAL before any file move.
        // (d) For each partition: move staged files into the snapshot dir,
        //     remove the per-partition WAL, swap the in-memory ListingTable.
        // (e) Remove the top-level WAL.
        // (f) Drop fence guards.
        prepared.sort_by(|a, b| a.table_id().cmp(b.table_id()));

        let mut fence_guards: Vec<tokio::sync::OwnedRwLockWriteGuard<()>> =
            Vec::with_capacity(prepared.len());
        for p in &prepared {
            fence_guards.push(p.lock_listing_fence_write_owned().await);
        }

        let commit_id = uuid::Uuid::now_v7().to_string();
        // The top-level partitioned WAL uses `tokio::fs` and only works for
        // local-filesystem table roots. For S3-backed tables we skip it and
        // rely on each partition's staging WAL for crash recovery (see the
        // S3 note in the module-level crash-safety comment). When the WAL
        // grows object-store IO, drop this branch.
        let table_root_str = self.table_root.to_string_lossy();
        let write_top_level_wal = !table_root_str.starts_with("s3://");
        if write_top_level_wal {
            let wal_entries: Vec<PartitionedWalEntry> = prepared
                .iter()
                .map(|p| PartitionedWalEntry {
                    table_id: p.table_id().to_string(),
                    staging_wal_path: Some(p.staging_wal_path().to_string_lossy().to_string()),
                })
                .collect();
            let top_level_wal =
                PartitionedWal::new(commit_id.clone(), table_root_str.to_string(), wal_entries);
            if let Err(e) = top_level_wal.write_to(&self.table_root).await {
                // Failed to even record the intent. Roll back every prepared
                // append. Fences will be released when fence_guards drops.
                drop(fence_guards);
                for prep in prepared {
                    if let Err(rb) = prep.rollback().await {
                        tracing::warn!("rollback after top-level WAL write failure: {rb}");
                    }
                }
                return Err(DataFusionError::from(e));
            }
        }

        // Apply the barrier on every partition. If any fails partway, the
        // top-level WAL stays on disk so the next process restart can
        // recover the set; we surface the error and stop. We do NOT attempt
        // automated mid-barrier rollback because already-applied partitions
        // have moved their files and removed their per-partition WALs —
        // reverting that without coordination is unsafe.
        for p in &prepared {
            if let Err(e) = p.apply_under_held_barrier().await {
                drop(fence_guards);
                return Err(DataFusionError::from(e));
            }
        }

        // Success path: remove top-level WAL, then release the fences. The
        // top-level WAL absence is what recovery uses to skip clean commits.
        // Mirrors the S3 skip on the write side — nothing to clean up if we
        // never wrote it.
        if write_top_level_wal
            && let Err(e) = PartitionedWal::remove(&self.table_root, &commit_id).await
        {
            // Visibility has already flipped on every partition; surface the
            // cleanup failure as a warning rather than rolling back. The
            // next coordinator's recovery sweep will treat the dangling WAL
            // as a no-op if every partition's per-partition WAL is absent.
            tracing::warn!(
                "Failed to remove top-level partitioned WAL after successful barrier: {e}"
            );
        }

        drop(fence_guards);

        // Phase 3: per-partition finish (drops the per-partition write guard,
        // returns row count).
        let mut total_rows: u64 = 0;
        for prep in prepared {
            match prep.finish().await {
                Ok(rows) => total_rows = total_rows.saturating_add(rows),
                Err(e) => {
                    tracing::warn!(
                        "finish() for prepared append failed after barrier: {e}; \
                         in-memory state will reconcile on next scan"
                    );
                }
            }
        }
        Ok(total_rows)
    }
}

impl CayennePartitionedAppendSink {
    /// Identical to [`CayennePartitionedOverwriteSink::get_or_create_partition_provider`].
    /// Duplicated here rather than abstracted so each sink remains
    /// independently readable; if a third coordinator joins, lift into a
    /// shared helper.
    async fn get_or_create_partition_provider(
        &self,
        partition_values: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, DataFusionError> {
        let partition_key = encode_composite_key(&partition_values).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode partition key: {e}"))
        })?;

        let mut partitions_lock = self.partitions.write().await;
        if let Some(partition) = partitions_lock.get(&partition_key) {
            return Ok(Arc::clone(&partition.table_provider));
        }
        let partition = self
            .creator
            .create_partition(partition_values)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let provider = Arc::clone(&partition.table_provider);
        partitions_lock.insert(partition_key, partition);
        Ok(provider)
    }
}
