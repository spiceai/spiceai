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
//!   participating partition stages its data into a prepared *target* snapshot;
//!   then, holding one shared `listing_fence.write()` barrier window, the staged
//!   files are made durable and every partition's `current_snapshot_id` pointer
//!   is advanced atomically in a single [`MetastoreTransaction`] (either every
//!   partition advances or none do), anchored by a top-level
//!   [`cayenne::PartitionedWal`] for crash recovery on local and object-store
//!   tables.
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
//! 3. Write a top-level [`cayenne::PartitionedWal`] anchor at
//!    `<table_root>/_partitioned_wal/<commit_id>.json` before any file move.
//! 4. For each receipt, call `apply_under_held_barrier`: move staged files
//!    into the snapshot directory, remove the per-partition WAL, swap the
//!    in-memory `ListingTable`.
//! 5. Remove the top-level WAL.
//! 6. Release fences (drop guards together).
//! 7. Run [`PreparedStagedAppend::finish`] on each receipt.

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

    /// Reconcile stale cross-partition WAL anchors after all partition
    /// providers have completed their per-partition staged-WAL recovery.
    ///
    /// The catalog pointer transaction is the only commit decision. For each
    /// set, every pointer must either equal its recorded target (committed) or
    /// differ (not committed). A mixed set is impossible under an atomic
    /// catalog transaction and is rejected rather than guessed at.
    pub async fn recover_partitioned_wals(
        &self,
        providers: &[Arc<dyn datafusion::catalog::TableProvider>],
    ) -> Result<(), DataFusionError> {
        let _coordinator_guard = Arc::clone(&self.coordinator_lock).lock_owned().await;
        let by_id: HashMap<&str, &CayenneTableProvider> = providers
            .iter()
            .filter_map(|provider| downcast_to_cayenne(provider))
            .map(|provider| (provider.table_id(), provider))
            .collect();

        let object_store_location = by_id
            .values()
            .next()
            .map(|provider| provider.partitioned_wal_object_store())
            .transpose()
            .map_err(DataFusionError::from)?
            .flatten();
        let local_wals;
        let object_wals;
        let wals: Vec<PartitionedWal> = if let Some((store, prefix, _)) = &object_store_location {
            object_wals = PartitionedWal::read_all_in_object_store(store.as_ref(), prefix)
                .await
                .map_err(DataFusionError::from)?;
            object_wals
        } else {
            local_wals = PartitionedWal::read_all_in(&self.table_root)
                .await
                .map_err(DataFusionError::from)?;
            local_wals.into_iter().map(|(wal, _)| wal).collect()
        };

        for wal in wals {
            let mut missing_table_ids = wal
                .partitions
                .iter()
                .filter(|entry| !by_id.contains_key(entry.table_id.as_str()))
                .map(|entry| entry.table_id.as_str())
                .collect::<Vec<_>>();
            missing_table_ids.sort_unstable();
            missing_table_ids.dedup();
            if !missing_table_ids.is_empty() {
                return Err(DataFusionError::Execution(format!(
                    "Failed to recover partitioned Cayenne commit {}: providers for participant table IDs [{}] are unavailable; retaining its WAL until every participant can be reconciled",
                    wal.commit_id,
                    missing_table_ids.join(", ")
                )));
            }

            let mut committed = 0usize;
            let mut uncommitted = 0usize;
            for entry in &wal.partitions {
                let target_snapshot_id = entry.target_snapshot_id.as_deref().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed to automatically recover legacy partitioned Cayenne commit {}: partition {} has no target snapshot; retaining its WAL for manual recovery",
                        wal.commit_id, entry.table_id
                    ))
                })?;
                let current_snapshot_id = self
                    .catalog
                    .current_snapshot_id_for_table(&entry.table_id)
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                if current_snapshot_id == target_snapshot_id {
                    committed += 1;
                } else {
                    uncommitted += 1;
                }
            }
            if committed > 0 && uncommitted > 0 {
                return Err(DataFusionError::Execution(format!(
                    "Failed to recover partitioned Cayenne commit {}: catalog contains mixed snapshot pointers ({committed} committed, {uncommitted} uncommitted); refusing potentially incorrect recovery",
                    wal.commit_id
                )));
            }

            // Opening a provider already invokes this recovery. Re-run it
            // here to make convergence explicit and to cover providers that
            // were opened before another participant finished recovery.
            for entry in &wal.partitions {
                let provider = by_id.get(entry.table_id.as_str()).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "provider for validated partitioned Cayenne participant {} disappeared during recovery",
                        entry.table_id
                    ))
                })?;
                provider
                    .recover_incomplete_writes()
                    .await
                    .map_err(DataFusionError::from)?;
            }

            if let Some((store, prefix, _)) = &object_store_location {
                PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &wal.commit_id)
                    .await
                    .map_err(DataFusionError::from)?;
            } else {
                PartitionedWal::remove(&self.table_root, &wal.commit_id)
                    .await
                    .map_err(DataFusionError::from)?;
            }
        }
        Ok(())
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
                    catalog: Arc::clone(&self.catalog),
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
/// `accelerated::write::dual_write.rs` for the same shape.
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
                        cayenne_owned
                            .begin_overwrite(stream, target_partitions)
                            .await
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

        // Fast path: take a read lock first. Existing partitions hit this
        // path on every subsequent insert, and we MUST NOT serialize those
        // through a write lock. The previous revision unconditionally
        // acquired `partitions.write().await`, which made every per-row
        // partition lookup contend on the same exclusive lock and produced
        // a global write barrier across the whole partitioned table — the
        // difference between ~1-row-per-RTT (write-locked) and parallel
        // processing across all partitions (read-locked) on sustained
        // partitioned ingestion.
        {
            let read_guard = self.partitions.read().await;
            if let Some(partition) = read_guard.get(&partition_key) {
                return Ok(Arc::clone(&partition.table_provider));
            }
        }

        // Slow path: the partition is new. Acquire the write lock, but
        // double-check the map first — another writer may have created
        // the same partition while we waited for the lock.
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
///
/// For bare `Expr::Column` partitions, the column is resolved unqualified
/// against the input schema. All other expressions (derived/computed
/// partitions) are compiled as-is — never via stringified column-name lookup,
/// because the debug form of a non-Column expression could spuriously match
/// an unrelated column name and route data through the wrong physical
/// expression.
fn create_partition_physical_exprs(
    partition_by: &[PartitionedBy],
    schema: SchemaRef,
) -> datafusion::common::Result<Vec<Arc<dyn PhysicalExpr>>> {
    let input_dfschema = DFSchema::try_from(schema)?;
    let execution_props = ExecutionProps::new();
    partition_by
        .iter()
        .map(|partitioned_by| match &partitioned_by.expression {
            Expr::Column(c) => create_physical_expr(
                &Expr::Column(Column::new_unqualified(c.name.clone())),
                &input_dfschema,
                &execution_props,
            ),
            other => create_physical_expr(other, &input_dfschema, &execution_props),
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
    provider.downcast_ref::<CayenneTableProvider>()
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
// Crash safety: the top-level `PartitionedWal`
// written at `<table_root>/_partitioned_wal/<commit_id>.json` records every
// partition participating in this barrier. If the writer crashes, provider
// startup converges each participant from the catalog pointer and then this
// coordinator validates and removes the stale set anchor. Local filesystems
// use an atomic file; S3-compatible stores use object-store put/delete.
// ============================================================================

/// `DataSink` that fans the input stream out by partition key, stages every
/// partition's append, and commits all of them under one shared
/// cross-partition barrier.
///
/// Streaming + backpressure work the same way as the overwrite sink (see
/// [`CayennePartitionedOverwriteSink`]); only the commit-side differs.
struct CayennePartitionedAppendSink {
    catalog: Arc<CayenneCatalog>,
    coordinator_lock: Arc<tokio::sync::Mutex<()>>,
    creator: Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions: Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    table_root: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
enum AppendCommitState {
    AllCommitted,
    AllUncommitted,
    Mixed {
        committed: usize,
        uncommitted: usize,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum AppendCommitFailureDisposition<E> {
    RecoverCommitted,
    Rollback,
    RetainMixed {
        committed: usize,
        uncommitted: usize,
    },
    RetainUnknown(E),
}

fn append_commit_failure_disposition<E>(
    classification: Result<AppendCommitState, E>,
) -> AppendCommitFailureDisposition<E> {
    match classification {
        Ok(AppendCommitState::AllCommitted) => AppendCommitFailureDisposition::RecoverCommitted,
        Ok(AppendCommitState::AllUncommitted) => AppendCommitFailureDisposition::Rollback,
        Ok(AppendCommitState::Mixed {
            committed,
            uncommitted,
        }) => AppendCommitFailureDisposition::RetainMixed {
            committed,
            uncommitted,
        },
        Err(error) => AppendCommitFailureDisposition::RetainUnknown(error),
    }
}

trait AmbiguousCommitReceipt {
    type OnConflict;

    fn restore_on_conflict(&mut self, prepared: Option<Self::OnConflict>);
    fn retain_for_wal_recovery(&mut self);
}

impl AmbiguousCommitReceipt for PreparedStagedAppend {
    type OnConflict = cayenne::provider::PreparedOnConflictDeletionPublish;

    fn restore_on_conflict(&mut self, prepared: Option<Self::OnConflict>) {
        self.restore_prepared_on_conflict(prepared);
    }

    fn retain_for_wal_recovery(&mut self) {
        self.retain_files_for_wal_recovery();
    }
}

fn retain_ambiguous_commit_receipts<R: AmbiguousCommitReceipt>(
    receipts: &mut [R],
    prepared_on_conflicts: Vec<Option<R::OnConflict>>,
) {
    for (receipt, on_conflict) in receipts.iter_mut().zip(prepared_on_conflicts) {
        receipt.restore_on_conflict(on_conflict);
        receipt.retain_for_wal_recovery();
    }
}

fn classify_pointer_matches(pointer_matches: impl IntoIterator<Item = bool>) -> AppendCommitState {
    let mut committed = 0usize;
    let mut uncommitted = 0usize;
    for matches_target in pointer_matches {
        if matches_target {
            committed += 1;
        } else {
            uncommitted += 1;
        }
    }
    if uncommitted == 0 {
        AppendCommitState::AllCommitted
    } else if committed == 0 {
        AppendCommitState::AllUncommitted
    } else {
        AppendCommitState::Mixed {
            committed,
            uncommitted,
        }
    }
}

fn same_partitioned_wal_backend(
    first: Option<&cayenne::provider::PartitionedWalObjectStore>,
    participant: Option<&cayenne::provider::PartitionedWalObjectStore>,
) -> bool {
    match (first, participant) {
        (None, None) => true,
        (Some((_, first_prefix, first_backend)), Some((_, prefix, backend))) => {
            first_prefix == prefix && first_backend == backend
        }
        _ => false,
    }
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
        let _coordinator_guard = Arc::clone(&self.coordinator_lock).lock_owned().await;

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
                    if !cayenne.supports_deferred_partition_append() {
                        return Err(DataFusionError::NotImplemented(
                            "This Cayenne partition does not support atomic deferred append"
                                .to_string(),
                        ));
                    }
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
                                .begin_deferred_snapshot_append(stream, target_partitions)
                                .await?;
                            Ok(staged)
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
        let table_root_str = self.table_root.to_string_lossy();
        let wal_entries: Vec<PartitionedWalEntry> = prepared
            .iter()
            .map(|p| PartitionedWalEntry {
                table_id: p.table_id().to_string(),
                target_snapshot_id: Some(p.target_snapshot_id().to_string()),
                staging_wal_path: Some(p.staging_wal_path().to_string_lossy().to_string()),
            })
            .collect();
        let top_level_wal =
            PartitionedWal::new(commit_id.clone(), table_root_str.to_string(), wal_entries);
        let object_store_wal = prepared[0]
            .partitioned_wal_object_store()
            .map_err(DataFusionError::from)?;
        for participant in prepared.iter().skip(1) {
            let participant_wal = participant
                .partitioned_wal_object_store()
                .map_err(DataFusionError::from)?;
            let same_backend =
                same_partitioned_wal_backend(object_store_wal.as_ref(), participant_wal.as_ref());
            if !same_backend {
                drop(fence_guards);
                for receipt in prepared {
                    if let Err(error) = receipt.rollback().await {
                        tracing::warn!(
                            %error,
                            "Failed to roll back deferred append after heterogeneous WAL backend validation"
                        );
                    }
                }
                return Err(DataFusionError::Execution(
                    "Cannot atomically append across Cayenne partitions configured with different WAL storage backends or prefixes"
                        .to_string(),
                ));
            }
        }
        let wal_write_result = if let Some((store, prefix, _)) = &object_store_wal {
            top_level_wal
                .write_to_object_store(store.as_ref(), prefix)
                .await
                .map(|_| ())
        } else {
            top_level_wal.write_to(&self.table_root).await.map(|_| ())
        };
        if let Err(e) = wal_write_result {
            drop(fence_guards);
            for prep in prepared {
                if let Err(rb) = prep.rollback().await {
                    tracing::warn!("rollback after top-level WAL write failure: {rb}");
                }
            }
            return Err(DataFusionError::from(e));
        }

        // Build every fallible in-memory publication object before moving a
        // staged file. Once the barrier move starts, rollback is no longer a
        // generally safe option; after the catalog commit publication itself
        // must be infallible and await-free.
        let publish_states = match prepared
            .iter()
            .map(PreparedStagedAppend::prepare_deferred_snapshot_publish)
            .collect::<cayenne::provider::Result<Vec<_>>>()
        {
            Ok(states) => states,
            Err(error) => {
                if let Err(cleanup_error) = if let Some((store, prefix, _)) = &object_store_wal {
                    PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &commit_id)
                        .await
                } else {
                    PartitionedWal::remove(&self.table_root, &commit_id).await
                } {
                    tracing::warn!(
                        "Failed to remove top-level WAL after append preparation failure: {cleanup_error}"
                    );
                }
                drop(fence_guards);
                for receipt in prepared {
                    if let Err(rollback_error) = receipt.rollback().await {
                        tracing::warn!("Failed to roll back deferred append: {rollback_error}");
                    }
                }
                return Err(DataFusionError::from(error));
            }
        };

        let mut prepared_on_conflicts = prepared
            .iter_mut()
            .map(PreparedStagedAppend::take_prepared_on_conflict)
            .collect::<Vec<_>>();

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
        drop(fence_guards);
        for receipt in &mut prepared {
            if let Err(error) = receipt.prepare_deferred_manifest().await {
                return Err(DataFusionError::from(error));
            }
        }
        let mut fence_guards: Vec<tokio::sync::OwnedRwLockWriteGuard<()>> =
            Vec::with_capacity(prepared.len());
        for receipt in &prepared {
            fence_guards.push(receipt.lock_listing_fence_write_owned().await);
        }

        // Own the durable commit, every participant publication, and cleanup in
        // one detached-safe task. Dropping the request future while COMMIT is in
        // flight only drops this JoinHandle; the task retains the receipts,
        // deletion-file guards, and listing fences and runs through publication
        // before releasing them. This closes the ambiguous-COMMIT cancellation
        // window where a database connection could commit after its caller was
        // dropped while abort guards unlinked newly-live deletion vectors.
        let catalog = Arc::clone(&self.catalog);
        let table_root = self.table_root.clone();
        let completion = tokio::spawn(async move {
            let _coordinator_guard = _coordinator_guard;
            // The complete post-append contents now exist in one private snapshot
            // per partition. Flip every pointer in a single metastore transaction;
            // readers cannot observe a subset through a fresh directory listing.
            if let Err(error) = Self::commit_append_snapshots_in_one_txn(
                catalog.as_ref(),
                &prepared,
                &mut prepared_on_conflicts,
            )
            .await
            {
                let commit_state =
                    Self::classify_append_snapshot_pointers(catalog.as_ref(), &prepared).await;
                match append_commit_failure_disposition(commit_state) {
                    AppendCommitFailureDisposition::RecoverCommitted => {
                        // `COMMIT` can complete but report an ambiguous transport
                        // failure. Durable pointers are the decision: restore each
                        // payload, prove its generated DV paths are in committed
                        // metadata, then let per-partition WAL recovery reload and
                        // publish the complete durable state.
                        for (receipt, on_conflict) in prepared.iter_mut().zip(prepared_on_conflicts)
                        {
                            receipt.restore_prepared_on_conflict(on_conflict);
                            receipt
                                .reconcile_committed_on_conflict_cleanup()
                                .await
                                .map_err(DataFusionError::from)?;
                        }
                        drop(fence_guards);
                        for receipt in &prepared {
                            receipt
                                .recover_committed_snapshot()
                                .await
                                .map_err(DataFusionError::from)?;
                        }
                        return Err(error);
                    }
                    AppendCommitFailureDisposition::Rollback => {}
                    AppendCommitFailureDisposition::RetainMixed {
                        committed,
                        uncommitted,
                    } => {
                        retain_ambiguous_commit_receipts(&mut prepared, prepared_on_conflicts);
                        drop(fence_guards);
                        return Err(DataFusionError::Execution(format!(
                            "Cross-partition Cayenne commit returned an error and durable catalog pointers are mixed ({committed} committed, {uncommitted} uncommitted); refusing rollback and retaining every WAL for manual recovery"
                        )));
                    }
                    AppendCommitFailureDisposition::RetainUnknown(classification_error) => {
                        retain_ambiguous_commit_receipts(&mut prepared, prepared_on_conflicts);
                        drop(fence_guards);
                        return Err(DataFusionError::Execution(format!(
                            "Cross-partition Cayenne commit returned an error and its durable outcome could not be classified ({classification_error}); refusing rollback and retaining every WAL for recovery. Original commit error: {error}"
                        )));
                    }
                }
                drop(fence_guards);
                for (receipt, on_conflict) in prepared.iter_mut().zip(prepared_on_conflicts) {
                    receipt.restore_prepared_on_conflict(on_conflict);
                }
                let mut rollback_failed = false;
                for receipt in prepared {
                    if let Err(rollback_error) = receipt.rollback().await {
                        rollback_failed = true;
                        tracing::warn!("Failed to roll back deferred append: {rollback_error}");
                    }
                }
                if !rollback_failed
                    && let Err(cleanup_error) = if let Some((store, prefix, _)) = &object_store_wal
                    {
                        PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &commit_id)
                            .await
                    } else {
                        PartitionedWal::remove(&table_root, &commit_id).await
                    }
                {
                    tracing::warn!(
                        "Failed to remove top-level WAL after append rollback: {cleanup_error}"
                    );
                }
                return Err(error);
            }

            // No await is permitted between these publications. The catalog commit
            // above is the durable global decision; cancellation must not leave a
            // proper subset of the participating providers on the new snapshots.
            for ((receipt, publish_state), prepared_on_conflict) in prepared
                .iter()
                .zip(publish_states)
                .zip(prepared_on_conflicts)
            {
                receipt.publish_deferred_snapshot_under_held_fence(publish_state);
                // Capture the on-conflict publish sequence before the value is
                // consumed below; an on-conflict append has no `append_sequence`,
                // so this is the sequence its validated keys must be stamped with.
                let on_conflict_sequence = prepared_on_conflict
                    .as_ref()
                    .map(cayenne::provider::PreparedOnConflictDeletionPublish::snapshot_sequence);
                if let Some(on_conflict) = prepared_on_conflict {
                    receipt.publish_on_conflict_under_held_fence(on_conflict);
                }
                receipt.publish_validated_file_keys(on_conflict_sequence);
            }

            // Publication is complete for every participant. Release all listing
            // fences before WAL deletion and other maintenance so readers are not
            // blocked by recoverable post-commit I/O.
            drop(fence_guards);

            // WAL cleanup is post-commit maintenance. Recovery consults the
            // catalog pointers and safely recognizes these as committed if cleanup
            // is interrupted.
            for receipt in &prepared {
                if let Err(error) = receipt.remove_committed_staging_wal().await {
                    tracing::warn!(
                        table_id = receipt.table_id(),
                        target_snapshot = receipt.target_snapshot_id(),
                        staging_wal_path = %receipt.staging_wal_path().display(),
                        %error,
                        "Failed to remove committed partition staging WAL"
                    );
                }
            }

            let wal_remove_result = if let Some((store, prefix, _)) = &object_store_wal {
                PartitionedWal::remove_from_object_store(store.as_ref(), prefix, &commit_id).await
            } else {
                PartitionedWal::remove(&table_root, &commit_id).await
            };
            if let Err(error) = wal_remove_result {
                tracing::warn!(
                    commit_id,
                    %error,
                    "Failed to remove committed cross-partition WAL; append remains committed"
                );
            }

            // Phase 3: per-partition finish (drops the per-partition write guard,
            // returns row count).
            let mut total_rows: u64 = 0;
            for prep in prepared {
                prep.finish_deferred_snapshot_maintenance().await;
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
        });

        match completion.await {
            Ok(result) => result,
            Err(error) => Err(DataFusionError::Execution(format!(
                "Cayenne cross-partition commit task failed: {error}"
            ))),
        }
    }
}

impl CayennePartitionedAppendSink {
    async fn classify_append_snapshot_pointers(
        catalog: &CayenneCatalog,
        prepared: &[PreparedStagedAppend],
    ) -> datafusion::common::Result<AppendCommitState> {
        let mut pointer_matches = Vec::with_capacity(prepared.len());
        for receipt in prepared {
            let current = catalog
                .current_snapshot_id_for_table(receipt.table_id())
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            pointer_matches.push(current == receipt.target_snapshot_id());
        }
        Ok(classify_pointer_matches(pointer_matches))
    }

    async fn commit_append_snapshots_in_one_txn(
        catalog: &CayenneCatalog,
        prepared: &[PreparedStagedAppend],
        prepared_on_conflicts: &mut [Option<
            cayenne::provider::PreparedOnConflictDeletionPublish,
        >],
    ) -> datafusion::common::Result<()> {
        let max_attempts = turso_shared::DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS;
        let snapshots: Vec<(&str, &str)> = prepared
            .iter()
            .map(|receipt| (receipt.table_id(), receipt.target_snapshot_id()))
            .collect();

        'attempts: for attempt in 1..=max_attempts {
            let mut txn = catalog
                .begin_transaction()
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if let Err(error) = catalog
                .set_current_snapshots_in_txn(&mut *txn, &snapshots)
                .await
            {
                // Roll back explicitly (not via the transaction's best-effort,
                // possibly-detached Drop) so the metastore writer lock is released
                // deterministically before this attempt backs off and retries.
                if let Err(rollback_error) = txn.rollback().await {
                    tracing::warn!(
                        "Failed to roll back cross-partition append transaction before retry: {rollback_error}"
                    );
                }
                if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) {
                    tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                    continue;
                }
                return Err(DataFusionError::External(Box::new(error)));
            }
            for on_conflict in prepared_on_conflicts.iter_mut().flatten() {
                if let Err(error) = catalog
                    .apply_prepared_on_conflict_in_txn(&mut *txn, on_conflict)
                    .await
                {
                    // Roll back explicitly (not via the transaction's best-effort,
                    // possibly-detached Drop) so the metastore writer lock is released
                    // deterministically before this attempt backs off and retries.
                    if let Err(rollback_error) = txn.rollback().await {
                        tracing::warn!(
                            "Failed to roll back cross-partition append transaction before retry: {rollback_error}"
                        );
                    }
                    if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) {
                        tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                        continue 'attempts;
                    }
                    return Err(DataFusionError::External(Box::new(error)));
                }
            }
            for receipt in prepared {
                if let Some(manifest) = receipt.deferred_manifest()
                    && let Err(error) = catalog
                        .replace_snapshot_files_in_txn(
                            &mut *txn,
                            receipt.table_id(),
                            receipt.target_snapshot_id(),
                            manifest,
                        )
                        .await
                {
                    // Roll back explicitly (not via the transaction's best-effort,
                    // possibly-detached Drop) so the metastore writer lock is released
                    // deterministically before this attempt backs off and retries.
                    if let Err(rollback_error) = txn.rollback().await {
                        tracing::warn!(
                            "Failed to roll back cross-partition append transaction before retry: {rollback_error}"
                        );
                    }
                    if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) {
                        tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                        continue 'attempts;
                    }
                    return Err(DataFusionError::External(Box::new(error)));
                }
            }
            match txn.commit().await {
                Ok(()) => {
                    for on_conflict in prepared_on_conflicts.iter_mut().flatten() {
                        on_conflict.mark_catalog_committed();
                    }
                    return Ok(());
                }
                Err(error)
                    if attempt < max_attempts && cayenne::is_retryable_write_conflict(&error) =>
                {
                    tokio::time::sleep(turso_shared::retry_backoff_delay(attempt)).await;
                }
                Err(error) => return Err(DataFusionError::External(Box::new(error))),
            }
        }

        Err(DataFusionError::Execution(format!(
            "cross-partition append commit exhausted {max_attempts} attempts without success"
        )))
    }

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

        // Fast path: take a read lock first. Existing partitions hit this
        // path on every subsequent insert, and we MUST NOT serialize those
        // through a write lock. The previous revision unconditionally
        // acquired `partitions.write().await`, which made every per-row
        // partition lookup contend on the same exclusive lock and produced
        // a global write barrier across the whole partitioned table — the
        // difference between ~1-row-per-RTT (write-locked) and parallel
        // processing across all partitions (read-locked) on sustained
        // partitioned ingestion.
        {
            let read_guard = self.partitions.read().await;
            if let Some(partition) = read_guard.get(&partition_key) {
                return Ok(Arc::clone(&partition.table_provider));
            }
        }

        // Slow path: the partition is new. Acquire the write lock, but
        // double-check the map first — another writer may have created
        // the same partition while we waited for the lock.
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectStorePath;

    fn memory_store() -> Arc<dyn object_store::ObjectStore> {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        store
    }

    #[derive(Debug, Default)]
    struct FakeAmbiguousReceipt {
        restored: Option<u8>,
        retained: bool,
    }

    impl AmbiguousCommitReceipt for FakeAmbiguousReceipt {
        type OnConflict = u8;

        fn restore_on_conflict(&mut self, prepared: Option<Self::OnConflict>) {
            self.restored = prepared;
        }

        fn retain_for_wal_recovery(&mut self) {
            self.retained = true;
        }
    }

    fn object_store_wal(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        backend: &str,
    ) -> cayenne::provider::PartitionedWalObjectStore {
        (store, ObjectStorePath::from(prefix), backend.to_string())
    }

    #[test]
    fn classifies_every_durable_pointer_outcome() {
        assert_eq!(
            classify_pointer_matches([true, true]),
            AppendCommitState::AllCommitted
        );
        assert_eq!(
            classify_pointer_matches([false, false]),
            AppendCommitState::AllUncommitted
        );
        assert_eq!(
            classify_pointer_matches([true, false, true]),
            AppendCommitState::Mixed {
                committed: 2,
                uncommitted: 1,
            }
        );
    }

    #[test]
    fn maps_mixed_and_unknown_outcomes_to_non_destructive_retention() {
        assert_eq!(
            append_commit_failure_disposition::<&str>(Ok(AppendCommitState::Mixed {
                committed: 1,
                uncommitted: 2,
            })),
            AppendCommitFailureDisposition::RetainMixed {
                committed: 1,
                uncommitted: 2,
            }
        );
        assert_eq!(
            append_commit_failure_disposition::<&str>(Err("catalog unavailable")),
            AppendCommitFailureDisposition::RetainUnknown("catalog unavailable")
        );
    }

    #[test]
    fn ambiguous_outcomes_restore_payloads_before_retaining_files() {
        let mut receipts = vec![
            FakeAmbiguousReceipt::default(),
            FakeAmbiguousReceipt::default(),
        ];
        retain_ambiguous_commit_receipts(&mut receipts, vec![Some(7), None]);

        assert_eq!(receipts[0].restored, Some(7));
        assert!(receipts[0].retained);
        assert_eq!(receipts[1].restored, None);
        assert!(receipts[1].retained);
    }

    #[test]
    fn accepts_distinct_handles_for_the_same_wal_backend() {
        let first = memory_store();
        let second = memory_store();
        assert!(!Arc::ptr_eq(&first, &second));

        assert!(same_partitioned_wal_backend(
            Some(&object_store_wal(first, "shared/table", "s3://bucket")),
            Some(&object_store_wal(second, "shared/table", "s3://bucket")),
        ));
    }

    #[test]
    fn rejects_same_prefix_on_heterogeneous_wal_backends() {
        let first = memory_store();
        let second = memory_store();

        assert!(!same_partitioned_wal_backend(
            Some(&object_store_wal(first, "shared/table", "s3://bucket-a")),
            Some(&object_store_wal(second, "shared/table", "s3://bucket-b")),
        ));
    }

    #[test]
    fn rejects_different_prefixes_on_the_same_wal_backend() {
        let first = memory_store();
        let second = memory_store();

        assert!(!same_partitioned_wal_backend(
            Some(&object_store_wal(first, "table-a", "s3://bucket")),
            Some(&object_store_wal(second, "table-b", "s3://bucket")),
        ));
    }
}
