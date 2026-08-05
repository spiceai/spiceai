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
//! - **Overwrite** ([`insert::CayennePartitionedOverwriteSink`]): every
//!   participating partition's catalog mutation is batched into a single
//!   `MetastoreTransaction` so the `current_snapshot_id` pointer flips
//!   happen atomically — either every partition advances or none do. See
//!   that type's docs for the full coordination flow.
//! - **Append / Replace** ([`append::CayennePartitionedAppendSink`]): every
//!   participating partition stages its data into a prepared *target*
//!   snapshot; then, holding one shared `listing_fence.write()` barrier
//!   window, the staged files are made durable and every partition's
//!   `current_snapshot_id` pointer is advanced atomically in a single
//!   `MetastoreTransaction` (either every partition advances or none do),
//!   anchored by a top-level [`cayenne::PartitionedWal`] for crash recovery
//!   on local and object-store tables. See that type's docs for the full
//!   coordination flow.
//!
//! This module holds the pieces shared by both sinks: the top-level
//! [`CayennePartitionedInsertStrategy`] that dispatches to whichever sink an
//! `InsertOp` needs, and the writer-task fan-out/error-resolution machinery
//! ([`WriteFanoutFailure`], [`join_writer_handles`], [`resolve_write_error`],
//! [`poison_open_writer_channels`]) both sinks drive identically.

mod append;
mod insert;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use cayenne::{CayenneCatalog, CayenneTableProvider, PartitionedWal};
use datafusion::common::{Column, DFSchema};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, dml::InsertOp};
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::encode_composite_key;
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::insert::{InsertStrategy, PartitionContext};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Per-partition key type used by `PartitionTableProvider`. The
/// runtime-table-partition crate keeps the concrete alias `pub(crate)`; for
/// our local map we just use the underlying `String`.
type CompositePartitionKey = String;

/// Per-partition writer-task channel depth. Bounds in-flight backpressure on
/// the slowest partition; larger values trade memory for throughput tolerance
/// when partitions write at different speeds. Matches the value used by
/// `accelerated_table::write::dual_write.rs` for the same shape.
const PARTITION_WRITER_CHANNEL_DEPTH: usize = 8;

/// Sentinel tag on a `DataFusionError::Context` wrapping a poison-pill error
/// the coordinator pushes into every still-open partition's channel after
/// another partition's writer (or the upstream stream) already failed. A
/// writer task's own `Err` carrying this tag is an echo of that other
/// failure, not a root cause it discovered itself — [`join_writer_handles`]
/// and [`resolve_write_error`] skip tagged errors so the real error (from
/// whichever writer failed first, or the upstream stream) surfaces instead
/// of a generic placeholder.
const POISONED_BY_UPSTREAM_TAG: &str =
    "cayenne partitioned writer: poisoned after a sibling partition/upstream failure";

/// True if `err` is a poison-pill echo the coordinator itself injected (see
/// [`POISONED_BY_UPSTREAM_TAG`]) rather than an error a writer task produced
/// on its own.
fn is_poisoned_echo(err: &DataFusionError) -> bool {
    matches!(err, DataFusionError::Context(tag, _) if tag == POISONED_BY_UPSTREAM_TAG)
}

/// Why the input loop stopped routing batches to writer tasks before the
/// input stream was fully drained.
enum WriteFanoutFailure {
    /// The input stream itself returned an error.
    Upstream(DataFusionError),
    /// A partition writer's channel closed — that task already returned
    /// before the coordinator finished sending it batches. The real cause is
    /// whatever that task's own `Err`/panic reports once joined; see
    /// [`resolve_write_error`].
    WriterChannelClosed,
}

/// Propagate a poison pill down every still-open writer channel so each
/// partition's writer task observes the failure and returns `Err` instead
/// of committing a truncated write. Tagged with [`POISONED_BY_UPSTREAM_TAG`]
/// so [`resolve_write_error`] can tell this echo apart from a writer's own
/// genuine error once handles are joined.
async fn poison_open_writer_channels(
    senders: &HashMap<String, mpsc::Sender<datafusion::common::Result<RecordBatch>>>,
    failure: &WriteFanoutFailure,
) {
    let poison_msg = match failure {
        WriteFanoutFailure::Upstream(err) => {
            format!("upstream stream terminated with error: {err}")
        }
        WriteFanoutFailure::WriterChannelClosed => {
            "a sibling partition's writer task failed; aborting this partition too".to_string()
        }
    };
    for sender in senders.values() {
        let _ = sender
            .send(Err(DataFusionError::Execution(poison_msg.clone())
                .context(POISONED_BY_UPSTREAM_TAG)))
            .await;
    }
}

/// Join every writer-task handle, separating task-side errors into "genuine"
/// (a task's own failure) vs "poisoned echo" (the coordinator's poison-pill,
/// delivered after some other failure already ended the write). Always
/// awaits every handle so no writer task leaks past `write_all`'s return.
async fn join_writer_handles<T>(
    handles: Vec<JoinHandle<cayenne::provider::Result<T>>>,
) -> (Vec<T>, Option<DataFusionError>, Option<DataFusionError>) {
    let mut prepared = Vec::with_capacity(handles.len());
    let mut genuine_task_err: Option<DataFusionError> = None;
    let mut poisoned_task_err: Option<DataFusionError> = None;
    for handle in handles {
        match handle.await {
            Ok(Ok(prep)) => prepared.push(prep),
            Ok(Err(e)) => {
                let e = DataFusionError::from(e);
                if is_poisoned_echo(&e) {
                    poisoned_task_err.get_or_insert(e);
                } else {
                    genuine_task_err.get_or_insert(e);
                }
            }
            Err(panic_err) => {
                genuine_task_err.get_or_insert(DataFusionError::Execution(format!(
                    "partition writer task panicked: {panic_err}"
                )));
            }
        }
    }
    (prepared, genuine_task_err, poisoned_task_err)
}

/// Resolve the single error to report for a failed `write_all`, preferring
/// whichever error actually carries the root cause:
/// - a genuine upstream (source-stream) error always wins — it's already
///   specific, and every writer task's own error is just a downstream effect
///   of it;
/// - otherwise, when a writer's channel closed early, prefer that task's own
///   `genuine_task_err` over the poison-pill echoes sent to every other
///   partition — that's the one writer that actually failed independently;
/// - a bare `WriterChannelClosed` with no writer-side error at all shouldn't
///   happen (the channel only closes because some task already returned),
///   but falls back to a labeled placeholder rather than losing the error
///   entirely.
fn resolve_write_error(
    fanout_failure: Option<WriteFanoutFailure>,
    genuine_task_err: Option<DataFusionError>,
    poisoned_task_err: Option<DataFusionError>,
) -> Option<DataFusionError> {
    match fanout_failure {
        Some(WriteFanoutFailure::Upstream(err)) => Some(err),
        Some(WriteFanoutFailure::WriterChannelClosed) => {
            Some(genuine_task_err.or(poisoned_task_err).unwrap_or_else(|| {
                DataFusionError::Execution(
                    "partition writer task terminated before stream end, and no writer task \
                     reported a specific error"
                        .to_string(),
                )
            }))
        }
        None => genuine_task_err.or(poisoned_task_err),
    }
}

/// Resolve or create the partition provider for the given partition values.
/// Shared by both [`insert::CayennePartitionedOverwriteSink`] and
/// [`append::CayennePartitionedAppendSink`], which hold identically-typed
/// `creator`/`partitions` fields. Mirrors
/// `PartitionTableProvider::get_or_create_partition_provider` but operates on
/// them directly so callers don't need a back-reference to the provider.
async fn get_or_create_partition_provider(
    creator: &Arc<dyn runtime_table_partition::creator::PartitionCreator>,
    partitions: &Arc<tokio::sync::RwLock<HashMap<CompositePartitionKey, Partition>>>,
    partition_values: Vec<ScalarValue>,
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
        let read_guard = partitions.read().await;
        if let Some(partition) = read_guard.get(&partition_key) {
            return Ok(Arc::clone(&partition.table_provider));
        }
    }

    // Slow path: the partition is new. Acquire the write lock, but
    // double-check the map first — another writer may have created
    // the same partition while we waited for the lock.
    let mut partitions_lock = partitions.write().await;
    if let Some(partition) = partitions_lock.get(&partition_key) {
        return Ok(Arc::clone(&partition.table_provider));
    }
    let partition = creator
        .create_partition(partition_values)
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let provider = Arc::clone(&partition.table_provider);
    partitions_lock.insert(partition_key, partition);
    Ok(provider)
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
    schema: arrow_schema::SchemaRef,
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
                let sink = Arc::new(insert::CayennePartitionedOverwriteSink::new(
                    Arc::clone(&self.catalog),
                    Arc::clone(&self.coordinator_lock),
                    Arc::clone(&context.creator),
                    Arc::clone(&context.partitions),
                    Arc::clone(&context.schema),
                    physical_exprs,
                ));
                Ok(Arc::new(DataSinkExec::new(input, sink, None)))
            }
            InsertOp::Append | InsertOp::Replace => {
                let physical_exprs = create_partition_physical_exprs(
                    &context.partition_by,
                    Arc::clone(&context.schema),
                )?;
                let sink = Arc::new(append::CayennePartitionedAppendSink::new(
                    Arc::clone(&self.catalog),
                    Arc::clone(&self.coordinator_lock),
                    Arc::clone(&context.creator),
                    Arc::clone(&context.partitions),
                    Arc::clone(&context.schema),
                    physical_exprs,
                    self.table_root.clone(),
                ));
                Ok(Arc::new(DataSinkExec::new(input, sink, None)))
            }
        }
    }
}
