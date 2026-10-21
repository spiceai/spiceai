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

//! Service layer: stateful partition reconciliation.
//!
//! Computes a [`PartitionDiff`] by querying the source via the
//! [`PartitionOperations`] trait and diffing against the [`PartitionStore`],
//! then applies it: assigns unassigned partitions to connected executors and
//! notifies executors of load/unload events.
//!
//! Public entry points:
//! - [`PartitionService::seed_table`] – write the diff to the store only.
//!   Used at scheduler startup before any executors connect.
//! - [`PartitionService::reconcile_table`] – seed + assign + notify for one
//!   table, with no per-cycle assignment cap (pre-refresh / on-demand path).
//! - [`PartitionService::reconcile_all`] – seed + assign + notify for every
//!   accelerated table in the app, with the periodic per-cycle cap applied.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use app::App;
use datafusion::common::TableReference;
use futures::future::join_all;
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use runtime_proto::scheduler_control_message::Message as SchedulerControlMessageEnum;
use runtime_proto::{BytesArray, SchedulerControlMessage, UpdatePartitions};
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;
use tokio::sync::RwLock;
use tokio::time::timeout;

use util::fibonacci_backoff::FibonacciBackoffBuilder;

use crate::context::PartitionOperations;
use crate::executor_liveness::ExecutorLiveness;
use crate::executor_registry::{self, ExecutorRegistry};
use crate::metadata::{PartitionKey, partition_key};
use crate::metrics;
use crate::partition_load_tracker::PartitionLoadTracker;
use crate::scheduler_task_config::PartitionAssignmentConfig;
use crate::{AssignmentRequest, PartitionStore, PartitionValue, partition_value_to_bytes, store};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to refresh partition store: {source}"))]
    PartitionStoreRefresh { source: store::Error },

    #[snafu(display("Failed to list tables: {source}"))]
    ListTables { source: store::Error },

    #[snafu(display("Table metadata not found for {table}"))]
    TableMetadataNotFound { table: String },

    #[snafu(display("Failed to write metadata for {table}: {source}"))]
    WriteMetadata { table: String, source: store::Error },

    #[snafu(display("Failed to send command to executor {executor_id}: {source}"))]
    SendCommand {
        executor_id: String,
        source: executor_registry::Error,
    },

    #[snafu(display("Failed to convert partition value to bytes: {source}"))]
    PartitionValueConversion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Partition discovery failed for table {table}: {source}"))]
    DiscoveryFailed {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Partition discovery timed out for table {table}"))]
    DiscoveryTimeout { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration for partition assignment operations.
#[derive(Debug, Clone)]
pub struct AssignmentConfig {
    /// Maximum partitions to assign per cycle. Only applied to
    /// [`PartitionService::reconcile_all`] (periodic path). On-demand
    /// reconciliation ([`PartitionService::reconcile_table`]) is uncapped to
    /// avoid leaving newly-discovered partitions unassigned when the refresh
    /// is forwarded to executors.
    pub max_assignments_per_cycle: usize,
    /// Maximum partitions per executor (soft limit).
    pub max_partitions_per_executor: usize,
    /// How long to wait for partition discovery before timing out.
    pub discovery_timeout: Duration,
}

impl Default for AssignmentConfig {
    fn default() -> Self {
        Self {
            max_assignments_per_cycle: 100,
            max_partitions_per_executor: 1000,
            discovery_timeout: Duration::from_mins(1),
        }
    }
}

/// How many assignments [`assign_pending`] may commit in a single invocation.
#[derive(Debug, Clone, Copy)]
enum AssignmentLimit {
    /// No cap — assign every unassigned partition.
    Unlimited,
    /// Cap total assignments at `max_assignments_per_cycle` from
    /// [`AssignmentConfig`].
    PerCycleCap,
}

/// Difference between source partitions and what is tracked in the store.
#[derive(Debug, Default, Clone)]
struct PartitionDiff {
    /// Partitions present in the source but not yet tracked in the store.
    new: Vec<PartitionValue>,
    /// Partitions tracked in the store but no longer present in the source.
    removed: Vec<PartitionValue>,
}

impl PartitionDiff {
    fn is_empty(&self) -> bool {
        self.new.is_empty() && self.removed.is_empty()
    }
}

struct CycleState {
    executor_ids: Vec<String>,
    tables: Vec<String>,
}

struct UnassignedPartition {
    table: TableReference,
    partition_value: PartitionValue,
}

#[derive(Debug)]
struct Assignment {
    table: TableReference,
    partition_value: PartitionValue,
    executor_id: String,
}

#[derive(Default, Clone)]
struct ExecutorLoad {
    partition_count: usize,
    tables: HashSet<String>,
}

#[derive(Debug)]
struct CommitResult {
    /// The assignments that hold: made here, or already in place.
    committed: Vec<Assignment>,
    /// The assignments another scheduler made first with another executor.
    lost: Vec<Assignment>,
}

/// Outcome of a [`PartitionService::reconcile_all`] cycle, used by the periodic
/// scheduler task to decide whether to open the first-assignment gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// The cycle ran its assignment pass with at least one connected executor,
    /// so any unassigned partitions were distributed and the store now reflects
    /// each executor's authoritative share (which may legitimately be empty when
    /// there were no pending partitions). Only this outcome should open the
    /// `allocate_initial_partitions` gate.
    Assigned,
    /// The cycle never reached its assignment pass — either no accelerated
    /// partitioned tables exist, or no executors were connected during the
    /// cycle. Opening the gate here would hand a connecting executor a share
    /// before the scheduler has had a chance to distribute to it.
    NoAssignment,
}

/// Shared partition infrastructure for discovery and assignment operations.
///
/// Holds the [`PartitionStore`], executor registry, and app reference. Public methods are `seed_table`, `reconcile_table`,
/// and `reconcile_all`; everything else is an internal composition helper.
/// Methods take `&dyn PartitionOperations` to avoid a concrete `DataFusion` dependency.
pub struct PartitionService {
    pub partition_store: Arc<PartitionStore>,
    pub executor_registry: Arc<ExecutorRegistry>,
    pub app: Arc<RwLock<Option<Arc<App>>>>,
    /// Set once the scheduler has completed its first assignment cycle. Until
    /// then `allocate_initial_partitions` returns `Unavailable` so a connecting
    /// executor waits for its fair share rather than loading an empty snapshot —
    /// CDC/Changes-mode accelerations (e.g. Cayenne) only load partition data at
    /// their initial snapshot and have no re-load path once assigned later.
    first_assignment_complete: Arc<AtomicBool>,
    /// When each executor that holds partitions was first found connected to
    /// no live scheduler, if it still is.
    absent_since: parking_lot::Mutex<HashMap<String, tokio::time::Instant>>,
}

/// How long an executor that holds partitions may be connected to no live
/// scheduler before its partitions are given to other executors. Long enough
/// for an executor to restart under the same id and keep them.
pub const DEPARTED_EXECUTOR_GRACE: Duration = Duration::from_mins(2);

/// Clock difference between schedulers tolerated when judging how recent
/// another scheduler's view of its executors is.
const LIVENESS_CLOCK_SKEW: Duration = Duration::from_secs(5);

/// How long after an executor acks an assigned partition the partition is sent
/// again if the executor has not reported it loaded. The executor ignores a
/// partition it is still loading, replays the report of one it loaded, and
/// loads again one whose load failed.
pub const UNLOADED_RESEND_AFTER: Duration = Duration::from_secs(30);

impl PartitionService {
    #[must_use]
    pub fn new(
        partition_store: Arc<PartitionStore>,
        executor_registry: Arc<ExecutorRegistry>,
        app: Arc<RwLock<Option<Arc<App>>>>,
    ) -> Self {
        Self {
            partition_store,
            executor_registry,
            app,
            first_assignment_complete: Arc::new(AtomicBool::new(false)),
            absent_since: parking_lot::Mutex::default(),
        }
    }

    /// Whether the scheduler has completed its first assignment cycle.
    ///
    /// Gates `allocate_initial_partitions`: an executor waits until this is
    /// `true` so its initial snapshot loads its assigned partitions.
    #[must_use]
    pub fn is_first_assignment_complete(&self) -> bool {
        self.first_assignment_complete.load(Ordering::Acquire)
    }

    /// Mark the first assignment cycle as complete. Idempotent; called by the
    /// periodic assignment task after its first `reconcile_all` succeeds.
    pub fn mark_first_assignment_complete(&self) {
        self.first_assignment_complete
            .store(true, Ordering::Release);
    }

    fn config_from_app(app: &App) -> AssignmentConfig {
        let Some(scheduler) = app.runtime.resolved_scheduler() else {
            return AssignmentConfig::default();
        };
        match PartitionAssignmentConfig::try_from(scheduler) {
            Ok(pa_config) => AssignmentConfig {
                max_assignments_per_cycle: pa_config.max_assignments_per_interval,
                max_partitions_per_executor: pa_config.max_partitions_per_executor,
                discovery_timeout: pa_config.discovery_timeout,
            },
            Err(e) => {
                tracing::warn!(
                    "Invalid runtime.scheduler partition assignment config; using defaults: {e}"
                );
                AssignmentConfig::default()
            }
        }
    }

    // ================================================================
    // Public entry points
    // ================================================================

    /// Seed the store with current source partitions for a single table.
    ///
    /// Writes new partitions as unassigned and removes stale ones; does **not**
    /// assign to executors or send notifications. Used at scheduler startup
    /// before executors have connected.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition store refresh, source discovery, or metadata write fails.
    pub async fn seed_table(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        let config = {
            let app = self.app.read().await.clone();
            app.as_deref()
                .map_or_else(AssignmentConfig::default, Self::config_from_app)
        };
        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;
        self.record_diff(table, partition_by, ops, &config).await?;
        Ok(())
    }

    /// Discover new/removed partitions for a single table, add/remove them in
    /// the store, and assign + notify executors for everything pending on that
    /// table. Uncapped — assigns every pending partition before returning.
    ///
    /// Used by the on-demand refresh path: a `spice refresh` command calls
    /// this before forwarding the refresh to executors, ensuring any new
    /// partition values are assigned first.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition store refresh, source discovery, metadata write, or executor notification fails.
    pub async fn reconcile_table(
        &self,
        table: &TableReference,
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        let Some(app) = self.app.read().await.clone() else {
            return Ok(());
        };
        let Some(partition_by) = get_partition_config(&app, table) else {
            return Ok(());
        };
        let config = Self::config_from_app(&app);

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        self.record_diff(table, &partition_by, ops, &config).await?;

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let executors = self.executor_registry.connected_executors().await;
        if executors.is_empty() {
            tracing::warn!(table = %table, "No executors connected, cannot assign new partitions");
            self.record_partitions_count_for_table(table);
            return Ok(());
        }

        let result = self
            .assign_pending(
                std::slice::from_ref(table),
                &executors,
                ops,
                AssignmentLimit::Unlimited,
                &config,
            )
            .await;
        self.record_partitions_count_for_table(table);
        result
    }

    /// Discover, add/remove, and assign partitions for every accelerated table
    /// declared in the app. The per-cycle assignment cap from
    /// [`AssignmentConfig::max_assignments_per_cycle`] is applied so that a
    /// slow cycle can't saturate the cluster in one tick.
    ///
    /// Used by the periodic partition-management background task.
    ///
    /// Returns [`ReconcileOutcome::Assigned`] only when partitions were actually
    /// distributed to connected executors this cycle; every no-op path (no app,
    /// no partitioned tables, no connected executors) returns
    /// [`ReconcileOutcome::NoAssignment`] so the caller does not prematurely open
    /// the first-assignment gate.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition store refresh or executor notification fails.
    /// Per-table diff failures are logged and skipped rather than propagated.
    pub async fn reconcile_all(&self, ops: &dyn PartitionOperations) -> Result<ReconcileOutcome> {
        let Some(app) = self.app.read().await.clone() else {
            tracing::warn!("App not initialized, skipping partition discovery");
            return Ok(ReconcileOutcome::NoAssignment);
        };
        let config = Self::config_from_app(&app);

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let tables = partitioned_tables(&app);

        let mut reconciled_tables: Vec<TableReference> = Vec::new();
        for (table, partition_by) in tables {
            match self.record_diff(&table, &partition_by, ops, &config).await {
                Ok(_) => reconciled_tables.push(table),
                Err(e) => {
                    tracing::warn!(table = %table, error = %e, "Per-table diff failed, continuing");
                }
            }
        }

        if reconciled_tables.is_empty() {
            return Ok(ReconcileOutcome::NoAssignment);
        }

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let executors = self.executor_registry.connected_executors().await;
        if executors.is_empty() {
            for table in &reconciled_tables {
                self.record_partitions_count_for_table(table);
            }
            return Ok(ReconcileOutcome::NoAssignment);
        }

        let result = self
            .assign_pending(
                &reconciled_tables,
                &executors,
                ops,
                AssignmentLimit::PerCycleCap,
                &config,
            )
            .await;
        for table in &reconciled_tables {
            self.record_partitions_count_for_table(table);
        }
        result.map(|()| ReconcileOutcome::Assigned)
    }

    /// Step 1: query the source via `ops`, diff against the store, and apply the diff.
    ///
    /// - Queries the source for current partition values.
    /// - Diffs against store metadata (new = in source but not store; removed = in store but not source).
    /// - When metadata is absent, treats all source partitions as new.
    /// - Writes the table's entry, the new partitions (unassigned) and the
    ///   removal of stale ones in one OCC write, then emits unload
    ///   notifications to executors that held the removed partitions.
    ///
    /// Returns the computed diff for logging/metrics in the caller.
    async fn record_diff(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
        ops: &dyn PartitionOperations,
        config: &AssignmentConfig,
    ) -> Result<PartitionDiff> {
        // Query source partitions via the trait (with timeout).
        let discovery_start = std::time::Instant::now();
        let discovery_result = timeout(
            config.discovery_timeout,
            ops.table_partition_values(table, partition_by),
        )
        .await;
        if let Some(node_id) = self.executor_registry.node_id() {
            let duration_ms = discovery_start.elapsed().as_secs_f64() * 1000.0;
            metrics::record_partition_discovery_duration(
                node_id,
                &dataset_label(table),
                duration_ms,
            );
        }
        let source_partitions = match discovery_result {
            Ok(Ok(partitions)) => partitions,
            Ok(Err(e)) => {
                return Err(Error::DiscoveryFailed {
                    table: table.to_string(),
                    source: e,
                });
            }
            Err(_) => {
                return Err(Error::DiscoveryTimeout {
                    table: table.to_string(),
                });
            }
        };

        // Diff against store metadata.
        let stored = self.partition_store.get_table_metadata(table).await;
        let has_entry = matches!(stored, Ok(Some(_)));
        let existing_partitions: Vec<crate::PartitionMetadata> = match stored {
            Ok(Some(m)) => m.partitions,
            Ok(None) => {
                tracing::debug!(
                    table = %table,
                    count = source_partitions.len(),
                    "No partition metadata found, treating all source partitions as new"
                );
                Vec::new()
            }
            Err(e) => {
                return Err(Error::DiscoveryFailed {
                    table: table.to_string(),
                    source: Box::new(e),
                });
            }
        };

        let existing: HashSet<PartitionKey> = existing_partitions
            .iter()
            .map(|p| partition_key(&p.partition_value))
            .collect();

        let source_set: HashSet<PartitionKey> =
            source_partitions.iter().map(partition_key).collect();

        let new: Vec<PartitionValue> = source_partitions
            .iter()
            .filter(|p| !existing.contains(&partition_key(p)))
            .cloned()
            .collect();

        let removed: Vec<PartitionValue> = existing_partitions
            .iter()
            .filter(|p| !source_set.contains(&partition_key(&p.partition_value)))
            .map(|p| p.partition_value.clone())
            .collect();

        let diff = PartitionDiff { new, removed };

        if has_entry && diff.is_empty() {
            return Ok(diff);
        }

        // One write publishes the discovered set: the entry (with
        // `updated_at > 0` even when the source has no partitions, which the
        // readiness shortcut in `PartitionLoadTracker::is_table_loaded` needs),
        // the new partitions and the removal of stale ones. A query never sees
        // an entry that lists only part of the source's partitions.
        let partition_expressions: Vec<String> =
            partition_by.iter().map(|p| p.expression.clone()).collect();
        let changes = self
            .partition_store
            .apply_discovery(table, &partition_expressions, &diff.new, &diff.removed)
            .await
            .context(WriteMetadataSnafu {
                table: table.to_string(),
            })?;

        if changes.added > 0 {
            tracing::info!(table = %table, count = changes.added, "Added new partitions");
            if let Some(node_id) = self.executor_registry.node_id() {
                metrics::record_partition_state_operation(
                    node_id,
                    metrics::PartitionStateOperation::Added,
                    changes.added as u64,
                );
            }
        }

        if changes.removed_count > 0 {
            tracing::info!(table = %table, count = changes.removed_count, "Removed stale partitions");
            for (executor_id, partitions) in changes.removed {
                if let Err(e) = notify_executor_to_unload(
                    &self.executor_registry,
                    ops,
                    &executor_id,
                    table,
                    partitions,
                )
                .await
                {
                    tracing::warn!(executor_id = %executor_id, table = %table, error = %e, "Failed to notify executor to unload partitions");
                }
            }
            if let Some(node_id) = self.executor_registry.node_id() {
                metrics::record_partition_state_operation(
                    node_id,
                    metrics::PartitionStateOperation::Removed,
                    changes.removed_count as u64,
                );
            }
        }

        Ok(diff)
    }

    /// Refresh and emit the `scheduler_partitions_count` gauge for `table`
    /// based on the current state of the partition store. No-op when the
    /// scheduler has no `node_id` configured for metrics.
    fn record_partitions_count_for_table(&self, table: &TableReference) {
        let Some(node_id) = self.executor_registry.node_id() else {
            return;
        };
        let Some(metadata) = self.partition_store.get_cached_table_metadata(table) else {
            return;
        };
        let (assigned, unassigned) =
            metadata
                .partitions
                .iter()
                .fold((0u64, 0u64), |(a, u), part| {
                    if part.assigned_executors.is_empty() {
                        (a, u + 1)
                    } else {
                        (a + 1, u)
                    }
                });
        metrics::set_scheduler_partitions_count(
            node_id,
            &dataset_label(table),
            assigned,
            unassigned,
        );
    }

    /// Step 2: find every unassigned partition across the given tables, assign
    /// them to connected executors, commit the assignments, and notify the
    /// executors of the new partitions.
    async fn assign_pending(
        &self,
        tables: &[TableReference],
        executors: &[String],
        ops: &dyn PartitionOperations,
        limit: AssignmentLimit,
        config: &AssignmentConfig,
    ) -> Result<()> {
        let state = CycleState {
            executor_ids: executors.to_vec(),
            tables: tables.iter().map(ToString::to_string).collect(),
        };

        let unassigned = find_unassigned_partitions(&self.partition_store, &state);
        if unassigned.is_empty() {
            return Ok(());
        }

        let assignments =
            assign_unassigned_partitions(unassigned, &state, &self.partition_store, config, limit);
        let CommitResult { committed, lost } = commit_assignments(
            &self.partition_store,
            assignments,
            self.executor_registry.node_id(),
        )
        .await?;
        if !lost.is_empty() {
            tracing::debug!(
                "{} partitions were assigned by another scheduler first",
                lost.len()
            );
        }
        notify_executors(&self.executor_registry, ops, committed).await
    }

    /// Publishes the executors this scheduler has connected, for other
    /// schedulers' [`Self::live_executors`].
    pub async fn publish_connected_executors(&self) {
        let Some(scheduler_id) = self.executor_registry.node_id() else {
            return;
        };
        let Ok(now_ms) = crate::cluster_state::now_ms() else {
            return;
        };
        let connected = self.executor_registry.connected_executors().await;
        if let Err(err) = ExecutorLiveness::new(self.partition_store.cluster_state())
            .publish(scheduler_id, connected, now_ms)
            .await
        {
            tracing::warn!("Failed to publish this scheduler's connected executors: {err}");
        }
    }

    /// Returns every executor this scheduler has connected or that a scheduler
    /// that published within three `interval`s has connected. Returns `None`
    /// when the other schedulers' views cannot be read: this scheduler's view
    /// alone cannot tell an executor that left from one it cannot reach.
    pub async fn live_executors(&self, interval: Duration) -> Option<HashSet<String>> {
        let connected = self.executor_registry.connected_executors().await;
        if self.executor_registry.node_id().is_none() {
            return Some(connected.into_iter().collect());
        }
        let now_ms = crate::cluster_state::now_ms().ok()?;
        match ExecutorLiveness::new(self.partition_store.cluster_state())
            .live_executors(now_ms, interval * 3 + LIVENESS_CLOCK_SKEW)
            .await
        {
            Ok(mut live) => {
                live.extend(connected);
                Some(live)
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to read the executors other schedulers have connected; not reassigning partitions this cycle: {err}"
                );
                None
            }
        }
    }

    /// Unassigns the partitions of each executor that has been absent from
    /// `live` for [`DEPARTED_EXECUTOR_GRACE`], so the next assignment pass gives
    /// them to executors that are connected. `live` holds every executor a live
    /// scheduler has connected. Returns how many partitions were unassigned.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition store refresh or write fails.
    pub async fn reassign_departed(&self, live: &HashSet<String>) -> Result<usize> {
        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;
        let tables = self.partition_store.list_tables().await.unwrap_or_default();
        let holders: HashSet<String> = tables
            .iter()
            .filter_map(|table| {
                self.partition_store
                    .get_cached_table_metadata(&TableReference::parse_str(table))
            })
            .flat_map(|metadata| {
                metadata
                    .partitions
                    .into_iter()
                    .flat_map(|p| p.assigned_executors)
            })
            .collect();

        let now = tokio::time::Instant::now();
        let departed: Vec<String> = {
            let mut absent_since = self.absent_since.lock();
            absent_since.retain(|id, _| holders.contains(id) && !live.contains(id));
            holders
                .iter()
                .filter(|id| !live.contains(*id))
                .filter(|id| {
                    let since = *absent_since.entry((*id).clone()).or_insert(now);
                    now.duration_since(since) >= DEPARTED_EXECUTOR_GRACE
                })
                .cloned()
                .collect()
        };

        let mut unassigned = 0;
        for executor_id in departed {
            let count = self
                .partition_store
                .unassign_executor(&executor_id)
                .await
                .context(WriteMetadataSnafu {
                    table: format!("<partitions of executor {executor_id}>"),
                })?;
            tracing::warn!(
                executor = %executor_id,
                partitions = count,
                "Executor has been connected to no scheduler for {DEPARTED_EXECUTOR_GRACE:?}; reassigning its partitions"
            );
            self.absent_since.lock().remove(&executor_id);
            unassigned += count;
        }
        Ok(unassigned)
    }

    /// Brings each connected executor's partitions in line with its
    /// assignment: sends it the partitions it is assigned but has not reported
    /// loaded, unless it acked them within [`UNLOADED_RESEND_AFTER`], and the
    /// removal of those it reported loaded or acked but is no longer assigned.
    /// An assignment whose notification was lost reaches the executor on the
    /// next call; one whose load failed or whose report was lost, once the
    /// wait has passed.
    ///
    /// # Errors
    ///
    /// Returns an error if a partition value cannot be encoded.
    pub async fn resync_executors(
        &self,
        ops: &dyn PartitionOperations,
        tracker: &PartitionLoadTracker,
    ) -> Result<()> {
        let executors = self.executor_registry.connected_executors().await;
        if executors.is_empty() {
            return Ok(());
        }
        let Some(app) = self.app.read().await.clone() else {
            return Ok(());
        };
        for (table, _) in partitioned_tables(&app) {
            let Some(metadata) = self.partition_store.get_cached_table_metadata(&table) else {
                continue;
            };
            let mut assigned: HashMap<String, HashSet<bytes::Bytes>> = HashMap::new();
            for partition in &metadata.partitions {
                if !partition.is_assigned() {
                    continue;
                }
                let bytes =
                    partition_value_to_bytes(partition.partition_value.clone(), &table, ops)
                        .await
                        .context(PartitionValueConversionSnafu)?;
                for executor_id in &partition.assigned_executors {
                    assigned
                        .entry(executor_id.clone())
                        .or_default()
                        .insert(bytes.clone());
                }
            }
            let acks = tracker.acks(&canonical_table(&table)).await;
            let table_name = table.to_string();
            for executor_id in &executors {
                let assigned = assigned.remove(executor_id).unwrap_or_default();
                let process = self.executor_registry.current_process(executor_id);
                let loaded = acks
                    .get(executor_id)
                    .filter(|ack| ack.process_id == process)
                    .map(|ack| ack.partitions.clone())
                    .unwrap_or_default();
                let delivered = self
                    .executor_registry
                    .delivered_partitions(executor_id, &table_name)
                    .await;
                let now = tokio::time::Instant::now();
                let added: Vec<bytes::Bytes> = assigned
                    .iter()
                    .filter(|bytes| {
                        !loaded.contains(*bytes)
                            && delivered.get(*bytes).is_none_or(|acked_at| {
                                now.duration_since(*acked_at) >= UNLOADED_RESEND_AFTER
                            })
                    })
                    .cloned()
                    .collect();
                let removed: Vec<bytes::Bytes> = loaded
                    .iter()
                    .chain(delivered.keys())
                    .filter(|bytes| !assigned.contains(*bytes))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .cloned()
                    .collect();
                if added.is_empty() && removed.is_empty() {
                    continue;
                }
                match self
                    .executor_registry
                    .send_partition_update(
                        executor_id,
                        &table_name,
                        &added,
                        &removed,
                        NOTIFY_ACK_TIMEOUT,
                    )
                    .await
                {
                    Ok(()) => tracing::info!(
                        executor = %executor_id,
                        table = %table,
                        added = added.len(),
                        removed = removed.len(),
                        "Re-sent partition assignment changes the executor had not applied"
                    ),
                    Err(err) => tracing::warn!(
                        executor = %executor_id,
                        table = %table,
                        error = %err,
                        "Failed to re-send partition assignment changes; retrying next cycle"
                    ),
                }
            }
        }
        Ok(())
    }
}

/// `table` resolved against the Spice defaults, the form executor reports are
/// recorded under.
fn canonical_table(table: &TableReference) -> TableReference {
    let resolved = table
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
    TableReference::full(resolved.catalog, resolved.schema, resolved.table)
}

/// The accelerated datasets and views of `app` that are partitioned, with their
/// `partition_by`.
fn partitioned_tables(app: &App) -> Vec<(TableReference, Vec<PartitionedBy>)> {
    let datasets = app.datasets.iter().filter_map(|ds| {
        let acc = ds.acceleration.as_ref()?;
        (acc.enabled && !acc.partition_by.is_empty()).then(|| {
            (
                TableReference::parse_str(&ds.name),
                acc.partition_by.clone(),
            )
        })
    });
    let views = app.views.iter().filter_map(|view| {
        let acc = view.acceleration.as_ref()?;
        (acc.enabled && !acc.partition_by.is_empty()).then(|| {
            (
                TableReference::parse_str(&view.name),
                acc.partition_by.clone(),
            )
        })
    });
    datasets.chain(views).collect()
}

/// Returns the `partition_by` config for a table from the App definition.
///
/// Searches both datasets and views for a matching table reference.
#[must_use]
pub fn get_partition_config(
    app: &app::App,
    table: &TableReference,
) -> Option<Vec<spicepod::partitioning::PartitionedBy>> {
    let acceleration = app
        .datasets
        .iter()
        .find(|d| resolved_equality(&d.name.clone().into(), table))
        .and_then(|d| d.acceleration.as_ref())
        .or_else(|| {
            app.views
                .iter()
                .find(|v| resolved_equality(&v.name.clone().into(), table))
                .and_then(|v| v.acceleration.as_ref())
        });

    acceleration
        .map(|a| a.partition_by.clone())
        .filter(|pb| !pb.is_empty())
}

fn resolved_equality(a: &TableReference, b: &TableReference) -> bool {
    a.clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
        == b.clone()
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
}

/// Build the `dataset` label for scheduler-side metrics as `schema.table`.
///
/// Resolves bare references against the Spice defaults so a Spicepod dataset
/// declared as `name: hits` and one declared as `name: public.hits` share the
/// same series. Matches the executor-side normalization in
/// `record_executor_assigned_partitions` so scheduler and executor metrics
/// can be joined by `dataset`.
fn dataset_label(table: &TableReference) -> String {
    let resolved = table
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
    format!("{}.{}", resolved.schema, resolved.table)
}

async fn notify_executor_to_unload(
    executor_registry: &ExecutorRegistry,
    ops: &dyn PartitionOperations,
    executor_id: &str,
    table: &TableReference,
    partitions: Vec<PartitionValue>,
) -> Result<()> {
    let mut partitions_bytes = Vec::new();
    for p in partitions {
        let bytes = partition_value_to_bytes(p, table, ops)
            .await
            .context(PartitionValueConversionSnafu)?;
        partitions_bytes.push(bytes.to_vec());
    }

    executor_registry
        .send_command(
            executor_id,
            SchedulerControlMessage {
                message: Some(SchedulerControlMessageEnum::UpdatePartitions(
                    UpdatePartitions {
                        new_partitions: HashMap::new(),
                        removed_partitions: HashMap::from([(
                            table.to_string(),
                            BytesArray {
                                items: partitions_bytes,
                            },
                        )]),
                        // Empty == no ack requested. This call site is the
                        // unload path; switch to ack-based in a follow-up.
                        request_id: String::new(),
                    },
                )),
            },
        )
        .await
        .context(SendCommandSnafu {
            executor_id: executor_id.to_string(),
        })?;

    Ok(())
}

fn find_unassigned_partitions(
    partition_store: &PartitionStore,
    state: &CycleState,
) -> Vec<UnassignedPartition> {
    let mut unassigned = Vec::new();

    for table_name in &state.tables {
        let table_ref = TableReference::parse_str(table_name);
        let Some(metadata) = partition_store.get_cached_table_metadata(&table_ref) else {
            continue;
        };

        for partition in metadata.unassigned_partitions() {
            unassigned.push(UnassignedPartition {
                table: table_ref.clone(),
                partition_value: partition.partition_value.clone(),
            });
        }
    }

    if !unassigned.is_empty() {
        tracing::info!(
            unassigned_count = unassigned.len(),
            "Found unassigned partitions"
        );
    }

    unassigned
}

fn assign_unassigned_partitions(
    unassigned: Vec<UnassignedPartition>,
    state: &CycleState,
    partition_store: &PartitionStore,
    config: &AssignmentConfig,
    limit: AssignmentLimit,
) -> Vec<Assignment> {
    if unassigned.is_empty() {
        return Vec::new();
    }

    let cap = match limit {
        AssignmentLimit::Unlimited => usize::MAX,
        AssignmentLimit::PerCycleCap => config.max_assignments_per_cycle,
    };

    let mut assignments = Vec::new();
    let mut assignments_this_cycle = 0;
    let mut executor_loads = build_executor_loads(state, partition_store);

    for unassigned_partition in unassigned {
        if assignments_this_cycle >= cap {
            tracing::debug!(
                max_assignments = cap,
                "Reached max assignments per cycle, deferring remaining partitions"
            );
            break;
        }

        let Some(executor_id) =
            select_executor_for_partition(&unassigned_partition, &executor_loads, state, config)
        else {
            tracing::warn!(
                table = %unassigned_partition.table,
                partition = ?unassigned_partition.partition_value,
                "No suitable executor found for partition"
            );
            continue;
        };

        assignments.push(Assignment {
            table: unassigned_partition.table.clone(),
            partition_value: unassigned_partition.partition_value.clone(),
            executor_id: executor_id.clone(),
        });

        executor_loads
            .entry(executor_id)
            .or_default()
            .partition_count += 1;

        assignments_this_cycle += 1;
    }

    tracing::info!(
        assignments_count = assignments.len(),
        "Generated partition assignments"
    );
    assignments
}

fn build_executor_loads(
    state: &CycleState,
    partition_store: &PartitionStore,
) -> HashMap<String, ExecutorLoad> {
    let mut loads = HashMap::new();

    for executor_id in &state.executor_ids {
        loads.insert(executor_id.clone(), ExecutorLoad::default());
    }

    for table_name in &state.tables {
        let table_ref = TableReference::parse_str(table_name);
        if let Some(metadata) = partition_store.get_cached_table_metadata(&table_ref) {
            for partition in &metadata.partitions {
                for executor_id in &partition.assigned_executors {
                    let load = loads.entry(executor_id.clone()).or_default();
                    load.partition_count += 1;
                    load.tables.insert(table_name.clone());
                }
            }
        }
    }

    loads
}

fn select_executor_for_partition(
    partition: &UnassignedPartition,
    executor_loads: &HashMap<String, ExecutorLoad>,
    state: &CycleState,
    config: &AssignmentConfig,
) -> Option<String> {
    let mut candidates: Vec<_> = state.executor_ids.iter().collect();

    if candidates.is_empty() {
        return None;
    }

    candidates.retain(|executor_id| {
        let load = executor_loads
            .get(*executor_id)
            .map_or(0, |l| l.partition_count);
        load < config.max_partitions_per_executor
    });

    if candidates.is_empty() {
        tracing::warn!("All executors at capacity");
        return None;
    }

    let mut scored_candidates: Vec<_> = candidates
        .into_iter()
        .map(|executor_id| {
            let score =
                score_executor_for_partition(executor_id, partition, executor_loads, config);
            (executor_id, score)
        })
        .collect();

    scored_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    scored_candidates
        .first()
        .map(|(executor, _)| (*executor).clone())
}

#[expect(clippy::cast_precision_loss)]
fn score_executor_for_partition(
    executor_id: &str,
    partition: &UnassignedPartition,
    executor_loads: &HashMap<String, ExecutorLoad>,
    config: &AssignmentConfig,
) -> f64 {
    let load = executor_loads.get(executor_id).cloned().unwrap_or_default();

    let mut score = 100.0;

    // Data locality
    let table_name = partition.table.to_string();
    if load.tables.contains(&table_name) {
        score += 50.0;
    }

    // Load balancing
    let load_factor = load.partition_count as f64 / config.max_partitions_per_executor as f64;
    score -= load_factor * 40.0;

    score.max(0.0)
}

/// Commits `assignments` in one write. A partition another scheduler assigned
/// first keeps its executor, and only the assignments that hold are notified.
async fn commit_assignments(
    partition_store: &PartitionStore,
    assignments: Vec<Assignment>,
    node_id: Option<&str>,
) -> Result<CommitResult> {
    if assignments.is_empty() {
        return Ok(CommitResult {
            committed: Vec::new(),
            lost: Vec::new(),
        });
    }
    let requests: Vec<AssignmentRequest> = assignments
        .iter()
        .map(|assignment| AssignmentRequest {
            table: assignment.table.clone(),
            partition_value: assignment.partition_value.clone(),
            executor_id: assignment.executor_id.clone(),
        })
        .collect();
    let held = partition_store
        .assign_unassigned(&requests)
        .await
        .context(WriteMetadataSnafu {
            table: "<assignment batch>".to_string(),
        })?;
    let held: HashSet<(String, PartitionKey, String)> = held
        .iter()
        .map(|a| {
            (
                crate::normalized_table_name(&a.table),
                partition_key(&a.partition_value),
                a.executor_id.clone(),
            )
        })
        .collect();

    let (committed, lost): (Vec<Assignment>, Vec<Assignment>) =
        assignments.into_iter().partition(|assignment| {
            held.contains(&(
                crate::normalized_table_name(&assignment.table),
                partition_key(&assignment.partition_value),
                assignment.executor_id.clone(),
            ))
        });
    for assignment in &committed {
        tracing::debug!(
            table = %assignment.table,
            partition = ?assignment.partition_value,
            executor = %assignment.executor_id,
            "Partition assigned"
        );
        if let Some(node_id) = node_id {
            metrics::record_partition_assignment(
                node_id,
                &assignment.executor_id,
                metrics::AssignmentStatus::Committed,
            );
        }
    }
    if !committed.is_empty() || !lost.is_empty() {
        tracing::info!(
            committed_count = committed.len(),
            lost_count = lost.len(),
            "Committed partition assignments"
        );
    }
    Ok(CommitResult { committed, lost })
}

async fn notify_executors(
    executor_registry: &ExecutorRegistry,
    ops: &dyn PartitionOperations,
    committed: Vec<Assignment>,
) -> Result<()> {
    let mut by_executor: HashMap<String, Vec<Assignment>> = HashMap::new();
    for assignment in committed {
        by_executor
            .entry(assignment.executor_id.clone())
            .or_default()
            .push(assignment);
    }

    let notifications: Vec<_> =
        by_executor
            .into_iter()
            .map(|(executor_id, assignments)| {
                let registry = executor_registry;
                async move {
                    notify_executor_of_assignments(registry, ops, &executor_id, assignments).await
                }
            })
            .collect();

    let results = join_all(notifications).await;

    let mut success_count = 0;
    let mut failure_count = 0;
    for result in results {
        match result {
            Ok(()) => success_count += 1,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to notify executor");
                failure_count += 1;
            }
        }
    }

    if success_count + failure_count > 0 {
        tracing::info!(
            success_count,
            failure_count,
            "Notified executors of partition assignments"
        );
    }

    Ok(())
}

/// How long the scheduler waits for an executor to ack a single
/// `UpdatePartitions` message before considering the attempt failed.
const NOTIFY_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on the per-attempt backoff between notify retries.
const NOTIFY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Maximum number of retry attempts for a single executor notification.
/// After exhaustion the assignment stays committed in the partition store and
/// [`PartitionService::resync_executors`] re-sends it.
const NOTIFY_MAX_RETRIES: usize = 8;

async fn notify_executor_of_assignments(
    registry: &ExecutorRegistry,
    ops: &dyn PartitionOperations,
    executor_id: &str,
    assignments: Vec<Assignment>,
) -> Result<()> {
    let mut by_table: HashMap<TableReference, Vec<PartitionValue>> = HashMap::new();
    for assignment in assignments {
        by_table
            .entry(assignment.table)
            .or_default()
            .push(assignment.partition_value);
    }

    for (table, partition_values) in by_table {
        let mut partitions_bytes = Vec::new();
        for p in partition_values {
            let bytes = partition_value_to_bytes(p, &table, ops)
                .await
                .context(PartitionValueConversionSnafu)?;
            partitions_bytes.push(bytes);
        }

        let mut backoff = FibonacciBackoffBuilder::new()
            .max_duration(Some(NOTIFY_BACKOFF_MAX))
            .max_retries(Some(NOTIFY_MAX_RETRIES))
            .build();
        let table_str = table.to_string();

        loop {
            let send_result = registry
                .send_partition_update(
                    executor_id,
                    &table_str,
                    &partitions_bytes,
                    &[],
                    NOTIFY_ACK_TIMEOUT,
                )
                .await;

            match send_result {
                Ok(()) => break,
                Err(err) if err.is_retryable() => {
                    let Some(delay) = backoff.next_duration() else {
                        tracing::warn!(
                            executor = %executor_id,
                            table = %table,
                            error = %err,
                            "Exhausted retries notifying executor of partition assignments; \
                             the next assignment cycle re-sends them"
                        );
                        return Err(err).context(SendCommandSnafu {
                            executor_id: executor_id.to_string(),
                        });
                    };
                    tracing::debug!(
                        executor = %executor_id,
                        table = %table,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        "Executor ack failed for partition update; retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => {
                    return Err(err).context(SendCommandSnafu {
                        executor_id: executor_id.to_string(),
                    });
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use crate::PartitionMetadata;
    use crate::cluster_state::ClusterStateStore;

    use super::*;

    async fn make_store() -> Arc<PartitionStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        Arc::new(PartitionStore::accelerations(cs))
    }

    fn pv(key: &str, val: &str) -> PartitionValue {
        HashMap::from([(key.to_string(), Some(val.to_string()))])
    }

    async fn setup_table(store: &PartitionStore, table: &str, partitions: Vec<PartitionMetadata>) {
        let table_ref = TableReference::parse_str(table);
        store
            .initialize_metadata(&table_ref, vec!["date".to_string()])
            .await
            .expect("init");
        let metadata = crate::metadata::TablePartitionMetadata {
            table_name: table.to_string(),
            partitions,
            schema_version: 1,
            updated_at: 1000,
            partition_expressions: vec!["date".to_string()],
            unregistered_at_ms: None,
        };
        store
            .write_metadata(&table_ref, metadata)
            .await
            .expect("write");
        store.refresh().await.expect("refresh");
    }

    fn assigned_partition(key: &str, val: &str, executor: &str) -> PartitionMetadata {
        let mut p = PartitionMetadata::new(pv(key, val));
        p.assign_to(executor.to_string(), 1000);
        p
    }

    fn unassigned_partition(key: &str, val: &str) -> PartitionMetadata {
        PartitionMetadata::new(pv(key, val))
    }

    /// `PartitionOperations` stub whose discovery always returns zero
    /// partition values, modelling an empty source table.
    struct EmptySourceOps;

    #[async_trait::async_trait]
    impl crate::context::PartitionExprResolver for EmptySourceOps {
        async fn try_parse_expr(
            &self,
            _tbl: &TableReference,
            expr: &str,
        ) -> std::result::Result<::datafusion_expr::Expr, datafusion::error::DataFusionError>
        {
            Ok(::datafusion_expr::col(expr))
        }
    }

    #[async_trait::async_trait]
    impl crate::context::PartitionDiscoverer for EmptySourceOps {
        async fn table_partition_values(
            &self,
            _table: &TableReference,
            _partition_by: &[PartitionedBy],
        ) -> std::result::Result<Vec<PartitionValue>, Box<dyn std::error::Error + Send + Sync>>
        {
            Ok(Vec::new())
        }
    }

    /// Seeding a table whose source has zero partitions must still create a
    /// metadata entry with `updated_at > 0` — readiness for zero-partition
    /// tables is gated on the `updated_at > 0` shortcut in
    /// `PartitionLoadTracker::is_table_loaded`, which can never trip if the
    /// entry is only written once partitions are discovered (#11152).
    #[tokio::test]
    async fn seed_table_with_empty_source_writes_metadata_entry() {
        let store = make_store().await;
        let registry = Arc::new(ExecutorRegistry::new(
            Arc::clone(&store),
            make_store().await,
        ));
        let service = PartitionService::new(
            Arc::clone(&store),
            registry,
            Arc::new(tokio::sync::RwLock::new(None)),
        );

        let table = TableReference::parse_str("empty_table");
        let partition_by = vec![PartitionedBy {
            name: "r_regionkey".to_string(),
            expression: "r_regionkey".to_string(),
        }];

        service
            .seed_table(&table, &partition_by, &EmptySourceOps)
            .await
            .expect("seed_table");

        store.refresh().await.expect("refresh");
        let metadata = store
            .get_cached_table_metadata(&table)
            .expect("metadata entry must exist for zero-partition table");
        assert!(metadata.partitions.is_empty());
        assert!(
            metadata.updated_at > 0,
            "updated_at must be stamped so the readiness shortcut can trip"
        );
    }

    #[tokio::test]
    async fn test_find_unassigned_partitions() {
        let store = make_store().await;
        setup_table(
            &store,
            "test_table",
            vec![
                assigned_partition("date", "2024-01-01", "exec1"),
                unassigned_partition("date", "2024-01-02"),
                unassigned_partition("date", "2024-01-03"),
            ],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string()],
            tables: vec!["test_table".to_string()],
        };

        let unassigned = find_unassigned_partitions(&store, &state);
        assert_eq!(unassigned.len(), 2);
    }

    #[tokio::test]
    async fn test_build_executor_loads() {
        let store = make_store().await;
        setup_table(
            &store,
            "table_a",
            vec![
                assigned_partition("date", "2024-01-01", "exec1"),
                assigned_partition("date", "2024-01-02", "exec1"),
                assigned_partition("date", "2024-01-03", "exec2"),
            ],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string(), "exec2".to_string()],
            tables: vec!["table_a".to_string()],
        };

        let loads = build_executor_loads(&state, &store);
        assert_eq!(loads["exec1"].partition_count, 2);
        assert_eq!(loads["exec2"].partition_count, 1);
        assert!(loads["exec1"].tables.contains("table_a"));
    }

    #[tokio::test]
    async fn test_select_executor_prefers_locality() {
        let store = make_store().await;
        setup_table(
            &store,
            "orders",
            vec![assigned_partition("date", "2024-01-01", "exec1")],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string(), "exec2".to_string()],
            tables: vec!["orders".to_string()],
        };
        let config = AssignmentConfig::default();
        let loads = build_executor_loads(&state, &store);

        let partition = UnassignedPartition {
            table: TableReference::parse_str("orders"),
            partition_value: pv("date", "2024-01-02"),
        };

        // exec1 already has partitions for "orders" → locality bonus → preferred
        let selected = select_executor_for_partition(&partition, &loads, &state, &config);
        assert_eq!(selected, Some("exec1".to_string()));
    }

    #[tokio::test]
    async fn test_select_executor_balances_load() {
        let store = make_store().await;
        setup_table(
            &store,
            "table_a",
            vec![
                assigned_partition("date", "1", "exec1"),
                assigned_partition("date", "2", "exec1"),
                assigned_partition("date", "3", "exec1"),
                assigned_partition("date", "4", "exec1"),
                assigned_partition("date", "5", "exec1"),
            ],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string(), "exec2".to_string()],
            tables: vec!["table_a".to_string()],
        };
        let config = AssignmentConfig {
            max_partitions_per_executor: 10,
            ..Default::default()
        };
        let loads = build_executor_loads(&state, &store);

        // New partition for a different table — no locality bonus
        let partition = UnassignedPartition {
            table: TableReference::parse_str("table_b"),
            partition_value: pv("date", "2024-01-01"),
        };

        // exec2 has lower load → preferred
        let selected = select_executor_for_partition(&partition, &loads, &state, &config);
        assert_eq!(selected, Some("exec2".to_string()));
    }

    #[test]
    fn test_select_executor_respects_capacity() {
        let mut loads = HashMap::new();
        loads.insert(
            "exec1".to_string(),
            ExecutorLoad {
                partition_count: 2,
                tables: HashSet::new(),
            },
        );
        loads.insert(
            "exec2".to_string(),
            ExecutorLoad {
                partition_count: 1,
                tables: HashSet::new(),
            },
        );

        let state = CycleState {
            executor_ids: vec!["exec1".to_string(), "exec2".to_string()],
            tables: vec![],
        };
        let config = AssignmentConfig {
            max_partitions_per_executor: 2,
            ..Default::default()
        };

        let partition = UnassignedPartition {
            table: TableReference::parse_str("table"),
            partition_value: pv("date", "2024-01-01"),
        };

        // exec1 at capacity → only exec2 eligible
        let selected = select_executor_for_partition(&partition, &loads, &state, &config);
        assert_eq!(selected, Some("exec2".to_string()));
    }

    #[test]
    fn test_select_executor_none_when_all_at_capacity() {
        let mut loads = HashMap::new();
        loads.insert(
            "exec1".to_string(),
            ExecutorLoad {
                partition_count: 1,
                tables: HashSet::new(),
            },
        );

        let state = CycleState {
            executor_ids: vec!["exec1".to_string()],
            tables: vec![],
        };
        let config = AssignmentConfig {
            max_partitions_per_executor: 1,
            ..Default::default()
        };

        let partition = UnassignedPartition {
            table: TableReference::parse_str("table"),
            partition_value: pv("date", "2024-01-01"),
        };

        let selected = select_executor_for_partition(&partition, &loads, &state, &config);
        assert!(selected.is_none());
    }

    #[tokio::test]
    async fn test_assign_respects_max_per_cycle() {
        let store = make_store().await;
        setup_table(
            &store,
            "test_table",
            vec![
                unassigned_partition("date", "2024-01-01"),
                unassigned_partition("date", "2024-01-02"),
                unassigned_partition("date", "2024-01-03"),
            ],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string()],
            tables: vec!["test_table".to_string()],
        };
        let config = AssignmentConfig {
            max_assignments_per_cycle: 2,
            ..Default::default()
        };

        let unassigned = find_unassigned_partitions(&store, &state);
        let assignments = assign_unassigned_partitions(
            unassigned,
            &state,
            &store,
            &config,
            AssignmentLimit::PerCycleCap,
        );
        assert_eq!(assignments.len(), 2);
    }

    #[tokio::test]
    async fn test_assign_unlimited_ignores_cap() {
        let store = make_store().await;
        setup_table(
            &store,
            "test_table",
            vec![
                unassigned_partition("date", "2024-01-01"),
                unassigned_partition("date", "2024-01-02"),
                unassigned_partition("date", "2024-01-03"),
            ],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string()],
            tables: vec!["test_table".to_string()],
        };
        let config = AssignmentConfig {
            // Low cap that reconcile_table must ignore.
            max_assignments_per_cycle: 1,
            ..Default::default()
        };

        let unassigned = find_unassigned_partitions(&store, &state);
        let assignments = assign_unassigned_partitions(
            unassigned,
            &state,
            &store,
            &config,
            AssignmentLimit::Unlimited,
        );
        // All 3 partitions get assigned despite the per-cycle cap of 1.
        assert_eq!(assignments.len(), 3);
    }

    #[tokio::test]
    async fn test_commit_assignments() {
        let store = make_store().await;
        setup_table(
            &store,
            "test_table",
            vec![unassigned_partition("date", "2024-01-01")],
        )
        .await;

        let assignments = vec![Assignment {
            table: TableReference::parse_str("test_table"),
            partition_value: pv("date", "2024-01-01"),
            executor_id: "exec1".to_string(),
        }];

        let result = commit_assignments(&store, assignments, None)
            .await
            .expect("commit");
        assert_eq!(result.committed.len(), 1);
        assert!(result.lost.is_empty());

        let metadata = store
            .get_table_metadata(&TableReference::parse_str("test_table"))
            .await
            .expect("get")
            .expect("exists");
        assert!(metadata.partitions[0].is_assigned_to("exec1"));
    }

    /// Simulates the post-discovery partition assignment flow:
    /// new partition values are added to the store, then assigned to executors.
    /// Verifies that after the flow completes, partitions are present and assigned in the store.
    #[tokio::test]
    async fn test_new_partitions_discovered_and_assigned() {
        let store = make_store().await;

        // Initialize table with 2 existing assigned partitions.
        setup_table(
            &store,
            "orders",
            vec![
                assigned_partition("date", "2024-01-01", "exec1"),
                assigned_partition("date", "2024-01-02", "exec2"),
            ],
        )
        .await;

        // Simulate discovery finding 2 new partition values.
        let new_partition_values = vec![pv("date", "2024-01-03"), pv("date", "2024-01-04")];
        let table = TableReference::parse_str("orders");

        // Step 1: Add new partitions to the store (as unassigned).
        store
            .apply_discovery(&table, &["date".to_string()], &new_partition_values, &[])
            .await
            .expect("add partitions");
        store.refresh().await.expect("refresh");

        // Verify: 4 partitions total, 2 new ones unassigned.
        let metadata = store
            .get_table_metadata(&table)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(metadata.partitions.len(), 4);
        let unassigned_count = metadata
            .partitions
            .iter()
            .filter(|p| !p.is_assigned())
            .count();
        assert_eq!(unassigned_count, 2, "New partitions should be unassigned");

        // Step 2: Assign unassigned partitions using the assignment algorithm.
        let state = CycleState {
            executor_ids: vec!["exec1".to_string(), "exec2".to_string()],
            tables: vec!["orders".to_string()],
        };
        let config = AssignmentConfig::default();

        let unassigned = find_unassigned_partitions(&store, &state);
        assert_eq!(unassigned.len(), 2);

        let assignments = assign_unassigned_partitions(
            unassigned,
            &state,
            &store,
            &config,
            AssignmentLimit::PerCycleCap,
        );
        assert_eq!(
            assignments.len(),
            2,
            "Both new partitions should be assigned"
        );

        // Step 3: Commit assignments to the store.
        let result = commit_assignments(&store, assignments, None)
            .await
            .expect("commit");
        assert_eq!(result.committed.len(), 2);
        assert!(result.lost.is_empty());

        // Verify: all 4 partitions are now assigned in the store.
        let metadata = store
            .get_table_metadata(&table)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(metadata.partitions.len(), 4);
        for p in &metadata.partitions {
            assert!(
                p.is_assigned(),
                "Partition {:?} should be assigned",
                p.partition_value
            );
        }
    }

    /// Verifies that adding partitions that already exist is a no-op (idempotent).
    #[tokio::test]
    async fn test_add_partitions_idempotent() {
        let store = make_store().await;
        setup_table(
            &store,
            "orders",
            vec![assigned_partition("date", "2024-01-01", "exec1")],
        )
        .await;

        let table = TableReference::parse_str("orders");

        // Try to add the same partition again.
        store
            .apply_discovery(
                &table,
                &["date".to_string()],
                &[pv("date", "2024-01-01")],
                &[],
            )
            .await
            .expect("should succeed (idempotent)");

        let metadata = store
            .get_table_metadata(&table)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(
            metadata.partitions.len(),
            1,
            "Duplicate partition should not be added"
        );
        assert!(metadata.partitions[0].is_assigned_to("exec1"));
    }

    /// Runs `race` once, right before the first `cluster.json` write reaches the
    /// store: another scheduler commits between this scheduler's read and its
    /// write.
    struct RaceBeforeFirstWrite {
        inner: Arc<dyn ObjectStore>,
        race: tokio::sync::Mutex<Option<futures::future::BoxFuture<'static, ()>>>,
        /// When set, how many more writes succeed; later ones fail.
        writes_left: std::sync::Mutex<Option<usize>>,
    }

    impl std::fmt::Display for RaceBeforeFirstWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RaceBeforeFirstWrite")
        }
    }

    impl std::fmt::Debug for RaceBeforeFirstWrite {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RaceBeforeFirstWrite")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RaceBeforeFirstWrite {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let race = self.race.lock().await.take();
            if let Some(race) = race {
                race.await;
            }
            if let Some(left) = self.writes_left.lock().expect("lock").as_mut() {
                if *left == 0 {
                    return Err(object_store::Error::Generic {
                        store: "RaceBeforeFirstWrite",
                        source: "write refused".into(),
                    });
                }
                *left -= 1;
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Two schedulers' partition stores on one object store. The first one's
    /// next write is preceded by `race`, which the second one performs.
    async fn racing_stores(
        table: &str,
        partitions: Vec<PartitionMetadata>,
    ) -> (
        PartitionStore,
        Arc<PartitionStore>,
        Arc<RaceBeforeFirstWrite>,
    ) {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let other = Arc::new(PartitionStore::accelerations(Arc::new(
            ClusterStateStore::new(Arc::clone(&inner), ""),
        )));
        other.cluster_state().bootstrap().await.expect("bootstrap");
        setup_table(&other, table, partitions).await;
        let racing = Arc::new(RaceBeforeFirstWrite {
            inner,
            race: tokio::sync::Mutex::new(None),
            writes_left: std::sync::Mutex::new(None),
        });
        let this = PartitionStore::accelerations(Arc::new(ClusterStateStore::new(
            Arc::clone(&racing) as Arc<dyn ObjectStore>,
            "",
        )));
        this.refresh().await.expect("refresh");
        (this, other, racing)
    }

    fn assigned_to(
        meta: &crate::metadata::TablePartitionMetadata,
        value: &PartitionValue,
    ) -> Vec<String> {
        meta.partitions
            .iter()
            .find(|p| p.partition_value == *value)
            .map(|p| p.assigned_executors.clone())
            .unwrap_or_default()
    }

    /// An assignment another scheduler commits while this one adds new
    /// partitions to the same table is kept.
    #[tokio::test]
    async fn adding_partitions_keeps_a_concurrent_assignment() {
        let table = TableReference::parse_str("t");
        let existing = pv("date", "2024-01-01");
        let (this, other, racing) =
            racing_stores("t", vec![unassigned_partition("date", "2024-01-01")]).await;
        let (race_table, race_value) = (table.clone(), existing.clone());
        *racing.race.lock().await = Some(Box::pin(async move {
            other
                .assign_partition(&race_table, &race_value, "exec1")
                .await
                .expect("the other scheduler assigns");
        }));

        this.apply_discovery(
            &table,
            &["date".to_string()],
            &[pv("date", "2024-01-02")],
            &[],
        )
        .await
        .expect("add partitions");

        let meta = this
            .get_table_metadata(&table)
            .await
            .expect("read")
            .expect("table metadata");
        assert_eq!(meta.partitions.len(), 2, "{meta:?}");
        assert_eq!(
            assigned_to(&meta, &existing),
            vec!["exec1".to_string()],
            "the concurrent assignment must survive: {meta:?}"
        );
    }

    /// An assignment another scheduler commits while this one removes other
    /// partitions of the same table is kept.
    #[tokio::test]
    async fn removing_partitions_keeps_a_concurrent_assignment() {
        let table = TableReference::parse_str("t");
        let kept = pv("date", "2024-01-01");
        let (this, other, racing) = racing_stores(
            "t",
            vec![
                unassigned_partition("date", "2024-01-01"),
                unassigned_partition("date", "2024-01-02"),
            ],
        )
        .await;
        let (race_table, race_value) = (table.clone(), kept.clone());
        *racing.race.lock().await = Some(Box::pin(async move {
            other
                .assign_partition(&race_table, &race_value, "exec1")
                .await
                .expect("the other scheduler assigns");
        }));

        this.apply_discovery(
            &table,
            &["date".to_string()],
            &[],
            &[pv("date", "2024-01-02")],
        )
        .await
        .expect("remove partitions");

        let meta = this
            .get_table_metadata(&table)
            .await
            .expect("read")
            .expect("table metadata");
        assert_eq!(meta.partitions.len(), 1, "{meta:?}");
        assert_eq!(
            assigned_to(&meta, &kept),
            vec!["exec1".to_string()],
            "the concurrent assignment must survive: {meta:?}"
        );
    }

    /// `PartitionOperations` stub whose discovery returns `values`.
    struct FixedSourceOps(Vec<PartitionValue>);

    #[async_trait::async_trait]
    impl crate::context::PartitionExprResolver for FixedSourceOps {
        async fn try_parse_expr(
            &self,
            _tbl: &TableReference,
            expr: &str,
        ) -> std::result::Result<::datafusion_expr::Expr, datafusion::error::DataFusionError>
        {
            Ok(::datafusion_expr::col(expr))
        }
    }

    #[async_trait::async_trait]
    impl crate::context::PartitionDiscoverer for FixedSourceOps {
        async fn table_partition_values(
            &self,
            _table: &TableReference,
            _partition_by: &[PartitionedBy],
        ) -> std::result::Result<Vec<PartitionValue>, Box<dyn std::error::Error + Send + Sync>>
        {
            Ok(self.0.clone())
        }
    }

    /// A table's first discovery publishes its entry together with every
    /// discovered partition: no reader ever finds an entry that lists only
    /// part of them, even when a later write fails.
    #[tokio::test]
    async fn first_discovery_publishes_the_entry_with_its_partitions() {
        let (this, _other, racing) = racing_stores("other", Vec::new()).await;
        *racing.writes_left.lock().expect("lock") = Some(1);
        let service = PartitionService::new(
            Arc::new(this.clone()),
            Arc::new(ExecutorRegistry::new(
                Arc::new(this.clone()),
                Arc::new(this.clone()),
            )),
            Arc::new(tokio::sync::RwLock::new(None)),
        );
        let table = TableReference::parse_str("t");
        let partition_by = vec![PartitionedBy {
            name: "date".to_string(),
            expression: "date".to_string(),
        }];

        service
            .seed_table(
                &table,
                &partition_by,
                &FixedSourceOps(vec![pv("date", "2024-01-01"), pv("date", "2024-01-02")]),
            )
            .await
            .expect("one write publishes the table");

        let meta = this
            .get_table_metadata(&table)
            .await
            .expect("read")
            .expect("the entry is published");
        assert_eq!(
            meta.partitions.len(),
            2,
            "a partial inventory was published: {meta:?}"
        );
    }

    /// A partition another scheduler assigned first keeps its executor, and
    /// this scheduler's competing assignment is not reported as committed.
    #[tokio::test]
    async fn the_first_scheduler_to_assign_a_partition_keeps_it() {
        let table = TableReference::parse_str("t");
        let value = pv("date", "2024-01-01");
        let (this, other, racing) =
            racing_stores("t", vec![unassigned_partition("date", "2024-01-01")]).await;
        let (race_table, race_value) = (table.clone(), value.clone());
        *racing.race.lock().await = Some(Box::pin(async move {
            other
                .assign_partition(&race_table, &race_value, "exec1")
                .await
                .expect("the other scheduler assigns");
        }));

        let result = commit_assignments(
            &this,
            vec![Assignment {
                table: table.clone(),
                partition_value: value.clone(),
                executor_id: "exec2".to_string(),
            }],
            None,
        )
        .await
        .expect("commit");

        let meta = this
            .get_table_metadata(&table)
            .await
            .expect("read")
            .expect("table metadata");
        assert_eq!(
            assigned_to(&meta, &value),
            vec!["exec1".to_string()],
            "the first assignment must hold: {meta:?}"
        );
        assert!(result.committed.is_empty(), "{result:?}");
        assert_eq!(result.lost.len(), 1, "{result:?}");
    }

    /// An executor connected to no live scheduler keeps its partitions for the
    /// grace period, then loses them; one that comes back in time keeps them.
    #[tokio::test(start_paused = true)]
    async fn an_executor_gone_for_the_grace_period_loses_its_partitions() {
        let store = make_store().await;
        setup_table(
            &store,
            "t",
            vec![
                assigned_partition("date", "1", "exec1"),
                assigned_partition("date", "2", "exec2"),
            ],
        )
        .await;
        let service = PartitionService::new(
            Arc::clone(&store),
            Arc::new(ExecutorRegistry::new(
                Arc::clone(&store),
                make_store().await,
            )),
            Arc::new(tokio::sync::RwLock::new(None)),
        );
        let only_exec2 = HashSet::from(["exec2".to_string()]);
        let both = HashSet::from(["exec1".to_string(), "exec2".to_string()]);

        assert_eq!(
            service.reassign_departed(&only_exec2).await.expect("cycle"),
            0
        );
        tokio::time::advance(DEPARTED_EXECUTOR_GRACE / 2).await;
        // Back before the grace period ends: the absence starts over.
        assert_eq!(service.reassign_departed(&both).await.expect("cycle"), 0);
        assert_eq!(
            service.reassign_departed(&only_exec2).await.expect("cycle"),
            0
        );
        tokio::time::advance(DEPARTED_EXECUTOR_GRACE / 2).await;
        assert_eq!(
            service.reassign_departed(&only_exec2).await.expect("cycle"),
            0
        );
        tokio::time::advance(DEPARTED_EXECUTOR_GRACE / 2).await;
        assert_eq!(
            service.reassign_departed(&only_exec2).await.expect("cycle"),
            1
        );

        let meta = store
            .get_table_metadata(&TableReference::parse_str("t"))
            .await
            .expect("read")
            .expect("table metadata");
        assert!(assigned_to(&meta, &pv("date", "1")).is_empty(), "{meta:?}");
        assert_eq!(
            assigned_to(&meta, &pv("date", "2")),
            vec!["exec2".to_string()]
        );
    }

    /// Answers every `UpdatePartitions` it receives and forwards it.
    fn ack_and_forward(
        mut rx: tokio::sync::mpsc::Receiver<SchedulerControlMessage>,
        pending_acks: crate::correlated::CorrelatedResponses<runtime_proto::Ack>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<UpdatePartitions> {
        let (tx, forwarded) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Some(SchedulerControlMessageEnum::UpdatePartitions(update)) = msg.message {
                    pending_acks.deliver(
                        &update.request_id,
                        runtime_proto::Ack {
                            request_id: update.request_id.clone(),
                            error: None,
                        },
                    );
                    let _ = tx.send(update);
                }
            }
        });
        forwarded
    }

    fn partitioned_app(table: &str) -> Arc<App> {
        let mut dataset = spicepod::component::dataset::Dataset::new("memory:data", table);
        dataset.acceleration = Some(spicepod::acceleration::Acceleration {
            enabled: true,
            partition_by: vec![PartitionedBy {
                name: "date".to_string(),
                expression: "date".to_string(),
            }],
            ..spicepod::acceleration::Acceleration::default()
        });
        Arc::new(app::AppBuilder::new("test").with_dataset(dataset).build())
    }

    async fn encoded(table: &TableReference, value: &str) -> bytes::Bytes {
        partition_value_to_bytes(pv("date", value), table, &EmptySourceOps)
            .await
            .expect("encode")
    }

    /// An assignment whose notification never reached the executor is sent
    /// again, once, and again after a while if the executor never reports it
    /// loaded; a partition the executor reports loaded but is no longer
    /// assigned is removed from it.
    #[tokio::test(start_paused = true)]
    async fn assignment_changes_an_executor_has_not_applied_are_sent_again() {
        let store = make_store().await;
        setup_table(&store, "t", vec![assigned_partition("date", "1", "exec1")]).await;
        let registry = Arc::new(ExecutorRegistry::new(
            Arc::clone(&store),
            make_store().await,
        ));
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let handles = registry
            .register("exec1".to_string(), "p1", tx)
            .await
            .handles;
        let mut updates = ack_and_forward(rx, handles.pending_acks);
        let service = PartitionService::new(
            Arc::clone(&store),
            Arc::clone(&registry),
            Arc::new(tokio::sync::RwLock::new(Some(partitioned_app("t")))),
        );
        let tracker = PartitionLoadTracker::new();
        let table = TableReference::parse_str("t");
        let ops = EmptySourceOps;
        service
            .resync_executors(&ops, &tracker)
            .await
            .expect("resync");
        let update = updates.try_recv().expect("the lost assignment is sent");
        assert_eq!(
            update.new_partitions["t"].items,
            vec![encoded(&table, "1").await.to_vec()]
        );
        assert!(update.removed_partitions.is_empty());

        // Acked: not sent again.
        service
            .resync_executors(&ops, &tracker)
            .await
            .expect("resync");
        updates
            .try_recv()
            .expect_err("an acked assignment is not sent again");

        // Still not reported loaded: sent again, so a failed load is retried.
        tokio::time::advance(UNLOADED_RESEND_AFTER).await;
        service
            .resync_executors(&ops, &tracker)
            .await
            .expect("resync");
        let update = updates
            .try_recv()
            .expect("an assignment that never loaded is sent again");
        assert_eq!(
            update.new_partitions["t"].items,
            vec![encoded(&table, "1").await.to_vec()]
        );

        // The executor reports a partition it is no longer assigned.
        tracker
            .replace(
                canonical_table(&table),
                "exec1".to_string(),
                crate::LoadedAck {
                    process_id: "p1".to_string(),
                    load_sequence: 1,
                    partitions: HashSet::from([
                        encoded(&table, "1").await,
                        encoded(&table, "2").await,
                    ]),
                },
            )
            .await;
        service
            .resync_executors(&ops, &tracker)
            .await
            .expect("resync");
        let update = updates.try_recv().expect("the stale partition is removed");
        assert!(update.new_partitions.is_empty());
        assert_eq!(
            update.removed_partitions["t"].items,
            vec![encoded(&table, "2").await.to_vec()]
        );
    }
}
