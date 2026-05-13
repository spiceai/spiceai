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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use app::App;
use datafusion::sql::TableReference;
use futures::future::join_all;
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use runtime_proto::scheduler_control_message::Message as SchedulerControlMessageEnum;
use runtime_proto::{BytesArray, SchedulerControlMessage, UpdatePartitions};
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;
use tokio::sync::RwLock;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use crate::context::PartitionOperations;
use crate::executor_registry::{self, ExecutorRegistry};
use crate::scheduler_task_config::PartitionAssignmentConfig;
use crate::{PartitionMetadata, PartitionStore, PartitionValue, partition_value_to_bytes, store};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to refresh partition store: {source}"))]
    PartitionStoreRefresh { source: store::Error },

    #[snafu(display("Failed to list tables: {source}"))]
    ListTables { source: store::Error },

    #[snafu(display("Failed to get table metadata for {table}: {source}"))]
    GetTableMetadata { table: String, source: store::Error },

    #[snafu(display("Table metadata not found for {table}"))]
    TableMetadataNotFound { table: String },

    #[snafu(display("Failed to write metadata for {table}: {source}"))]
    WriteMetadata { table: String, source: store::Error },

    #[snafu(display(
        "Concurrent modification detected for {table} partition {partition} after {retries} retries"
    ))]
    MaxRetriesExceeded {
        table: String,
        partition: String,
        retries: u32,
    },

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

    #[snafu(display("Failed to read current system time: {source}"))]
    SystemTime { source: std::time::SystemTimeError },
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
            discovery_timeout: Duration::from_secs(60),
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
pub struct PartitionDiff {
    /// Partitions present in the source but not yet tracked in the store.
    pub new: Vec<PartitionValue>,
    /// Partitions tracked in the store but no longer present in the source.
    pub removed: Vec<PartitionValue>,
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
    committed: Vec<Assignment>,
    failed: Vec<(Assignment, Error)>,
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
}

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
        }
    }

    fn config_from_app(app: &App) -> AssignmentConfig {
        let Some(scheduler) = app.runtime.scheduler.clone() else {
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
        values: Vec<PartitionValue>,
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;
        self.diff_and_apply(table, partition_by, values, ops)
            .await?;
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

        // Submit a partition discovery job and poll synchronously until it
        // completes or the discovery timeout is reached.  This preserves the
        // on-demand semantics of reconcile_table (caller expects partitions to
        // be up-to-date before returning) while routing discovery through the
        // same Ballista job machinery used by the periodic scheduler tick.
        let partition_expressions: Vec<String> =
            partition_by.iter().map(|p| p.expression.clone()).collect();
        let job_id = ops
            .submit_discovery_job(table, &partition_by)
            .await
            .map_err(|e| Error::DiscoveryFailed {
                table: table.to_string(),
                source: e,
            })?;
        tracing::info!(table = %table, job_id = %job_id, "Submitted on-demand partition discovery job");

        let deadline = tokio::time::Instant::now() + config.discovery_timeout;
        let values = loop {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    table = %table,
                    job_id = %job_id,
                    timeout = ?config.discovery_timeout,
                    "On-demand partition discovery timed out; skipping diff"
                );
                return Ok(());
            }
            match ops.poll_discovery_job(&job_id, &partition_expressions).await {
                Ok(crate::context::DiscoveryJobPollResult::Completed(values)) => {
                    break values;
                }
                Ok(crate::context::DiscoveryJobPollResult::Failed(msg)) => {
                    return Err(Error::DiscoveryFailed {
                        table: table.to_string(),
                        source: msg.into(),
                    });
                }
                Ok(crate::context::DiscoveryJobPollResult::StillRunning) => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => {
                    return Err(Error::DiscoveryFailed {
                        table: table.to_string(),
                        source: e,
                    });
                }
            }
        };

        self.diff_and_apply(table, &partition_by, values, ops)
            .await?;

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let executors = self.executor_registry.connected_executors().await;
        if executors.is_empty() {
            tracing::warn!(table = %table, "No executors connected, cannot assign new partitions");
            return Ok(());
        }

        self.assign_pending(
            std::slice::from_ref(table),
            &executors,
            ops,
            AssignmentLimit::Unlimited,
            &config,
        )
        .await
    }

    /// Discover, add/remove, and assign partitions for every accelerated table
    /// declared in the app. The per-cycle assignment cap from
    /// [`AssignmentConfig::max_assignments_per_cycle`] is applied so that a
    /// slow cycle can't saturate the cluster in one tick.
    ///
    /// Used by the periodic partition-management background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition store refresh or executor notification fails.
    /// Per-table diff failures are logged and skipped rather than propagated.
    pub async fn reconcile_all(&self, ops: &dyn PartitionOperations) -> Result<()> {
        let Some(app) = self.app.read().await.clone() else {
            tracing::warn!("App not initialized, skipping partition discovery");
            return Ok(());
        };
        let config = Self::config_from_app(&app);

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        // In the job-based discovery design, partition values are already in the
        // store (seeded at startup for static tables, or written by
        // `check_and_process_discovery_jobs` for dynamic tables). This step only
        // assigns any pending unassigned partitions to executors.
        let all_tables: Vec<TableReference> = {
            let ds = app.datasets.iter().filter_map(|ds| {
                if let Some(acc) = &ds.acceleration
                    && acc.enabled
                    && !acc.partition_by.is_empty()
                {
                    Some(TableReference::parse_str(&ds.name))
                } else {
                    None
                }
            });
            let views = app.views.iter().filter_map(|view| {
                if let Some(acc) = &view.acceleration
                    && acc.enabled
                    && !acc.partition_by.is_empty()
                {
                    Some(TableReference::parse_str(&view.name))
                } else {
                    None
                }
            });
            ds.chain(views).collect()
        };

        if all_tables.is_empty() {
            return Ok(());
        }

        let executors = self.executor_registry.connected_executors().await;
        if executors.is_empty() {
            return Ok(());
        }

        self.assign_pending(
            &all_tables,
            &executors,
            ops,
            AssignmentLimit::PerCycleCap,
            &config,
        )
        .await
    }

    /// Apply pre-discovered partition values to the store.
    ///
    /// Diffs `values` against the current store state, then adds new partitions
    /// and removes stale ones. Called by [`Self::seed_table`], [`Self::reconcile_table`],
    /// and [`Self::check_and_process_discovery_jobs`].
    async fn diff_and_apply(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
        values: Vec<PartitionValue>,
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        let existing_partitions: Vec<crate::PartitionMetadata> =
            match self.partition_store.get_table_metadata(table).await {
                Ok(Some(m)) => m.partitions,
                Ok(None) => {
                    tracing::debug!(
                        table = %table,
                        count = values.len(),
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

        let diff = compute_diff(&values, &existing_partitions);

        if diff.is_empty() {
            return Ok(());
        }

        if !diff.new.is_empty() {
            tracing::info!(
                table = %table,
                count = diff.new.len(),
                "Adding new partitions"
            );
            let partition_expressions: Vec<String> =
                partition_by.iter().map(|p| p.expression.clone()).collect();
            if let Err(e) = self
                .partition_store
                .initialize_metadata(table, partition_expressions)
                .await
            {
                tracing::warn!(table = %table, error = %e, "Failed to initialize partition metadata");
            }
            add_partitions_with_retry(&self.partition_store, table, diff.new).await?;
        }

        if !diff.removed.is_empty() {
            tracing::info!(
                table = %table,
                count = diff.removed.len(),
                "Removing stale partitions"
            );
            remove_partitions_with_cleanup(
                &self.partition_store,
                &self.executor_registry,
                ops,
                table,
                diff.removed,
            )
            .await?;
        }

        Ok(())
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
        let CommitResult { committed, failed } =
            commit_assignments(&self.partition_store, assignments).await?;
        if !failed.is_empty() {
            tracing::warn!("Failed to commit {} partition assignments", failed.len());
        }
        notify_executors(&self.executor_registry, ops, committed).await
    }

    // ================================================================
    // Async discovery (non-blocking) entry points
    // ================================================================

    /// Submit a discovery job for a single table without waiting for results.
    ///
    /// Writes the job ID to `TablePartitionMetadata.active_discovery_job_id`
    /// so any scheduler can poll for completion later. The caller should have
    /// already checked for static partition resolution (e.g. `bucket(N, col)`)
    /// before calling this.
    ///
    /// If the table already has an `active_discovery_job_id`, this is a no-op.
    pub async fn submit_discovery_job(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        // Ensure metadata is initialized so we have a place to store the job ID.
        let partition_expressions: Vec<String> =
            partition_by.iter().map(|p| p.expression.clone()).collect();
        if let Err(e) = self
            .partition_store
            .initialize_metadata(table, partition_expressions)
            .await
        {
            tracing::warn!(table = %table, error = %e, "Failed to initialize partition metadata");
        }

        // Check if there's already an active discovery job.
        if let Some(metadata) = self.partition_store.get_cached_table_metadata(table) {
            if metadata.active_discovery_job_id.is_some() {
                tracing::debug!(table = %table, "Discovery job already in progress, skipping");
                return Ok(());
            }
        }

        // Submit the discovery job.
        let job_id = ops
            .submit_discovery_job(table, partition_by)
            .await
            .map_err(|e| Error::DiscoveryFailed {
                table: table.to_string(),
                source: e,
            })?;

        tracing::info!(table = %table, job_id = %job_id, "Submitted partition discovery job");

        // Record the job ID in the partition metadata (OCC-safe).
        self.partition_store
            .set_discovery_job_id(table, Some(job_id))
            .await
            .context(WriteMetadataSnafu {
                table: table.to_string(),
            })?;

        Ok(())
    }

    /// Check all in-flight discovery jobs and process completed ones.
    ///
    /// For each table with an `active_discovery_job_id`:
    /// - `Completed` → compute diff, apply changes, clear the job ID.
    /// - `Failed` → log warning, clear the job ID.
    /// - `StillRunning` → skip (will check again next tick).
    pub async fn check_and_process_discovery_jobs(
        &self,
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let tables = self
            .partition_store
            .list_tables()
            .await
            .context(ListTablesSnafu)?;

        for table_name in tables {
            let table_ref = TableReference::parse_str(&table_name);
            let Some(metadata) = self.partition_store.get_cached_table_metadata(&table_ref) else {
                continue;
            };
            let Some(job_id) = &metadata.active_discovery_job_id else {
                continue;
            };
            let job_id = job_id.clone();
            let partition_expressions = metadata.partition_expressions.clone();

            match ops
                .poll_discovery_job(&job_id, &partition_expressions)
                .await
            {
                Ok(crate::context::DiscoveryJobPollResult::Completed(values)) => {
                    tracing::info!(
                        table = %table_name,
                        job_id = %job_id,
                        partition_count = values.len(),
                        "Discovery job completed"
                    );

                    // Get the partition_by config from the app.
                    let app_guard = self.app.read().await;
                    let partition_by = app_guard
                        .as_ref()
                        .and_then(|app| get_partition_config(app, &table_ref))
                        .unwrap_or_default();
                    drop(app_guard);

                    if let Err(e) = self
                        .diff_and_apply(&table_ref, &partition_by, values, ops)
                        .await
                    {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "Failed to apply discovery results"
                        );
                    }

                    // Clear the discovery job ID.
                    if let Err(e) = self
                        .partition_store
                        .set_discovery_job_id(&table_ref, None)
                        .await
                    {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "Failed to clear discovery job ID"
                        );
                    }
                }
                Ok(crate::context::DiscoveryJobPollResult::Failed(msg)) => {
                    tracing::warn!(
                        table = %table_name,
                        job_id = %job_id,
                        error = %msg,
                        "Discovery job failed"
                    );
                    if let Err(e) = self
                        .partition_store
                        .set_discovery_job_id(&table_ref, None)
                        .await
                    {
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "Failed to clear discovery job ID"
                        );
                    }
                }
                Ok(crate::context::DiscoveryJobPollResult::StillRunning) => {
                    tracing::debug!(
                        table = %table_name,
                        job_id = %job_id,
                        "Discovery job still running"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        job_id = %job_id,
                        error = %e,
                        "Failed to poll discovery job"
                    );
                    // Clear the job ID to allow a retry on the next cycle.
                    let _ = self
                        .partition_store
                        .set_discovery_job_id(&table_ref, None)
                        .await;
                }
            }
        }

        Ok(())
    }

    /// Submit discovery jobs for tables that have no active job and no
    /// statically resolvable partition values.
    ///
    /// `tables_needing_discovery` should be pre-filtered by the caller to
    /// exclude tables with statically resolvable partitions (e.g. `bucket(N, col)`).
    pub async fn submit_discovery_for_pending_tables(
        &self,
        tables: &[(TableReference, Vec<PartitionedBy>)],
        ops: &dyn PartitionOperations,
    ) -> Result<()> {
        for (table, partition_by) in tables {
            if let Err(e) = self.submit_discovery_job(table, partition_by, ops).await {
                tracing::warn!(
                    table = %table,
                    error = %e,
                    "Failed to submit discovery job"
                );
            }
        }
        Ok(())
    }
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

/// Compute the diff between source-discovered partition values and what is
/// already tracked in the store. Pure function — no IO, no store access.
pub fn compute_diff(
    source_partitions: &[PartitionValue],
    existing_partitions: &[crate::PartitionMetadata],
) -> PartitionDiff {
    let existing: HashSet<Vec<(String, String)>> = existing_partitions
        .iter()
        .map(|p| sorted_kv(&p.partition_value))
        .collect();

    let source_set: HashSet<Vec<(String, String)>> =
        source_partitions.iter().map(sorted_kv).collect();

    let new: Vec<PartitionValue> = source_partitions
        .iter()
        .filter(|p| !existing.contains(&sorted_kv(p)))
        .cloned()
        .collect();

    let removed: Vec<PartitionValue> = existing_partitions
        .iter()
        .filter(|p| !source_set.contains(&sorted_kv(&p.partition_value)))
        .map(|p| p.partition_value.clone())
        .collect();

    PartitionDiff { new, removed }
}

/// Sort a `PartitionValue` into a deterministic `Vec<(k, v)>` for equality comparisons.
fn sorted_kv(p: &PartitionValue) -> Vec<(String, String)> {
    let mut v: Vec<_> = p.clone().into_iter().collect();
    v.sort();
    v
}

async fn add_partitions_with_retry(
    partition_store: &PartitionStore,
    table: &TableReference,
    partition_values: Vec<PartitionValue>,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();

    loop {
        let mut metadata = partition_store
            .get_table_metadata(table)
            .await
            .context(GetTableMetadataSnafu {
                table: table.to_string(),
            })?
            .ok_or_else(|| Error::TableMetadataNotFound {
                table: table.to_string(),
            })?;

        let now = now_ms()?;
        let mut added_any = false;
        for partition_value in &partition_values {
            if metadata
                .partitions
                .iter()
                .any(|p| p.partition_value == *partition_value)
            {
                continue;
            }
            metadata.add_partition(PartitionMetadata::new(partition_value.clone()));
            added_any = true;
        }

        if !added_any {
            return Ok(());
        }

        metadata.updated_at = now;
        match partition_store.write_metadata(table, metadata).await {
            Ok(()) => {
                tracing::debug!(table = %table, count = partition_values.len(), "Added new partitions to metadata");
                return Ok(());
            }
            Err(store::Error::ConcurrentModification { .. }) => match backoff.next_duration() {
                Some(duration) => tokio::time::sleep(duration).await,
                None => {
                    return Err(Error::MaxRetriesExceeded {
                        table: table.to_string(),
                        partition: format!("{} partitions", partition_values.len()),
                        retries: 5,
                    });
                }
            },
            Err(e) => {
                return Err(Error::WriteMetadata {
                    table: table.to_string(),
                    source: e,
                });
            }
        }
    }
}

async fn remove_partitions_with_cleanup(
    partition_store: &PartitionStore,
    executor_registry: &ExecutorRegistry,
    ops: &dyn PartitionOperations,
    table: &TableReference,
    partition_values: Vec<PartitionValue>,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();
    // Rebuilt inside the loop every iteration so that retries on
    // ConcurrentModification don't accumulate duplicate unload notifications.
    let mut executors_to_notify: HashMap<String, Vec<PartitionValue>> = HashMap::new();

    loop {
        let mut this_attempt: HashMap<String, Vec<PartitionValue>> = HashMap::new();
        let mut metadata = partition_store
            .get_table_metadata(table)
            .await
            .context(GetTableMetadataSnafu {
                table: table.to_string(),
            })?
            .ok_or_else(|| Error::TableMetadataNotFound {
                table: table.to_string(),
            })?;

        let mut removed_any = false;
        for partition_value in &partition_values {
            if let Some(pos) = metadata
                .partitions
                .iter()
                .position(|p| p.partition_value == *partition_value)
            {
                let partition = &metadata.partitions[pos];
                for executor_id in &partition.assigned_executors {
                    this_attempt
                        .entry(executor_id.clone())
                        .or_default()
                        .push(partition_value.clone());
                }
                metadata.partitions.remove(pos);
                removed_any = true;
            }
        }

        if !removed_any {
            break;
        }

        metadata.updated_at = now_ms()?;

        match partition_store.write_metadata(table, metadata).await {
            Ok(()) => {
                tracing::debug!(table = %table, count = partition_values.len(), "Removed stale partitions");
                executors_to_notify = this_attempt;
                break;
            }
            Err(store::Error::ConcurrentModification { .. }) => match backoff.next_duration() {
                Some(duration) => tokio::time::sleep(duration).await,
                None => {
                    return Err(Error::MaxRetriesExceeded {
                        table: table.to_string(),
                        partition: format!("{} partitions", partition_values.len()),
                        retries: 5,
                    });
                }
            },
            Err(e) => {
                return Err(Error::WriteMetadata {
                    table: table.to_string(),
                    source: e,
                });
            }
        }
    }

    for (executor_id, partitions) in &executors_to_notify {
        if let Err(e) = notify_executor_to_unload(
            executor_registry,
            ops,
            executor_id,
            table,
            partitions.clone(),
        )
        .await
        {
            tracing::warn!(executor_id = %executor_id, table = %table, error = %e, "Failed to notify executor to unload partitions");
        }
    }

    Ok(())
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

async fn commit_assignments(
    partition_store: &PartitionStore,
    assignments: Vec<Assignment>,
) -> Result<CommitResult> {
    let mut committed = Vec::new();
    let mut failed = Vec::new();

    for assignment in assignments {
        match assign_partition_with_retry(
            partition_store,
            &assignment.table,
            &assignment.partition_value,
            &assignment.executor_id,
        )
        .await
        {
            Ok(()) => {
                tracing::debug!(
                    table = %assignment.table,
                    partition = ?assignment.partition_value,
                    executor = %assignment.executor_id,
                    "Partition assigned"
                );
                committed.push(assignment);
            }
            Err(e) => {
                tracing::warn!(
                    table = %assignment.table,
                    partition = ?assignment.partition_value,
                    executor = %assignment.executor_id,
                    error = %e,
                    "Failed to assign partition"
                );
                failed.push((assignment, e));
            }
        }
    }

    if !committed.is_empty() || !failed.is_empty() {
        tracing::info!(
            committed_count = committed.len(),
            failed_count = failed.len(),
            "Committed partition assignments"
        );
    }
    Ok(CommitResult { committed, failed })
}

async fn assign_partition_with_retry(
    partition_store: &PartitionStore,
    table: &TableReference,
    partition_value: &PartitionValue,
    executor_id: &str,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(3)).build();

    loop {
        match partition_store
            .assign_partition(table, partition_value, executor_id)
            .await
        {
            Ok(()) => return Ok(()),
            Err(store::Error::ConcurrentModification { .. }) => match backoff.next_duration() {
                Some(duration) => {
                    tracing::debug!(
                        table = %table,
                        partition = ?partition_value,
                        "Concurrent modification detected, retrying"
                    );
                    tokio::time::sleep(duration).await;
                }
                None => {
                    return Err(Error::MaxRetriesExceeded {
                        table: table.to_string(),
                        partition: format!("{partition_value:?}"),
                        retries: 3,
                    });
                }
            },
            Err(e) => {
                return Err(Error::WriteMetadata {
                    table: table.to_string(),
                    source: e,
                });
            }
        }
    }
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
            partitions_bytes.push(bytes.to_vec());
        }

        let command = SchedulerControlMessage {
            message: Some(SchedulerControlMessageEnum::UpdatePartitions(
                UpdatePartitions {
                    new_partitions: HashMap::from([(
                        table.to_string(),
                        BytesArray {
                            items: partitions_bytes,
                        },
                    )]),
                    removed_partitions: HashMap::new(),
                },
            )),
        };

        registry
            .send_command(executor_id, command)
            .await
            .context(SendCommandSnafu {
                executor_id: executor_id.to_string(),
            })?;
    }

    Ok(())
}

#[expect(clippy::result_large_err)]
fn now_ms() -> Result<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .context(SystemTimeSnafu)
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use crate::cluster_state::ClusterStateStore;

    use super::*;

    async fn make_store() -> Arc<PartitionStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        Arc::new(PartitionStore::accelerations(cs))
    }

    fn pv(key: &str, val: &str) -> PartitionValue {
        HashMap::from([(key.to_string(), val.to_string())])
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
            active_discovery_job_id: None,
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

        let result = commit_assignments(&store, assignments)
            .await
            .expect("commit");
        assert_eq!(result.committed.len(), 1);
        assert!(result.failed.is_empty());

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
        add_partitions_with_retry(&store, &table, new_partition_values)
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
        let result = commit_assignments(&store, assignments)
            .await
            .expect("commit");
        assert_eq!(result.committed.len(), 2);
        assert!(result.failed.is_empty());

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
        add_partitions_with_retry(&store, &table, vec![pv("date", "2024-01-01")])
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
}
