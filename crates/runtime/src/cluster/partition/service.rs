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

//! Partition service — discovery, assignment, and executor notification.
//!
//! Both [`super::scheduler_task::PartitionManagementTask`] (periodic) and
//! [`crate::datafusion::DataFusion`] (on-demand refresh) use this service
//! to discover and assign partitions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use app::App;
use datafusion::sql::TableReference;
use futures::future::join_all;
use runtime_proto::scheduler_control_message::Message as SchedulerControlMessageEnum;
use runtime_proto::{BytesArray, SchedulerControlMessage, UpdatePartitions};
use snafu::prelude::*;
use tokio::sync::RwLock;
use tokio::time::timeout;

use util::fibonacci_backoff::FibonacciBackoffBuilder;

use crate::cluster::executor_registry::{self, ExecutorRegistry};
use crate::cluster::partition::discovery::{discover_new_partitions, table_partition_values};
use crate::cluster::partition::{
    PartitionMetadata, PartitionStore, PartitionValue, partition_value_to_bytes,
};
use crate::datafusion::DataFusion;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to refresh partition store: {source}"))]
    PartitionStoreRefresh {
        source: crate::cluster::partition::store::Error,
    },

    #[snafu(display("Failed to list tables: {source}"))]
    ListTables {
        source: crate::cluster::partition::store::Error,
    },

    #[snafu(display("Failed to get table metadata for {table}: {source}"))]
    GetTableMetadata {
        table: String,
        source: crate::cluster::partition::store::Error,
    },

    #[snafu(display("Table metadata not found for {table}"))]
    TableMetadataNotFound { table: String },

    #[snafu(display("Failed to write metadata for {table}: {source}"))]
    WriteMetadata {
        table: String,
        source: crate::cluster::partition::store::Error,
    },

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
        source: crate::cluster::partition::Error,
    },

    #[snafu(display("Partition discovery timed out for table {table}"))]
    DiscoveryTimeout { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration for partition assignment operations.
#[derive(Debug, Clone)]
pub struct AssignmentConfig {
    /// Maximum partitions to assign per cycle
    pub max_assignments_per_cycle: usize,
    /// Maximum partitions per executor (soft limit)
    pub max_partitions_per_executor: usize,
    /// How long to wait for partition discovery before timing out
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

struct DiscoveryResult {
    new_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
    removed_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
}

/// Shared partition infrastructure for discovery and assignment operations.
///
/// Holds the partition manager, executor registry, app reference, and assignment configuration.
/// Methods take `&Arc<DataFusion>` as a parameter to avoid circular references.
pub struct PartitionService {
    pub partition_store: Arc<PartitionStore>,
    pub executor_registry: Arc<ExecutorRegistry>,
    pub config: AssignmentConfig,
    pub(crate) app: Arc<RwLock<Option<Arc<App>>>>,
}

impl PartitionService {
    #[must_use]
    pub fn new(
        partition_store: Arc<PartitionStore>,
        executor_registry: Arc<ExecutorRegistry>,
        config: AssignmentConfig,
        app: Arc<RwLock<Option<Arc<App>>>>,
    ) -> Self {
        Self {
            partition_store,
            executor_registry,
            config,
            app,
        }
    }

    /// Discover new partitions for a single table, assign them, and notify executors.
    pub async fn discover_and_assign_for_table(
        &self,
        table: &TableReference,
        df: &Arc<DataFusion>,
    ) -> Result<()> {
        let Some(app) = self.app.read().await.clone() else {
            return Ok(());
        };
        let Some(partition_by) = get_partition_config(&app, table) else {
            return Ok(());
        };

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let new_partitions = match timeout(
            self.config.discovery_timeout,
            discover_new_partitions(table, &partition_by, &self.partition_store, df),
        )
        .await
        {
            Ok(Ok(new)) => new,
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

        if new_partitions.is_empty() {
            return Ok(());
        }

        tracing::info!(
            table = %table,
            count = new_partitions.len(),
            "Discovered new partitions before refresh"
        );

        // Ensure partition metadata is initialized for this table. This handles
        // the case where refresh is triggered before the periodic metadata seeding
        // has run (e.g., immediately after startup).
        let partition_expressions: Vec<String> =
            partition_by.iter().map(|p| p.expression.clone()).collect();
        if let Err(e) = self
            .partition_store
            .initialize_metadata(table, partition_expressions)
            .await
        {
            tracing::warn!(table = %table, error = %e, "Failed to initialize partition metadata");
        }

        add_partitions_with_retry(&self.partition_store, table, new_partitions).await?;

        self.partition_store
            .refresh()
            .await
            .context(PartitionStoreRefreshSnafu)?;

        let executor_ids = self.executor_registry.connected_executors().await;
        if executor_ids.is_empty() {
            tracing::warn!(table = %table, "No executors connected, cannot assign new partitions");
            return Ok(());
        }

        let tables = self
            .partition_store
            .list_tables()
            .await
            .context(ListTablesSnafu)?;
        let state = CycleState {
            executor_ids,
            tables,
        };

        let unassigned = find_unassigned_partitions_for_table(&self.partition_store, table);
        if !unassigned.is_empty() {
            let assignments = assign_unassigned_partitions(
                unassigned,
                &state,
                &self.partition_store,
                &self.config,
            );
            let CommitResult { committed, failed } =
                commit_assignments(&self.partition_store, assignments).await?;
            if !failed.is_empty() {
                tracing::warn!("Failed to commit {} partition assignments", failed.len());
            }
            notify_executors(&self.executor_registry, df, committed).await?;
        }

        Ok(())
    }

    /// Discover new/removed partitions for all tracked tables, assign, and notify executors.
    pub async fn discover_and_assign_all_tables(&self, df: &Arc<DataFusion>) -> Result<()> {
        let Some(app) = self.app.read().await.clone() else {
            tracing::warn!("App not initialized, skipping partition discovery");
            return Ok(());
        };

        let state = refresh_state(&self.partition_store, &self.executor_registry).await?;

        let discovery_result =
            discover_and_sync_partitions(&app, df, &self.partition_store, &state, &self.config)
                .await?;

        if !discovery_result.new_partitions.is_empty() {
            add_new_partitions_to_store(&self.partition_store, discovery_result.new_partitions)
                .await?;
        }

        if !discovery_result.removed_partitions.is_empty() {
            remove_stale_partitions_from_store(
                &self.partition_store,
                &self.executor_registry,
                df,
                discovery_result.removed_partitions,
            )
            .await?;
        }

        let unassigned = find_unassigned_partitions(&self.partition_store, &state);
        if !unassigned.is_empty() {
            let assignments = assign_unassigned_partitions(
                unassigned,
                &state,
                &self.partition_store,
                &self.config,
            );
            let CommitResult { committed, failed } =
                commit_assignments(&self.partition_store, assignments).await?;
            if !failed.is_empty() {
                tracing::warn!("Failed to commit {} partition assignments", failed.len());
            }
            notify_executors(&self.executor_registry, df, committed).await?;
        }

        Ok(())
    }
}

/// Returns the `partition_by` config for a table from the App definition.
///
/// Searches both datasets and views for a matching table reference.
#[must_use]
pub(crate) fn get_partition_config(
    app: &app::App,
    table: &TableReference,
) -> Option<Vec<spicepod::partitioning::PartitionedBy>> {
    let acceleration = app
        .datasets
        .iter()
        .find(|d| crate::datafusion::resolved_equality(d.name.clone().into(), table.clone()))
        .and_then(|d| d.acceleration.as_ref())
        .or_else(|| {
            app.views
                .iter()
                .find(|v| {
                    crate::datafusion::resolved_equality(v.name.clone().into(), table.clone())
                })
                .and_then(|v| v.acceleration.as_ref())
        });

    acceleration
        .map(|a| a.partition_by.clone())
        .filter(|pb| !pb.is_empty())
}

async fn refresh_state(
    partition_store: &PartitionStore,
    executor_registry: &ExecutorRegistry,
) -> Result<CycleState> {
    partition_store
        .refresh()
        .await
        .context(PartitionStoreRefreshSnafu)?;

    let executor_ids = executor_registry.connected_executors().await;
    let tables = partition_store
        .list_tables()
        .await
        .context(ListTablesSnafu)?;

    Ok(CycleState {
        executor_ids,
        tables,
    })
}

/// For each tracked table, queries the source for current partition values and
/// diffs against the stored metadata. Returns new partitions (in source but not
/// in metadata) and removed partitions (in metadata but no longer in source).
/// Does not assign or notify — the caller handles that.
async fn discover_and_sync_partitions(
    app: &App,
    df: &Arc<DataFusion>,
    partition_store: &PartitionStore,
    state: &CycleState,
    config: &AssignmentConfig,
) -> Result<DiscoveryResult> {
    let mut new_partitions = Vec::new();
    let mut removed_partitions = Vec::new();

    for table_name in &state.tables {
        let table_ref = TableReference::parse_str(table_name);

        let Some(partition_by) = get_partition_config(app, &table_ref) else {
            continue;
        };

        let Some(metadata) = partition_store.get_cached_table_metadata(&table_ref) else {
            continue;
        };

        let current_partitions: HashSet<Vec<(String, String)>> = metadata
            .partitions
            .iter()
            .map(|p| {
                let mut v: Vec<_> = p.partition_value.clone().into_iter().collect();
                v.sort();
                v
            })
            .collect();

        let source_partitions_list = match timeout(
            config.discovery_timeout,
            table_partition_values(&table_ref, &partition_by, df),
        )
        .await
        {
            Ok(Ok(partitions)) => partitions,
            Ok(Err(e)) => {
                tracing::warn!(table = %table_ref, error = %e, "Failed to discover partitions from source");
                continue;
            }
            Err(_) => {
                tracing::warn!(table = %table_ref, timeout_secs = config.discovery_timeout.as_secs(), "Partition discovery timed out");
                continue;
            }
        };

        let source_partitions: HashSet<Vec<(String, String)>> = source_partitions_list
            .into_iter()
            .map(|p| {
                let mut v: Vec<_> = p.into_iter().collect();
                v.sort();
                v
            })
            .collect();

        let new: Vec<PartitionValue> = source_partitions
            .difference(&current_partitions)
            .map(|v| v.iter().cloned().collect())
            .collect();

        let removed: Vec<PartitionValue> = current_partitions
            .difference(&source_partitions)
            .map(|v| v.iter().cloned().collect())
            .collect();

        if !new.is_empty() {
            tracing::info!(table = %table_name, count = new.len(), "Discovered new partitions");
            new_partitions.push((table_ref.clone(), new));
        }

        if !removed.is_empty() {
            tracing::info!(table = %table_name, count = removed.len(), "Detected removed partitions");
            removed_partitions.push((table_ref.clone(), removed));
        }
    }

    Ok(DiscoveryResult {
        new_partitions,
        removed_partitions,
    })
}

async fn add_new_partitions_to_store(
    partition_store: &PartitionStore,
    new_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
) -> Result<()> {
    for (table, partition_values) in new_partitions {
        if let Err(e) = add_partitions_with_retry(partition_store, &table, partition_values).await {
            tracing::error!(table = %table, error = %e, "Failed to add new partitions to metadata");
        }
    }
    Ok(())
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

        let now = now_ms();
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
            Err(crate::cluster::partition::store::Error::ConcurrentModification { .. }) => {
                match backoff.next_duration() {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => {
                        return Err(Error::MaxRetriesExceeded {
                            table: table.to_string(),
                            partition: format!("{} partitions", partition_values.len()),
                            retries: 5,
                        });
                    }
                }
            }
            Err(e) => {
                return Err(Error::WriteMetadata {
                    table: table.to_string(),
                    source: e,
                });
            }
        }
    }
}

async fn remove_stale_partitions_from_store(
    partition_store: &PartitionStore,
    executor_registry: &ExecutorRegistry,
    df: &Arc<DataFusion>,
    removed_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
) -> Result<()> {
    for (table, partition_values) in removed_partitions {
        if let Err(e) = remove_partitions_with_cleanup(
            partition_store,
            executor_registry,
            df,
            &table,
            partition_values,
        )
        .await
        {
            tracing::error!(table = %table, error = %e, "Failed to remove stale partitions");
        }
    }
    Ok(())
}

async fn remove_partitions_with_cleanup(
    partition_store: &PartitionStore,
    executor_registry: &ExecutorRegistry,
    df: &Arc<DataFusion>,
    table: &TableReference,
    partition_values: Vec<PartitionValue>,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();
    let mut executors_to_notify: HashMap<String, Vec<PartitionValue>> = HashMap::new();

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

        let mut removed_any = false;
        for partition_value in &partition_values {
            if let Some(pos) = metadata
                .partitions
                .iter()
                .position(|p| p.partition_value == *partition_value)
            {
                let partition = &metadata.partitions[pos];
                for executor_id in &partition.assigned_executors {
                    executors_to_notify
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

        metadata.updated_at = now_ms();

        match partition_store.write_metadata(table, metadata).await {
            Ok(()) => {
                tracing::debug!(table = %table, count = partition_values.len(), "Removed stale partitions");
                break;
            }
            Err(crate::cluster::partition::store::Error::ConcurrentModification { .. }) => {
                match backoff.next_duration() {
                    Some(duration) => tokio::time::sleep(duration).await,
                    None => {
                        return Err(Error::MaxRetriesExceeded {
                            table: table.to_string(),
                            partition: format!("{} partitions", partition_values.len()),
                            retries: 5,
                        });
                    }
                }
            }
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
            df,
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
    df: &Arc<DataFusion>,
    executor_id: &str,
    table: &TableReference,
    partitions: Vec<PartitionValue>,
) -> Result<()> {
    let mut partitions_bytes = Vec::new();
    for p in partitions {
        let bytes = partition_value_to_bytes(p, table, df)
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

fn find_unassigned_partitions_for_table(
    partition_store: &PartitionStore,
    table: &TableReference,
) -> Vec<UnassignedPartition> {
    let Some(metadata) = partition_store.get_cached_table_metadata(table) else {
        return Vec::new();
    };

    metadata
        .unassigned_partitions()
        .iter()
        .map(|p| UnassignedPartition {
            table: table.clone(),
            partition_value: p.partition_value.clone(),
        })
        .collect()
}

fn assign_unassigned_partitions(
    unassigned: Vec<UnassignedPartition>,
    state: &CycleState,
    partition_store: &PartitionStore,
    config: &AssignmentConfig,
) -> Vec<Assignment> {
    if unassigned.is_empty() {
        return Vec::new();
    }

    let mut assignments = Vec::new();
    let mut assignments_this_cycle = 0;
    let mut executor_loads = build_executor_loads(state, partition_store);

    for unassigned_partition in unassigned {
        if assignments_this_cycle >= config.max_assignments_per_cycle {
            tracing::debug!(
                max_assignments = config.max_assignments_per_cycle,
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
            Err(crate::cluster::partition::store::Error::ConcurrentModification { .. }) => {
                match backoff.next_duration() {
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
                }
            }
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
    df: &Arc<DataFusion>,
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
                    notify_executor_of_assignments(registry, df, &executor_id, assignments).await
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
    df: &Arc<DataFusion>,
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
            let bytes = partition_value_to_bytes(p, &table, df)
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

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn make_store() -> Arc<PartitionStore> {
        Arc::new(PartitionStore::new(Arc::new(InMemory::new())))
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
        let metadata = super::super::metadata::TablePartitionMetadata {
            table_name: table.to_string(),
            partitions,
            schema_version: 1,
            updated_at: 1000,
            partition_expressions: vec!["date".to_string()],
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
        let store = make_store();
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
    async fn test_find_unassigned_for_single_table() {
        let store = make_store();
        setup_table(
            &store,
            "test_table",
            vec![
                assigned_partition("date", "2024-01-01", "exec1"),
                unassigned_partition("date", "2024-01-02"),
            ],
        )
        .await;

        let table_ref = TableReference::parse_str("test_table");
        let unassigned = find_unassigned_partitions_for_table(&store, &table_ref);
        assert_eq!(unassigned.len(), 1);
        assert_eq!(
            unassigned[0].partition_value.get("date"),
            Some(&"2024-01-02".to_string())
        );
    }

    #[tokio::test]
    async fn test_build_executor_loads() {
        let store = make_store();
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
        let store = make_store();
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
        let store = make_store();
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
        let store = make_store();
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
        let assignments = assign_unassigned_partitions(unassigned, &state, &store, &config);
        assert_eq!(assignments.len(), 2);
    }

    #[tokio::test]
    async fn test_commit_assignments() {
        let store = make_store();
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
        let store = make_store();

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

        let unassigned = find_unassigned_partitions_for_table(&store, &table);
        assert_eq!(unassigned.len(), 2);

        let assignments = assign_unassigned_partitions(unassigned, &state, &store, &config);
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

    /// Verifies that new partitions are assigned with load balancing:
    /// if exec1 already has 2 partitions and exec2 has 0, new partitions
    /// should prefer exec2.
    #[tokio::test]
    async fn test_new_partitions_assigned_with_load_balancing() {
        let store = make_store();

        // exec1 has 2 partitions, exec2 has none.
        setup_table(
            &store,
            "orders",
            vec![
                assigned_partition("date", "2024-01-01", "exec1"),
                assigned_partition("date", "2024-01-02", "exec1"),
            ],
        )
        .await;

        // Add 1 new partition to a DIFFERENT table so exec1's locality bonus
        // for "orders" doesn't apply — pure load balancing.
        let other_table = TableReference::parse_str("inventory");
        setup_table(
            &store,
            "inventory",
            vec![unassigned_partition("date", "2024-01-03")],
        )
        .await;

        let state = CycleState {
            executor_ids: vec!["exec1".to_string(), "exec2".to_string()],
            tables: vec!["orders".to_string(), "inventory".to_string()],
        };
        let config = AssignmentConfig {
            max_partitions_per_executor: 10,
            ..Default::default()
        };

        let unassigned = find_unassigned_partitions_for_table(&store, &other_table);
        let assignments = assign_unassigned_partitions(unassigned, &state, &store, &config);
        assert_eq!(assignments.len(), 1);

        // exec2 has lower load and no locality bonus for "inventory" → preferred.
        assert_eq!(
            assignments[0].executor_id, "exec2",
            "New partition should be assigned to the less-loaded executor"
        );
    }

    /// Verifies that adding partitions that already exist is a no-op (idempotent).
    #[tokio::test]
    async fn test_add_partitions_idempotent() {
        let store = make_store();
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
