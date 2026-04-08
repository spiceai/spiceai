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
    PartitionManager, PartitionMetadata, PartitionValue, partition_value_to_bytes,
};
use crate::datafusion::DataFusion;

// --- Error and config types ---

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to refresh partition manager: {source}"))]
    PartitionManagerRefresh {
        source: crate::cluster::partition::manager::Error,
    },

    #[snafu(display("Failed to list tables: {source}"))]
    ListTables {
        source: crate::cluster::partition::manager::Error,
    },

    #[snafu(display("Failed to get table metadata for {table}: {source}"))]
    GetTableMetadata {
        table: String,
        source: crate::cluster::partition::manager::Error,
    },

    #[snafu(display("Table metadata not found for {table}"))]
    TableMetadataNotFound { table: String },

    #[snafu(display("Failed to write metadata for {table}: {source}"))]
    WriteMetadata {
        table: String,
        source: crate::cluster::partition::manager::Error,
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

// --- Internal types ---

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
    pub partition_manager: Arc<PartitionManager>,
    pub executor_registry: Arc<ExecutorRegistry>,
    pub config: AssignmentConfig,
    pub(crate) app: Arc<RwLock<Option<Arc<App>>>>,
}

impl PartitionService {
    #[must_use]
    pub fn new(
        partition_manager: Arc<PartitionManager>,
        executor_registry: Arc<ExecutorRegistry>,
        config: AssignmentConfig,
        app: Arc<RwLock<Option<Arc<App>>>>,
    ) -> Self {
        Self {
            partition_manager,
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

        self.partition_manager
            .refresh()
            .await
            .context(PartitionManagerRefreshSnafu)?;

        let new_partitions = match timeout(
            self.config.discovery_timeout,
            discover_new_partitions(table, &partition_by, &self.partition_manager, df),
        )
        .await
        {
            Ok(Ok(new)) => new,
            Ok(Err(e)) => {
                tracing::warn!(table = %table, error = %e, "Failed to discover partitions");
                return Ok(());
            }
            Err(_) => {
                tracing::warn!(table = %table, "Partition discovery timed out");
                return Ok(());
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

        add_partitions_with_retry(&self.partition_manager, table, new_partitions).await?;

        self.partition_manager
            .refresh()
            .await
            .context(PartitionManagerRefreshSnafu)?;

        let executor_ids = self.executor_registry.connected_executors().await;
        if executor_ids.is_empty() {
            tracing::warn!(table = %table, "No executors connected, cannot assign new partitions");
            return Ok(());
        }

        let tables = self
            .partition_manager
            .list_tables()
            .await
            .context(ListTablesSnafu)?;
        let state = CycleState {
            executor_ids,
            tables,
        };

        let unassigned = find_unassigned_partitions_for_table(&self.partition_manager, table);
        if !unassigned.is_empty() {
            let assignments = assign_unassigned_partitions(
                unassigned,
                &state,
                &self.partition_manager,
                &self.config,
            );
            let CommitResult { committed, failed } =
                commit_assignments(&self.partition_manager, assignments).await?;
            if !failed.is_empty() {
                tracing::warn!("Failed to commit {} partition assignments", failed.len());
            }
            notify_executors(&self.executor_registry, df, committed).await?;
        }

        Ok(())
    }

    /// Discover new/removed partitions for all tracked tables, assign, and notify executors.
    pub async fn discover_and_assign_all(&self, df: &Arc<DataFusion>) -> Result<()> {
        let Some(app) = self.app.read().await.clone() else {
            tracing::warn!("App not initialized, skipping partition discovery");
            return Ok(());
        };

        let state = refresh_state(&self.partition_manager, &self.executor_registry).await?;

        let discovery_result =
            discover_and_sync_partitions(&app, df, &self.partition_manager, &state, &self.config)
                .await?;

        if !discovery_result.new_partitions.is_empty() {
            add_new_partitions(&self.partition_manager, discovery_result.new_partitions).await?;
        }

        if !discovery_result.removed_partitions.is_empty() {
            remove_stale_partitions(
                &self.partition_manager,
                &self.executor_registry,
                df,
                discovery_result.removed_partitions,
            )
            .await?;
        }

        let unassigned = find_unassigned_partitions(&self.partition_manager, &state);
        if !unassigned.is_empty() {
            let assignments = assign_unassigned_partitions(
                unassigned,
                &state,
                &self.partition_manager,
                &self.config,
            );
            let CommitResult { committed, failed } =
                commit_assignments(&self.partition_manager, assignments).await?;
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
    partition_manager: &PartitionManager,
    executor_registry: &ExecutorRegistry,
) -> Result<CycleState> {
    partition_manager
        .refresh()
        .await
        .context(PartitionManagerRefreshSnafu)?;

    let executor_ids = executor_registry.connected_executors().await;
    let tables = partition_manager
        .list_tables()
        .await
        .context(ListTablesSnafu)?;

    Ok(CycleState {
        executor_ids,
        tables,
    })
}

async fn discover_and_sync_partitions(
    app: &App,
    df: &Arc<DataFusion>,
    partition_manager: &PartitionManager,
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

        let Some(metadata) = partition_manager.get_cached_table_metadata(&table_ref) else {
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

async fn add_new_partitions(
    partition_manager: &PartitionManager,
    new_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
) -> Result<()> {
    for (table, partition_values) in new_partitions {
        if let Err(e) = add_partitions_with_retry(partition_manager, &table, partition_values).await
        {
            tracing::error!(table = %table, error = %e, "Failed to add new partitions to metadata");
        }
    }
    Ok(())
}

async fn add_partitions_with_retry(
    partition_manager: &PartitionManager,
    table: &TableReference,
    partition_values: Vec<PartitionValue>,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();

    loop {
        let mut metadata = partition_manager
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
        match partition_manager
            .write_metadata(&table.to_string(), metadata)
            .await
        {
            Ok(()) => {
                tracing::debug!(table = %table, count = partition_values.len(), "Added new partitions to metadata");
                return Ok(());
            }
            Err(crate::cluster::partition::manager::Error::ConcurrentModification { .. }) => {
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

async fn remove_stale_partitions(
    partition_manager: &PartitionManager,
    executor_registry: &ExecutorRegistry,
    df: &Arc<DataFusion>,
    removed_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
) -> Result<()> {
    for (table, partition_values) in removed_partitions {
        if let Err(e) = remove_partitions_with_cleanup(
            partition_manager,
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
    partition_manager: &PartitionManager,
    executor_registry: &ExecutorRegistry,
    df: &Arc<DataFusion>,
    table: &TableReference,
    partition_values: Vec<PartitionValue>,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(5)).build();
    let mut executors_to_notify: HashMap<String, Vec<PartitionValue>> = HashMap::new();

    loop {
        let mut metadata = partition_manager
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

        match partition_manager
            .write_metadata(&table.to_string(), metadata)
            .await
        {
            Ok(()) => {
                tracing::debug!(table = %table, count = partition_values.len(), "Removed stale partitions");
                break;
            }
            Err(crate::cluster::partition::manager::Error::ConcurrentModification { .. }) => {
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
    partition_manager: &PartitionManager,
    state: &CycleState,
) -> Vec<UnassignedPartition> {
    let mut unassigned = Vec::new();

    for table_name in &state.tables {
        let table_ref = TableReference::parse_str(table_name);
        let Some(metadata) = partition_manager.get_cached_table_metadata(&table_ref) else {
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
    partition_manager: &PartitionManager,
    table: &TableReference,
) -> Vec<UnassignedPartition> {
    let Some(metadata) = partition_manager.get_cached_table_metadata(table) else {
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
    partition_manager: &PartitionManager,
    config: &AssignmentConfig,
) -> Vec<Assignment> {
    if unassigned.is_empty() {
        return Vec::new();
    }

    let mut assignments = Vec::new();
    let mut assignments_this_cycle = 0;
    let mut executor_loads = build_executor_loads(state, partition_manager);

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
    partition_manager: &PartitionManager,
) -> HashMap<String, ExecutorLoad> {
    let mut loads = HashMap::new();

    for executor_id in &state.executor_ids {
        loads.insert(executor_id.clone(), ExecutorLoad::default());
    }

    for table_name in &state.tables {
        let table_ref = TableReference::parse_str(table_name);
        if let Some(metadata) = partition_manager.get_cached_table_metadata(&table_ref) {
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
    partition_manager: &PartitionManager,
    assignments: Vec<Assignment>,
) -> Result<CommitResult> {
    let mut committed = Vec::new();
    let mut failed = Vec::new();

    for assignment in assignments {
        match assign_partition_with_retry(
            partition_manager,
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
    partition_manager: &PartitionManager,
    table: &TableReference,
    partition_value: &PartitionValue,
    executor_id: &str,
) -> Result<()> {
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(Some(3)).build();

    loop {
        match partition_manager
            .assign_partition(table, partition_value, executor_id)
            .await
        {
            Ok(()) => return Ok(()),
            Err(crate::cluster::partition::manager::Error::ConcurrentModification { .. }) => {
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
                let df = df;
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
