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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datafusion::sql::TableReference;
use futures::future::join_all;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use runtime_proto::scheduler_control_message::Message as SchedulerControlMessageEnum;
use runtime_proto::{BytesArray, SchedulerControlMessage, UpdatePartitions};
use snafu::prelude::*;
use tokio::time::{MissedTickBehavior, timeout};
use tokio_util::sync::CancellationToken;

use crate::cluster::executor_registry::{self, ExecutorRegistry};
use crate::cluster::partition::discovery::table_partition_values;
use crate::cluster::partition::{
    PartitionManager, PartitionMetadata, PartitionValue, partition_value_to_bytes,
};
use crate::datafusion::DataFusion;
use app::App;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

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

    #[snafu(display("Failed to discover partitions for {table}: {source}"))]
    DiscoverPartitions {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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

#[derive(Debug, Clone)]
pub struct PartitionManagementConfig {
    /// How often to run the management cycle
    pub interval: Duration,

    /// Maximum partitions to assign per executor per cycle
    pub max_assignments_per_cycle: usize,

    /// Maximum partitions per executor (soft limit)
    pub max_partitions_per_executor: usize,

    /// How long to wait for partition discovery before timing out
    pub discovery_timeout: Duration,
}

impl From<spicepod::component::runtime::PartitionManagement> for PartitionManagementConfig {
    fn from(config: spicepod::component::runtime::PartitionManagement) -> Self {
        let interval = fundu::parse_duration(&config.interval).unwrap_or(Duration::from_secs(30));
        let discovery_timeout =
            fundu::parse_duration(&config.discovery_timeout).unwrap_or(Duration::from_secs(60));

        Self {
            interval,
            max_assignments_per_cycle: config.max_assignments_per_cycle,
            max_partitions_per_executor: config.max_partitions_per_executor,
            discovery_timeout,
        }
    }
}

impl Default for PartitionManagementConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            max_assignments_per_cycle: 100,
            max_partitions_per_executor: 1000,
            discovery_timeout: Duration::from_secs(60),
        }
    }
}

/// Background task responsible for managing the assignment of accelerated table partitions to executors. Responsible for
/// 1. Discovering new partition values from tables that are accelerated (by querying the underlying source).
/// 2. Adding new partitions to the partition metadata (initially unassigned).
/// 3. Removing partitions that no longer exist in the source and notifying executors to unload them.
/// 4. Assigning unassigned partitions to executors.
pub struct PartitionManagementTask {
    app: Arc<RwLock<Option<Arc<App>>>>,
    df: Arc<DataFusion>,
    io_runtime: Handle,
    partition_manager: Arc<PartitionManager>,
    executor_registry: Arc<ExecutorRegistry>,

    /// Configuration
    config: PartitionManagementConfig,

    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
}

struct CycleState {
    executors: Vec<ExecutorInfo>,
    tables: Vec<String>,
    #[expect(dead_code)]
    timestamp: u128,
}

#[derive(Clone)]
struct ExecutorInfo {
    id: String,
    #[expect(dead_code)]
    current_partitions: usize,
}

struct UnassignedPartition {
    table: TableReference,
    partition_value: PartitionValue,
}

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

struct CommitResult {
    committed: Vec<Assignment>,
    #[expect(dead_code)]
    failed: Vec<(Assignment, Error)>,
}

struct DiscoveryResult {
    new_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
    removed_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
}

impl PartitionManagementTask {
    pub fn new(
        app: Arc<RwLock<Option<Arc<App>>>>,
        df: Arc<DataFusion>,
        io_runtime: Handle,
        partition_manager: Arc<PartitionManager>,
        executor_registry: Arc<ExecutorRegistry>,
        config: PartitionManagementConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            app,
            df,
            io_runtime,
            partition_manager,
            executor_registry,
            config,
            cancel,
        }
    }

    pub async fn run(self) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = self.cancel.cancelled() => {
                    tracing::info!("Partition management task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    let cycle_start = Instant::now();

                    if let Err(e) = self.run_management_cycle().await {
                        tracing::warn!(
                            error = %e,
                            "Partition management cycle failed"
                        );
                    }

                    let cycle_duration = cycle_start.elapsed();
                    if cycle_duration > self.config.interval {
                        tracing::warn!(
                            duration_ms = cycle_duration.as_millis(),
                            interval_ms = self.config.interval.as_millis(),
                            "Partition management cycle took longer than interval"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Underlying logic for a single management cycle.
    async fn run_management_cycle(&self) -> Result<()> {
        let state = self.refresh_state().await?;

        let discovery_result = self.discover_and_sync_partitions(&state).await?;

        if !discovery_result.new_partitions.is_empty() {
            self.add_new_partitions(discovery_result.new_partitions)
                .await?;
        }

        if !discovery_result.removed_partitions.is_empty() {
            self.remove_stale_partitions(discovery_result.removed_partitions)
                .await?;
        }

        let unassigned = self.find_unassigned_partitions(&state).await?;

        if !unassigned.is_empty() {
            let assignments = self
                .assign_unassigned_partitions(unassigned, &state)
                .await?;

            let commit_result = self.commit_assignments(assignments).await?;

            self.notify_executors(commit_result.committed).await?;
        }

        Ok(())
    }

    async fn refresh_state(&self) -> Result<CycleState> {
        // Refresh partition metadata cache from object store
        self.partition_manager
            .refresh()
            .await
            .context(PartitionManagerRefreshSnafu)?;

        // Get current executor state
        let executor_ids = self.executor_registry.connected_executors().await;

        // Get partition counts for each executor from registry
        let mut executors = Vec::new();
        let registry_partitions = self.executor_registry.partitions.read().await;

        for id in executor_ids {
            let count = registry_partitions
                .get(&id)
                .map_or(0, |tables| tables.values().map(std::vec::Vec::len).sum());

            executors.push(ExecutorInfo {
                id,
                current_partitions: count,
            });
        }

        // Get all tables with partition metadata
        let tables = self
            .partition_manager
            .list_tables()
            .await
            .context(ListTablesSnafu)?;

        Ok(CycleState {
            executors,
            tables,
            timestamp: now_ms(),
        })
    }

    async fn discover_and_sync_partitions(&self, state: &CycleState) -> Result<DiscoveryResult> {
        let mut new_partitions = Vec::new();
        let mut removed_partitions = Vec::new();

        for table_name in &state.tables {
            let table_ref = TableReference::parse_str(table_name);

            // Get current metadata
            let metadata = match self.partition_manager.get_cached_table_metadata(&table_ref) {
                Some(m) => m,
                None => continue,
            };

            // Use HashMap for set comparison of partition values
            let current_partitions: HashSet<Vec<(String, String)>> = metadata
                .partitions
                .iter()
                .map(|p| {
                    let mut v: Vec<_> = p.partition_value.clone().into_iter().collect();
                    v.sort(); // Sort for consistent comparison
                    v
                })
                .collect();

            // Discover partitions from source using shared logic from startup.rs
            let source_partitions_list = 'discovery: {
                let app_ref = &self.app;
                let app_lock = app_ref.read().await;
                if let Some(app) = app_lock.as_ref() {
                    if let Some(ds) = app.datasets.iter().find(|d| d.name == table_ref.table()) {
                        if let Some(acceleration) = &ds.acceleration {
                            match timeout(
                                self.config.discovery_timeout,
                                table_partition_values(
                                    &table_ref,
                                    &acceleration.partition_by,
                                    &self.df,
                                    Arc::new(SpiceObjectStoreRegistry::new(
                                        self.io_runtime.clone(),
                                    )),
                                ),
                            )
                            .await
                            {
                                Ok(Ok(partitions)) => break 'discovery Some(partitions),
                                Ok(Err(e)) => {
                                    tracing::warn!(
                                        table = %table_ref,
                                        error = %e,
                                        "Failed to discover partitions from source"
                                    );
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        table = %table_ref,
                                        timeout_secs = self.config.discovery_timeout.as_secs(),
                                        "Partition discovery timed out"
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                table = %table_ref,
                                "Acceleration not configured for table"
                            );
                        }
                    } else {
                        tracing::warn!(
                            table = %table_ref,
                            "Dataset not found for table"
                        );
                    }
                }
                None
            };

            let Some(source_partitions_list) = source_partitions_list else {
                continue;
            };

            let source_partitions: HashSet<Vec<(String, String)>> = source_partitions_list
                .into_iter()
                .map(|p| {
                    let mut v: Vec<_> = p.into_iter().collect();
                    v.sort();
                    v
                })
                .collect();

            // Find new partitions
            let new: Vec<PartitionValue> = source_partitions
                .difference(&current_partitions)
                .map(|v| v.iter().cloned().collect())
                .collect();

            // Find removed partitions
            let removed: Vec<PartitionValue> = current_partitions
                .difference(&source_partitions)
                .map(|v| v.iter().cloned().collect())
                .collect();

            if !new.is_empty() {
                tracing::info!(
                    table = %table_name,
                    count = new.len(),
                    "Discovered new partitions"
                );
                new_partitions.push((table_ref.clone(), new));
            }

            if !removed.is_empty() {
                tracing::info!(
                    table = %table_name,
                    count = removed.len(),
                    "Detected removed partitions"
                );
                removed_partitions.push((table_ref.clone(), removed));
            }
        }

        Ok(DiscoveryResult {
            new_partitions,
            removed_partitions,
        })
    }

    async fn add_new_partitions(
        &self,
        new_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
    ) -> Result<()> {
        for (table, partition_values) in new_partitions {
            if let Err(e) = self
                .add_partitions_with_retry(&table, partition_values)
                .await
            {
                tracing::error!(
                    table = %table,
                    error = %e,
                    "Failed to add new partitions to metadata"
                );
            }
        }

        Ok(())
    }

    async fn add_partitions_with_retry(
        &self,
        table: &TableReference,
        partition_values: Vec<PartitionValue>,
    ) -> Result<()> {
        let mut retries = 0;
        let max_retries = 5;

        loop {
            // Get current metadata
            let mut metadata = self
                .partition_manager
                .get_table_metadata(table)
                .await
                .context(GetTableMetadataSnafu {
                    table: table.to_string(),
                })?
                .ok_or_else(|| Error::TableMetadataNotFound {
                    table: table.to_string(),
                })?;

            // Add new partitions (mark as unassigned)
            let now = now_ms();
            let mut added_any = false;
            for partition_value in &partition_values {
                // Check if already exists (using exact match on HashMap)
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

            match self
                .partition_manager
                .write_metadata(&table.to_string(), metadata)
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        table = %table,
                        count = partition_values.len(),
                        "Added new partitions to metadata"
                    );
                    return Ok(());
                }
                Err(crate::cluster::partition::manager::Error::ConcurrentModification {
                    ..
                }) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(Error::MaxRetriesExceeded {
                            table: table.to_string(),
                            partition: format!("{} partitions", partition_values.len()),
                            retries,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(retries))).await;
                    continue;
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
        &self,
        removed_partitions: Vec<(TableReference, Vec<PartitionValue>)>,
    ) -> Result<()> {
        for (table, partition_values) in removed_partitions {
            if let Err(e) = self
                .remove_partitions_with_cleanup(&table, partition_values)
                .await
            {
                tracing::error!(
                    table = %table,
                    error = %e,
                    "Failed to remove stale partitions from metadata"
                );
            }
        }

        Ok(())
    }

    async fn remove_partitions_with_cleanup(
        &self,
        table: &TableReference,
        partition_values: Vec<PartitionValue>,
    ) -> Result<()> {
        let mut retries = 0;
        let max_retries = 5;

        // Track which executors need to be notified
        let mut executors_to_notify: HashMap<String, Vec<PartitionValue>> = HashMap::new();

        loop {
            // Get current metadata
            let mut metadata = self
                .partition_manager
                .get_table_metadata(table)
                .await
                .context(GetTableMetadataSnafu {
                    table: table.to_string(),
                })?
                .ok_or_else(|| Error::TableMetadataNotFound {
                    table: table.to_string(),
                })?;

            // Remove partitions and track executors
            let mut removed_any = false;
            for partition_value in &partition_values {
                if let Some(pos) = metadata
                    .partitions
                    .iter()
                    .position(|p| p.partition_value == *partition_value)
                {
                    let partition = &metadata.partitions[pos];
                    // Track executors that had this partition
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

            match self
                .partition_manager
                .write_metadata(&table.to_string(), metadata)
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        table = %table,
                        count = partition_values.len(),
                        "Removed stale partitions from metadata"
                    );
                    break;
                }
                Err(crate::cluster::partition::manager::Error::ConcurrentModification {
                    ..
                }) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(Error::MaxRetriesExceeded {
                            table: table.to_string(),
                            partition: format!("{} partitions", partition_values.len()),
                            retries,
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(retries))).await;
                    continue;
                }
                Err(e) => {
                    return Err(Error::WriteMetadata {
                        table: table.to_string(),
                        source: e,
                    });
                }
            }
        }

        // Notify executors to unload partitions
        for (executor_id, partitions) in &executors_to_notify {
            if let Err(e) = self
                .notify_executor_to_unload(executor_id, table, partitions.clone())
                .await
            {
                tracing::warn!(
                    executor_id = %executor_id,
                    table = %table,
                    error = %e,
                    "Failed to notify executor to unload partitions"
                );
            }
        }

        Ok(())
    }

    async fn notify_executor_to_unload(
        &self,
        executor_id: &str,
        table: &TableReference,
        partitions: Vec<PartitionValue>,
    ) -> Result<()> {
        let df = self.df.clone();
        let mut removed_partitions_map = HashMap::new();

        let mut partitions_bytes = Vec::new();
        for p in partitions {
            let bytes = partition_value_to_bytes(p, table, &df)
                .await
                .context(PartitionValueConversionSnafu)?;
            partitions_bytes.push(bytes.to_vec());
        }
        removed_partitions_map.insert(
            table.to_string(),
            BytesArray {
                items: partitions_bytes,
            },
        );

        self.executor_registry
            .send_command(
                executor_id,
                SchedulerControlMessage {
                    message: Some(SchedulerControlMessageEnum::UpdatePartitions(
                        UpdatePartitions {
                            new_partitions: HashMap::new(),
                            removed_partitions: removed_partitions_map,
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

    async fn find_unassigned_partitions(
        &self,
        state: &CycleState,
    ) -> Result<Vec<UnassignedPartition>> {
        let mut unassigned = Vec::new();

        for table_name in &state.tables {
            let table_ref = TableReference::parse_str(table_name);

            let metadata =
                if let Some(m) = self.partition_manager.get_cached_table_metadata(&table_ref) {
                    m
                } else {
                    tracing::warn!(table = %table_name, "No cached metadata, skipping");
                    continue;
                };

            for partition in metadata.unassigned_partitions() {
                unassigned.push(UnassignedPartition {
                    table: table_ref.clone(),
                    partition_value: partition.partition_value.clone(),
                });
            }
        }

        tracing::info!(
            unassigned_count = unassigned.len(),
            "Found unassigned partitions"
        );

        Ok(unassigned)
    }

    async fn assign_unassigned_partitions(
        &self,
        unassigned: Vec<UnassignedPartition>,
        state: &CycleState,
    ) -> Result<Vec<Assignment>> {
        if unassigned.is_empty() {
            tracing::debug!("No unassigned partitions to assign");
            return Ok(Vec::new());
        }

        let mut assignments = Vec::new();
        let mut assignments_this_cycle = 0;

        // Build executor load map
        let mut executor_loads = self.build_executor_loads(state).await?;

        for unassigned_partition in unassigned {
            if assignments_this_cycle >= self.config.max_assignments_per_cycle {
                tracing::debug!(
                    max_assignments = self.config.max_assignments_per_cycle,
                    "Reached max assignments per cycle, deferring remaining partitions"
                );
                break;
            }

            // Select best executor for this partition
            let executor = if let Some(e) = self
                .select_executor_for_partition(&unassigned_partition, &executor_loads, state)
                .await?
            {
                e
            } else {
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
                executor_id: executor.id.clone(),
            });

            // Update executor load tracking
            executor_loads
                .entry(executor.id.clone())
                .or_default()
                .partition_count += 1;

            assignments_this_cycle += 1;
        }

        tracing::info!(
            assignments_count = assignments.len(),
            "Generated partition assignments"
        );

        Ok(assignments)
    }

    async fn build_executor_loads(
        &self,
        state: &CycleState,
    ) -> Result<HashMap<String, ExecutorLoad>> {
        let mut loads = HashMap::new();

        // Initialize with empty loads
        for executor in &state.executors {
            loads.insert(executor.id.clone(), ExecutorLoad::default());
        }

        // Count current assignments
        for table_name in &state.tables {
            let table_ref = TableReference::parse_str(table_name);

            if let Some(metadata) = self.partition_manager.get_cached_table_metadata(&table_ref) {
                for partition in &metadata.partitions {
                    for executor_id in &partition.assigned_executors {
                        let load = loads.entry(executor_id.clone()).or_default();
                        load.partition_count += 1;
                        load.tables.insert(table_name.clone());
                    }
                }
            }
        }

        Ok(loads)
    }

    async fn select_executor_for_partition(
        &self,
        partition: &UnassignedPartition,
        executor_loads: &HashMap<String, ExecutorLoad>,
        state: &CycleState,
    ) -> Result<Option<ExecutorInfo>> {
        let mut candidates: Vec<_> = state.executors.iter().collect();

        if candidates.is_empty() {
            return Ok(None);
        }

        // Filter out executors at capacity
        candidates.retain(|executor| {
            let load = executor_loads
                .get(&executor.id)
                .map_or(0, |l| l.partition_count);
            load < self.config.max_partitions_per_executor
        });

        if candidates.is_empty() {
            tracing::warn!("All executors at capacity");
            return Ok(None);
        }

        // Score each candidate
        let mut scored_candidates: Vec<_> = candidates
            .into_iter()
            .map(|executor| {
                let score = self.score_executor_for_partition(executor, partition, executor_loads);
                (executor, score)
            })
            .collect();

        // Sort by score (highest first)
        scored_candidates
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Return best candidate
        Ok(scored_candidates
            .first()
            .map(|(executor, _)| (*executor).clone()))
    }

    // Simple scoring function that considers:
    // 1. Data locality (if executor already has partitions for this table)
    // 2. Current load (number of partitions assigned) for load balancing
    fn score_executor_for_partition(
        &self,
        executor: &ExecutorInfo,
        partition: &UnassignedPartition,
        executor_loads: &HashMap<String, ExecutorLoad>,
    ) -> f64 {
        let load = executor_loads
            .get(&executor.id)
            .cloned()
            .unwrap_or_default();

        let mut score = 100.0;

        // Data locality
        let table_name = partition.table.to_string();
        if load.tables.contains(&table_name) {
            score += 50.0;
        }

        // Load balancing
        let load_factor =
            load.partition_count as f64 / self.config.max_partitions_per_executor as f64;
        score -= load_factor * 40.0;

        score.max(0.0)
    }

    async fn commit_assignments(&self, assignments: Vec<Assignment>) -> Result<CommitResult> {
        let mut committed = Vec::new();
        let mut failed = Vec::new();

        for assignment in assignments {
            let table_key = assignment.table.to_string();

            // Attempt to assign partition with retries
            match self
                .assign_partition_with_retry(
                    &assignment.table,
                    &assignment.partition_value,
                    &assignment.executor_id,
                )
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        table = %table_key,
                        partition = ?assignment.partition_value,
                        executor = %assignment.executor_id,
                        "Partition assigned"
                    );
                    committed.push(assignment);
                }
                Err(e) => {
                    tracing::warn!(
                        table = %table_key,
                        partition = ?assignment.partition_value,
                        executor = %assignment.executor_id,
                        error = %e,
                        "Failed to assign partition"
                    );
                    failed.push((assignment, e));
                }
            }
        }

        tracing::info!(
            committed_count = committed.len(),
            failed_count = failed.len(),
            "Committed partition assignments"
        );

        Ok(CommitResult { committed, failed })
    }

    async fn assign_partition_with_retry(
        &self,
        table: &TableReference,
        partition_value: &PartitionValue,
        executor_id: &str,
    ) -> Result<()> {
        let mut retries = 0;
        let max_retries = 3;

        loop {
            match self
                .partition_manager
                .assign_partition(table, partition_value, executor_id)
                .await
            {
                Ok(()) => return Ok(()),
                Err(crate::cluster::partition::manager::Error::ConcurrentModification {
                    ..
                }) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(Error::MaxRetriesExceeded {
                            table: table.to_string(),
                            partition: format!("{partition_value:?}"),
                            retries,
                        });
                    }
                    tracing::debug!(
                        table = %table,
                        partition = ?partition_value,
                        "Concurrent modification detected, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(retries))).await;
                    continue;
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

    async fn notify_executors(&self, committed: Vec<Assignment>) -> Result<()> {
        // Group assignments by executor
        let mut by_executor: HashMap<String, Vec<Assignment>> = HashMap::new();
        for assignment in committed {
            by_executor
                .entry(assignment.executor_id.clone())
                .or_default()
                .push(assignment);
        }

        // Send notifications in parallel
        let notifications: Vec<_> = by_executor
            .into_iter()
            .map(|(executor_id, assignments)| {
                let registry = self.executor_registry.clone();
                let df = self.df.clone();
                async move {
                    notify_executor_of_assignments(registry, df, executor_id, assignments).await
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

        tracing::info!(
            success_count,
            failure_count,
            "Notified executors of partition assignments"
        );

        Ok(())
    }
}

async fn notify_executor_of_assignments(
    registry: Arc<ExecutorRegistry>,
    df: Arc<DataFusion>,
    executor_id: String,
    assignments: Vec<Assignment>,
) -> Result<()> {
    // Group by table for more efficient loading
    let mut by_table: HashMap<TableReference, Vec<PartitionValue>> = HashMap::new();
    for assignment in assignments {
        by_table
            .entry(assignment.table)
            .or_default()
            .push(assignment.partition_value);
    }

    // Send UpdatePartitions command via control stream
    for (table, partition_values) in by_table {
        let mut partitions_bytes = Vec::new();
        for p in partition_values {
            let bytes = partition_value_to_bytes(p, &table, &df)
                .await
                .context(PartitionValueConversionSnafu)?;
            partitions_bytes.push(bytes.to_vec());
        }

        let new_partitions = HashMap::from([(
            table.to_string(),
            BytesArray {
                items: partitions_bytes,
            },
        )]);

        let update_message = UpdatePartitions {
            new_partitions,
            removed_partitions: HashMap::new(),
        };

        let command = SchedulerControlMessage {
            message: Some(SchedulerControlMessageEnum::UpdatePartitions(
                update_message,
            )),
        };

        registry
            .send_command(&executor_id, command)
            .await
            .context(SendCommandSnafu {
                executor_id: executor_id.clone(),
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
