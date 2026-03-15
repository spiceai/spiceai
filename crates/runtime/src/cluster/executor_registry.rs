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

//! Executor registry for tracking executor control stream connections.
//!
//! This module provides the `ExecutorRegistry` which manages bidirectional
//! control streams between schedulers and executors. Schedulers use this
//! registry to request metrics from executors on-demand.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use data_components::flightsql::{FlightSQLTable, FlightSqlClient};
use datafusion::{catalog::TableProvider, datasource::DefaultTableSource, sql::TableReference};
use datafusion_expr::{Expr, TableScan};
use datafusion_federation::FederatedTableProviderAdaptor;
use flight_client::cookie::CookieStore;
use runtime_datafusion::analyzer_rule::TablePartitionProvider;
use runtime_proto::{MetricsRequest, MetricsResponse, SchedulerControlMessage};
use snafu::prelude::*;
use tokio::sync::{RwLock, mpsc, oneshot};
use uuid::Uuid;

use crate::{
    accelerated_table::AcceleratedTable,
    cluster::{
        PartitionManager,
        partition::{PartitionValue, executor_selection},
    },
};
#[cfg(not(windows))]
use cayenne::CayenneTableProvider;

/// Error type for executor registry operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to send control message to executor {executor_id}"))]
    SendFailed { executor_id: String },

    #[snafu(display("Failed to receive metrics response from executor {executor_id}: {reason}"))]
    ReceiveFailed { executor_id: String, reason: String },

    #[snafu(display("Metrics collection failed for executors: [{failed_executors}]"))]
    PartialFailure { failed_executors: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a single executor's control stream connection.
#[derive(Debug)]
pub struct ExecutorConnection {
    /// Channel to send control messages to this executor
    request_tx: mpsc::Sender<SchedulerControlMessage>,
    /// Pending metrics requests awaiting responses
    pending_requests: Arc<RwLock<HashMap<String, oneshot::Sender<MetricsResponse>>>>,
}

impl ExecutorConnection {
    /// Creates a new executor connection.
    #[must_use]
    pub fn new(request_tx: mpsc::Sender<SchedulerControlMessage>) -> Self {
        Self {
            request_tx,
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns a clone of the pending requests map for handling responses.
    #[must_use]
    pub fn pending_requests(
        &self,
    ) -> Arc<RwLock<HashMap<String, oneshot::Sender<MetricsResponse>>>> {
        Arc::clone(&self.pending_requests)
    }

    /// Sends a metrics request to this executor and waits for the response.
    async fn request_metrics(&self, executor_id: &str) -> Result<MetricsResponse> {
        let request_id = Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();

        // Register the pending request
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(request_id.clone(), response_tx);
        }

        // Send the metrics request
        let message = SchedulerControlMessage {
            message: Some(
                runtime_proto::scheduler_control_message::Message::RequestMetrics(MetricsRequest {
                    request_id: request_id.clone(),
                }),
            ),
        };

        if self.request_tx.send(message).await.is_err() {
            // Clean up the pending request on send failure
            let mut pending = self.pending_requests.write().await;
            pending.remove(&request_id);
            return Err(Error::SendFailed {
                executor_id: executor_id.to_string(),
            });
        }

        // Wait for the response
        response_rx.await.map_err(|_| Error::ReceiveFailed {
            executor_id: executor_id.to_string(),
            reason: "response channel closed".to_string(),
        })
    }
}

pub type TablePartitions = HashMap<TableReference, Vec<Expr>>;

/// Registry for tracking executor control stream connections.
///
/// Schedulers use this registry to:
/// - Register executors when they connect via control stream
/// - Unregister executors when they disconnect
/// - Request metrics from all connected executors
#[derive(Debug)]
pub struct ExecutorRegistry {
    /// Map of `executor_id` -> connection
    connections: Arc<RwLock<HashMap<String, ExecutorConnection>>>,

    /// Map of `executor_id` -> `FlightSqlClient`
    /// An executor may be in `connections` and not in `flight_sql_clients` (e.g. during initial connection).
    pub flight_sql_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,

    /// Map of `executor_id` -> table partitions for that executor
    pub partitions: Arc<RwLock<HashMap<String, TablePartitions>>>,

    /// Manager for accelerated partition metadata. Used to validate partition completeness
    /// and optimize executor selection. If None, fallback to legacy behavior.
    accelerations_partition_manager: Arc<PartitionManager>,

    federated_partition_manager: Arc<PartitionManager>,
}

impl ExecutorRegistry {
    /// Creates a new executor registry.
    #[must_use]
    pub fn new(
        accelerations_partition_manager: Arc<PartitionManager>,
        federated_partition_manager: Arc<PartitionManager>,
    ) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            flight_sql_clients: Arc::new(RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            accelerations_partition_manager,
            federated_partition_manager,
        }
    }

    #[must_use]
    pub fn accelerations_partition_manager(&self) -> Arc<PartitionManager> {
        Arc::clone(&self.accelerations_partition_manager)
    }

    #[must_use]
    pub fn federated_partition_manager(&self) -> Arc<PartitionManager> {
        Arc::clone(&self.federated_partition_manager)
    }

    /// Registers an executor connection.
    ///
    /// If an executor with the same ID is already registered, the old connection
    /// is replaced (the executor reconnected).
    pub async fn register(
        &self,
        executor_id: String,
        request_tx: mpsc::Sender<SchedulerControlMessage>,
    ) -> Arc<RwLock<HashMap<String, oneshot::Sender<MetricsResponse>>>> {
        let connection = ExecutorConnection::new(request_tx);
        let pending_requests = connection.pending_requests();

        let mut connections = self.connections.write().await;
        if connections.contains_key(&executor_id) {
            tracing::debug!("Executor {executor_id} reconnected, replacing existing connection");
        } else {
            tracing::debug!("Executor {executor_id} connected");
        }
        connections.insert(executor_id, connection);

        pending_requests
    }

    /// Unregisters an executor connection.
    pub async fn unregister(&self, executor_id: &str) {
        let mut connections = self.connections.write().await;
        if connections.remove(executor_id).is_some() {
            tracing::debug!("Executor {executor_id} disconnected");
        }
    }

    /// Returns the list of currently connected executor IDs.
    pub async fn connected_executors(&self) -> Vec<String> {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect()
    }

    /// Sends a control message to a specific executor.
    pub async fn send_command(
        &self,
        executor_id: &str,
        command: SchedulerControlMessage,
    ) -> Result<()> {
        let connections = self.connections.read().await;

        if let Some(connection) = connections.get(executor_id) {
            let tx = connection.request_tx.clone();
            drop(connections);

            tx.send(command).await.map_err(|_| Error::SendFailed {
                executor_id: executor_id.to_string(),
            })?;
            Ok(())
        } else {
            Err(Error::SendFailed {
                executor_id: executor_id.to_string(),
            })
        }
    }

    /// Requests metrics from all connected executors.
    ///
    /// Returns a list of (`executor_id`, `otlp_metrics`) tuples for successful responses.
    /// If any executor fails, returns an error containing the list of failed executors.
    pub async fn request_metrics_from_all(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let connections = self.connections.read().await;

        if connections.is_empty() {
            return Ok(Vec::new());
        }

        // Spawn metrics requests to all executors in parallel
        let mut handles = Vec::with_capacity(connections.len());
        for (executor_id, connection) in connections.iter() {
            let executor_id = executor_id.clone();
            let request_tx = connection.request_tx.clone();
            let pending_requests = connection.pending_requests();

            handles.push(tokio::spawn(async move {
                let temp_connection = ExecutorConnection {
                    request_tx,
                    pending_requests,
                };
                let result = temp_connection.request_metrics(&executor_id).await;
                (executor_id, result)
            }));
        }

        drop(connections); // Release lock while waiting for responses

        // Collect results
        let mut results = Vec::new();
        let mut failures = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((executor_id, Ok(response))) => {
                    results.push((executor_id, response.otlp_metrics));
                }
                Ok((executor_id, Err(e))) => {
                    failures.push(format!("{executor_id}: {e}"));
                }
                Err(e) => {
                    failures.push(format!("task panic: {e}"));
                }
            }
        }

        if failures.is_empty() {
            Ok(results)
        } else {
            Err(Error::PartialFailure {
                failed_executors: failures.join(", "),
            })
        }
    }
}

fn is_accelerated_table_provider(table_provider: &Arc<dyn TableProvider>) -> bool {
    if table_provider
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .is_some()
    {
        return true;
    }

    if let Some(adaptor) = table_provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
        && let Some(inner_provider) = adaptor.table_provider.as_ref()
    {
        return inner_provider
            .as_any()
            .downcast_ref::<AcceleratedTable>()
            .is_some();
    }

    false
}

fn flight_sql_table_provider(
    executor_id: &str,
    client: FlightSqlClient,
    table: &TableReference,
    schema: SchemaRef,
) -> Arc<dyn TableProvider> {
    Arc::new(FlightSQLTable::create_with_schema(
        "flightsql",
        executor_id,
        client,
        table.clone(),
        schema,
        Arc::new(CookieStore::new()),
    )) as Arc<dyn TableProvider>
}

/// Shared logic for `get_partitions` across accelerated and federated partition providers.
///
/// Uses the given [`PartitionManager`] to look up partition metadata, validates liveness against
/// `connections`, selects a minimal executor set, and returns `(FlightSQL provider, partition values)` pairs.
fn get_partitions_from_manager(
    partition_manager: &PartitionManager,
    connections: &HashMap<String, ExecutorConnection>,
    flight_sql_clients: &HashMap<String, FlightSqlClient>,
    table: &TableReference,
    schema: &SchemaRef,
) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
    let Some(table_metadata) = partition_manager.get_cached_table_metadata(table) else {
        // No partition metadata — route to a single live executor to avoid duplicate results.
        let Some((executor_id, client)) = flight_sql_clients
            .iter()
            .filter(|(eid, _)| connections.contains_key(*eid))
            .min_by(|(a, _), (b, _)| a.cmp(b))
        else {
            tracing::warn!(
                "No partition assignments for table {table:?} and no connected executors with FlightSQL clients"
            );
            return Vec::new();
        };

        tracing::debug!(
            "No partition assignments for table {table:?}; routing query to executor '{executor_id}'"
        );

        return vec![(
            flight_sql_table_provider(executor_id, client.clone(), table, Arc::clone(schema)),
            Vec::new(),
        )];
    };

    // All required partitions (future: filter by query predicates)
    let required_partitions: Vec<HashMap<String, String>> = table_metadata
        .partitions
        .iter()
        .map(|p| p.partition_value.clone())
        .collect();

    if required_partitions.is_empty() {
        tracing::debug!("No partitions required for table {table}");
        return Vec::new();
    }

    // Build executor -> partitions map, excluding dead executors
    let mut executor_partition_map: HashMap<String, Vec<PartitionValue>> = HashMap::new();
    for partition_meta in &table_metadata.partitions {
        for executor_id in &partition_meta.assigned_executors {
            if !connections.contains_key(executor_id) {
                tracing::debug!(
                    "Executor '{}' has partition assignment but is no longer alive; excluding from selection",
                    executor_id
                );
                continue;
            }
            executor_partition_map
                .entry(executor_id.clone())
                .or_default()
                .push(partition_meta.partition_value.clone());
        }
    }

    // Select minimal set of executors to cover all partitions
    let selected_executors = match executor_selection::select_executors(
        &required_partitions,
        &executor_partition_map,
    ) {
        Ok(executors) => executors,
        Err(executor_selection::Error::MissingPartitions(missing)) => {
            tracing::error!(
                "Cannot execute query on table {}: {} partition(s) not assigned to any alive executor. Missing: {:?}",
                table,
                missing.len(),
                missing.iter().take(5).collect::<Vec<_>>()
            );
            return Vec::new();
        }
    };

    tracing::debug!(
        "Selected {} executor(s) from {} available for table {} (covering {} partition(s))",
        selected_executors.len(),
        executor_partition_map.len(),
        table,
        required_partitions.len()
    );

    selected_executors
        .into_iter()
        .filter_map(|executor_id| {
            let client = flight_sql_clients.get(&executor_id)?;
            let partition_values = executor_partition_map.remove(&executor_id)?;
            let provider =
                flight_sql_table_provider(&executor_id, client.clone(), table, Arc::clone(schema));
            Some((provider, partition_values))
        })
        .collect()
}

impl TablePartitionProvider for ExecutorRegistry {
    /// Partitions accelerated tables using the accelerations partition manager.
    fn should_partition(&self, tbl: &TableScan) -> bool {
        let Some(default) = tbl.source.as_any().downcast_ref::<DefaultTableSource>() else {
            return false;
        };
        is_accelerated_table_provider(&default.table_provider)
    }

    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        let Ok(flight_sql_clients) = self.flight_sql_clients.try_read() else {
            tracing::warn!("Failed to acquire read lock on flight_sql_clients");
            return Vec::new();
        };
        let Ok(connections) = self.connections.try_read() else {
            tracing::warn!("Failed to acquire read lock on connections");
            return Vec::new();
        };

        get_partitions_from_manager(
            &self.accelerations_partition_manager,
            &connections,
            &flight_sql_clients,
            table,
            schema,
        )
    }
}

/// Partition provider for federated (non-accelerated) tables such as Cayenne tables.
///
/// Uses the `federated_partition_manager` from [`ExecutorRegistry`] to route queries
/// to the correct executors.
#[derive(Debug)]
pub struct FederatedPartitionProvider {
    connections: Arc<RwLock<HashMap<String, ExecutorConnection>>>,
    flight_sql_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,
    partition_manager: Arc<PartitionManager>,
}

impl FederatedPartitionProvider {
    /// Creates a new `FederatedPartitionProvider` from an [`ExecutorRegistry`].
    #[must_use]
    pub fn from_registry(registry: &ExecutorRegistry) -> Self {
        Self {
            connections: Arc::clone(&registry.connections),
            flight_sql_clients: Arc::clone(&registry.flight_sql_clients),
            partition_manager: Arc::clone(&registry.federated_partition_manager),
        }
    }
}

impl TablePartitionProvider for FederatedPartitionProvider {
    fn should_partition(&self, tbl: &TableScan) -> bool {
        self.partition_manager
            .get_cached_table_metadata(&tbl.table_name)
            .is_some()
        //     &&
        // let Some(default) = tbl.source.as_any().downcast_ref::<DefaultTableSource>() else {
        //     return false;
        // };

        // #[cfg(not(windows))]
        // if default
        //     .table_provider
        //     .as_any()
        //     .downcast_ref::<CayenneTableProvider>()
        //     .is_some()
        // {
        //     // TODO: this is the bug. Most likely this bad boy is wrapped like crazy.
        //     return true;
        // }

        // let _ = default; // suppress unused warning on windows
        // false
    }

    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        let Ok(flight_sql_clients) = self.flight_sql_clients.try_read() else {
            tracing::warn!("Failed to acquire read lock on flight_sql_clients");
            return Vec::new();
        };
        let Ok(connections) = self.connections.try_read() else {
            tracing::warn!("Failed to acquire read lock on connections");
            return Vec::new();
        };

        get_partitions_from_manager(
            &self.partition_manager,
            &connections,
            &flight_sql_clients,
            table,
            schema,
        )
        .into_iter()
        .map(|(provider, _)| (provider, vec![])) // For now, do not need partition values. Executors only have required data.
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn test_register_unregister() {
        let registry = ExecutorRegistry::new(
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
        );
        let (tx, _rx) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx).await;

        let executors = registry.connected_executors().await;
        assert_eq!(executors, vec!["executor-1"]);

        registry.unregister("executor-1").await;

        let executors = registry.connected_executors().await;
        assert!(executors.is_empty());
    }

    #[tokio::test]
    async fn test_reconnect_replaces_connection() {
        let registry = ExecutorRegistry::new(
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
        );
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx1).await;
        registry.register("executor-1".to_string(), tx2).await;

        let executors = registry.connected_executors().await;
        assert_eq!(executors.len(), 1);
    }

    #[tokio::test]
    async fn test_request_metrics_empty_registry() {
        let registry = ExecutorRegistry::new(
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
        );
        let result = registry.request_metrics_from_all().await;
        assert!(result.is_ok());
        assert!(result.expect("should succeed").is_empty());
    }

    #[tokio::test]
    async fn test_multiple_executors() {
        let registry = ExecutorRegistry::new(
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
        );
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx1).await;
        registry.register("executor-2".to_string(), tx2).await;
        registry.register("executor-3".to_string(), tx3).await;

        let mut executors = registry.connected_executors().await;
        executors.sort(); // Sort for deterministic comparison

        assert_eq!(executors.len(), 3);
        assert_eq!(executors, vec!["executor-1", "executor-2", "executor-3"]);

        // Unregister one
        registry.unregister("executor-2").await;

        let mut executors = registry.connected_executors().await;
        executors.sort();

        assert_eq!(executors.len(), 2);
        assert_eq!(executors, vec!["executor-1", "executor-3"]);
    }

    #[tokio::test]
    async fn test_unregister_nonexistent() {
        let registry = ExecutorRegistry::new(
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
            Arc::new(PartitionManager::new(Arc::new(InMemory::new()))),
        );
        let (tx, _rx) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx).await;

        // Unregistering a non-existent executor should not panic
        registry.unregister("executor-nonexistent").await;

        // Original executor should still be registered
        let executors = registry.connected_executors().await;
        assert_eq!(executors, vec!["executor-1"]);
    }
}
