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
use datafusion::{catalog::TableProvider, sql::TableReference};
use datafusion_expr::{Expr, TableScan};
use flight_client::cookie::CookieStore;
use runtime_datafusion::analyzer_rule::TablePartitionProvider;
use runtime_proto::{MetricsRequest, MetricsResponse, SchedulerControlMessage};
use snafu::prelude::*;
use tokio::sync::{RwLock, mpsc, oneshot};
use uuid::Uuid;

use crate::{PartitionStore, PartitionValue, executor_selection};

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

/// Append-only log of DDL SQL statements applied to the cluster.
///
/// Used to replay DDL on executors that join after the statements were originally executed.
/// Each statement is stored in executor-compatible form (e.g. `IF NOT EXISTS`/`IF EXISTS`).
///
/// The version is the count of statements in the log. `statements_since(version)` returns
/// all statements appended after that version.
#[derive(Debug, Default)]
struct DdlLog {
    statements: Vec<String>,
}

impl DdlLog {
    /// Appends a DDL SQL statement. Returns the new version (count of statements).
    fn append(&mut self, sql: String) -> u64 {
        self.statements.push(sql);
        self.statements.len() as u64
    }

    /// Returns all statements appended after `since_version`.
    fn statements_since(&self, since_version: u64) -> &[String] {
        let idx = usize::try_from(since_version).unwrap_or(usize::MAX);
        if idx >= self.statements.len() {
            &[]
        } else {
            &self.statements[idx..]
        }
    }

    /// Returns all statements and the current version.
    fn snapshot(&self) -> (&[String], u64) {
        (&self.statements, self.statements.len() as u64)
    }
}

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
    /// An executor with a `FlightSqlClient` is considered "ready" — the scheduler can route queries to it.
    flight_sql_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,

    /// Map of `executor_id` -> table partitions for that executor
    partitions: Arc<RwLock<HashMap<String, TablePartitions>>>,

    /// Manager for accelerated partition metadata. Used to validate partition completeness
    /// and optimize executor selection. If None, fallback to legacy behavior.
    accelerations_partition_store: Arc<PartitionStore>,

    federated_partition_store: Arc<PartitionStore>,

    /// Append-only log of DDL SQL statements applied to the cluster.
    ddl_log: Arc<RwLock<DdlLog>>,
}

impl ExecutorRegistry {
    /// Creates a new executor registry.
    #[must_use]
    pub fn new(
        accelerations_partition_store: Arc<PartitionStore>,
        federated_partition_store: Arc<PartitionStore>,
    ) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            flight_sql_clients: Arc::new(RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            accelerations_partition_store,
            federated_partition_store,
            ddl_log: Arc::new(RwLock::new(DdlLog::default())),
        }
    }

    #[must_use]
    pub fn accelerations_partition_store(&self) -> Arc<PartitionStore> {
        Arc::clone(&self.accelerations_partition_store)
    }

    #[must_use]
    pub fn federated_partition_store(&self) -> Arc<PartitionStore> {
        Arc::clone(&self.federated_partition_store)
    }

    /// Appends a DDL SQL statement to the cluster DDL log.
    ///
    /// Must be called **before** forwarding to executors so that a concurrent
    /// `GetAppDefinition` will include the statement in its snapshot.
    pub async fn append_ddl(&self, sql: String) {
        let version = self.ddl_log.write().await.append(sql);
        tracing::debug!(ddl_version = version, "Appended DDL to cluster log");
    }

    /// Returns a snapshot of all DDL statements and the current version.
    pub async fn ddl_snapshot(&self) -> (Vec<String>, u64) {
        let log = self.ddl_log.read().await;
        let (stmts, version) = log.snapshot();
        (stmts.to_vec(), version)
    }

    /// Returns DDL statements appended after `since_version`.
    pub async fn ddl_statements_since(&self, since_version: u64) -> Vec<String> {
        self.ddl_log
            .read()
            .await
            .statements_since(since_version)
            .to_vec()
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

    /// Unregisters an executor and removes it from all three tracking maps.
    pub async fn unregister(&self, executor_id: &str) {
        if self.connections.write().await.remove(executor_id).is_some() {
            tracing::debug!("Executor {executor_id} disconnected");
        }
        self.flight_sql_clients.write().await.remove(executor_id);
        self.partitions.write().await.remove(executor_id);
    }

    /// Returns `true` if at least one executor has an active `FlightSqlClient`.
    pub async fn has_flight_sql_clients(&self) -> bool {
        !self.flight_sql_clients.read().await.is_empty()
    }

    /// Returns a point-in-time snapshot of all `executor_id` → `FlightSqlClient` mappings.
    pub async fn flight_sql_clients_snapshot(&self) -> HashMap<String, FlightSqlClient> {
        self.flight_sql_clients.read().await.clone()
    }

    /// Inserts or replaces the `FlightSqlClient` for `executor_id`.
    pub async fn insert_flight_sql_client(&self, executor_id: String, client: FlightSqlClient) {
        self.flight_sql_clients
            .write()
            .await
            .insert(executor_id, client);
    }

    /// Replaces the cached `TablePartitions` for `executor_id`.
    pub async fn set_executor_partitions(&self, executor_id: String, partitions: TablePartitions) {
        self.partitions
            .write()
            .await
            .insert(executor_id, partitions);
    }

    /// Returns a shared handle to the `flight_sql_clients` map.
    ///
    /// Prefer the snapshot/mutation methods above for most use cases. Use this
    /// only when a long-lived `Arc` to the map is required (e.g. background tasks
    /// that need to observe live updates).
    #[must_use]
    pub fn flight_sql_clients_handle(&self) -> Arc<RwLock<HashMap<String, FlightSqlClient>>> {
        Arc::clone(&self.flight_sql_clients)
    }

    /// Returns a point-in-time snapshot of `executor_id` → `TablePartitions` mappings.
    pub async fn executor_partitions_snapshot(&self) -> HashMap<String, TablePartitions> {
        self.partitions.read().await.clone()
    }

    /// Returns the number of executors that currently have a `FlightSqlClient` — i.e. the
    /// scheduler can route queries to them. This is the "ready executor count" used by
    /// `/v1/ready` query-param gating.
    pub async fn flight_sql_clients_count(&self) -> usize {
        self.flight_sql_clients.read().await.len()
    }

    /// Returns the number of executors currently registered via control stream.
    ///
    /// An executor is "registered" once its control stream is open but may not yet be "ready"
    /// (queryable via `FlightSQL`) — the window between `register()` and the executor's first
    /// `AllocateInitialPartitions` RPC. Used as the denominator for `/v1/ready` percentage gating.
    pub async fn connected_executor_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Returns the list of currently connected executor IDs.
    pub async fn connected_executors(&self) -> Vec<String> {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect()
    }

    /// Sends a control message to a specific executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the executor is not registered or the channel send fails.
    pub async fn send_command(
        &self,
        executor_id: &str,
        command: SchedulerControlMessage,
    ) -> Result<()> {
        let connections = self.connections.read().await;

        if let Some(connection) = connections.get(executor_id) {
            let tx = connection.request_tx.clone();
            drop(connections);

            tx.send(command).await.map_err(|e| {
                tracing::error!("Failed to send command to executor {executor_id}: {e}");
                Error::SendFailed {
                    executor_id: executor_id.to_string(),
                }
            })?;
            Ok(())
        } else {
            tracing::error!(
                "Failed to send command to executor: missing executor '{executor_id}' in registry"
            );
            Err(Error::SendFailed {
                executor_id: executor_id.to_string(),
            })
        }
    }

    /// Requests metrics from all connected executors.
    ///
    /// Returns a list of (`executor_id`, `otlp_metrics`) tuples for successful responses.
    /// If any executor fails, returns an error containing the list of failed executors.
    ///
    /// # Errors
    ///
    /// Returns an error if one or more executors fail to respond with metrics.
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

    /// Resolves a table's partitions using the accelerations partition store and returns
    /// one `FlightSQL` table provider per selected executor. The caller decides whether the
    /// table should actually be partitioned (e.g. `AcceleratedPartitionProvider` checks
    /// `AcceleratedTable` downcast in the runtime crate).
    ///
    /// This function is called from a synchronous `TablePartitionProvider::get_partitions`
    /// implementation during `DataFusion` query planning, which runs on Tokio runtime threads.
    /// `block_in_place` moves the current thread out of the Tokio worker pool for the
    /// duration of the lock acquisitions, preventing async task starvation.
    #[must_use]
    pub fn resolve_accelerated_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        tokio::task::block_in_place(|| {
            let connections = self.connections.blocking_read();
            let flight_sql_clients = self.flight_sql_clients.blocking_read();

            let executors = ready_executors(&connections, &flight_sql_clients);

            get_partitions_from_store(
                &self.accelerations_partition_store,
                &executors,
                table,
                schema,
            )
        })
    }
}

/// Returns executors that have both an active connection and a `FlightSQL` client.
fn ready_executors<'a>(
    connections: &'a HashMap<String, ExecutorConnection>,
    flight_sql_clients: &'a HashMap<String, FlightSqlClient>,
) -> HashMap<String, (&'a ExecutorConnection, &'a FlightSqlClient)> {
    connections
        .iter()
        .filter_map(|(id, conn)| {
            let client = flight_sql_clients.get(id)?;
            Some((id.clone(), (conn, client)))
        })
        .collect()
}

pub(crate) fn flight_sql_table_provider(
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
/// Uses the given [`PartitionStore`] to look up partition metadata, checks readiness (both an
/// active connection and a `FlightSQL` client) via the `executors` map, selects a minimal
/// executor set, and returns `(FlightSQL provider, partition values)` pairs.
pub(crate) fn get_partitions_from_store(
    partition_store: &PartitionStore,
    executors: &HashMap<String, (&ExecutorConnection, &FlightSqlClient)>,
    table: &TableReference,
    schema: &SchemaRef,
) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
    let Some(table_metadata) = partition_store.get_cached_table_metadata(table) else {
        // No partition metadata — route to a single live executor to avoid duplicate results.
        let Some((executor_id, (_, client))) = executors.iter().min_by_key(|(id, _)| id.as_str())
        else {
            tracing::warn!(
                "No partition assignments for table {table:?} and no connected executors with FlightSQL clients"
            );
            return Vec::new();
        };

        return vec![(
            flight_sql_table_provider(executor_id, (*client).clone(), table, Arc::clone(schema)),
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
            if !executors.contains_key(executor_id) {
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
            let (_, client) = executors.get(&executor_id)?;
            let partition_values = executor_partition_map.remove(&executor_id)?;
            let provider = flight_sql_table_provider(
                &executor_id,
                (*client).clone(),
                table,
                Arc::clone(schema),
            );
            Some((provider, partition_values))
        })
        .collect()
}

/// Partition provider for federated (non-accelerated) tables such as Cayenne tables.
///
/// Uses the `federated_partition_store` from [`ExecutorRegistry`] to route queries
/// to the correct executors.
#[derive(Debug)]
pub struct FederatedPartitionProvider {
    connections: Arc<RwLock<HashMap<String, ExecutorConnection>>>,
    flight_sql_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,
    partition_store: Arc<PartitionStore>,
}

impl FederatedPartitionProvider {
    /// Creates a new `FederatedPartitionProvider` from an [`ExecutorRegistry`].
    #[must_use]
    pub fn from_registry(registry: &ExecutorRegistry) -> Self {
        Self {
            connections: Arc::clone(&registry.connections),
            flight_sql_clients: Arc::clone(&registry.flight_sql_clients),
            partition_store: Arc::clone(&registry.federated_partition_store),
        }
    }
}

impl TablePartitionProvider for FederatedPartitionProvider {
    fn should_partition(&self, tbl: &TableScan) -> bool {
        self.partition_store
            .get_cached_table_metadata(&tbl.table_name)
            .is_some()
    }

    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        // `get_partitions` is called synchronously from DataFusion analyzer rules on Tokio
        // runtime threads. Use `block_in_place` to move the thread out of the worker pool
        // while holding the locks, preventing async task starvation.
        tokio::task::block_in_place(|| {
            let connections = self.connections.blocking_read();
            let flight_sql_clients = self.flight_sql_clients.blocking_read();

            let executors = ready_executors(&connections, &flight_sql_clients);

            get_partitions_from_store(&self.partition_store, &executors, table, schema)
                .into_iter()
                .map(|(provider, _)| (provider, vec![])) // For now, do not need partition values. Executors only have required data.
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use crate::cluster_state::ClusterStateStore;

    use super::*;

    async fn make_registry() -> ExecutorRegistry {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        ExecutorRegistry::new(
            Arc::new(PartitionStore::accelerations(Arc::clone(&cs))),
            Arc::new(PartitionStore::catalog(Arc::clone(&cs))),
        )
    }

    #[tokio::test]
    async fn test_register_unregister() {
        let registry = make_registry().await;
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
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx1).await;
        registry.register("executor-1".to_string(), tx2).await;

        let executors = registry.connected_executors().await;
        assert_eq!(executors.len(), 1);
    }

    #[tokio::test]
    async fn test_request_metrics_empty_registry() {
        let registry = make_registry().await;
        let result = registry.request_metrics_from_all().await;
        assert!(result.is_ok());
        assert!(result.expect("should succeed").is_empty());
    }

    #[tokio::test]
    async fn test_multiple_executors() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx1).await;
        registry.register("executor-2".to_string(), tx2).await;
        registry.register("executor-3".to_string(), tx3).await;

        let mut executors = registry.connected_executors().await;
        executors.sort();

        assert_eq!(executors.len(), 3);
        assert_eq!(executors, vec!["executor-1", "executor-2", "executor-3"]);

        // Unregister one
        registry.unregister("executor-2").await;

        let mut executors = registry.connected_executors().await;
        executors.sort();

        assert_eq!(executors.len(), 2);
        assert_eq!(executors, vec!["executor-1", "executor-3"]);
    }

    fn dummy_flight_sql_client() -> FlightSqlClient {
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::sql::client::FlightSqlServiceClient;
        use flight_client::cookie::CookieService;
        use tonic::transport::Endpoint;

        // FlightSqlClient wraps a tonic channel; these tests only exercise the registry's
        // bookkeeping, not actual flight calls. Build one with `connect_lazy` to a
        // non-routable address so no connection is ever attempted.
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let cookie_channel = CookieService::new(channel, Arc::new(CookieStore::new()));
        FlightSqlServiceClient::new_from_inner(FlightServiceClient::new(cookie_channel))
    }

    #[tokio::test]
    async fn test_ready_and_connected_count_tracking() {
        let registry = make_registry().await;

        assert_eq!(registry.connected_executor_count().await, 0);
        assert_eq!(registry.flight_sql_clients_count().await, 0);

        // Control stream opens for three executors → connected, but not yet ready.
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);
        registry.register("e1".to_string(), tx1).await;
        registry.register("e2".to_string(), tx2).await;
        registry.register("e3".to_string(), tx3).await;
        assert_eq!(registry.connected_executor_count().await, 3);
        assert_eq!(registry.flight_sql_clients_count().await, 0);

        // Two of them complete the handshake (AllocateInitialPartitions) → ready.
        registry
            .insert_flight_sql_client("e1".to_string(), dummy_flight_sql_client())
            .await;
        registry
            .insert_flight_sql_client("e2".to_string(), dummy_flight_sql_client())
            .await;
        assert_eq!(registry.connected_executor_count().await, 3);
        assert_eq!(registry.flight_sql_clients_count().await, 2);

        // Unregister one ready executor — both counts drop.
        registry.unregister("e2").await;
        assert_eq!(registry.connected_executor_count().await, 2);
        assert_eq!(registry.flight_sql_clients_count().await, 1);

        // Unregister the not-yet-ready executor — connected drops, ready unchanged.
        registry.unregister("e3").await;
        assert_eq!(registry.connected_executor_count().await, 1);
        assert_eq!(registry.flight_sql_clients_count().await, 1);
    }

    #[tokio::test]
    async fn test_unregister_nonexistent() {
        let registry = make_registry().await;
        let (tx, _rx) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx).await;

        // Unregistering a non-existent executor should not panic
        registry.unregister("executor-nonexistent").await;

        // Original executor should still be registered
        let executors = registry.connected_executors().await;
        assert_eq!(executors, vec!["executor-1"]);
    }

    /// Regression test: an executor that is in `connections` but not yet in
    /// `flight_sql_clients` (e.g. reconnected but hasn't sent
    /// `AllocateInitialPartitions` yet) must NOT appear in `ready_executors`.
    #[test]
    fn ready_executors_excludes_missing_flight_client() {
        let (tx, _rx) = mpsc::channel(1);
        let conn = ExecutorConnection::new(tx);

        let mut connections: HashMap<String, ExecutorConnection> = HashMap::new();
        connections.insert("exec-1".to_string(), conn);

        let flight_sql_clients: HashMap<String, FlightSqlClient> = HashMap::new();

        let ready = ready_executors(&connections, &flight_sql_clients);
        assert!(
            ready.is_empty(),
            "executor without FlightSQL client should not be ready"
        );
    }

    /// Verify `ready_executors` includes executors present in both maps.
    #[tokio::test]
    async fn ready_executors_includes_fully_registered() {
        let (tx, _rx) = mpsc::channel(1);
        let conn = ExecutorConnection::new(tx);

        let mut connections: HashMap<String, ExecutorConnection> = HashMap::new();
        connections.insert("exec-1".to_string(), conn);

        let mut flight_sql_clients: HashMap<String, FlightSqlClient> = HashMap::new();
        flight_sql_clients.insert("exec-1".to_string(), dummy_flight_sql_client());

        let ready = ready_executors(&connections, &flight_sql_clients);
        assert_eq!(ready.len(), 1);
        assert!(ready.contains_key("exec-1"));
    }

    /// Verify only the intersection is returned when maps partially overlap.
    #[tokio::test]
    async fn ready_executors_returns_intersection() {
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);

        let mut connections: HashMap<String, ExecutorConnection> = HashMap::new();
        connections.insert("exec-1".to_string(), ExecutorConnection::new(tx1));
        connections.insert("exec-2".to_string(), ExecutorConnection::new(tx2));

        // Only exec-2 has a FlightSQL client (exec-1 just reconnected).
        let mut flight_sql_clients: HashMap<String, FlightSqlClient> = HashMap::new();
        flight_sql_clients.insert("exec-2".to_string(), dummy_flight_sql_client());

        let ready = ready_executors(&connections, &flight_sql_clients);
        assert_eq!(ready.len(), 1);
        assert!(ready.contains_key("exec-2"));
        assert!(!ready.contains_key("exec-1"));
    }

    #[tokio::test]
    async fn test_ddl_log_empty() {
        let registry = make_registry().await;
        let (stmts, version) = registry.ddl_snapshot().await;
        assert!(stmts.is_empty());
        assert_eq!(version, 0);
        assert!(registry.ddl_statements_since(0).await.is_empty());
    }

    #[tokio::test]
    async fn test_ddl_log_append_and_snapshot() {
        let registry = make_registry().await;

        registry
            .append_ddl("CREATE SCHEMA IF NOT EXISTS \"cat\".\"s1\"".to_string())
            .await;
        registry
            .append_ddl(
                "CREATE TABLE IF NOT EXISTS \"cat\".\"s1\".\"t1\" (id BIGINT NOT NULL)".to_string(),
            )
            .await;

        let (stmts, version) = registry.ddl_snapshot().await;
        assert_eq!(version, 2);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE SCHEMA"));
        assert!(stmts[1].contains("CREATE TABLE"));
    }

    #[tokio::test]
    async fn test_ddl_log_statements_since() {
        let registry = make_registry().await;
        registry.append_ddl("stmt0".to_string()).await;
        registry.append_ddl("stmt1".to_string()).await;
        registry.append_ddl("stmt2".to_string()).await;

        assert_eq!(
            registry.ddl_statements_since(0).await,
            vec!["stmt0", "stmt1", "stmt2"]
        );
        assert_eq!(
            registry.ddl_statements_since(1).await,
            vec!["stmt1", "stmt2"]
        );
        assert_eq!(registry.ddl_statements_since(2).await, vec!["stmt2"]);
        assert!(registry.ddl_statements_since(3).await.is_empty());
        // Beyond end returns empty
        assert!(registry.ddl_statements_since(100).await.is_empty());
    }
}
