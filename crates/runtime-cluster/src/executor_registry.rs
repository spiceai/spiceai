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
use runtime_proto::{Ack, MetricsRequest, MetricsResponse, SchedulerControlMessage};
use snafu::prelude::*;
use tokio::sync::{RwLock, mpsc};

use crate::correlated::{CorrelatedResponses, CorrelationError, send_correlated};
use crate::{PartitionStore, PartitionValue, executor_selection};

/// Error type for executor registry operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to send control message to executor {executor_id}"))]
    SendFailed { executor_id: String },

    #[snafu(display("Failed to receive metrics response from executor {executor_id}: {reason}"))]
    ReceiveFailed { executor_id: String, reason: String },

    #[snafu(display("Timed out waiting for ack from executor {executor_id} after {duration:?}"))]
    AckTimeout {
        executor_id: String,
        duration: std::time::Duration,
    },

    #[snafu(display("Executor {executor_id} reported failure applying command: {error}"))]
    AckFailed { executor_id: String, error: String },

    #[snafu(display("Executor {executor_id} not registered"))]
    ExecutorNotRegistered { executor_id: String },

    #[snafu(display("Metrics collection failed for executors: [{failed_executors}]"))]
    PartialFailure { failed_executors: String },
}

impl Error {
    /// Returns true if this error indicates a transient condition where the
    /// caller should retry (e.g. executor not yet ready). Returns false for
    /// permanent failures (e.g. executor unregistered).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::AckTimeout { .. } | Error::AckFailed { .. })
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a single executor's control stream connection.
#[derive(Debug)]
pub struct ExecutorConnection {
    /// Channel to send control messages to this executor.
    request_tx: mpsc::Sender<SchedulerControlMessage>,
    /// Pending metrics requests awaiting responses, keyed by `request_id`.
    pending_metrics: CorrelatedResponses<MetricsResponse>,
    /// Pending control-command acks awaiting responses, keyed by `request_id`.
    /// Used by commands (e.g. `UpdatePartitions`) that need delivery
    /// confirmation rather than fire-and-forget.
    pending_acks: CorrelatedResponses<Ack>,
}

impl ExecutorConnection {
    /// Creates a new executor connection.
    #[must_use]
    pub fn new(request_tx: mpsc::Sender<SchedulerControlMessage>) -> Self {
        Self {
            request_tx,
            pending_metrics: CorrelatedResponses::new(),
            pending_acks: CorrelatedResponses::new(),
        }
    }

    /// Returns a cheap clone of the pending-metrics registry. Used by the
    /// control-stream inbound handler to deliver `MetricsResponse` messages.
    #[must_use]
    pub fn pending_metrics(&self) -> CorrelatedResponses<MetricsResponse> {
        self.pending_metrics.clone()
    }

    /// Returns a cheap clone of the pending-acks registry. Used by the
    /// control-stream inbound handler to deliver `Ack` messages, and by
    /// notify-with-ack call sites to await delivery confirmation.
    #[must_use]
    pub fn pending_acks(&self) -> CorrelatedResponses<Ack> {
        self.pending_acks.clone()
    }

    /// Sends a metrics request to this executor and waits for the response.
    async fn request_metrics(&self, executor_id: &str) -> Result<MetricsResponse> {
        send_correlated(
            &self.request_tx,
            &self.pending_metrics,
            |request_id| SchedulerControlMessage {
                message: Some(
                    runtime_proto::scheduler_control_message::Message::RequestMetrics(
                        MetricsRequest { request_id },
                    ),
                ),
            },
            None,
        )
        .await
        .map_err(|e| match e {
            CorrelationError::SendFailed => Error::SendFailed {
                executor_id: executor_id.to_string(),
            },
            CorrelationError::Cancelled => Error::ReceiveFailed {
                executor_id: executor_id.to_string(),
                reason: "response channel closed".to_string(),
            },
            CorrelationError::Timeout { duration } => Error::ReceiveFailed {
                executor_id: executor_id.to_string(),
                reason: format!("timed out after {duration:?}"),
            },
        })
    }
}

pub type TablePartitions = HashMap<TableReference, Vec<Expr>>;

/// Cheap-to-clone handles returned to the control-stream inbound dispatcher
/// at registration time. Routes correlated executor→scheduler messages
/// (metrics responses, command acks) to whoever is awaiting them.
#[derive(Debug, Clone)]
pub struct RegisteredHandles {
    pub pending_metrics: CorrelatedResponses<MetricsResponse>,
    pub pending_acks: CorrelatedResponses<Ack>,
}

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
    ) -> RegisteredHandles {
        let connection = ExecutorConnection::new(request_tx);
        let handles = RegisteredHandles {
            pending_metrics: connection.pending_metrics(),
            pending_acks: connection.pending_acks(),
        };

        let mut connections = self.connections.write().await;
        if connections.contains_key(&executor_id) {
            tracing::debug!("Executor {executor_id} reconnected, replacing existing connection");
        } else {
            tracing::debug!("Executor {executor_id} connected");
        }
        connections.insert(executor_id, connection);

        handles
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

    /// Sends a control message to a specific executor and waits for an Ack
    /// correlated by `request_id`.
    ///
    /// `build_command` is given a freshly generated `request_id` and must
    /// place it onto the underlying message payload (e.g. into
    /// `UpdatePartitions::request_id`). The executor's message handler is
    /// expected to send a matching `ExecutorMessage::Ack` back via the
    /// control stream.
    ///
    /// # Errors
    ///
    /// - [`Error::ExecutorNotRegistered`] if the target is not in the registry.
    /// - [`Error::SendFailed`] if delivery to the control stream channel fails.
    /// - [`Error::AckTimeout`] if no ack arrives within `timeout`.
    /// - [`Error::AckFailed`] if the executor reports an application error.
    pub async fn send_command_with_ack(
        &self,
        executor_id: &str,
        build_command: impl FnOnce(String) -> SchedulerControlMessage + Send,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let (request_tx, pending_acks) = {
            let connections = self.connections.read().await;
            let Some(connection) = connections.get(executor_id) else {
                return Err(Error::ExecutorNotRegistered {
                    executor_id: executor_id.to_string(),
                });
            };
            (connection.request_tx.clone(), connection.pending_acks())
        };

        match send_correlated(&request_tx, &pending_acks, build_command, Some(timeout)).await {
            Ok(ack) => match ack.error {
                Some(error) if !error.is_empty() => Err(Error::AckFailed {
                    executor_id: executor_id.to_string(),
                    error,
                }),
                _ => Ok(()),
            },
            Err(CorrelationError::SendFailed) => Err(Error::SendFailed {
                executor_id: executor_id.to_string(),
            }),
            Err(CorrelationError::Cancelled) => Err(Error::ReceiveFailed {
                executor_id: executor_id.to_string(),
                reason: "ack channel closed".to_string(),
            }),
            Err(CorrelationError::Timeout { duration }) => Err(Error::AckTimeout {
                executor_id: executor_id.to_string(),
                duration,
            }),
        }
    }

    /// Sends a control message to a specific executor without waiting for
    /// acknowledgement.
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
            let pending_metrics = connection.pending_metrics();

            handles.push(tokio::spawn(async move {
                let temp_connection = ExecutorConnection {
                    request_tx,
                    pending_metrics,
                    pending_acks: CorrelatedResponses::new(),
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
    pub fn resolve_accelerated_partitions(
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

        get_partitions_from_store(
            &self.accelerations_partition_store,
            &connections,
            &flight_sql_clients,
            table,
            schema,
        )
    }
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
/// Uses the given [`PartitionStore`] to look up partition metadata, validates liveness against
/// `connections`, selects a minimal executor set, and returns `(FlightSQL provider, partition values)` pairs.
pub(crate) fn get_partitions_from_store(
    partition_store: &PartitionStore,
    connections: &HashMap<String, ExecutorConnection>,
    flight_sql_clients: &HashMap<String, FlightSqlClient>,
    table: &TableReference,
    schema: &SchemaRef,
) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
    let Some(table_metadata) = partition_store.get_cached_table_metadata(table) else {
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

        return vec![(
            flight_sql_table_provider(executor_id, client.clone(), table, Arc::clone(schema)),
            Vec::new(),
        )];
    };

    // All required partitions (future: filter by query predicates)
    let required_partitions: Vec<HashMap<String, Option<String>>> = table_metadata
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
        let Ok(flight_sql_clients) = self.flight_sql_clients.try_read() else {
            tracing::warn!("Failed to acquire read lock on flight_sql_clients");
            return Vec::new();
        };
        let Ok(connections) = self.connections.try_read() else {
            tracing::warn!("Failed to acquire read lock on connections");
            return Vec::new();
        };

        get_partitions_from_store(
            &self.partition_store,
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

    /// Spawn a tiny fake executor: receives `SchedulerControlMessage`s from
    /// `rx`, extracts the request_id from `UpdatePartitions`, and delivers an
    /// `Ack` (with the provided error, if any) via `pending_acks`.
    fn spawn_fake_executor_ack(
        mut rx: mpsc::Receiver<SchedulerControlMessage>,
        pending_acks: CorrelatedResponses<Ack>,
        responder_error: Option<String>,
    ) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Some(runtime_proto::scheduler_control_message::Message::UpdatePartitions(
                    up,
                )) = msg.message
                {
                    let request_id = up.request_id.clone();
                    if request_id.is_empty() {
                        continue; // legacy fire-and-forget
                    }
                    pending_acks.deliver(
                        &request_id,
                        Ack {
                            request_id,
                            error: responder_error.clone(),
                        },
                    );
                }
            }
        });
    }

    fn empty_update_partitions(request_id: String) -> SchedulerControlMessage {
        SchedulerControlMessage {
            message: Some(
                runtime_proto::scheduler_control_message::Message::UpdatePartitions(
                    runtime_proto::UpdatePartitions {
                        new_partitions: HashMap::new(),
                        removed_partitions: HashMap::new(),
                        request_id,
                    },
                ),
            ),
        }
    }

    #[tokio::test]
    async fn send_command_with_ack_success() {
        let registry = make_registry().await;
        let (tx, rx) = mpsc::channel(8);
        let handles = registry.register("e1".to_string(), tx).await;
        spawn_fake_executor_ack(rx, handles.pending_acks.clone(), None);

        let result = registry
            .send_command_with_ack(
                "e1",
                empty_update_partitions,
                std::time::Duration::from_secs(1),
            )
            .await;

        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    #[tokio::test]
    async fn send_command_with_ack_propagates_application_error() {
        let registry = make_registry().await;
        let (tx, rx) = mpsc::channel(8);
        let handles = registry.register("e1".to_string(), tx).await;
        spawn_fake_executor_ack(
            rx,
            handles.pending_acks.clone(),
            Some("table not yet loaded".to_string()),
        );

        let err = registry
            .send_command_with_ack(
                "e1",
                empty_update_partitions,
                std::time::Duration::from_secs(1),
            )
            .await
            .expect_err("ack with error should fail");

        match err {
            Error::AckFailed { executor_id, error } => {
                assert_eq!(executor_id, "e1");
                assert_eq!(error, "table not yet loaded");
            }
            other => panic!("expected AckFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_command_with_ack_times_out_when_no_response() {
        let registry = make_registry().await;
        let (tx, _rx) = mpsc::channel(8); // _rx kept alive but never read
        let _handles = registry.register("e1".to_string(), tx).await;

        let err = registry
            .send_command_with_ack(
                "e1",
                empty_update_partitions,
                std::time::Duration::from_millis(50),
            )
            .await
            .expect_err("missing ack should time out");

        assert!(matches!(err, Error::AckTimeout { .. }), "got {err:?}");
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn send_command_with_ack_unknown_executor() {
        let registry = make_registry().await;
        let err = registry
            .send_command_with_ack(
                "ghost",
                empty_update_partitions,
                std::time::Duration::from_millis(10),
            )
            .await
            .expect_err("unknown executor should fail");

        assert!(matches!(err, Error::ExecutorNotRegistered { .. }), "got {err:?}");
        assert!(!err.is_retryable());
    }
}
