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
use flight_client::cookie::CookieStore;
use runtime_datafusion::analyzer_rule::TablePartitionProvider;
use runtime_proto::{MetricsRequest, MetricsResponse, SchedulerControlMessage};
use snafu::prelude::*;
use tokio::sync::{RwLock, mpsc, oneshot};
use uuid::Uuid;

use crate::accelerated_table::AcceleratedTable;

/// Error type for executor registry operations.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to send metrics request to executor {executor_id}: channel closed"))]
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
#[derive(Debug, Default)]
pub struct ExecutorRegistry {
    /// Map of `executor_id` -> connection
    connections: Arc<RwLock<HashMap<String, ExecutorConnection>>>,

    /// Map of `executor_id` -> `FlightSqlClient`
    /// An executor may be in `connections` and not in `flight_sql_clients` (e.g. during initial connection).
    pub flight_sql_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,

    /// Map of `executor_id` -> table partitions for that executor
    pub partitions: Arc<RwLock<HashMap<String, TablePartitions>>>,
}

impl ExecutorRegistry {
    /// Creates a new executor registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            flight_sql_clients: Arc::new(RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
        }
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

impl TablePartitionProvider for ExecutorRegistry {
    /// Determines if the given table scan should be partitioned. Executors in [`ExecutorRegistry`] will only have partitions for accelerated tables.
    fn should_partition(&self, tbl: &TableScan) -> bool {
        let Some(default) = tbl.source.as_any().downcast_ref::<DefaultTableSource>() else {
            return false;
        };
        default
            .table_provider
            .as_any()
            .downcast_ref::<AcceleratedTable>()
            .is_some()
    }

    fn get_partitions(
        &self,
        table: &TableReference,
        schema: SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<Expr>)> {
        let Ok(partitions) = self.partitions.try_read() else {
            tracing::warn!(
                "For table {table:?}, failed to acquire read lock on ExecutorRegistry partitions"
            );
            return Vec::new();
        };

        let Ok(flight_sql_clients) = self.flight_sql_clients.try_read() else {
            tracing::warn!(
                "For table {table:?}, failed to acquire read lock on ExecutorRegistry flight_sql_clients"
            );
            return Vec::new();
        };

        partitions
            .iter()
            .filter_map(|(executor_id, table_map)| {
                let parts = table_map.get(table)?;
                let Some(client) = flight_sql_clients.get(executor_id) else {
                    tracing::warn!(
                        "Executor '{executor_id}' registered with partitions for table {table:?}, but no FlightSQL client found."
                    );
                    return None;
                };
                let table_provider = Arc::new(FlightSQLTable::create_with_schema(
                    "flightsql",
                    executor_id,
                    client.clone(),
                    table.clone(),
                    Arc::clone(&schema),
                    Arc::new(CookieStore::new()),

                )) as Arc<dyn TableProvider>;

                Some((table_provider, parts.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_unregister() {
        let registry = ExecutorRegistry::new();
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
        let registry = ExecutorRegistry::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx1).await;
        registry.register("executor-1".to_string(), tx2).await;

        let executors = registry.connected_executors().await;
        assert_eq!(executors.len(), 1);
    }

    #[tokio::test]
    async fn test_request_metrics_empty_registry() {
        let registry = ExecutorRegistry::new();
        let result = registry.request_metrics_from_all().await;
        assert!(result.is_ok());
        assert!(result.expect("should succeed").is_empty());
    }

    #[tokio::test]
    async fn test_multiple_executors() {
        let registry = ExecutorRegistry::new();
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
        let registry = ExecutorRegistry::new();
        let (tx, _rx) = mpsc::channel(1);

        registry.register("executor-1".to_string(), tx).await;

        // Unregistering a non-existent executor should not panic
        registry.unregister("executor-nonexistent").await;

        // Original executor should still be registered
        let executors = registry.connected_executors().await;
        assert_eq!(executors, vec!["executor-1"]);
    }
}
