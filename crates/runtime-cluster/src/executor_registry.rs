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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use data_components::flightsql::{FlightSQLTable, FlightSqlClient};
use datafusion::{
    catalog::TableProvider,
    common::TableReference,
    common::{Statistics, stats::Precision},
    error::{DataFusionError, Result as DataFusionResult},
};
use datafusion_expr::{Expr, TableScan};
use flight_client::cookie::CookieStore;
use runtime_datafusion::analyzer_rule::{PartitionProviders, TablePartitionProvider};
use runtime_datafusion::error::SpiceExternalError;
use runtime_proto::{
    Ack, BytesArray, MetricsRequest, MetricsResponse, SchedulerControlMessage, UpdatePartitions,
};
use snafu::prelude::*;
use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;

use crate::cluster_state::now_ms;
use crate::correlated::{CorrelatedResponses, CorrelationError, send_correlated};
use crate::executor_processes::{Admission, ExecutorProcessOrder};
use crate::metadata::{PartitionKey, normalized_table_name, partition_key};
use crate::{PartitionStore, PartitionValue, executor_selection, metrics};

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
    /// Per table, the partition expression bytes this connection has acked as
    /// added by an `UpdatePartitions`, and when. A new connection starts with
    /// none.
    delivered: Arc<parking_lot::Mutex<HashMap<String, HashMap<Bytes, Instant>>>>,
}

impl ExecutorConnection {
    /// Creates a new executor connection.
    #[must_use]
    pub fn new(request_tx: mpsc::Sender<SchedulerControlMessage>) -> Self {
        Self {
            request_tx,
            pending_metrics: CorrelatedResponses::new(),
            pending_acks: CorrelatedResponses::new(),
            delivered: Arc::default(),
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

/// The partitions of a table one executor process has reported loaded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedPartitions {
    /// The process that loaded them; empty for executors that send no process id.
    pub process_id: String,
    pub partitions: HashSet<PartitionKey>,
}

/// What a scheduler knows of the data executors hold for one accelerated table.
#[derive(Debug, Default)]
struct TableCoverage {
    /// Per executor, the partitions it has reported loaded.
    loaded: HashMap<String, LoadedPartitions>,
    /// The partitions of the table when a query last found every partition
    /// loaded on a ready executor.
    last_complete: Option<HashSet<PartitionKey>>,
}

/// Cheap-to-clone handles returned to the control-stream inbound dispatcher
/// at registration time. Routes correlated executor→scheduler messages
/// (metrics responses, command acks) to whoever is awaiting them.
#[derive(Debug, Clone)]
pub struct RegisteredHandles {
    pub pending_metrics: CorrelatedResponses<MetricsResponse>,
    pub pending_acks: CorrelatedResponses<Ack>,
}

/// How a control-stream registration relates to the earlier ones under the same
/// executor id. Executors name themselves by their instance identity, which is
/// stable across restarts, and send a per-process id with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorRegistration {
    /// No earlier process is known under this id.
    New,
    /// The process that registered last connected again.
    Reconnected,
    /// Another process registered under this id: the earlier run is gone, and
    /// nothing it held — running tasks, shuffle output, loaded partitions — can
    /// be counted on. Partition assignments follow the id and stay.
    Restarted,
    /// A process older than the one registered last connected, so two running
    /// processes share the id. The connection is not registered.
    Duplicate,
}

/// What [`ExecutorRegistry::register`] returns: the handles for the connection
/// and how it relates to earlier registrations.
#[derive(Debug, Clone)]
pub struct Registered {
    pub handles: RegisteredHandles,
    pub registration: ExecutorRegistration,
    /// Identifies this connection to [`ExecutorRegistry::is_current`] and
    /// [`ExecutorRegistry::unregister`].
    pub connection: u64,
}

/// Replaced processes remembered per executor id, so a replaced process that is
/// still running is recognized when it connects again.
const REPLACED_PROCESSES_KEPT: usize = 8;

/// The processes seen under one executor id.
#[derive(Debug, Default)]
struct ExecutorProcesses {
    /// The connection registered last, if it is still registered.
    connection: Option<u64>,
    /// The process that registered last. Executors that send no process id
    /// leave this unset.
    current: Option<String>,
    /// Processes a later registration replaced, oldest first.
    replaced: VecDeque<String>,
    /// The generation each of `current` and `replaced` has in the cluster's
    /// [`ExecutorProcessOrder`]. A process the order could not be read for has none.
    generations: HashMap<String, u64>,
}

impl ExecutorProcesses {
    /// Whether process `process_id`, at `generation`, started before the known
    /// process `other`: by the cluster's order when both have a generation,
    /// else by id, as process ids sort by the creation time their host's clock read.
    fn precedes(&self, process_id: &str, generation: Option<u64>, other: &str) -> bool {
        match (generation, self.generations.get(other)) {
            (Some(generation), Some(other_generation)) => generation < *other_generation,
            _ => process_id < other,
        }
    }

    /// Whether the known process `process_id` started before the known process `other`.
    fn is_older(&self, process_id: &str, other: &str) -> bool {
        self.precedes(process_id, self.generations.get(process_id).copied(), other)
    }

    fn push_replaced(&mut self, process_id: String) {
        if self.replaced.len() == REPLACED_PROCESSES_KEPT
            && let Some(forgotten) = self.replaced.pop_front()
        {
            self.generations.remove(&forgotten);
        }
        self.replaced.push_back(process_id);
    }

    fn is_known(&self, process_id: &str) -> bool {
        self.current.as_deref() == Some(process_id)
            || self.replaced.iter().any(|replaced| replaced == process_id)
    }
}

/// An executor's `FlightSQL` endpoint.
#[derive(Debug, Clone)]
struct FlightEndpoint {
    /// `host:port`, without a scheme.
    address: String,
    /// The process that registered the endpoint; empty for executors that send
    /// no process id.
    process_id: String,
}

/// Classifies a registration by the process it comes from and updates `seen`.
/// `admission` is where a process `seen` does not know stands in the cluster's
/// order. `was_connected` says whether a connection is registered under the id.
fn classify_registration(
    seen: &mut ExecutorProcesses,
    process_id: &str,
    admission: Option<Admission>,
    was_connected: bool,
) -> ExecutorRegistration {
    if process_id.is_empty() {
        return if was_connected {
            ExecutorRegistration::Reconnected
        } else {
            ExecutorRegistration::New
        };
    }
    // A process older than the one registered last is still running alongside
    // it, whatever order they connect in.
    if seen.replaced.iter().any(|replaced| replaced == process_id) {
        return ExecutorRegistration::Duplicate;
    }
    let generation = admission.map(|admission| admission.generation);
    let superseded = admission.is_some_and(|admission| admission.superseded)
        || seen
            .current
            .as_deref()
            .is_some_and(|current| seen.precedes(process_id, generation, current));
    if let Some(generation) = generation {
        seen.generations.insert(process_id.to_string(), generation);
    }
    // A duplicate is remembered as replaced, so its endpoint is refused too.
    if superseded {
        seen.push_replaced(process_id.to_string());
        return ExecutorRegistration::Duplicate;
    }
    match seen.current.replace(process_id.to_string()) {
        None => ExecutorRegistration::New,
        Some(previous) if previous == process_id => ExecutorRegistration::Reconnected,
        Some(previous) => {
            seen.push_replaced(previous);
            ExecutorRegistration::Restarted
        }
    }
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

/// Statistics an executor reported for its local slice of a table, retaining the
/// column names so per-column stats can be projected onto a (possibly projected)
/// leaf scan schema by name.
#[derive(Debug, Clone)]
pub struct ExecutorTableStatistics {
    /// `num_rows` + positional per-column `min`/`max`/`null_count` for the executor's slice.
    pub statistics: Statistics,
    /// Column names aligned positionally with `statistics.column_statistics`.
    pub column_names: Vec<String>,
}

impl ExecutorTableStatistics {
    /// Builds a [`Statistics`] aligned to `leaf_schema`: carries the reported
    /// `num_rows` and, for each leaf column, the reported per-column stats matched
    /// by name (`Absent`/unknown when the executor reported none for that column).
    /// This lets the coordinator's planner estimate join/aggregate cardinalities
    /// from the executor's column min/max even when the leaf scan is projected.
    ///
    /// Every value the executor actually reported is downgraded to
    /// [`Precision::Inexact`]; a value it did not report stays
    /// [`Precision::Absent`]. The registry holds an
    /// asynchronous snapshot — executors rebroadcast on a 45s timer (plus a
    /// debounced write-completion nudge), so the executor's data has moved on by
    /// the time the coordinator plans against it. `Precision::Exact` is a
    /// correctness contract, not a confidence hint: `aggregate_statistics` folds
    /// `COUNT(*)`/`MIN`/`MAX` out of exact leaf statistics and substitutes them
    /// into the result, which would answer from the stale snapshot instead of the
    /// executor (issue #12303). Inexact keeps the values themselves, so the
    /// cost-based sizing these reports exist for — `should_swap_join_order` and
    /// friends read `Precision::get_value()` — is unaffected.
    #[must_use]
    pub fn projected_onto(&self, leaf_schema: &SchemaRef) -> Statistics {
        use datafusion::common::ColumnStatistics;
        let by_name: HashMap<&str, &ColumnStatistics> = self
            .column_names
            .iter()
            .map(String::as_str)
            .zip(self.statistics.column_statistics.iter())
            .collect();
        let column_statistics = leaf_schema
            .fields()
            .iter()
            .map(|f| {
                by_name
                    .get(f.name().as_str())
                    .map_or_else(ColumnStatistics::new_unknown, |c| (*c).clone())
            })
            .collect();
        Statistics {
            num_rows: self.statistics.num_rows,
            total_byte_size: Precision::Absent,
            column_statistics,
        }
        .to_inexact()
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

    /// Map of `executor_id` -> the processes seen under it. Locked only while
    /// `connections` is write-locked, or on its own for reads.
    processes: Arc<parking_lot::Mutex<HashMap<String, ExecutorProcesses>>>,

    /// Source of the ids in [`Registered::connection`].
    next_connection: Arc<AtomicU64>,

    /// Map of `executor_id` -> `FlightSqlClient`
    /// An executor may be in `connections` and not in `flight_sql_clients` (e.g. during initial connection).
    /// An executor with a `FlightSqlClient` is considered "ready" — the scheduler can route queries to it.
    flight_sql_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,

    /// Map of `executor_id` -> where its `FlightSqlClient` connects and which
    /// process registered it. Changed only while `flight_sql_clients` is
    /// write-locked.
    flight_sql_endpoints: Arc<parking_lot::RwLock<HashMap<String, FlightEndpoint>>>,

    /// Map of `executor_id` -> table partitions for that executor
    partitions: Arc<RwLock<HashMap<String, TablePartitions>>>,

    /// Manager for accelerated partition metadata. Used to validate partition completeness
    /// and optimize executor selection. If None, fallback to legacy behavior.
    accelerations_partition_store: Arc<PartitionStore>,

    federated_partition_store: Arc<PartitionStore>,

    /// Per-(table, executor) statistics reported by executors on their
    /// `PartitionsLoaded` acks (today: `num_rows` summed from each executor's
    /// local accelerated partition record counts). Held in memory rather than in
    /// `cluster.json` because it is ack-derived, ephemeral optimizer-hint state
    /// (mirroring [`crate::PartitionLoadTracker`]): persisting it per ack would
    /// hammer the shared cluster document and starve the ingest path's own
    /// partition-assignment writes. Each scheduler receives every executor's
    /// broadcast, so its in-memory view is complete; it is re-populated by the
    /// executors' next refresh after a scheduler restart. Read at query-planning
    /// time to size the coordinator's per-executor federated scans.
    ///
    /// Keyed by [`normalized_table_name`] (the same canonical form the partition
    /// store uses) so a report recorded from an executor's fully-qualified table
    /// reference matches the bare `TableScan` reference the query planner looks
    /// up with — a raw `TableReference` key hashes `Bare{t}` ≠ `Full{c,s,t}` and
    /// would miss on every lookup.
    executor_statistics:
        Arc<parking_lot::RwLock<HashMap<String, HashMap<String, ExecutorTableStatistics>>>>,

    /// Per accelerated table (by [`normalized_table_name`]), the partitions
    /// executors have reported loaded. Queries route by it.
    coverage: Arc<parking_lot::RwLock<HashMap<String, TableCoverage>>>,

    /// Append-only log of DDL SQL statements applied to the cluster.
    ddl_log: Arc<RwLock<DdlLog>>,

    /// The order executor processes registered in, shared by the schedulers.
    process_order: Arc<ExecutorProcessOrder>,

    /// Scheduler's `<host>:<port>` identity, used as the `node_id` attribute on
    /// cluster metrics recorded from this registry. `None` when metrics
    /// shouldn't be emitted (e.g. tests, runtime hasn't propagated identity).
    node_id: Option<Arc<str>>,
}

impl ExecutorRegistry {
    /// Creates a new executor registry without a `node_id`. Metrics recorded
    /// via this registry will be skipped. Use [`Self::with_node_id`] in
    /// production to attach the scheduler's `<host>:<port>` identity.
    #[must_use]
    pub fn new(
        accelerations_partition_store: Arc<PartitionStore>,
        federated_partition_store: Arc<PartitionStore>,
    ) -> Self {
        Self::with_node_id(
            accelerations_partition_store,
            federated_partition_store,
            None,
        )
    }

    /// Creates a new executor registry with an explicit `node_id`. The
    /// `<host>:<port>` string is attached to every cluster metric recorded
    /// through this registry.
    #[must_use]
    pub fn with_node_id(
        accelerations_partition_store: Arc<PartitionStore>,
        federated_partition_store: Arc<PartitionStore>,
        node_id: Option<Arc<str>>,
    ) -> Self {
        let process_order = Arc::new(ExecutorProcessOrder::new(
            accelerations_partition_store.cluster_state(),
        ));
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            processes: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            next_connection: Arc::new(AtomicU64::new(1)),
            flight_sql_clients: Arc::new(RwLock::new(HashMap::new())),
            flight_sql_endpoints: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashMap::new())),
            accelerations_partition_store,
            federated_partition_store,
            executor_statistics: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            coverage: Arc::default(),
            ddl_log: Arc::new(RwLock::new(DdlLog::default())),
            process_order,
            node_id,
        }
    }

    /// Records the statistics an executor reported for the slice of `table` it
    /// serves, overwriting any previous report. In-memory only — cheap enough to
    /// call on every report. `column_names` is positionally aligned with
    /// `statistics.column_statistics` so the coordinator can project column stats
    /// onto a (possibly projected) leaf scan by name.
    pub fn record_executor_statistics(
        &self,
        table: &TableReference,
        executor_id: String,
        statistics: Statistics,
        column_names: Vec<String>,
    ) {
        self.executor_statistics
            .write()
            .entry(normalized_table_name(table))
            .or_default()
            .insert(
                executor_id,
                ExecutorTableStatistics {
                    statistics,
                    column_names,
                },
            );
    }

    /// Returns a snapshot of the per-executor statistics for `table` (keyed by
    /// executor id), or an empty map if none have been reported. Used at
    /// query-planning time; the small clone avoids holding the lock across
    /// planning.
    #[must_use]
    pub fn executor_statistics_snapshot(
        &self,
        table: &TableReference,
    ) -> HashMap<String, ExecutorTableStatistics> {
        self.executor_statistics
            .read()
            .get(&normalized_table_name(table))
            .cloned()
            .unwrap_or_default()
    }

    /// Returns `true` once at least one executor has reported statistics for
    /// `table`. Used to gate a distributed table's query-readiness on having
    /// the row-count statistics needed to size the coordinator's joins — the
    /// report may itself carry unknown stats (the executor always reports per
    /// served table), so this signals "we have heard from an executor", not
    /// "the stats are usable".
    #[must_use]
    pub fn has_statistics_for(&self, table: &TableReference) -> bool {
        self.executor_statistics
            .read()
            .get(&normalized_table_name(table))
            .is_some_and(|per_executor| !per_executor.is_empty())
    }

    /// Returns the scheduler's `node_id` if one was provided at construction.
    #[must_use]
    pub fn node_id(&self) -> Option<&str> {
        self.node_id.as_deref()
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

    /// Registers the control-stream connection of the executor `executor_id`
    /// whose process sent `process_id` (empty for executors that send none).
    ///
    /// The connection replaces any earlier one under the same id, except for a
    /// [`ExecutorRegistration::Duplicate`], which is not registered.
    pub async fn register(
        &self,
        executor_id: String,
        process_id: &str,
        request_tx: mpsc::Sender<SchedulerControlMessage>,
    ) -> Registered {
        let connection = ExecutorConnection::new(request_tx);
        let handles = RegisteredHandles {
            pending_metrics: connection.pending_metrics(),
            pending_acks: connection.pending_acks(),
        };
        let connection_id = self.next_connection.fetch_add(1, Ordering::Relaxed);
        let admission = self.admit(&executor_id, process_id).await;

        let mut connections = self.connections.write().await;
        let is_reconnect = connections.contains_key(&executor_id);
        let registration = {
            let mut processes = self.processes.lock();
            let seen = processes.entry(executor_id.clone()).or_default();
            let registration = classify_registration(seen, process_id, admission, is_reconnect);
            if registration != ExecutorRegistration::Duplicate {
                seen.connection = Some(connection_id);
            }
            registration
        };
        if registration == ExecutorRegistration::Duplicate {
            return Registered {
                handles,
                registration,
                connection: connection_id,
            };
        }
        if is_reconnect {
            tracing::debug!("Executor {executor_id} reconnected, replacing existing connection");
        } else {
            tracing::debug!("Executor {executor_id} connected");
        }
        connections.insert(executor_id.clone(), connection);
        drop(connections);

        if let Some(node_id) = self.node_id.as_deref() {
            metrics::set_scheduler_executor_active_connection(node_id, &executor_id, true);
            if is_reconnect {
                metrics::record_scheduler_executor_connection_retry(node_id, &executor_id);
            }
        }

        // The earlier run's endpoint may be gone, and the new process has not
        // loaded its partitions yet, so it is not ready until it registers its
        // own endpoint.
        if registration == ExecutorRegistration::Restarted {
            let mut clients = self.flight_sql_clients.write().await;
            let processes = self.processes.lock();
            let mut endpoints = self.flight_sql_endpoints.write();
            if let Some(seen) = processes.get(&executor_id)
                && endpoints
                    .get(&executor_id)
                    .is_some_and(|endpoint| seen.is_older(&endpoint.process_id, process_id))
            {
                clients.remove(&executor_id);
                endpoints.remove(&executor_id);
            }
        }

        Registered {
            handles,
            registration,
            connection: connection_id,
        }
    }

    /// Where `process_id` stands in the cluster's order of the processes under
    /// `executor_id`, recording it when it is new. `None` for an empty process
    /// id, a process this scheduler already knows, or when the order cannot be
    /// read or written; the process is then ordered by its id.
    async fn admit(&self, executor_id: &str, process_id: &str) -> Option<Admission> {
        if process_id.is_empty()
            || self
                .processes
                .lock()
                .get(executor_id)
                .is_some_and(|seen| seen.is_known(process_id))
        {
            return None;
        }
        let connected: HashSet<String> = self.connections.read().await.keys().cloned().collect();
        match self
            .process_order
            .admit(
                executor_id,
                process_id,
                now_ms().unwrap_or_default(),
                |id| connected.contains(id),
            )
            .await
        {
            Ok(admission) => Some(admission),
            Err(err) => {
                tracing::warn!(
                    "Failed to record the start of a process of executor {executor_id} in the cluster state, so it is ordered against the executor's other processes by its id, which depends on its host's clock: {err}"
                );
                None
            }
        }
    }

    /// Whether `connection` is the registered connection of `executor_id`. A
    /// connection stops being current when a later one under the same id
    /// replaces it.
    #[must_use]
    pub fn is_current(&self, executor_id: &str, connection: u64) -> bool {
        self.processes
            .lock()
            .get(executor_id)
            .is_some_and(|seen| seen.connection == Some(connection))
    }

    /// Unregisters `executor_id` and removes it from all three tracking maps,
    /// if `connection` is still its registered connection. Returns whether it
    /// was: a connection that a later one replaced leaves the executor alone.
    ///
    /// A later process under the same id may already have registered its Flight
    /// endpoint before its control stream connects; that endpoint, and the
    /// executor's other state, are then left for the later process.
    pub async fn unregister(&self, executor_id: &str, connection: u64) -> bool {
        let mut connections = self.connections.write().await;
        let process_id = {
            let mut processes = self.processes.lock();
            match processes.get_mut(executor_id) {
                Some(seen) if seen.connection == Some(connection) => {
                    seen.connection = None;
                    seen.current.clone().unwrap_or_default()
                }
                _ => return false,
            }
        };
        if connections.remove(executor_id).is_some() {
            tracing::debug!("Executor {executor_id} disconnected");
        }
        // Held while `connections` is, so no endpoint registers between the
        // check above and its removal below.
        let successor_registered = {
            let mut clients = self.flight_sql_clients.write().await;
            let processes = self.processes.lock();
            let mut endpoints = self.flight_sql_endpoints.write();
            let successor_registered = processes.get(executor_id).is_some_and(|seen| {
                endpoints
                    .get(executor_id)
                    .is_some_and(|endpoint| seen.is_older(&process_id, &endpoint.process_id))
            });
            if !successor_registered {
                clients.remove(executor_id);
                endpoints.remove(executor_id);
            }
            successor_registered
        };
        drop(connections);
        if let Some(node_id) = self.node_id.as_deref() {
            metrics::set_scheduler_executor_active_connection(node_id, executor_id, false);
        }
        if successor_registered {
            return true;
        }
        self.partitions.write().await.remove(executor_id);

        // Drop the disconnected executor's reported statistics across all tables
        // so a departed executor's stale row counts don't linger in planning.
        {
            let mut stats = self.executor_statistics.write();
            for table_stats in stats.values_mut() {
                table_stats.remove(executor_id);
            }
            stats.retain(|_, table_stats| !table_stats.is_empty());
        }
        true
    }

    /// Returns `true` if at least one executor has an active `FlightSqlClient`.
    pub async fn has_flight_sql_clients(&self) -> bool {
        !self.flight_sql_clients.read().await.is_empty()
    }

    /// Returns a point-in-time snapshot of all `executor_id` → `FlightSqlClient` mappings.
    pub async fn flight_sql_clients_snapshot(&self) -> HashMap<String, FlightSqlClient> {
        self.flight_sql_clients.read().await.clone()
    }

    /// Inserts or replaces the `FlightSqlClient` for `executor_id`, which
    /// connects to `address` (`host:port`) and was registered by the executor
    /// process `process_id` (empty for executors that send none). Returns
    /// `false`, inserting nothing, when a later process under the same id
    /// is newer than `process_id`, by its control stream or its endpoint: an
    /// earlier process's endpoint must not displace its successor's, whichever
    /// arrives first.
    pub async fn insert_flight_sql_client(
        &self,
        executor_id: String,
        address: String,
        process_id: &str,
        client: FlightSqlClient,
    ) -> bool {
        let mut clients = self.flight_sql_clients.write().await;
        // Older than the process registered last (a replaced run or a refused
        // duplicate), or older than the process whose endpoint is registered.
        let superseded = !process_id.is_empty() && {
            let processes = self.processes.lock();
            let unseen = ExecutorProcesses::default();
            let seen = processes.get(&executor_id).unwrap_or(&unseen);
            seen.replaced.iter().any(|replaced| replaced == process_id)
                || seen
                    .current
                    .as_deref()
                    .is_some_and(|current| seen.is_older(process_id, current))
                || self
                    .flight_sql_endpoints
                    .read()
                    .get(&executor_id)
                    .is_some_and(|endpoint| seen.is_older(process_id, &endpoint.process_id))
        };
        if superseded {
            return false;
        }
        self.flight_sql_endpoints.write().insert(
            executor_id.clone(),
            FlightEndpoint {
                address,
                process_id: process_id.to_string(),
            },
        );
        clients.insert(executor_id, client);
        true
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

    /// Best-effort SYNCHRONOUS snapshot of ready executors' addresses
    /// (`host:port`, no scheme — the same form `executor_table` expects after
    /// prepending a scheme). Returns empty if the lock is momentarily contended.
    /// Used by the broadcast-join physical optimizer rule, which runs in a sync
    /// planning context and cannot await.
    #[must_use]
    pub fn ready_executor_addresses_sync(&self) -> Vec<String> {
        match self.flight_sql_endpoints.try_read() {
            Some(endpoints) => endpoints
                .values()
                .map(|endpoint| endpoint.address.clone())
                .collect(),
            None => Vec::new(),
        }
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
        send_ack(
            executor_id,
            &request_tx,
            &pending_acks,
            build_command,
            timeout,
        )
        .await
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
                    delivered: Arc::default(),
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

    /// Records, for an accelerated table, the partitions each executor process
    /// has reported loaded. A query reads a partition only from an executor
    /// whose current process reported it loaded.
    pub fn set_loaded_partitions(
        &self,
        table: &TableReference,
        loaded: HashMap<String, LoadedPartitions>,
    ) {
        self.coverage
            .write()
            .entry(normalized_table_name(table))
            .or_default()
            .loaded = loaded;
    }

    /// The process registered last under `executor_id`; empty when it sent none.
    #[must_use]
    pub fn current_process(&self, executor_id: &str) -> String {
        self.processes
            .lock()
            .get(executor_id)
            .and_then(|seen| seen.current.clone())
            .unwrap_or_default()
    }

    /// The partition expression bytes of `table` that the registered connection
    /// of `executor_id` has acked as added, and when it acked each.
    pub async fn delivered_partitions(
        &self,
        executor_id: &str,
        table: &str,
    ) -> HashMap<Bytes, Instant> {
        let connections = self.connections.read().await;
        connections
            .get(executor_id)
            .and_then(|connection| connection.delivered.lock().get(table).cloned())
            .unwrap_or_default()
    }

    /// Sends `executor_id` an `UpdatePartitions` that adds `added` and removes
    /// `removed` partitions of `table`, and waits for its ack. Once acked, the
    /// connection it went out on counts `added` as delivered and `removed` as
    /// no longer delivered.
    ///
    /// # Errors
    ///
    /// See [`Self::send_command_with_ack`].
    pub async fn send_partition_update(
        &self,
        executor_id: &str,
        table: &str,
        added: &[Bytes],
        removed: &[Bytes],
        timeout: std::time::Duration,
    ) -> Result<()> {
        let (request_tx, pending_acks, delivered) = {
            let connections = self.connections.read().await;
            let Some(connection) = connections.get(executor_id) else {
                return Err(Error::ExecutorNotRegistered {
                    executor_id: executor_id.to_string(),
                });
            };
            (
                connection.request_tx.clone(),
                connection.pending_acks(),
                Arc::clone(&connection.delivered),
            )
        };
        let items = |partitions: &[Bytes]| {
            if partitions.is_empty() {
                HashMap::new()
            } else {
                HashMap::from([(
                    table.to_string(),
                    BytesArray {
                        items: partitions.iter().map(|b| b.to_vec()).collect(),
                    },
                )])
            }
        };
        let (new_partitions, removed_partitions) = (items(added), items(removed));
        let build = move |request_id| SchedulerControlMessage {
            message: Some(
                runtime_proto::scheduler_control_message::Message::UpdatePartitions(
                    UpdatePartitions {
                        new_partitions,
                        removed_partitions,
                        request_id,
                    },
                ),
            ),
        };
        send_ack(executor_id, &request_tx, &pending_acks, build, timeout).await?;
        let now = Instant::now();
        let mut delivered = delivered.lock();
        let table_delivered = delivered.entry(table.to_string()).or_default();
        table_delivered.extend(added.iter().map(|bytes| (bytes.clone(), now)));
        for bytes in removed {
            table_delivered.remove(bytes);
        }
        Ok(())
    }

    /// Resolves a table's partitions using the accelerations partition store and returns
    /// one `FlightSQL` table provider per selected executor. The caller decides whether the
    /// table should actually be partitioned (e.g. `AcceleratedPartitionProvider` checks
    /// `AcceleratedTable` downcast in the runtime crate).
    ///
    /// `discovered` says whether the table's partitions come from discovery (a
    /// spicepod dataset or view with `partition_by`): such a table without
    /// partition metadata has not been discovered yet, and its query fails. Any
    /// other accelerated table without partition metadata is read from one
    /// executor.
    ///
    /// # Errors
    ///
    /// See [`get_partitions_from_store`].
    pub fn resolve_accelerated_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
        discovered: bool,
    ) -> DataFusionResult<PartitionProviders> {
        let (Ok(connections), Ok(flight_sql_clients)) = (
            self.connections.try_read(),
            self.flight_sql_clients.try_read(),
        ) else {
            return Err(executors_changing(table));
        };
        let executors = ready_executors(&connections, &flight_sql_clients);
        let executor_statistics = self.executor_statistics_snapshot(table);
        let current: HashMap<String, String> = {
            let processes = self.processes.lock();
            executors
                .keys()
                .map(|id| {
                    let process = processes
                        .get(id)
                        .and_then(|seen| seen.current.clone())
                        .unwrap_or_default();
                    (id.clone(), process)
                })
                .collect()
        };
        let key = normalized_table_name(table);
        let routed = {
            let coverage = self.coverage.read();
            let gate = LoadGate {
                coverage: coverage.get(&key),
                current: &current,
                discovered,
            };
            get_partitions_from_store(
                &self.accelerations_partition_store,
                &executors,
                table,
                schema,
                &executor_statistics,
                self.node_id.as_deref(),
                Some(&gate),
            )?
        };
        if let Some(complete) = routed.complete {
            self.coverage.write().entry(key).or_default().last_complete = Some(complete);
        }
        Ok(routed.providers)
    }
}

/// Lets a query read a partition only from an executor whose current process
/// reported it loaded.
struct LoadGate<'a> {
    coverage: Option<&'a TableCoverage>,
    /// The current process of each ready executor.
    current: &'a HashMap<String, String>,
    /// Whether the table's partitions come from discovery, so a table without
    /// partition metadata has not been discovered yet.
    discovered: bool,
}

impl LoadGate<'_> {
    /// The partitions the current process of `executor_id` reported loaded.
    fn loaded(&self, executor_id: &str) -> Option<&HashSet<PartitionKey>> {
        let loaded = self.coverage?.loaded.get(executor_id)?;
        (self.current.get(executor_id) == Some(&loaded.process_id)).then_some(&loaded.partitions)
    }

    /// Whether every partition of the last set read whole is still `covered`,
    /// so a query reads that complete set while the partitions added since
    /// load. A partition of that set that was removed or is no longer loaded
    /// means the rows it held may now be in a partition that has not loaded.
    fn can_skip(&self, covered: &HashSet<PartitionKey>) -> bool {
        self.coverage
            .and_then(|coverage| coverage.last_complete.as_ref())
            .is_some_and(|complete| complete.is_subset(covered))
    }
}

/// Partitions resolved for one table scan.
struct Routed {
    providers: PartitionProviders,
    /// With a [`LoadGate`], the table's partitions when every one of them was
    /// loaded on a ready executor.
    complete: Option<HashSet<PartitionKey>>,
}

/// A routing failure a later retry can overcome.
fn partitions_unavailable(message: String) -> DataFusionError {
    DataFusionError::External(SpiceExternalError::partitions_unavailable(message))
}

/// Sends the command `build_command` makes on `request_tx` and waits for the
/// executor's ack on `pending_acks`.
async fn send_ack(
    executor_id: &str,
    request_tx: &mpsc::Sender<SchedulerControlMessage>,
    pending_acks: &CorrelatedResponses<Ack>,
    build_command: impl FnOnce(String) -> SchedulerControlMessage + Send,
    timeout: std::time::Duration,
) -> Result<()> {
    match send_correlated(request_tx, pending_acks, build_command, Some(timeout)).await {
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

/// The error for a query planned while an executor connects or disconnects. Planning cannot
/// wait for the registry's locks, and a partial executor list would drop rows.
fn executors_changing(table: &TableReference) -> DataFusionError {
    partitions_unavailable(format!(
        "Cannot query {table}: the scheduler's executors are changing; retry the query"
    ))
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
    statistics: Option<Statistics>,
) -> Arc<dyn TableProvider> {
    Arc::new(
        FlightSQLTable::create_with_schema(
            "flightsql",
            executor_id,
            client,
            table.clone(),
            schema,
            Arc::new(CookieStore::new()),
        )
        .with_statistics(statistics),
    ) as Arc<dyn TableProvider>
}

/// Shared logic for `get_partitions` across accelerated and federated partition providers.
///
/// Uses the given [`PartitionStore`] to look up partition metadata, checks readiness (both an
/// active connection and a `FlightSQL` client) via the `executors` map, selects a minimal
/// executor set, and returns `(FlightSQL provider, partition values)` pairs.
/// Projects the `executor_id`'s reported statistics for `table` onto `schema`,
/// warning when no usable plan statistics (a known `num_rows`) are available for
/// this leaf. Absent row counts prevent `DataFusion`'s cost-based join-side swap
/// (`should_swap_join_order`), so a distributed plan can end up buffering the
/// large side of a hash join — worth surfacing rather than silently degrading.
fn stamp_executor_statistics(
    executor_statistics: &HashMap<String, ExecutorTableStatistics>,
    executor_id: &str,
    table: &TableReference,
    schema: &SchemaRef,
) -> Option<Statistics> {
    let statistics = executor_statistics
        .get(executor_id)
        .map(|ets| ets.projected_onto(schema));
    if statistics
        .as_ref()
        .and_then(|s| s.num_rows.get_value())
        .is_none()
    {
        tracing::warn!(
            table = %table,
            executor = %executor_id,
            "No plan statistics retrieved from executor for table {table:?}; distributed join sizing may be degraded"
        );
    }
    statistics
}

/// Returns an empty list only when the table's partition metadata lists no
/// partitions, or, with a `gate`, none that a query can read yet.
///
/// With a `gate`, a partition is read only from an executor whose current process
/// reported it loaded, and only from an executor that has not loaded a partition
/// the table assigns to another executor (its scan would return those rows too).
/// A partition no ready executor has loaded fails the query, unless it is newer
/// than the last time the whole table was loaded: the query then reads that
/// complete set, as a single node reads its last refresh while the next one
/// loads.
///
/// # Errors
///
/// Returns an error when the table has no partition metadata and `gate` says its
/// partitions come from discovery, or when no ready executor serves the table, or
/// one of its partitions.
fn get_partitions_from_store(
    partition_store: &PartitionStore,
    executors: &HashMap<String, (&ExecutorConnection, &FlightSqlClient)>,
    table: &TableReference,
    schema: &SchemaRef,
    executor_statistics: &HashMap<String, ExecutorTableStatistics>,
    node_id: Option<&str>,
    gate: Option<&LoadGate<'_>>,
) -> DataFusionResult<Routed> {
    let Some(table_metadata) = partition_store.get_cached_table_metadata(table) else {
        if gate.is_some_and(|gate| gate.discovered) {
            if let Some(node_id) = node_id {
                metrics::record_query_planning_failure(
                    node_id,
                    metrics::PlanningFailure::MissingPartitions,
                );
            }
            return Err(partitions_unavailable(format!(
                "Cannot query {table}: its partitions have not been discovered yet; retry the query"
            )));
        }
        // No partition metadata — route to a single live executor to avoid duplicate results.
        // A table whose executors each hold all of it, such as an accelerated
        // table created by DDL.
        let Some((executor_id, (_, client))) = executors.iter().min_by_key(|(id, _)| id.as_str())
        else {
            tracing::warn!(
                "No partition assignments for table {table:?} and no connected executors with FlightSQL clients"
            );
            if let Some(node_id) = node_id {
                metrics::record_query_planning_failure(
                    node_id,
                    metrics::PlanningFailure::NoExecutors,
                );
            }
            return Err(partitions_unavailable(format!(
                "Cannot query {table}: no ready executor serves it"
            )));
        };

        if let Some(node_id) = node_id {
            metrics::record_query_executor_count(node_id, 1);
        }
        // Stamp the chosen executor's reported stats onto this single leaf scan
        // so the coordinator can size joins even on this no-partition-metadata
        // fallback path (previously this passed `None`, silently degrading plans).
        let statistics = stamp_executor_statistics(executor_statistics, executor_id, table, schema);
        return Ok(Routed {
            providers: vec![(
                flight_sql_table_provider(
                    executor_id,
                    (*client).clone(),
                    table,
                    Arc::clone(schema),
                    statistics,
                ),
                Vec::new(),
            )],
            complete: None,
        });
    };

    if table_metadata.partitions.is_empty() {
        tracing::debug!("No partitions required for table {table}");
        return Ok(Routed {
            providers: Vec::new(),
            complete: gate.map(|_| HashSet::new()),
        });
    }

    // Executors whose scans may be used: ready, and, with a gate, holding only
    // partitions the table assigns to them.
    let owners: HashMap<PartitionKey, &[String]> = table_metadata
        .partitions
        .iter()
        .map(|p| {
            (
                partition_key(&p.partition_value),
                p.assigned_executors.as_slice(),
            )
        })
        .collect();
    let usable: HashMap<&str, Option<&HashSet<PartitionKey>>> = executors
        .keys()
        .filter_map(|executor_id| {
            let Some(gate) = gate else {
                return Some((executor_id.as_str(), None));
            };
            let loaded = gate.loaded(executor_id)?;
            let holds_others = loaded.iter().any(|key| {
                owners
                    .get(key)
                    .is_some_and(|assigned| !assigned.iter().any(|e| e == executor_id))
            });
            (!holds_others).then_some((executor_id.as_str(), Some(loaded)))
        })
        .collect();

    // Build executor -> partitions map from the executors that can serve each
    // partition. Per-executor row-count statistics (used below to size the
    // coordinator's joins) come from the reports each executor sends for the
    // slice of the table it serves (see `handle_executor_statistics`).
    let mut executor_partition_map: HashMap<String, Vec<PartitionValue>> = HashMap::new();
    let mut holders: HashMap<PartitionKey, Vec<&str>> = HashMap::new();
    let mut covered: Vec<PartitionValue> = Vec::new();
    let mut covered_keys: HashSet<PartitionKey> = HashSet::new();
    let mut uncovered: Vec<PartitionKey> = Vec::new();
    for partition_meta in &table_metadata.partitions {
        let key = partition_key(&partition_meta.partition_value);
        let mut served = false;
        for executor_id in &partition_meta.assigned_executors {
            let Some(loaded) = usable.get(executor_id.as_str()) else {
                tracing::debug!(
                    "Executor '{executor_id}' has partition assignment but is not ready or holds other executors' partitions; excluding from selection"
                );
                continue;
            };
            if loaded.is_some_and(|loaded| !loaded.contains(&key)) {
                continue;
            }
            served = true;
            holders.entry(key.clone()).or_default().push(executor_id);
            executor_partition_map
                .entry(executor_id.clone())
                .or_default()
                .push(partition_meta.partition_value.clone());
        }
        if served {
            covered.push(partition_meta.partition_value.clone());
            covered_keys.insert(key);
        } else {
            uncovered.push(key);
        }
    }

    let total = table_metadata.partitions.len();
    let complete = uncovered.is_empty();
    if !complete && !gate.is_some_and(|gate| gate.can_skip(&covered_keys)) {
        tracing::error!(
            "Cannot execute query on table {}: {} partition(s) not served by any ready executor. Missing: {:?}",
            table,
            uncovered.len(),
            uncovered.iter().take(5).collect::<Vec<_>>()
        );
        if let Some(node_id) = node_id {
            metrics::record_query_planning_failure(
                node_id,
                metrics::PlanningFailure::MissingPartitions,
            );
        }
        return Err(partitions_unavailable(format!(
            "Cannot query {table}: no ready executor serves {} of its {total} partitions; retry the query",
            uncovered.len()
        )));
    }

    // Select minimal set of executors to cover all servable partitions
    let selected_executors =
        executor_selection::select_executors(&covered, &executor_partition_map)
            .map_err(|err| partitions_unavailable(format!("Cannot query {table}: {err}")))?;

    if let Some(node_id) = node_id {
        metrics::record_query_executor_count(node_id, selected_executors.len() as u64);
    }

    tracing::debug!(
        "Selected {} executor(s) from {} available for table {} (covering {} of {} partition(s))",
        selected_executors.len(),
        executor_partition_map.len(),
        table,
        covered.len(),
        total
    );

    // Each partition is read from one selected executor, so a scan filtered to the
    // partitions its executor serves returns no partition twice.
    let mut to_read: HashMap<&str, Vec<PartitionValue>> = HashMap::new();
    for partition_value in &covered {
        let reader = holders
            .get(&partition_key(partition_value))
            .and_then(|ids| {
                ids.iter()
                    .find(|id| selected_executors.iter().any(|selected| selected == **id))
            });
        if let Some(reader) = reader {
            to_read
                .entry(*reader)
                .or_default()
                .push(partition_value.clone());
        }
    }

    let providers = selected_executors
        .iter()
        .filter_map(|executor_id| {
            let (_, client) = executors.get(executor_id)?;
            let partition_values = to_read.remove(executor_id.as_str())?;
            // Stamp the executor-reported statistics onto this leaf scan so the
            // coordinator's planner can size joins. Project the reported per-column
            // stats onto the leaf's (possibly projected) schema by name, carrying
            // num_rows and per-column min/max.
            let statistics =
                stamp_executor_statistics(executor_statistics, executor_id, table, schema);
            let provider = flight_sql_table_provider(
                executor_id,
                (*client).clone(),
                table,
                Arc::clone(schema),
                statistics,
            );
            Some((provider, partition_values))
        })
        .collect();
    Ok(Routed {
        providers,
        complete: (gate.is_some() && complete).then(|| owners.into_keys().collect()),
    })
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
    accelerations_partition_store: Arc<PartitionStore>,
    executor_statistics:
        Arc<parking_lot::RwLock<HashMap<String, HashMap<String, ExecutorTableStatistics>>>>,
    node_id: Option<Arc<str>>,
}

impl FederatedPartitionProvider {
    /// Creates a new `FederatedPartitionProvider` from an [`ExecutorRegistry`].
    #[must_use]
    pub fn from_registry(registry: &ExecutorRegistry) -> Self {
        Self {
            connections: Arc::clone(&registry.connections),
            flight_sql_clients: Arc::clone(&registry.flight_sql_clients),
            partition_store: Arc::clone(&registry.federated_partition_store),
            accelerations_partition_store: Arc::clone(&registry.accelerations_partition_store),
            executor_statistics: Arc::clone(&registry.executor_statistics),
            node_id: registry.node_id.clone(),
        }
    }
}

impl TablePartitionProvider for FederatedPartitionProvider {
    fn should_partition(&self, tbl: &TableScan) -> bool {
        // The accelerated rewrite routes a table whose partitions the acceleration
        // store holds, and its per-executor scans keep the table's name.
        self.partition_store
            .get_cached_table_metadata(&tbl.table_name)
            .is_some()
            && self
                .accelerations_partition_store
                .get_cached_table_metadata(&tbl.table_name)
                .is_none()
    }

    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> DataFusionResult<PartitionProviders> {
        let (Ok(connections), Ok(flight_sql_clients)) = (
            self.connections.try_read(),
            self.flight_sql_clients.try_read(),
        ) else {
            return Err(executors_changing(table));
        };

        let executors = ready_executors(&connections, &flight_sql_clients);
        let executor_statistics = self
            .executor_statistics
            .read()
            .get(&normalized_table_name(table))
            .cloned()
            .unwrap_or_default();

        // Each scan is filtered to the partitions its executor serves: an executor can
        // hold rows of other partitions, such as a table it loaded whole.
        Ok(get_partitions_from_store(
            &self.partition_store,
            &executors,
            table,
            schema,
            &executor_statistics,
            self.node_id.as_deref(),
            None,
        )?
        .providers)
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use crate::cluster_state::ClusterStateStore;

    use super::*;

    async fn make_registry() -> ExecutorRegistry {
        registry_over(&(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)).await
    }

    /// A scheduler's registry over the shared state in `store`.
    async fn registry_over(store: &Arc<dyn ObjectStore>) -> ExecutorRegistry {
        let cs = Arc::new(ClusterStateStore::new(Arc::clone(store), ""));
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

        let registered = registry.register("executor-1".to_string(), "", tx).await;

        let executors = registry.connected_executors().await;
        assert_eq!(executors, vec!["executor-1"]);

        assert!(
            registry
                .unregister("executor-1", registered.connection)
                .await
        );

        let executors = registry.connected_executors().await;
        assert!(executors.is_empty());
    }

    #[tokio::test]
    async fn test_reconnect_replaces_connection() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);

        registry.register("executor-1".to_string(), "", tx1).await;
        registry.register("executor-1".to_string(), "", tx2).await;

        let executors = registry.connected_executors().await;
        assert_eq!(executors.len(), 1);
    }

    /// An executor that restarts under the same id sends a new process id. The
    /// new process is a restart of the old one, and the old process's connection
    /// closing late leaves the new one registered.
    #[tokio::test]
    async fn a_new_process_under_an_executor_id_is_a_restart() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);

        let first = registry.register("exec-a".to_string(), "run-1", tx1).await;
        assert_eq!(first.registration, ExecutorRegistration::New);
        let restarted = registry.register("exec-a".to_string(), "run-2", tx2).await;
        assert_eq!(restarted.registration, ExecutorRegistration::Restarted);
        assert!(!registry.is_current("exec-a", first.connection));
        assert!(registry.is_current("exec-a", restarted.connection));

        assert!(
            !registry.unregister("exec-a", first.connection).await,
            "the replaced connection closing must not unregister its successor"
        );
        assert_eq!(registry.connected_executors().await, vec!["exec-a"]);

        let reconnected = registry.register("exec-a".to_string(), "run-2", tx3).await;
        assert_eq!(reconnected.registration, ExecutorRegistration::Reconnected);
        assert!(registry.unregister("exec-a", reconnected.connection).await);
        assert!(registry.connected_executors().await.is_empty());
    }

    /// A restart drops the endpoint the earlier run registered, so the executor
    /// is not ready until the new process registers its own. An endpoint the new
    /// process registered before its control stream connected is kept. Broadcast
    /// joins reach executors at these endpoints, not at their identity.
    #[tokio::test]
    async fn a_restart_drops_the_endpoint_of_the_earlier_run() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);

        registry.register("exec-a".to_string(), "run-1", tx1).await;
        registry
            .insert_flight_sql_client(
                "exec-a".to_string(),
                "127.0.0.1:50101".to_string(),
                "run-1",
                dummy_flight_sql_client(),
            )
            .await;
        assert_eq!(
            registry.ready_executor_addresses_sync(),
            ["127.0.0.1:50101"]
        );

        registry.register("exec-a".to_string(), "run-2", tx2).await;
        assert_eq!(registry.flight_sql_clients_count().await, 0);
        assert!(registry.ready_executor_addresses_sync().is_empty());

        registry
            .insert_flight_sql_client(
                "exec-a".to_string(),
                "127.0.0.1:50102".to_string(),
                "run-3",
                dummy_flight_sql_client(),
            )
            .await;
        registry.register("exec-a".to_string(), "run-3", tx3).await;
        assert_eq!(registry.flight_sql_clients_count().await, 1);
        assert_eq!(
            registry.ready_executor_addresses_sync(),
            ["127.0.0.1:50102"]
        );

        // A replaced process registering its endpoint late is ignored.
        assert!(
            !registry
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50101".to_string(),
                    "run-1",
                    dummy_flight_sql_client(),
                )
                .await
        );
        assert_eq!(
            registry.ready_executor_addresses_sync(),
            ["127.0.0.1:50102"]
        );

        // The next run registers its endpoint, the run it replaces registers
        // its endpoint late, and only then does the next run's control stream
        // connect: the next run's endpoint stays.
        let (tx4, _rx4) = mpsc::channel(1);
        assert!(
            registry
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50103".to_string(),
                    "run-4",
                    dummy_flight_sql_client(),
                )
                .await
        );
        assert!(
            !registry
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50102".to_string(),
                    "run-3",
                    dummy_flight_sql_client(),
                )
                .await
        );
        registry.register("exec-a".to_string(), "run-4", tx4).await;
        assert_eq!(
            registry.ready_executor_addresses_sync(),
            ["127.0.0.1:50103"]
        );
    }

    /// The earlier run's connection closing after the next run registered its
    /// endpoint, but before the next run's control stream connected, leaves
    /// that endpoint in place.
    #[tokio::test]
    async fn an_earlier_connection_closing_keeps_its_successors_endpoint() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);

        let earlier = registry.register("exec-a".to_string(), "run-1", tx1).await;
        registry
            .insert_flight_sql_client(
                "exec-a".to_string(),
                "127.0.0.1:50102".to_string(),
                "run-2",
                dummy_flight_sql_client(),
            )
            .await;
        assert!(registry.unregister("exec-a", earlier.connection).await);
        assert!(registry.connected_executors().await.is_empty());
        assert_eq!(
            registry.ready_executor_addresses_sync(),
            ["127.0.0.1:50102"]
        );
    }

    /// A replaced process that connects again is still running, so two processes
    /// share the id. It is refused and the later process stays registered.
    #[tokio::test]
    async fn a_replaced_process_that_connects_again_is_a_duplicate() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);

        registry.register("exec-a".to_string(), "run-1", tx1).await;
        let later = registry.register("exec-a".to_string(), "run-2", tx2).await;
        let returning = registry.register("exec-a".to_string(), "run-1", tx3).await;

        assert_eq!(returning.registration, ExecutorRegistration::Duplicate);
        assert!(registry.is_current("exec-a", later.connection));
        assert!(!registry.is_current("exec-a", returning.connection));
        assert_eq!(registry.connected_executors().await, vec!["exec-a"]);
    }

    /// A process that started earlier but connects after a later one, for
    /// example to a scheduler that started after both, is a duplicate, not a
    /// restart. The schedulers share the order the processes first registered
    /// in, which here is the reverse of their ids' order.
    #[tokio::test]
    async fn an_older_process_connecting_later_is_a_duplicate() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let earlier_scheduler = registry_over(&store).await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        earlier_scheduler
            .register("exec-a".to_string(), "run-2", tx1)
            .await;
        earlier_scheduler
            .register("exec-a".to_string(), "run-1", tx2)
            .await;

        let later_scheduler = registry_over(&store).await;
        let (tx3, _rx3) = mpsc::channel(1);
        let (tx4, _rx4) = mpsc::channel(1);
        let newer = later_scheduler
            .register("exec-a".to_string(), "run-1", tx3)
            .await;
        let older = later_scheduler
            .register("exec-a".to_string(), "run-2", tx4)
            .await;

        assert_eq!(newer.registration, ExecutorRegistration::New);
        assert_eq!(older.registration, ExecutorRegistration::Duplicate);
        assert!(later_scheduler.is_current("exec-a", newer.connection));

        // Its endpoint is refused too, even before the newer process registers one.
        assert!(
            !later_scheduler
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50101".to_string(),
                    "run-2",
                    dummy_flight_sql_client(),
                )
                .await
        );
        assert!(later_scheduler.ready_executor_addresses_sync().is_empty());
    }

    /// A scheduler that starts after a process was replaced, and sees the
    /// replaced process first, refuses it and its endpoint from the shared
    /// order alone, then registers the later process.
    #[tokio::test]
    async fn a_replaced_process_connecting_first_to_a_new_scheduler_is_a_duplicate() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let earlier_scheduler = registry_over(&store).await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        earlier_scheduler
            .register("exec-a".to_string(), "run-2", tx1)
            .await;
        earlier_scheduler
            .register("exec-a".to_string(), "run-1", tx2)
            .await;

        let later_scheduler = registry_over(&store).await;
        let (tx3, _rx3) = mpsc::channel(1);
        let (tx4, _rx4) = mpsc::channel(1);
        let replaced = later_scheduler
            .register("exec-a".to_string(), "run-2", tx3)
            .await;
        assert_eq!(replaced.registration, ExecutorRegistration::Duplicate);
        assert!(
            !later_scheduler
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50101".to_string(),
                    "run-2",
                    dummy_flight_sql_client(),
                )
                .await
        );

        let live = later_scheduler
            .register("exec-a".to_string(), "run-1", tx4)
            .await;
        assert_eq!(live.registration, ExecutorRegistration::New);
        assert!(later_scheduler.is_current("exec-a", live.connection));
    }

    /// A process that has dropped out of the cluster's record, after many later
    /// processes registered under its id, is still a duplicate when it connects.
    #[tokio::test]
    async fn a_process_dropped_from_the_record_is_a_duplicate() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let earlier_scheduler = registry_over(&store).await;
        let mut receivers = Vec::new();
        for run in 0..10 {
            let (tx, rx) = mpsc::channel(1);
            receivers.push(rx);
            earlier_scheduler
                .register("exec-a".to_string(), &format!("run-{run:02}"), tx)
                .await;
        }

        let later_scheduler = registry_over(&store).await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let live = later_scheduler
            .register("exec-a".to_string(), "run-09", tx1)
            .await;
        let forgotten = later_scheduler
            .register("exec-a".to_string(), "run-00", tx2)
            .await;
        assert_eq!(forgotten.registration, ExecutorRegistration::Duplicate);
        assert!(later_scheduler.is_current("exec-a", live.connection));
    }

    /// A process that replaces an executor is a restart even when its id sorts
    /// before its predecessor's, as when it starts on a host whose clock reads
    /// earlier. Its endpoint replaces the predecessor's, and the predecessor is
    /// a duplicate if it connects again.
    #[tokio::test]
    async fn a_restart_whose_id_sorts_first_is_a_restart() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let (tx3, _rx3) = mpsc::channel(1);

        let predecessor = registry.register("exec-a".to_string(), "run-2", tx1).await;
        assert!(
            registry
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50101".to_string(),
                    "run-2",
                    dummy_flight_sql_client(),
                )
                .await
        );
        assert!(registry.unregister("exec-a", predecessor.connection).await);

        let replacement = registry.register("exec-a".to_string(), "run-1", tx2).await;
        assert_eq!(replacement.registration, ExecutorRegistration::Restarted);
        assert!(registry.is_current("exec-a", replacement.connection));
        assert!(
            registry
                .insert_flight_sql_client(
                    "exec-a".to_string(),
                    "127.0.0.1:50102".to_string(),
                    "run-1",
                    dummy_flight_sql_client(),
                )
                .await
        );
        assert_eq!(
            registry.ready_executor_addresses_sync(),
            ["127.0.0.1:50102"]
        );

        let returning = registry.register("exec-a".to_string(), "run-2", tx3).await;
        assert_eq!(returning.registration, ExecutorRegistration::Duplicate);
        assert!(registry.is_current("exec-a", replacement.connection));
    }

    /// An executor that sends no process id is registered as before: a second
    /// connection under its id is a reconnect, never a restart.
    #[tokio::test]
    async fn an_executor_without_a_process_id_reconnects() {
        let registry = make_registry().await;
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);

        let first = registry
            .register("127.0.0.1:50062".to_string(), "", tx1)
            .await;
        let second = registry
            .register("127.0.0.1:50062".to_string(), "", tx2)
            .await;
        assert_eq!(first.registration, ExecutorRegistration::New);
        assert_eq!(second.registration, ExecutorRegistration::Reconnected);
        assert!(
            !registry
                .unregister("127.0.0.1:50062", first.connection)
                .await
        );
        assert!(
            registry
                .unregister("127.0.0.1:50062", second.connection)
                .await
        );
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

        registry.register("executor-1".to_string(), "", tx1).await;
        let executor_2 = registry.register("executor-2".to_string(), "", tx2).await;
        registry.register("executor-3".to_string(), "", tx3).await;

        let mut executors = registry.connected_executors().await;
        executors.sort();

        assert_eq!(executors.len(), 3);
        assert_eq!(executors, vec!["executor-1", "executor-2", "executor-3"]);

        // Unregister one
        registry
            .unregister("executor-2", executor_2.connection)
            .await;

        let mut executors = registry.connected_executors().await;
        executors.sort();

        assert_eq!(executors.len(), 2);
        assert_eq!(executors, vec!["executor-1", "executor-3"]);
    }

    #[tokio::test]
    async fn unknown_statistics_report_still_marks_table_as_having_statistics() {
        use arrow::datatypes::{DataType, Field, Schema};

        // Mirrors the decode-failure fallback in `handle_executor_statistics`: a
        // malformed/forward-incompatible report is recorded as unknown statistics
        // rather than dropped, so `has_statistics_for` flips `true` and the
        // readiness gate can open. Dropping it would leave the table stuck out of
        // `Ready` forever (and could hang /v1/ready).
        let registry = make_registry().await;
        let table = TableReference::bare("orders");

        assert!(
            !registry.has_statistics_for(&table),
            "no report recorded yet"
        );

        registry.record_executor_statistics(
            &table,
            "executor-1".to_string(),
            Statistics::new_unknown(&Schema::empty()),
            Vec::new(),
        );

        assert!(
            registry.has_statistics_for(&table),
            "an unknown-statistics report must still count so table readiness can proceed"
        );

        // It also reads back safely: projecting onto a real leaf schema yields
        // absent row count and unknown per-column stats — no panic, no bogus
        // cardinalities fed to the planner.
        let snapshot = registry.executor_statistics_snapshot(&table);
        let entry = snapshot.get("executor-1").expect("recorded entry present");
        let leaf: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let projected = entry.projected_onto(&leaf);
        assert_eq!(projected.num_rows, Precision::Absent);
        assert_eq!(projected.column_statistics.len(), 1);
    }

    /// Regression test for #12303.
    ///
    /// The registry holds an asynchronous snapshot of what an executor reported
    /// (rebroadcast on a 45s timer plus a debounced write-completion nudge), so a
    /// reported `Exact` value describes the executor's data at report time, not at
    /// plan time. Carrying `Exact` through to the leaf scan licenses the
    /// `aggregate_statistics` rule to fold `COUNT(*)`/`MIN`/`MAX` into the result
    /// from that stale snapshot. Downgrading to `Inexact` keeps the values (join
    /// sizing reads `Precision::get_value()`) while refusing the fold.
    #[tokio::test]
    async fn projected_statistics_are_inexact_because_the_report_is_a_snapshot() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{ColumnStatistics, ScalarValue};

        let registry = make_registry().await;
        let table = TableReference::bare("orders");

        registry.record_executor_statistics(
            &table,
            "executor-1".to_string(),
            Statistics {
                num_rows: Precision::Exact(1_000_000),
                total_byte_size: Precision::Exact(64_000_000),
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(1_000_000))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
                    distinct_count: Precision::Exact(1_000_000),
                    ..ColumnStatistics::new_unknown()
                }],
            },
            vec!["id".to_string()],
        );

        let snapshot = registry.executor_statistics_snapshot(&table);
        let entry = snapshot.get("executor-1").expect("recorded entry present");
        // `total` is not in the report, so it projects to unknown.
        let leaf: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("total", DataType::Float64, true),
        ]));
        let projected = entry.projected_onto(&leaf);

        assert_eq!(
            projected.num_rows,
            Precision::Inexact(1_000_000),
            "a cached remote row count must not be Exact — aggregate_statistics would fold COUNT(*) to it"
        );
        assert_eq!(
            projected.num_rows.get_value(),
            Some(&1_000_000),
            "the value itself must survive so the planner can still size joins"
        );

        let id = &projected.column_statistics[0];
        assert_eq!(
            id.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1))),
            "cached column bounds must not be Exact — MIN() would be folded to them"
        );
        assert_eq!(
            id.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(1_000_000))),
            "cached column bounds must not be Exact — MAX() would be folded to them"
        );
        assert_eq!(id.null_count, Precision::Inexact(0));
        assert_eq!(id.distinct_count, Precision::Inexact(1_000_000));

        let total = &projected.column_statistics[1];
        assert_eq!(
            total.min_value,
            Precision::Absent,
            "a column the executor did not report stays unknown"
        );
        assert_eq!(total.max_value, Precision::Absent);
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
        registry.register("e1".to_string(), "", tx1).await;
        let e2 = registry.register("e2".to_string(), "", tx2).await;
        let e3 = registry.register("e3".to_string(), "", tx3).await;
        assert_eq!(registry.connected_executor_count().await, 3);
        assert_eq!(registry.flight_sql_clients_count().await, 0);

        // Two of them complete the handshake (AllocateInitialPartitions) → ready.
        registry
            .insert_flight_sql_client(
                "e1".to_string(),
                "127.0.0.1:50101".to_string(),
                "",
                dummy_flight_sql_client(),
            )
            .await;
        registry
            .insert_flight_sql_client(
                "e2".to_string(),
                "127.0.0.1:50102".to_string(),
                "",
                dummy_flight_sql_client(),
            )
            .await;
        assert_eq!(registry.connected_executor_count().await, 3);
        assert_eq!(registry.flight_sql_clients_count().await, 2);

        // Unregister one ready executor — both counts drop.
        registry.unregister("e2", e2.connection).await;
        assert_eq!(registry.connected_executor_count().await, 2);
        assert_eq!(registry.flight_sql_clients_count().await, 1);

        // Unregister the not-yet-ready executor — connected drops, ready unchanged.
        registry.unregister("e3", e3.connection).await;
        assert_eq!(registry.connected_executor_count().await, 1);
        assert_eq!(registry.flight_sql_clients_count().await, 1);
    }

    #[tokio::test]
    async fn test_unregister_nonexistent() {
        let registry = make_registry().await;
        let (tx, _rx) = mpsc::channel(1);

        let registered = registry.register("executor-1".to_string(), "", tx).await;

        // Unregistering a non-existent executor should not panic
        assert!(
            !registry
                .unregister("executor-nonexistent", registered.connection)
                .await
        );

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

    fn id_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]))
    }

    /// A table with no partition metadata has not been discovered: reading
    /// one executor would return only that executor's rows, or none.
    #[tokio::test]
    async fn a_table_without_partition_metadata_does_not_resolve() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        let table = TableReference::bare("t");

        let error = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("the table has no partition metadata");
        assert!(
            runtime_datafusion::error::is_partitions_unavailable(&error),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("have not been discovered yet"),
            "unexpected error: {error}"
        );

        // An accelerated table whose partitions are not discovered, such as one
        // created by DDL, is read from one executor.
        let partitions = registry
            .resolve_accelerated_partitions(&table, &id_schema(), false)
            .expect("a table that is not discovered resolves");
        assert_eq!(partitions.len(), 1);
    }

    /// Each executor of a federated table is read for the partitions it serves,
    /// so a scan filtered to them returns none of the rows it holds for others.
    #[tokio::test]
    async fn federated_scans_carry_the_partitions_their_executor_serves() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        ready_executor(&registry, "executor-2", "").await;
        let table = TableReference::full("ice", "ns", "t");
        let store = registry.federated_partition_store();
        store
            .initialize_metadata(&table, vec!["region".to_string()])
            .await
            .expect("initialize");
        let region = |value: &str| HashMap::from([("region".to_string(), Some(value.to_string()))]);
        let (east, west, north) = (region("'east'"), region("'west'"), region("'north'"));
        store
            .add_and_assign_partitions(
                &table,
                &[
                    (&east, "executor-1"),
                    (&west, "executor-2"),
                    (&north, "executor-1"),
                ],
            )
            .await
            .expect("assign");

        let mut served: Vec<Vec<String>> = FederatedPartitionProvider::from_registry(&registry)
            .get_partitions(&table, &id_schema())
            .expect("resolve")
            .into_iter()
            .map(|(_, values)| {
                let mut regions: Vec<String> = values
                    .iter()
                    .filter_map(|value| value.get("region").cloned().flatten())
                    .collect();
                regions.sort();
                regions
            })
            .collect();
        served.sort();
        assert_eq!(served, [vec!["'east'", "'north'"], vec!["'west'"]]);
    }

    /// The accelerated rewrite runs first. A table whose partitions the acceleration
    /// store holds is routed there, and the per-executor scans it leaves keep the
    /// table's name, so the federated rewrite must not partition them again. The
    /// single-executor scan it leaves for a table only the federated store
    /// partitions is still expanded.
    #[tokio::test]
    async fn a_table_the_acceleration_store_partitions_is_not_partitioned_again() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        let table = TableReference::bare("t");
        registry
            .federated_partition_store()
            .initialize_metadata(&table, vec!["id".to_string()])
            .await
            .expect("initialize federated");
        let provider = FederatedPartitionProvider::from_registry(&registry);
        let client = registry
            .flight_sql_clients_snapshot()
            .await
            .remove("executor-1")
            .expect("client");
        let executor_scan = {
            let source = flight_sql_table_provider("executor-1", client, &table, id_schema(), None);
            let plan = datafusion::logical_expr::LogicalPlanBuilder::scan(
                table.clone(),
                datafusion::datasource::provider_as_source(source),
                None,
            )
            .expect("scan")
            .build()
            .expect("plan");
            let datafusion::logical_expr::LogicalPlan::TableScan(scan) = plan else {
                panic!("a table scan");
            };
            scan
        };
        assert!(
            provider.should_partition(&executor_scan),
            "a table only the federated store partitions is expanded"
        );

        registry
            .accelerations_partition_store()
            .initialize_metadata(&table, vec!["id".to_string()])
            .await
            .expect("initialize acceleration");
        assert!(
            !provider.should_partition(&executor_scan),
            "a table the acceleration store partitions is left to the accelerated rewrite"
        );
    }

    async fn ready_executor(registry: &ExecutorRegistry, executor_id: &str, process_id: &str) {
        let (tx, rx) = mpsc::channel(1);
        std::mem::forget(rx);
        registry
            .register(executor_id.to_string(), process_id, tx)
            .await;
        registry
            .insert_flight_sql_client(
                executor_id.to_string(),
                "127.0.0.1:1".to_string(),
                process_id,
                dummy_flight_sql_client(),
            )
            .await;
    }

    fn bucket(value: i64) -> PartitionValue {
        HashMap::from([("bucket(2, id)".to_string(), Some(value.to_string()))])
    }

    /// Seeds `t` with one partition per entry of `owners`, assigned to it.
    async fn seed_assigned(registry: &ExecutorRegistry, owners: &[&str]) -> TableReference {
        let table = TableReference::bare("t");
        let store = registry.accelerations_partition_store();
        let partitions: Vec<PartitionValue> = (0_i64..).take(owners.len()).map(bucket).collect();
        store
            .set_unassigned_partitions(
                &table,
                partitions.clone(),
                vec!["bucket(2, id)".to_string()],
            )
            .await
            .expect("seed partitions");
        for (partition, owner) in partitions.iter().zip(owners) {
            store
                .assign_partition(&table, partition, owner)
                .await
                .expect("assign partition");
        }
        table
    }

    fn loaded(process_id: &str, buckets: &[i64]) -> LoadedPartitions {
        LoadedPartitions {
            process_id: process_id.to_string(),
            partitions: buckets.iter().map(|b| partition_key(&bucket(*b))).collect(),
        }
    }

    /// A partition assigned to a ready executor that has not reported it loaded
    /// is not read: the executor would answer from data that lacks it.
    #[tokio::test]
    async fn an_assigned_partition_not_reported_loaded_does_not_resolve() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        let table = seed_assigned(&registry, &["executor-1", "executor-1"]).await;

        let error = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("no partition is loaded");
        assert!(
            error
                .to_string()
                .contains("no ready executor serves 2 of its 2 partitions"),
            "unexpected error: {error}"
        );
        assert!(runtime_datafusion::error::is_partitions_unavailable(&error));

        registry.set_loaded_partitions(
            &table,
            HashMap::from([("executor-1".to_string(), loaded("", &[0, 1]))]),
        );
        let partitions = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect("every partition is loaded");
        assert_eq!(partitions.len(), 1);
    }

    /// Partitions an earlier process of the executor reported loaded do not
    /// count for the process now registered.
    #[tokio::test]
    async fn partitions_an_earlier_process_loaded_do_not_count() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "p2").await;
        let table = seed_assigned(&registry, &["executor-1"]).await;
        registry.set_loaded_partitions(
            &table,
            HashMap::from([("executor-1".to_string(), loaded("p1", &[0]))]),
        );

        registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("the loaded partition belongs to an earlier process");
    }

    /// A partition added after the whole table was last loaded is skipped
    /// until it loads; a partition of that complete set that is no longer
    /// loaded fails the query.
    #[tokio::test]
    async fn a_new_partition_is_read_once_loaded_and_a_lost_one_fails() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        let table = seed_assigned(&registry, &["executor-1"]).await;
        registry.set_loaded_partitions(
            &table,
            HashMap::from([("executor-1".to_string(), loaded("", &[0]))]),
        );
        registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect("the whole table is loaded");

        let store = registry.accelerations_partition_store();
        store
            .apply_discovery(&table, &["bucket(2, id)".to_string()], &[bucket(1)], &[])
            .await
            .expect("discover a partition");
        store
            .assign_partition(&table, &bucket(1), "executor-1")
            .await
            .expect("assign partition");
        let partitions = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect("the new partition is skipped until it loads");
        assert_eq!(partitions.len(), 1);

        registry.set_loaded_partitions(
            &table,
            HashMap::from([("executor-1".to_string(), loaded("", &[1]))]),
        );
        let error = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("a partition of the complete set is no longer loaded");
        assert!(
            error
                .to_string()
                .contains("no ready executor serves 1 of its 2 partitions"),
            "unexpected error: {error}"
        );
    }

    /// When a partition replaces one of the last set read whole, its rows may
    /// have moved there, so the table is not read until it loads.
    #[tokio::test]
    async fn a_partition_that_replaces_a_read_one_is_not_skipped() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        let table = seed_assigned(&registry, &["executor-1"]).await;
        registry.set_loaded_partitions(
            &table,
            HashMap::from([("executor-1".to_string(), loaded("", &[0]))]),
        );
        registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect("the whole table is loaded");

        let store = registry.accelerations_partition_store();
        store
            .apply_discovery(
                &table,
                &["bucket(2, id)".to_string()],
                &[bucket(1)],
                &[bucket(0)],
            )
            .await
            .expect("replace the partition");
        store
            .assign_partition(&table, &bucket(1), "executor-1")
            .await
            .expect("assign partition");
        let error = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("the replacement has not loaded");
        assert!(
            error
                .to_string()
                .contains("no ready executor serves 1 of its 1 partitions"),
            "unexpected error: {error}"
        );
    }

    /// An executor that holds a partition the table assigns to another
    /// executor is not read: its scan would return that partition's rows too.
    #[tokio::test]
    async fn an_executor_holding_another_executors_partition_is_not_read() {
        let registry = make_registry().await;
        ready_executor(&registry, "executor-1", "").await;
        ready_executor(&registry, "executor-2", "").await;
        let table = seed_assigned(&registry, &["executor-1", "executor-2"]).await;
        registry.set_loaded_partitions(
            &table,
            HashMap::from([
                ("executor-1".to_string(), loaded("", &[0, 1])),
                ("executor-2".to_string(), loaded("", &[1])),
            ]),
        );

        let error = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("executor-1 also holds executor-2's partition");
        assert!(
            error
                .to_string()
                .contains("no ready executor serves 1 of its 2 partitions"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn partitions_without_a_ready_executor_do_not_resolve() {
        let registry = make_registry().await;
        let table = TableReference::bare("t");
        let store = registry.accelerations_partition_store();
        let partitions: Vec<PartitionValue> = (0..2)
            .map(|bucket| HashMap::from([("bucket(2, id)".to_string(), Some(bucket.to_string()))]))
            .collect();
        store
            .set_unassigned_partitions(
                &table,
                partitions.clone(),
                vec!["bucket(2, id)".to_string()],
            )
            .await
            .expect("seed partitions");
        store
            .assign_partition(&table, &partitions[0], "executor-1")
            .await
            .expect("assign partition");
        // Connected, but without a FlightSQL client, so not ready.
        let (tx, _rx) = mpsc::channel(1);
        registry.register("executor-1".to_string(), "", tx).await;

        let error = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect_err("no partition has a ready executor");
        assert!(
            error
                .to_string()
                .contains("no ready executor serves 2 of its 2 partitions"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_table_with_no_partitions_resolves_to_no_scans() {
        let registry = make_registry().await;
        let table = TableReference::bare("t");
        registry
            .accelerations_partition_store()
            .set_unassigned_partitions(&table, Vec::new(), vec!["bucket(2, id)".to_string()])
            .await
            .expect("seed partitions");

        let partitions = registry
            .resolve_accelerated_partitions(&table, &id_schema(), true)
            .expect("a table with no partitions resolves");
        assert!(partitions.is_empty());
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
    /// `rx`, extracts the `request_id` from `UpdatePartitions`, and delivers
    /// an `Ack` (with the provided error, if any) via `pending_acks`.
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
                    let request_id = up.request_id;
                    if request_id.is_empty() {
                        continue; // legacy fire-and-forget
                    }
                    pending_acks.deliver(
                        &request_id,
                        Ack {
                            request_id: request_id.clone(),
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
        let handles = registry.register("e1".to_string(), "", tx).await.handles;
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
        let handles = registry.register("e1".to_string(), "", tx).await.handles;
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
        let _handles = registry.register("e1".to_string(), "", tx).await;

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

        assert!(
            matches!(err, Error::ExecutorNotRegistered { .. }),
            "got {err:?}"
        );
        assert!(!err.is_retryable());
    }
}
