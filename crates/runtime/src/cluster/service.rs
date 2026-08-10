/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

//! Internal cluster gRPC service implementation.
//!
//! This service handles scheduler-executor communication for cluster mode,
//! including app definition retrieval, secret expansion, and control stream
//! management for sending `PollNow` commands to executors.

use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;

use app::App;
use arrow::array::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_ipc::writer::StreamWriter;
use data_components::flightsql::FlightSqlClient;

use datafusion::{
    prelude::Expr,
    sql::{
        TableReference,
        sqlparser::{
            ast::{Ident, ObjectNamePart, visit_relations_mut},
            dialect::PostgreSqlDialect,
            parser::Parser,
        },
    },
};

use ballista_core::serde::protobuf::{ExecutorStoppedParams, scheduler_grpc_server::SchedulerGrpc};

use datafusion_proto::bytes::Serializeable;
use flight_client::{
    MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE, configure_endpoint_for_high_throughput,
    cookie::{CookieService, CookieStore},
};
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::RwLock;
use runtime_proto::{
    AllocateInitialPartitionsRequest, AllocateInitialPartitionsResponse, BytesArray,
    CancelTasksCommand, ExecutorControlMessage, ExpandSecretRequest, ExpandSecretResponse,
    GetAppDefinitionRequest, GetAppDefinitionResponse, GetDdlCatchupRequest, GetDdlCatchupResponse,
    GetMetricsRequest, GetMetricsResponse, GetSchedulersRequest, GetSchedulersResponse,
    GetTaskHistoryRequest, GetTaskHistoryResponse, PollNowCommand, SchedulerControlMessage,
    SchedulerInstance, TaskCancelInfo, cluster_service_server::ClusterService,
    executor_control_message::Message as ExecutorMessage,
    scheduler_control_message::Message as SchedulerMessage,
};
use runtime_secrets::{SECRETS, Secrets, iter_secret_references};
use secrecy::ExposeSecret;
use std::collections::{HashMap, HashSet};
use std::task::{Context, Poll};
use tokio::sync::RwLock as TokioRwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tonic::{
    Request, Response, Status, Streaming,
    transport::{ClientTlsConfig, Endpoint},
};

use crate::cluster::{
    ExecutorRegistry, TablePartitions,
    {SchedulerPeers, partition::partition_value_to_bytes},
};
use crate::datafusion::{
    DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, SPICE_RUNTIME_SCHEMA,
};
use crate::metrics_reader::MetricsReader;
use crate::task_history::{DEFAULT_TASK_HISTORY_TABLE, LOCAL_TASK_HISTORY_TABLE};

/// Handle for sending messages to a connected executor.
struct ExecutorStreamHandle {
    tx: mpsc::Sender<SchedulerControlMessage>,
}

/// Shared registry of connected executor control streams.
///
/// This is extracted from `ClusterServiceImpl` to allow sharing with the
/// scheduler callback for broadcasting `PollNow` notifications.
#[derive(Clone, Default)]
pub struct ExecutorControlStreamRegistry {
    streams: Arc<RwLock<HashMap<String, ExecutorStreamHandle>>>,
}

impl ExecutorControlStreamRegistry {
    /// Creates a new empty executor stream registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Broadcasts a `PollNow` command to all connected executors.
    ///
    /// This notifies executors that new work may be available, causing them
    /// to immediately poll for tasks rather than waiting for the next poll interval.
    pub fn broadcast_poll_now(&self, reason: &str) {
        let streams = self.streams.read();
        if streams.is_empty() {
            return;
        }

        let message = SchedulerControlMessage {
            message: Some(SchedulerMessage::PollNow(PollNowCommand {
                reason: reason.to_string(),
            })),
        };

        let count = streams.len();
        for (executor_id, handle) in streams.iter() {
            // Use try_send to avoid blocking. If the channel is full, the executor
            // will poll on its next interval anyway.
            if let Err(e) = handle.tx.try_send(message.clone()) {
                tracing::debug!("Failed to send PollNow to executor {executor_id}: {e}");
            }
        }

        tracing::debug!("Broadcast PollNow to {count} executors: {reason}");
    }

    /// Sends a `CancelTasks` command to a specific connected executor.
    ///
    /// Returns `true` when the message is accepted into the outbound channel.
    #[must_use]
    pub fn send_cancel_tasks(&self, executor_id: &str, tasks: Vec<TaskCancelInfo>) -> bool {
        let streams = self.streams.read();
        let Some(handle) = streams.get(executor_id) else {
            return false;
        };

        let message = SchedulerControlMessage {
            message: Some(SchedulerMessage::CancelTasks(CancelTasksCommand { tasks })),
        };

        handle.tx.try_send(message).is_ok()
    }

    /// Registers an executor stream for receiving control messages.
    pub(crate) fn register(&self, executor_id: &str, tx: mpsc::Sender<SchedulerControlMessage>) {
        let mut streams = self.streams.write();
        streams.insert(executor_id.to_string(), ExecutorStreamHandle { tx });
        tracing::debug!(
            "Registered executor stream: {executor_id} (total: {})",
            streams.len()
        );
    }

    /// Unregisters an executor stream.
    pub(crate) fn unregister(&self, executor_id: &str) {
        let mut streams = self.streams.write();
        if streams.remove(executor_id).is_some() {
            tracing::debug!(
                "Unregistered executor stream: {executor_id} (remaining: {})",
                streams.len()
            );
        }
    }
}

/// Internal cluster service for scheduler-executor communication.
pub struct ClusterServiceImpl {
    app: Arc<TokioRwLock<Option<Arc<App>>>>,
    secrets: Arc<TokioRwLock<Secrets>>,
    advertise_address: String,
    scheduler_peers: Arc<TokioRwLock<SchedulerPeers>>,
    datafusion: Arc<DataFusion>,
    executor_registry: Arc<ExecutorRegistry>,
    /// Metrics reader for collecting local OTLP metrics on demand.
    metrics_reader: Option<MetricsReader>,
    allow_secret_expansion: bool,
    /// Registry of connected executor streams for [`PollNow`] broadcasts.
    executor_streams: ExecutorControlStreamRegistry,
}

impl ClusterServiceImpl {
    /// Creates a new cluster service implementation.
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        app: Arc<TokioRwLock<Option<Arc<App>>>>,
        secrets: Arc<TokioRwLock<Secrets>>,
        advertise_address: String,
        scheduler_peers: Arc<TokioRwLock<SchedulerPeers>>,
        datafusion: Arc<DataFusion>,
        executor_registry: Arc<ExecutorRegistry>,
        metrics_reader: Option<MetricsReader>,
        allow_secret_expansion: bool,
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
            datafusion,
            executor_registry,
            metrics_reader,
            allow_secret_expansion,
            executor_streams: ExecutorControlStreamRegistry::new(),
        }
    }

    /// Creates a new cluster service with a pre-existing executor stream registry.
    ///
    /// This allows sharing the registry with the scheduler callback for
    /// broadcasting `PollNow` notifications.
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn with_executor_streams(
        app: Arc<TokioRwLock<Option<Arc<App>>>>,
        secrets: Arc<TokioRwLock<Secrets>>,
        advertise_address: String,
        scheduler_peers: Arc<TokioRwLock<SchedulerPeers>>,
        datafusion: Arc<DataFusion>,
        executor_registry: Arc<ExecutorRegistry>,
        metrics_reader: Option<MetricsReader>,
        executor_streams: ExecutorControlStreamRegistry,
        allow_secret_expansion: bool,
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
            datafusion,
            executor_registry,
            metrics_reader,
            allow_secret_expansion,
            executor_streams,
        }
    }

    /// Returns a clone of the executor stream registry.
    ///
    /// This can be used to share the registry with the scheduler callback.
    #[must_use]
    pub fn executor_streams(&self) -> ExecutorControlStreamRegistry {
        self.executor_streams.clone()
    }

    /// Broadcasts a `PollNow` command to all connected executors.
    ///
    /// This notifies executors that new work may be available, causing them
    /// to immediately poll for tasks rather than waiting for the next poll interval.
    pub fn broadcast_poll_now(&self, reason: &str) {
        self.executor_streams.broadcast_poll_now(reason);
    }

    /// Returns the first accelerated table that hasn't yet been registered in
    /// the local `SessionContext`, or `None` if every accelerated table is
    /// ready. Snapshots the app under the read lock and releases it before
    /// the per-table `get_table` lookups so no async guard is held across
    /// `.await`.
    async fn first_unready_accelerated_table(&self) -> Option<TableReference> {
        let app = self.app.read().await.clone();
        let app = app?;
        super::partition::first_unready_accelerated_table(&app, self.datafusion.as_ref()).await
    }

    /// Whether the app declares any accelerated, partitioned table. When it does
    /// not, there is nothing for the scheduler to assign, so the
    /// first-assignment gate in `allocate_initial_partitions` must be bypassed —
    /// otherwise executors would block and retry for a full assignment interval
    /// during startup with no assignment ever coming.
    async fn has_partitioned_accelerated_tables(&self) -> bool {
        let Some(app) = self.app.read().await.clone() else {
            return false;
        };
        !super::partition::accelerated_tables(&app).is_empty()
    }

    /// Returns the executor registry for use by other components.
    #[must_use]
    pub fn executor_registry(&self) -> Arc<ExecutorRegistry> {
        Arc::clone(&self.executor_registry)
    }
}

struct ControlStreamOutbound {
    inner: ReceiverStream<SchedulerControlMessage>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    _outbound_tx: mpsc::Sender<SchedulerControlMessage>,
}

impl Stream for ControlStreamOutbound {
    type Item = Result<SchedulerControlMessage, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(message)) => Poll::Ready(Some(Ok(message))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ControlStreamOutbound {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

#[tonic::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn get_app_definition(
        &self,
        request: Request<GetAppDefinitionRequest>,
    ) -> Result<Response<GetAppDefinitionResponse>, Status> {
        let request = request.into_inner();
        tracing::trace!(
            "ClusterService::get_app_definition for executor {}",
            request.executor_id
        );

        let app_json = {
            let app_guard = self.app.read().await;
            let Some(ref app) = *app_guard else {
                return Err(Status::internal("App context not available"));
            };
            serde_json::to_string(app.as_ref())
                .map_err(|e| Status::internal(format!("Failed to serialize app: {e}")))?
        };

        // Snapshot the DDL log so the executor can replay DDL-created tables/schemas.
        let (ddl_statements, ddl_version) = self.executor_registry.ddl_snapshot().await;

        Ok(Response::new(GetAppDefinitionResponse {
            app_json,
            ddl_statements,
            ddl_version,
        }))
    }

    async fn expand_secret(
        &self,
        request: Request<ExpandSecretRequest>,
    ) -> Result<Response<ExpandSecretResponse>, Status> {
        let request = request.into_inner();

        if !self.allow_secret_expansion {
            tracing::warn!(
                executor_id = %request.executor_id,
                "Denied cluster secret expansion without mTLS"
            );
            return Err(Status::permission_denied(
                "Secret expansion requires cluster mTLS",
            ));
        }

        // Empty key is never a valid secret reference and is rejected before
        // any store lookup or allowlist work.
        if request.key.is_empty() {
            return Err(Status::invalid_argument(
                "Unable to expand secret: empty key",
            ));
        }

        let span = tracing::span!(
            target: "task_history",
            tracing::Level::INFO,
            "cluster::expand_secret",
            executor_id = %request.executor_id,
            key = %request.key
        );
        let _guard = span.enter();

        tracing::trace!(
            "ClusterService::expand_secret for executor {}, key {}",
            request.executor_id,
            request.key
        );

        // Only keys referenced by the current app (spicepod) may be expanded.
        // This closes the "any mTLS peer can request any env/vault key by name"
        // hole: unreferenced secrets in the host environment or external stores
        // are never returned, and unallowlisted keys never hit the secret store
        // (so deny does not create a lookup side-channel).
        //
        // Snapshot the `Arc<App>` under the lock and drop the guard before the
        // (CPU-bound) allowlist build so ExpandSecret does not hold the app
        // write path while serializing/scanning the spicepod.
        let Some(app) = self.app.read().await.clone() else {
            tracing::warn!(
                executor_id = %request.executor_id,
                "Denied cluster secret expansion: app context not available"
            );
            return Err(Status::failed_precondition(
                "Secret expansion requires a loaded app definition",
            ));
        };
        let allowed_keys = expandable_secret_keys(&app);

        let Some(allowed_stores) = allowed_keys.get(request.key.as_str()) else {
            tracing::warn!(
                executor_id = %request.executor_id,
                key = %request.key,
                "Denied cluster secret expansion: key is not referenced by the app"
            );
            // Same status/message shape as a miss so callers cannot distinguish
            // "not in spicepod" from "not in any store" for unallowlisted keys.
            return Err(Status::invalid_argument(format!(
                "Unable to expand secret {}",
                request.key
            )));
        };

        tracing::debug!(
            "ExpandSecret: expanding secret {} for executor {}",
            request.key,
            request.executor_id
        );

        let secrets = Secrets::snapshot(&self.secrets).await;
        // A reference through the `secrets:` sentinel keeps its normal
        // "search every configured store in precedence order" resolution; a
        // reference scoped to a specific store (e.g. `${ env:KEY }`) is
        // restricted to that store, so a same-named key in an unrelated
        // store can't answer in its place.
        let lookup = if allowed_stores.contains(SECRETS) {
            secrets.get_secret(&request.key).await
        } else {
            secrets
                .get_secret_from_stores(&request.key, allowed_stores)
                .await
        };
        let Some(value) =
            lookup.map_err(|e| Status::internal(format!("Failed to get secret: {e}")))?
        else {
            tracing::error!(target: "task_history", "Secret not found");
            return Err(Status::invalid_argument(format!(
                "Unable to expand secret {}",
                request.key
            )));
        };

        let exposed = value.expose_secret();

        tracing::debug!(target: "task_history", "Secret expanded successfully");

        Ok(Response::new(ExpandSecretResponse {
            key: request.key,
            value: exposed.to_string(),
        }))
    }

    async fn get_schedulers(
        &self,
        _request: Request<GetSchedulersRequest>,
    ) -> Result<Response<GetSchedulersResponse>, Status> {
        tracing::debug!("ClusterService::get_schedulers request");

        let peers = self.scheduler_peers.read().await;
        let mut schedulers = peers
            .values()
            .map(|record| SchedulerInstance {
                advertise_address: record.advertise_address.clone(),
                labels: record.labels.clone(),
            })
            .collect::<Vec<_>>();

        if schedulers.is_empty() {
            schedulers.push(SchedulerInstance {
                advertise_address: self.advertise_address.clone(),
                labels: std::collections::HashMap::new(),
            });
        }

        let scheduler_addresses = schedulers
            .iter()
            .map(|scheduler| scheduler.advertise_address.as_str())
            .collect::<Vec<_>>()
            .join(",");
        tracing::debug!(
            "ClusterService::get_schedulers response schedulers=[{scheduler_addresses}]"
        );

        Ok(Response::new(GetSchedulersResponse { schedulers }))
    }
    async fn get_task_history(
        &self,
        request: Request<GetTaskHistoryRequest>,
    ) -> Result<Response<GetTaskHistoryResponse>, Status> {
        let request = request.into_inner();

        tracing::debug!(
            "ClusterService::get_task_history executing query: {}",
            request.sql
        );

        // Parse and rewrite the SQL to query local_task_history instead of task_history.
        // This avoids infinite recursion: the federated task_history table would fan out
        // to peers, but peers need to query their local data only.
        let local_sql = rewrite_task_history_sql(&request.sql)
            .map_err(|e| Status::invalid_argument(format!("Invalid task history query: {e}")))?;

        // Always run under the strict read-only validator: GetTaskHistory is a
        // cluster-internal fan-in for observability, not a general SQL surface.
        // Without this gate, a peer could smuggle DDL/DML that merely *mentions*
        // task_history (e.g. `INSERT INTO writable SELECT * FROM runtime.task_history`)
        // and execute it with the scheduler's full DataFusion context.
        let query_result = self
            .datafusion
            .query_builder(&local_sql)
            .read_only(true)
            .build()
            .run()
            .await
            .map_err(|e| map_task_history_query_error(&e))?;

        // Collect all record batches
        let batches: Vec<RecordBatch> = query_result
            .data
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Failed to collect query results: {e}")))?;

        // Encode as Arrow IPC
        let arrow_ipc = encode_batches_to_ipc(&batches)
            .map_err(|e| Status::internal(format!("Failed to encode results as Arrow IPC: {e}")))?;

        Ok(Response::new(GetTaskHistoryResponse { arrow_ipc }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        // Collect local OTLP metrics and return as protobuf bytes
        let otlp_metrics = self
            .metrics_reader
            .as_ref()
            .map(MetricsReader::collect_otlp)
            .unwrap_or_default();

        Ok(Response::new(GetMetricsResponse { otlp_metrics }))
    }

    type ControlStreamStream =
        Pin<Box<dyn Stream<Item = Result<SchedulerControlMessage, Status>> + Send>>;

    async fn control_stream(
        &self,
        request: Request<Streaming<ExecutorControlMessage>>,
    ) -> Result<Response<Self::ControlStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let cancel = CancellationToken::new();
        let inbound_cancel = cancel.clone();

        // Create a channel for outbound messages to the executor.
        let (outbound_tx, outbound_rx) = mpsc::channel::<SchedulerControlMessage>(32);

        // Clone the executor_streams registry for use in the spawned task.
        let executor_streams = self.executor_streams.clone();

        // Clone outbound_tx for registration after we identify the executor.
        let registration_tx = outbound_tx.clone();

        // We need to identify the executor from its first message.
        // Spawn a task to handle the bidirectional stream.
        let executor_registry = Arc::clone(&self.executor_registry);
        let datafusion = Arc::clone(&self.datafusion);
        let outbound_tx_for_registry = outbound_tx.clone();
        let metrics_node_id = self.advertise_address.clone();
        let inbound_task = tokio::spawn(async move {
            let executor_id = match inbound.next().await {
                Some(Ok(msg)) => {
                    let executor_id = msg.executor_id.clone();
                    if executor_id.is_empty() {
                        tracing::warn!("Executor connected with empty executor_id, closing stream");
                        return;
                    }
                    tracing::debug!("Executor control stream connected: {executor_id}");

                    // Handle the first message if it contains data.
                    if let Some(message) = msg.message {
                        handle_executor_message(&executor_id, &message, &datafusion).await;
                    }
                    executor_id
                }
                Some(Err(e)) => {
                    tracing::warn!("Error receiving first executor control message: {e}");
                    return;
                }
                None => {
                    tracing::debug!("Executor control stream closed before sending any messages");
                    return;
                }
            };

            // Register the executor with the registry.
            let handles = executor_registry
                .register(executor_id.clone(), outbound_tx_for_registry)
                .await;

            // Update active executor count metric.
            let count = executor_registry.connected_executors().await.len();
            runtime_metrics::cluster::set_active_executor_count(&metrics_node_id, count as u64);

            // Register the executor stream for PollNow broadcasts.
            executor_streams.register(&executor_id, registration_tx);

            loop {
                tokio::select! {
                    () = inbound_cancel.cancelled() => {
                        tracing::debug!("Executor control stream cancelled: {executor_id}");
                        break;
                    }
                    result = inbound.next() => {
                        match result {
                            Some(Ok(msg)) => {
                                if let Some(message) = msg.message {
                                    // Route correlated responses (metrics, acks) to their waiters;
                                    // anything else goes to the general handler.
                                    match &message {
                                        ExecutorMessage::Metrics(response) => {
                                            if !handles
                                                .pending_metrics
                                                .deliver(&response.request_id, response.clone())
                                            {
                                                tracing::warn!(
                                                    "Received metrics response for unknown request_id: {}",
                                                    response.request_id
                                                );
                                            }
                                        }
                                        ExecutorMessage::Ack(ack) => {
                                            if !handles
                                                .pending_acks
                                                .deliver(&ack.request_id, ack.clone())
                                            {
                                                tracing::warn!(
                                                    "Received ack for unknown request_id: {}",
                                                    ack.request_id
                                                );
                                            }
                                        }
                                        _ => {
                                            handle_executor_message(
                                                &executor_id,
                                                &message,
                                                &datafusion,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                tracing::debug!("Executor control stream error for {executor_id}: {e}");
                                break;
                            }
                            None => {
                                tracing::debug!("Executor control stream closed by executor {executor_id}");
                                break;
                            }
                        }
                    }
                }
            }

            // Unregister the executor when the stream ends.
            executor_registry.unregister(&executor_id).await;

            // Update active executor count metric.
            let count = executor_registry.connected_executors().await.len();
            runtime_metrics::cluster::set_active_executor_count(&metrics_node_id, count as u64);

            // Unregister the executor stream.
            executor_streams.unregister(&executor_id);

            // Drop any partition-load acks the executor had; until it
            // reconnects (or another executor takes over and acks), the
            // affected datasets should no longer be considered loaded.
            if let Some(tracker) = datafusion.partition_load_tracker.as_ref() {
                tracker.drop_executor(&executor_id).await;
            }

            tracing::debug!("Executor control stream ended: {executor_id}");
        });

        let stream = ControlStreamOutbound {
            inner: ReceiverStream::new(outbound_rx),
            cancel,
            task: inbound_task,
            _outbound_tx: outbound_tx,
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn allocate_initial_partitions(
        &self,
        request: Request<AllocateInitialPartitionsRequest>,
    ) -> Result<Response<AllocateInitialPartitionsResponse>, Status> {
        let AllocateInitialPartitionsRequest { executor_url } = request.into_inner();

        // Current standard is to have executor id be without scheme.
        let executor_id = if let Some(index) = executor_url.find("://") {
            &executor_url[index + 3..]
        } else {
            &executor_url
        };

        // Gate on dataset readiness. Partition-expression serialization needs
        // every accelerated table's schema to be in the SessionContext; if a
        // dataset is still loading from its source, `partition_value_to_bytes`
        // would fail with "Table not found when parsing expression" — return
        // a transient `Unavailable` so the executor retries with backoff.
        if let Some(not_ready) = self.first_unready_accelerated_table().await {
            tracing::debug!(
                executor = %executor_id,
                table = %not_ready,
                "Deferring allocate_initial_partitions: accelerated table not yet registered"
            );
            return Err(Status::unavailable(format!(
                "partition metadata not ready: accelerated table {not_ready} still loading"
            )));
        }

        // Gate on the scheduler's first assignment cycle. Returning already-assigned
        // partitions before the scheduler has fairly distributed them would hand this
        // executor an empty set, and its initial snapshot would load zero rows with no
        // way to backfill (CDC/Changes-mode accelerations only load partition data at
        // the initial snapshot). Wait for the first cycle — the executor retries this
        // RPC on `Unavailable` with backoff — so the returned share is the fair one.
        //
        // Only gate when there are accelerated partitioned tables to assign. With
        // none, the scheduler never assigns anything, so blocking would just make
        // executors retry for a full assignment interval during startup for no
        // reason.
        if let Some(partition_service) = self.datafusion.partition_service.as_ref()
            && !partition_service.is_first_assignment_complete()
            && self.has_partitioned_accelerated_tables().await
        {
            tracing::debug!(
                executor = %executor_id,
                "Deferring allocate_initial_partitions: first assignment cycle not yet complete"
            );
            return Err(Status::unavailable(
                "partition assignment pending: scheduler has not completed its first assignment cycle",
            ));
        }

        let tls_config_opt = self.datafusion.cluster_config.client_tls_config();
        match create_executor_flight_client(&executor_url, tls_config_opt) {
            Ok(client) => {
                self.executor_registry
                    .insert_flight_sql_client(executor_id.to_string(), client)
                    .await;
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create Flight SQL client for executor {executor_id}: {e}"
                );
            }
        }

        let mut table_partitions: HashMap<String, BytesArray> = HashMap::new();

        let partition_store = self.executor_registry().accelerations_partition_store();
        // Snapshot the `Arc<App>` out of the lock and release the guard before the
        // loop — `partition_value_to_bytes` is awaited per partition below, and
        // holding the async `RwLock` read guard across those awaits would block
        // writers (and risk deadlock if an awaited path re-acquires the lock).
        let app_snapshot = self.app.read().await.clone();
        if let Some(app) = app_snapshot.as_ref() {
            // Partition assignment is driven solely by the scheduler's periodic
            // partition-assignment cycle, which fairly distributes partitions
            // across all connected executors and pushes them over the control
            // stream (`notify_executor_of_assignments`). This RPC no longer
            // allocates — it only returns partitions *already assigned* to this
            // executor so a reconnecting or failed-over executor recovers its
            // existing assignments. On a cold start this is empty; the first
            // assignment cycle assigns and pushes shortly after.
            for table_ref in super::partition::accelerated_tables(app).keys() {
                let Some(metadata) = partition_store.get_cached_table_metadata(table_ref) else {
                    continue;
                };
                let mut items = Vec::new();
                for partition in &metadata.partitions {
                    if !partition.is_assigned_to(executor_id) {
                        continue;
                    }
                    match partition_value_to_bytes(
                        partition.partition_value.clone(),
                        table_ref,
                        self.datafusion.as_ref(),
                    )
                    .await
                    {
                        Ok(bytes) => items.push(bytes.to_vec()),
                        Err(e) => {
                            // The readiness gate above should make this path
                            // unreachable for the dataset-not-ready case.
                            // Anything that lands here is a real bug (corrupt
                            // expression, etc.) — fail loud rather than silently
                            // dropping the partition.
                            tracing::error!(
                                "Failed to serialize partition expression for table {table_ref}: {e}"
                            );
                            return Err(Status::internal(format!(
                                "Failed to serialize partition expression for table {table_ref}: {e}"
                            )));
                        }
                    }
                }
                if !items.is_empty() {
                    table_partitions.insert(table_ref.to_string(), BytesArray { items });
                }
            }
        }

        // Register the allocated partitions in the executor registry so the scheduler knows where they are
        {
            let task_ctx = self.datafusion.ctx.task_ctx();
            let mut partition_map: TablePartitions = table_partitions
                .iter()
                .map(|(tbl, sa)| {
                    let exprs = sa
                        .items
                        .iter()
                        .filter_map(|bytes| match Expr::from_bytes_with_ctx(bytes, &task_ctx) {
                            Ok(expr) => Some(expr),
                            Err(e) => {
                                tracing::error!("Failed to deserialize expr: {e}");
                                None
                            }
                        })
                        .collect();
                    (TableReference::parse_str(tbl), exprs)
                })
                .collect();

            // Register Cayenne tables as single unpartitioned entries (empty filter expressions).
            // This tells the scheduler that queries targeting these tables should be forwarded to this executor.
            #[cfg(not(windows))]
            for table_ref in discover_cayenne_tables(&self.datafusion).await {
                partition_map.entry(table_ref).or_default();
            }

            self.executor_registry
                .set_executor_partitions(executor_id.to_string(), partition_map)
                .await;
        }

        Ok(Response::new(AllocateInitialPartitionsResponse {
            table_partitions,
        }))
    }

    async fn get_ddl_catchup(
        &self,
        request: Request<GetDdlCatchupRequest>,
    ) -> Result<Response<GetDdlCatchupResponse>, Status> {
        let request = request.into_inner();
        tracing::debug!(
            "ClusterService::get_ddl_catchup for executor {}, since_version={}",
            request.executor_id,
            request.since_version
        );

        let ddl_statements = self
            .executor_registry
            .ddl_statements_since(request.since_version)
            .await;

        Ok(Response::new(GetDdlCatchupResponse { ddl_statements }))
    }
}

fn create_executor_flight_client(
    endpoint: &str,
    client_tls_config: Option<ClientTlsConfig>,
) -> Result<FlightSqlClient, tonic::transport::Error> {
    let executor_address = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };

    let mut flight_channel =
        configure_endpoint_for_high_throughput(Endpoint::from_shared(executor_address)?);
    if let Some(tls_config) = client_tls_config {
        flight_channel = flight_channel.tls_config(tls_config)?;
    }

    Ok(FlightSqlServiceClient::new_from_inner(
        FlightServiceClient::new(CookieService::new(
            flight_channel.connect_lazy(),
            Arc::new(CookieStore::new()),
        ))
        .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
    ))
}

/// Handles an executor control message (heartbeat, shutdown, etc.)
async fn handle_executor_message(
    executor_id: &str,
    message: &ExecutorMessage,
    datafusion: &DataFusion,
) {
    match message {
        ExecutorMessage::Heartbeat(heartbeat) => {
            tracing::trace!(
                "Received heartbeat from executor {executor_id}: timestamp_ms={}",
                heartbeat.timestamp_ms
            );
        }
        ExecutorMessage::Metrics(_) => {
            // Metrics responses are handled separately in the stream handler
            // This shouldn't be reached, but log if it is
            tracing::warn!(
                "Unexpected metrics response in handle_executor_message for {executor_id}"
            );
        }
        ExecutorMessage::Ack(_) => {
            // Acks are handled separately in the stream handler via pending_acks.
            // This shouldn't be reached, but log if it is.
            tracing::warn!("Unexpected ack in handle_executor_message for {executor_id}");
        }
        ExecutorMessage::PartitionsLoaded(loaded) => {
            handle_partitions_loaded(executor_id, loaded, datafusion).await;
        }
        ExecutorMessage::ExecutorStatistics(stats_msg) => {
            handle_executor_statistics(executor_id, stats_msg, datafusion).await;
        }
        ExecutorMessage::Shutdown(shutdown) => {
            let reason = if shutdown.reason.is_empty() {
                "executor shutdown".to_string()
            } else {
                shutdown.reason.clone()
            };
            let ballista_executor_id = if shutdown.ballista_executor_id.is_empty() {
                executor_id
            } else {
                shutdown.ballista_executor_id.as_str()
            };
            tracing::info!(
                executor_id = %executor_id,
                ballista_executor_id = %ballista_executor_id,
                reason = %reason,
                "Executor shutdown requested"
            );
            if let Err(err) =
                notify_scheduler_executor_shutdown(datafusion, ballista_executor_id, &reason).await
            {
                tracing::warn!(
                    "Failed to notify scheduler about executor shutdown for {ballista_executor_id}: {err}"
                );
            }
        }
    }
}

async fn notify_scheduler_executor_shutdown(
    datafusion: &DataFusion,
    executor_id: &str,
    reason: &str,
) -> Result<(), String> {
    let scheduler = datafusion
        .scheduler_server
        .read()
        .map_err(|_| "Failed to lock scheduler server".to_string())?
        .clone()
        .ok_or_else(|| "Scheduler server not initialized".to_string())?;

    scheduler
        .executor_stopped(Request::new(ExecutorStoppedParams {
            executor_id: executor_id.to_string(),
            reason: reason.to_string(),
        }))
        .await
        .map_err(|e| format!("Failed to notify scheduler about executor shutdown: {e}"))?;

    Ok(())
}

/// Records a per-table [`ExecutorStatistics`] report into the scheduler's
/// in-memory `ExecutorRegistry`, where it's read at query-planning time to size
/// the coordinator's per-executor federated scans. Also re-evaluates the
/// table's readiness: `evaluate_table_readiness` gates `Ready` on the first
/// stats report, so a table whose partitions loaded before its stats arrived
/// flips to `Ready` here rather than waiting for the next periodic sweep.
async fn handle_executor_statistics(
    executor_id: &str,
    msg: &runtime_proto::ExecutorStatistics,
    datafusion: &DataFusion,
) {
    let resolved = TableReference::parse_str(&msg.table_name)
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
    let table = TableReference::full(
        Arc::<str>::clone(&resolved.catalog),
        Arc::<str>::clone(&resolved.schema),
        Arc::<str>::clone(&resolved.table),
    );
    let Some(registry) = datafusion.executor_registry() else {
        return;
    };

    // A malformed or forward-incompatible payload decodes to `None`. Record an
    // explicit unknown-statistics entry rather than dropping the report: since
    // `evaluate_table_readiness` gates `Ready` on `has_statistics_for`,
    // silently dropping it would leave the table stuck out of `Ready` forever
    // (and could hang /v1/ready) with no signal. Recording unknown stats still
    // lets the coordinator observe that this executor reported for the table;
    // the planner then treats its slice as unknown-cardinality — the same as a
    // deliberate unknown report from the executor.
    let (statistics, column_names) = if let Some(statistics) =
        runtime_cluster::decode_statistics(&msg.statistics)
    {
        (statistics, msg.column_names.clone())
    } else {
        tracing::warn!(
            table = %table,
            executor = %executor_id,
            "Failed to decode executor statistics report ({} bytes); recording unknown statistics so the table can still become Ready",
            msg.statistics.len()
        );
        (
            datafusion::common::Statistics::new_unknown(&arrow::datatypes::Schema::empty()),
            Vec::new(),
        )
    };

    registry.record_executor_statistics(&table, executor_id.to_string(), statistics, column_names);
    evaluate_table_readiness(datafusion, &table).await;
}

/// Records a `PartitionsLoaded` ack from an executor and, if all assigned
/// partitions for the table are now covered by an executor ack, flips the
/// dataset's status to `Ready`. This is the only path that marks an
/// accelerated dataset ready on the scheduler — the dataset starts
/// `Refreshing` at registration time and stays there until the cluster
/// actually has data.
async fn handle_partitions_loaded(
    executor_id: &str,
    loaded: &runtime_proto::PartitionsLoaded,
    datafusion: &DataFusion,
) {
    let Some(tracker) = datafusion.partition_load_tracker.as_ref() else {
        return;
    };
    let Some(partition_store) = datafusion
        .partition_service
        .as_ref()
        .map(|s| Arc::clone(&s.partition_store))
    else {
        // No partition store available — nothing to evaluate against.
        return;
    };

    // Canonicalize the executor-sent table name. Executors can legitimately
    // emit different textual forms across paths (bare `foo`, partial
    // `public.foo`, full `spice.public.foo`); resolving against Spice defaults
    // produces a single key so a `replace(...)` on one form doesn't shadow an
    // ack on another, and metadata lookup hits the same entry the scheduler
    // populated.
    let resolved = TableReference::parse_str(&loaded.table_name)
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
    let table = TableReference::full(
        Arc::<str>::clone(&resolved.catalog),
        Arc::<str>::clone(&resolved.schema),
        Arc::<str>::clone(&resolved.table),
    );

    let partition_expr_bytes: std::collections::HashSet<bytes::Bytes> = loaded
        .partition_expr_bytes
        .as_ref()
        .map(|arr| {
            arr.items
                .iter()
                .map(|v| bytes::Bytes::copy_from_slice(v))
                .collect()
        })
        .unwrap_or_default();

    tracing::debug!(
        executor_id,
        table = %loaded.table_name,
        partitions = partition_expr_bytes.len(),
        "Received PartitionsLoaded ack"
    );

    // Statistics now flow via the dedicated ExecutorStatistics message
    // (handle_executor_statistics); PartitionsLoaded is readiness-only.
    tracker
        .replace(table.clone(), executor_id.to_string(), partition_expr_bytes)
        .await;

    // Refresh in-memory partition metadata snapshot before evaluating
    // readiness — the tracker compares against the latest assigned set.
    // Fail closed: if we can't refresh we may be looking at a stale
    // assignment, which could flip a dataset to `Ready` based on outdated
    // executor membership. Skip this evaluation and let the next ack or
    // readiness sweep retry.
    if let Err(err) = partition_store.refresh().await {
        tracing::warn!(
            table = %loaded.table_name,
            "Skipping readiness evaluation: partition store refresh failed: {err}"
        );
        return;
    }

    evaluate_table_readiness(datafusion, &table).await;
}

/// Evaluates whether every assigned partition for `table` is covered by an
/// executor ack and, if so, flips the dataset's status to `Ready`. Reads the
/// partition store's *cached* metadata — callers must refresh the store
/// first. No-op when the dataset is already `Ready`, when partition metadata
/// hasn't been seeded yet, or when ack coverage is incomplete.
///
/// `table` must be the canonical (fully resolved) reference — the same form
/// `handle_partitions_loaded` uses as the tracker key.
pub(crate) async fn evaluate_table_readiness(datafusion: &DataFusion, table: &TableReference) {
    let Some(tracker) = datafusion.partition_load_tracker.as_ref() else {
        return;
    };
    let Some(partition_store) = datafusion
        .partition_service
        .as_ref()
        .map(|s| Arc::clone(&s.partition_store))
    else {
        return;
    };

    let resolved = table
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

    // Find the dataset keys registered at init time. The original key may be
    // bare/partial (`foo`) while the canonical form is full
    // (`spice.public.foo`); calling `update_dataset(&canonical)` would create
    // a *new* status entry and leave the original stuck in `Refreshing`,
    // keeping `/v1/ready` at 503. Match by resolve-equality to update the
    // existing entries. Collect *all* matches: an ack that arrived before
    // dataset registration can have created a canonical-key entry alongside
    // the registered bare key, and every resolve-equal entry must reach
    // `Ready` for `/v1/ready` to flip.
    let matching: Vec<(TableReference, crate::status::ComponentStatus)> = datafusion
        .runtime_status
        .get_dataset_statuses()
        .into_iter()
        .filter(|(key, _)| {
            key.clone()
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                == resolved
        })
        .collect();
    let pending: Vec<TableReference> = matching
        .iter()
        .filter(|(_, status)| !matches!(status, crate::status::ComponentStatus::Ready))
        .map(|(key, _)| key.clone())
        .collect();

    // Every matching entry is already `Ready` — skip so re-acks and periodic
    // readiness sweeps don't re-emit the status-change log/metric every time.
    // (No matching entry at all still proceeds: the ack may have arrived
    // before the dataset registered its status, in which case the canonical
    // entry is created below.)
    if pending.is_empty() && !matching.is_empty() {
        return;
    }

    let Some(metadata) = partition_store.get_cached_table_metadata(table) else {
        // No partition metadata yet — the ack raced the first discovery
        // cycle; the readiness sweep after the next reconcile re-evaluates.
        return;
    };

    if tracker.is_table_loaded(table, &metadata, datafusion).await {
        // Gate readiness on having received at least one executor statistics
        // report for this table. Distributed query plans need the reported
        // row-count statistics to size joins (so DataFusion's cost-based swap
        // builds the small side of a hash join rather than the large one); a
        // table marked `Ready` before stats arrive can plan a query that
        // exhausts the memory pool. The executor always reports per served
        // table (unknown stats when unavailable — see
        // `local_executor_table_statistics`), so a loaded table can't hang
        // here. Only gates distributed mode: single-node has no registry.
        if let Some(registry) = datafusion.executor_registry()
            && !registry.has_statistics_for(table)
        {
            tracing::debug!(
                table = %table,
                "All assigned partitions loaded but awaiting first executor statistics report before marking Ready"
            );
            return;
        }
        tracing::info!(
            table = %table,
            "All assigned partitions loaded; marking dataset Ready"
        );
        if pending.is_empty() {
            datafusion
                .runtime_status
                .update_dataset(table, crate::status::ComponentStatus::Ready);
        } else {
            for key in pending {
                datafusion
                    .runtime_status
                    .update_dataset(&key, crate::status::ComponentStatus::Ready);
            }
        }
    }
}

/// Re-evaluates readiness for every table with at least one recorded executor
/// ack. Called from the partition-assignment task after metadata seeding and
/// after each reconcile cycle: an ack that arrives *before* the table's
/// partition metadata is seeded (e.g. replayed by an executor that connected
/// while the scheduler was still starting up) is recorded in the tracker but
/// can't flip the dataset to `Ready` at arrival time — this sweep picks it up
/// once metadata exists.
pub(crate) async fn evaluate_acked_tables_readiness(datafusion: &DataFusion) {
    let Some(tracker) = datafusion.partition_load_tracker.as_ref() else {
        return;
    };
    let Some(partition_store) = datafusion
        .partition_service
        .as_ref()
        .map(|s| Arc::clone(&s.partition_store))
    else {
        return;
    };

    let acked = tracker.acked_tables().await;
    if acked.is_empty() {
        return;
    }
    if let Err(err) = partition_store.refresh().await {
        tracing::warn!("Skipping readiness sweep: partition store refresh failed: {err}");
        return;
    }
    for table in acked {
        evaluate_table_readiness(datafusion, &table).await;
    }
}

/// Discovers all Cayenne table references registered in the `DataFusion` catalog.
///
/// Iterates through all catalogs, identifies Cayenne-backed catalogs, and returns
/// fully qualified [`TableReference`]s for each table found. These are used to
/// register unpartitioned entries in the executor's partition map so that queries
/// for Cayenne tables are forwarded to the executor.
#[cfg(not(windows))]
pub(crate) async fn discover_cayenne_tables(datafusion: &DataFusion) -> Vec<TableReference> {
    use crate::datafusion::cayenne_ddl::is_cayenne_catalog;
    use cayenne::CayenneSchemaProvider;

    let mut tables = Vec::new();
    let mut seen = HashSet::new();
    for catalog_name in datafusion.ctx.catalog_names() {
        let Some(catalog) = datafusion.ctx.catalog(&catalog_name) else {
            continue;
        };
        if !is_cayenne_catalog(catalog.as_ref()) {
            continue;
        }
        for schema_name in catalog.schema_names() {
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };

            // Prefer metadata-catalog discovery to avoid relying on in-memory schema cache.
            if let Some(cayenne_schema) = schema.downcast_ref::<CayenneSchemaProvider>() {
                let namespace_prefix = format!("{}/", cayenne_schema.namespace());
                match cayenne_schema.metadata_catalog().list_table_names().await {
                    Ok(all_table_names) => {
                        for full_name in all_table_names {
                            let Some(short_name) = full_name.strip_prefix(&namespace_prefix) else {
                                continue;
                            };
                            // Listing the metadata catalog reaches tables the
                            // catalog's include/exclude withheld, which the
                            // schema provider itself never registered.
                            if !cayenne_schema.selects_table(short_name) {
                                continue;
                            }
                            let key = (
                                catalog_name.clone(),
                                schema_name.clone(),
                                short_name.to_string(),
                            );
                            if seen.insert(key.clone()) {
                                tables.push(TableReference::full(key.0, key.1, key.2));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to list Cayenne metadata tables for {catalog_name}.{schema_name}: {e}"
                        );
                    }
                }
                continue;
            }

            for table_name in schema.table_names() {
                let key = (
                    catalog_name.clone(),
                    schema_name.clone(),
                    table_name.clone(),
                );
                if seen.insert(key.clone()) {
                    tables.push(TableReference::full(key.0, key.1, key.2));
                }
            }
        }
    }
    tables
}

/// Encodes a slice of `RecordBatch` into Arrow IPC streaming format.
///
/// Returns an empty vec if no batches are provided.
///
/// A single [`StreamWriter`] is constructed once and reused across every batch
/// in the stream (the dictionary-tracking and, where enabled, compression
/// context live on the writer). This is the cheap hot path Arrow 58.1/58.2
/// optimized for; constructing a writer (or compression codec) per batch would
/// re-emit the schema/dictionaries and rebuild the codec each time. Do not
/// move the writer construction inside the loop.
fn encode_batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, arrow::error::ArrowError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let mut buffer = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    Ok(buffer)
}

/// Collects, for each secret key the cluster may expand via [`ExpandSecret`],
/// the set of store names it was referenced through.
///
/// Keys are taken from every `${ store:key }` reference in the serialized app
/// definition (datasets, catalogs, models, tools, runtime auth, snapshots, …);
/// a key the spicepod never references is absent from the map and therefore
/// denied. The per-key store set lets the `expand_secret` handler honor the
/// store a reference named — e.g. `${ env:KEY }` may only expand from
/// `env` — instead of an unscoped search across every configured store, which
/// could return an unrelated, same-named secret from a different store than
/// the spicepod referenced. A key reached via the `${ secrets:KEY }` sentinel
/// keeps its normal "any configured store" semantics: its store set contains
/// [`SECRETS`].
///
/// Serialization failure fails closed (empty map) so a broken app cannot
/// open the expansion surface.
fn expandable_secret_keys(app: &App) -> HashMap<String, HashSet<String>> {
    match serde_json::to_string(app) {
        Ok(json) => {
            let mut allowed: HashMap<String, HashSet<String>> = HashMap::new();
            for reference in iter_secret_references(&json) {
                allowed
                    .entry(reference.key)
                    .or_default()
                    .insert(reference.store);
            }
            allowed
        }
        Err(e) => {
            tracing::error!(
                "Failed to serialize app while building ExpandSecret allowlist: {e}. Denying all secret expansion."
            );
            HashMap::new()
        }
    }
}

/// Maps a task-history query execution error to a gRPC status.
///
/// Read-only validator failures (DDL/DML/COPY/etc.) become
/// [`Status::permission_denied`] so callers can distinguish policy rejections
/// from unexpected internal failures.
fn map_task_history_query_error(e: &crate::datafusion::query::Error) -> Status {
    // Classify on the underlying DataFusion message, not `Error`'s Display —
    // `UnableToExecuteQuery` already prefixes with "Failed to execute query: ",
    // and re-wrapping that string would double the prefix and couple the
    // mutation classifier to wrapper formatting.
    let underlying = match e {
        crate::datafusion::query::Error::UnableToExecuteQuery { source }
        | crate::datafusion::query::Error::UnableToCreateMemoryStream { source }
        | crate::datafusion::query::Error::UnableToCollectResults { source }
        | crate::datafusion::query::Error::BindingParameters { source } => source.to_string(),
        other => other.to_string(),
    };

    // Prefer PermissionDenied for any mutation rejection so cluster peers get a
    // clear policy signal rather than a 500-class Internal. The read-only
    // validator is the primary gate; the general operations validator can also
    // reject writes first (e.g. internal datasets) with a different message.
    if is_task_history_mutation_rejection(&underlying) {
        Status::permission_denied(format!(
            "Task history queries are read-only and cannot mutate data: {underlying}"
        ))
    } else {
        // `Error`'s Display already formats `UnableToExecuteQuery` as
        // "Failed to execute query: …"; use it as-is to avoid a second prefix.
        Status::internal(e.to_string())
    }
}

fn is_task_history_mutation_rejection(message: &str) -> bool {
    message.contains("read-only SQL context")
        || message.contains("INSERT operations are not allowed")
        || message.contains("DELETE operations are not allowed")
        || message.contains("UPDATE operations are not allowed")
        || message.contains("COPY operations are not allowed")
        || message.contains("DDL operation")
        || message.contains("are not allowed in read-only")
}

/// Rewrites a task history SQL query to use `local_task_history` instead of `task_history`.
///
/// This function parses the SQL, validates it references the expected table, and rewrites
/// all table references from `runtime.task_history` to `runtime.local_task_history`.
///
/// # Errors
///
/// Returns an error if:
/// - The SQL cannot be parsed
/// - The query contains multiple statements
/// - The query doesn't reference the `runtime.task_history` table
fn rewrite_task_history_sql(sql: &str) -> Result<String, String> {
    let dialect = PostgreSqlDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("Failed to parse SQL: {e}"))?;

    if statements.len() != 1 {
        return Err(format!(
            "Expected single SQL statement, got {}",
            statements.len()
        ));
    }

    let statement = &mut statements[0];

    // Track whether we found and rewrote the task_history table
    let mut found_task_history = false;

    // Visit all table references and rewrite task_history -> local_task_history
    let _ = visit_relations_mut(statement, |table_name| {
        // Check if this is runtime.task_history (2 parts) or just task_history (1 part)
        let parts: Vec<&str> = table_name
            .0
            .iter()
            .filter_map(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
                ObjectNamePart::Function(_) => None,
            })
            .collect();

        let is_task_history_table = match parts.as_slice() {
            [schema, table] => {
                *schema == SPICE_RUNTIME_SCHEMA && *table == DEFAULT_TASK_HISTORY_TABLE
            }
            [table] => *table == DEFAULT_TASK_HISTORY_TABLE,
            _ => false,
        };

        if is_task_history_table {
            found_task_history = true;

            // Rewrite the table name: find and replace the task_history identifier
            for part in &mut table_name.0 {
                if let ObjectNamePart::Identifier(ident) = part
                    && ident.value == DEFAULT_TASK_HISTORY_TABLE
                {
                    *ident = Ident::new(LOCAL_TASK_HISTORY_TABLE);
                }
            }
        }

        ControlFlow::<()>::Continue(())
    });

    if !found_task_history {
        return Err(format!(
            "Query must reference the \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\" table"
        ));
    }

    Ok(statement.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::datasource::MemTable;
    use runtime_proto::{
        cluster_service_client::ClusterServiceClient, cluster_service_server::ClusterServiceServer,
    };
    use runtime_secrets::{AnyErrorResult, SecretStore};
    use secrecy::SecretString;
    use spicepod::component::dataset::Dataset;
    use spicepod::param::Params;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Channel, Server};

    /// Fixed-map secret store for `ExpandSecret` allowlist tests.
    struct FakeSecretStore(HashMap<String, String>);

    #[async_trait]
    impl SecretStore for FakeSecretStore {
        async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
            Ok(self.0.get(key).map(|v| SecretString::from(v.clone())))
        }
    }

    fn secrets_with(entries: &[(&str, &str)]) -> Secrets {
        let mut secrets = Secrets::new();
        secrets.register_store(
            "fake",
            Arc::new(FakeSecretStore(
                entries
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )),
        );
        secrets
    }

    fn app_with_secret_ref(param: &str, reference: &str) -> Arc<App> {
        let mut ds = Dataset::new("memory:data", "orders");
        let map: HashMap<String, String> =
            HashMap::from([(param.to_string(), reference.to_string())]);
        ds.params = Some(Params::from_string_map(map));
        Arc::new(AppBuilder::new("test").with_dataset(ds).build())
    }

    async fn make_test_service_with(
        app: Option<Arc<App>>,
        secrets: Secrets,
        allow_secret_expansion: bool,
    ) -> ClusterServiceImpl {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = Arc::new(
            DataFusion::builder(
                crate::status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                tokio::runtime::Handle::current(),
            )
            .build(),
        );
        let task_history_schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            false,
        )]));
        let task_history_table = Arc::new(
            MemTable::try_new(Arc::clone(&task_history_schema), vec![vec![]])
                .expect("empty task history table should be created"),
        );
        datafusion
            .ctx
            .register_table(
                TableReference::partial(SPICE_RUNTIME_SCHEMA, LOCAL_TASK_HISTORY_TABLE),
                task_history_table,
            )
            .expect("local task history table should be registered");

        // Writable sink used by read-only mutation tests: INSERT INTO sink
        // SELECT FROM task_history must still be denied by the read-only gate
        // even though the target table is individually writable.
        let sink = Arc::new(
            MemTable::try_new(Arc::clone(&task_history_schema), vec![vec![]])
                .expect("empty sink table should be created"),
        );
        let sink_ref = TableReference::bare("task_history_sink");
        datafusion
            .ctx
            .register_table(sink_ref.clone(), sink)
            .expect("sink table should be registered");
        datafusion
            .mark_dataset_writable(&sink_ref)
            .expect("sink table should be marked writable");

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let cluster_state = Arc::new(runtime_cluster::ClusterStateStore::new(store, ""));
        cluster_state
            .bootstrap()
            .await
            .expect("cluster state should bootstrap");
        let executor_registry = Arc::new(ExecutorRegistry::new(
            Arc::new(runtime_cluster::PartitionStore::accelerations(Arc::clone(
                &cluster_state,
            ))),
            Arc::new(runtime_cluster::PartitionStore::catalog(Arc::clone(
                &cluster_state,
            ))),
        ));

        ClusterServiceImpl::new(
            Arc::new(TokioRwLock::new(app)),
            Arc::new(TokioRwLock::new(secrets)),
            "127.0.0.1:0".to_string(),
            Arc::new(TokioRwLock::new(HashMap::new())),
            datafusion,
            executor_registry,
            None,
            allow_secret_expansion,
        )
    }

    async fn make_test_service() -> ClusterServiceImpl {
        make_test_service_with(None, Secrets::default(), true).await
    }

    async fn make_test_client() -> (ClusterServiceClient<Channel>, CancellationToken) {
        let service = make_test_service().await;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test cluster service listener should bind");
        let address = listener
            .local_addr()
            .expect("test cluster service listener should have a local address");
        let shutdown = CancellationToken::new();
        let shutdown_signal = shutdown.clone();

        tokio::spawn(async move {
            Server::builder()
                .add_service(ClusterServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    shutdown_signal.cancelled().await;
                })
                .await
                .expect("test cluster service server should run");
        });

        let client = ClusterServiceClient::connect(format!("http://{address}"))
            .await
            .expect("test cluster service client should connect");

        (client, shutdown)
    }

    #[test]
    fn expandable_secret_keys_collects_dataset_refs() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let keys = expandable_secret_keys(&app);
        let stores = keys.get("PG_PASS");
        assert_eq!(
            stores.map(|s| s.contains(SECRETS)),
            Some(true),
            "expected PG_PASS allowlisted via the `secrets` sentinel, got {keys:?}"
        );
        assert!(
            !keys.contains_key("AWS_SECRET_ACCESS_KEY"),
            "unreferenced keys must not be allowlisted"
        );
    }

    #[test]
    fn expandable_secret_keys_empty_when_app_has_no_refs() {
        let app = AppBuilder::new("empty").build();
        let keys = expandable_secret_keys(&app);
        assert!(keys.is_empty(), "expected empty allowlist, got {keys:?}");
    }

    #[tokio::test]
    async fn expand_secret_allows_app_referenced_key() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let secrets = secrets_with(&[
            ("PG_PASS", "correct-horse"),
            ("UNRELATED_ENV_SECRET", "should-not-leak"),
        ]);
        let service = make_test_service_with(Some(app), secrets, true).await;

        let response = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "PG_PASS".to_string(),
            }))
            .await
            .expect("referenced secret should expand");

        let body = response.into_inner();
        assert_eq!(body.key, "PG_PASS");
        assert_eq!(body.value, "correct-horse");
    }

    #[tokio::test]
    async fn expand_secret_denies_unreferenced_key_even_if_present_in_store() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let secrets = secrets_with(&[
            ("PG_PASS", "correct-horse"),
            ("AWS_SECRET_ACCESS_KEY", "should-not-leak"),
        ]);
        let service = make_test_service_with(Some(app), secrets, true).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "AWS_SECRET_ACCESS_KEY".to_string(),
            }))
            .await
            .expect_err("unreferenced secret must be denied");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("Unable to expand secret"),
            "unexpected message: {}",
            err.message()
        );
        // Must not leak the secret value in the error.
        assert!(!err.message().contains("should-not-leak"));
    }

    #[tokio::test]
    async fn expand_secret_store_scoped_reference_ignores_other_stores() {
        // The spicepod references `${ env:API_KEY }` — a store-scoped
        // reference, not the `secrets:` sentinel. A higher-precedence
        // `vault` store happens to define an unrelated secret under the
        // same key name; ExpandSecret must resolve from `env` (the store
        // the reference named) and must never return `vault`'s value in
        // its place.
        let app = app_with_secret_ref("api_key", "${ env:API_KEY }");
        let mut secrets = Secrets::new();
        secrets.register_store(
            "vault",
            Arc::new(FakeSecretStore(HashMap::from([(
                "API_KEY".to_string(),
                "wrong-store-value".to_string(),
            )]))),
        );
        secrets.register_store(
            "env",
            Arc::new(FakeSecretStore(HashMap::from([(
                "API_KEY".to_string(),
                "correct-store-value".to_string(),
            )]))),
        );
        let service = make_test_service_with(Some(app), secrets, true).await;

        let response = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "API_KEY".to_string(),
            }))
            .await
            .expect("store-scoped key referenced by the app should expand");

        assert_eq!(response.into_inner().value, "correct-store-value");
    }

    #[tokio::test]
    async fn expand_secret_denies_when_mtls_disabled() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let secrets = secrets_with(&[("PG_PASS", "correct-horse")]);
        let service = make_test_service_with(Some(app), secrets, false).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "PG_PASS".to_string(),
            }))
            .await
            .expect_err("ExpandSecret without mTLS must be denied");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("requires cluster mTLS"));
    }

    #[tokio::test]
    async fn expand_secret_denies_when_app_missing() {
        let secrets = secrets_with(&[("PG_PASS", "correct-horse")]);
        let service = make_test_service_with(None, secrets, true).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "PG_PASS".to_string(),
            }))
            .await
            .expect_err("ExpandSecret without app must be denied");

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn expand_secret_denies_empty_key() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let secrets = secrets_with(&[("PG_PASS", "correct-horse")]);
        let service = make_test_service_with(Some(app), secrets, true).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: String::new(),
            }))
            .await
            .expect_err("empty key must be denied");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn expand_secret_referenced_but_missing_from_store_is_invalid_argument() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        // Allowlisted key is referenced by the app but not present in any store.
        let secrets = secrets_with(&[]);
        let service = make_test_service_with(Some(app), secrets, true).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "PG_PASS".to_string(),
            }))
            .await
            .expect_err("missing allowlisted secret must fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Unable to expand secret PG_PASS"));
    }

    #[tokio::test]
    async fn test_internal_get_metrics_transport_allows_repeated_requests() {
        let (mut client, shutdown) = make_test_client().await;

        // Internal cluster RPCs are intentionally not rate-limited; the Prometheus HTTP
        // metrics endpoint applies the external scrape limit.
        client
            .get_metrics(Request::new(GetMetricsRequest {}))
            .await
            .expect("first metrics request should succeed");

        client
            .get_metrics(Request::new(GetMetricsRequest {}))
            .await
            .expect("second metrics request should also succeed");

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_internal_get_task_history_transport_allows_repeated_requests() {
        let (mut client, shutdown) = make_test_client().await;
        let request = || {
            Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "SELECT trace_id FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            })
        };

        client
            .get_task_history(request())
            .await
            .expect("first task history request should succeed");

        client
            .get_task_history(request())
            .await
            .expect("second task history request should also succeed");

        shutdown.cancel();
    }

    #[tokio::test]
    async fn get_task_history_allows_select() {
        let service = make_test_service().await;
        let response = service
            .get_task_history(Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "SELECT trace_id FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            }))
            .await
            .expect("SELECT against task_history must succeed under read-only");
        // Empty MemTable still produces a valid response (possibly empty IPC).
        let _ = response.into_inner().arrow_ipc;
    }

    #[tokio::test]
    async fn get_task_history_rejects_insert_into_writable_from_task_history() {
        let service = make_test_service().await;
        // Target is a writable non-system table so the operations validator
        // would allow the INSERT; the read-only gate must still reject it.
        let err = service
            .get_task_history(Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "INSERT INTO task_history_sink \
                     SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            }))
            .await
            .expect_err("INSERT into writable sink must be rejected by read-only GetTaskHistory");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("read-only"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn get_task_history_rejects_delete() {
        let service = make_test_service().await;
        let err = service
            .get_task_history(Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "DELETE FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            }))
            .await
            .expect_err("DELETE must be rejected by read-only GetTaskHistory");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("read-only") || err.message().contains("not allowed"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn get_task_history_rejects_ddl_over_task_history_select() {
        let service = make_test_service().await;
        // CREATE VIEW ... AS SELECT FROM task_history passes the rewrite gate
        // (references task_history) but is DDL and must be rejected.
        let err = service
            .get_task_history(Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "CREATE VIEW \"{SPICE_RUNTIME_SCHEMA}\".\"leaked\" AS \
                     SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            }))
            .await
            .expect_err("DDL must be rejected by read-only GetTaskHistory");

        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("read-only") || err.message().contains("DDL"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[test]
    fn map_task_history_query_error_classifies_read_only() {
        let e = crate::datafusion::query::Error::UnableToExecuteQuery {
            source: datafusion::error::DataFusionError::Plan(
                "INSERT operations are not allowed in read-only SQL context.".to_string(),
            ),
        };
        let status = map_task_history_query_error(&e);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().contains("read-only"));
        // Underlying Plan text only — no Error Display wrapper re-prefixed.
        assert!(
            !status
                .message()
                .contains("Failed to execute query: Failed to execute query:"),
            "must not double-prefix Display: {}",
            status.message()
        );
    }

    #[test]
    fn map_task_history_query_error_keeps_other_failures_internal() {
        let e = crate::datafusion::query::Error::UnableToExecuteQuery {
            source: datafusion::error::DataFusionError::Internal(
                "something unexpected".to_string(),
            ),
        };
        let status = map_task_history_query_error(&e);
        assert_eq!(status.code(), tonic::Code::Internal);
        // Error Display already includes "Failed to execute query: " once.
        assert!(
            status.message().starts_with("Failed to execute query:"),
            "unexpected message: {}",
            status.message()
        );
        assert!(
            !status
                .message()
                .contains("Failed to execute query: Failed to execute query:"),
            "must not double-prefix Display: {}",
            status.message()
        );
        assert!(
            status.message().contains("something unexpected"),
            "unexpected message: {}",
            status.message()
        );
    }

    #[test]
    fn test_rewrite_task_history_sql_simple() {
        let sql = r#"SELECT * FROM "runtime"."task_history""#;
        let result = rewrite_task_history_sql(sql).expect("should rewrite");
        assert!(
            result.contains("local_task_history"),
            "Expected local_task_history in: {result}"
        );
        assert!(
            !result.contains(r#""task_history""#),
            "Should not contain task_history: {result}"
        );
    }

    #[test]
    fn test_rewrite_task_history_sql_with_where() {
        let sql = r#"SELECT * FROM "runtime"."task_history" WHERE task = 'sql'"#;
        let result = rewrite_task_history_sql(sql).expect("should rewrite");
        assert!(
            result.contains("local_task_history"),
            "Expected local_task_history in: {result}"
        );
        assert!(
            result.contains("task = 'sql'"),
            "Should preserve WHERE clause: {result}"
        );
    }

    #[test]
    fn test_rewrite_task_history_sql_with_limit() {
        let sql = r#"SELECT * FROM "runtime"."task_history" LIMIT 100"#;
        let result = rewrite_task_history_sql(sql).expect("should rewrite");
        assert!(
            result.contains("local_task_history"),
            "Expected local_task_history in: {result}"
        );
        assert!(
            result.contains("LIMIT 100"),
            "Should preserve LIMIT: {result}"
        );
    }

    #[test]
    fn test_rewrite_task_history_sql_rejects_other_tables() {
        let sql = r#"SELECT * FROM "runtime"."other_table""#;
        let result = rewrite_task_history_sql(sql);
        assert!(result.is_err(), "Should reject queries to other tables");
    }

    #[test]
    fn test_rewrite_task_history_sql_rejects_multiple_statements() {
        let sql = r#"SELECT * FROM "runtime"."task_history"; DROP TABLE foo"#;
        let result = rewrite_task_history_sql(sql);
        assert!(
            result.is_err(),
            "Should reject multiple statements: {result:?}"
        );
    }

    #[test]
    fn test_rewrite_task_history_sql_with_filter_and_limit() {
        let sql =
            r#"SELECT * FROM "runtime"."task_history" WHERE status = Utf8("completed") LIMIT 50"#;
        let result = rewrite_task_history_sql(sql).expect("should rewrite");
        assert!(
            result.contains("local_task_history"),
            "Expected local_task_history in: {result}"
        );
    }

    #[test]
    fn test_encode_batches_to_ipc_empty_is_empty() {
        let encoded = encode_batches_to_ipc(&[]).expect("encoding empty slice should succeed");
        assert!(encoded.is_empty());
    }

    /// Round-trips multiple batches through a single IPC stream. This both
    /// exercises the multi-batch path and guards the writer-reuse invariant:
    /// one `StreamWriter` must emit a single, well-formed stream that a single
    /// `StreamReader` decodes back into every original batch in order.
    #[test]
    fn test_encode_batches_to_ipc_reuses_writer_across_batches() {
        use arrow::array::Int64Array;
        use arrow_ipc::reader::StreamReader;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = |vals: Vec<i64>| {
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vals))])
                .expect("batch should be valid")
        };
        let batches = vec![batch(vec![1, 2, 3]), batch(vec![4, 5]), batch(vec![6])];

        let encoded = encode_batches_to_ipc(&batches).expect("encoding should succeed");

        let reader = StreamReader::try_new(std::io::Cursor::new(encoded), None)
            .expect("stream reader should parse the single reused-writer stream");
        let decoded: Vec<RecordBatch> = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("all batches should decode");

        assert_eq!(decoded.len(), batches.len());
        let total_rows: usize = decoded.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 6);
        for (got, want) in decoded.iter().zip(batches.iter()) {
            assert_eq!(got, want);
        }
    }
}
