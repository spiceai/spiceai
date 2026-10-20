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

use std::pin::Pin;
use std::sync::Arc;

use app::App;
use arrow::array::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_ipc::writer::StreamWriter;
use data_components::flightsql::FlightSqlClient;

use datafusion::{
    common::TableReference,
    prelude::Expr,
    sql::sqlparser::{
        ast::{LimitClause, Statement},
        dialect::PostgreSqlDialect,
        parser::Parser,
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
use crate::task_history::{DEFAULT_TASK_HISTORY_TABLE, LOCAL_TASK_HISTORY_TABLE};
use runtime_cluster::{ExecutorRegistration, Registered};
use telemetry::metrics_reader::MetricsReader;

/// How long a scheduler waits for any message on an executor's control stream
/// before it treats the executor as gone. Executors send a heartbeat every 10
/// seconds.
const CONTROL_STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
    /// Registry of connected executor streams for [`PollNow`] broadcasts.
    executor_streams: ExecutorControlStreamRegistry,
}

impl ClusterServiceImpl {
    /// Where the cluster keeps its shared state, for messages about it: the
    /// scheduler state location, or this scheduler's address when there is none.
    async fn state_location(&self) -> String {
        self.app
            .read()
            .await
            .as_ref()
            .and_then(|app| app.runtime.scheduler_state())
            .map_or_else(|| self.advertise_address.clone(), |state| state.location)
    }

    /// Creates a new cluster service implementation.
    #[must_use]
    pub fn new(
        app: Arc<TokioRwLock<Option<Arc<App>>>>,
        secrets: Arc<TokioRwLock<Secrets>>,
        advertise_address: String,
        scheduler_peers: Arc<TokioRwLock<SchedulerPeers>>,
        datafusion: Arc<DataFusion>,
        executor_registry: Arc<ExecutorRegistry>,
        metrics_reader: Option<MetricsReader>,
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
            datafusion,
            executor_registry,
            metrics_reader,
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
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
            datafusion,
            executor_registry,
            metrics_reader,
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
    /// The task that reads the executor's messages. The response ends when it ends, so an
    /// executor the scheduler no longer reads (it sent nothing for
    /// `CONTROL_STREAM_IDLE_TIMEOUT` and was unregistered) sees its stream end and reconnects.
    reader: Option<tokio::task::JoinHandle<()>>,
    _outbound_tx: mpsc::Sender<SchedulerControlMessage>,
}

impl Stream for ControlStreamOutbound {
    type Item = Result<SchedulerControlMessage, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(message)) => return Poll::Ready(Some(Ok(message))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }
        match this.reader.as_mut().map(|reader| Pin::new(reader).poll(cx)) {
            Some(Poll::Pending) => Poll::Pending,
            Some(Poll::Ready(_)) | None => {
                this.reader = None;
                Poll::Ready(None)
            }
        }
    }
}

/// Dropped when the executor's connection ends, also without a shutdown message (a
/// crash). Cancelling lets the task that reads the executor's messages unregister it
/// before it ends; aborting that task would skip the unregister and keep the executor
/// registered, so its partitions would never move.
impl Drop for ControlStreamOutbound {
    fn drop(&mut self) {
        self.cancel.cancel();
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

        let Some(app) = self.app.read().await.clone() else {
            return Err(Status::internal("App context not available"));
        };
        let app_json = serde_json::to_string(&executor_app_definition(&app))
            .map_err(|e| Status::internal(format!("Failed to serialize app: {e}")))?;

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
        let allowed_keys = expandable_secret_keys(&executor_app_definition(&app));

        let Some(allowed_stores) = allowed_keys.get(request.key.as_str()) else {
            tracing::warn!(
                executor_id = %request.executor_id,
                key = %request.key,
                "Denied cluster secret expansion: key is not referenced by the app"
            );
            // Same status/message shape as a miss so callers cannot distinguish
            // "not in spicepod" from "not in any store" for unallowlisted keys.
            return Err(Status::not_found(format!(
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
            return Err(Status::not_found(format!(
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

        // Peers get this node's own rows: the federated task_history table would fan out
        // again, so the query reads local_task_history.
        let limit = peer_task_history_limit(&request.sql)
            .map_err(|e| Status::invalid_argument(format!("Invalid task history query: {e}")))?;
        let local_sql = match limit {
            Some(limit) => format!(
                "SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{LOCAL_TASK_HISTORY_TABLE}\" LIMIT {limit}"
            ),
            None => {
                format!("SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{LOCAL_TASK_HISTORY_TABLE}\"")
            }
        };

        let query_result = self
            .datafusion
            .query_builder(&local_sql)
            .read_only(true)
            .for_peer_scheduler_read()
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
        let state_location = self.state_location().await;
        let reader = tokio::spawn(async move {
            let (executor_id, process_id) = match inbound.next().await {
                Some(Ok(msg)) => {
                    let executor_id = msg.executor_id.clone();
                    if executor_id.is_empty() {
                        tracing::warn!("Executor connected with empty executor_id, closing stream");
                        return;
                    }
                    tracing::debug!("Executor control stream connected: {executor_id}");

                    // Handle the first message if it contains data.
                    if let Some(message) = msg.message {
                        handle_executor_message(
                            &executor_id,
                            &msg.process_id,
                            &message,
                            &datafusion,
                        )
                        .await;
                    }
                    (executor_id, msg.process_id)
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
            let Registered {
                handles,
                registration,
                connection,
            } = executor_registry
                .register(executor_id.clone(), &process_id, outbound_tx_for_registry)
                .await;
            match registration {
                ExecutorRegistration::Duplicate => {
                    tracing::error!(
                        "{}",
                        util::instance_id::duplicate_instance_message(
                            &executor_id,
                            &state_location
                        )
                    );
                    // Keep the stream open and ignore it, so the duplicate does not
                    // reconnect and register again.
                    while let Some(Ok(_)) = inbound.next().await {}
                    return;
                }
                ExecutorRegistration::Restarted => {
                    tracing::info!(
                        "Executor '{executor_id}' restarted, so the tasks and shuffle output of its earlier run are rescheduled"
                    );
                    if let Err(err) = notify_scheduler_executor_shutdown(
                        &datafusion,
                        &executor_id,
                        "the executor restarted",
                    )
                    .await
                    {
                        tracing::warn!(
                            "Failed to reschedule the work of executor '{executor_id}' after its restart, so queries running on its earlier run wait until it is reaped: {err}"
                        );
                    }
                    if let Some(tracker) = datafusion.partition_load_tracker.as_ref() {
                        tracker.drop_executor(&executor_id).await;
                    }
                }
                ExecutorRegistration::New | ExecutorRegistration::Reconnected => {}
            }

            // Update active executor count metric.
            let count = executor_registry.connected_executors().await.len();
            runtime_metrics::cluster::set_active_executor_count(&metrics_node_id, count as u64);

            // Register the executor stream for PollNow broadcasts.
            executor_streams.register(&executor_id, registration_tx);

            let mut reported_duplicate = false;
            loop {
                tokio::select! {
                    () = inbound_cancel.cancelled() => {
                        tracing::debug!("Executor control stream cancelled: {executor_id}");
                        break;
                    }
                    result = tokio::time::timeout(CONTROL_STREAM_IDLE_TIMEOUT, inbound.next()) => {
                        let Ok(result) = result else {
                            tracing::warn!(
                                "Executor {executor_id} sent nothing on its control stream for {CONTROL_STREAM_IDLE_TIMEOUT:?}; treating it as gone"
                            );
                            break;
                        };
                        match result {
                            // A later process registered under this id while this
                            // one is still sending: two processes share the id.
                            Some(Ok(_)) if !executor_registry.is_current(&executor_id, connection) => {
                                if !reported_duplicate {
                                    reported_duplicate = true;
                                    tracing::error!(
                                        "{}",
                                        util::instance_id::duplicate_instance_message(
                                            &executor_id,
                                            &state_location
                                        )
                                    );
                                }
                            }
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
                                        ExecutorMessage::FlightEndpoint(endpoint) => {
                                            register_executor_flight_endpoint(
                                                &executor_registry,
                                                &datafusion,
                                                &executor_id,
                                                &endpoint.executor_url,
                                                &process_id,
                                            )
                                            .await;
                                        }
                                        ExecutorMessage::Shutdown(_) => {
                                            handle_executor_message(
                                                &executor_id,
                                                &process_id,
                                                &message,
                                                &datafusion,
                                            )
                                            .await;
                                            // The executor stops serving now:
                                            // unregister it rather than wait
                                            // for its stream to close.
                                            break;
                                        }
                                        _ => {
                                            handle_executor_message(
                                                &executor_id,
                                                &process_id,
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

            // Unregister the executor when the stream ends, unless a later
            // connection under its id replaced this one.
            if !executor_registry.unregister(&executor_id, connection).await {
                tracing::debug!("Replaced executor control stream ended: {executor_id}");
                return;
            }

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
            reader: Some(reader),
            _outbound_tx: outbound_tx,
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn allocate_initial_partitions(
        &self,
        request: Request<AllocateInitialPartitionsRequest>,
    ) -> Result<Response<AllocateInitialPartitionsResponse>, Status> {
        let AllocateInitialPartitionsRequest {
            executor_url,
            executor_id,
            process_id,
        } = request.into_inner();

        // The executor's advertise address, without the scheme. An executor that
        // sends no id is identified by it.
        let executor_address = executor_url
            .split_once("://")
            .map_or(executor_url.as_str(), |(_, address)| address);
        let executor_id = if executor_id.is_empty() {
            executor_address
        } else {
            executor_id.as_str()
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

        register_executor_flight_endpoint(
            &self.executor_registry,
            &self.datafusion,
            executor_id,
            &executor_url,
            &process_id,
        )
        .await;

        // Partitions assigned while this executor was identified by its advertise
        // address follow it to its instance identity.
        if executor_id != executor_address {
            for store in [
                self.executor_registry.accelerations_partition_store(),
                self.executor_registry.federated_partition_store(),
            ] {
                match store.move_assignments(executor_address, executor_id).await {
                    Ok(0) => {}
                    Ok(moved) => tracing::info!(
                        "Moved {moved} partition assignments of executor '{executor_address}' to its instance identity '{executor_id}'"
                    ),
                    // Unavailable makes the executor retry, rather than start with
                    // none of the partitions still assigned to its address.
                    Err(err) => {
                        return Err(Status::unavailable(format!(
                            "Failed to move the partition assignments of executor '{executor_address}' to its instance identity '{executor_id}': {err}"
                        )));
                    }
                }
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

/// Creates the Flight SQL client the scheduler sends executor-bound scans
/// through, from the executor's advertise URL, and registers it for
/// `executor_id` as the endpoint of executor process `process_id`.
async fn register_executor_flight_endpoint(
    executor_registry: &ExecutorRegistry,
    datafusion: &DataFusion,
    executor_id: &str,
    executor_url: &str,
    process_id: &str,
) {
    let executor_address = executor_url
        .split_once("://")
        .map_or(executor_url, |(_, address)| address);
    let tls_config_opt = datafusion.cluster_config.client_tls_config();
    match create_executor_flight_client(executor_url, tls_config_opt) {
        Ok(client) => {
            if !executor_registry
                .insert_flight_sql_client(
                    executor_id.to_string(),
                    executor_address.to_string(),
                    process_id,
                    client,
                )
                .await
            {
                tracing::debug!(
                    "Ignoring the Flight endpoint of a replaced process of executor {executor_id}"
                );
            }
        }
        Err(e) => {
            tracing::warn!("Failed to create Flight SQL client for executor {executor_id}: {e}");
        }
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
    process_id: &str,
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
        ExecutorMessage::FlightEndpoint(_) => {
            // Handled in the stream handler once the executor is registered. A
            // stream's first message is its heartbeat, so this is not reached.
            tracing::warn!(
                "Unexpected Flight endpoint in handle_executor_message for {executor_id}"
            );
        }
        ExecutorMessage::PartitionsLoaded(loaded) => {
            handle_partitions_loaded(executor_id, process_id, loaded, datafusion).await;
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
    process_id: &str,
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
    let recorded = tracker
        .replace(
            table.clone(),
            executor_id.to_string(),
            runtime_cluster::LoadedAck {
                process_id: process_id.to_string(),
                load_sequence: loaded.load_sequence,
                partitions: partition_expr_bytes,
            },
        )
        .await;
    if !recorded {
        tracing::debug!(
            executor_id,
            table = %loaded.table_name,
            load_sequence = loaded.load_sequence,
            "Ignoring the PartitionsLoaded ack of a load older than one already reported"
        );
        return;
    }

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

    update_table_coverage(datafusion, &table).await;
    evaluate_table_readiness(datafusion, &table).await;
}

/// Recomputes, from the executors' `PartitionsLoaded` acks, which partitions
/// of `table` each executor process has loaded, which is what queries route
/// by. Reads the partition store's cached metadata: callers refresh it first.
pub(crate) async fn update_table_coverage(datafusion: &DataFusion, table: &TableReference) {
    let (Some(tracker), Some(registry), Some(partition_store)) = (
        datafusion.partition_load_tracker.as_ref(),
        datafusion.executor_registry(),
        datafusion
            .partition_service
            .as_ref()
            .map(|s| Arc::clone(&s.partition_store)),
    ) else {
        return;
    };
    let Some(metadata) = partition_store.get_cached_table_metadata(table) else {
        return;
    };
    let loaded = tracker
        .loaded_partitions(table, &metadata, datafusion)
        .await;
    registry.set_loaded_partitions(table, loaded);
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
        update_table_coverage(datafusion, &table).await;
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

/// The app definition executors receive, and whose secret references they may expand.
/// It leaves out `runtime.auth` (API keys) and `runtime.tls` (the public endpoints'
/// private key): an executor builds its endpoint auth and TLS from its own config.
fn executor_app_definition(app: &App) -> App {
    let mut app = app.clone();
    app.runtime.auth = None;
    app.runtime.tls = None;
    for spicepod in &mut app.spicepods {
        spicepod.runtime.auth = None;
        spicepod.runtime.tls = None;
    }
    app
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

/// The row limit of `sql` when it is the query a peer scheduler sends for its federated
/// `runtime.task_history` scan: `SELECT * FROM runtime.task_history`, with an optional
/// `LIMIT n`. Any other statement is an error.
fn peer_task_history_limit(sql: &str) -> Result<Option<u64>, String> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|e| format!("Failed to parse SQL: {e}"))?;
    let unsupported = || {
        format!(
            "Only SELECT * FROM {SPICE_RUNTIME_SCHEMA}.{DEFAULT_TASK_HISTORY_TABLE} with an optional LIMIT is supported"
        )
    };
    let [Statement::Query(query)] = statements.as_mut_slice() else {
        return Err(unsupported());
    };

    let limit = match query.limit_clause.take() {
        None => None,
        Some(LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by,
        }) if limit_by.is_empty() => Some(
            limit
                .to_string()
                .parse::<u64>()
                .map_err(|_| unsupported())?,
        ),
        Some(_) => return Err(unsupported()),
    };

    let query = query.to_string();
    let accepted = [
        format!("SELECT * FROM {SPICE_RUNTIME_SCHEMA}.{DEFAULT_TASK_HISTORY_TABLE}"),
        format!("SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""),
    ];
    if accepted.contains(&query) {
        Ok(limit)
    } else {
        Err(unsupported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::servers::internal_cluster_server;
    use app::AppBuilder;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use ballista_core::serde::protobuf::{
        GetJobStatusParams, scheduler_grpc_client::SchedulerGrpcClient,
    };
    use datafusion::datasource::MemTable;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use runtime_proto::{
        cluster_service_client::ClusterServiceClient, cluster_service_server::ClusterServiceServer,
    };
    use runtime_secrets::{AnyErrorResult, SecretStore};
    use secrecy::SecretString;
    use spicepod::component::dataset::Dataset;
    use spicepod::component::runtime::{ApiKey, ApiKeyAuth, Auth, Runtime};
    use spicepod::param::Params;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Channel;

    /// Fixed-map secret store for `ExpandSecret` allowlist tests.
    struct FakeSecretStore(HashMap<String, String>);

    #[async_trait]
    impl SecretStore for FakeSecretStore {
        async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
            Ok(self.0.get(key).map(|v| SecretString::from(v.clone())))
        }
    }

    /// Fixed-map secret store that records every key it is asked for.
    struct RecordingSecretStore {
        values: HashMap<String, String>,
        lookups: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SecretStore for RecordingSecretStore {
        async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
            self.lookups.lock().push(key.to_string());
            Ok(self.values.get(key).map(|v| SecretString::from(v.clone())))
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

    async fn make_test_service_with(app: Option<Arc<App>>, secrets: Secrets) -> ClusterServiceImpl {
        make_test_service_full(app, secrets, None, &[]).await
    }

    /// Builds the service under test. `trace_ids` seed the local task history
    /// table (a single `trace_id` column), and `metrics_reader` is what
    /// `GetMetrics` collects from.
    async fn make_test_service_full(
        app: Option<Arc<App>>,
        secrets: Secrets,
        metrics_reader: Option<MetricsReader>,
        trace_ids: &[&str],
    ) -> ClusterServiceImpl {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = Arc::new(
            DataFusion::builder(
                crate::status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                tokio::runtime::Handle::current(),
            )
            .max_concurrent_queries(None)
            .build(),
        );
        let task_history_schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            false,
        )]));
        let task_history_batches = if trace_ids.is_empty() {
            vec![]
        } else {
            vec![
                RecordBatch::try_new(
                    Arc::clone(&task_history_schema),
                    vec![Arc::new(StringArray::from(trace_ids.to_vec()))],
                )
                .expect("seeded task history batch should be valid"),
            ]
        };
        let task_history_table = Arc::new(
            MemTable::try_new(Arc::clone(&task_history_schema), vec![task_history_batches])
                .expect("task history table should be created"),
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
            metrics_reader,
        )
    }

    async fn make_test_service() -> ClusterServiceImpl {
        make_test_service_with(None, Secrets::default()).await
    }

    /// A service whose local task history holds `trace_ids`.
    async fn make_test_service_with_task_history(trace_ids: &[&str]) -> ClusterServiceImpl {
        make_test_service_full(None, Secrets::default(), None, trace_ids).await
    }

    /// A metrics reader holding one cumulative counter,
    /// `cluster_rpc_test_counter`, at 7, so a `GetMetrics` payload carries a
    /// known value. The provider is local rather than global, so no other test
    /// sees it, and must be held for as long as the reader is collected.
    fn metrics_reader_with_test_counter() -> (MetricsReader, SdkMeterProvider) {
        use opentelemetry::metrics::MeterProvider as _;

        let reader = MetricsReader::new_cumulative();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        provider
            .meter("cluster_service_test")
            .u64_counter("cluster_rpc_test_counter")
            .build()
            .add(7, &[]);
        (reader, provider)
    }

    /// Every integer data point of the sum `name` in an OTLP `GetMetrics`
    /// payload.
    fn otlp_sum_points(payload: &[u8], name: &str) -> Vec<i64> {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point::Value};
        use prost::Message as _;

        let request = ExportMetricsServiceRequest::decode(payload)
            .expect("GetMetrics payload should be an OTLP export request");
        request
            .resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .filter(|metric| metric.name == name)
            .flat_map(|metric| match &metric.data {
                Some(Data::Sum(sum)) => sum.data_points.iter(),
                other => panic!("{name} should be a sum, got {other:?}"),
            })
            .map(|point| match &point.value {
                Some(Value::AsInt(value)) => *value,
                other => panic!("{name} should have an integer value, got {other:?}"),
            })
            .collect()
    }

    /// Decodes a `GetTaskHistory` Arrow IPC payload and renders it as a table.
    fn render_task_history(arrow_ipc: Vec<u8>) -> String {
        let reader =
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(arrow_ipc), None)
                .expect("task history payload should be an Arrow IPC stream");
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("task history batches should decode");
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("task history batches should render")
            .to_string()
    }

    /// Serves `service` on a loopback port built like the internal cluster port and returns
    /// its address. Plain TCP stands in for the transport: the port's gate reads only
    /// whether the port runs cluster mTLS.
    async fn spawn_cluster_server(
        service: ClusterServiceImpl,
        mtls_enabled: bool,
    ) -> (std::net::SocketAddr, CancellationToken) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test cluster service listener should bind");
        let address = listener
            .local_addr()
            .expect("test cluster service listener should have a local address");
        let shutdown = CancellationToken::new();
        let shutdown_signal = shutdown.clone();

        tokio::spawn(async move {
            internal_cluster_server(mtls_enabled)
                .add_service(ClusterServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    shutdown_signal.cancelled().await;
                })
                .await
                .expect("test cluster service server should run");
        });

        (address, shutdown)
    }

    /// Serves `service` on a cluster port and returns a channel to it.
    async fn serve_on_cluster_port(
        service: ClusterServiceImpl,
        mtls_enabled: bool,
    ) -> (Channel, CancellationToken) {
        let (address, shutdown) = spawn_cluster_server(service, mtls_enabled).await;
        let channel = Channel::from_shared(format!("http://{address}"))
            .expect("test cluster service address should be a valid URI")
            .connect()
            .await
            .expect("test cluster service client should connect");

        (channel, shutdown)
    }

    /// Serves `service` on a cluster port that runs cluster mTLS.
    async fn serve_test_service(
        service: ClusterServiceImpl,
    ) -> (ClusterServiceClient<Channel>, CancellationToken) {
        let (channel, shutdown) = serve_on_cluster_port(service, true).await;
        (ClusterServiceClient::new(channel), shutdown)
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
    async fn get_app_definition_omits_runtime_auth_and_tls_key() {
        let dir = tempfile::tempdir().expect("temp dir should be created");
        std::fs::write(
            dir.path().join("spicepod.yaml"),
            "version: v1\n\
             kind: Spicepod\n\
             name: auth_app\n\
             runtime:\n  \
               auth:\n    \
                 api_key:\n      \
                   enabled: true\n      \
                   keys:\n        \
                     - ro-cluster-test-key:ro\n        \
                     - rw-cluster-test-key:rw\n  \
               tls:\n    \
                 enabled: true\n    \
                 certificate: test-certificate\n    \
                 key: tls-private-test-key\n\
             datasets:\n  \
               - from: memory:data\n    \
                 name: orders\n",
        )
        .expect("spicepod should be written");
        let app = AppBuilder::build_from_path(dir.path())
            .await
            .expect("spicepod should load");
        let service = make_test_service_with(Some(Arc::new(app)), Secrets::default()).await;

        let app_json = service
            .get_app_definition(Request::new(GetAppDefinitionRequest {
                executor_id: "executor-1".to_string(),
            }))
            .await
            .expect("GetAppDefinition should succeed")
            .into_inner()
            .app_json;

        for credential in ["cluster-test-key", "tls-private-test-key"] {
            assert!(
                !app_json.contains(credential),
                "app definition must not carry {credential}: {app_json}"
            );
        }
        let definition: App =
            serde_json::from_str(&app_json).expect("app definition should deserialize");
        assert_eq!(definition.datasets.len(), 1);
        assert_eq!(definition.datasets[0].name, "orders");
    }

    #[tokio::test]
    async fn expand_secret_allows_app_referenced_key() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let secrets = secrets_with(&[
            ("PG_PASS", "correct-horse"),
            ("UNRELATED_ENV_SECRET", "should-not-leak"),
        ]);
        let service = make_test_service_with(Some(app), secrets).await;

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
        let service = make_test_service_with(Some(app), secrets).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "AWS_SECRET_ACCESS_KEY".to_string(),
            }))
            .await
            .expect_err("unreferenced secret must be denied");

        assert_eq!(err.code(), tonic::Code::NotFound);
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
        let service = make_test_service_with(Some(app), secrets).await;

        let response = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "API_KEY".to_string(),
            }))
            .await
            .expect("store-scoped key referenced by the app should expand");

        assert_eq!(response.into_inner().value, "correct-store-value");
    }

    /// Every RPC on the internal cluster port, the Ballista scheduler's included, is
    /// refused when the port does not run cluster mTLS.
    #[tokio::test]
    async fn cluster_port_refuses_every_rpc_without_mtls() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        let secrets = secrets_with(&[("PG_PASS", "correct-horse")]);
        let service = make_test_service_full(Some(app), secrets, None, &["trace-a"]).await;
        let (channel, shutdown) = serve_on_cluster_port(service, false).await;
        let mut client = ClusterServiceClient::new(channel.clone());
        let mut scheduler = SchedulerGrpcClient::new(channel);

        let outcomes = [
            (
                "GetAppDefinition",
                client
                    .get_app_definition(GetAppDefinitionRequest::default())
                    .await
                    .map(drop),
            ),
            (
                "ExpandSecret",
                client
                    .expand_secret(ExpandSecretRequest {
                        executor_id: "executor-1".to_string(),
                        key: "PG_PASS".to_string(),
                    })
                    .await
                    .map(drop),
            ),
            (
                "GetSchedulers",
                client
                    .get_schedulers(GetSchedulersRequest::default())
                    .await
                    .map(drop),
            ),
            (
                "GetTaskHistory",
                client
                    .get_task_history(GetTaskHistoryRequest {
                        sql: format!(
                            "SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                        ),
                    })
                    .await
                    .map(drop),
            ),
            (
                "GetMetrics",
                client
                    .get_metrics(GetMetricsRequest::default())
                    .await
                    .map(drop),
            ),
            (
                "ControlStream",
                client
                    .control_stream(tokio_stream::empty::<ExecutorControlMessage>())
                    .await
                    .map(drop),
            ),
            (
                "AllocateInitialPartitions",
                client
                    .allocate_initial_partitions(AllocateInitialPartitionsRequest::default())
                    .await
                    .map(drop),
            ),
            (
                "GetDdlCatchup",
                client
                    .get_ddl_catchup(GetDdlCatchupRequest::default())
                    .await
                    .map(drop),
            ),
            (
                "SchedulerGrpc/GetJobStatus",
                scheduler
                    .get_job_status(GetJobStatusParams::default())
                    .await
                    .map(drop),
            ),
        ];

        for (rpc, outcome) in outcomes {
            let Err(status) = outcome else {
                panic!("{rpc} without cluster mTLS must be refused");
            };
            assert_eq!(
                status.code(),
                tonic::Code::PermissionDenied,
                "{rpc}: {status:?}"
            );
            assert!(
                status.message().contains("require cluster mTLS"),
                "{rpc}: {status:?}"
            );
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn expand_secret_denies_key_referenced_only_by_runtime_auth() {
        let app = AppBuilder::new("test")
            .with_runtime(Runtime {
                auth: Some(Auth {
                    api_key: Some(ApiKeyAuth {
                        enabled: true,
                        keys: vec![ApiKey::parse_str("${ secrets:API_KEY }:rw")],
                    }),
                }),
                ..Runtime::default()
            })
            .build();
        let secrets = secrets_with(&[("API_KEY", "runtime-api-key")]);
        let service = make_test_service_with(Some(Arc::new(app)), secrets).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "API_KEY".to_string(),
            }))
            .await
            .expect_err("a key only runtime.auth references must not expand");

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn expand_secret_denies_when_app_missing() {
        let secrets = secrets_with(&[("PG_PASS", "correct-horse")]);
        let service = make_test_service_with(None, secrets).await;

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
        let service = make_test_service_with(Some(app), secrets).await;

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
    async fn expand_secret_referenced_but_missing_from_store_is_not_found() {
        let app = app_with_secret_ref("pg_pass", "${ secrets:PG_PASS }");
        // Allowlisted key is referenced by the app but not present in any store.
        let secrets = secrets_with(&[]);
        let service = make_test_service_with(Some(app), secrets).await;

        let err = service
            .expand_secret(Request::new(ExpandSecretRequest {
                executor_id: "executor-1".to_string(),
                key: "PG_PASS".to_string(),
            }))
            .await
            .expect_err("missing allowlisted secret must fail");

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("Unable to expand secret PG_PASS"));
    }

    #[tokio::test]
    async fn executor_secrets_treat_keys_the_scheduler_declines_as_not_found() {
        let mut ds = Dataset::new("memory:data", "orders");
        ds.params = Some(Params::from_string_map(HashMap::from([
            ("pg_pass".to_string(), "${ secrets:PG_PASS }".to_string()),
            ("pg_user".to_string(), "${ secrets:PG_USER }".to_string()),
        ])));
        let app = Arc::new(AppBuilder::new("test").with_dataset(ds).build());
        let lookups = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut secrets = Secrets::new();
        secrets.register_store(
            "fake",
            Arc::new(RecordingSecretStore {
                values: HashMap::from([
                    ("PG_PASS".to_string(), "correct-horse".to_string()),
                    ("s3_region".to_string(), "us-west-2".to_string()),
                ]),
                lookups: Arc::clone(&lookups),
            }),
        );
        let service = make_test_service_with(Some(app), secrets).await;
        let (client, shutdown) = serve_test_service(service).await;
        let executor_secrets = Secrets::new_for_cluster_executor(
            Box::new(crate::cluster::ClusterSecretExpanderImpl::new(client)),
            "executor-1".to_string(),
        );

        // Not referenced by the app, so the scheduler declines it even though
        // its store has a value.
        assert!(matches!(
            executor_secrets.get_secret("s3_region").await,
            Ok(None)
        ));
        // Referenced by the app, but in no store.
        assert!(matches!(
            executor_secrets.get_secret("PG_USER").await,
            Ok(None)
        ));
        let value = executor_secrets
            .get_secret("PG_PASS")
            .await
            .expect("referenced secret lookup should succeed")
            .expect("referenced secret should resolve");
        assert_eq!(value.expose_secret(), "correct-horse");
        // The declined key never reaches the store, and each key is
        // requested once.
        assert_eq!(
            *lookups.lock(),
            vec!["PG_USER".to_string(), "PG_PASS".to_string()]
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn executor_secrets_report_unreachable_scheduler_as_error() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have a local address");
        drop(listener);
        let channel = Channel::from_shared(format!("http://{address}"))
            .expect("loopback URI should parse")
            .connect_lazy();
        let executor_secrets = Secrets::new_for_cluster_executor(
            Box::new(crate::cluster::ClusterSecretExpanderImpl::new(
                ClusterServiceClient::new(channel),
            )),
            "executor-1".to_string(),
        );

        let err = executor_secrets
            .get_secret("s3_region")
            .await
            .expect_err("an unreachable scheduler must be an error, not a missing key");
        assert!(
            err.to_string()
                .contains("Failed to expand secret from scheduler"),
            "unexpected error: {err}"
        );
    }

    /// Back-to-back requests a transport test issues: well past any burst a
    /// rate limiter would let through.
    const REPEATED_REQUESTS: usize = 20;

    /// Polls `condition` until it holds or five seconds pass.
    async fn eventually(mut condition: impl AsyncFnMut() -> bool) -> bool {
        for _ in 0..100 {
            if condition().await {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        false
    }

    /// An executor that announces its shutdown is unregistered at once, while
    /// its control stream is still open, so queries stop routing to it.
    #[tokio::test]
    async fn an_executor_that_shuts_down_is_unregistered_at_once() {
        use runtime_proto::{ExecutorControlMessage, ExecutorHeartbeat, ExecutorShutdown};

        let service = make_test_service().await;
        let registry = service.executor_registry();
        let (mut client, shutdown) = serve_test_service(service).await;
        let (tx, rx) = mpsc::channel::<ExecutorControlMessage>(8);
        let message = |message| ExecutorControlMessage {
            executor_id: "exec-1".to_string(),
            message: Some(message),
            process_id: "p1".to_string(),
        };
        tx.send(message(ExecutorMessage::Heartbeat(ExecutorHeartbeat {
            timestamp_ms: 0,
        })))
        .await
        .expect("send heartbeat");
        let _inbound = client
            .control_stream(ReceiverStream::new(rx))
            .await
            .expect("open the control stream");
        assert!(
            eventually(async || registry.connected_executors().await == ["exec-1"]).await,
            "the executor registers"
        );

        tx.send(message(ExecutorMessage::Shutdown(ExecutorShutdown {
            ballista_executor_id: String::new(),
            reason: "test".to_string(),
        })))
        .await
        .expect("send shutdown");
        assert!(
            eventually(async || registry.connected_executors().await.is_empty()).await,
            "the executor is unregistered while its stream is open"
        );

        drop(tx);
        shutdown.cancel();
    }

    /// An executor whose connection ends without a shutdown message, as when its process
    /// is killed, is unregistered, so its partitions can move to other executors.
    #[tokio::test]
    async fn an_executor_whose_connection_drops_is_unregistered() {
        use runtime_proto::{ExecutorControlMessage, ExecutorHeartbeat};

        let service = make_test_service().await;
        let registry = service.executor_registry();
        let (scheduler, shutdown) = spawn_cluster_server(service, true).await;
        // The executor reaches the scheduler through this relay, so the test can cut the
        // connection as a killed process does, while its stream of messages stays open.
        let relay = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the relay binds");
        let relay_address = relay.local_addr().expect("the relay has an address");
        let connection = tokio::spawn(async move {
            let (mut executor_side, _) = relay.accept().await.expect("the relay accepts");
            let mut scheduler_side = tokio::net::TcpStream::connect(scheduler)
                .await
                .expect("the relay reaches the scheduler");
            let _ = tokio::io::copy_bidirectional(&mut executor_side, &mut scheduler_side).await;
        });
        let channel = Channel::from_shared(format!("http://{relay_address}"))
            .expect("the relay address is a valid URI")
            .connect()
            .await
            .expect("the executor connects");
        let mut client = ClusterServiceClient::new(channel);
        let (tx, rx) = mpsc::channel::<ExecutorControlMessage>(8);
        tx.send(ExecutorControlMessage {
            executor_id: "exec-1".to_string(),
            message: Some(ExecutorMessage::Heartbeat(ExecutorHeartbeat {
                timestamp_ms: 0,
            })),
            process_id: "p1".to_string(),
        })
        .await
        .expect("send heartbeat");
        let _inbound = client
            .control_stream(ReceiverStream::new(rx))
            .await
            .expect("open the control stream");
        assert!(
            eventually(async || registry.connected_executors().await == ["exec-1"]).await,
            "the executor registers"
        );

        connection.abort();
        assert!(
            eventually(async || registry.connected_executors().await.is_empty()).await,
            "the executor is unregistered"
        );

        drop(tx);
        shutdown.cancel();
    }

    /// An executor that sends nothing for longer than the idle timeout, while its connection
    /// stays open (its process paused), is unregistered and its control stream ends, so it
    /// reconnects when it resumes.
    #[tokio::test(start_paused = true)]
    async fn the_control_stream_of_an_idle_executor_ends() {
        use runtime_proto::{ExecutorControlMessage, ExecutorHeartbeat};

        let service = make_test_service().await;
        let registry = service.executor_registry();
        let (mut client, shutdown) = serve_test_service(service).await;
        let (tx, rx) = mpsc::channel::<ExecutorControlMessage>(8);
        tx.send(ExecutorControlMessage {
            executor_id: "exec-1".to_string(),
            message: Some(ExecutorMessage::Heartbeat(ExecutorHeartbeat {
                timestamp_ms: 0,
            })),
            process_id: "p1".to_string(),
        })
        .await
        .expect("send heartbeat");
        let mut inbound = client
            .control_stream(ReceiverStream::new(rx))
            .await
            .expect("open the control stream")
            .into_inner();
        assert!(
            eventually(async || registry.connected_executors().await == ["exec-1"]).await,
            "the executor registers"
        );

        tokio::time::sleep(CONTROL_STREAM_IDLE_TIMEOUT).await;
        assert!(
            eventually(async || registry.connected_executors().await.is_empty()).await,
            "the idle executor is unregistered"
        );
        let end = tokio::time::timeout(std::time::Duration::from_secs(5), inbound.next()).await;
        assert!(
            matches!(end, Ok(None)),
            "the control stream ends, got {end:?}"
        );

        drop(tx);
        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_internal_get_metrics_transport_allows_repeated_requests() {
        let (metrics_reader, _provider) = metrics_reader_with_test_counter();
        let service =
            make_test_service_full(None, Secrets::default(), Some(metrics_reader), &[]).await;
        let (mut client, shutdown) = serve_test_service(service).await;

        // Internal cluster RPCs are intentionally not rate-limited; the Prometheus HTTP
        // metrics endpoint applies the external scrape limit. Every request must succeed
        // and carry the node's metrics.
        for attempt in 1..=REPEATED_REQUESTS {
            let response = client
                .get_metrics(Request::new(GetMetricsRequest {}))
                .await
                .unwrap_or_else(|status| {
                    panic!("metrics request {attempt} should succeed, got {status:?}")
                });
            assert_eq!(
                otlp_sum_points(
                    &response.into_inner().otlp_metrics,
                    "cluster_rpc_test_counter"
                ),
                vec![7],
                "metrics request {attempt}"
            );
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_internal_get_task_history_transport_allows_repeated_requests() {
        let service = make_test_service_with_task_history(&["trace-a", "trace-b"]).await;
        let (mut client, shutdown) = serve_test_service(service).await;
        let request = || {
            Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            })
        };

        for attempt in 1..=REPEATED_REQUESTS {
            let response = client
                .get_task_history(request())
                .await
                .unwrap_or_else(|status| {
                    panic!("task history request {attempt} should succeed, got {status:?}")
                });
            assert_eq!(
                render_task_history(response.into_inner().arrow_ipc),
                "+----------+\n\
                 | trace_id |\n\
                 +----------+\n\
                 | trace-a  |\n\
                 | trace-b  |\n\
                 +----------+",
                "task history request {attempt}"
            );
        }

        shutdown.cancel();
    }

    #[tokio::test]
    async fn get_task_history_reads_local_rows_with_optional_limit() {
        let service = make_test_service_with_task_history(&["trace-a", "trace-b"]).await;
        let table = format!("\"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\"");

        // The query names `task_history`; the rows can only come from the seeded
        // `local_task_history`.
        for (sql, expected) in [
            (
                format!("SELECT * FROM {table}"),
                "+----------+\n\
                 | trace_id |\n\
                 +----------+\n\
                 | trace-a  |\n\
                 | trace-b  |\n\
                 +----------+",
            ),
            (
                format!("SELECT * FROM {table} LIMIT 1"),
                "+----------+\n\
                 | trace_id |\n\
                 +----------+\n\
                 | trace-a  |\n\
                 +----------+",
            ),
        ] {
            let response = service
                .get_task_history(Request::new(GetTaskHistoryRequest { sql: sql.clone() }))
                .await
                .unwrap_or_else(|status| panic!("{sql} should succeed, got {status:?}"));
            assert_eq!(
                render_task_history(response.into_inner().arrow_ipc),
                expected,
                "{sql}"
            );
        }
    }

    /// `GetTaskHistory` serves a peer scheduler's `runtime.task_history` query,
    /// which holds an admission permit on that peer while it waits. With every
    /// permit here held by this scheduler's own such queries, the read must
    /// still run, or the two schedulers wait on each other forever.
    #[tokio::test]
    async fn get_task_history_runs_while_every_admission_permit_is_held() {
        let service = make_test_service_with_task_history(&["trace-a"]).await;
        let semaphore = service
            .datafusion
            .query_admission_semaphore()
            .expect("the scheduler gates queries");
        let permits =
            u32::try_from(semaphore.available_permits()).expect("permit count fits in u32");
        let _held = semaphore
            .acquire_many_owned(permits)
            .await
            .expect("the semaphore is open");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            service.get_task_history(Request::new(GetTaskHistoryRequest {
                sql: format!(
                    "SELECT * FROM \"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\""
                ),
            })),
        )
        .await
        .expect("GetTaskHistory must not wait for an admission permit")
        .expect("GetTaskHistory should succeed");
        assert_eq!(
            render_task_history(response.into_inner().arrow_ipc),
            "+----------+\n\
             | trace_id |\n\
             +----------+\n\
             | trace-a  |\n\
             +----------+"
        );
    }

    /// `GetTaskHistory` serves only the scan a peer scheduler sends. Any other statement,
    /// including one that reads another table next to `task_history`, is refused.
    #[tokio::test]
    async fn get_task_history_refuses_other_sql() {
        let service = make_test_service_with_task_history(&["trace-a"]).await;
        let table = format!("\"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\"");

        for sql in [
            format!("SELECT s.* FROM {table} t RIGHT JOIN task_history_sink s ON false"),
            format!("SELECT * FROM {table}, task_history_sink"),
            format!("SELECT trace_id FROM {table}"),
            format!("SELECT * FROM {table} WHERE trace_id = 'trace-a'"),
            format!("SELECT * FROM {table} ORDER BY trace_id"),
            format!("SELECT * FROM {table} LIMIT 1 OFFSET 1"),
            format!("SELECT * FROM {table} LIMIT (SELECT count(*) FROM task_history_sink)"),
            format!("SELECT * FROM {table}; SELECT * FROM {table}"),
            format!("INSERT INTO task_history_sink SELECT * FROM {table}"),
            format!("DELETE FROM {table}"),
            format!("CREATE VIEW \"{SPICE_RUNTIME_SCHEMA}\".\"leaked\" AS SELECT * FROM {table}"),
        ] {
            let Err(err) = service
                .get_task_history(Request::new(GetTaskHistoryRequest { sql: sql.clone() }))
                .await
            else {
                panic!("{sql} must be refused");
            };
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{sql}: {err:?}");
        }
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
    fn peer_task_history_limit_accepts_the_peer_scan_query() {
        for (sql, limit) in [
            ("SELECT * FROM \"runtime\".\"task_history\"", None),
            (
                "SELECT * FROM \"runtime\".\"task_history\" LIMIT 5",
                Some(5),
            ),
            ("select * from runtime.task_history limit 5", Some(5)),
        ] {
            assert_eq!(peer_task_history_limit(sql), Ok(limit), "{sql}");
        }
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
