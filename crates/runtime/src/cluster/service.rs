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
use flight_client::cookie::{CookieService, CookieStore};
use flight_client::{MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE};
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
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use spicepod::component::runtime;
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
use crate::datafusion::{DataFusion, SPICE_RUNTIME_SCHEMA};
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
        let (ddl_statements, ddl_version) = self
            .executor_registry
            .ddl_snapshot()
            .await
            .map_err(|e| Status::internal(format!("Failed to snapshot DDL log: {e}")))?;

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

        tracing::debug!(
            "ExpandSecret: expanding secret {} for executor {}",
            request.key,
            request.executor_id
        );

        let secrets = self.secrets.read().await;
        let Some(value) = secrets
            .get_secret(&request.key)
            .await
            .map_err(|e| Status::internal(format!("Failed to get secret: {e}")))?
        else {
            tracing::error!(target: "task_history", "Secret not found");
            return Err(Status::invalid_argument(format!(
                "Unable to read secret {}",
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

        // Execute the query against local_task_history
        let query_result = self
            .datafusion
            .query_builder(&local_sql)
            .build()
            .run()
            .await
            .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

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
            let pending_requests = executor_registry
                .register(executor_id.clone(), outbound_tx_for_registry)
                .await;

            // Update active executor count metric.
            let count = executor_registry.connected_executors().await.len();
            crate::metrics::cluster::set_active_executor_count(&metrics_node_id, count as u64);

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
                                    // Handle metrics responses by completing pending requests.
                                    if let ExecutorMessage::Metrics(response) = &message {
                                        let mut pending = pending_requests.write().await;
                                        if let Some(sender) = pending.remove(&response.request_id) {
                                            let _ = sender.send(response.clone());
                                        } else {
                                            tracing::warn!(
                                                "Received metrics response for unknown request_id: {}",
                                                response.request_id
                                            );
                                        }
                                    } else {
                                        handle_executor_message(
                                            &executor_id,
                                            &message,
                                            &datafusion,
                                        )
                                        .await;
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
            crate::metrics::cluster::set_active_executor_count(&metrics_node_id, count as u64);

            // Unregister the executor stream.
            executor_streams.unregister(&executor_id);

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
        let app_guard = self.app.read().await;
        let mut total_assigned: usize = 0;
        if let Some(app) = app_guard.as_ref() {
            let max_partitions_per_executor = app.runtime.scheduler.as_ref().map_or(
                runtime::default_max_partitions_per_executor(),
                |scheduler| scheduler.max_partitions_per_executor,
            );

            // Find accelerated datasets with partitioning
            for table_ref in super::partition::accelerated_tables(app).keys() {
                if total_assigned >= max_partitions_per_executor {
                    tracing::debug!(
                        "Executor {executor_id} reached max_partitions_per_executor ({max_partitions_per_executor}) during initial allocation, skipping remaining tables"
                    );
                    break;
                }
                let remaining = max_partitions_per_executor.saturating_sub(total_assigned);

                if partition_store
                    .get_cached_table_metadata(table_ref)
                    .is_none()
                {
                    tracing::info!(
                        "No cached partition metadata for table {table_ref}. Scheduler likely has not finished discovering partitions for the table. Will not assign in initial allocation, but will get assigned on future assignments"
                    );
                    continue;
                }
                match partition_store
                    .allocate_partitions(table_ref, executor_id, remaining)
                    .await
                {
                    Ok(result) => {
                        let newly_assigned = result.newly_assigned.len();
                        let partitions = result.all_assigned();
                        if partitions.is_empty() {
                            continue;
                        }
                        let mut items = Vec::with_capacity(partitions.len());
                        for partition in &partitions {
                            match partition_value_to_bytes(
                                partition.clone(),
                                table_ref,
                                self.datafusion.as_ref(),
                            )
                            .await
                            {
                                Ok(bytes) => items.push(bytes.to_vec()),
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to serialize partition expression for table {table_ref}: {e}"
                                    );
                                }
                            }
                        }
                        total_assigned += newly_assigned;
                        table_partitions.insert(table_ref.to_string(), BytesArray { items });
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to allocate partitions for table {table_ref} to executor {executor_id}: {e}",
                        );
                    }
                }
            }
        }

        // Register the allocated partitions in the executor registry so the scheduler knows where they are
        {
            let registry = self.datafusion.ctx.as_ref();
            let mut partition_map: TablePartitions = table_partitions
                .iter()
                .map(|(tbl, sa)| {
                    let exprs = sa
                        .items
                        .iter()
                        .filter_map(
                            |bytes| match Expr::from_bytes_with_registry(bytes, registry) {
                                Ok(expr) => Some(expr),
                                Err(e) => {
                                    tracing::error!("Failed to deserialize expr: {e}");
                                    None
                                }
                            },
                        )
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
            .await
            .map_err(|e| Status::internal(format!("Failed to read DDL catch-up: {e}")))?;

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

    let mut flight_channel = Endpoint::from_shared(executor_address)?;
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
/// Discovers all Cayenne table references registered in the `DataFusion` catalog.
///
/// Iterates through all catalogs, identifies Cayenne-backed catalogs, and returns
/// fully qualified [`TableReference`]s for each table found. These are used to
/// register unpartitioned entries in the executor's partition map so that queries
/// for Cayenne tables are forwarded to the executor.
#[cfg(not(windows))]
async fn discover_cayenne_tables(datafusion: &DataFusion) -> Vec<TableReference> {
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
            if let Some(cayenne_schema) = schema.as_any().downcast_ref::<CayenneSchemaProvider>() {
                let namespace_prefix = format!("{}/", cayenne_schema.namespace());
                match cayenne_schema.metadata_catalog().list_table_names().await {
                    Ok(all_table_names) => {
                        for full_name in all_table_names {
                            let Some(short_name) = full_name.strip_prefix(&namespace_prefix) else {
                                continue;
                            };
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
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use runtime_proto::{
        cluster_service_client::ClusterServiceClient, cluster_service_server::ClusterServiceServer,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Channel, Server};

    async fn make_test_service() -> ClusterServiceImpl {
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
            Arc::new(runtime_cluster::OccDdlLog::new(Arc::clone(&cluster_state))),
        ));

        ClusterServiceImpl::new(
            Arc::new(TokioRwLock::new(None)),
            Arc::new(TokioRwLock::new(Secrets::default())),
            "127.0.0.1:0".to_string(),
            Arc::new(TokioRwLock::new(HashMap::new())),
            datafusion,
            executor_registry,
            None,
            true,
        )
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
}
