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
    GetAppDefinitionRequest, GetAppDefinitionResponse, GetMetricsRequest, GetMetricsResponse,
    GetSchedulersRequest, GetSchedulersResponse, GetTaskHistoryRequest, GetTaskHistoryResponse,
    PollNowCommand, SchedulerControlMessage, SchedulerInstance, TaskCancelInfo,
    cluster_service_server::ClusterService, executor_control_message::Message as ExecutorMessage,
    scheduler_control_message::Message as SchedulerMessage,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::task::{Context, Poll};
use tokio::sync::RwLock as TokioRwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tonic::{
    Request, Response, Status, Streaming,
    transport::{ClientTlsConfig, Endpoint},
};

use crate::cluster::executor_registry::ExecutorRegistry;
use crate::cluster::partition::PartitionManager;
use crate::cluster::{SchedulerPeers, partition::partition_value_to_bytes};
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
    /// Manager for partition metadata (scheduler only).
    partition_manager: Option<Arc<PartitionManager>>,
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
        partition_manager: Option<Arc<PartitionManager>>,
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
            datafusion,
            executor_registry,
            metrics_reader,
            partition_manager,
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
        partition_manager: Option<Arc<PartitionManager>>,
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
            partition_manager,
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

        let app_guard = self.app.read().await;
        let Some(ref app) = *app_guard else {
            return Err(Status::internal("App context not available"));
        };

        let app_json = serde_json::to_string(app.as_ref())
            .map_err(|e| Status::internal(format!("Failed to serialize app: {e}")))?;

        Ok(Response::new(GetAppDefinitionResponse { app_json }))
    }

    async fn expand_secret(
        &self,
        request: Request<ExpandSecretRequest>,
    ) -> Result<Response<ExpandSecretResponse>, Status> {
        let request = request.into_inner();

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
        let AllocateInitialPartitionsRequest { executor_id } = request.into_inner();

        let tls_config_opt = self.datafusion.cluster_config.client_tls_config().cloned();
        match create_executor_flight_client(&executor_id, tls_config_opt) {
            Ok(client) => {
                let mut flight_client_registry =
                    self.executor_registry.flight_sql_clients.write().await;
                flight_client_registry.insert(executor_id.clone(), client);
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create Flight SQL client for executor {executor_id}: {e}"
                );
            }
        }

        let mut table_partitions: HashMap<String, BytesArray> = HashMap::new();

        if let Some(partition_manager) = &self.partition_manager {
            let app_guard = self.app.read().await;
            if let Some(app) = app_guard.as_ref() {
                // Find accelerated datasets with partitioning

                for table_ref in super::partition::accelerated_tables(app).keys() {
                    match partition_manager
                        .allocate_partitions(table_ref, &executor_id, 10)
                        .await
                    {
                        Ok(partitions) => {
                            if partitions.is_empty() {
                                continue;
                            }
                            let mut items = Vec::with_capacity(partitions.len());
                            for partition in partitions {
                                match partition_value_to_bytes(
                                    partition,
                                    table_ref,
                                    &self.datafusion,
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
                            table_partitions.insert(table_ref.to_string(), BytesArray { items });
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to allocate partitions for table {} to executor {}: {}",
                                table_ref.to_string(),
                                executor_id,
                                e
                            );
                        }
                    }
                }
            }
        }

        // Register the allocated partitions in the executor registry so the scheduler knows where they are
        {
            let mut executor_partitions = self.executor_registry.partitions.write().await;
            executor_partitions.insert(
                executor_id.clone(),
                table_partitions
                    .iter()
                    .map(|(tbl, sa)| {
                        let exprs = sa
                            .items
                            .iter()
                            .filter_map(|bytes| match Expr::from_bytes(bytes) {
                                Ok(expr) => Some(expr),
                                Err(e) => {
                                    tracing::error!("Failed to deserialize expr: {e}");
                                    None
                                }
                            })
                            .collect();
                        (TableReference::parse_str(tbl), exprs)
                    })
                    .collect(),
            );
        }

        Ok(Response::new(AllocateInitialPartitionsResponse {
            table_partitions,
        }))
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
