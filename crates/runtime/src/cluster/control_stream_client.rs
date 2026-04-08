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

//! Executor-side control stream client for scheduler communication.
//!
//! This module provides functionality for executors to establish and maintain
//! bidirectional control streams with schedulers. These streams allow schedulers
//! to request metrics from executors on-demand, issue task cancellations, and
//! send `PollNow` commands to trigger immediate work polling.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ballista_core::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use ballista_executor::executor::Executor;
use futures::StreamExt;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_proto::scheduler_control_message::Message as SchedulerMessage;
use runtime_proto::{
    ComponentStatusUpdate, ExecutorControlMessage, ExecutorHeartbeat, ExecutorShutdown,
    MetricsResponse, executor_control_message::Message as ExecutorMessage,
};
use tokio::sync::{Notify, RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::ClientTlsConfig;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};
use uuid::Uuid;

use crate::cluster::proto_conv::component_status_to_proto;
use crate::metrics_reader::MetricsReader;
use crate::status::{ComponentStatus, RuntimeStatus};

const CONTROL_STREAM_BACKOFF_MAX: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const STATUS_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Callback type for when an executor has received an update to its partitions (via the control stream).
///
/// The handler takes two arguments:
/// 1. `new_partitions`: A map of dataset names to a list of partition values (as byte vectors) that have been assigned.
/// 2. `removed_partitions`: A map of dataset names to a list of partition values (as byte vectors) that should be unloaded.
pub type PartitionUpdateHandler = Arc<
    dyn Fn(
            HashMap<String, Vec<Vec<u8>>>,
            HashMap<String, Vec<Vec<u8>>>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// Callback type for when an executor receives a dataset refresh command from the scheduler.
///
/// The handler takes two arguments:
/// 1. `dataset_name`: The dataset name to refresh.
/// 2. `overrides_json`: Optional JSON-serialized `RefreshOverrides`.
pub type RefreshDatasetHandler =
    Arc<dyn Fn(String, Option<String>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Handle for a single control stream connection to a scheduler.
struct ControlStreamHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    outbound_tx: Arc<RwLock<Option<mpsc::Sender<ExecutorControlMessage>>>>,
}

/// Spawns a control stream connection to a single scheduler.
///
/// The stream will:
/// 1. Connect to the scheduler
/// 2. Send periodic heartbeats
/// 3. Respond to metrics requests from the scheduler
/// 4. Receive control messages (e.g., `PollNow`) and signal the notify
/// 5. Reconnect on failure with exponential backoff
#[expect(clippy::too_many_arguments)]
fn spawn_control_stream(
    scheduler_address: String,
    executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
    metrics_reader: Option<Arc<MetricsReader>>,
    executor: Option<Arc<Executor>>,
    poll_now_notify: Arc<Notify>,
    outbound_tx_state: Arc<RwLock<Option<mpsc::Sender<ExecutorControlMessage>>>>,
    partition_update_handler: Option<&PartitionUpdateHandler>,
    refresh_dataset_handler: Option<&RefreshDatasetHandler>,
    runtime_status: Arc<RuntimeStatus>,
) -> ControlStreamHandle {
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let outbound_tx_state_for_task = Arc::clone(&outbound_tx_state);
    let partition_update_handler = partition_update_handler.cloned();
    let refresh_dataset_handler = refresh_dataset_handler.cloned();

    let task = tokio::spawn(async move {
        let tls_enabled = client_tls_config.is_some();
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_duration(Some(CONTROL_STREAM_BACKOFF_MAX))
            .build();
        // Channel for forwarding ComponentStatusAck update_ids to the status sender task.
        // Re-created each connection since the sender task is re-spawned.
        let mut component_ack_rx: Option<mpsc::Receiver<String>>;

        loop {
            if token.is_cancelled() {
                tracing::debug!("Control stream to scheduler {scheduler_address} cancelled");
                break;
            }

            // Build endpoint
            let endpoint_url = normalize_scheduler_endpoint(&scheduler_address, tls_enabled);
            let endpoint = match create_grpc_client_endpoint(
                endpoint_url.clone(),
                Some(&GrpcClientConfig::default()),
            ) {
                Ok(ep) => ep,
                Err(e) => {
                    tracing::warn!(
                        "Failed to create control stream endpoint to {endpoint_url}: {e}"
                    );
                    if let Some(delay) = backoff.next_duration() {
                        tokio::select! {
                            () = token.cancelled() => break,
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                    continue;
                }
            };

            let endpoint = if let Some(ref tls_config) = client_tls_config {
                match endpoint.tls_config(tls_config.clone()) {
                    Ok(ep) => ep,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to configure TLS for control stream to {endpoint_url}: {e}"
                        );
                        if let Some(delay) = backoff.next_duration() {
                            tokio::select! {
                                () = token.cancelled() => break,
                                () = tokio::time::sleep(delay) => {}
                            }
                        }
                        continue;
                    }
                }
            } else {
                endpoint
            };

            // Connect
            let channel = match endpoint.connect().await {
                Ok(ch) => ch,
                Err(e) => {
                    tracing::warn!("Failed to connect control stream to {endpoint_url}: {e}");
                    if let Some(delay) = backoff.next_duration() {
                        tokio::select! {
                            () = token.cancelled() => break,
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                    continue;
                }
            };

            let mut client = ClusterServiceClient::new(channel)
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX);

            // Create channels for outbound messages
            let (outbound_tx, outbound_rx) = mpsc::channel::<ExecutorControlMessage>(32);
            // Create channel for forwarding acks to the component status task
            let (ack_tx, ack_rx_new) = mpsc::channel::<String>(32);
            component_ack_rx = Some(ack_rx_new);
            {
                let mut outbound_guard = outbound_tx_state_for_task.write().await;
                *outbound_guard = Some(outbound_tx.clone());
            }

            // Spawn heartbeat sender
            let heartbeat_executor_id = executor_id.clone();
            let heartbeat_tx = outbound_tx.clone();
            let heartbeat_token = token.clone();
            let heartbeat_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
                loop {
                    tokio::select! {
                        () = heartbeat_token.cancelled() => break,
                        _ = interval.tick() => {
                            let msg = build_heartbeat_message(&heartbeat_executor_id);
                            if heartbeat_tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });

            // Spawn component status sender (ack-based, with retry)
            let component_status_task = tokio::spawn(run_component_status_sender(
                executor_id.clone(),
                outbound_tx.clone(),
                token.clone(),
                Arc::clone(&runtime_status),
                component_ack_rx.take(),
            ));

            // Send initial identification message
            let init_msg = build_heartbeat_message(&executor_id);
            if outbound_tx.send(init_msg).await.is_err() {
                heartbeat_task.abort();
                component_status_task.abort();
                let mut outbound_guard = outbound_tx_state_for_task.write().await;
                *outbound_guard = None;
                continue;
            }

            // Create the bidirectional stream
            let outbound_stream = ReceiverStream::new(outbound_rx);
            let stream_result = client.control_stream(outbound_stream).await;

            let mut inbound = match stream_result {
                Ok(response) => response.into_inner(),
                Err(e) => {
                    tracing::warn!(
                        "Failed to establish control stream to {scheduler_address}: {e}"
                    );
                    heartbeat_task.abort();
                    component_status_task.abort();
                    let mut outbound_guard = outbound_tx_state_for_task.write().await;
                    *outbound_guard = None;
                    if let Some(delay) = backoff.next_duration() {
                        tokio::select! {
                            () = token.cancelled() => break,
                            () = tokio::time::sleep(delay) => {}
                        }
                    }
                    continue;
                }
            };

            tracing::debug!("Control stream established to scheduler {scheduler_address}");
            backoff.reset();

            // Process inbound messages (metrics requests)
            loop {
                tokio::select! {
                    () = token.cancelled() => {
                        heartbeat_task.abort();
                        component_status_task.abort();
                        {
                            let mut outbound_guard = outbound_tx_state_for_task.write().await;
                            *outbound_guard = None;
                        }
                        tracing::debug!(
                            "Control stream to {scheduler_address} cancelled"
                        );
                        return;
                    }
                    result = inbound.next() => {
                        match result {
                            Some(Ok(msg)) => {
                                if let Some(message) = msg.message {
                                    handle_scheduler_message(
                                        &scheduler_address,
                                        &executor_id,
                                        message,
                                        &outbound_tx,
                                        metrics_reader.as_deref(),
                                        executor.as_deref(),
                                        &poll_now_notify,
                                        partition_update_handler.as_ref(),
                                        refresh_dataset_handler.as_ref(),
                                        &ack_tx,
                                    )
                                    .await;
                                }
                            }
                            Some(Err(e)) => {
                                tracing::debug!(
                                    "Control stream error from {scheduler_address}: {e}"
                                );
                                break;
                            }
                            None => {
                                tracing::debug!(
                                    "Control stream to {scheduler_address} closed by scheduler"
                                );
                                break;
                            }
                        }
                    }
                }
            }

            heartbeat_task.abort();
            component_status_task.abort();
            {
                let mut outbound_guard = outbound_tx_state_for_task.write().await;
                *outbound_guard = None;
            }
            tracing::debug!("Control stream to {scheduler_address} disconnected, will reconnect");

            if let Some(delay) = backoff.next_duration() {
                tokio::select! {
                    () = token.cancelled() => break,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    });

    ControlStreamHandle {
        cancel,
        task,
        outbound_tx: outbound_tx_state,
    }
}

/// Builds an `ExecutorControlMessage` heartbeat.
fn build_heartbeat_message(executor_id: &str) -> ExecutorControlMessage {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0);

    ExecutorControlMessage {
        executor_id: executor_id.to_string(),
        message: Some(ExecutorMessage::Heartbeat(ExecutorHeartbeat {
            timestamp_ms,
        })),
    }
}

/// Builds a `ComponentStatusUpdate` message.
fn build_component_status_message(
    executor_id: &str,
    update_id: &str,
    component_name: &str,
    status: &ComponentStatus,
) -> ExecutorControlMessage {
    ExecutorControlMessage {
        executor_id: executor_id.to_string(),
        message: Some(ExecutorMessage::ComponentStatus(ComponentStatusUpdate {
            update_id: update_id.to_string(),
            component_name: component_name.to_string(),
            status: component_status_to_proto(status),
        })),
    }
}

/// Runs the component status sender task for one control stream connection.
///
/// Sends `ComponentStatusUpdate` messages to the scheduler whenever component
/// statuses change on the executor, and retries unacked updates periodically.
/// On reconnect (not initial connect), sends all current statuses immediately.
async fn run_component_status_sender(
    executor_id: String,
    tx: mpsc::Sender<ExecutorControlMessage>,
    cancel: CancellationToken,
    runtime_status: Arc<RuntimeStatus>,
    ack_rx: Option<mpsc::Receiver<String>>,
) {
    let Some(mut ack_rx) = ack_rx else {
        cancel.cancelled().await;
        return;
    };

    let mut change_rx = runtime_status.subscribe_component_status_changes();
    // component_name → (update_id, most_recent_status)
    let mut pending: HashMap<String, (String, ComponentStatus)> = HashMap::new();

    // Send all current component statuses on connect (no-op if empty)
    for (component_name, cs_status) in runtime_status.get_all_statuses() {
        let update_id = Uuid::new_v4().to_string();
        let msg =
            build_component_status_message(&executor_id, &update_id, &component_name, &cs_status);
        if tx.send(msg).await.is_err() {
            return;
        }
        pending.insert(component_name, (update_id, cs_status));
    }

    let mut retry_interval = tokio::time::interval(STATUS_RETRY_INTERVAL);
    retry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            // Status changed — send update for the specific component
            result = change_rx.recv() => {
                match result {
                    Ok((component_name, new_status)) => {
                        let update_id = Uuid::new_v4().to_string();
                        let msg = build_component_status_message(
                            &executor_id, &update_id, &component_name, &new_status,
                        );
                        if tx.send(msg).await.is_err() {
                            return;
                        }
                        pending.insert(component_name, (update_id, new_status));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Fell behind — re-send all current statuses
                        for (component_name, cs_status) in runtime_status.get_all_statuses() {
                            let update_id = Uuid::new_v4().to_string();
                            let msg = build_component_status_message(
                                &executor_id, &update_id, &component_name, &cs_status,
                            );
                            if tx.send(msg).await.is_err() {
                                return;
                            }
                            pending.insert(component_name, (update_id, cs_status));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            // Ack received — remove from pending
            Some(acked_id) = ack_rx.recv() => {
                pending.retain(|_, (id, _)| id != &acked_id);
            }
            // Retry unacked updates periodically
            _ = retry_interval.tick() => {
                for (component_name, (update_id, cs_status)) in &pending {
                    let msg = build_component_status_message(
                        &executor_id, update_id, component_name, cs_status,
                    );
                    if tx.send(msg).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Handles a message from the scheduler on the control stream.
#[expect(clippy::too_many_arguments)]
async fn handle_scheduler_message(
    scheduler_address: &str,
    executor_id: &str,
    message: SchedulerMessage,
    outbound_tx: &mpsc::Sender<ExecutorControlMessage>,
    metrics_reader: Option<&MetricsReader>,
    executor: Option<&Executor>,
    poll_now_notify: &Notify,
    partition_update_handler: Option<&PartitionUpdateHandler>,
    refresh_dataset_handler: Option<&RefreshDatasetHandler>,
    ack_tx: &mpsc::Sender<String>,
) {
    match message {
        SchedulerMessage::RequestMetrics(request) => {
            tracing::debug!(
                "Received metrics request from {scheduler_address}: request_id={}",
                request.request_id
            );

            // Collect local OTLP metrics using the MetricsReader if available
            let otlp_metrics = if let Some(reader) = metrics_reader {
                reader.collect_otlp()
            } else {
                tracing::debug!("No MetricsReader available, returning empty metrics");
                Vec::new()
            };

            let response = ExecutorControlMessage {
                executor_id: executor_id.to_string(),
                message: Some(ExecutorMessage::Metrics(MetricsResponse {
                    request_id: request.request_id,
                    otlp_metrics,
                })),
            };

            if let Err(e) = outbound_tx.send(response).await {
                tracing::warn!("Failed to send metrics response to {scheduler_address}: {e}");
            }
        }
        SchedulerMessage::PollNow(cmd) => {
            tracing::debug!(
                reason = %cmd.reason,
                "Received PollNow from scheduler {scheduler_address}"
            );
            poll_now_notify.notify_one();
        }
        SchedulerMessage::UpdatePartitions(update) => {
            tracing::debug!(
                "Received UpdatePartitions from scheduler {scheduler_address}: {} new, {} removed",
                update.new_partitions.len(),
                update.removed_partitions.len()
            );

            if let Some(handler) = partition_update_handler {
                let new_partitions = update
                    .new_partitions
                    .into_iter()
                    .map(|(k, v)| (k, v.items))
                    .collect();
                let removed_partitions = update
                    .removed_partitions
                    .into_iter()
                    .map(|(k, v)| (k, v.items))
                    .collect();
                handler(new_partitions, removed_partitions).await;
            }
        }
        SchedulerMessage::CancelTasks(cmd) => {
            let Some(executor) = executor else {
                tracing::warn!(
                    "Received CancelTasks from {scheduler_address} but no executor is available"
                );
                return;
            };

            for task in cmd.tasks {
                match executor
                    .cancel_task(
                        task.task_id as usize,
                        task.job_id.clone(),
                        task.stage_id as usize,
                        task.partition_id as usize,
                    )
                    .await
                {
                    Ok(true) => {
                        tracing::debug!(
                            task_id = task.task_id,
                            job_id = %task.job_id,
                            "Cancelled task from scheduler {scheduler_address}"
                        );
                    }
                    Ok(false) => {
                        tracing::debug!(
                            task_id = task.task_id,
                            job_id = %task.job_id,
                            "Task not found for cancellation (may have already completed)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            task_id = task.task_id,
                            job_id = %task.job_id,
                            "Failed to cancel task: {e}"
                        );
                    }
                }
            }
        }
        SchedulerMessage::RefreshDataset(cmd) => {
            tracing::info!(
                dataset = %cmd.dataset_name,
                "Received RefreshDataset from scheduler {scheduler_address}"
            );

            if let Some(handler) = refresh_dataset_handler {
                handler(cmd.dataset_name, cmd.overrides_json).await;
            } else {
                tracing::warn!("Received RefreshDataset command but no handler is registered");
            }
        }
        SchedulerMessage::ComponentStatusAck(ack) => {
            let _ = ack_tx.send(ack.update_id.clone()).await;
        }
    }
}

/// Normalizes a scheduler endpoint address to a URL with scheme.
fn normalize_scheduler_endpoint(address: &str, tls_enabled: bool) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        return address.to_string();
    }

    let scheme = if tls_enabled { "https" } else { "http" };
    format!("{scheme}://{address}")
}

/// Manages control stream connections to all schedulers.
///
/// This struct tracks scheduler membership and ensures control streams
/// are established to all known schedulers. It also provides a shared `Notify`
/// handle that is signaled when any scheduler sends a [`PollNow`] command.
pub struct ControlStreamManager {
    executor_id: String,
    ballista_executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
    metrics_reader: Option<Arc<MetricsReader>>,
    executor: Option<Arc<Executor>>,
    streams: HashMap<String, ControlStreamHandle>,
    known_schedulers: HashSet<String>,
    /// Shared notify handle signaled when any scheduler sends `PollNow`.
    poll_now_notify: Arc<Notify>,
    /// Callback handler for partition updates.
    partition_update_handler: Option<PartitionUpdateHandler>,
    /// Callback handler for dataset refresh commands.
    refresh_dataset_handler: Option<RefreshDatasetHandler>,
    /// Executor's runtime status, used to send real-time
    /// `ComponentStatusUpdate` messages for runtime components.
    runtime_status: Arc<RuntimeStatus>,
}

impl ControlStreamManager {
    /// Creates a new control stream manager.
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        executor_id: String,
        ballista_executor_id: String,
        client_tls_config: Option<ClientTlsConfig>,
        metrics_reader: Option<MetricsReader>,
        partition_update_handler: Option<PartitionUpdateHandler>,
        executor: Option<Arc<Executor>>,
        refresh_dataset_handler: Option<RefreshDatasetHandler>,
        runtime_status: Arc<RuntimeStatus>,
    ) -> Self {
        Self {
            executor_id,
            ballista_executor_id,
            client_tls_config,
            metrics_reader: metrics_reader.map(Arc::new),
            executor,
            streams: HashMap::new(),
            known_schedulers: HashSet::new(),
            poll_now_notify: Arc::new(Notify::new()),
            partition_update_handler,
            refresh_dataset_handler,
            runtime_status,
        }
    }

    /// Returns a clone of the shared `Notify` handle.
    ///
    /// This handle is signaled when any connected scheduler sends a `PollNow` command.
    /// Pass this to the poll loop to enable immediate wake-up on new work.
    #[must_use]
    pub fn poll_now_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.poll_now_notify)
    }

    /// Sends a shutdown notification to all connected schedulers.
    pub async fn notify_shutdown(&self, reason: &str) {
        if self.streams.is_empty() {
            return;
        }

        let message = ExecutorControlMessage {
            executor_id: self.executor_id.clone(),
            message: Some(ExecutorMessage::Shutdown(ExecutorShutdown {
                ballista_executor_id: self.ballista_executor_id.clone(),
                reason: reason.to_string(),
            })),
        };

        let mut sent = 0usize;
        for (scheduler_address, handle) in &self.streams {
            let outbound_tx = { handle.outbound_tx.read().await.clone() };
            if let Some(outbound_tx) = outbound_tx {
                match outbound_tx.try_send(message.clone()) {
                    Ok(()) => {
                        sent += 1;
                    }
                    Err(err) => {
                        tracing::debug!(
                            "Failed to send shutdown to scheduler {scheduler_address}: {err}"
                        );
                    }
                }
            }
        }

        tracing::debug!(
            "Sent executor shutdown notification to {sent} scheduler streams: {reason}"
        );
    }

    /// Updates the set of schedulers and spawns/removes control streams as needed.
    pub fn update_schedulers(&mut self, scheduler_addresses: Vec<String>) {
        let next_schedulers: HashSet<String> = scheduler_addresses.into_iter().collect();

        let added: Vec<String> = next_schedulers
            .difference(&self.known_schedulers)
            .cloned()
            .collect();
        let removed: Vec<String> = self
            .known_schedulers
            .difference(&next_schedulers)
            .cloned()
            .collect();

        if !added.is_empty() || !removed.is_empty() {
            tracing::debug!(
                "Control stream membership updated; added={}, removed={}",
                added.len(),
                removed.len()
            );
        }

        // Spawn new control streams
        for address in added {
            let outbound_tx_state = Arc::new(RwLock::new(None));
            let handle = spawn_control_stream(
                address.clone(),
                self.executor_id.clone(),
                self.client_tls_config.clone(),
                self.metrics_reader.clone(),
                self.executor.clone(),
                Arc::clone(&self.poll_now_notify),
                Arc::clone(&outbound_tx_state),
                self.partition_update_handler.as_ref(),
                self.refresh_dataset_handler.as_ref(),
                Arc::clone(&self.runtime_status),
            );
            self.streams.insert(address, handle);
        }

        // Cancel and remove old control streams
        for address in removed {
            if let Some(handle) = self.streams.remove(&address) {
                handle.cancel.cancel();
                handle.task.abort();
            }
        }

        self.known_schedulers = next_schedulers;
    }

    /// Cancels all control streams.
    pub fn shutdown(&mut self) {
        for (_, handle) in self.streams.drain() {
            handle.cancel.cancel();
            handle.task.abort();
        }
        self.known_schedulers.clear();
    }
}

impl Drop for ControlStreamManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_scheduler_endpoint_without_scheme() {
        assert_eq!(
            normalize_scheduler_endpoint("localhost:50051", false),
            "http://localhost:50051"
        );
        assert_eq!(
            normalize_scheduler_endpoint("localhost:50051", true),
            "https://localhost:50051"
        );
        assert_eq!(
            normalize_scheduler_endpoint("192.168.1.10:50052", false),
            "http://192.168.1.10:50052"
        );
    }

    #[test]
    fn test_normalize_scheduler_endpoint_with_scheme() {
        // Already has scheme - should not be modified
        assert_eq!(
            normalize_scheduler_endpoint("http://localhost:50051", false),
            "http://localhost:50051"
        );
        assert_eq!(
            normalize_scheduler_endpoint("https://localhost:50051", true),
            "https://localhost:50051"
        );
        // Scheme takes precedence over tls_enabled flag
        assert_eq!(
            normalize_scheduler_endpoint("http://localhost:50051", true),
            "http://localhost:50051"
        );
        assert_eq!(
            normalize_scheduler_endpoint("https://localhost:50051", false),
            "https://localhost:50051"
        );
    }

    #[test]
    fn test_control_stream_manager_new() {
        let manager = ControlStreamManager::new(
            "executor-1".to_string(),
            "executor-1".to_string(),
            None, // no TLS
            None, // no metrics reader
            None,
            None,
            None,
            RuntimeStatus::new(),
        );
        assert!(manager.known_schedulers.is_empty());
        assert!(manager.streams.is_empty());
        assert_eq!(manager.executor_id, "executor-1");
    }

    #[test]
    fn test_control_stream_manager_new_with_metrics_reader() {
        let reader = MetricsReader::new();
        let manager = ControlStreamManager::new(
            "executor-2".to_string(),
            "executor-2".to_string(),
            None,
            Some(reader),
            None,
            None,
            None,
            RuntimeStatus::new(),
        );
        assert!(manager.metrics_reader.is_some());
    }

    #[test]
    fn test_control_stream_manager_update_schedulers_empty() {
        let mut manager = ControlStreamManager::new(
            "executor-1".to_string(),
            "executor-1".to_string(),
            None,
            None,
            None,
            None,
            None,
            RuntimeStatus::new(),
        );
        manager.update_schedulers(vec![]);
        assert!(manager.known_schedulers.is_empty());
        assert!(manager.streams.is_empty());
    }

    #[test]
    fn test_control_stream_manager_shutdown_empty() {
        let mut manager = ControlStreamManager::new(
            "executor-1".to_string(),
            "executor-1".to_string(),
            None,
            None,
            None,
            None,
            None,
            RuntimeStatus::new(),
        );
        // Should not panic on empty manager
        manager.shutdown();
        assert!(manager.known_schedulers.is_empty());
        assert!(manager.streams.is_empty());
    }

    // --- Component status sender tests ---

    /// Extract `ComponentStatusUpdate` from an outbound message, or panic.
    fn expect_status_update(msg: ExecutorControlMessage) -> runtime_proto::ComponentStatusUpdate {
        match msg.message {
            Some(ExecutorMessage::ComponentStatus(update)) => update,
            other => panic!("Expected ComponentStatus, got {other:?}"),
        }
    }

    /// Spawn `run_component_status_sender` and return channels to drive it.
    fn spawn_status_sender(
        status: Arc<RuntimeStatus>,
    ) -> (
        mpsc::Receiver<ExecutorControlMessage>,
        mpsc::Sender<String>,
        CancellationToken,
    ) {
        let (outbound_tx, outbound_rx) = mpsc::channel(32);
        let (ack_tx, ack_rx) = mpsc::channel(32);
        let cancel = CancellationToken::new();

        tokio::spawn(run_component_status_sender(
            "test-executor".to_string(),
            outbound_tx,
            cancel.clone(),
            status,
            Some(ack_rx),
        ));

        (outbound_rx, ack_tx, cancel)
    }

    #[tokio::test]
    async fn test_sends_current_statuses_on_connect() {
        let status = RuntimeStatus::new();
        status.update_dataset(
            &datafusion::sql::TableReference::bare("orders"),
            ComponentStatus::Ready,
        );

        let (mut rx, _ack_tx, cancel) = spawn_status_sender(Arc::clone(&status));

        let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("should receive message")
            .expect("channel open");
        let update = expect_status_update(msg);
        assert_eq!(update.component_name, "dataset:orders");
        assert_eq!(update.status, 1); // Ready

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_sends_update_on_status_change() {
        let status = RuntimeStatus::new();
        let (mut rx, _ack_tx, cancel) = spawn_status_sender(Arc::clone(&status));

        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Change a status
        status.update_dataset(
            &datafusion::sql::TableReference::bare("orders"),
            ComponentStatus::Refreshing,
        );

        let msg = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("should receive message")
            .expect("channel open");
        let update = expect_status_update(msg);
        assert_eq!(update.component_name, "dataset:orders");
        assert_eq!(update.status, 4); // Refreshing

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_ack_removes_from_pending_no_retry() {
        let status = RuntimeStatus::new();
        status.update_dataset(
            &datafusion::sql::TableReference::bare("orders"),
            ComponentStatus::Ready,
        );

        let (mut rx, ack_tx, cancel) = spawn_status_sender(Arc::clone(&status));

        // Receive the initial update
        let msg = rx.recv().await.expect("channel open");
        let update = expect_status_update(msg);

        // Ack it
        ack_tx
            .send(update.update_id)
            .await
            .expect("ack channel open");

        // Wait longer than the retry interval. If acked properly, no retry should arrive.
        let result =
            tokio::time::timeout(STATUS_RETRY_INTERVAL + Duration::from_secs(2), rx.recv()).await;
        assert!(
            result.is_err(),
            "Should not retry after ack (timeout expected)"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_status_change_before_ack_sends_latest() {
        let status = RuntimeStatus::new();
        let table_ref = datafusion::sql::TableReference::bare("orders");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        let (mut rx, ack_tx, cancel) = spawn_status_sender(Arc::clone(&status));

        // Receive initial update (Initializing)
        let msg = rx.recv().await.expect("channel open");
        let update1 = expect_status_update(msg);
        assert_eq!(update1.status, 0); // Initializing
        let old_update_id = update1.update_id.clone();

        // Change status before acking
        status.update_dataset(&table_ref, ComponentStatus::Ready);

        // Should receive new update (Ready) with a different update_id
        let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("should receive update")
            .expect("channel open");
        let update2 = expect_status_update(msg);
        assert_eq!(update2.status, 1); // Ready
        assert_ne!(update2.update_id, old_update_id);

        // Ack the OLD update_id — should be ignored (stale)
        ack_tx.send(old_update_id).await.expect("ack channel open");

        // Wait for retry — should still retry the new update (not acked)
        let msg = tokio::time::timeout(STATUS_RETRY_INTERVAL + Duration::from_secs(2), rx.recv())
            .await
            .expect("should receive retry")
            .expect("channel open");
        let retry = expect_status_update(msg);
        assert_eq!(retry.update_id, update2.update_id);
        assert_eq!(retry.status, 1); // Still Ready

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_retry_resends_unacked() {
        let status = RuntimeStatus::new();
        status.update_dataset(
            &datafusion::sql::TableReference::bare("orders"),
            ComponentStatus::Ready,
        );

        let (mut rx, _ack_tx, cancel) = spawn_status_sender(Arc::clone(&status));

        // Receive initial update
        let msg = rx.recv().await.expect("channel open");
        let update = expect_status_update(msg);

        // Don't ack — advance past retry interval
        tokio::time::sleep(STATUS_RETRY_INTERVAL + Duration::from_secs(1)).await;

        // Should receive a retry with the same update_id
        let msg = rx.recv().await.expect("channel open");
        let retry = expect_status_update(msg);
        assert_eq!(retry.update_id, update.update_id);
        assert_eq!(retry.component_name, "dataset:orders");
        assert_eq!(retry.status, 1); // Ready

        cancel.cancel();
    }
}
