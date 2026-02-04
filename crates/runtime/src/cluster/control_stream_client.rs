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
//! to request metrics from executors on-demand and receive `PollNow` commands
//! to trigger immediate work polling.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use ballista_core::utils::create_grpc_client_endpoint;
use futures::StreamExt;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_proto::scheduler_control_message::Message as SchedulerMessage;
use runtime_proto::{
    ExecutorControlMessage, ExecutorHeartbeat, ExecutorShutdown, MetricsResponse,
    executor_control_message::Message as ExecutorMessage,
};
use tokio::sync::{Notify, RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::ClientTlsConfig;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

use crate::metrics_reader::MetricsReader;

const CONTROL_STREAM_BACKOFF_MAX: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

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
fn spawn_control_stream(
    scheduler_address: String,
    executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
    metrics_reader: Option<Arc<MetricsReader>>,
    poll_now_notify: Arc<Notify>,
    outbound_tx_state: Arc<RwLock<Option<mpsc::Sender<ExecutorControlMessage>>>>,
) -> ControlStreamHandle {
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let outbound_tx_state_for_task = Arc::clone(&outbound_tx_state);

    let task = tokio::spawn(async move {
        let tls_enabled = client_tls_config.is_some();
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_duration(Some(CONTROL_STREAM_BACKOFF_MAX))
            .build();

        loop {
            if token.is_cancelled() {
                tracing::debug!("Control stream to scheduler {scheduler_address} cancelled");
                break;
            }

            // Build endpoint
            let endpoint_url = normalize_scheduler_endpoint(&scheduler_address, tls_enabled);
            let endpoint = match create_grpc_client_endpoint(endpoint_url.clone()) {
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
                            let timestamp_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
                                .unwrap_or(0);

                            let msg = ExecutorControlMessage {
                                executor_id: heartbeat_executor_id.clone(),
                                message: Some(ExecutorMessage::Heartbeat(ExecutorHeartbeat {
                                    timestamp_ms,
                                })),
                            };

                            if heartbeat_tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });

            // Send initial identification message
            let init_msg = ExecutorControlMessage {
                executor_id: executor_id.clone(),
                message: Some(ExecutorMessage::Heartbeat(ExecutorHeartbeat {
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
                        .unwrap_or(0),
                })),
            };
            if outbound_tx.send(init_msg).await.is_err() {
                heartbeat_task.abort();
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
                                        &poll_now_notify,
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

/// Handles a message from the scheduler on the control stream.
async fn handle_scheduler_message(
    scheduler_address: &str,
    executor_id: &str,
    message: SchedulerMessage,
    outbound_tx: &mpsc::Sender<ExecutorControlMessage>,
    metrics_reader: Option<&MetricsReader>,
    poll_now_notify: &Notify,
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
    streams: HashMap<String, ControlStreamHandle>,
    known_schedulers: HashSet<String>,
    /// Shared notify handle signaled when any scheduler sends `PollNow`.
    poll_now_notify: Arc<Notify>,
}

impl ControlStreamManager {
    /// Creates a new control stream manager.
    #[must_use]
    pub fn new(
        executor_id: String,
        ballista_executor_id: String,
        client_tls_config: Option<ClientTlsConfig>,
        metrics_reader: Option<MetricsReader>,
    ) -> Self {
        Self {
            executor_id,
            ballista_executor_id,
            client_tls_config,
            metrics_reader: metrics_reader.map(Arc::new),
            streams: HashMap::new(),
            known_schedulers: HashSet::new(),
            poll_now_notify: Arc::new(Notify::new()),
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
                Arc::clone(&self.poll_now_notify),
                Arc::clone(&outbound_tx_state),
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
        );
        // Should not panic on empty manager
        manager.shutdown();
        assert!(manager.known_schedulers.is_empty());
        assert!(manager.streams.is_empty());
    }
}
