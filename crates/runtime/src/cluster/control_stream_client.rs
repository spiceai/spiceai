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
//! bidirectional control streams with schedulers. These streams currently send
//! periodic heartbeats for liveness tracking.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ballista_core::utils::create_grpc_client_endpoint;
use futures::StreamExt;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use runtime_proto::executor_control_message::Message as ExecutorMessage;
use runtime_proto::{ExecutorControlMessage, ExecutorHeartbeat, SchedulerControlMessage};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::ClientTlsConfig;
use util::fibonacci_backoff::{Backoff, FibonacciBackoffBuilder};

const CONTROL_STREAM_BACKOFF_MAX: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Handle for a single control stream connection to a scheduler.
struct ControlStreamHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

/// Spawns a control stream connection to a single scheduler.
///
/// The stream will:
/// 1. Connect to the scheduler
/// 2. Send periodic heartbeats
/// 3. Reconnect on failure with exponential backoff
fn spawn_control_stream(
    scheduler_address: String,
    executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
) -> ControlStreamHandle {
    let cancel = CancellationToken::new();
    let token = cancel.clone();

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

            loop {
                tokio::select! {
                    () = token.cancelled() => {
                        heartbeat_task.abort();
                        tracing::debug!(
                            "Control stream to {scheduler_address} cancelled"
                        );
                        return;
                    }
                    result = inbound.next() => {
                        match result {
                            Some(Ok(message)) => {
                                handle_scheduler_message(&scheduler_address, message);
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
            tracing::debug!("Control stream to {scheduler_address} disconnected, will reconnect");

            if let Some(delay) = backoff.next_duration() {
                tokio::select! {
                    () = token.cancelled() => break,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    });

    ControlStreamHandle { cancel, task }
}

/// Handles a message from the scheduler on the control stream.
fn handle_scheduler_message(scheduler_address: &str, message: SchedulerControlMessage) {
    tracing::trace!(
        message = ?message,
        "Ignoring control message from scheduler {scheduler_address}"
    );
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
/// are established to all known schedulers.
pub struct ControlStreamManager {
    executor_id: String,
    client_tls_config: Option<ClientTlsConfig>,
    streams: HashMap<String, ControlStreamHandle>,
    known_schedulers: HashSet<String>,
}

impl ControlStreamManager {
    /// Creates a new control stream manager.
    #[must_use]
    pub fn new(executor_id: String, client_tls_config: Option<ClientTlsConfig>) -> Self {
        Self {
            executor_id,
            client_tls_config,
            streams: HashMap::new(),
            known_schedulers: HashSet::new(),
        }
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
            let handle = spawn_control_stream(
                address.clone(),
                self.executor_id.clone(),
                self.client_tls_config.clone(),
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
