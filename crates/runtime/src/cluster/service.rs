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
//! including app definition retrieval and secret expansion.

use app::App;
use futures::{Stream, StreamExt};
use runtime_proto::cluster_service_server::ClusterService;
use runtime_proto::executor_control_message::Message as ExecutorMessage;
use runtime_proto::{
    ExecutorControlMessage, ExpandSecretRequest, ExpandSecretResponse, GetAppDefinitionRequest,
    GetAppDefinitionResponse, GetSchedulersRequest, GetSchedulersResponse, SchedulerControlMessage,
    SchedulerInstance,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

use crate::cluster::SchedulerPeers;

/// Internal cluster service for scheduler-executor communication.
pub struct ClusterServiceImpl {
    app: Arc<RwLock<Option<Arc<App>>>>,
    secrets: Arc<RwLock<Secrets>>,
    advertise_address: String,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
}

impl ClusterServiceImpl {
    /// Creates a new cluster service implementation.
    #[must_use]
    pub fn new(
        app: Arc<RwLock<Option<Arc<App>>>>,
        secrets: Arc<RwLock<Secrets>>,
        advertise_address: String,
        scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
        }
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

    type ControlStreamStream =
        Pin<Box<dyn Stream<Item = Result<SchedulerControlMessage, Status>> + Send>>;

    async fn control_stream(
        &self,
        request: Request<Streaming<ExecutorControlMessage>>,
    ) -> Result<Response<Self::ControlStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let cancel = CancellationToken::new();
        let inbound_cancel = cancel.clone();

        // Create a channel for outbound messages to the executor (unused for now).
        let (outbound_tx, outbound_rx) = mpsc::channel::<SchedulerControlMessage>(32);

        // We need to identify the executor from its first message.
        let inbound_task = tokio::spawn(async move {
            let executor_id = match inbound.next().await {
                Some(Ok(msg)) => {
                    let executor_id = msg.executor_id.clone();
                    if executor_id.is_empty() {
                        tracing::warn!("Executor connected with empty executor_id, closing stream");
                        return;
                    }
                    tracing::debug!("Executor control stream connected: {executor_id}");

                    if let Some(message) = msg.message {
                        handle_executor_message(&executor_id, message);
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
                                    handle_executor_message(&executor_id, message);
                                }
                            }
                            Some(Err(e)) => {
                                tracing::debug!(
                                    "Executor control stream error for {executor_id}: {e}"
                                );
                                break;
                            }
                            None => {
                                tracing::debug!(
                                    "Executor control stream closed by executor {executor_id}"
                                );
                                break;
                            }
                        }
                    }
                }
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
}

fn handle_executor_message(executor_id: &str, message: ExecutorMessage) {
    match message {
        ExecutorMessage::Heartbeat(heartbeat) => {
            tracing::trace!(
                "Received heartbeat from executor {executor_id}: timestamp_ms={}",
                heartbeat.timestamp_ms
            );
        }
    }
}
