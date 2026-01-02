/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::Runtime;
use ballista_core::serde::protobuf::{
    ExecutorHeartbeat, ExecutorStatus, TaskStatus, executor_status,
};
use prost::Message;
use runtime_proto::PollExecutorRequest;
use runtime_proto::executor_service_client::ExecutorServiceClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_STATUS_BATCH: u32 = 1024;

pub async fn start_executor_poll_loop(
    rt: Arc<Runtime>,
    shutdown: CancellationToken,
) -> crate::Result<()> {
    let scheduler = rt
        .df
        .scheduler_server
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .ok_or_else(|| crate::Error::FailedToStartClusterScheduler {
            source: "Scheduler server not available for executor polling"
                .to_string()
                .into(),
        })?;

    let scheduler_id = scheduler.scheduler_name.clone();
    let client_tls_config = rt.df.cluster_config.client_tls_config().cloned();
    let mut clients: HashMap<String, ExecutorServiceClient<Channel>> = HashMap::new();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("Executor polling loop shutting down");
                return Ok(());
            }
            () = tokio::time::sleep(POLL_INTERVAL) => {}
        }

        let executors = match scheduler.state.executor_manager.get_executor_state().await {
            Ok(executors) => executors,
            Err(e) => {
                tracing::warn!("Executor polling: failed to list executors: {e}");
                continue;
            }
        };

        for (metadata, _last_seen) in executors {
            let host = metadata.host.clone();
            let scheme = if client_tls_config.is_some() {
                "https"
            } else {
                "http"
            };
            let endpoint_url = format!("{scheme}://{host}:{}", metadata.port);

            let client = if let Some(client) = clients.get(&metadata.id).cloned() {
                client
            } else {
                let mut endpoint = Endpoint::from_shared(endpoint_url.clone()).map_err(|e| {
                    crate::Error::FailedToStartClusterScheduler {
                        source: Box::new(e),
                    }
                })?;
                if let Some(tls_config) = client_tls_config.clone() {
                    endpoint = endpoint.tls_config(tls_config).map_err(|e| {
                        crate::Error::FailedToStartClusterScheduler {
                            source: Box::new(e),
                        }
                    })?;
                }
                let channel = match endpoint.connect().await {
                    Ok(channel) => channel,
                    Err(e) => {
                        tracing::debug!(
                            "Executor polling: failed to connect to {}: {e}",
                            metadata.id
                        );
                        continue;
                    }
                };
                let client = ExecutorServiceClient::new(channel);
                clients.insert(metadata.id.clone(), client.clone());
                client
            };

            let response = match client
                .clone()
                .poll_executor(PollExecutorRequest {
                    scheduler_id: scheduler_id.clone(),
                    max_statuses: MAX_STATUS_BATCH,
                })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(e) => {
                    tracing::debug!("Executor polling: poll failed for {}: {e}", metadata.id);
                    clients.remove(&metadata.id);
                    continue;
                }
            };

            if !response.task_statuses.is_empty() {
                let mut decoded: Vec<TaskStatus> = Vec::with_capacity(response.task_statuses.len());
                for bytes in response.task_statuses {
                    match TaskStatus::decode(bytes.as_slice()) {
                        Ok(status) => decoded.push(status),
                        Err(e) => {
                            tracing::warn!(
                                "Executor polling: failed to decode task status from {}: {e}",
                                metadata.id
                            );
                        }
                    }
                }
                if !decoded.is_empty()
                    && let Err(e) = scheduler
                        .update_task_status(&response.executor_id, decoded)
                        .await
                {
                    tracing::warn!(
                        "Executor polling: failed to update task status for {}: {e}",
                        response.executor_id
                    );
                }
            }

            let status = if response.terminating {
                executor_status::Status::Terminating(String::new())
            } else {
                executor_status::Status::Active(String::new())
            };

            let heartbeat = ExecutorHeartbeat {
                executor_id: response.executor_id,
                timestamp: response.timestamp_millis / 1000,
                metrics: Vec::new(),
                status: Some(ExecutorStatus {
                    status: Some(status),
                }),
            };

            if let Err(e) = scheduler
                .state
                .executor_manager
                .save_executor_heartbeat(heartbeat)
                .await
            {
                tracing::warn!(
                    "Executor polling: failed to save heartbeat for {}: {e}",
                    metadata.id
                );
            }
        }
    }
}
