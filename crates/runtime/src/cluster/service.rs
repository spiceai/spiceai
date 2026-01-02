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

//! Internal cluster gRPC service implementation.
//!
//! This service handles scheduler-executor communication for cluster mode,
//! including app definition retrieval and secret expansion.

use app::App;
use runtime_proto::cluster_service_server::ClusterService;
use runtime_proto::executor_service_server::ExecutorService;
use runtime_proto::{
    DescribeExecutorRequest, DescribeExecutorResponse, ExpandSecretRequest, ExpandSecretResponse,
    GetAppDefinitionRequest, GetAppDefinitionResponse, PollExecutorRequest, PollExecutorResponse,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::datafusion::DataFusion;
use ballista_executor::executor_server::TERMINATING;
use prost::Message;

/// Internal cluster service for scheduler-executor communication.
pub struct ClusterServiceImpl {
    app: Arc<RwLock<Option<Arc<App>>>>,
    secrets: Arc<RwLock<Secrets>>,
}

impl ClusterServiceImpl {
    /// Creates a new cluster service implementation.
    #[must_use]
    pub fn new(app: Arc<RwLock<Option<Arc<App>>>>, secrets: Arc<RwLock<Secrets>>) -> Self {
        Self { app, secrets }
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
}

/// Executor service for scheduler-driven discovery.
///
/// This service is exposed by executors so schedulers can query their
/// registration information (executor ID, host, port, task slots).
pub struct ExecutorServiceImpl {
    df: Arc<DataFusion>,
}

impl ExecutorServiceImpl {
    /// Creates a new executor service implementation.
    #[must_use]
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self { df }
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServiceImpl {
    async fn describe_executor(
        &self,
        _request: Request<DescribeExecutorRequest>,
    ) -> Result<Response<DescribeExecutorResponse>, Status> {
        tracing::trace!("ExecutorService::describe_executor called");

        let executor_guard = self
            .df
            .executor
            .read()
            .map_err(|_| Status::internal("Failed to acquire executor lock"))?;

        let Some(ref executor) = *executor_guard else {
            return Err(Status::unavailable(
                "Executor registration not yet available",
            ));
        };

        let registration = &executor.metadata;

        let task_slots = registration
            .specification
            .as_ref()
            .and_then(|spec| {
                spec.resources.iter().find_map(|r| {
                    r.resource.as_ref().map(|res| {
                        let ballista_core::serde::protobuf::executor_resource::Resource::TaskSlots(
                            slots,
                        ) = res;
                        *slots
                    })
                })
            })
            .unwrap_or(0);

        Ok(Response::new(DescribeExecutorResponse {
            executor_id: registration.id.clone(),
            host: registration.host.clone().unwrap_or_default(),
            port: registration.port,
            grpc_port: registration.grpc_port,
            task_slots,
        }))
    }

    async fn poll_executor(
        &self,
        request: Request<PollExecutorRequest>,
    ) -> Result<Response<PollExecutorResponse>, Status> {
        let request = request.into_inner();
        tracing::trace!(
            "ExecutorService::poll_executor called for scheduler {}",
            request.scheduler_id
        );

        let executor_guard = self
            .df
            .executor
            .read()
            .map_err(|_| Status::internal("Failed to acquire executor lock"))?;

        let Some(ref executor) = *executor_guard else {
            return Err(Status::unavailable(
                "Executor registration not yet available",
            ));
        };

        let max_statuses = usize::try_from(request.max_statuses).unwrap_or(0);
        let task_statuses = executor
            .status_store()
            .drain_task_statuses(&request.scheduler_id, max_statuses);

        let mut encoded = Vec::with_capacity(task_statuses.len());
        for status in task_statuses {
            let mut buf = Vec::new();
            status
                .encode(&mut buf)
                .map_err(|e| Status::internal(format!("Failed to encode task status: {e}")))?;
            encoded.push(buf);
        }

        let timestamp_millis = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Status::internal(format!("Failed to read system time: {e}")))?
                .as_millis(),
        )
        .map_err(|e| Status::internal(format!("Failed to read system time: {e}")))?;

        Ok(Response::new(PollExecutorResponse {
            executor_id: executor.metadata.id.clone(),
            terminating: TERMINATING.load(Ordering::Acquire),
            timestamp_millis,
            task_statuses: encoded,
        }))
    }
}
