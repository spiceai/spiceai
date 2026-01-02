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
use runtime_proto::executor_lease_service_server::ExecutorLeaseService;
use runtime_proto::executor_service_server::ExecutorService;
use runtime_proto::{
    DescribeExecutorRequest, DescribeExecutorResponse, ExpandSecretRequest, ExpandSecretResponse,
    GetAppDefinitionRequest, GetAppDefinitionResponse, GetCapacityRequest, GetCapacityResponse,
    PollExecutorRequest, PollExecutorResponse, ReleaseLeaseRequest, ReleaseLeaseResponse,
    RenewLeaseRequest, RenewLeaseResponse, ReserveSlotsRequest, ReserveSlotsResponse,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use super::lease::{LeaseManager, ReleaseResult, RenewalResult, ReservationResult};
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

/// Executor lease service for task slot reservation.
///
/// This service allows schedulers to reserve task slots on the executor
/// before dispatching tasks, enabling better capacity management.
pub struct ExecutorLeaseServiceImpl {
    lease_manager: Arc<LeaseManager>,
}

impl ExecutorLeaseServiceImpl {
    /// Creates a new executor lease service implementation.
    #[must_use]
    pub fn new(lease_manager: Arc<LeaseManager>) -> Self {
        Self { lease_manager }
    }
}

#[tonic::async_trait]
impl ExecutorLeaseService for ExecutorLeaseServiceImpl {
    async fn reserve_slots(
        &self,
        request: Request<ReserveSlotsRequest>,
    ) -> Result<Response<ReserveSlotsResponse>, Status> {
        let request = request.into_inner();

        if request.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is required"));
        }
        if request.scheduler_id.is_empty() {
            return Err(Status::invalid_argument("scheduler_id is required"));
        }

        tracing::trace!(
            request_id = %request.request_id,
            scheduler_id = %request.scheduler_id,
            slots = request.slots,
            ttl_ms = request.ttl_ms,
            "ExecutorLeaseService::reserve_slots"
        );

        let result = self
            .lease_manager
            .reserve_slots(
                &request.request_id,
                &request.scheduler_id,
                request.slots,
                request.ttl_ms,
            )
            .await;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "TTL is clamped to max 5 minutes, fits in u64"
        )]
        let response = match result {
            ReservationResult::Granted {
                lease_id,
                slots_granted,
                ttl,
            }
            | ReservationResult::Duplicate {
                lease_id,
                slots_granted,
                ttl,
            } => ReserveSlotsResponse {
                granted: true,
                lease_id: lease_id.to_string(),
                slots_granted,
                ttl_ms: ttl.as_millis() as u64,
                rejection_reason: String::new(),
            },
            ReservationResult::Denied { reason } => ReserveSlotsResponse {
                granted: false,
                lease_id: String::new(),
                slots_granted: 0,
                ttl_ms: 0,
                rejection_reason: reason,
            },
        };

        Ok(Response::new(response))
    }

    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> Result<Response<RenewLeaseResponse>, Status> {
        let request = request.into_inner();

        if request.lease_id.is_empty() {
            return Err(Status::invalid_argument("lease_id is required"));
        }
        if request.scheduler_id.is_empty() {
            return Err(Status::invalid_argument("scheduler_id is required"));
        }

        tracing::trace!(
            lease_id = %request.lease_id,
            scheduler_id = %request.scheduler_id,
            ttl_ms = request.ttl_ms,
            "ExecutorLeaseService::renew_lease"
        );

        let result = self
            .lease_manager
            .renew_lease(&request.lease_id, &request.scheduler_id, request.ttl_ms)
            .await;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "TTL is clamped to max 5 minutes, fits in u64"
        )]
        let response = match result {
            RenewalResult::Renewed { new_ttl } => RenewLeaseResponse {
                renewed: true,
                ttl_ms: new_ttl.as_millis() as u64,
                rejection_reason: String::new(),
            },
            RenewalResult::Failed { reason } => RenewLeaseResponse {
                renewed: false,
                ttl_ms: 0,
                rejection_reason: reason,
            },
        };

        Ok(Response::new(response))
    }

    async fn release_lease(
        &self,
        request: Request<ReleaseLeaseRequest>,
    ) -> Result<Response<ReleaseLeaseResponse>, Status> {
        let request = request.into_inner();

        if request.lease_id.is_empty() {
            return Err(Status::invalid_argument("lease_id is required"));
        }
        if request.scheduler_id.is_empty() {
            return Err(Status::invalid_argument("scheduler_id is required"));
        }

        tracing::trace!(
            lease_id = %request.lease_id,
            scheduler_id = %request.scheduler_id,
            "ExecutorLeaseService::release_lease"
        );

        let result = self
            .lease_manager
            .release_lease(&request.lease_id, &request.scheduler_id)
            .await;

        let response = match result {
            ReleaseResult::Released { slots_released } => ReleaseLeaseResponse {
                released: true,
                slots_released,
            },
            ReleaseResult::Failed { reason: _ } => ReleaseLeaseResponse {
                released: false,
                slots_released: 0,
            },
        };

        Ok(Response::new(response))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        tracing::trace!("ExecutorLeaseService::get_capacity");

        let capacity = self.lease_manager.get_capacity().await;

        Ok(Response::new(GetCapacityResponse {
            total_slots: capacity.total_slots,
            available_slots: capacity.available_slots,
            reserved_slots: capacity.reserved_slots,
            in_use_slots: capacity.in_use_slots,
        }))
    }
}
