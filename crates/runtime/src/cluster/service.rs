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

use super::lease::LeaseManager;
use app::App;
use runtime_proto::cluster_service_server::ClusterService;
use runtime_proto::executor_lease_service_server::ExecutorLeaseService;
use runtime_proto::{
    ExpandSecretRequest, ExpandSecretResponse, GetAppDefinitionRequest, GetAppDefinitionResponse,
    GetCapacityRequest, GetCapacityResponse, ReleaseLeaseRequest, ReleaseLeaseResponse,
    RenewLeaseRequest, RenewLeaseResponse, ReserveSlotsRequest, ReserveSlotsResponse,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

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

/// Executor lease service for task slot reservation.
///
/// This service exposes the [`LeaseManager`] functionality via gRPC,
/// allowing schedulers to reserve task slots before dispatching work.
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

        tracing::debug!(
            request_id = %request.request_id,
            scheduler_id = %request.scheduler_id,
            slots = request.slots,
            lease_duration_ms = request.lease_duration_ms,
            "ExecutorLeaseService::reserve_slots"
        );

        let lease_duration = Duration::from_millis(request.lease_duration_ms);
        let result = self
            .lease_manager
            .reserve_slots(
                &request.request_id,
                &request.scheduler_id,
                request.slots,
                lease_duration,
            )
            .await;

        Ok(Response::new(ReserveSlotsResponse {
            lease_id: result.lease_id,
            slots_reserved: result.slots_reserved,
            expires_at_ms: result.expires_at_ms,
        }))
    }

    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> Result<Response<RenewLeaseResponse>, Status> {
        let request = request.into_inner();

        tracing::debug!(
            lease_id = %request.lease_id,
            scheduler_id = %request.scheduler_id,
            lease_duration_ms = request.lease_duration_ms,
            "ExecutorLeaseService::renew_lease"
        );

        let lease_duration = Duration::from_millis(request.lease_duration_ms);
        let expires_at_ms = self
            .lease_manager
            .renew_lease(&request.lease_id, &request.scheduler_id, lease_duration)
            .await
            .map_err(Status::invalid_argument)?;

        Ok(Response::new(RenewLeaseResponse { expires_at_ms }))
    }

    async fn release_lease(
        &self,
        request: Request<ReleaseLeaseRequest>,
    ) -> Result<Response<ReleaseLeaseResponse>, Status> {
        let request = request.into_inner();

        tracing::debug!(
            lease_id = %request.lease_id,
            scheduler_id = %request.scheduler_id,
            "ExecutorLeaseService::release_lease"
        );

        let slots_released = self
            .lease_manager
            .release_lease(&request.lease_id, &request.scheduler_id)
            .await
            .map_err(Status::invalid_argument)?;

        Ok(Response::new(ReleaseLeaseResponse { slots_released }))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        let (total_slots, available_slots, leased_slots, in_use_slots) =
            self.lease_manager.get_capacity().await;

        tracing::trace!(
            total_slots = total_slots,
            available_slots = available_slots,
            leased_slots = leased_slots,
            in_use_slots = in_use_slots,
            "ExecutorLeaseService::get_capacity"
        );

        Ok(Response::new(GetCapacityResponse {
            total_slots,
            available_slots,
            leased_slots,
            in_use_slots,
        }))
    }
}
