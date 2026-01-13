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
use runtime_proto::{
    ExpandSecretRequest, ExpandSecretResponse, GetAppDefinitionRequest, GetAppDefinitionResponse,
    GetClusterStateRequest, GetClusterStateResponse, SchedulerInstance,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::cluster::SchedulerPeers;
use crate::cluster::scheduler_registry::SpicepodGeneration;

/// Internal cluster service for scheduler-executor communication.
pub struct ClusterServiceImpl {
    app: Arc<RwLock<Option<Arc<App>>>>,
    secrets: Arc<RwLock<Secrets>>,
    advertise_address: String,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    /// Whether this scheduler is outdated (newer generation exists in cluster).
    outdated: Arc<AtomicBool>,
    /// Current spicepod generation state.
    current_generation: Arc<RwLock<SpicepodGeneration>>,
}

impl ClusterServiceImpl {
    /// Creates a new cluster service implementation.
    #[must_use]
    pub fn new(
        app: Arc<RwLock<Option<Arc<App>>>>,
        secrets: Arc<RwLock<Secrets>>,
        advertise_address: String,
        scheduler_peers: Arc<RwLock<SchedulerPeers>>,
        outdated: Arc<AtomicBool>,
        current_generation: Arc<RwLock<SpicepodGeneration>>,
    ) -> Self {
        Self {
            app,
            secrets,
            advertise_address,
            scheduler_peers,
            outdated,
            current_generation,
        }
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

        // If this scheduler is outdated, refuse to serve app definitions
        if self.outdated.load(Ordering::Relaxed) {
            tracing::warn!(
                "Refusing GetAppDefinition for executor {} - scheduler is outdated",
                request.executor_id
            );
            return Err(Status::unavailable(
                "Scheduler has outdated spicepod configuration. Please retry with another scheduler.",
            ));
        }

        let app_guard = self.app.read().await;
        let Some(ref app) = *app_guard else {
            return Err(Status::internal("App context not available"));
        };

        let app_json = serde_json::to_string(app.as_ref())
            .map_err(|e| Status::internal(format!("Failed to serialize app: {e}")))?;

        // Get current generation info
        let gen_state = self.current_generation.read().await;
        let spicepod_generation = gen_state.generation;
        let spicepod_content_hash = gen_state.content_hash.clone();
        drop(gen_state);

        Ok(Response::new(GetAppDefinitionResponse {
            app_json,
            spicepod_generation,
            spicepod_content_hash,
        }))
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

    async fn get_cluster_state(
        &self,
        _request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        tracing::debug!("ClusterService::get_cluster_state request");

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

        // Get current generation info
        let gen_state = self.current_generation.read().await;
        let spicepod_generation = gen_state.generation;
        let spicepod_content_hash = gen_state.content_hash.clone();
        drop(gen_state);

        tracing::debug!(
            "ClusterService::get_cluster_state response schedulers=[{scheduler_addresses}], generation={spicepod_generation}"
        );

        Ok(Response::new(GetClusterStateResponse {
            schedulers,
            spicepod_generation,
            spicepod_content_hash,
        }))
    }
}
