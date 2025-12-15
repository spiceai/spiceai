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

use crate::datafusion::DataFusion;
use app::App;
use runtime_proto::cluster_service_server::ClusterService;
use runtime_proto::{
    ExpandSecretRequest, ExpandSecretResponse, GetAppDefinitionRequest, GetAppDefinitionResponse,
};
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Internal cluster service for scheduler-executor communication.
pub struct ClusterServiceImpl {
    app: Arc<RwLock<Option<Arc<App>>>>,
    datafusion: Arc<DataFusion>,
    secrets: Arc<RwLock<Secrets>>,
}

impl ClusterServiceImpl {
    /// Creates a new cluster service implementation.
    #[must_use]
    pub fn new(
        app: Arc<RwLock<Option<Arc<App>>>>,
        datafusion: Arc<DataFusion>,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Self {
        Self {
            app,
            datafusion,
            secrets,
        }
    }

    /// Checks if executor is a part of the Ballista cluster.
    async fn check_executor_registration(&self, executor_id: &str) -> Result<(), Status> {
        let scheduler = {
            let Some(maybe_scheduler) = self.datafusion.scheduler_server.read().ok() else {
                return Err(Status::internal("Cluster scheduler context cannot be read"));
            };

            let Some(ref scheduler) = *maybe_scheduler else {
                return Err(Status::internal("Cluster scheduler context not available"));
            };

            Arc::clone(scheduler)
        };

        let executor_state = scheduler
            .state
            .executor_manager
            .get_executor_state()
            .await
            .map_err(|e| Status::internal(format!("Failed to get executor state: {e}")))?;

        let executors = executor_state
            .into_iter()
            .map(|(e, _)| e.id)
            .collect::<HashSet<_>>();

        if executors.contains(executor_id) {
            Ok(())
        } else {
            Err(Status::invalid_argument(format!(
                "Executor {executor_id} is not a part of the cluster"
            )))
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

        self.check_executor_registration(&request.executor_id)
            .await?;

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

        self.check_executor_registration(&request.executor_id)
            .await?;

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
