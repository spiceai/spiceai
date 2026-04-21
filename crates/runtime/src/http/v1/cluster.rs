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

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use tokio::sync::RwLock;

use app::App;

use crate::{LogErrors, Runtime};

use super::dataset_status;

use crate::datafusion::request_context_extension::get_current_datafusion;
use runtime_request_context::{AsyncMarker, RequestContext};

#[derive(Debug, Serialize)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub role: String,
    pub status: String,
    pub datasets_count: usize,
    pub partitions_count: usize,
}

#[derive(Debug, Serialize)]
pub struct ClusterNodesResponse {
    pub scheduler: ClusterNodeInfo,
    pub executors: Vec<ClusterNodeInfo>,
}

pub(crate) async fn get_nodes(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    let Some(executor_registry) = rt.executor_registry() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Cluster endpoints are only available on scheduler nodes",
        )
            .into_response();
    };

    let executor_ids = executor_registry.connected_executors().await;
    let partitions_map = executor_registry.partitions.read().await;

    let executors: Vec<ClusterNodeInfo> = executor_ids
        .iter()
        .map(|id| {
            let partition_count: usize = partitions_map
                .get(id)
                .map(|tp| tp.values().map(Vec::len).sum::<usize>())
                .unwrap_or(0);
            let dataset_count = partitions_map
                .get(id)
                .map(|tp| tp.len())
                .unwrap_or(0);
            ClusterNodeInfo {
                id: id.clone(),
                role: "executor".to_string(),
                status: "Connected".to_string(),
                datasets_count: dataset_count,
                partitions_count: partition_count,
            }
        })
        .collect();

    let scheduler = ClusterNodeInfo {
        id: rt
            .config()
            .cluster
            .node_advertise_address
            .clone()
            .unwrap_or_else(|| rt.config().cluster.node_bind_address.to_string()),
        role: "scheduler".to_string(),
        status: "Ready".to_string(),
        datasets_count: 0,
        partitions_count: 0,
    };

    (
        StatusCode::OK,
        Json(ClusterNodesResponse {
            scheduler,
            executors,
        }),
    )
        .into_response()
}

#[derive(Debug, Serialize)]
pub struct ClusterDatasetInfo {
    pub name: String,
    pub from: String,
    pub acceleration_enabled: bool,
    pub status: String,
}

pub(crate) async fn get_datasets(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
) -> Response {
    if rt.executor_registry().is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Cluster endpoints are only available on scheduler nodes",
        )
            .into_response();
    }

    let app_lock = app.read().await;
    let Some(readable_app) = app_lock.as_ref() else {
        return (StatusCode::OK, Json::<Vec<ClusterDatasetInfo>>(vec![])).into_response();
    };

    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let valid_datasets = Arc::clone(&rt).get_valid_datasets(readable_app, LogErrors(false));

    let datasets: Vec<ClusterDatasetInfo> = valid_datasets
        .iter()
        .map(|d| {
            let status = dataset_status(&df, d);
            ClusterDatasetInfo {
                name: d.name.to_quoted_string(),
                from: d.from.clone(),
                acceleration_enabled: d.acceleration.as_ref().is_some_and(|a| a.enabled),
                status: format!("{status}"),
            }
        })
        .collect();

    (StatusCode::OK, Json(datasets)).into_response()
}

#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition_value: std::collections::HashMap<String, String>,
    pub assigned_executors: Vec<String>,
    pub last_assigned_at: Option<u128>,
}

#[derive(Debug, Serialize)]
pub struct DatasetPartitionsResponse {
    pub dataset: String,
    pub partition_expressions: Vec<String>,
    pub partitions: Vec<PartitionInfo>,
}

pub(crate) async fn get_dataset_partitions(
    Extension(rt): Extension<Arc<Runtime>>,
    Path(name): Path<String>,
) -> Response {
    let Some(partition_manager) = rt.partition_manager() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Cluster endpoints are only available on scheduler nodes",
        )
            .into_response();
    };

    let table_ref = datafusion::sql::TableReference::bare(name.clone());

    match partition_manager.get_table_metadata(&table_ref).await {
        Ok(Some(metadata)) => {
            let partitions: Vec<PartitionInfo> = metadata
                .partitions
                .iter()
                .map(|p| PartitionInfo {
                    partition_value: p.partition_value.clone(),
                    assigned_executors: p.assigned_executors.clone(),
                    last_assigned_at: p.last_assigned_at,
                })
                .collect();

            let resp = DatasetPartitionsResponse {
                dataset: name,
                partition_expressions: metadata.partition_expressions,
                partitions,
            };

            (StatusCode::OK, Json(resp)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("No partition metadata found for dataset '{name}'"),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to get partition metadata: {e}"),
        )
            .into_response(),
    }
}

/// Returns names of all tables that have partition metadata.
pub(crate) async fn get_partitioned_tables(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    let Some(partition_manager) = rt.partition_manager() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Cluster endpoints are only available on scheduler nodes",
        )
            .into_response();
    };

    match partition_manager.list_tables().await {
        Ok(tables) => (StatusCode::OK, Json(tables)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to list partitioned tables: {e}"),
        )
            .into_response(),
    }
}
