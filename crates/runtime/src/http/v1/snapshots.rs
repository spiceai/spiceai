/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! HTTP API endpoints for managing acceleration snapshots.
//!
//! Provides endpoints for:
//! - Listing all snapshots for a dataset
//! - Getting details of a specific snapshot
//! - Setting the current snapshot (for rollback operations)

use std::sync::Arc;

use crate::{
    LogErrors, Runtime, component::dataset::Dataset, dataaccelerator::acceleration_file_path,
};
use app::App;
use axum::{
    Extension, Json,
    extract::Path,
    http::status,
    response::{IntoResponse, Response},
};
use datafusion::sql::TableReference;
use runtime_acceleration::snapshot::{AccelerationEngine, SnapshotManager};
use runtime_acceleration::snapshot::{
    SnapshotInfo as SnapshotInfoInternal, SnapshotSummary as SnapshotSummaryInternal,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::datasets::MessageResponse;
use crate::component::dataset::acceleration::Engine;

/// Public snapshot information for API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SnapshotInfo {
    /// Unique identifier for this snapshot.
    pub snapshot_id: u64,
    /// Timestamp when the snapshot was created (milliseconds since epoch).
    pub timestamp_ms: i64,
    /// URI location of the snapshot file.
    pub location: String,
    /// SHA256 checksum of the snapshot file.
    pub checksum: String,
    /// Checksum algorithm used (e.g., "SHA256").
    pub checksum_algorithm: String,
    /// Size of the snapshot file in bytes.
    pub size_bytes: u64,
    /// Number of rows in the snapshot, if known.
    pub row_count: Option<u64>,
    /// Whether this is the current (active) snapshot used for bootstrapping.
    pub is_current: bool,
}

impl From<SnapshotInfoInternal> for SnapshotInfo {
    fn from(info: SnapshotInfoInternal) -> Self {
        Self {
            snapshot_id: info.snapshot_id,
            timestamp_ms: info.timestamp_ms,
            location: info.location,
            checksum: info.checksum,
            checksum_algorithm: info.checksum_algorithm,
            size_bytes: info.size_bytes,
            row_count: info.row_count,
            is_current: info.is_current,
        }
    }
}

/// Summary of all snapshots for a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SnapshotSummary {
    /// Name of the dataset.
    pub dataset_name: String,
    /// Base location URI for snapshots.
    pub location: String,
    /// Timestamp of the last metadata update (milliseconds since epoch).
    pub last_updated_ms: i64,
    /// ID of the current (active) snapshot, if any.
    pub current_snapshot_id: Option<u64>,
    /// List of all available snapshots.
    pub snapshots: Vec<SnapshotInfo>,
}

impl From<SnapshotSummaryInternal> for SnapshotSummary {
    fn from(summary: SnapshotSummaryInternal) -> Self {
        Self {
            dataset_name: summary.dataset_name,
            location: summary.location,
            last_updated_ms: summary.last_updated_ms,
            current_snapshot_id: summary.current_snapshot_id,
            snapshots: summary.snapshots.into_iter().map(Into::into).collect(),
        }
    }
}

/// Response for snapshot list endpoint.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SnapshotListResponse {
    /// Summary of all snapshots for the dataset.
    #[serde(flatten)]
    pub summary: SnapshotSummary,
}

/// Request to set the current snapshot.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SetCurrentSnapshotRequest {
    /// The snapshot ID to set as current.
    pub snapshot_id: u64,
}

/// List Snapshots
///
/// Returns all available snapshots for an accelerated dataset.
///
/// This endpoint lists all snapshots stored for a dataset that has snapshots enabled,
/// including their timestamps, sizes, checksums, and which one is currently active.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/datasets/{name}/acceleration/snapshots",
    operation_id = "list_dataset_snapshots",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset.")
    ),
    responses(
        (status = 200, description = "List of snapshots for the dataset", content((
            SnapshotListResponse = "application/json",
            example = json!({
                "dataset_name": "taxi_trips",
                "location": "s3://my-bucket/snapshots/",
                "last_updated_ms": 1_705_315_200_000_i64,
                "current_snapshot_id": 3,
                "snapshots": [
                    {
                        "snapshot_id": 1,
                        "timestamp_ms": 1_705_228_800_000_i64,
                        "location": "s3://my-bucket/snapshots/month=2025-01/day=2025-01-14/dataset=taxi_trips/taxi_trips_20250114T120000Z.db",
                        "checksum": "abc123def456",
                        "checksum_algorithm": "SHA256",
                        "size_bytes": 1_048_576,
                        "is_current": false
                    },
                    {
                        "snapshot_id": 3,
                        "timestamp_ms": 1_705_315_200_000_i64,
                        "location": "s3://my-bucket/snapshots/month=2025-01/day=2025-01-15/dataset=taxi_trips/taxi_trips_20250115T000000Z.db",
                        "checksum": "xyz789abc012",
                        "checksum_algorithm": "SHA256",
                        "size_bytes": 2_097_152,
                        "is_current": true
                    }
                ]
            })
        ))),
        (status = 400, description = "Snapshots are not enabled for this dataset", content((
            String, example = "Snapshots are not enabled for dataset taxi_trips"
        ))),
        (status = 404, description = "Dataset not found", content((
            String, example = "Dataset not found: taxi_trips"
        ))),
        (status = 500, description = "Internal server error", content((
            String, example = "Failed to list snapshots: error message"
        )))
    )
))]
pub(crate) async fn list(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
) -> Response {
    let dataset = match get_dataset_with_snapshots(&app, &rt, &dataset_name).await {
        Ok(ds) => ds,
        Err(resp) => return resp,
    };

    let manager = match create_snapshot_manager_for_dataset(&dataset).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            return (
                status::StatusCode::BAD_REQUEST,
                format!("Snapshots are not enabled for dataset {dataset_name}"),
            )
                .into_response();
        }
        Err(e) => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to initialize snapshot manager: {e}"),
            )
                .into_response();
        }
    };

    match manager.list_snapshots().await {
        Ok(summary) => (
            status::StatusCode::OK,
            Json(SnapshotListResponse {
                summary: summary.into(),
            }),
        )
            .into_response(),
        Err(e) => (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to list snapshots: {e}"),
        )
            .into_response(),
    }
}

/// Get Snapshot
///
/// Returns details about a specific snapshot for an accelerated dataset.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/datasets/{name}/acceleration/snapshots/{snapshot_id}",
    operation_id = "get_dataset_snapshot",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset."),
        ("snapshot_id" = u64, Path, description = "The snapshot ID.")
    ),
    responses(
        (status = 200, description = "Snapshot details", content((
            SnapshotInfo = "application/json",
            example = json!({
                "snapshot_id": 3,
                "timestamp_ms": 1_705_315_200_000_i64,
                "location": "s3://my-bucket/snapshots/month=2025-01/day=2025-01-15/dataset=taxi_trips/taxi_trips_20250115T000000Z.db",
                "checksum": "xyz789abc012",
                "checksum_algorithm": "SHA256",
                "size_bytes": 2_097_152,
                "is_current": true
            })
        ))),
        (status = 400, description = "Snapshots are not enabled for this dataset"),
        (status = 404, description = "Dataset or snapshot not found"),
        (status = 500, description = "Internal server error")
    )
))]
pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path((dataset_name, snapshot_id)): Path<(String, u64)>,
) -> Response {
    let dataset = match get_dataset_with_snapshots(&app, &rt, &dataset_name).await {
        Ok(ds) => ds,
        Err(resp) => return resp,
    };

    let manager = match create_snapshot_manager_for_dataset(&dataset).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            return (
                status::StatusCode::BAD_REQUEST,
                format!("Snapshots are not enabled for dataset {dataset_name}"),
            )
                .into_response();
        }
        Err(e) => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to initialize snapshot manager: {e}"),
            )
                .into_response();
        }
    };

    match manager.get_snapshot(snapshot_id).await {
        Ok(info) => (status::StatusCode::OK, Json(SnapshotInfo::from(info))).into_response(),
        Err(e) => (status::StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Set Current Snapshot
///
/// Sets the current snapshot pointer for an accelerated dataset.
/// This is used for rollback operations - the next time the runtime starts,
/// it will bootstrap from this snapshot instead of the latest one.
///
/// **Warning**: This operation only updates the metadata pointer. The runtime
/// must be restarted for the rollback to take effect.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/datasets/{name}/acceleration/snapshots/current",
    operation_id = "set_current_snapshot",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset.")
    ),
    request_body(
        description = "The snapshot ID to set as current",
        content((
            SetCurrentSnapshotRequest = "application/json",
            example = json!({
                "snapshot_id": 2
            })
        ))
    ),
    responses(
        (status = 200, description = "Current snapshot updated successfully", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Current snapshot set to 2 for dataset taxi_trips. Restart the runtime to bootstrap from this snapshot."
            })
        ))),
        (status = 400, description = "Snapshots are not enabled or invalid request"),
        (status = 404, description = "Dataset or snapshot not found"),
        (status = 500, description = "Internal server error")
    )
))]
pub(crate) async fn set_current(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
    Json(request): Json<SetCurrentSnapshotRequest>,
) -> Response {
    let dataset = match get_dataset_with_snapshots(&app, &rt, &dataset_name).await {
        Ok(ds) => ds,
        Err(resp) => return resp,
    };

    let manager = match create_snapshot_manager_for_dataset(&dataset).await {
        Ok(Some(m)) => m,
        Ok(None) => {
            return (
                status::StatusCode::BAD_REQUEST,
                format!("Snapshots are not enabled for dataset {dataset_name}"),
            )
                .into_response();
        }
        Err(e) => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to initialize snapshot manager: {e}"),
            )
                .into_response();
        }
    };

    match manager.set_current_snapshot(request.snapshot_id).await {
        Ok(()) => (
            status::StatusCode::OK,
            Json(MessageResponse {
                message: format!(
                    "Current snapshot set to {} for dataset {dataset_name}. Restart the runtime to bootstrap from this snapshot.",
                    request.snapshot_id
                ),
            }),
        )
            .into_response(),
        Err(e) => {
            let status_code = if e.to_string().contains("not found") {
                status::StatusCode::NOT_FOUND
            } else {
                status::StatusCode::INTERNAL_SERVER_ERROR
            };
            (status_code, e.to_string()).into_response()
        }
    }
}

/// Helper to get a dataset with snapshots enabled.
async fn get_dataset_with_snapshots(
    app: &Arc<RwLock<Option<Arc<App>>>>,
    rt: &Arc<Runtime>,
    dataset_name: &str,
) -> Result<Arc<Dataset>, Response> {
    let Ok(app_lock) = tokio::time::timeout(std::time::Duration::from_secs(5), app.read()).await
    else {
        return Err((status::StatusCode::REQUEST_TIMEOUT, "timeout".to_string()).into_response());
    };

    let Some(readable_app) = app_lock.as_ref() else {
        return Err((
            status::StatusCode::INTERNAL_SERVER_ERROR,
            "App not initialized".to_string(),
        )
            .into_response());
    };

    // Parse dataset name as table reference
    let table_ref = match TableReference::parse_str(dataset_name) {
        table_ref if table_ref.table().is_empty() => {
            return Err((
                status::StatusCode::BAD_REQUEST,
                format!("Invalid dataset name: {dataset_name}"),
            )
                .into_response());
        }
        table_ref => table_ref,
    };

    // Find the dataset in the app
    let valid_datasets = Arc::clone(rt).get_valid_datasets(readable_app, LogErrors(false));
    let dataset = valid_datasets
        .into_iter()
        .find(|d| d.name == table_ref)
        .ok_or_else(|| {
            (
                status::StatusCode::NOT_FOUND,
                format!("Dataset not found: {dataset_name}"),
            )
                .into_response()
        })?;

    // Check if acceleration is enabled
    if dataset.acceleration.is_none() {
        return Err((
            status::StatusCode::BAD_REQUEST,
            format!("Acceleration is not enabled for dataset {dataset_name}"),
        )
            .into_response());
    }

    Ok(dataset)
}

/// Creates a snapshot manager for a dataset on-demand.
async fn create_snapshot_manager_for_dataset(
    dataset: &Dataset,
) -> Result<Option<SnapshotManager>, String> {
    let Some(acceleration) = &dataset.acceleration else {
        return Ok(None);
    };

    if !acceleration.snapshot_behavior.bootstrap_enabled()
        && !acceleration.snapshot_behavior.create_enabled()
    {
        return Ok(None);
    }

    let acceleration_engine = match acceleration.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => AccelerationEngine::DuckDB,
        #[cfg(feature = "duckdb")]
        Engine::TableModePartitionedDuckDB => AccelerationEngine::DuckDB,
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => AccelerationEngine::Sqlite,
        #[cfg(feature = "turso")]
        Engine::Turso => AccelerationEngine::Turso,
        _ => {
            return Ok(None);
        }
    };

    let snapshot_path = acceleration_file_path(dataset)
        .await
        .map_err(|e| format!("Failed to get acceleration file path: {e}"))?;

    Ok(SnapshotManager::try_new(
        dataset.name.to_string(),
        acceleration.snapshot_behavior.clone(),
        snapshot_path,
        acceleration_engine,
    )
    .await)
}
