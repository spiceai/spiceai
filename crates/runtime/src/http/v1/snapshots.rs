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

//! HTTP endpoints for managing acceleration snapshots.

use std::sync::Arc;

use app::App;
use axum::{
    Extension, Json,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
};
#[cfg(not(feature = "snapshots"))]
use runtime_acceleration::snapshot::SNAPSHOTS_ENTERPRISE_ONLY_MESSAGE;
use runtime_acceleration::snapshot::{SnapshotApiError, SnapshotBehavior, SnapshotManager, api};
use serde::{Deserialize, Serialize};
use spicepod::component::snapshot::Snapshots;
use tokio::sync::RwLock;

use crate::Runtime;
use crate::component::dataset::snapshot_source::SnapshotSource;

use super::require_write_access;

const DEFAULT_SNAPSHOTS_LIMIT: usize = 10;

#[cfg(feature = "snapshots")]
fn snapshots_feature_response() -> Option<Response> {
    None
}

#[cfg(not(feature = "snapshots"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "mirrors the snapshots-on impl which returns Option"
)]
fn snapshots_feature_response() -> Option<Response> {
    Some(
        (
            StatusCode::NOT_IMPLEMENTED,
            Json(MessageResponse {
                message: SNAPSHOTS_ENTERPRISE_ONLY_MESSAGE.to_string(),
            }),
        )
            .into_response(),
    )
}

#[derive(Debug, Deserialize)]
pub struct ListSnapshotsQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageResponse {
    pub message: String,
}

/// List all snapshots for a dataset.
///
/// `GET /v1/datasets/{name}/acceleration/snapshots`
pub async fn list_snapshots(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
    Query(query): Query<ListSnapshotsQuery>,
) -> Response {
    if let Some(resp) = snapshots_feature_response() {
        return resp;
    }
    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return (StatusCode::REQUEST_TIMEOUT, "timeout").into_response();
        }
    };

    let Some(readable_app) = &*app_lock else {
        return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
    };

    let Some(dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.to_lowercase() == dataset_name.to_lowercase())
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} not found"),
            }),
        )
            .into_response();
    };

    if let Some(manager) = snapshot_source_manager(dataset, &rt).await {
        let snapshot_manager = match manager {
            Ok(manager) => manager,
            Err(response) => return response,
        };
        drop(app_lock);
        let limit = query.limit.unwrap_or(DEFAULT_SNAPSHOTS_LIMIT);
        return match snapshot_manager.get_snapshot_summary(limit).await {
            Ok(summary) => (StatusCode::OK, Json(summary)).into_response(),
            Err(e) => snapshot_api_error_to_response(&e),
        };
    }

    let Some(acceleration) = &dataset.acceleration else {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    };

    if !acceleration.enabled {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    }

    // Create a snapshot manager to query the metadata
    let Some(snapshot_manager) = create_snapshot_manager_for_query(
        &dataset_name,
        readable_app.snapshots.clone(),
        acceleration,
        rt.secrets_weak(),
        rt.tokio_io_runtime(),
    )
    .await
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have snapshots configured"),
            }),
        )
            .into_response();
    };

    // Need to drop the app lock before the async call
    drop(app_lock);

    let limit = query.limit.unwrap_or(DEFAULT_SNAPSHOTS_LIMIT);
    match snapshot_manager.get_snapshot_summary(limit).await {
        Ok(summary) => (StatusCode::OK, Json(summary)).into_response(),
        Err(e) => snapshot_api_error_to_response(&e),
    }
}

/// Get details of a specific snapshot.
///
/// `GET /v1/datasets/{name}/acceleration/snapshots/{snapshot_id}`
pub async fn get_snapshot(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path((dataset_name, snapshot_id)): Path<(String, u64)>,
) -> Response {
    if let Some(resp) = snapshots_feature_response() {
        return resp;
    }
    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return (StatusCode::REQUEST_TIMEOUT, "timeout").into_response();
        }
    };

    let Some(readable_app) = &*app_lock else {
        return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
    };

    let Some(dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.to_lowercase() == dataset_name.to_lowercase())
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} not found"),
            }),
        )
            .into_response();
    };

    if let Some(manager) = snapshot_source_manager(dataset, &rt).await {
        let snapshot_manager = match manager {
            Ok(manager) => manager,
            Err(response) => return response,
        };
        drop(app_lock);
        return match snapshot_manager.get_snapshot(snapshot_id).await {
            Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
            Err(e) => snapshot_api_error_to_response(&e),
        };
    }

    let Some(acceleration) = &dataset.acceleration else {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    };

    if !acceleration.enabled {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    }

    let Some(snapshot_manager) = create_snapshot_manager_for_query(
        &dataset_name,
        readable_app.snapshots.clone(),
        acceleration,
        rt.secrets_weak(),
        rt.tokio_io_runtime(),
    )
    .await
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have snapshots configured"),
            }),
        )
            .into_response();
    };

    // Need to drop the app lock before the async call
    drop(app_lock);

    match snapshot_manager.get_snapshot(snapshot_id).await {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(e) => snapshot_api_error_to_response(&e),
    }
}

/// Set the current snapshot for a dataset.
///
/// `POST /v1/datasets/{name}/acceleration/snapshots/current`
pub async fn set_current_snapshot(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
    Json(request): Json<api::SetCurrentSnapshotRequest>,
) -> Response {
    if let Some(resp) = snapshots_feature_response() {
        return resp;
    }
    if let Some(response) = require_write_access().await {
        return response;
    }

    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return (StatusCode::REQUEST_TIMEOUT, "timeout").into_response();
        }
    };

    let Some(readable_app) = &*app_lock else {
        return (StatusCode::INTERNAL_SERVER_ERROR).into_response();
    };

    let Some(dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.to_lowercase() == dataset_name.to_lowercase())
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} not found"),
            }),
        )
            .into_response();
    };

    // A dataset reading snapshots never changes them: its current snapshot is the one
    // the publishing instance names in the metadata this dataset reads.
    if SnapshotSource::from_spicepod(dataset).is_ok_and(|source| source.is_some()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!(
                    "Dataset {dataset_name} reads snapshots (`file_format: snapshot`) and never changes them. Set the current snapshot on the Spice instance that publishes them."
                ),
            }),
        )
            .into_response();
    }

    let Some(acceleration) = &dataset.acceleration else {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    };

    if !acceleration.enabled {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    }

    let Some(snapshot_manager) = create_snapshot_manager_for_query(
        &dataset_name,
        readable_app.snapshots.clone(),
        acceleration,
        rt.secrets_weak(),
        rt.tokio_io_runtime(),
    )
    .await
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have snapshots configured"),
            }),
        )
            .into_response();
    };

    // Need to drop the app lock before the async call
    drop(app_lock);

    match snapshot_manager.set_current_snapshot(request.snapshot_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(MessageResponse {
                message: format!(
                    "Current snapshot for dataset {} set to {}. Restart the runtime to bootstrap from this snapshot.",
                    dataset_name, request.snapshot_id
                ),
            }),
        )
            .into_response(),
        Err(e) => snapshot_api_error_to_response(&e),
    }
}

/// A `SnapshotManager` over the snapshots `dataset` reads, when it reads snapshots
/// (`file_format: snapshot`): its own `from` location and `s3_*` params, not the
/// top-level `snapshots` section, which says where snapshotting datasets publish.
/// `None` for any other dataset.
async fn snapshot_source_manager(
    dataset: &spicepod::component::dataset::Dataset,
    rt: &Runtime,
) -> Option<Result<SnapshotManager, Response>> {
    let bad_request = |message: String| {
        (StatusCode::BAD_REQUEST, Json(MessageResponse { message })).into_response()
    };
    let source = match SnapshotSource::from_spicepod(dataset) {
        Ok(None) => return None,
        Ok(Some(source)) => source,
        Err(err) => return Some(Err(bad_request(err.to_string()))),
    };
    let params = dataset
        .params
        .as_ref()
        .map(spicepod::param::Params::as_string_map)
        .unwrap_or_default();
    let behavior = SnapshotBehavior::bootstrap_only(
        Arc::new(source.snapshots(&params)),
        rt.secrets_weak(),
        rt.tokio_io_runtime(),
    );
    Some(
        SnapshotManager::try_new_for_metadata_queries(dataset.name.clone(), behavior)
            .await
            .ok_or_else(|| {
                bad_request(format!(
                    "Dataset {} reads snapshots from '{}', which could not be opened. Check the dataset's `s3_*` params.",
                    dataset.name,
                    source.location()
                ))
            }),
    )
}

/// Creates a `SnapshotManager` for querying snapshot metadata.
///
/// This creates a lightweight manager suitable for reading metadata. It doesn't
/// require a full accelerator setup since we only need access to the object store.
async fn create_snapshot_manager_for_query(
    dataset_name: &str,
    app_snapshots: Option<Arc<Snapshots>>,
    acceleration: &spicepod::acceleration::Acceleration,
    secrets: std::sync::Weak<RwLock<runtime_secrets::Secrets>>,
    io_runtime: tokio::runtime::Handle,
) -> Option<SnapshotManager> {
    // Create the snapshot behavior using the app's global snapshots config
    // and the dataset's per-acceleration snapshot settings
    let snapshot_behavior = SnapshotBehavior::from(
        app_snapshots,
        acceleration.snapshots,
        secrets,
        io_runtime,
        acceleration.snapshots_compaction,
    );

    // If snapshots are disabled, return None
    if matches!(snapshot_behavior, SnapshotBehavior::Disabled) {
        return None;
    }

    // Use the metadata-only constructor that doesn't require an enabled adapter.
    // This avoids the issue where SnapshotAdapter::None would cause try_new()
    // to always return None due to the adapter.is_enabled() check.
    SnapshotManager::try_new_for_metadata_queries(dataset_name.to_string(), snapshot_behavior).await
}

fn snapshot_api_error_to_response(error: &SnapshotApiError) -> Response {
    match error {
        SnapshotApiError::SnapshotNotFound { .. } => (
            StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: error.to_string(),
            }),
        )
            .into_response(),
        SnapshotApiError::ReadMetadata { .. }
        | SnapshotApiError::ParseMetadata { .. }
        | SnapshotApiError::UnsupportedVersion { .. }
        | SnapshotApiError::WriteMetadata { .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(MessageResponse {
                message: error.to_string(),
            }),
        )
            .into_response(),
    }
}

#[cfg(all(test, not(feature = "snapshots")))]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn assert_enterprise_only_response(response: Response) {
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("Failed to collect snapshots response body")
            .to_bytes();
        let body: MessageResponse =
            serde_json::from_slice(&body_bytes).expect("Failed to deserialize message response");

        assert_eq!(body.message, SNAPSHOTS_ENTERPRISE_ONLY_MESSAGE);
    }

    #[tokio::test]
    async fn snapshot_handlers_return_enterprise_only_response_without_feature() {
        let app = Arc::new(RwLock::new(None));
        let runtime = Arc::new(Runtime::builder().build().await);

        assert_enterprise_only_response(
            list_snapshots(
                Extension(Arc::clone(&app)),
                Extension(Arc::clone(&runtime)),
                Path("test_dataset".to_string()),
                Query(ListSnapshotsQuery { limit: None }),
            )
            .await,
        )
        .await;

        assert_enterprise_only_response(
            get_snapshot(
                Extension(Arc::clone(&app)),
                Extension(Arc::clone(&runtime)),
                Path(("test_dataset".to_string(), 1)),
            )
            .await,
        )
        .await;

        assert_enterprise_only_response(
            set_current_snapshot(
                Extension(app),
                Extension(runtime),
                Path("test_dataset".to_string()),
                Json(api::SetCurrentSnapshotRequest { snapshot_id: 1 }),
            )
            .await,
        )
        .await;
    }
}
