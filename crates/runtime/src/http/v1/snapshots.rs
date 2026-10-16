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
use runtime_acceleration::snapshot::{
    SnapshotApiError, SnapshotBehavior, SnapshotManager, Unpinned, api,
};
use serde::Deserialize;
use spicepod::component::snapshot::Snapshots;
use tokio::sync::RwLock;

use crate::Runtime;
use crate::component::dataset::snapshot_source::SnapshotSource;

use super::datasets::MessageResponse;
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
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub struct ListSnapshotsQuery {
    /// The number of most recent snapshots to return. Defaults to 10.
    pub limit: Option<usize>,
}

/// List Dataset Snapshots
///
/// List the most recent acceleration snapshots of a dataset, with the snapshot that
/// instances load and the snapshot the dataset is pinned to.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/datasets/{name}/acceleration/snapshots",
    operation_id = "get_dataset_snapshots",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset."),
        ListSnapshotsQuery
    ),
    responses(
        (status = 200, description = "The dataset's snapshots", content((
            api::SnapshotSummary = "application/json",
            example = json!({
                "dataset_name": "taxi_trips",
                "location": "s3://my-bucket/spice/snapshots",
                "last_updated_ms": 1_792_147_320_000_i64,
                "engine": "duckdb",
                "current_snapshot_id": 41,
                "pinned_snapshot_id": 41,
                "snapshots": [{
                    "snapshot_id": 41,
                    "timestamp_ms": 1_792_139_520_000_i64,
                    "location": "s3://my-bucket/spice/snapshots/month=2026-10/day=2026-10-16/dataset=taxi_trips/taxi_trips_20261016T084520Z_0199f2b4d1e87c2a9d3e5f60718293a4.duckdb",
                    "checksum": "9f2c4e1b7a5d3c8e6f0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60",
                    "checksum_algorithm": "SHA256",
                    "size_bytes": 52_428_800,
                    "engine": "duckdb",
                    "row_count": 1_198_004,
                    "is_current": true,
                    "status": "verified"
                }]
            })
        ))),
        (status = 404, description = "Dataset not found", content((
            MessageResponse = "application/json",
            example = json!({ "message": "Dataset taxi_trips not found" })
        ))),
        (status = 400, description = "The dataset has no snapshots configured", content((
            MessageResponse = "application/json",
            example = json!({ "message": "Dataset taxi_trips does not have snapshots configured" })
        )))
    )
))]
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
        readable_app.dataset_snapshots(acceleration),
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

/// Get Dataset Snapshot
///
/// Get one acceleration snapshot of a dataset.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/datasets/{name}/acceleration/snapshots/{snapshot_id}",
    operation_id = "get_dataset_snapshot",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset."),
        ("snapshot_id" = u64, Path, description = "The id of the snapshot.")
    ),
    responses(
        (status = 200, description = "The snapshot", content((
            api::SnapshotInfo = "application/json"
        ))),
        (status = 404, description = "Dataset or snapshot not found", content((
            MessageResponse = "application/json",
            example = json!({ "message": "Snapshot 7 not found for dataset taxi_trips" })
        ))),
        (status = 400, description = "The dataset has no snapshots configured", content((
            MessageResponse = "application/json",
            example = json!({ "message": "Dataset taxi_trips does not have snapshots configured" })
        )))
    )
))]
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
        readable_app.dataset_snapshots(acceleration),
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

/// Pin Dataset Snapshot
///
/// Pin an accelerated dataset to a snapshot. Every Spice instance that uses the dataset
/// serves the pinned snapshot: readers load it at their next check, and instances that
/// refresh the dataset from its source serve it from a second local copy at their next
/// refresh while they keep refreshing and publishing. Newer snapshots are published but
/// not served until the dataset is unpinned. A pin while pinned replaces the pin.
#[cfg_attr(feature = "openapi", utoipa::path(
    put,
    path = "/v1/datasets/{name}/acceleration/snapshots/pin",
    operation_id = "put_dataset_snapshot_pin",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset.")
    ),
    request_body(
        description = "The snapshot to pin the dataset to.",
        content((
            api::PinSnapshotRequest = "application/json",
            example = json!({ "snapshot_id": 41 })
        ))
    ),
    responses(
        (status = 200, description = "The dataset is pinned", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset taxi_trips is pinned to snapshot 41. Every instance serves it once loaded; readers load it at their next check. Newer snapshots are published but not served until you unpin with DELETE /v1/datasets/taxi_trips/acceleration/snapshots/pin."
            })
        ))),
        (status = 404, description = "Dataset or snapshot not found", content((
            MessageResponse = "application/json",
            example = json!({ "message": "Snapshot 41 not found for dataset taxi_trips" })
        ))),
        (status = 409, description = "The snapshot's file is gone from the snapshot location", content((
            MessageResponse = "application/json"
        ))),
        (status = 400, description = "The dataset reads snapshots, or has no snapshots configured", content((
            MessageResponse = "application/json"
        )))
    )
))]
pub async fn pin_snapshot(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
    Json(request): Json<api::PinSnapshotRequest>,
) -> Response {
    let snapshot_manager = match writable_snapshot_manager(&app, &rt, &dataset_name).await {
        Ok(manager) => manager,
        Err(response) => return *response,
    };
    match snapshot_manager
        .set_current_snapshot(request.snapshot_id)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(MessageResponse {
                message: pinned_message(&dataset_name, request.snapshot_id),
            }),
        )
            .into_response(),
        Err(e) => snapshot_api_error_to_response(&e),
    }
}

/// Unpin Dataset Snapshot
///
/// Unpin an accelerated dataset: its newest snapshot becomes current, and every Spice
/// instance serves the newest data again. Deletes no snapshot. Unpinning a dataset that
/// is not pinned changes nothing.
#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/v1/datasets/{name}/acceleration/snapshots/pin",
    operation_id = "delete_dataset_snapshot_pin",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset.")
    ),
    responses(
        (status = 200, description = "The dataset is not pinned", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset taxi_trips is no longer pinned. Serving snapshot 44 (published 2026-10-16 10:42 UTC, 1,204,331 rows)."
            })
        ))),
        (status = 404, description = "Dataset not found, or it has no snapshots", content((
            MessageResponse = "application/json",
            example = json!({ "message": "Dataset taxi_trips not found" })
        ))),
        (status = 400, description = "The dataset reads snapshots, or has no snapshots configured", content((
            MessageResponse = "application/json"
        )))
    )
))]
pub async fn unpin_snapshot(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
) -> Response {
    let snapshot_manager = match writable_snapshot_manager(&app, &rt, &dataset_name).await {
        Ok(manager) => manager,
        Err(response) => return *response,
    };
    match snapshot_manager.unpin_current_snapshot().await {
        Ok(unpinned) => (
            StatusCode::OK,
            Json(MessageResponse {
                message: unpinned_message(&dataset_name, &unpinned),
            }),
        )
            .into_response(),
        Err(e) => snapshot_api_error_to_response(&e),
    }
}

fn pinned_message(dataset_name: &str, snapshot_id: u64) -> String {
    format!(
        "Dataset {dataset_name} is pinned to snapshot {snapshot_id}. Every instance serves it once loaded; readers load it at their next check. Newer snapshots are published but not served until you unpin with DELETE /v1/datasets/{dataset_name}/acceleration/snapshots/pin."
    )
}

fn unpinned_message(dataset_name: &str, unpinned: &Unpinned) -> String {
    let state = if unpinned.was_pinned {
        "is no longer pinned"
    } else {
        "is not pinned"
    };
    let published = chrono::DateTime::from_timestamp_millis(unpinned.published_ms).map_or_else(
        || unpinned.published_ms.to_string(),
        |at| at.format("%Y-%m-%d %H:%M UTC").to_string(),
    );
    let rows = match unpinned.row_count {
        Some(1) => ", 1 row".to_string(),
        Some(rows) => format!(", {} rows", group_thousands(rows)),
        None => String::new(),
    };
    format!(
        "Dataset {dataset_name} {state}. Serving snapshot {} (published {published}{rows}).",
        unpinned.current_snapshot_id
    )
}

/// `1204331` as `1,204,331`.
fn group_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// The `SnapshotManager` that changes which snapshot of `dataset_name` is current, or
/// the response refusing the request.
async fn writable_snapshot_manager(
    app: &RwLock<Option<Arc<App>>>,
    rt: &Runtime,
    dataset_name: &str,
) -> Result<SnapshotManager, Box<Response>> {
    if let Some(resp) = snapshots_feature_response() {
        return Err(Box::new(resp));
    }
    if let Some(response) = require_write_access().await {
        return Err(Box::new(response));
    }

    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return Err(Box::new((StatusCode::REQUEST_TIMEOUT, "timeout").into_response()));
        }
    };

    let Some(readable_app) = &*app_lock else {
        return Err(Box::new(
            (StatusCode::INTERNAL_SERVER_ERROR).into_response(),
        ));
    };

    let bad_request = |message: String| {
        Box::new((StatusCode::BAD_REQUEST, Json(MessageResponse { message })).into_response())
    };

    let Some(dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.to_lowercase() == dataset_name.to_lowercase())
    else {
        return Err(Box::new(
            (
                StatusCode::NOT_FOUND,
                Json(MessageResponse {
                    message: format!("Dataset {dataset_name} not found"),
                }),
            )
                .into_response(),
        ));
    };

    // A dataset reading snapshots never changes them: its current snapshot is the one
    // the publishing instance names in the metadata this dataset reads.
    if SnapshotSource::from_spicepod(dataset).is_ok_and(|source| source.is_some()) {
        return Err(bad_request(format!(
            "Dataset {dataset_name} reads snapshots (`file_format: snapshot`) and never changes them. Pin or unpin it on the Spice instance that publishes them."
        )));
    }

    let Some(acceleration) = dataset.acceleration.as_ref().filter(|a| a.enabled) else {
        return Err(bad_request(format!(
            "Dataset {dataset_name} does not have acceleration enabled"
        )));
    };

    create_snapshot_manager_for_query(
        dataset_name,
        readable_app.dataset_snapshots(acceleration),
        acceleration,
        rt.secrets_weak(),
        rt.tokio_io_runtime(),
    )
    .await
    .ok_or_else(|| {
        bad_request(format!(
            "Dataset {dataset_name} does not have snapshots configured"
        ))
    })
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
    snapshots: Arc<Snapshots>,
    acceleration: &spicepod::acceleration::Acceleration,
    secrets: std::sync::Weak<RwLock<runtime_secrets::Secrets>>,
    io_runtime: tokio::runtime::Handle,
) -> Option<SnapshotManager> {
    // Create the snapshot behavior using the dataset's resolved snapshot config
    // and its per-acceleration snapshot settings
    let snapshot_behavior = SnapshotBehavior::from(
        Some(snapshots),
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
        SnapshotApiError::SnapshotNotFound { .. } | SnapshotApiError::NoSnapshots { .. } => (
            StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: error.to_string(),
            }),
        )
            .into_response(),
        SnapshotApiError::SnapshotFileMissing { .. } => (
            StatusCode::CONFLICT,
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
            pin_snapshot(
                Extension(Arc::clone(&app)),
                Extension(Arc::clone(&runtime)),
                Path("test_dataset".to_string()),
                Json(api::PinSnapshotRequest { snapshot_id: 1 }),
            )
            .await,
        )
        .await;

        assert_enterprise_only_response(
            unpin_snapshot(
                Extension(app),
                Extension(runtime),
                Path("test_dataset".to_string()),
            )
            .await,
        )
        .await;
    }
}

#[cfg(test)]
mod reply_tests {
    use super::*;

    /// 2026-10-16 10:42 UTC.
    const PUBLISHED_MS: i64 = 1_792_147_320_000;

    #[test]
    fn the_pin_reply_names_the_snapshot_and_how_to_unpin() {
        assert_eq!(
            pinned_message("taxi_trips", 41),
            "Dataset taxi_trips is pinned to snapshot 41. Every instance serves it once loaded; readers load it at their next check. Newer snapshots are published but not served until you unpin with DELETE /v1/datasets/taxi_trips/acceleration/snapshots/pin."
        );
    }

    #[test]
    fn the_unpin_reply_names_the_snapshot_served_with_its_publish_time_and_rows() {
        let unpinned = Unpinned {
            was_pinned: true,
            current_snapshot_id: 44,
            published_ms: PUBLISHED_MS,
            row_count: Some(1_204_331),
        };
        assert_eq!(
            unpinned_message("taxi_trips", &unpinned),
            "Dataset taxi_trips is no longer pinned. Serving snapshot 44 (published 2026-10-16 10:42 UTC, 1,204,331 rows)."
        );
    }

    #[test]
    fn the_unpin_reply_leaves_out_a_row_count_the_snapshot_does_not_record() {
        let unpinned = Unpinned {
            was_pinned: false,
            current_snapshot_id: 44,
            published_ms: PUBLISHED_MS,
            row_count: None,
        };
        assert_eq!(
            unpinned_message("taxi_trips", &unpinned),
            "Dataset taxi_trips is not pinned. Serving snapshot 44 (published 2026-10-16 10:42 UTC)."
        );
    }

    #[test]
    fn row_counts_are_grouped_in_thousands() {
        assert_eq!(group_thousands(0), "0");
        assert_eq!(group_thousands(999), "999");
        assert_eq!(group_thousands(1_000), "1,000");
        assert_eq!(group_thousands(1_204_331), "1,204,331");
    }
}
