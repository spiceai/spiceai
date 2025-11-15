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

use std::{sync::Arc, time::Duration};

use crate::{
    Runtime,
    component::dataset::{Dataset, builder::DatasetBuilder},
    dataaccelerator::{AccelerationSource, FilePathError, acceleration_file_path},
    datafusion::request_context_extension::get_current_datafusion,
    http::v1::datasets::MessageResponse,
};
use app::App;
use axum::{
    Extension, Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use runtime_acceleration::snapshot::{DatasetSnapshots, SnapshotManager, SnapshotMetadataError};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Serialize)]
struct DatasetSnapshotsResponse {
    dataset: String,
    location: String,
    last_updated_ms: Option<i64>,
    current_snapshot_id: Option<u64>,
    snapshots: Vec<SnapshotResponseItem>,
}

#[derive(Debug, Serialize)]
struct SnapshotResponseItem {
    snapshot_id: u64,
    timestamp_ms: i64,
    uri: String,
    size_bytes: u64,
    checksum: String,
    checksum_algorithm: String,
    is_current: bool,
}

impl From<DatasetSnapshots> for DatasetSnapshotsResponse {
    fn from(listing: DatasetSnapshots) -> Self {
        Self {
            dataset: listing.dataset,
            location: listing.location,
            last_updated_ms: listing.last_updated_ms,
            current_snapshot_id: listing.current_snapshot_id,
            snapshots: listing
                .snapshots
                .into_iter()
                .map(|snapshot| SnapshotResponseItem {
                    snapshot_id: snapshot.snapshot_id,
                    timestamp_ms: snapshot.timestamp_ms,
                    uri: snapshot.uri,
                    size_bytes: snapshot.size_bytes,
                    checksum: snapshot.checksum,
                    checksum_algorithm: snapshot.checksum_algorithm,
                    is_current: snapshot.is_current,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SetSnapshotHeadRequest {
    pub snapshot_id: u64,
}

pub(crate) async fn list(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
) -> Response {
    match list_inner(app, rt, dataset_name).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => err.into_response(),
    }
}

pub(crate) async fn create(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
) -> Response {
    match create_inner(app, rt, dataset_name).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

pub(crate) async fn set_head(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
    Json(payload): Json<SetSnapshotHeadRequest>,
) -> Response {
    match set_head_inner(app, rt, dataset_name, payload.snapshot_id).await {
        Ok(message) => message,
        Err(err) => err.into_response(),
    }
}

pub(crate) async fn delete(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path((dataset_name, snapshot_id)): Path<(String, u64)>,
) -> Response {
    match delete_inner(app, rt, dataset_name, snapshot_id).await {
        Ok(message) => message,
        Err(err) => err.into_response(),
    }
}

async fn list_inner(
    app: Arc<RwLock<Option<Arc<App>>>>,
    rt: Arc<Runtime>,
    dataset_name: String,
) -> Result<DatasetSnapshotsResponse, SnapshotHttpError> {
    let dataset = resolve_dataset(&app, &rt, &dataset_name).await?;
    let manager = build_snapshot_manager(&dataset).await?;
    let listing = manager
        .list_snapshots()
        .await
        .map_err(SnapshotHttpError::from)?;
    Ok(DatasetSnapshotsResponse::from(listing))
}

async fn create_inner(
    app: Arc<RwLock<Option<Arc<App>>>>,
    rt: Arc<Runtime>,
    dataset_name: String,
) -> Result<Response, SnapshotHttpError> {
    let dataset = resolve_dataset(&app, &rt, &dataset_name).await?;
    let manager = build_snapshot_manager(&dataset).await?;

    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);
    let provider = df
        .get_accelerated_table_provider(&dataset.name.to_string())
        .await
        .map_err(|err| SnapshotHttpError::bad_request(err.to_string()))?;

    let schema = provider.schema();
    manager
        .create_snapshot(&schema)
        .await
        .map_err(|err| SnapshotHttpError::internal(err.to_string()))?;

    let listing = manager
        .list_snapshots()
        .await
        .map_err(SnapshotHttpError::from)?;

    Ok((
        StatusCode::CREATED,
        Json(DatasetSnapshotsResponse::from(listing)),
    )
        .into_response())
}

async fn set_head_inner(
    app: Arc<RwLock<Option<Arc<App>>>>,
    rt: Arc<Runtime>,
    dataset_name: String,
    snapshot_id: u64,
) -> Result<Response, SnapshotHttpError> {
    let dataset = resolve_dataset(&app, &rt, &dataset_name).await?;
    let manager = build_snapshot_manager(&dataset).await?;

    manager
        .set_current_snapshot(snapshot_id)
        .await
        .map_err(SnapshotHttpError::from)?;

    Ok(success_message(
        StatusCode::OK,
        format!("Snapshot {snapshot_id} is now current for {dataset_name}"),
    ))
}

async fn delete_inner(
    app: Arc<RwLock<Option<Arc<App>>>>,
    rt: Arc<Runtime>,
    dataset_name: String,
    snapshot_id: u64,
) -> Result<Response, SnapshotHttpError> {
    let dataset = resolve_dataset(&app, &rt, &dataset_name).await?;
    let manager = build_snapshot_manager(&dataset).await?;

    manager
        .delete_snapshot(snapshot_id)
        .await
        .map_err(SnapshotHttpError::from)?;

    Ok(success_message(
        StatusCode::OK,
        format!("Deleted snapshot {snapshot_id} for {dataset_name}"),
    ))
}

async fn resolve_dataset(
    app: &Arc<RwLock<Option<Arc<App>>>>,
    rt: &Arc<Runtime>,
    dataset_name: &str,
) -> Result<Arc<Dataset>, SnapshotHttpError> {
    let readable_app = read_app(app).await?;
    let Some(spicepod_dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(dataset_name))
        .cloned()
    else {
        return Err(SnapshotHttpError::not_found(format!(
            "Dataset {dataset_name} not found"
        )));
    };

    let builder = DatasetBuilder::try_from(spicepod_dataset)
        .map_err(|err| SnapshotHttpError::internal(err.to_string()))?;

    let dataset = builder
        .with_app(Arc::clone(&readable_app))
        .with_runtime(Arc::clone(rt))
        .build()
        .map_err(|err| SnapshotHttpError::internal(err.to_string()))?;

    Ok(Arc::new(dataset))
}

async fn read_app(app: &Arc<RwLock<Option<Arc<App>>>>) -> Result<Arc<App>, SnapshotHttpError> {
    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(Duration::from_secs(5)) => {
            return Err(SnapshotHttpError::timeout());
        }
    };
    let Some(app) = app_lock.as_ref() else {
        return Err(SnapshotHttpError::internal(
            "Runtime app configuration unavailable".to_string(),
        ));
    };
    Ok(Arc::clone(app))
}

async fn build_snapshot_manager(
    dataset: &Arc<Dataset>,
) -> Result<SnapshotManager, SnapshotHttpError> {
    let acceleration = dataset
        .acceleration()
        .ok_or_else(|| SnapshotHttpError::bad_request("Acceleration is not enabled"))?;

    let path = acceleration_file_path(dataset.as_ref())
        .await
        .map_err(|err| map_file_path_error(&err))?;

    SnapshotManager::try_new(
        dataset.name.to_string(),
        acceleration.snapshots.clone(),
        path,
    )
    .await
    .ok_or_else(|| {
        SnapshotHttpError::bad_request(format!(
            "Snapshots are not enabled for dataset {}",
            dataset.name
        ))
    })
}

fn map_file_path_error(err: &FilePathError) -> SnapshotHttpError {
    SnapshotHttpError::bad_request(err.to_string())
}

fn success_message(status: StatusCode, message: String) -> Response {
    (status, Json(MessageResponse { message })).into_response()
}

enum SnapshotHttpError {
    Timeout,
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl SnapshotHttpError {
    fn timeout() -> Self {
        Self::Timeout
    }

    fn not_found(message: String) -> Self {
        Self::NotFound(message)
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    fn into_response(self) -> Response {
        match self {
            SnapshotHttpError::Timeout => (
                StatusCode::REQUEST_TIMEOUT,
                Json(MessageResponse {
                    message: "timeout".to_string(),
                }),
            )
                .into_response(),
            SnapshotHttpError::NotFound(message) => {
                (StatusCode::NOT_FOUND, Json(MessageResponse { message })).into_response()
            }
            SnapshotHttpError::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, Json(MessageResponse { message })).into_response()
            }
            SnapshotHttpError::Internal(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MessageResponse { message }),
            )
                .into_response(),
        }
    }
}

impl From<SnapshotMetadataError> for SnapshotHttpError {
    fn from(err: SnapshotMetadataError) -> Self {
        match err {
            SnapshotMetadataError::MetadataMissing { dataset } => Self::bad_request(format!(
                "Snapshot metadata is not initialized for dataset {dataset}"
            )),
            SnapshotMetadataError::MetadataSnapshotNotFound {
                dataset,
                snapshot_id,
            } => Self::not_found(format!(
                "Snapshot {snapshot_id} not found for dataset {dataset}"
            )),
            _ => Self::internal(err.to_string()),
        }
    }
}
