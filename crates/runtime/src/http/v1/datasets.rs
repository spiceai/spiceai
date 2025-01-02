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
use std::{collections::HashMap, sync::Arc};

use crate::{
    accelerated_table::refresh::RefreshOverrides, component::dataset::Dataset,
    datafusion::DataFusion, status::ComponentStatus, LogErrors, Runtime,
};
use app::App;
use axum::{
    extract::Path,
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};
use datafusion::sql::TableReference;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;

use super::{convert_entry_to_csv, dataset_status, Format};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams, utoipa::ToSchema))]
pub struct DatasetFilter {
    /// Filters datasets by source (e.g., `postgres:aidemo_messages`).
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema, utoipa::IntoParams))]
pub struct DatasetQueryParams {
    #[serde(default)]
    status: bool,

    /// The format of the response. Possible values are 'json' (default) or 'csv'.
    #[serde(default)]
    format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct DatasetResponseItem {
    /// The source where the dataset is located
    pub from: String,

    /// The name of the dataset
    pub name: String,

    /// Whether replication is enabled for the dataset
    pub replication_enabled: bool,

    /// Whether acceleration is enabled for the dataset
    pub acceleration_enabled: bool,

    /// Optional status of the dataset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ComponentStatus>,

    /// Custom properties for the dataset
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub properties: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct Property {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>, // support any valid JSON type (String, Int, Object, etc)
}

/// List Datasets
///
/// This endpoint returns a list of configured datasets. The response can be formatted as **JSON** or **CSV**,
/// and additional filters can be applied using query parameters.

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/datasets",
    operation_id = "get_datasets",
    tag = "Datasets",
    params(DatasetQueryParams, DatasetFilter),
    responses(
        (status = 200, description = "List of datasets", content((
            DatasetResponseItem = "application/json",
            example = json!([
                {
                    "from": "postgres:syncs",
                    "name": "daily_journal_accelerated",
                    "replication_enabled": false,
                    "acceleration_enabled": true
                },
                {
                    "from": "databricks:hive_metastore.default.messages",
                    "name": "messages_accelerated",
                    "replication_enabled": false,
                    "acceleration_enabled": true
                },
                {
                    "from": "postgres:aidemo_messages",
                    "name": "general",
                    "replication_enabled": false,
                    "acceleration_enabled": false
                }
            ])
        ), (
            String = "text/csv",
            example = "
from,name,replication_enabled,acceleration_enabled
postgres:syncs,daily_journal_accelerated,false,true
databricks:hive_metastore.default.messages,messages_accelerated,false,true
postgres:aidemo_messages,general,false,false
"
        ))),
        (status = 500, description = "Internal server error occurred while processing datasets", content((
            String, example = "An unexpected error occurred while processing datasets"
        )))
    )
))]
pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Query(filter): Query<DatasetFilter>,
    Query(params): Query<DatasetQueryParams>,
) -> Response {
    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return (
                status::StatusCode::REQUEST_TIMEOUT,
                "timeout".to_string()
            ).into_response();
        }
    };
    let Some(readable_app) = app_lock.as_ref() else {
        return (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json::<Vec<DatasetResponseItem>>(vec![]),
        )
            .into_response();
    };

    let valid_datasets = Runtime::get_valid_datasets(readable_app, LogErrors(false));
    let datasets: Vec<Arc<Dataset>> = match filter.source {
        Some(source) => valid_datasets
            .into_iter()
            .filter(|d| d.source() == source)
            .collect(),
        None => valid_datasets,
    };

    let resp: Vec<_> = datasets
        .iter()
        .map(|d| DatasetResponseItem {
            from: d.from.clone(),
            name: d.name.to_quoted_string(),
            replication_enabled: d.replication.as_ref().is_some_and(|f| f.enabled),
            acceleration_enabled: d.acceleration.as_ref().is_some_and(|f| f.enabled),
            properties: dataset_properties(d),
            status: if params.status {
                Some(dataset_status(&df, d))
            } else {
                None
            },
        })
        .collect();

    match params.format {
        Format::Json => (status::StatusCode::OK, Json(resp)).into_response(),
        Format::Csv => match convert_entry_to_csv(&resp) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub(crate) struct MessageResponse {
    /// The message describing the result of the request
    pub message: String,
}

#[derive(Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AccelerationRequest {
    /// SQL statement used for the refresh. Defaults to the `refresh_sql` specified in the spicepod.
    pub refresh_sql: Option<String>,
}

/// Refresh Dataset
///
/// Trigger an on-demand refresh for an accelerated dataset.
///
/// This endpoint triggers an on-demand refresh for an accelerated dataset.
/// The refresh only applies to `full` and `append` refresh modes (not `changes` mode).
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/datasets/{name}/acceleration/refresh",
    operation_id = "post_dataset_refresh",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset to refresh.")
    ),
    request_body(
        description = "On-demand refresh request for a specific dataset.",
        content((
            AccelerationRequest = "application/json",
            example = json!({
                "refresh_sql": "SELECT * FROM taxi_trips WHERE tip_amount > 10.0"
            })
        ))
    ),
    responses(
        (status = 201, description = "Dataset refresh triggered successfully", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset refresh triggered for taxi_trips."
            })
        ))),
        (status = 404, description = "Dataset not found", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset taxi_trips not found"
            })
        ))),
        (status = 400, description = "Acceleration not enabled for the dataset", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset taxi_trips does not have acceleration enabled"
            })
        ))),
        (status = 500, description = "Internal server error occurred while processing refresh", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Unexpected internal error occurred while processing refresh"
            })
        )))
    )
))]
pub(crate) async fn refresh(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Path(dataset_name): Path<String>,
    overrides_opt: Option<Json<RefreshOverrides>>,
    // When this is an Option<Json>, Json rejections are silenced
    // This means malformed Json, etc, will simply return None
    // To get around this, we would need to implement a custom extractor
) -> Response {
    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return (
                status::StatusCode::REQUEST_TIMEOUT,
                "timeout".to_string()
            ).into_response();
        }
    };
    let Some(readable_app) = &*app_lock else {
        return (status::StatusCode::INTERNAL_SERVER_ERROR).into_response();
    };

    let Some(dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.to_lowercase() == dataset_name.to_lowercase())
    else {
        return (
            status::StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} not found"),
            }),
        )
            .into_response();
    };

    let acceleration_enabled = dataset.acceleration.as_ref().is_some_and(|f| f.enabled);

    if !acceleration_enabled {
        return (
            status::StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    };

    match df
        .refresh_table(
            &TableReference::parse_str(dataset.name.as_str()),
            overrides_opt.map(|Json(overrides)| overrides),
        )
        .await
    {
        Ok(()) => (
            status::StatusCode::CREATED,
            Json(MessageResponse {
                message: format!("Dataset refresh triggered for {dataset_name}."),
            }),
        )
            .into_response(),
        Err(err) => (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json(MessageResponse {
                message: format!("{err}"),
            }),
        )
            .into_response(),
    }
}

/// Update Refresh SQL
///
/// Update the refresh SQL for a dataset's acceleration.
///
/// This endpoint allows for updating the `refresh_sql` parameter for a dataset's acceleration at runtime.
/// The change is **temporary** and will revert to the `spicepod.yml` definition at the next runtime restart.
///

#[cfg_attr(feature = "openapi", utoipa::path(
    patch,
    path = "/v1/datasets/{name}/acceleration",
    operation_id = "patch_dataset_acceleration",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset to update.")
    ),
    request_body(
        description = "The updated SQL statement for the dataset's refresh.",
        content((
            AccelerationRequest = "application/json",
            example = json!({
                "refresh_sql": "SELECT * FROM eth_recent_blocks WHERE block_number > 100"
            })
        ))
    ),
    responses(
        (status = 200, description = "The refresh SQL was updated successfully."),
        (status = 404, description = "The specified dataset was not found", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset eth_recent_blocks not found"
            })
        ))),
        (status = 500, description = "An internal server error occurred while updating the refresh SQL", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Request failed. An internal server error occurred while updating refresh SQL."
            })
        )))
    )
))]
pub(crate) async fn acceleration(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Path(dataset_name): Path<String>,
    Json(payload): Json<AccelerationRequest>,
) -> Response {
    let app_lock = tokio::select! {
        lock = app.read() => lock,
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return (
                status::StatusCode::REQUEST_TIMEOUT,
                "timeout".to_string()
            ).into_response();
        }
    };
    let Some(readable_app) = &*app_lock else {
        return (status::StatusCode::INTERNAL_SERVER_ERROR).into_response();
    };

    let Some(dataset) = readable_app
        .datasets
        .iter()
        .find(|d| d.name.to_lowercase() == dataset_name.to_lowercase())
    else {
        return (
            status::StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} not found"),
            }),
        )
            .into_response();
    };

    if payload.refresh_sql.is_none() {
        return (status::StatusCode::OK).into_response();
    }

    match df
        .update_refresh_sql(
            TableReference::parse_str(&dataset.name),
            payload.refresh_sql,
        )
        .await
    {
        Ok(()) => (status::StatusCode::OK).into_response(),
        Err(e) => (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json(MessageResponse {
                message: format!("Request failed. {e}"),
            }),
        )
            .into_response(),
    }
}

fn dataset_properties(ds: &Dataset) -> HashMap<String, Value> {
    let mut properties = HashMap::new();

    #[cfg(feature = "models")]
    properties.insert(
        "vector_search".to_string(),
        if ds.has_embeddings() {
            Value::String("supported".to_string())
        } else {
            Value::String("unsupported".to_string())
        },
    );

    properties
}
