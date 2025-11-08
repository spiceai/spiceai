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
    LogErrors, Runtime, accelerated_table::refresh::RefreshOverrides, component::dataset::Dataset,
    datafusion::request_context_extension::get_current_datafusion, status::ComponentStatus,
};
use app::App;
use axum::{
    Extension, Json,
    extract::Path,
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
};
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use runtime_request_context::{AsyncMarker, RequestContext};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;

use super::{Format, convert_entry_to_csv, dataset_status};

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

#[allow(dead_code)]
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
    Extension(rt): Extension<Arc<Runtime>>,
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

    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let valid_datasets = rt.get_valid_datasets(readable_app, LogErrors(false));
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
            RefreshOverrides = "application/json",
            example = json!({
                "refresh_sql": "SELECT * FROM taxi_trips WHERE tip_amount > 10.0",
                "refresh_mode": "full",
                "refresh_jitter_max": "10s"
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

    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

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
    }

    match df
        .refresh_table(
            &TableReference::parse_str(dataset.name.as_str()),
            overrides_opt.map(|Json(overrides)| overrides),
        )
        .await
    {
        Ok(_) => (
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

    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

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
    #[cfg(feature = "models")]
    properties.insert(
        "search".to_string(),
        if ds.has_embeddings() || ds.has_full_text_column() {
            Value::String("supported".to_string())
        } else {
            Value::String("unsupported".to_string())
        },
    );

    properties
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DatasetGetRequest {
    /// Parameters to pass to the data connector
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DatasetGetResponse {
    /// The retrieved data
    pub data: Vec<HashMap<String, serde_json::Value>>,
    
    /// Whether the data was served from cache
    pub cached: bool,
    
    /// The SQL query used for cache key
    pub cache_key_sql: Option<String>,
}

/// Dataset Get
///
/// Read-through HTTP proxy-cache for datasets. Fetches data from the data connector,
/// caches it using Spice's LRU cache, and optionally inserts into acceleration.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/datasets/{name}/get",
    operation_id = "post_dataset_get",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "The name of the dataset to query.")
    ),
    request_body(
        description = "Parameters to pass to the data connector",
        content((
            DatasetGetRequest = "application/json",
            example = json!({
                "params": {
                    "user_id": 123,
                    "status": "active"
                }
            })
        ))
    ),
    responses(
        (status = 200, description = "Data retrieved successfully", content((
            DatasetGetResponse = "application/json",
            example = json!({
                "data": [
                    {"id": 1, "name": "John", "status": "active"},
                    {"id": 2, "name": "Jane", "status": "active"}
                ],
                "cached": false,
                "cache_key_sql": "SELECT * FROM users WHERE user_id = 123 AND status = 'active'"
            })
        ))),
        (status = 404, description = "Dataset not found", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Dataset users not found"
            })
        ))),
        (status = 500, description = "Internal server error occurred", content((
            MessageResponse = "application/json",
            example = json!({
                "message": "Failed to fetch data from connector"
            })
        )))
    )
))]
pub(crate) async fn dataset_get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(dataset_name): Path<String>,
    Json(payload): Json<DatasetGetRequest>,
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

    // Generate SQL from params for cache key
    let cache_key_sql = match generate_sql_from_params(&dataset.name, &payload.params) {
        Ok(sql) => sql,
        Err(e) => {
            return (
                status::StatusCode::BAD_REQUEST,
                Json(MessageResponse {
                    message: format!("Failed to generate SQL from params: {e}"),
                }),
            )
                .into_response();
        }
    };

    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    // Execute the query through DataFusion (which handles caching)
    match df
        .query_builder(&cache_key_sql)
        .build()
        .run()
        .await
    {
        Ok(query_result) => {
            let cache_status = query_result.cache_status;
            
            // Collect the stream into record batches
            let batches = match query_result.data.try_collect::<Vec<_>>().await {
                Ok(b) => b,
                Err(e) => {
                    return (
                        status::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(MessageResponse {
                            message: format!("Failed to collect query results: {e}"),
                        }),
                    )
                        .into_response();
                }
            };
            
            // Convert RecordBatches to JSON
            let data = match record_batches_to_json(&batches) {
                Ok(d) => d,
                Err(e) => {
                    return (
                        status::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(MessageResponse {
                            message: format!("Failed to convert results to JSON: {e}"),
                        }),
                    )
                        .into_response();
                }
            };

            (
                status::StatusCode::OK,
                Json(DatasetGetResponse {
                    data,
                    cached: matches!(cache_status, cache::result::CacheStatus::CacheHit),
                    cache_key_sql: Some(cache_key_sql),
                }),
            )
                .into_response()
        }
        Err(e) => (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json(MessageResponse {
                message: format!("Failed to execute query: {e}"),
            }),
        )
            .into_response(),
    }
}

fn generate_sql_from_params(
    table_name: &str,
    params: &HashMap<String, serde_json::Value>,
) -> Result<String, String> {
    if params.is_empty() {
        return Ok(format!("SELECT * FROM {table_name}"));
    }

    let mut where_clauses = Vec::new();
    for (key, value) in params {
        let sql_value = match value {
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            _ => return Err(format!("Unsupported parameter type for key: {key}")),
        };
        where_clauses.push(format!("{key} = {sql_value}"));
    }

    Ok(format!(
        "SELECT * FROM {table_name} WHERE {}",
        where_clauses.join(" AND ")
    ))
}

fn record_batches_to_json(
    batches: &[arrow::array::RecordBatch],
) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let mut result = Vec::new();
    
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row_map = HashMap::new();
            
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = arrow_value_to_json(column, row_idx)?;
                row_map.insert(field.name().clone(), value);
            }
            
            result.push(row_map);
        }
    }
    
    Ok(result)
}

fn arrow_value_to_json(
    array: &dyn arrow::array::Array,
    row_idx: usize,
) -> Result<serde_json::Value, String> {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    
    if array.is_null(row_idx) {
        return Ok(serde_json::Value::Null);
    }
    
    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Number(arr.value(row_idx).into()))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or("downcast failed")?;
            serde_json::Number::from_f64(f64::from(arr.value(row_idx)))
                .map(serde_json::Value::Number)
                .ok_or_else(|| "invalid float".to_string())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or("downcast failed")?;
            serde_json::Number::from_f64(arr.value(row_idx))
                .map(serde_json::Value::Number)
                .ok_or_else(|| "invalid float".to_string())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or("downcast failed")?;
            Ok(serde_json::Value::Bool(arr.value(row_idx)))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or("downcast failed")?;
            Ok(serde_json::Value::String(arr.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().ok_or("downcast failed")?;
            Ok(serde_json::Value::String(arr.value(row_idx).to_string()))
        }
        _ => Err(format!("Unsupported data type: {:?}", array.data_type())),
    }
}
