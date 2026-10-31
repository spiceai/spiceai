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
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    LogErrors, Runtime,
    accelerated::refresh::RefreshOverrides,
    component::dataset::{Dataset, snapshot_source},
    datafusion::{
        DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA,
        request_context_extension::get_current_datafusion,
    },
};
use app::App;
use axum::{
    Extension, Json,
    extract::Path,
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
};
use datafusion::common::TableReference;
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
    /// Whether to include the status field in the response. When `true`, the response includes
    /// the current status of each dataset (e.g., `ready`, `initializing`, `refreshing`, `error`).
    /// Defaults to `false`.
    #[serde(default)]
    status: bool,

    /// The format of the response. Possible values are 'json' (default) or 'csv'.
    #[serde(default)]
    format: Format,
}

// Re-export the shared type for backwards compatibility
pub use runtime_api_types::v1::DatasetInfo as DatasetResponseItem;

/// List Datasets
///
/// This endpoint returns a list of configured datasets. The response can be formatted as **JSON** or **CSV**,
/// and additional filters can be applied using query parameters.
///
/// Use `status=true` query parameter to include the current status of each dataset in the response.
/// Possible status values: `initializing`, `ready`, `disabled`, `error`, `refreshing`, `shuttingdown`.
/// When `status=true` and a dataset is in `Error`, the response also includes:
/// - `error`: structured code object with `category`, `type`, and stable `code`
/// - `error_message`: user-visible details
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/datasets",
    operation_id = "get_datasets",
    tag = "Datasets",
    params(DatasetQueryParams, DatasetFilter),
    responses(
        (status = 200, description = "List of datasets. When `status=true` is specified, each dataset includes `status` and error metadata (`error`, `error_message`) when applicable.", content((
            DatasetResponseItem = "application/json",
            example = json!([
                {
                    "from": "postgres:syncs",
                    "name": "daily_journal_accelerated",
                    "replication_enabled": false,
                    "acceleration_enabled": true,
                    "status": "Ready",
                    "error": null,
                    "error_message": null
                },
                {
                    "from": "databricks:hive_metastore.default.messages",
                    "name": "messages_accelerated",
                    "replication_enabled": false,
                    "acceleration_enabled": true,
                    "status": "Error",
                    "error": {
                        "category": "dataset",
                        "type": "auth",
                        "code": "dataset.auth"
                    },
                    "error_message": "Unable to authenticate with datasource credentials"
                },
                {
                    "from": "postgres:aidemo_messages",
                    "name": "general",
                    "replication_enabled": false,
                    "acceleration_enabled": false,
                    "status": "Initializing",
                    "error": null,
                    "error_message": null
                }
            ])
        ), (
            String = "text/csv",
            example = "
from,name,replication_enabled,acceleration_enabled,status,error,error_message,last_refresh,next_refresh,properties.search,properties.vector_search
postgres:syncs,daily_journal_accelerated,false,true,Ready,,,,,unsupported,unsupported
databricks:hive_metastore.default.messages,messages_accelerated,false,true,Error,dataset.auth,Unable to authenticate with datasource credentials,,,unsupported,unsupported
postgres:aidemo_messages,general,false,false,Initializing,,,,,unsupported,unsupported
"
        ))),
        (status = 500, description = "Internal server error occurred while processing datasets", content((
            String, example = "Failed to format the dataset list as CSV"
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

    let valid_datasets = Arc::clone(&rt).get_valid_datasets(readable_app, LogErrors(false));
    let datasets: Vec<Arc<Dataset>> = match filter.source {
        Some(source) => valid_datasets
            .into_iter()
            .filter(|d| d.source() == source)
            .collect(),
        None => valid_datasets,
    };

    let resp = dataset_infos(&df, &datasets, params.status);

    match params.format {
        Format::Json => (status::StatusCode::OK, Json(resp)).into_response(),
        Format::Csv => match datasets_to_csv(&resp) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => datasets_csv_error_response(e),
        },
    }
}

/// Body returned when `GET /v1/datasets?format=csv` cannot be encoded. The
/// `rust-csv` crate error names its `GitHub` issue tracker; that text is not a
/// user-facing message.
const DATASETS_CSV_ERROR_BODY: &str = "Failed to format the dataset list as CSV";

/// Flat CSV row for `GET /v1/datasets?format=csv`.
///
/// `csv::Writer::serialize` rejects `HashMap` fields and expands nested structs
/// into extra columns, so [`DatasetResponseItem`] cannot be written directly.
#[derive(Debug, Serialize)]
struct DatasetCsvRow {
    from: String,
    name: String,
    replication_enabled: bool,
    acceleration_enabled: bool,
    status: Option<String>,
    error: Option<String>,
    error_message: Option<String>,
    last_refresh: Option<String>,
    next_refresh: Option<String>,
    #[serde(rename = "properties.search")]
    properties_search: Option<String>,
    #[serde(rename = "properties.vector_search")]
    properties_vector_search: Option<String>,
}

impl From<&DatasetResponseItem> for DatasetCsvRow {
    fn from(item: &DatasetResponseItem) -> Self {
        Self {
            from: item.from.clone(),
            name: item.name.clone(),
            replication_enabled: item.replication_enabled,
            acceleration_enabled: item.acceleration_enabled,
            status: item.status.as_ref().map(ToString::to_string),
            error: item.error.as_ref().map(|error| error.code.clone()),
            error_message: item.error_message.clone(),
            last_refresh: item.last_refresh.clone(),
            next_refresh: item.next_refresh.clone(),
            properties_search: json_value_cell(item.properties.get("search")),
            properties_vector_search: json_value_cell(item.properties.get("vector_search")),
        }
    }
}

fn json_value_cell(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    }
}

fn datasets_to_csv(datasets: &[DatasetResponseItem]) -> Result<String, Box<dyn std::error::Error>> {
    let rows: Vec<DatasetCsvRow> = datasets.iter().map(DatasetCsvRow::from).collect();
    convert_entry_to_csv(&rows)
}

fn datasets_csv_error_response(error: impl std::fmt::Display) -> Response {
    tracing::error!(
        "Failed to format the dataset list as CSV, so `GET /v1/datasets?format=csv` cannot be answered. Cause: {error}"
    );
    (
        status::StatusCode::INTERNAL_SERVER_ERROR,
        DATASETS_CSV_ERROR_BODY.to_string(),
    )
        .into_response()
}

/// The `GET /v1/datasets?status=true` document for the app `rt` serves: every
/// valid dataset with its status.
///
/// Answers the Cloud Connect `GetDatasets` command, so the control plane reads
/// the same rows a local operator does. No loaded app is an empty list, not an
/// error: there is nothing to list.
pub async fn dataset_infos_with_status(rt: &Arc<Runtime>) -> Vec<DatasetResponseItem> {
    let Some(app) = rt.read_app().await else {
        return Vec::new();
    };
    let datasets = Arc::clone(rt).get_valid_datasets(&app, LogErrors(false));
    dataset_infos(&rt.datafusion(), &datasets, true)
}

fn dataset_infos(
    df: &DataFusion,
    datasets: &[Arc<Dataset>],
    include_status: bool,
) -> Vec<DatasetResponseItem> {
    let datasets_by_name = if include_status {
        let mut by_name = HashMap::with_capacity(datasets.len());
        for dataset in datasets {
            by_name
                .entry(dataset.name.clone())
                .or_insert(dataset.as_ref());
        }
        by_name
    } else {
        HashMap::new()
    };
    datasets
        .iter()
        .map(|d| {
            let status = if include_status {
                Some(dataset_status(df, d))
            } else {
                None
            };
            let error = status.as_ref().and_then(|s| {
                if s.is_error() {
                    Some(runtime_api_types::v1::ComponentError::from_status_message(
                        runtime_api_types::v1::ComponentErrorCategory::Dataset,
                        s.error_message(),
                    ))
                } else {
                    None
                }
            });
            let error_message = status
                .as_ref()
                .and_then(|s| s.error_message().map(String::from));
            let (last_refresh, next_refresh) = if include_status {
                dataset_freshness(df, d, &datasets_by_name)
            } else {
                (None, None)
            };
            DatasetResponseItem {
                from: d.from.clone(),
                name: d.name.to_quoted_string(),
                replication_enabled: d.replication.as_ref().is_some_and(|f| f.enabled),
                acceleration_enabled: d.acceleration.as_ref().is_some_and(|f| f.enabled),
                properties: dataset_properties(d),
                status,
                error,
                error_message,
                last_refresh,
                next_refresh,
            }
        })
        .collect()
}

/// `ds`'s last refresh and next scheduled refresh, as RFC 3339 timestamps, for an
/// accelerated dataset, following its active parent scheduler when synchronized.
/// Only a dataset with a schedule has a next refresh. For a
/// `refresh_cron` dataset it is the next cron time after now, or, once a refresh
/// has been triggered, its recorded due time including jitter until completion.
fn dataset_freshness(
    df: &DataFusion,
    ds: &Dataset,
    datasets: &HashMap<TableReference, &Dataset>,
) -> (Option<String>, Option<String>) {
    let Some(_) = ds.acceleration.as_ref().filter(|a| a.enabled) else {
        return (None, None);
    };
    let freshness = df.runtime_status().dataset_freshness(&ds.name);
    let next_refresh = df
        .runtime_status()
        .dataset_refresh_source(&ds.name)
        .and_then(|source| {
            let source_dataset = datasets.get(&source)?;
            let acceleration = source_dataset.acceleration.as_ref().filter(|a| a.enabled)?;
            let scheduled = acceleration.refresh_check_interval.is_some()
                || acceleration.refresh_cron.is_some();
            df.runtime_status()
                .dataset_freshness(&source)
                .next_refresh
                .filter(|_| scheduled)
                .or_else(|| {
                    let cron = acceleration.refresh_cron.as_deref()?;
                    scheduler::channel::cron::next_cron_time(cron, std::time::SystemTime::now())
                        .ok()
                })
        });
    (
        freshness.last_refresh.map(rfc3339),
        next_refresh.map(rfc3339),
    )
}

fn rfc3339(time: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
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
    /// SQL statement used for the refresh. Defaults to current `refresh_sql` configured (either from the spicepod or a previous `refresh_sql` update).
    pub refresh_sql: Option<String>,
}

/// Refresh Dataset
///
/// Trigger an on-demand refresh for an accelerated dataset.
///
/// The refresh only applies to `full` and `append` refresh modes (not
/// `changes` mode). Datasets without acceleration return 400 — the
/// previous `load: on_demand` flow has been removed; deferred datasets
/// (declared schema + `ready_state: on_registration`) are materialised
/// automatically on first query and do not need a manual trigger.
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
        (status = 400, description = "Dataset is not accelerated, or the overrides are invalid: a `refresh_mode` other than `full` or `append`, or a `refresh_sql` that does not parse or omits the dataset's `time_column`", content((
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

    // Resolve the requested name to a full table reference so that both
    // bare names (e.g. "wiki") and fully-qualified names (e.g. "spice.public.wiki") match.
    let requested_ref = TableReference::parse_str(&dataset_name)
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

    // Search both datasets and accelerated views for the given name. A dataset that reads
    // snapshots (`file_format: snapshot`) is accelerated whether or not it writes an
    // `acceleration` block, and a refresh checks for a newer snapshot.
    let (name, acceleration, reads_snapshots) = if let Some(dataset) =
        readable_app.datasets.iter().find(|d| {
            TableReference::parse_str(&d.name).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                == requested_ref
        }) {
        let reads_snapshots = dataset
            .params
            .as_ref()
            .is_some_and(|params| snapshot_source::is_snapshot_format(&params.as_string_map()));
        (
            dataset.name.as_str(),
            &dataset.acceleration,
            reads_snapshots,
        )
    } else if let Some(view) = readable_app.views.iter().find(|v| {
        TableReference::parse_str(&v.name).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
            == requested_ref
    }) {
        (view.name.as_str(), &view.acceleration, false)
    } else {
        return (
            status::StatusCode::NOT_FOUND,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} not found"),
            }),
        )
            .into_response();
    };

    let acceleration_enabled = reads_snapshots || acceleration.as_ref().is_some_and(|f| f.enabled);

    if !acceleration_enabled {
        return (
            status::StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("Dataset {dataset_name} does not have acceleration enabled"),
            }),
        )
            .into_response();
    }

    let table_ref = TableReference::parse_str(name);

    match df
        .refresh_table(&table_ref, overrides_opt.map(|Json(overrides)| overrides))
        .await
    {
        Ok(_) => (
            status::StatusCode::CREATED,
            Json(MessageResponse {
                message: format!("Dataset refresh triggered for {dataset_name}."),
            }),
        )
            .into_response(),
        Err(
            err @ crate::datafusion::Error::UnableToTriggerRefresh {
                source: crate::accelerated::Error::InvalidRefreshOverrides { .. },
                ..
            },
        ) => (
            status::StatusCode::BAD_REQUEST,
            Json(MessageResponse {
                message: format!("{err}"),
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

    let Some(sql) = payload.refresh_sql else {
        return (status::StatusCode::OK).into_response();
    };

    match df
        .update_refresh_sql(TableReference::parse_str(&dataset.name), sql)
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
    #[cfg_attr(not(feature = "models"), expect(unused_mut))]
    let mut properties = HashMap::new();

    #[cfg(not(feature = "models"))]
    let _ = ds;

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

#[cfg(test)]
mod tests {
    use super::{
        DATASETS_CSV_ERROR_BODY, DatasetResponseItem, convert_entry_to_csv, datasets_to_csv,
        json_value_cell,
    };
    use runtime_api_types::v1::{
        ComponentError, ComponentErrorCategory, ComponentErrorType, ComponentStatus,
    };
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn dataset_item(
        name: &str,
        status: Option<ComponentStatus>,
        error_message: Option<&str>,
    ) -> DatasetResponseItem {
        let mut properties = HashMap::new();
        properties.insert("search".to_string(), json!("supported"));
        properties.insert("vector_search".to_string(), json!("unsupported"));
        let error = status.as_ref().and_then(|status| {
            status.is_error().then(|| {
                ComponentError::from_status_message(ComponentErrorCategory::Dataset, error_message)
            })
        });
        DatasetResponseItem {
            from: "s3://spiceai-public-datasets/trips.parquet".to_string(),
            name: name.to_string(),
            replication_enabled: false,
            acceleration_enabled: true,
            properties,
            status,
            error,
            error_message: error_message.map(ToString::to_string),
            last_refresh: None,
            next_refresh: None,
        }
    }

    #[test]
    fn dataset_info_csv_rejects_map_properties() {
        let error = convert_entry_to_csv(&[dataset_item("trips", None, None)])
            .expect_err("DatasetInfo CSV should fail when properties is a map");
        let message = error.to_string();
        assert!(
            message.contains("serializing maps is not supported"),
            "expected rust-csv to reject map properties, got: {message}"
        );
    }

    #[test]
    fn datasets_csv_without_status_flattens_properties() {
        let csv = datasets_to_csv(&[dataset_item("trips", None, None)]).expect("csv");
        assert_eq!(
            csv,
            "from,name,replication_enabled,acceleration_enabled,status,error,error_message,last_refresh,next_refresh,properties.search,properties.vector_search\n\
             s3://spiceai-public-datasets/trips.parquet,trips,false,true,,,,,,supported,unsupported\n"
        );
    }

    #[test]
    fn datasets_csv_with_status_includes_error_code() {
        let csv = datasets_to_csv(&[
            dataset_item("ready_trips", Some(ComponentStatus::Ready), None),
            dataset_item(
                "broken_trips",
                Some(ComponentStatus::error_with_message(
                    "UnableToConnectInvalidUsernameOrPassword",
                )),
                Some("UnableToConnectInvalidUsernameOrPassword"),
            ),
        ])
        .expect("csv");
        assert_eq!(
            csv,
            "from,name,replication_enabled,acceleration_enabled,status,error,error_message,last_refresh,next_refresh,properties.search,properties.vector_search\n\
             s3://spiceai-public-datasets/trips.parquet,ready_trips,false,true,Ready,,,,,supported,unsupported\n\
             s3://spiceai-public-datasets/trips.parquet,broken_trips,false,true,Error,dataset.auth,UnableToConnectInvalidUsernameOrPassword,,,supported,unsupported\n"
        );
        let broken = csv
            .lines()
            .find(|line| line.contains("broken_trips"))
            .expect("error dataset row");
        assert!(
            broken.contains("dataset.auth"),
            "error.code should flatten into the error cell: {broken}"
        );
        assert!(
            !csv.contains("serializing maps"),
            "flat CSV must not leak the rust-csv map error: {csv}"
        );
    }

    #[test]
    fn datasets_csv_error_body_does_not_leak_crate_text() {
        assert_eq!(
            DATASETS_CSV_ERROR_BODY,
            "Failed to format the dataset list as CSV"
        );
        assert!(!DATASETS_CSV_ERROR_BODY.contains("BurntSushi"));
        assert!(!DATASETS_CSV_ERROR_BODY.contains("serializing maps"));
    }

    #[test]
    fn json_value_cell_encodes_non_string_properties() {
        assert_eq!(
            json_value_cell(Some(&json!("supported"))),
            Some("supported".to_string())
        );
        assert_eq!(json_value_cell(Some(&Value::Null)), None);
        assert_eq!(json_value_cell(None), None);
        assert_eq!(
            json_value_cell(Some(&json!({"enabled": true}))),
            Some("{\"enabled\":true}".to_string())
        );
    }

    /// The `OpenAPI` `text/csv` example must stay 11 columns, matching
    /// [`super::DatasetCsvRow`]. Extra commas shift `properties.*` off the header.
    #[test]
    fn openapi_csv_example_rows_match_flat_header() {
        let csv = datasets_to_csv(&[
            DatasetResponseItem {
                from: "postgres:syncs".to_string(),
                name: "daily_journal_accelerated".to_string(),
                replication_enabled: false,
                acceleration_enabled: true,
                properties: example_properties(),
                status: Some(ComponentStatus::Ready),
                error: None,
                error_message: None,
                last_refresh: None,
                next_refresh: None,
            },
            DatasetResponseItem {
                from: "databricks:hive_metastore.default.messages".to_string(),
                name: "messages_accelerated".to_string(),
                replication_enabled: false,
                acceleration_enabled: true,
                properties: example_properties(),
                status: Some(ComponentStatus::error_with_message(
                    "Unable to authenticate with datasource credentials",
                )),
                error: Some(ComponentError {
                    category: ComponentErrorCategory::Dataset,
                    error_type: ComponentErrorType::Auth,
                    code: "dataset.auth".to_string(),
                }),
                error_message: Some(
                    "Unable to authenticate with datasource credentials".to_string(),
                ),
                last_refresh: None,
                next_refresh: None,
            },
            DatasetResponseItem {
                from: "postgres:aidemo_messages".to_string(),
                name: "general".to_string(),
                replication_enabled: false,
                acceleration_enabled: false,
                properties: example_properties(),
                status: Some(ComponentStatus::Initializing),
                error: None,
                error_message: None,
                last_refresh: None,
                next_refresh: None,
            },
        ])
        .expect("csv");
        assert_eq!(
            csv,
            "from,name,replication_enabled,acceleration_enabled,status,error,error_message,last_refresh,next_refresh,properties.search,properties.vector_search\n\
             postgres:syncs,daily_journal_accelerated,false,true,Ready,,,,,unsupported,unsupported\n\
             databricks:hive_metastore.default.messages,messages_accelerated,false,true,Error,dataset.auth,Unable to authenticate with datasource credentials,,,unsupported,unsupported\n\
             postgres:aidemo_messages,general,false,false,Initializing,,,,,unsupported,unsupported\n"
        );
        for (line_no, line) in csv.lines().enumerate() {
            assert_eq!(
                line.matches(',').count(),
                10,
                "OpenAPI example line {line_no} must have 11 fields: {line}"
            );
        }
    }

    fn example_properties() -> HashMap<String, Value> {
        let mut properties = HashMap::new();
        properties.insert("search".to_string(), json!("unsupported"));
        properties.insert("vector_search".to_string(), json!("unsupported"));
        properties
    }
}
