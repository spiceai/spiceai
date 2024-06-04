/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::{
    component::dataset::Dataset,
    datafusion::query::{Protocol, QueryBuilder},
};
use arrow::array::RecordBatch;
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use csv::Writer;
use datafusion::execution::context::SQLOptions;
use serde::{Deserialize, Serialize};

use crate::{datafusion::DataFusion, status::ComponentStatus};

use futures::TryStreamExt;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Json,
    Csv,
}

fn convert_entry_to_csv<T: Serialize>(entries: &[T]) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for e in entries {
        w.serialize(e)?;
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}

fn dataset_status(df: &DataFusion, ds: &Dataset) -> ComponentStatus {
    if df.table_exists(ds.name.clone()) {
        ComponentStatus::Ready
    } else {
        ComponentStatus::Error
    }
}

// Runs query and converts query results to HTTP response (as JSON).
pub async fn sql_to_http_response(
    df: Arc<DataFusion>,
    sql: &str,
    restricted_sql_options: Option<SQLOptions>,
    nsql: Option<String>,
) -> Response {
    let query = QueryBuilder::new(sql.to_string(), Arc::clone(&df), Protocol::Http)
        .restricted_sql_options(restricted_sql_options)
        .nsql(nsql)
        .protocol(Protocol::Http)
        .build();

    let (data, is_data_from_cache) = match query.run().await {
        Ok(query_result) => match query_result.data.try_collect::<Vec<RecordBatch>>().await {
            Ok(batches) => (batches, query_result.from_cache),
            Err(e) => {
                tracing::debug!("Error executing query: {e}");
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Error processing batch: {e}"),
                )
                    .into_response();
            }
        },
        Err(e) => {
            tracing::debug!("Error executing query: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    if let Err(e) = writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice()) {
        tracing::debug!("Error converting results to JSON: {e}");
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    if let Err(e) = writer.finish() {
        tracing::debug!("Error finishing JSON conversion: {e}");
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let buf = writer.into_inner();
    let res = match String::from_utf8(buf) {
        Ok(res) => res,
        Err(e) => {
            tracing::debug!("Error converting JSON buffer to string: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut headers = HeaderMap::new();

    match is_data_from_cache {
        Some(true) => {
            if let Ok(value) = "Hit from spiceai".parse() {
                headers.insert("X-Cache", value);
            }
        }
        Some(false) => {
            if let Ok(value) = "Miss from spiceai".parse() {
                headers.insert("X-Cache", value);
            }
        }
        None => {}
    };
    (StatusCode::OK, headers, res).into_response()
}

pub(crate) mod query {
    use std::sync::Arc;

    use axum::{
        body::Bytes,
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension,
    };
    use datafusion::execution::context::SQLOptions;

    use crate::datafusion::DataFusion;

    use super::sql_to_http_response;

    pub(crate) async fn post(Extension(df): Extension<Arc<DataFusion>>, body: Bytes) -> Response {
        let query = match String::from_utf8(body.to_vec()) {
            Ok(query) => query,
            Err(e) => {
                tracing::debug!("Error reading query: {e}");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        };

        let restricted_sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);

        sql_to_http_response(df, &query, Some(restricted_sql_options), None).await
    }
}

pub(crate) mod status {
    use csv::Writer;
    use flight_client::FlightClient;
    use serde::{Deserialize, Serialize};
    use std::{net::SocketAddr, sync::Arc};
    use tonic_0_9_0::transport::Channel;
    use tonic_health::{pb::health_client::HealthClient, ServingStatus};

    use axum::{
        extract::Query,
        http::status,
        response::{IntoResponse, Response},
        Extension, Json,
    };

    use crate::{config, status::ComponentStatus};

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub struct QueryParams {
        #[serde(default = "default_format")]
        format: Format,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum Format {
        Json,
        Csv,
    }

    #[derive(Deserialize, Serialize)]
    pub struct ConnectionDetails {
        name: &'static str,
        endpoint: String,
        status: ComponentStatus,
    }

    fn default_format() -> Format {
        Format::Json
    }

    pub(crate) async fn get(
        Extension(cfg): Extension<Arc<config::Config>>,
        Extension(with_metrics): Extension<Option<SocketAddr>>,
        Query(params): Query<QueryParams>,
    ) -> Response {
        let cfg = cfg.as_ref();
        let flight_url = cfg.flight_bind_address.to_string();

        let details = vec![
            ConnectionDetails {
                name: "http",
                endpoint: cfg.http_bind_address.to_string(),
                status: ComponentStatus::Ready,
            },
            ConnectionDetails {
                name: "flight",
                status: get_flight_status(&flight_url).await,
                endpoint: flight_url,
            },
            ConnectionDetails {
                name: "metrics",
                endpoint: with_metrics.map_or("N/A".to_string(), |addr| addr.to_string()),
                status: match with_metrics {
                    Some(metrics_url) => match get_metrics_status(&metrics_url.to_string()).await {
                        Ok(status) => status,
                        Err(e) => {
                            tracing::error!("Error getting metrics status from {metrics_url}: {e}");
                            ComponentStatus::Error
                        }
                    },
                    None => ComponentStatus::Disabled,
                },
            },
            ConnectionDetails {
                name: "opentelemetry",
                status: match get_opentelemetry_status(
                    cfg.open_telemetry_bind_address.to_string().as_str(),
                )
                .await
                {
                    Ok(status) => status,
                    Err(e) => {
                        tracing::error!(
                            "Error getting opentelemetry status from {}: {}",
                            cfg.open_telemetry_bind_address,
                            e
                        );
                        ComponentStatus::Error
                    }
                },
                endpoint: cfg.open_telemetry_bind_address.to_string(),
            },
        ];

        match params.format {
            Format::Json => (status::StatusCode::OK, Json(details)).into_response(),
            Format::Csv => match convert_details_to_csv(&details) {
                Ok(csv) => (status::StatusCode::OK, csv).into_response(),
                Err(e) => {
                    tracing::error!("Error converting to CSV: {e}");
                    (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            },
        }
    }

    fn convert_details_to_csv(
        details: &[ConnectionDetails],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut w = Writer::from_writer(vec![]);
        for d in details {
            let _ = w.serialize(d);
        }
        w.flush()?;
        Ok(String::from_utf8(w.into_inner()?)?)
    }

    async fn get_flight_status(flight_addr: &str) -> ComponentStatus {
        tracing::trace!("Checking flight status at {flight_addr}");
        match FlightClient::new(&format!("http://{flight_addr}"), "", "").await {
            Ok(_) => ComponentStatus::Ready,
            Err(e) => {
                tracing::error!("Error connecting to flight when checking status: {e}");
                ComponentStatus::Error
            }
        }
    }

    async fn get_metrics_status(
        metrics_addr: &str,
    ) -> Result<ComponentStatus, Box<dyn std::error::Error>> {
        let resp = reqwest::get(format!("http://{metrics_addr}/health")).await?;
        if resp.status().is_success() && resp.text().await? == "OK" {
            Ok(ComponentStatus::Ready)
        } else {
            Ok(ComponentStatus::Error)
        }
    }
    async fn get_opentelemetry_status(
        addr: &str,
    ) -> Result<ComponentStatus, Box<dyn std::error::Error>> {
        let channel = Channel::from_shared(format!("http://{addr}"))?
            .connect()
            .await?;

        let mut client = HealthClient::new(channel);

        let resp = client
            .check(tonic_health::pb::HealthCheckRequest {
                service: String::new(),
            })
            .await?;

        if resp.into_inner().status == ServingStatus::Serving as i32 {
            Ok(ComponentStatus::Ready)
        } else {
            Ok(ComponentStatus::Error)
        }
    }
}

pub(crate) mod datasets {
    use std::sync::Arc;

    use crate::{component::dataset::Dataset, Runtime};
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
    use tokio::sync::RwLock;
    use tract_core::tract_data::itertools::Itertools;

    use crate::{datafusion::DataFusion, status::ComponentStatus};

    use super::{convert_entry_to_csv, dataset_status, Format};

    #[derive(Debug, Deserialize)]
    pub(crate) struct DatasetFilter {
        source: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct DatasetQueryParams {
        #[serde(default)]
        status: bool,

        #[serde(default)]
        format: Format,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub(crate) struct DatasetResponseItem {
        pub from: String,
        pub name: String,
        pub replication_enabled: bool,
        pub acceleration_enabled: bool,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub status: Option<ComponentStatus>,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Query(filter): Query<DatasetFilter>,
        Query(params): Query<DatasetQueryParams>,
    ) -> Response {
        let app_lock = app.read().await;
        let Some(readable_app) = &*app_lock else {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                Json::<Vec<DatasetResponseItem>>(vec![]),
            )
                .into_response();
        };

        let valid_datasets = Runtime::get_valid_datasets(readable_app, false);
        let datasets: Vec<Dataset> = match filter.source {
            Some(source) => valid_datasets
                .into_iter()
                .filter(|d| d.source() == source)
                .collect(),
            None => valid_datasets,
        };

        let resp = datasets
            .iter()
            .map(|d| DatasetResponseItem {
                from: d.from.clone(),
                name: d.name.to_quoted_string(),
                replication_enabled: d.replication.as_ref().is_some_and(|f| f.enabled),
                acceleration_enabled: d.acceleration.as_ref().is_some_and(|f| f.enabled),
                status: if params.status {
                    Some(dataset_status(&df, d))
                } else {
                    None
                },
            })
            .collect_vec();

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
    #[serde(rename_all = "lowercase")]
    pub(crate) struct MessageResponse {
        pub message: String,
    }

    #[derive(Deserialize)]
    pub struct AccelerationRequest {
        pub refresh_sql: Option<String>,
    }

    pub(crate) async fn refresh(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(dataset_name): Path<String>,
    ) -> Response {
        let app_lock = app.read().await;
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

        match df.refresh_table(&dataset.name).await {
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
                    message: format!("Failed to trigger refresh for {dataset_name}: {err}."),
                }),
            )
                .into_response(),
        }
    }

    pub(crate) async fn acceleration(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(dataset_name): Path<String>,
        Json(payload): Json<AccelerationRequest>,
    ) -> Response {
        let app_lock = app.read().await;
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
}

pub(crate) mod spicepods {
    use std::sync::Arc;

    use app::App;
    use axum::{
        extract::Query,
        http::status,
        response::{IntoResponse, Response},
        Extension, Json,
    };
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use spicepod::Spicepod;
    use tokio::sync::RwLock;

    use super::{convert_entry_to_csv, Format};

    #[derive(Debug, Deserialize)]
    pub(crate) struct SpicepodQueryParams {
        #[allow(dead_code)]
        #[serde(default)]
        status: bool,

        #[serde(default)]
        format: Format,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub(crate) struct SpicepodCsvRow {
        pub name: String,

        pub version: String,

        #[serde(default)]
        pub datasets_count: usize,

        #[serde(default)]
        pub models_count: usize,

        #[serde(default)]
        pub dependencies_count: usize,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Query(params): Query<SpicepodQueryParams>,
    ) -> Response {
        let Some(readable_app) = &*app.read().await else {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                Json::<Vec<Spicepod>>(vec![]),
            )
                .into_response();
        };

        match params.format {
            Format::Json => {
                (status::StatusCode::OK, Json(readable_app.spicepods.clone())).into_response()
            }
            Format::Csv => {
                let resp: Vec<SpicepodCsvRow> = readable_app
                    .spicepods
                    .iter()
                    .map(|spod| SpicepodCsvRow {
                        version: spod.version.to_string(),
                        name: spod.name.clone(),
                        models_count: spod.models.len(),
                        datasets_count: spod.datasets.len(),
                        dependencies_count: spod.dependencies.len(),
                    })
                    .collect_vec();
                match convert_entry_to_csv(&resp) {
                    Ok(csv) => (status::StatusCode::OK, csv).into_response(),
                    Err(e) => {
                        tracing::error!("Error converting to CSV: {e}");
                        (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                    }
                }
            }
        }
    }
}

pub(crate) mod models {
    use std::{collections::HashMap, sync::Arc};

    use axum::{
        extract::Query,
        http::status,
        response::{IntoResponse, Json, Response},
        Extension,
    };
    use csv::Writer;
    use model_components::model::Model;
    use serde::{Deserialize, Serialize};
    use tokio::sync::RwLock;

    use super::Format;

    #[derive(Debug, Deserialize)]
    pub(crate) struct ModelsQueryParams {
        #[serde(default)]
        format: Format,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub(crate) struct ModelResponse {
        pub name: String,
        pub from: String,
        pub datasets: Option<Vec<String>>,
    }

    pub(crate) async fn get(
        Extension(model): Extension<Arc<RwLock<HashMap<String, Model>>>>,
        Query(params): Query<ModelsQueryParams>,
    ) -> Response {
        let resp = model
            .read()
            .await
            .values()
            .map(|m| {
                let datasets = if m.model.datasets.is_empty() {
                    None
                } else {
                    Some(m.model.datasets.clone())
                };
                ModelResponse {
                    name: m.model.name.clone(),
                    from: m.model.from.clone(),
                    datasets,
                }
            })
            .collect::<Vec<ModelResponse>>();

        match params.format {
            Format::Json => (status::StatusCode::OK, Json(resp)).into_response(),
            Format::Csv => match convert_details_to_csv(&resp) {
                Ok(csv) => (status::StatusCode::OK, csv).into_response(),
                Err(e) => {
                    tracing::error!("Error converting to CSV: {e}");
                    (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            },
        }
    }

    fn convert_details_to_csv(
        models: &[ModelResponse],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut w = Writer::from_writer(vec![]);
        for d in models {
            let _ = w.serialize(d);
        }
        w.flush()?;
        Ok(String::from_utf8(w.into_inner()?)?)
    }
}

pub(crate) mod inference {
    use crate::{datafusion::DataFusion, model::run};

    use app::App;
    use arrow::array::Float32Array;
    use axum::{
        extract::Path,
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension, Json,
    };
    use model_components::{model::Model, modelsource};
    use serde::{Deserialize, Serialize};
    use std::time::Instant;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;
    use tract_core::tract_data::itertools::Itertools;

    #[derive(Deserialize)]
    pub struct BatchPredictRequest {
        #[serde(default)]
        pub predictions: Vec<PredictRequest>,
    }

    #[derive(Deserialize)]
    pub struct PredictRequest {
        pub model_name: String,
    }

    #[derive(Serialize)]
    pub struct BatchPredictResponse {
        pub predictions: Vec<PredictResponse>,
        pub duration_ms: u128,
    }

    #[derive(Serialize)]
    pub struct PredictResponse {
        pub status: PredictStatus,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub error_message: Option<String>,

        pub model_name: String,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub model_version: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub prediction: Option<Vec<f32>>,

        pub duration_ms: u128,
    }

    #[derive(Serialize)]
    pub enum PredictStatus {
        Success,
        BadRequest,
        InternalError,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(model_name): Path<String>,
        Extension(models): Extension<Arc<RwLock<HashMap<String, Model>>>>,
    ) -> Response {
        let model_predict_response = run_inference(app, df, models, model_name).await;

        match model_predict_response.status {
            PredictStatus::Success => {
                (StatusCode::OK, Json(model_predict_response)).into_response()
            }
            PredictStatus::BadRequest => {
                (StatusCode::BAD_REQUEST, Json(model_predict_response)).into_response()
            }
            PredictStatus::InternalError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(model_predict_response),
            )
                .into_response(),
        }
    }

    pub(crate) async fn post(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Extension(models): Extension<Arc<RwLock<HashMap<String, Model>>>>,
        Json(payload): Json<BatchPredictRequest>,
    ) -> Response {
        let start_time = Instant::now();
        let mut model_predictions = Vec::new();
        let mut model_prediction_futures = Vec::new();

        for model_predict_request in payload.predictions {
            let prediction_future = run_inference(
                Arc::clone(&app),
                Arc::clone(&df),
                Arc::clone(&models),
                model_predict_request.model_name,
            );
            model_prediction_futures.push(prediction_future);
        }

        for prediction_future in model_prediction_futures {
            model_predictions.push(prediction_future.await);
        }

        (
            StatusCode::OK,
            Json(BatchPredictResponse {
                duration_ms: start_time.elapsed().as_millis(),
                predictions: model_predictions,
            }),
        )
            .into_response()
    }

    async fn run_inference(
        app: Arc<RwLock<Option<App>>>,
        df: Arc<DataFusion>,
        models: Arc<RwLock<HashMap<String, Model>>>,
        model_name: String,
    ) -> PredictResponse {
        let start_time = Instant::now();

        let app_lock = app.read().await;
        let Some(readable_app) = &*app_lock else {
            return PredictResponse {
                status: PredictStatus::BadRequest,
                error_message: Some("App not found".to_string()),
                model_name,
                model_version: None,
                prediction: None,
                duration_ms: start_time.elapsed().as_millis(),
            };
        };

        let model = readable_app.models.iter().find(|m| m.name == model_name);
        let Some(model) = model else {
            tracing::debug!("Model {model_name} not found");
            return PredictResponse {
                status: PredictStatus::BadRequest,
                error_message: Some(format!("Model {model_name} not found")),
                model_name,
                model_version: None,
                prediction: None,
                duration_ms: start_time.elapsed().as_millis(),
            };
        };

        let loaded_models = models.read().await;
        let Some(runnable) = loaded_models.get(&model.name) else {
            tracing::debug!("Model {model_name} not found");
            return PredictResponse {
                status: PredictStatus::BadRequest,
                error_message: Some(format!("Model {model_name} not found")),
                model_name,
                model_version: Some(modelsource::version(&model.from)),
                prediction: None,
                duration_ms: start_time.elapsed().as_millis(),
            };
        };

        match run(runnable, Arc::clone(&df)).await {
            Ok(inference_result) => {
                if let Some(column_data) = inference_result.column_by_name("y") {
                    if let Some(array) = column_data.as_any().downcast_ref::<Float32Array>() {
                        let result = array.values().iter().copied().collect_vec();
                        return PredictResponse {
                            status: PredictStatus::Success,
                            error_message: None,
                            model_name,
                            model_version: Some(modelsource::version(&model.from)),
                            prediction: Some(result),
                            duration_ms: start_time.elapsed().as_millis(),
                        };
                    }
                    tracing::error!(
                        "Failed to cast inference result for model {model_name} to Float32Array"
                    );
                    tracing::debug!("Failed to cast inference result for model {model_name} to Float32Array: {column_data:?}");
                    return PredictResponse {
                        status: PredictStatus::InternalError,
                        error_message: Some(
                            "Unable to cast inference result to Float32Array".to_string(),
                        ),
                        model_name,
                        model_version: Some(modelsource::version(&model.from)),
                        prediction: None,
                        duration_ms: start_time.elapsed().as_millis(),
                    };
                }
                tracing::error!(
                    "Unable to find column 'y' in inference result for model {model_name}"
                );
                PredictResponse {
                    status: PredictStatus::InternalError,
                    error_message: Some(
                        "Unable to find column 'y' in inference result".to_string(),
                    ),
                    model_name,
                    model_version: Some(modelsource::version(&model.from)),
                    prediction: None,
                    duration_ms: start_time.elapsed().as_millis(),
                }
            }
            Err(e) => {
                tracing::error!("Unable to run inference: {e}");
                PredictResponse {
                    status: PredictStatus::InternalError,
                    error_message: Some(e.to_string()),
                    model_name,
                    model_version: Some(modelsource::version(&model.from)),
                    prediction: None,
                    duration_ms: start_time.elapsed().as_millis(),
                }
            }
        }
    }
}

pub(crate) mod nsql {
    use arrow_sql_gen::statement::CreateTableBuilder;
    use axum::{
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension, Json,
    };
    use datafusion::execution::context::SQLOptions;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::{datafusion::DataFusion, http::v1::sql_to_http_response, LLMModelStore};

    fn clean_model_based_sql(input: &str) -> String {
        let no_dashes = match input.strip_prefix("--") {
            Some(rest) => rest.to_string(),
            None => input.to_string(),
        };

        // Only take the first query, if there are multiple.
        let one_query = no_dashes.split(';').next().unwrap_or(&no_dashes);
        one_query.trim().to_string()
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub struct Request {
        pub query: String,

        #[serde(rename = "use", default = "default_model")]
        pub model: String,
    }

    fn default_model() -> String {
        "nql".to_string()
    }

    pub(crate) async fn post(
        Extension(df): Extension<Arc<DataFusion>>,
        Extension(nsql_models): Extension<Arc<RwLock<LLMModelStore>>>,
        Json(payload): Json<Request>,
    ) -> Response {
        // Get all public table CREATE TABLE statements to add to prompt.
        let tables = match df.get_public_table_names() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Error getting tables: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

        let mut table_create_stms: Vec<String> = Vec::with_capacity(tables.len());
        for t in &tables {
            match df.get_arrow_schema(t).await {
                Ok(schm) => {
                    let c = CreateTableBuilder::new(Arc::new(schm), format!("public.{t}").as_str());
                    table_create_stms.push(c.build_postgres());
                }
                Err(e) => {
                    tracing::error!("Error getting table={t} schema: {e}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            }
        }

        // Construct prompt
        let nsql_query = format!(
            "```SQL\n{table_create_schemas}\n-- Using valid postgres SQL, without comments, answer the following questions for the tables provided above.\n-- {user_query}",
            user_query=payload.query,
            table_create_schemas=table_create_stms.join("\n")
        );

        let nsql_query_copy = nsql_query.clone();

        tracing::trace!("Running prompt: {nsql_query}");

        let result = match nsql_models.read().await.get(&payload.model) {
            Some(nql_model) => nql_model.write().await.run(nsql_query).await,
            None => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Model {} not found", payload.model),
                )
                    .into_response()
            }
        };

        let restricted_sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);

        // Run the SQL from the NSQL model through datafusion.
        match result {
            Ok(Some(model_sql_query)) => {
                let cleaned_query = clean_model_based_sql(&model_sql_query);
                tracing::trace!("Running query:\n{cleaned_query}");

                sql_to_http_response(
                    Arc::clone(&df),
                    &cleaned_query,
                    Some(restricted_sql_options),
                    Some(nsql_query_copy),
                )
                .await
            }
            Ok(None) => {
                tracing::trace!("No query produced from NSQL model");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "No query produced from NSQL model".to_string(),
                )
                    .into_response()
            }
            Err(e) => {
                tracing::error!("Error running NSQL model: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

pub(crate) mod embed {
    use arrow::array::StringArray;
    use axum::{
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension, Json,
    };
    use datafusion::execution::context::SQLOptions;
    use futures::TryStreamExt;

    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::{
        datafusion::{
            query::{Protocol, QueryBuilder},
            DataFusion,
        },
        EmbeddingModelStore,
    };

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub struct TextRequest {
        pub text: String,

        #[serde(rename = "use", default = "default_model")]
        pub model: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub struct SqlRequest {
        pub sql: String,

        #[serde(rename = "use", default = "default_model")]
        pub model: String,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum Request {
        Sql(SqlRequest),
        Text(TextRequest),
    }

    fn default_model() -> String {
        "embed".to_string()
    }

    // For [`SqlRequest`], create the text to embed by querying [`Datafusion`].
    async fn to_text(
        df: Arc<DataFusion>,
        sql: String,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let opt = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);

        let query = QueryBuilder::new(sql, Arc::clone(&df), Protocol::Http)
            .restricted_sql_options(Some(opt))
            .build();

        // Attempt to convert first column to String
        let result: Result<Vec<Result<Vec<String>, _>>, _> =
            query
                .run()
                .await
                .map(|r| r.data)?
                .map_ok(
                    |r| match r.column(0).as_any().downcast_ref::<StringArray>() {
                        Some(s) => Ok(s
                            .into_iter()
                            .flatten()
                            .map(ToString::to_string)
                            .collect::<Vec<String>>()),
                        None => Err("Expected first column of SQL query to return a String type"
                            .to_string()),
                    },
                )
                .try_collect()
                .await;

        match result {
            Ok(result) => {
                let result = result
                    .into_iter()
                    .collect::<Result<Vec<Vec<String>>, _>>()?;
                Ok(result.into_iter().flatten().collect())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn post(
        Extension(df): Extension<Arc<DataFusion>>,
        Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
        Json(payload): Json<Request>,
    ) -> Response {
        let (text, model) = match payload {
            Request::Text(TextRequest { text, model }) => (vec![text], model),
            Request::Sql(SqlRequest { sql, model }) => {
                let text = match to_text(Arc::clone(&df), sql).await {
                    Ok(text) => text,
                    Err(e) => {
                        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                    }
                };
                (text, model)
            }
        };

        match embeddings.read().await.get(&model) {
            Some(embedding_model) => {
                let mut embedding_model = embedding_model.write().await;
                match embedding_model
                    .embed(llms::embeddings::EmbeddingInput::StringBatch(text))
                    .await
                {
                    Ok(embedding) => (StatusCode::OK, Json(embedding)).into_response(),
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
                }
            }
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Model {model} not found"),
            )
                .into_response(),
        }
    }
}

pub(crate) mod assist {
    use arrow::array::StringArray;
    use axum::{
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension, Json,
    };
    use datafusion::{datasource::TableProvider, sql::TableReference};

    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::RwLock;

    use crate::{
        datafusion::DataFusion, embeddings::table::EmbeddingTable, EmbeddingModelStore,
        LLMModelStore,
    };

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub struct Request {
        pub text: String,

        /// The model to use for chat completion. The embedding model is determined via the data source (that has the associated embeddings).
        #[serde(rename = "use", default = "default_model")]
        pub model: String,

        /// Which datasources in the [`DataFusion`] instance to retrieve data from.
        #[serde(rename = "from", default)]
        pub data_source: Vec<String>,
    }

    fn default_model() -> String {
        "embed".to_string()
    }

    async fn create_input_embeddings(
        input: &str,
        embeddings_to_run: Vec<String>,
        embeddings: Arc<RwLock<EmbeddingModelStore>>,
    ) -> Result<HashMap<String, Vec<f32>>, Box<dyn std::error::Error>> {
        let mut embedded_inputs: HashMap<String, Vec<f32>> = HashMap::new();
        for (name, model) in embeddings
            .read()
            .await
            .iter()
            .filter(|(model_name, _)| embeddings_to_run.contains(model_name))
        {
            match model
                .write()
                .await
                .embed(llms::embeddings::EmbeddingInput::String(input.to_string()))
                .await
            {
                Ok(embedding) => match embedding.first() {
                    Some(embedding) => {
                        embedded_inputs.insert(name.clone(), embedding.clone());
                    }
                    None => {
                        return Err(
                            format!("No embeddings returned for input text from {name}").into()
                        )
                    }
                },
                Err(e) => return Err(Box::new(e)),
            }
        }
        Ok(embedded_inputs)
    }

    fn combined_relevant_data_and_input(
        relevant_data: &HashMap<TableReference, Vec<String>>,
        input: &str,
    ) -> String {
        let data = relevant_data.values().map(|v| v.join("\n")).join("\n");
        format!("{data}\n{input}")
    }

    /// Find the name of columns in the table reference that have associated embedding columns.
    fn embedding_columns_in(tbl: &Arc<dyn TableProvider>) -> Vec<String> {
        match tbl.as_any().downcast_ref::<EmbeddingTable>() {
            Some(embedding_table) => embedding_table.get_embedding_models_used(),
            None => vec![],
        }
    }

    /// For the data sources that assumedly exist in the [`DataFusion`] instance, find the embedding models used in each data source.
    async fn find_relevant_embedding_models(
        data_sources: Vec<TableReference>,
        df: Arc<DataFusion>,
    ) -> Result<HashMap<TableReference, Vec<String>>, Box<dyn std::error::Error>> {
        let mut embeddings_to_run = HashMap::new();
        for data_source in data_sources {
            match df.get_table(data_source.clone()).await {
                None => {
                    return Err(
                        format!("Data source {} does not exist", data_source.clone()).into(),
                    )
                }
                Some(table) => match embedding_columns_in(&table) {
                    v if v.is_empty() => {
                        return Err(format!(
                            "Data source {} does not have an embedded column",
                            data_source.clone()
                        )
                        .into())
                    }
                    v => {
                        embeddings_to_run.insert(data_source, v);
                    }
                },
            }
        }
        Ok(embeddings_to_run)
    }

    async fn vector_search(
        df: Arc<DataFusion>,
        embedded_inputs: HashMap<TableReference, Vec<Vec<f32>>>,
        n: usize,
    ) -> Result<HashMap<TableReference, Vec<String>>, Box<dyn std::error::Error>> {
        let mut search_result: HashMap<TableReference, Vec<String>> = HashMap::new();

        for (tbl, search_vectors) in embedded_inputs {
            tracing::debug!("Running vector search for table {tbl:#?}");

            let provider = df
                .get_table(tbl.clone())
                .await
                .ok_or(format!("Table {} not found", tbl.clone()))?;

            let embedding_table = provider
                .as_any()
                .downcast_ref::<EmbeddingTable>()
                .ok_or(format!("Table {tbl} is not an embedding table"))?;

            // Only support one embedding column per table.
            let embedding_columns = embedding_table.get_embedding_columns();
            let embedding_column = embedding_columns
                .first()
                .ok_or(format!("No embeddings found for table {tbl}"))?;

            if search_vectors.len() != 1 {
                return Err(format!("Only one embedding column per table currently supported. Table: {tbl} has {} embeddings", search_vectors.len()).into());
            }
            match search_vectors.first() {
                None => return Err(format!("No embeddings found for table {tbl}").into()),
                Some(embedding) => {
                    let sql_query = format!("SELECT {embedding_column} FROM {tbl} ORDER BY array_distance({embedding_column}_embedding, {embedding:?}) LIMIT {n}");

                    let result = df.ctx.sql(&sql_query).await?;
                    let batch = result.collect().await?;

                    let outt: Vec<_> = batch
                        .iter()
                        .map(|b| {
                            let z = b.column(0).as_any().downcast_ref::<StringArray>().ok_or(
                                "Expected first column of SQL query to return a String type",
                            );
                            let zz = z.map(|s| {
                                s.iter()
                                    .map(|ss| ss.unwrap_or_default().to_string())
                                    .collect::<Vec<String>>()
                            });
                            zz
                        })
                        .collect::<Result<Vec<_>, &str>>()?;

                    let outtt: Vec<String> =
                        outt.iter().flat_map(std::clone::Clone::clone).collect();
                    search_result.insert(tbl, outtt);
                }
            };
        }

        Ok(search_result)
    }

    /// Assist runs a question or statement through an LLM with additional context retrieved from data within the [`DataFusion`] instance.
    /// Logic:
    /// 1. If user did not provide which/what data source to use, figure this out (?).
    /// 2. Get embedding provider for each data source.
    /// 2. Create embedding(s) of question/statement
    /// 3. Retrieve relevant data from the data source.
    /// 4. Run [relevant data;  question/statement] through LLM.
    /// 5. Return response.
    pub(crate) async fn post(
        Extension(df): Extension<Arc<DataFusion>>,
        Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
        Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
        Json(payload): Json<Request>,
    ) -> Response {
        // For now, force the user to specify which data.
        if payload.data_source.is_empty() {
            return (StatusCode::BAD_REQUEST, "No data sources provided").into_response();
        }

        // Determine which embedding models need to be run. If a table does not have an embedded column, return an error.
        let embeddings_to_run = match find_relevant_embedding_models(
            payload
                .data_source
                .iter()
                .map(TableReference::from)
                .collect(),
            Arc::clone(&df),
        )
        .await
        {
            Ok(embeddings_to_run) => embeddings_to_run,
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        };

        // Create embedding(s) for question/statement. `embedded_inputs` model_name -> embedding.
        let embedded_inputs = match create_input_embeddings(
            &payload.text.clone(),
            embeddings_to_run.values().flatten().cloned().collect(),
            Arc::clone(&embeddings),
        )
        .await
        {
            Ok(embedded_inputs) => embedded_inputs,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };

        let per_table_embeddings: HashMap<TableReference, _> = embeddings_to_run
            .iter()
            .map(|(t, model_names)| {
                let z: Vec<_> = model_names
                    .iter()
                    .filter_map(|m| embedded_inputs.get(m).cloned())
                    .collect();
                (t.clone(), z)
            })
            .collect();

        // Vector search to get relevant data from data sources.
        let relevant_data = match vector_search(Arc::clone(&df), per_table_embeddings, 3).await {
            Ok(relevant_data) => relevant_data,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };

        // Using returned data, create input for LLM.
        let model_input = combined_relevant_data_and_input(&relevant_data, &payload.text);

        // Run LLM with input.
        match llms.read().await.get(&payload.model) {
            Some(llm_model) => {
                // TODO: We need to either 1. Separate LLMs from NQL, or create a LLM trait separately.
                match llm_model.write().await.run(model_input).await {
                    Ok(Some(assist)) => (StatusCode::OK, Json(assist)).into_response(),
                    Ok(None) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("No response from LLM {}", payload.model),
                    )
                        .into_response(),
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
                }
            }
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Model {} not found", payload.model),
            )
                .into_response(),
        }
    }
}
