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

use csv::Writer;
use serde::{Deserialize, Serialize};
use spicepod::component::dataset::Dataset;

use crate::{datafusion::DataFusion, status::ComponentStatus};

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
    if df.table_exists(ds.name.as_str()) {
        ComponentStatus::Ready
    } else {
        ComponentStatus::Error
    }
}

pub(crate) mod query {
    use arrow::record_batch::RecordBatch;
    use axum::{
        body::Bytes,
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension,
    };
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::datafusion::DataFusion;

    pub(crate) async fn post(
        Extension(df): Extension<Arc<RwLock<DataFusion>>>,
        body: Bytes,
    ) -> Response {
        let query = match String::from_utf8(body.to_vec()) {
            Ok(query) => query,
            Err(e) => {
                tracing::debug!("Error reading query: {e}");
                return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
            }
        };

        let data_frame = match df.read().await.ctx.sql(&query).await {
            Ok(data_frame) => data_frame,
            Err(e) => {
                tracing::debug!("Error running query: {e}");
                return (StatusCode::BAD_REQUEST, query.to_string()).into_response();
            }
        };

        let results = match data_frame.collect().await {
            Ok(results) => results,
            Err(e) => {
                tracing::debug!("Error collecting results: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);

        if let Err(e) =
            writer.write_batches(results.iter().collect::<Vec<&RecordBatch>>().as_slice())
        {
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

        (StatusCode::OK, res).into_response()
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

    use app::App;
    use axum::{
        extract::Path,
        extract::Query,
        http::status,
        response::{IntoResponse, Response},
        Extension, Json,
    };
    use serde::{Deserialize, Serialize};
    use spicepod::component::dataset::Dataset;
    use tokio::sync::RwLock;
    use tract_core::tract_data::itertools::Itertools;

    use crate::{datafusion::DataFusion, status::ComponentStatus};

    use super::{convert_entry_to_csv, dataset_status, Format};

    #[derive(Debug, Deserialize)]
    pub(crate) struct DatasetFilter {
        source: Option<String>,

        #[serde(default)]
        remove_views: bool,
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
        pub depends_on: Option<String>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub status: Option<ComponentStatus>,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(df): Extension<Arc<RwLock<DataFusion>>>,
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

        let mut datasets: Vec<Dataset> = match filter.source {
            Some(source) => readable_app
                .datasets
                .iter()
                .filter(|d| d.source() == source)
                .cloned()
                .collect(),
            None => readable_app.datasets.clone(),
        };

        if filter.remove_views {
            datasets.retain(|d| !d.is_view());
        }

        let df_read = df.read().await;

        let resp = datasets
            .iter()
            .map(|d| DatasetResponseItem {
                from: d.from.clone(),
                name: d.name.clone(),
                replication_enabled: d.replication.as_ref().is_some_and(|f| f.enabled),
                acceleration_enabled: d.acceleration.as_ref().is_some_and(|f| f.enabled),
                depends_on: if d.depends_on.is_empty() {
                    None
                } else {
                    Some(d.depends_on.join(", "))
                },
                status: if params.status {
                    Some(dataset_status(&df_read, d))
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
    pub(crate) struct DatasetRefreshResponse {
        pub message: String,
    }

    pub(crate) async fn refresh(
        Extension(app): Extension<Arc<RwLock<Option<App>>>>,
        Extension(_df): Extension<Arc<RwLock<DataFusion>>>,
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
                Json(DatasetRefreshResponse {
                    message: format!("Dataset {dataset_name} not found"),
                }),
            )
                .into_response();
        };

        let acceleration_enabled = dataset.acceleration.as_ref().is_some_and(|f| f.enabled);

        if !acceleration_enabled {
            return (
                status::StatusCode::BAD_REQUEST,
                Json(DatasetRefreshResponse {
                    message: format!("Dataset {dataset_name} does not have acceleration enabled"),
                }),
            )
                .into_response();
        };

        (status::StatusCode::NOT_IMPLEMENTED).into_response()
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
    use serde::{Deserialize, Serialize};
    use tokio::sync::RwLock;

    use crate::model::Model;

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
    use crate::datafusion::DataFusion;
    use crate::model::version as model_version;
    use crate::model::Model;
    use app::App;
    use arrow::array::Float32Array;
    use axum::{
        extract::Path,
        http::StatusCode,
        response::{IntoResponse, Response},
        Extension, Json,
    };
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
        Extension(df): Extension<Arc<RwLock<DataFusion>>>,
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
        Extension(df): Extension<Arc<RwLock<DataFusion>>>,
        Extension(models): Extension<Arc<RwLock<HashMap<String, Model>>>>,
        Json(payload): Json<BatchPredictRequest>,
    ) -> Response {
        let start_time = Instant::now();
        let mut model_predictions = Vec::new();
        let mut model_prediction_futures = Vec::new();

        for model_predict_request in payload.predictions {
            let prediction_future = run_inference(
                app.clone(),
                df.clone(),
                models.clone(),
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
        df: Arc<RwLock<DataFusion>>,
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
                model_version: Some(model_version(&model.from)),
                prediction: None,
                duration_ms: start_time.elapsed().as_millis(),
            };
        };

        match runnable.run(df.clone()).await {
            Ok(inference_result) => {
                if let Some(column_data) = inference_result.column_by_name("y") {
                    if let Some(array) = column_data.as_any().downcast_ref::<Float32Array>() {
                        let result = array.values().iter().copied().collect_vec();
                        return PredictResponse {
                            status: PredictStatus::Success,
                            error_message: None,
                            model_name,
                            model_version: Some(model_version(&model.from)),
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
                        model_version: Some(model_version(&model.from)),
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
                    model_version: Some(model_version(&model.from)),
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
                    model_version: Some(model_version(&model.from)),
                    prediction: None,
                    duration_ms: start_time.elapsed().as_millis(),
                }
            }
        }
    }
}
