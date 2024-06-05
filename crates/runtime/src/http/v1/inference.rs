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
        PredictStatus::Success => (StatusCode::OK, Json(model_predict_response)).into_response(),
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
            tracing::error!("Unable to find column 'y' in inference result for model {model_name}");
            PredictResponse {
                status: PredictStatus::InternalError,
                error_message: Some("Unable to find column 'y' in inference result".to_string()),
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
