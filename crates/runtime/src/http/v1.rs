pub(crate) mod datasets {
    use std::sync::Arc;

    use app::App;
    use axum::{extract::Query, Extension, Json};
    use serde::Deserialize;
    use spicepod::component::dataset::Dataset;

    #[derive(Debug, Deserialize)]
    pub(crate) struct DatasetFilter {
        source: Option<String>,

        #[serde(default)]
        remove_views: bool,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Query(filter): Query<DatasetFilter>,
    ) -> Json<Vec<Dataset>> {
        let mut datasets: Vec<Dataset> = match filter.source {
            Some(source) => app
                .datasets
                .iter()
                .filter(|d| d.source() == source)
                .cloned()
                .collect(),
            None => app.datasets.clone(),
        };

        if filter.remove_views {
            datasets.retain(|d| !d.is_view());
        }

        Json(datasets)
    }
}

pub(crate) mod inference {
    use crate::datafusion::DataFusion;
    use crate::model::Model;
    use app::App;
    use arrow::array::Float32Array;
    use axum::{
        extract::{Path, Query},
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
    pub struct PredictRequest {
        #[serde(default)]
        pub predictions: Vec<ModelPredictRequest>,
    }

    #[derive(Deserialize)]
    pub struct ModelPredictRequest {
        pub model_name: String,

        #[serde(default = "default_lookback")]
        pub lookback: usize,
    }

    #[derive(Deserialize)]
    pub struct ModelPredictParams {
        #[serde(default = "default_lookback")]
        pub lookback: usize,
    }

    // TODO(jeadie): This needs to come from the training_run postgres table in cloud, for the specific training run that made the model.
    fn default_lookback() -> usize {
        4
    }

    #[derive(Serialize)]
    pub struct PredictResponse {
        pub predictions: Vec<ModelPredictResponse>,
        pub duration_ms: u128,
    }

    #[derive(Serialize)]
    pub struct ModelPredictResponse {
        pub status: ModelPredictStatus,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub error_message: Option<String>,

        pub model_name: String,

        pub lookback: usize,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub forecast: Vec<f32>,

        pub duration_ms: u128,
    }

    #[derive(Serialize)]
    pub enum ModelPredictStatus {
        Success,
        BadRequest,
        InternalError,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<RwLock<DataFusion>>>,
        Path(model_name): Path<String>,
        Query(params): Query<ModelPredictParams>,
        Extension(models): Extension<Arc<HashMap<String, Model>>>,
    ) -> Response {
        let model_predict_response =
            run_inference(app, df, models, model_name, params.lookback).await;

        match model_predict_response.status {
            ModelPredictStatus::Success => {
                (StatusCode::OK, Json(model_predict_response)).into_response()
            }
            ModelPredictStatus::BadRequest => {
                (StatusCode::BAD_REQUEST, Json(model_predict_response)).into_response()
            }
            ModelPredictStatus::InternalError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(model_predict_response),
            )
                .into_response(),
        }
    }

    pub(crate) async fn post(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<RwLock<DataFusion>>>,
        Extension(models): Extension<Arc<HashMap<String, Model>>>,
        Json(payload): Json<PredictRequest>,
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
                model_predict_request.lookback,
            );
            model_prediction_futures.push(prediction_future);
        }

        for prediction_future in model_prediction_futures {
            model_predictions.push(prediction_future.await);
        }

        (
            StatusCode::OK,
            Json(PredictResponse {
                duration_ms: start_time.elapsed().as_millis(),
                predictions: model_predictions,
            }),
        )
            .into_response()
    }

    async fn run_inference(
        app: Arc<App>,
        df: Arc<RwLock<DataFusion>>,
        models: Arc<HashMap<String, Model>>,
        model_name: String,
        lookback: usize,
    ) -> ModelPredictResponse {
        let start_time = Instant::now();

        let model = app.models.iter().find(|m| m.name == model_name);

        let Some(model) = model else {
            tracing::debug!("Model {model_name} not found");
            return ModelPredictResponse {
                status: ModelPredictStatus::BadRequest,
                error_message: Some(format!("Model {model_name} not found")),
                model_name,
                lookback,
                forecast: vec![],
                duration_ms: start_time.elapsed().as_millis(),
            };
        };

        let Some(runnable) = models.get(&model.name) else {
            tracing::debug!("Model {model_name} not found");
            return ModelPredictResponse {
                status: ModelPredictStatus::BadRequest,
                error_message: Some(format!("Model {model_name} not found")),
                model_name,
                lookback,
                forecast: vec![],
                duration_ms: start_time.elapsed().as_millis(),
            };
        };

        match runnable.run(df.clone(), lookback).await {
            Ok(inference_result) => {
                if let Some(column_data) = inference_result.column_by_name("y") {
                    if let Some(array) = column_data.as_any().downcast_ref::<Float32Array>() {
                        let result = array.values().iter().copied().collect_vec();
                        return ModelPredictResponse {
                            status: ModelPredictStatus::Success,
                            error_message: None,
                            model_name,
                            lookback,
                            forecast: result,
                            duration_ms: start_time.elapsed().as_millis(),
                        };
                    }
                    tracing::error!(
                        "Unable to cast inference result for model {model_name} to Float32Array: {column_data:?}"
                    );
                    return ModelPredictResponse {
                        status: ModelPredictStatus::InternalError,
                        error_message: Some(
                            "Unable to cast inference result to Float32Array".to_string(),
                        ),
                        model_name,
                        lookback,
                        forecast: vec![],
                        duration_ms: start_time.elapsed().as_millis(),
                    };
                }
                tracing::error!(
                    "Unable to find column 'y' in inference result for model {model_name}"
                );
                ModelPredictResponse {
                    status: ModelPredictStatus::InternalError,
                    error_message: Some(
                        "Unable to find column 'y' in inference result".to_string(),
                    ),
                    model_name,
                    lookback,
                    forecast: vec![],
                    duration_ms: start_time.elapsed().as_millis(),
                }
            }
            Err(e) => {
                tracing::error!("Unable to run inference: {e}");
                ModelPredictResponse {
                    status: ModelPredictStatus::InternalError,
                    error_message: Some(e.to_string()),
                    model_name,
                    lookback,
                    forecast: vec![],
                    duration_ms: start_time.elapsed().as_millis(),
                }
            }
        }
    }
}
