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
            datasets.retain(|d| d.sql.is_none());
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
    use tract_core::tract_data::itertools::Itertools;

    #[derive(Serialize)]
    pub struct ForecastResponse {
        forecast: Vec<f32>,
        duration_ms: u128,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct ModelQueryInfo {
        #[serde(default = "default_lookback")]
        lookback: usize,
    }

    fn default_lookback() -> usize {
        10
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(name): Path<String>,
        Query(params): Query<ModelQueryInfo>,
        Extension(models): Extension<Arc<HashMap<String, Model>>>,
    ) -> Response {
        let start_time = Instant::now();

        let model = app.models.iter().find(|m| m.name == name);

        let Some(model) = model else {
            return (StatusCode::NOT_FOUND, format!("Model {name} not found")).into_response();
        };

        let runnable = match models.get(&model.name) {
            Some(model) => model,
            None => {
                tracing::debug!("Model {name} not found");
                return (StatusCode::NOT_FOUND, format!("Model {name} not found")).into_response();
            }
        };

        match runnable.run(df, params.lookback).await {
            Ok(inference_result) => match inference_result.column_by_name("y") {
                Some(column_data) => match column_data.as_any().downcast_ref::<Float32Array>() {
                    Some(array) => {
                        let result = array.values().iter().copied().collect_vec();
                        (
                            StatusCode::OK,
                            Json(ForecastResponse {
                                forecast: result,
                                duration_ms: start_time.elapsed().as_millis(),
                            }),
                        )
                            .into_response()
                    }
                    None => (StatusCode::INTERNAL_SERVER_ERROR,).into_response(),
                },
                None => (StatusCode::INTERNAL_SERVER_ERROR,).into_response(),
            },
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR,).into_response(),
        }
    }
}
