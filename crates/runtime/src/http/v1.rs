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
    use axum::{debug_handler, extract::Path, Extension, Json};
    use serde::Serialize;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;
    use tract_core::tract_data::itertools::Itertools;

    #[derive(Serialize)]
    pub struct InferenceResponse {
        forecast: Vec<f32>,
        duration_ms: u128,
    }

    #[debug_handler]
    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Extension(models): Extension<Arc<HashMap<String, Model>>>,
        Path(name): Path<String>,
    ) -> Json<InferenceResponse> {
        let start_time = Instant::now();

        let model = models.get(&name);
        if model.is_none() {
            return Json(InferenceResponse {
                forecast: Vec::new(),
                duration_ms: start_time.elapsed().as_millis(),
            });
        }

        let result: Vec<f32> = model
            .unwrap()
            .run(df)
            .await
            .unwrap()
            .column_by_name("y")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|v| *v)
            .collect_vec();

        Json(InferenceResponse {
            forecast: result,
            duration_ms: start_time.elapsed().as_millis(),
        })
    }
}
