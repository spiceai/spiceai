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
    use std::sync::Arc;
    use std::time::Instant;
    use tract_core::tract_data::itertools::Itertools;

    #[derive(Serialize)]
    pub struct Response {
        forecast: Vec<f32>,
        duration_ms: u128,
    }

    #[debug_handler]
    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(name): Path<String>,
    ) -> Json<Response> {
        let start_time = Instant::now();

        let model = app.models.iter().find(|m| m.name == name);
        if model.is_none() {
            return Json(Response {
                forecast: Vec::new(),
                duration_ms: start_time.elapsed().as_millis(),
            });
        }

        let runnable = Model::load(&(model.unwrap())).unwrap();
        let result: Vec<f32> = runnable
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
            .copied()
            .collect_vec();

        Json(Response {
            forecast: result,
            duration_ms: start_time.elapsed().as_millis(),
        })
    }
}
