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
    use std::sync::Arc;
    use std::collections::HashMap;

    #[debug_handler]
    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(name): Path<String>,
    ) -> Json<HashMap<String, Vec<f32>>> {
        let model = app.models.iter().find(|m| m.name == name);

        let mut final_result: HashMap<String, Vec<f32>> = HashMap::new();

        if model.is_none() {
            return Json(final_result);
        }

        let runnable = Model::load(&(model.unwrap())).unwrap();
        let result = runnable.run(df);
        let a = result
            .await
            .column_by_name("y")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone();

        let mut return_val = Vec::new();
        a.values().iter().for_each(|v| {
            return_val.push(*v);
        });

        final_result.insert("forecast".to_string(), return_val);

        Json(final_result)
    }
}
