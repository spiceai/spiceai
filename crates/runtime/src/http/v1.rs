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
    use arrow::{array::Array, datatypes::Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::array::Float32Array;
    use axum::{extract::Path, Extension, Json};
    use std::sync::Arc;

    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Extension(df): Extension<Arc<DataFusion>>,
        Path(name): Path<String>,
    ) -> Json<Vec<f32>> {
        let model = app.models.iter().find(|m| m.name == name);

        let mut result = RecordBatch::new_empty(Arc::new(Schema::empty()));
        if model.is_some() {
            let runnable = Model::load(&(model.unwrap())).unwrap();

            result = runnable.run(df);
        }
        let result = result.column_by_name("result")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone();

        let mut return_val = Vec::new();
        result.values().iter().for_each(|v| {
            return_val.push(*v);
        });

        Json(return_val)
    }
}
