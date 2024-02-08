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
    use crate::model::Model;
    use app::App;
    use axum::{extract::Path, Extension, Json};
    use std::sync::Arc;

    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Path(name): Path<String>,
    ) -> Json<Vec<Vec<f32>>> {
        let model = app.models.iter().find(|m| m.name == name);

        let mut result = Vec::new();
        if model.is_some() {
            let runnable = Model::load(&(model.unwrap())).unwrap();

            result = runnable.run();
        }

        Json(result)
    }
}
