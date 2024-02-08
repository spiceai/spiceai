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
    use app::App;
    use std::collections::HashMap;
    use std::sync::Arc;
    use axum::{Extension, Json, extract::Path};
    use crate::model::Model;

    pub(crate) async fn get(Extension(app): Extension<Arc<App>>, Path(name): Path<String>) -> Json<HashMap<String, String>> {
        tracing::info!("app models: {:?}", app.models);

        let model = app
            .models
            .iter()
            .find(|m| m.name == name);

        if model.is_none() {
            return Json(HashMap::new());
        } else {
            let _ = Model::load(&(model.unwrap())).unwrap();
        }

        Json(HashMap::new())
    }
}
