pub(crate) mod datasets {
    use std::sync::Arc;

    use app::App;
    use axum::{extract::Query, Extension, Json};
    use serde::Deserialize;
    use spicepod::component::dataset::Dataset;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub(crate) enum DatasetSource {
        Sink,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct DatasetFilter {
        source: Option<DatasetSource>,
    }

    pub(crate) async fn get(
        Extension(app): Extension<Arc<App>>,
        Query(filter): Query<DatasetFilter>,
    ) -> Json<Vec<Dataset>> {
        let datasets: Vec<Dataset> = match filter.source {
            Some(source) => match source {
                DatasetSource::Sink => app.datasets.clone(),
            },
            None => app.datasets.clone(),
        };

        Json(datasets)
    }
}
