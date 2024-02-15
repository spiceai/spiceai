use std::{collections::HashMap, sync::Arc};

use crate::{datafusion::DataFusion, model::Model};
use app::App;

use axum::{
    routing::{get, post, Router},
    Extension,
};

use super::v1;

pub(crate) fn routes(
    app: Arc<App>,
    df: Arc<DataFusion>,
    models: Arc<HashMap<String, Model>>,
) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/datasets", get(v1::datasets::get))
        .route("/v1/models/predict", post(v1::inference::post))
        .layer(Extension(app))
        .layer(Extension(df))
        .layer(Extension(models))
}
