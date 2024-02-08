use std::sync::Arc;

use crate::datafusion::DataFusion;
use app::App;

use axum::{
    routing::{get, Router},
    Extension,
};

use super::v1;

pub(crate) fn routes(app: Arc<App>, df: Arc<DataFusion>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/datasets", get(v1::datasets::get))
        .route("/v1/models/:name/inference", get(v1::inference::get))
        .layer(Extension(app))
        .layer(Extension(df))
}
