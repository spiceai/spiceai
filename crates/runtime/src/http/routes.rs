use std::sync::Arc;

use app::App;
use axum::{
    routing::{get, Router},
    Extension,
};

use super::v1;

pub(crate) fn routes(app: Arc<App>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok\n" }))
        .route("/v1/datasets", get(v1::datasets::get))
        .layer(Extension(app))
}
