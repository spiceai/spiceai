/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! `POST /v1/evaluate` — System One evaluation (TypeSafe Jev and similar).
//!
//! Not a chat-completions endpoint. Request carries `model`, `state`, and a
//! map of typed `questions`; response returns typed `answers` with confidence.

use std::sync::Arc;

use axum::{
    Extension, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
#[cfg(feature = "openapi")]
use evaluate_api::EvaluateResponse;
use evaluate_api::{Error as EvaluateError, EvaluateRequest};
use tokio::sync::RwLock;

use crate::model::EvaluateModelStore;

/// Evaluate
///
/// Evaluate unstructured `state` against a map of typed System One questions
/// (noul / choice / score). Returns structured answers with calibrated
/// probabilities and confidence. Chat completions are not supported for these
/// models — use this endpoint instead of `/v1/chat/completions`.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/evaluate",
    operation_id = "post_evaluate",
    tag = "AI",
    request_body = EvaluateRequest,
    responses(
        (status = 200, description = "Evaluation succeeded", body = EvaluateResponse),
        (status = 404, description = "Model not found"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Upstream authentication failed"),
        (status = 429, description = "Rate limited"),
        (status = 500, description = "Evaluation failed")
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<Arc<RwLock<EvaluateModelStore>>>,
    Json(req): Json<EvaluateRequest>,
) -> Response {
    let model_id = req.model.clone();
    let Some(model) = models.read().await.get(&model_id).cloned() else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!(
                    "Evaluation model '{model_id}' not found. Configure a System One model (e.g. `from: typesafe:jev`) in your Spicepod."
                )
            })),
        )
            .into_response();
    };

    match model.evaluate(req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => evaluate_error_response(e),
    }
}

fn evaluate_error_response(err: EvaluateError) -> Response {
    let (status, message) = match &err {
        EvaluateError::InvalidRequest { message, .. } => (StatusCode::BAD_REQUEST, message.clone()),
        EvaluateError::AuthenticationFailed { message, .. } => {
            (StatusCode::UNAUTHORIZED, message.clone())
        }
        EvaluateError::ModelNotFound { message, .. } => (StatusCode::NOT_FOUND, message.clone()),
        EvaluateError::RateLimited { message, .. } => {
            (StatusCode::TOO_MANY_REQUESTS, message.clone())
        }
        EvaluateError::RatePermitFailed { .. } => (StatusCode::TOO_MANY_REQUESTS, err.to_string()),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    };
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}
