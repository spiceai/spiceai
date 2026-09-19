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

//! `POST /v1/evaluate` — System One evaluation (`TypeSafe` Jev and similar).
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

use runtime_request_context::{AsyncMarker, RequestContext};
use tracing_futures::Instrument;

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
        (status = 422, description = "Malformed JSON request body (Axum Json extractor)"),
        (status = 401, description = "Upstream authentication failed"),
        (status = 403, description = "Upstream permission denied"),
        (status = 429, description = "Rate limited"),
        (status = 500, description = "Evaluation failed")
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<Arc<RwLock<EvaluateModelStore>>>,
    Json(req): Json<EvaluateRequest>,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);

    // Mirrors `/v1/chat/completions`: evaluations are billable inference and belong in
    // `runtime.task_history` with their model label and trace correlation.
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_evaluate",
        input = %serde_json::to_string(&req).unwrap_or_default()
    );
    span.in_scope(|| tracing::info!(target: "task_history", model = %req.model, "labels"));
    crate::task_history::correlation::record_task_history_trace_id(&span, &context);

    async move {
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
        Err(e) => evaluate_error_response(&e),
    }
    }
    .instrument(span.clone())
    .await
}

fn evaluate_error_response(err: &EvaluateError) -> Response {
    let (status, message) = match err {
        EvaluateError::InvalidRequest { message, .. } => (StatusCode::BAD_REQUEST, message.clone()),
        EvaluateError::AuthenticationFailed { message, .. } => {
            (StatusCode::UNAUTHORIZED, message.clone())
        }
        EvaluateError::PermissionDenied { message, .. } => (StatusCode::FORBIDDEN, message.clone()),
        EvaluateError::ModelNotFound { message, .. } => (StatusCode::NOT_FOUND, message.clone()),
        EvaluateError::RateLimited { message, .. } => {
            (StatusCode::TOO_MANY_REQUESTS, message.clone())
        }
        // Acquire failures are controller/internal faults, not provider 429s.
        EvaluateError::RatePermitFailed { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        }
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    };
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use evaluate_api::{
        Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, Question, Usage,
    };
    use http_body_util::BodyExt;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[derive(Debug)]
    struct DummyEvaluate {
        name: String,
    }

    #[async_trait]
    impl Evaluate for DummyEvaluate {
        async fn evaluate(
            &self,
            request: EvaluateRequest,
        ) -> evaluate_api::Result<EvaluateResponse> {
            if request.questions.is_empty() {
                return evaluate_api::InvalidRequestSnafu {
                    model: self.name.clone(),
                    message: "empty questions",
                }
                .fail();
            }
            let mut answers = BTreeMap::new();
            answers.insert("is_urgent".to_string(), Answer::Noul { noul: 0.91 });
            Ok(EvaluateResponse {
                model: "jev-test".into(),
                answers,
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                }),
            })
        }
    }

    fn request_with_question(model: &str) -> EvaluateRequest {
        let mut questions = BTreeMap::new();
        questions.insert(
            "is_urgent".into(),
            Question::Noul {
                instructions: "urgent?".into(),
                criteria: None,
            },
        );
        EvaluateRequest {
            model: model.into(),
            state: EvaluateState::from("hello"),
            questions,
        }
    }

    #[tokio::test]
    async fn evaluate_returns_404_for_unknown_model() {
        let models = Arc::new(RwLock::new(EvaluateModelStore::new()));
        let response = post(Extension(models), Json(request_with_question("missing"))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn evaluate_returns_200_for_registered_model() {
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let models = Arc::new(RwLock::new(store));
        let response = post(Extension(models), Json(request_with_question("jev"))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["model"], "jev-test");
        assert_eq!(body["answers"]["is_urgent"]["noul"], 0.91);
    }

    #[tokio::test]
    async fn evaluate_maps_invalid_request_to_400() {
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let models = Arc::new(RwLock::new(store));
        let response = post(
            Extension(models),
            Json(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::from("x"),
                questions: BTreeMap::new(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
