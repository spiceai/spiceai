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

//! `POST /v1/evaluate` — System One evaluation, by a System One model (`TypeSafe` Jev
//! and similar) or by any chat model.
//!
//! Not a chat-completions endpoint. Request carries `model`, `state`, and a
//! map of typed `questions`; response returns typed `answers` with confidence.

use std::sync::Arc;

use crate::model::metrics::{handle_metrics, handle_token_metrics};
use axum::{
    Extension, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
#[cfg(feature = "openapi")]
use evaluate_api::EvaluateResponse;
use evaluate_api::{Error as EvaluateError, EvaluateRequest};
use opentelemetry::{Key, KeyValue, Value};
use std::time::Instant;
use tokio::sync::RwLock;

use runtime_request_context::{AsyncMarker, RequestContext};
use tracing_futures::Instrument;

use crate::model::{EvaluateModelStore, LLMChatCompletionsModelStore};

/// Evaluate
///
/// Evaluate unstructured `state` against a map of typed System One questions
/// (noul / choice / score). Returns structured answers with probabilities and
/// confidence. `model` names either a System One model (`TypeSafe` Jev), whose
/// probabilities are calibrated, or any chat model, whose probabilities are the
/// model's own estimates. System One models do not support chat completions.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/evaluate",
    operation_id = "post_evaluate",
    tag = "AI",
    request_body = EvaluateRequest,
    responses(
        (status = 200, description = "Evaluation succeeded", body = EvaluateResponse),
        (status = 404, description = "No System One or chat model with this name"),
        (status = 400, description = "Invalid request"),
        (status = 422, description = "Malformed JSON request body (Axum Json extractor)"),
        (status = 401, description = "Upstream authentication failed"),
        (status = 403, description = "Upstream permission denied"),
        (status = 429, description = "Rate limited"),
        (status = 503, description = "Upstream provider unavailable"),
        (status = 500, description = "Evaluation failed")
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<Arc<RwLock<EvaluateModelStore>>>,
    Extension(chat_models): Extension<Arc<RwLock<LLMChatCompletionsModelStore>>>,
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
    // Validated here rather than during deserialization so an empty map returns this
    // endpoint's documented 400 body instead of an Axum extractor rejection.
    if req.questions.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "`questions` must contain at least one question."
            })),
        )
            .into_response();
    }

    let model_id = req.model.clone();
    // Evaluations are inference: they belong in the same request, failure, duration and
    // token series as the chat and responses paths rather than a family of their own.
    // A chat model's evaluation already lands there, since every call it makes to the
    // model — corrective retries included — is recorded by the chat model itself, so
    // recording the evaluation too would count its requests and tokens twice. Both stores
    // are read under one hold, so the evaluator and whether its calls are recorded come
    // from the same moment even while the model is reloaded.
    let (model, recorded_per_call) = {
        let chat_models = chat_models.read().await;
        let models = models.read().await;
        (
            models.get(&model_id).cloned(),
            chat_models.contains_key(&model_id),
        )
    };
    let Some(model) = model else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!(
                    "Model '{model_id}' not found. Evaluate with a chat model or a System One model (e.g. `from: typesafe:jev`) configured in your Spicepod. See: https://spiceai.org/docs/components/models"
                )
            })),
        )
            .into_response();
    };

    let labels = [KeyValue::new(
        Key::new("model"),
        Value::String(model_id.clone().into()),
    )];
    let start = Instant::now();

    let result = model.evaluate(req).await;
    if !recorded_per_call {
        handle_metrics(start.elapsed(), result.is_err(), &labels);
    }

    match result {
        Ok(response) => {
            if let Some(usage) = response.usage.as_ref().filter(|_| !recorded_per_call) {
                handle_token_metrics(
                    u32::try_from(usage.input_tokens).unwrap_or(u32::MAX),
                    u32::try_from(usage.output_tokens).unwrap_or(u32::MAX),
                    &labels,
                );
            }
            // The exporter reads `captured_output` for the row's result and derives
            // `error_message` only from ERROR events, so both are emitted here.
            tracing::info!(
                target: "task_history",
                captured_output = %serde_json::to_string(&response).unwrap_or_default()
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            // Task history copies ERROR event text into `error_message` without
            // the redaction applied to `input` / `captured_output`.
            tracing::error!(target: "task_history", "{}", e.telemetry_message());
            evaluate_error_response(&e)
        }
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
        EvaluateError::ServiceUnavailable { message, .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, message.clone())
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

    fn no_chat_models() -> Arc<RwLock<LLMChatCompletionsModelStore>> {
        Arc::new(RwLock::new(LLMChatCompletionsModelStore::new()))
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
        let response = post(
            Extension(models),
            Extension(no_chat_models()),
            Json(request_with_question("missing")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn evaluate_returns_200_for_registered_model() {
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let models = Arc::new(RwLock::new(store));
        let response = post(
            Extension(models),
            Extension(no_chat_models()),
            Json(request_with_question("jev")),
        )
        .await;
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
            Extension(no_chat_models()),
            Json(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::from("x"),
                questions: BTreeMap::new(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    /// An empty `questions` map must return this endpoint's documented 400 envelope,
    /// not an extractor rejection with a different shape.
    #[tokio::test]
    async fn evaluate_rejects_empty_questions_with_the_documented_body() {
        let models = Arc::new(RwLock::new(EvaluateModelStore::new()));
        let response = post(
            Extension(models),
            Extension(no_chat_models()),
            Json(EvaluateRequest {
                model: "jev".to_string(),
                state: EvaluateState::String("s".to_string()),
                questions: BTreeMap::new(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert!(
            json.get("error").is_some(),
            "must use the documented error envelope: {json}"
        );
    }

    #[derive(Debug)]
    struct UnavailableEvaluate;

    #[async_trait]
    impl Evaluate for UnavailableEvaluate {
        async fn evaluate(
            &self,
            _request: EvaluateRequest,
        ) -> evaluate_api::Result<EvaluateResponse> {
            evaluate_api::ServiceUnavailableSnafu {
                model: "jev",
                message: "upstream 503",
            }
            .fail()
        }
    }

    #[tokio::test]
    async fn evaluate_maps_service_unavailable_to_503() {
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(UnavailableEvaluate));
        let models = Arc::new(RwLock::new(store));
        let response = post(
            Extension(models),
            Extension(no_chat_models()),
            Json(request_with_question("jev")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(json["error"], "upstream 503");
    }

    /// A chat model that replies with a fixed `answers` object.
    struct AnsweringChat;

    #[async_trait]
    impl llms::chat::Chat for AnsweringChat {
        fn as_sql(&self) -> Option<&dyn llms::chat::SqlGeneration> {
            None
        }

        async fn chat_request(
            &self,
            _req: async_openai::types::chat::CreateChatCompletionRequest,
        ) -> Result<
            async_openai::types::chat::CreateChatCompletionResponse,
            async_openai::error::OpenAIError,
        > {
            Ok(serde_json::from_value(serde_json::json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "created": 0,
                "model": "judge",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "{\"answers\": {\"is_urgent\": 0.75}}"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 50, "completion_tokens": 9, "total_tokens": 59}
            }))
            .expect("chat completion"))
        }
    }

    /// A chat model answers `/v1/evaluate` through the evaluator registered for it.
    #[tokio::test]
    async fn evaluate_answers_with_a_chat_model() {
        let chat: Arc<dyn llms::chat::Chat> = Arc::new(AnsweringChat);
        let mut evaluators = EvaluateModelStore::new();
        evaluators.insert(
            "judge".into(),
            crate::model::chat_evaluator("judge", Arc::clone(&chat)),
        );
        let mut chats = LLMChatCompletionsModelStore::new();
        chats.insert("judge".into(), chat);

        let response = post(
            Extension(Arc::new(RwLock::new(evaluators))),
            Extension(Arc::new(RwLock::new(chats))),
            Json(request_with_question("judge")),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(json["model"], "judge");
        assert_eq!(json["answers"]["is_urgent"]["noul"], 0.75);
        assert_eq!(json["usage"]["input_tokens"], 50);
    }
}
