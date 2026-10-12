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

//! `POST /v1/decisions` — typed decisions in the shape of `OpenAI`'s Decisions API,
//! answered by any model in the Spicepod: a decision model (`TypeSafe` Jev) or a chat
//! model. `OpenAI` SDKs call it with `client.decisions.create(...)` by pointing their
//! base URL at Spice.

use std::sync::Arc;

use axum::{
    Extension, Json,
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use evaluate_api::Error as EvaluateError;
use evaluate_api::openai::DecisionRequest;
#[cfg(feature = "openapi")]
use evaluate_api::openai::DecisionResponse;
use tokio::sync::RwLock;

use runtime_request_context::{AsyncMarker, RequestContext};
use tracing_futures::Instrument;

use crate::model::EvaluateModelStore;

/// Where to read about the models that answer decisions.
const MODELS_DOCS: &str = "https://spiceai.org/docs/components/models";

/// Create a decision
///
/// Answers typed questions about `input`, compatible with `OpenAI`'s Decisions API: a
/// `predicate` returns the probability a statement is true, a `choice` returns the
/// most probable of its `choices` with the distribution and a confidence, and a
/// `score` returns the probability-weighted 0-based index of its `levels`. Answers come
/// back in question order with each question's `name`. `model` names any model in the
/// Spicepod: a decision model such as `TypeSafe` Jev, whose probabilities are
/// calibrated, or a chat model, whose probabilities are its own estimates.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/decisions",
    operation_id = "post_decisions",
    tag = "AI",
    request_body = DecisionRequest,
    responses(
        (status = 200, description = "The answers, in question order", body = DecisionResponse),
        (status = 400, description = "Invalid request, including unknown fields, image inputs, and `reasoning_effort` for a decision model"),
        (status = 401, description = "The model provider rejected the credentials"),
        (status = 403, description = "The model provider denied access"),
        (status = 404, description = "No model with this name"),
        (status = 429, description = "Rate limited"),
        (status = 500, description = "The model could not answer"),
        (status = 503, description = "The model provider is unavailable")
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<Arc<RwLock<EvaluateModelStore>>>,
    body: Bytes,
) -> Response {
    let request: DecisionRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                None,
                None,
                &format!("The request is not a valid decision request: {e}"),
            );
        }
    };

    let context = RequestContext::current(AsyncMarker::new().await);
    // Decisions are billable inference, so they belong in `runtime.task_history` with
    // their model label and trace correlation, like chat completions.
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_decision",
        input = %serde_json::to_string(&request).unwrap_or_default()
    );
    span.in_scope(|| tracing::info!(target: "task_history", model = %request.model, "labels"));
    crate::task_history::correlation::record_task_history_trace_id(&span, &context);

    async move {
        let translated = match request.to_system_one() {
            Ok(translated) => translated,
            Err(invalid) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    Some(&invalid.param),
                    None,
                    &invalid.message,
                );
            }
        };

        let Some(model) = models.read().await.get(&request.model).cloned() else {
            return error_response(
                StatusCode::NOT_FOUND,
                "invalid_request_error",
                Some("model"),
                Some("model_not_found"),
                &format!(
                    "Model '{}' not found. Name a model under `models` in your Spicepod: a decision model such as `from: typesafe:jev`, or any chat model. See: {MODELS_DOCS}",
                    request.model
                ),
            );
        };

        // A decision model has no reasoning effort to set; answering anyway would hide
        // that the level the caller asked for was never applied.
        if request.reasoning_effort.is_some() && model.is_decision_model() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                Some("reasoning_effort"),
                Some("unsupported_parameter"),
                &format!(
                    "Model '{}' is a decision model, which does not take `reasoning_effort`. Omit `reasoning_effort`, or name a chat model to set how much it reasons. See: {MODELS_DOCS}",
                    request.model
                ),
            );
        }

        // Request, duration and token metrics are recorded by the model itself, where the
        // inference happens (`ChatWrapper`, or the decision model's wrapper).
        match model.evaluate(translated.request.clone()).await {
            Ok(answered) => match translated.decision_response(answered) {
                Ok(decision) => {
                    // The exporter reads `captured_output` for the row's result and
                    // derives `error_message` only from ERROR events.
                    tracing::info!(
                        target: "task_history",
                        captured_output = %serde_json::to_string(&decision).unwrap_or_default()
                    );
                    (StatusCode::OK, Json(decision)).into_response()
                }
                Err(detail) => {
                    let message = format!(
                        "Model '{}' returned answers that do not match the questions, so no answer is returned: {detail}",
                        request.model
                    );
                    tracing::error!(target: "task_history", "{message}");
                    error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "server_error",
                        None,
                        None,
                        &message,
                    )
                }
            },
            Err(e) => {
                // Task history copies ERROR event text into `error_message` without the
                // redaction applied to `input` / `captured_output`.
                tracing::error!(target: "task_history", "{}", e.telemetry_message());
                evaluate_error_response(&e)
            }
        }
    }
    .instrument(span.clone())
    .await
}

/// An error in `OpenAI`'s envelope: `{"error": {"message", "type", "param", "code"}}`.
fn error_response(
    status: StatusCode,
    error_type: &str,
    param: Option<&str>,
    code: Option<&str>,
    message: &str,
) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": {
                "message": message,
                "type": error_type,
                "param": param,
                "code": code,
            }
        })),
    )
        .into_response()
}

fn evaluate_error_response(err: &EvaluateError) -> Response {
    match err {
        EvaluateError::InvalidRequest { message, .. } => error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            None,
            None,
            message,
        ),
        EvaluateError::AuthenticationFailed { message, .. } => error_response(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            None,
            Some("invalid_api_key"),
            message,
        ),
        EvaluateError::PermissionDenied { message, .. } => error_response(
            StatusCode::FORBIDDEN,
            "permission_error",
            None,
            None,
            message,
        ),
        EvaluateError::ModelNotFound { message, .. } => error_response(
            StatusCode::NOT_FOUND,
            "invalid_request_error",
            Some("model"),
            Some("model_not_found"),
            message,
        ),
        EvaluateError::RateLimited { message, .. } => error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            None,
            Some("rate_limit_exceeded"),
            message,
        ),
        EvaluateError::ServiceUnavailable { message, .. } => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            None,
            Some("service_unavailable"),
            message,
        ),
        // A permit failure is a fault in the runtime's rate controller, not a provider 429.
        other => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            None,
            None,
            &other.to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use evaluate_api::{Answer, Evaluate, EvaluateRequest, EvaluateResponse, Usage};
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Answers every predicate with 0.91, and records the request it was sent.
    #[derive(Debug, Default)]
    struct DummyEvaluate {
        seen: std::sync::Mutex<Vec<EvaluateRequest>>,
    }

    #[async_trait]
    impl Evaluate for DummyEvaluate {
        async fn evaluate(
            &self,
            request: EvaluateRequest,
        ) -> evaluate_api::Result<EvaluateResponse> {
            self.seen.lock().expect("seen lock").push(request.clone());
            let answers = request
                .questions
                .keys()
                .map(|id| (id.clone(), Answer::Noul { noul: 0.91 }))
                .collect::<BTreeMap<_, _>>();
            Ok(EvaluateResponse {
                model: "jev-test".into(),
                answers,
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    ..Usage::default()
                }),
            })
        }

        async fn health(&self) -> evaluate_api::Result<()> {
            Ok(())
        }

        fn is_decision_model(&self) -> bool {
            true
        }
    }

    async fn call(
        store: EvaluateModelStore,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let response = post(
            Extension(Arc::new(RwLock::new(store))),
            Bytes::from(body.to_string()),
        )
        .await;
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (status, serde_json::from_slice(&bytes).expect("json body"))
    }

    fn store_with(name: &str, model: Arc<dyn Evaluate>) -> EvaluateModelStore {
        let mut store = EvaluateModelStore::new();
        store.insert(name.into(), model);
        store
    }

    #[tokio::test]
    async fn answers_come_back_in_question_order_with_names() {
        let model = Arc::new(DummyEvaluate::default());
        let (status, body) = call(
            store_with("jev", Arc::clone(&model) as Arc<dyn Evaluate>),
            json!({
                "model": "jev",
                "input": "The package arrived with a broken screen.",
                "questions": [
                    {"type": "predicate", "name": "damaged", "instructions": "Is the item damaged?"},
                    {"type": "predicate", "instructions": "Is the customer angry?"}
                ]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "model": "jev-test",
                "answers": [
                    {"type": "predicate", "name": "damaged", "probability": 0.91},
                    {"type": "predicate", "name": null, "probability": 0.91}
                ],
                "usage": {
                    "input_tokens": 10,
                    "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                    "output_tokens": 2,
                    "output_tokens_details": {"reasoning_tokens": 0},
                    "total_tokens": 12
                }
            })
        );
        let seen = model.seen.lock().expect("seen lock");
        assert_eq!(seen[0].model, "jev", "the Spicepod name reaches the model");
        assert_eq!(seen[0].questions.len(), 2);
    }

    #[tokio::test]
    async fn an_unknown_model_is_a_404_naming_it() {
        let (status, body) = call(
            EvaluateModelStore::new(),
            json!({"model": "missing", "input": "x", "questions": [{"type": "predicate", "instructions": "?"}]}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "model_not_found");
        assert_eq!(body["error"]["param"], "model");
        assert_eq!(
            body["error"]["message"],
            "Model 'missing' not found. Name a model under `models` in your Spicepod: a decision model such as `from: typesafe:jev`, or any chat model. See: https://spiceai.org/docs/components/models"
        );
    }

    #[tokio::test]
    async fn invalid_requests_are_400_in_the_openai_envelope() {
        let store = || store_with("jev", Arc::new(DummyEvaluate::default()));
        let cases = [
            (
                json!({"model": "jev", "input": "x", "questions": [{"type": "predicate", "instructions": "?"}], "stream": true}),
                None,
            ),
            (
                json!({"model": "jev", "input": "x", "questions": []}),
                Some("questions"),
            ),
            (
                json!({"model": "jev", "input": [{"role": "user", "content": [{"type": "input_image", "image_url": "data:image/png;base64,AA=="}]}], "questions": [{"type": "predicate", "instructions": "?"}]}),
                Some("input[0].content[0]"),
            ),
        ];
        for (request, param) in cases {
            let (status, body) = call(store(), request.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
            assert_eq!(body["error"]["type"], "invalid_request_error", "{request}");
            assert_eq!(body["error"]["param"].as_str(), param, "{request}");
        }
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

        async fn health(&self) -> evaluate_api::Result<()> {
            Ok(())
        }

        fn is_decision_model(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn an_unavailable_provider_is_a_503() {
        let (status, body) = call(
            store_with("jev", Arc::new(UnavailableEvaluate)),
            json!({"model": "jev", "input": "x", "questions": [{"type": "predicate", "instructions": "?"}]}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            json!({"error": {"message": "upstream 503", "type": "server_error", "param": null, "code": "service_unavailable"}})
        );
    }

    /// A chat model that replies with a fixed `answers` object, and records the
    /// reasoning effort of every request it is sent.
    #[derive(Default)]
    struct AnsweringChat {
        efforts: std::sync::Mutex<Vec<Option<async_openai::types::chat::ReasoningEffort>>>,
    }

    #[async_trait]
    impl llms::chat::Chat for AnsweringChat {
        fn as_sql(&self) -> Option<&dyn llms::chat::SqlGeneration> {
            None
        }

        async fn chat_request(
            &self,
            req: async_openai::types::chat::CreateChatCompletionRequest,
        ) -> Result<
            async_openai::types::chat::CreateChatCompletionResponse,
            async_openai::error::OpenAIError,
        > {
            self.efforts
                .lock()
                .expect("efforts lock")
                .push(req.reasoning_effort);
            Ok(serde_json::from_value(json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "created": 0,
                "model": "judge",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "{\"answers\": {\"q000\": {\"billing\": 0.75, \"technical\": 0.25}}}"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 50, "completion_tokens": 9, "total_tokens": 59}
            }))
            .expect("chat completion"))
        }
    }

    /// A chat model answers through its evaluator, and its choice comes back typed.
    #[tokio::test]
    async fn a_chat_model_answers_a_choice() {
        let (status, body) = call(
            store_with(
                "judge",
                Arc::new(evaluate_chat::ChatEvaluator::new(
                    "judge",
                    Arc::new(AnsweringChat::default()),
                )),
            ),
            json!({
                "model": "judge",
                "input": "My payout failed",
                "questions": [{"type": "choice", "name": "team", "instructions": "Which team?", "choices": [{"value": "technical"}, {"value": "billing", "description": "Payments"}]}]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["model"], "judge");
        assert_eq!(body["answers"][0]["type"], "choice");
        assert_eq!(body["answers"][0]["name"], "team");
        assert_eq!(body["answers"][0]["choice"], "billing");
        assert_eq!(
            body["answers"][0]["probabilities"],
            json!([{"value": "technical", "probability": 0.25}, {"value": "billing", "probability": 0.75}])
        );
        assert_eq!(body["usage"]["input_tokens"], 50);
    }

    /// A chat model receives the requested level on its completion request.
    #[tokio::test]
    async fn reasoning_effort_reaches_a_chat_model() {
        let chat = Arc::new(AnsweringChat::default());
        let (status, body) = call(
            store_with(
                "judge",
                Arc::new(evaluate_chat::ChatEvaluator::new(
                    "judge",
                    Arc::clone(&chat) as Arc<dyn llms::chat::Chat>,
                )),
            ),
            json!({
                "model": "judge",
                "input": "My payout failed",
                "questions": [{"type": "choice", "name": "team", "instructions": "Which team?", "choices": [{"value": "technical"}, {"value": "billing", "description": "Payments"}]}],
                "reasoning_effort": "high"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["answers"][0]["choice"], "billing");
        assert_eq!(
            *chat.efforts.lock().expect("efforts lock"),
            vec![Some(async_openai::types::chat::ReasoningEffort::High)]
        );
    }

    /// A decision model has no reasoning effort to set, so a request that sets one is
    /// refused before the model is called.
    #[tokio::test]
    async fn reasoning_effort_for_a_decision_model_is_a_400() {
        let model = Arc::new(DummyEvaluate::default());
        let (status, body) = call(
            store_with("jev", Arc::clone(&model) as Arc<dyn Evaluate>),
            json!({
                "model": "jev",
                "input": "x",
                "questions": [{"type": "predicate", "instructions": "?"}],
                "reasoning_effort": "high"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            json!({"error": {
                "message": "Model 'jev' is a decision model, which does not take `reasoning_effort`. Omit `reasoning_effort`, or name a chat model to set how much it reasons. See: https://spiceai.org/docs/components/models",
                "type": "invalid_request_error",
                "param": "reasoning_effort",
                "code": "unsupported_parameter"
            }})
        );
        assert_eq!(
            model.seen.lock().expect("seen lock").len(),
            0,
            "the model is never called"
        );
    }
}
