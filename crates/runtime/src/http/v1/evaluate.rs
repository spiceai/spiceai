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

use axum::{
    Extension, Json,
    extract::rejection::JsonRejection,
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
        (status = 400, description = "Invalid request: a body that is not an evaluation request, or a question that cannot be answered"),
        (status = 415, description = "The request body is not `application/json`"),
        (status = 401, description = "Upstream authentication failed"),
        (status = 403, description = "Upstream permission denied"),
        (status = 429, description = "Rate limited"),
        (status = 503, description = "Upstream provider unavailable"),
        (status = 500, description = "Evaluation failed")
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<Arc<RwLock<EvaluateModelStore>>>,
    body: Result<Json<EvaluateRequest>, JsonRejection>,
) -> Response {
    // A body that is not an evaluation request is refused in the same envelope, and
    // with the same status, as a request whose questions cannot be answered, rather
    // than as Axum's plain-text rejection.
    let req = match body {
        Ok(Json(req)) => req,
        Err(rejection) => {
            let status = match &rejection {
                JsonRejection::JsonDataError(_) | JsonRejection::JsonSyntaxError(_) => {
                    StatusCode::BAD_REQUEST
                }
                other => other.status(),
            };
            return error_response(status, &rejection.body_text());
        }
    };
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

    // From here on, every response but a 200 emits an ERROR event, which is what marks
    // the `ai_evaluate` row as failed: the exporter derives `error_message` from ERROR
    // events only. A body refused above never starts the task, so it leaves no row, as
    // on `/v1/chat/completions`. Task history copies the ERROR text without the
    // redaction applied to `input` / `captured_output`, so it never carries
    // provider-supplied text.
    async move {
    if let Err(e) = evaluate_api::check_questions(&req.model, &req.questions) {
        tracing::error!(target: "task_history", "{}", e.telemetry_message());
        return evaluate_error_response(&e);
    }

    let model_id = req.model.clone();
    let Some(model) = models.read().await.get(&model_id).cloned() else {
        let message = format!(
            "Model '{model_id}' not found. Evaluate with a model under `models` in your Spicepod: a chat model, or a System One model such as `from: typesafe:jev`. See: https://spiceai.org/docs/components/models"
        );
        tracing::error!(target: "task_history", "{message}");
        return error_response(StatusCode::NOT_FOUND, &message);
    };

    // Request, duration and token metrics are recorded by the model itself, where the
    // inference happens (`ChatWrapper`, or the System One model's wrapper).
    match model.evaluate(req).await {
        Ok(response) => {
            // The exporter reads `captured_output` for the row's result and derives
            // `error_message` only from ERROR events, so both are emitted here.
            tracing::info!(
                target: "task_history",
                captured_output = %serde_json::to_string(&response).unwrap_or_default()
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!(target: "task_history", "{}", e.telemetry_message());
            evaluate_error_response(&e)
        }
    }
    }
    .instrument(span.clone())
    .await
}

/// The error envelope every non-200 response of this endpoint carries.
fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// The status for `err`, with its full message: the model it names and the kind of
/// failure, followed by whatever the provider said.
fn evaluate_error_response(err: &EvaluateError) -> Response {
    let status = match err {
        EvaluateError::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
        EvaluateError::AuthenticationFailed { .. } => StatusCode::UNAUTHORIZED,
        EvaluateError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
        EvaluateError::ModelNotFound { .. } => StatusCode::NOT_FOUND,
        EvaluateError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
        EvaluateError::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        // Acquire failures are controller/internal faults, not provider 429s.
        EvaluateError::RatePermitFailed { .. }
        | EvaluateError::ModelCallFailed { .. }
        | EvaluateError::UnparseableResponse { .. }
        | EvaluateError::HttpClientCreationFailed { .. }
        | EvaluateError::HealthCheckFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };
    error_response(status, &err.to_string())
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

        async fn health(&self) -> evaluate_api::Result<()> {
            Ok(())
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
        let response = post(
            Extension(models),
            Ok(Json(request_with_question("missing"))),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn evaluate_returns_200_for_registered_model() {
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let models = Arc::new(RwLock::new(store));
        let response = post(Extension(models), Ok(Json(request_with_question("jev")))).await;
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
            Ok(Json(EvaluateRequest {
                model: "jev".into(),
                state: EvaluateState::from("x"),
                questions: BTreeMap::new(),
            })),
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
            Ok(Json(EvaluateRequest {
                model: "jev".to_string(),
                state: EvaluateState::String("s".to_string()),
                questions: BTreeMap::new(),
            })),
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

        async fn health(&self) -> evaluate_api::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn evaluate_maps_service_unavailable_to_503() {
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(UnavailableEvaluate));
        let models = Arc::new(RwLock::new(store));
        let response = post(Extension(models), Ok(Json(request_with_question("jev")))).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(
            json["error"],
            "Evaluation provider is unavailable for model 'jev': upstream 503"
        );
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

    /// A chat model answers `/v1/evaluate` through its evaluator.
    #[tokio::test]
    async fn evaluate_answers_with_a_chat_model() {
        let mut evaluators = EvaluateModelStore::new();
        evaluators.insert(
            "judge".into(),
            Arc::new(evaluate_chat::ChatEvaluator::new(
                "judge",
                Arc::new(AnsweringChat),
            )),
        );

        let response = post(
            Extension(Arc::new(RwLock::new(evaluators))),
            Ok(Json(request_with_question("judge"))),
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

    /// The `ai_evaluate` task-history events the handler emits, as `(level, message)`.
    #[derive(Clone, Default)]
    struct TaskHistoryEvents(Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

    impl<S> tracing_subscriber::Layer<S> for TaskHistoryEvents
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message(Option<String>);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = Some(format!("{value:?}"));
                    }
                }
            }

            let in_evaluate_span = ctx
                .event_span(event)
                .is_some_and(|span| span.name() == "ai_evaluate");
            if event.metadata().target() != "task_history" || !in_evaluate_span {
                return;
            }
            let mut message = Message(None);
            event.record(&mut message);
            if let Some(message) = message.0 {
                self.0
                    .lock()
                    .expect("events lock")
                    .push((*event.metadata().level(), message));
            }
        }
    }

    /// Runs `request` through the handler and returns its status, its JSON body, and the
    /// ERROR events it recorded in task history. An ERROR event is what the exporter turns
    /// into the row's `error_message`; a row without one is recorded as a success.
    async fn evaluate_recording_task_history(
        store: EvaluateModelStore,
        request: EvaluateRequest,
    ) -> (StatusCode, serde_json::Value, Vec<String>) {
        use tracing_subscriber::layer::SubscriberExt as _;

        let events = TaskHistoryEvents::default();
        // The handler runs on this thread, so a thread-local subscriber sees all of it.
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));
        let response = post(Extension(Arc::new(RwLock::new(store))), Ok(Json(request))).await;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let json = serde_json::from_slice(&body).expect("json body");
        let errors = events
            .0
            .lock()
            .expect("events lock")
            .iter()
            .filter(|(level, _)| *level == tracing::Level::ERROR)
            .map(|(_, message)| message.clone())
            .collect();
        (status, json, errors)
    }

    // regression test for #14910: a 404 and a refused question were recorded as successes.
    #[tokio::test]
    async fn every_refused_evaluation_is_recorded_as_failed_in_task_history() {
        let (status, json, errors) = evaluate_recording_task_history(
            EvaluateModelStore::new(),
            request_with_question("nope"),
        )
        .await;
        let not_found = "Model 'nope' not found. Evaluate with a model under `models` in your Spicepod: a chat model, or a System One model such as `from: typesafe:jev`. See: https://spiceai.org/docs/components/models";
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json, serde_json::json!({ "error": not_found }));
        assert_eq!(errors, [not_found]);

        let mut one_option = request_with_question("jev");
        one_option.questions = BTreeMap::from([(
            "q".to_string(),
            Question::Choice {
                instructions: "which?".into(),
                criteria: BTreeMap::from([("only".to_string(), "the one".into())]),
            },
        )]);
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let (status, json, errors) = evaluate_recording_task_history(store, one_option).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            json,
            serde_json::json!({ "error": "Invalid evaluation request for model 'jev': choice question 'q' needs at least two options in `criteria`" })
        );
        assert_eq!(
            errors,
            ["Evaluation of model 'jev' failed: invalid request"]
        );

        // The control: a 200 records no error, so the assertions above are not met by
        // every request.
        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let (status, _, errors) =
            evaluate_recording_task_history(store, request_with_question("jev")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            errors.is_empty(),
            "a successful evaluation recorded {errors:?}"
        );
    }

    /// Sends `body` to `/v1/evaluate` through the router, so the JSON extractor runs.
    async fn post_raw(content_type: Option<&str>, body: &str) -> (StatusCode, serde_json::Value) {
        use tower::ServiceExt as _;

        let mut store = EvaluateModelStore::new();
        store.insert("jev".into(), Arc::new(DummyEvaluate { name: "jev".into() }));
        let router = axum::Router::new()
            .route("/v1/evaluate", axum::routing::post(post))
            .layer(Extension(Arc::new(RwLock::new(store))));
        let mut request = axum::http::Request::post("/v1/evaluate");
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = router
            .oneshot(
                request
                    .body(axum::body::Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let json = serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("{status} body is not JSON ({e}): {bytes:?}"));
        (status, json)
    }

    // regression test for #14910: score rubric failures were Axum's plain-text 422, choice
    // failures this endpoint's 400 envelope.
    #[tokio::test]
    async fn every_invalid_request_is_a_400_in_the_error_envelope() {
        const JSON: Option<&str> = Some("application/json");
        let cases = [
            (
                r#"{"model":"jev","state":"s","questions":{"q":{"type":"choice","criteria":{"only":null}}}}"#,
                "Invalid evaluation request for model 'jev': choice question 'q' needs at least two options in `criteria`",
            ),
            (
                r#"{"model":"jev","state":"s","questions":{"q":{"type":"score","criteria":["only"]}}}"#,
                "Invalid evaluation request for model 'jev': score question 'q' needs two to ten levels in `criteria`, but has 1",
            ),
            (
                r#"{"model":"jev","state":"s","questions":{}}"#,
                "Invalid evaluation request for model 'jev': `questions` must contain at least one question",
            ),
            (
                r#"{"model":"jev","state":"s","questions":{"q":{"type":"score","criteria":["low",null]}}}"#,
                "Failed to deserialize the JSON body into the target type: questions.q: a score level must be a string, an object, or an array, not null at line 1 column 85",
            ),
            (
                r#"{"model":"jev","questions":{}}"#,
                "Failed to deserialize the JSON body into the target type: missing field `state` at line 1 column 30",
            ),
            (
                r#"{"model":"jev","#,
                "Failed to parse the request body as JSON: EOF while parsing a value at line 1 column 15",
            ),
        ];
        for (body, error) in cases {
            assert_eq!(
                post_raw(JSON, body).await,
                (
                    StatusCode::BAD_REQUEST,
                    serde_json::json!({ "error": error })
                ),
                "{body}"
            );
        }

        assert_eq!(
            post_raw(None, r#"{"model":"jev"}"#).await,
            (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                serde_json::json!({ "error": "Expected request with `Content-Type: application/json`" })
            )
        );
    }
}
