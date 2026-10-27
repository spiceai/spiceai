use std::sync::Arc;

use crate::http::v1::chat::{KEEP_ALIVE_INTERVAL, OpenaiErrorEvent, api_error_status};
use async_openai::error::{ApiError, OpenAIError, WrappedError};
use async_openai::traits::EventType;
use async_openai::types::responses::{
    CreateResponse, OutputItem, OutputMessageContent, Response as OpenAIResponse,
    ResponseCompletedEvent, ResponseIncompleteEvent, ResponseStream, ResponseStreamEvent,
};
use axum::{
    Extension, Json,
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::StreamExt;
use itertools::Itertools;
use runtime_request_context::{AsyncMarker, RequestContext};
use std::convert::Infallible;
use std::time::Duration;
use tracing::{Instrument, Span};

use crate::{Runtime, status::RuntimeStatus};
use llms::responses::Responses;
use model_capabilities::Refusal;

fn extract_text(resp: &OpenAIResponse) -> String {
    resp.output
        .iter()
        .filter_map(|out| {
            let OutputItem::Message(msg) = out else {
                return None;
            };
            match msg.content.first()? {
                OutputMessageContent::OutputText(output_text) => Some(output_text.text.clone()),
                OutputMessageContent::Refusal(_) => None,
            }
        })
        .join("\n")
}

/// An error in the `OpenAI` API's envelope, `{"error": {...}}`, which its SDKs read on every
/// error response.
fn error_envelope(
    status: StatusCode,
    message: impl Into<String>,
    r#type: &str,
    param: Option<&str>,
    code: Option<&str>,
) -> Response {
    api_error_envelope(
        status,
        ApiError {
            message: message.into(),
            r#type: Some(r#type.to_string()),
            param: param.map(str::to_string),
            code: code.map(str::to_string),
        },
    )
}

fn api_error_envelope(status: StatusCode, error: ApiError) -> Response {
    (status, Json(WrappedError { error })).into_response()
}

/// A model's error, in the `OpenAI` API's envelope. An error that is not the provider's own
/// answer is reported without its detail, which can name the provider's address; it is in
/// the runtime's log.
fn openai_error_envelope(error: OpenAIError) -> Response {
    match error {
        OpenAIError::ApiError(api_error) => {
            api_error_envelope(api_error_status(&api_error), api_error)
        }
        OpenAIError::InvalidArgument(message) => error_envelope(
            StatusCode::BAD_REQUEST,
            message,
            "invalid_request_error",
            None,
            None,
        ),
        _ => error_envelope(
            StatusCode::INTERNAL_SERVER_ERROR,
            "The model failed to answer. The runtime's log has the cause.",
            "server_error",
            None,
            None,
        ),
    }
}

/// `models` as an English list of quoted names: `'a', 'b' and 'c'`.
fn quoted_list(models: &[String]) -> String {
    let quoted: Vec<String> = models.iter().map(|model| format!("'{model}'")).collect();
    match quoted.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// The response to a request naming a model that has no Responses API handler. `served` is
/// every model `/v1/responses` serves, sorted, which a request naming no loaded model is told.
fn responses_refusal(
    model_id: &str,
    refusal: &Refusal,
    status: &RuntimeStatus,
    served: &[String],
) -> Response {
    match refusal {
        // A chat model never reaches here: it is answered through its chat completions.
        Refusal::Unsupported(capabilities) => {
            let provider = capabilities.provider();
            error_envelope(
                StatusCode::BAD_REQUEST,
                format!(
                    "Model '{model_id}' uses provider '{provider}' which is a decision model and does not support the OpenAI Responses API or chat completions. Use POST /v1/decisions, or the SQL decision functions such as ai_if, instead."
                ),
                "invalid_request_error",
                Some("model"),
                Some("invalid_request_error"),
            )
        }
        Refusal::Unavailable => error_envelope(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("model '{model_id}' is unavailable via /v1/responses"),
            "service_unavailable_error",
            Some("model"),
            Some("service_unavailable"),
        ),
        Refusal::NotFound => {
            let message = status.unavailable_model_reason(model_id).unwrap_or_else(|| {
                if served.is_empty() {
                    format!(
                        "Model '{model_id}' is not in this Spicepod, which serves no chat model. Add one under `models`. See: https://spiceai.org/docs/components/models"
                    )
                } else {
                    format!(
                        "Model '{model_id}' is not in this Spicepod, which serves {}. Name one of them. See: https://spiceai.org/docs/components/models",
                        quoted_list(served)
                    )
                }
            });
            error_envelope(
                StatusCode::NOT_FOUND,
                message,
                "invalid_request_error",
                Some("model"),
                Some("model_not_found"),
            )
        }
    }
}

/// The models `/v1/responses` serves, sorted, for an error that has to name them.
async fn served_models(rt: &Runtime) -> Vec<String> {
    let mut served: Vec<String> = rt
        .responses_supported_model_names()
        .await
        .into_iter()
        .collect();
    served.sort_unstable();
    served
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/responses",
    operation_id = "post_responses",
    tag = "AI",
    request_body(
        description = "Create a response using the OpenAI Responses API format. This endpoint provides a more flexible conversation interface compared to Chat Completions.",
        content((
            CreateResponse = "application/json",
            example = json!({
                "model": "gpt-4o",
                "input": "You are a helpful assistant.",
                "stream": false
            })
        ))
    ),
    responses(
        (status = 200, description = "Response generated successfully", content((
            OpenAIResponse = "application/json",
            example = json!({
                "created_at": 1_755_639_134,
                "id": "resp_68a4ed5e2258819485ece563a803bbf2075163a5e5b1c982",
                "metadata": {},
                "model": "test",
                "object": "response",
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {
                                "type": "output_text",
                                "annotations": [],
                                "text": "Thank you! How can I assist you today?"
                            }
                        ],
                        "id": "msg_68a4ed5eb7e88194bf0b2560d8b5c0c1075163a5e5b1c982",
                        "role": "assistant",
                        "status": "completed"
                    }
                ],
                "parallel_tool_calls": true,
                "reasoning": {},
                "store": true,
                "service_tier": "default",
                "status": "completed",
                "temperature": 1.0,
                "text": {
                    "format": {
                        "type": "text"
                    }
                },
                "tool_choice": "auto",
                "tools": [],
                "top_p": 1.0,
                "truncation": "disabled",
                "usage": {
                    "input_tokens": 13,
                    "input_tokens_details": {
                        "audio_tokens": null,
                        "cached_tokens": 0
                    },
                    "output_tokens": 11,
                    "output_tokens_details": {
                        "accepted_prediction_tokens": null,
                        "audio_tokens": null,
                        "reasoning_tokens": 0,
                        "rejected_prediction_tokens": null
                    },
                    "total_tokens": 24
                }
            })
        ))),
        (status = 400, description = "The specified model provider does not support the Responses API or the request is invalid"),
        (status = 404, description = "The specified model was not found"),
        (status = 503, description = "The specified model is unavailable via the Responses API", content((
            serde_json::Value = "application/json",
            example = json!({
                "message": "model 'my_model' is unavailable via /v1/responses",
                "type": "service_unavailable_error",
                "param": "model",
                "code": "service_unavailable"
            })
        ))),
        (status = 500, description = "An internal server error occurred while processing the response", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "An internal server error occurred while processing the response."
            })
        )))
    )
))]
pub(crate) async fn post(
    Extension(rt): Extension<Arc<Runtime>>,
    Json(req): Json<CreateResponse>,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);

    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_chat",
        input = %serde_json::to_string(&req).unwrap_or_default()
    );

    let Some(model_id) = req.model.clone() else {
        return error_envelope(
            StatusCode::BAD_REQUEST,
            "`model` is required: name one of the Spicepod's models.",
            "invalid_request_error",
            Some("model"),
            Some("missing_required_parameter"),
        );
    };
    span.in_scope(
        || tracing::info!(target: "task_history", model = %model_id, api = "responses", "labels"),
    );

    crate::task_history::correlation::record_task_history_trace_id(&span, &context);

    let span_clone = span.clone();
    async move {
        let model_id = model_id.clone();
        let stream = req.stream.unwrap_or(false);

        let model = match rt.responses_model(&model_id).await {
            Ok(model) => model,
            Err(refusal) => {
                // Only a request naming no loaded model is told which ones are served.
                let served = if matches!(refusal, Refusal::NotFound) {
                    served_models(&rt).await
                } else {
                    Vec::new()
                };
                return responses_refusal(&model_id, &refusal, &rt.status(), &served);
            }
        };

        if stream {
            // Streaming response
            create_response_sse_response(model, req, span_clone).await
        } else {
            // Non-streaming response
            match model.responses_request(req).await {
                Ok(response) => {
                    let message = extract_text(&response);
                    if !message.is_empty() {
                        tracing::info!(target: "task_history", parent: &span_clone, captured_output = %message);
                    }
                    tracing::info!(target: "task_history", parent: &span_clone,  id = %response.id, "labels");

                    Json(response).into_response()
                }
                Err(e) => {
                    tracing::error!(target: "task_history", parent: &span_clone, "{e}");

                    openai_error_envelope(e)
                }
            }
        }
    }
    .instrument(span)
    .await
}

fn to_openai_error_event(err: impl Into<String>) -> Event {
    Event::default().event("error").data(
        serde_json::to_string(&OpenaiErrorEvent::new(err))
            .unwrap_or_else(|_| r#"{"error": "Failed to serialize error"}"#.to_string()),
    )
}

/// Create a SSE [`axum::response::Response`] from a [`ResponseStream`].
async fn create_response_sse_response(
    model: Arc<dyn Responses>,
    req: CreateResponse,
    span: Span,
) -> Response {
    let strm: ResponseStream = match model.responses_stream(req).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return openai_error_envelope(e);
        }
    };

    let sse_stream = {
        let span_clone = span.clone();
        futures::stream::unfold(
            (strm, String::new(), None::<u64>, span_clone),
            move |(mut strm, mut captured_output, mut id, span)| async move {
                if let Some(msg) = strm.next().instrument(span.clone()).await {
                    match msg {
                        Ok(response_event) => {
                            let should_break = match &response_event {
                                ResponseStreamEvent::ResponseOutputTextDelta(delta) => {
                                    captured_output.push_str(&delta.delta);
                                    false
                                }
                                ResponseStreamEvent::ResponseIncomplete(
                                    ResponseIncompleteEvent {
                                        sequence_number, ..
                                    },
                                )
                                | ResponseStreamEvent::ResponseCompleted(
                                    ResponseCompletedEvent {
                                        sequence_number, ..
                                    },
                                ) => {
                                    if id.is_none() {
                                        id = Some(*sequence_number);
                                    }
                                    true
                                }
                                ResponseStreamEvent::ResponseFailed(_) => true,
                                _ => false,
                            };

                            let event = Ok::<Event, Infallible>(
                                Event::default().event(response_event.event_type()).data(
                                    serde_json::to_string(&response_event).unwrap_or_else(|e| {
                                        format!(r#"{{"error": "Serialization failed: {e}"}}"#)
                                    }),
                                ),
                            );

                            if should_break {
                                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                                if let Some(id) = id {
                                    tracing::info!(target: "task_history", parent: &span, id = %id, "labels");
                                }
                                Some((event, (strm, captured_output, id, span)))
                            } else {
                                Some((event, (strm, captured_output, id, span)))
                            }
                        }
                        Err(e) => {
                            let event = Ok(to_openai_error_event(e.to_string()));
                            tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                            if let Some(id) = id {
                                tracing::info!(target: "task_history", parent: &span, id = %id, "labels");
                            }
                            Some((event, (strm, captured_output, id, span)))
                        }
                    }
                } else {
                    tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                    if let Some(id) = id {
                        tracing::info!(target: "task_history", parent: &span, id = %id, "labels");
                    }
                    None
                }
            },
        )
    };

    Sse::new(Box::pin(sse_stream))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(KEEP_ALIVE_INTERVAL)))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::{
        error::OpenAIError,
        types::responses::{CreateResponse, Response as OpenAIResponse, ResponseStream},
    };
    use async_trait::async_trait;
    use futures::stream;
    use http_body_util::BodyExt;
    use llms::responses::Responses;
    use serde_json::json;
    use tracing::Span;

    struct DummyResponses {
        events: Vec<ResponseStreamEvent>,
    }

    #[async_trait]
    impl Responses for DummyResponses {
        async fn health(&self) -> llms::responses::Result<()> {
            Ok(())
        }

        async fn responses_stream(
            &self,
            _req: CreateResponse,
        ) -> Result<ResponseStream, OpenAIError> {
            let events: Vec<Result<ResponseStreamEvent, OpenAIError>> =
                self.events.iter().cloned().map(Ok).collect();
            Ok(Box::pin(stream::iter(events)))
        }

        async fn responses_request(
            &self,
            _req: CreateResponse,
        ) -> Result<OpenAIResponse, OpenAIError> {
            unimplemented!()
        }
    }

    fn minimal_response_json(status: &str) -> serde_json::Value {
        json!({
            "created_at": 1_755_639_134,
            "id": "resp_test",
            "model": "test-model",
            "object": "response",
            "output": [],
            "status": status
        })
    }

    /// Collects the SSE response body and returns each event as a (`event_name`, `data_json`) pair.
    async fn collect_sse_events(
        events: Vec<ResponseStreamEvent>,
    ) -> Vec<(String, serde_json::Value)> {
        let model: Arc<dyn Responses> = Arc::new(DummyResponses { events });
        let req: CreateResponse = serde_json::from_value(json!({
            "model": "test-model",
            "input": "hello"
        }))
        .expect("request should deserialize");

        let response = create_response_sse_response(model, req, Span::none()).await;
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("should collect SSE response body")
            .to_bytes();
        let body_str = String::from_utf8(body_bytes.to_vec()).expect("body should be UTF-8");

        body_str
            .split("\n\n")
            .filter(|block| !block.trim().is_empty())
            .map(|block| {
                let mut event_name = String::new();
                let mut data_str = String::new();
                for line in block.lines() {
                    if let Some(name) = line.strip_prefix("event: ") {
                        event_name = name.to_string();
                    } else if let Some(data) = line.strip_prefix("data: ") {
                        data_str = data.to_string();
                    }
                }
                let data_json: serde_json::Value =
                    serde_json::from_str(&data_str).unwrap_or(serde_json::Value::Null);
                (event_name, data_json)
            })
            .collect()
    }

    #[tokio::test]
    async fn sse_events_include_event_name_field() {
        let created: ResponseStreamEvent = serde_json::from_value(json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": minimal_response_json("in_progress")
        }))
        .expect("created event should deserialize");

        let delta: ResponseStreamEvent = serde_json::from_value(json!({
            "type": "response.output_text.delta",
            "sequence_number": 1,
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "delta": "hi"
        }))
        .expect("delta event should deserialize");

        let completed: ResponseStreamEvent = serde_json::from_value(json!({
            "type": "response.completed",
            "sequence_number": 2,
            "response": minimal_response_json("completed")
        }))
        .expect("completed event should deserialize");

        let events = collect_sse_events(vec![created, delta, completed]).await;

        // Every SSE frame must carry an event: name
        for (name, _) in &events {
            assert!(!name.is_empty(), "SSE event: field must not be empty");
        }

        // The event: name must match the type field in the JSON body
        for (name, data) in &events {
            let json_type = data["type"]
                .as_str()
                .expect("data should have a type field");
            assert_eq!(
                name, json_type,
                "SSE event: field '{name}' must match JSON type field '{json_type}'"
            );
        }

        // Verify the specific event names nemoclaw probes for
        let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"response.output_text.delta"),
            "stream must include a response.output_text.delta SSE event"
        );
        assert!(
            names.contains(&"response.completed"),
            "stream must include a response.completed SSE event"
        );
    }

    fn capabilities(from: &str) -> model_capabilities::ModelCapabilities {
        model_capabilities::ModelCapabilities::of(&spicepod::component::model::Model::new(
            from, "model",
        ))
        .expect("a known provider")
    }

    #[tokio::test]
    async fn unsupported_provider_returns_invalid_request_error() {
        let response = responses_refusal(
            "anthropic_model",
            &Refusal::Unsupported(capabilities("anthropic:claude-3-5-sonnet")),
            &RuntimeStatus::new(),
            &[],
        );

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body).expect("body should be valid json");

        assert_eq!(
            body_json["error"]["type"].as_str(),
            Some("invalid_request_error")
        );
        assert_eq!(body_json["error"]["param"].as_str(), Some("model"));
        assert_eq!(
            body_json["error"]["code"].as_str(),
            Some("invalid_request_error")
        );
    }

    #[tokio::test]
    async fn unavailable_support_returns_service_unavailable() {
        let response = responses_refusal(
            "temporary_model",
            &Refusal::Unavailable,
            &RuntimeStatus::new(),
            &[],
        );

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body).expect("body should be valid json");

        assert_eq!(
            body_json["error"]["message"].as_str(),
            Some("model 'temporary_model' is unavailable via /v1/responses")
        );
        assert_eq!(
            body_json["error"]["type"].as_str(),
            Some("service_unavailable_error")
        );
        assert_eq!(body_json["error"]["param"].as_str(), Some("model"));
        assert_eq!(
            body_json["error"]["code"].as_str(),
            Some("service_unavailable")
        );
    }

    #[tokio::test]
    async fn a_model_not_in_the_spicepod_is_told_which_ones_are() {
        let response = responses_refusal(
            "gpt-5",
            &Refusal::NotFound,
            &RuntimeStatus::new(),
            &["claude".to_string(), "qwen".to_string()],
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body).expect("body should be valid json");
        assert_eq!(
            body_json,
            serde_json::json!({"error": {
                "message": "Model 'gpt-5' is not in this Spicepod, which serves 'claude' and 'qwen'. Name one of them. See: https://spiceai.org/docs/components/models",
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found",
            }})
        );
    }

    #[tokio::test]
    async fn evaluate_only_provider_directs_to_decisions() {
        let response = responses_refusal(
            "jev_model",
            &Refusal::Unsupported(capabilities("typesafe:jev")),
            &RuntimeStatus::new(),
            &[],
        );

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body).expect("body should be valid json");

        assert_eq!(
            body_json["error"]["type"].as_str(),
            Some("invalid_request_error")
        );
        assert_eq!(body_json["error"]["param"].as_str(), Some("model"));
        let message = body_json["error"]["message"].as_str().expect("message");
        assert!(
            message.contains("/v1/decisions"),
            "message should direct callers to /v1/decisions: {message}"
        );
        assert!(
            !message.contains("/v1/chat/completions"),
            "message must not suggest chat for evaluate-only providers: {message}"
        );
    }
}
