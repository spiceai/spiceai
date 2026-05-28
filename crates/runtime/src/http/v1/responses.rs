use std::sync::Arc;

use crate::http::v1::chat::{KEEP_ALIVE_INTERVAL, OpenaiErrorEvent, openai_error_to_response};
use async_openai::error::{ApiError, OpenAIError};
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
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::{
    Runtime,
    model::{LLMResponsesModelStore, ResponsesApiSupport},
};
use llms::responses::Responses;

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

fn responses_support_gate(model_id: &str, support: &ResponsesApiSupport) -> Option<Response> {
    match support {
        ResponsesApiSupport::UnsupportedProvider { provider } => {
            Some(openai_error_to_response(OpenAIError::ApiError(ApiError {
                message: format!(
                    "Model '{model_id}' uses provider '{provider}' which does not support the OpenAI Responses API. Use /v1/chat/completions for this model or configure a model provider that supports Responses."
                ),
                r#type: Some("invalid_request_error".to_string()),
                param: Some("model".to_string()),
                code: Some("invalid_request_error".to_string()),
            })))
        }
        ResponsesApiSupport::Unavailable => Some(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("model '{model_id}' is unavailable via /v1/responses"),
            )
                .into_response(),
        ),
        ResponsesApiSupport::Supported => None,
    }
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
    Extension(llms): Extension<Arc<RwLock<LLMResponsesModelStore>>>,
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
        return (StatusCode::BAD_REQUEST, "model is required").into_response();
    };
    span.in_scope(
        || tracing::info!(target: "task_history", model = %model_id, api = "responses", "labels"),
    );

    if let Some(traceparent) = context.trace_parent() {
        crate::http::traceparent::override_task_history_with_trace_parent(&span, traceparent);
    }

    let span_clone = span.clone();
    async move {
        let model_id = model_id.clone();
        let stream = req.stream.unwrap_or(false);

        let responses_support = rt.responses_api_support_for_model(&model_id).await;
        if let Some(response) = responses_support_gate(&model_id, &responses_support) {
            return response;
        }

        let Some(model) = llms.read().await.get(&model_id).cloned() else {
            return (StatusCode::NOT_FOUND, format!("model '{model_id}' not found")).into_response();
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

                    openai_error_to_response(e)
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
            return openai_error_to_response(e);
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

    #[tokio::test]
    async fn unsupported_provider_returns_invalid_request_error() {
        let response = responses_support_gate(
            "anthropic_model",
            &ResponsesApiSupport::UnsupportedProvider {
                provider: "anthropic".to_string(),
            },
        )
        .expect("unsupported provider should produce an early response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body).expect("body should be valid json");

        assert_eq!(body_json["type"].as_str(), Some("invalid_request_error"));
        assert_eq!(body_json["param"].as_str(), Some("model"));
        assert_eq!(body_json["code"].as_str(), Some("invalid_request_error"));
    }

    #[tokio::test]
    async fn unavailable_support_returns_service_unavailable() {
        let response = responses_support_gate("temporary_model", &ResponsesApiSupport::Unavailable)
            .expect("unavailable support should produce an early response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should be readable")
            .to_bytes();
        let body_text = String::from_utf8(body.to_vec()).expect("body should be valid utf-8");
        assert!(body_text.contains("temporary_model"));
        assert!(body_text.contains("unavailable via /v1/responses"));
    }
}
