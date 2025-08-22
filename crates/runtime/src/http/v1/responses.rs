use std::sync::Arc;

use crate::http::{
    traceparent::override_task_history_with_traceparent,
    v1::chat::{KEEP_ALIVE_INTERVAL, OpenaiErrorEvent, openai_error_to_response},
};
use async_openai::types::responses::{
    Content, CreateResponse, OutputContent, OutputMessage, Response as OpenAIResponse,
    ResponseCompleted, ResponseEvent, ResponseIncomplete, ResponseStream,
};
use axum::{
    Extension, Json,
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::StreamExt;
use itertools::Itertools;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::model::LLMResponsesModelStore;
use llms::responses::Responses;

fn extract_text(resp: &OpenAIResponse) -> String {
    resp.output
        .iter()
        .filter_map(|out| {
            let OutputContent::Message(OutputMessage { content, .. }) = out else {
                return None;
            };
            match content.first()? {
                Content::OutputText(output_text) => Some(output_text.text.clone()),
                Content::Refusal(_) => None,
            }
        })
        .join("\n")
}

#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/responses",
    operation_id = "post_chat_responses",
    tag = "AI",
    request_body(
        description = "Create an Open AI Responses API using a language model.",
        content((
            serde::Value = "application/json",
            example = json!({
                "model": "gpt-4o",
                "input": "You are a helpful assistant.",
                "stream": false
            })
        ))
    ),
    responses(
        (status = 200, description = "Response generated successfully", content((
            serde::Value = "application/json",
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
        (status = 404, description = "The specified model was not found"),
        (status = 500, description = "An internal server error occurred while processing the chat completion", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "An internal server error occurred while processing the chat completion."
            })
        )))
    )
))]
pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMResponsesModelStore>>>,
    headers: HeaderMap,
    Json(req): Json<CreateResponse>,
) -> Response {
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_chat",
        input = %serde_json::to_string(&req).unwrap_or_default()
    );
    span.in_scope(
        || tracing::info!(target: "task_history", model = %req.model, api = "responses", "labels"),
    );

    override_task_history_with_traceparent(&span.clone(), &headers);

    let span_clone = span.clone();
    async move {
        let model_id = req.model.clone();
        let stream = req.stream.unwrap_or(false);

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
                                ResponseEvent::ResponseOutputTextDelta(delta) => {
                                    captured_output.push_str(&delta.delta);
                                    false
                                }
                                ResponseEvent::ResponseIncomplete(ResponseIncomplete {
                                    sequence_number,
                                    ..
                                })
                                | ResponseEvent::ResponseCompleted(ResponseCompleted {
                                    sequence_number,
                                    ..
                                }) => {
                                    if id.is_none() {
                                        id = Some(*sequence_number);
                                    }
                                    true
                                }
                                ResponseEvent::ResponseFailed(_) => true,
                                _ => false,
                            };

                            let event = Ok::<Event, Infallible>(Event::default().data(
                                serde_json::to_string(&response_event).unwrap_or_else(|e| {
                                    format!(r#"{{"error": "Serialization failed: {e}"}}"#)
                                }),
                            ));

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
