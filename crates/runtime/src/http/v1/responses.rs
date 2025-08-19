use std::sync::Arc;

use crate::http::{
    traceparent::override_task_history_with_traceparent, v1::chat::openai_error_to_response,
};
use async_openai::types::responses::{
    Content, CreateResponse, OutputContent, Response as OpenAIResponse, ResponseEvent,
    ResponseStream,
};
use async_stream::stream;
use axum::{
    Extension, Json,
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::StreamExt;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::model::LLMResponsesModelStore;
use llms::responses::Responses;
fn extract_text(resp: &OpenAIResponse) -> Option<String> {
    resp.output
        .first()
        .and_then(|out| {
            if let OutputContent::Message(msg) = out {
                msg.content.first()
            } else {
                None
            }
        })
        .and_then(|content| match content {
            Content::OutputText(output_text) => Some(output_text.text.clone()),
            Content::Refusal(_) => None,
        })
}

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
    span.in_scope(|| tracing::info!(target: "task_history", model = %req.model, "labels"));

    override_task_history_with_traceparent(&span.clone(), &headers);

    let span_clone = span.clone();
    async move {
        let model_id = req.model.clone();
        let stream = req.stream.unwrap_or(false);
        match llms.read().await.get(&model_id) {
            Some(model) => {
                if stream {
                    // Streaming response
                    create_response_sse_response(model, req, span_clone).await
                } else {
                    // Non-streaming response
                    match model.responses_request(req).await {
                        Ok(response) => {
                            if let Some(message) = extract_text(&response) {
                                tracing::info!(target: "task_history", parent: &span_clone, captured_output = %message);
                            }
                            tracing::info!(target: "task_history", parent: &span_clone,  id = %response.id, "labels");

                            Json(response).into_response()
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", parent: &span_clone, "{e}");
                            tracing::error!("Error from v1/responses: {e}");

                            openai_error_to_response(e)
                        }
                    }
                }
            }
            None => (StatusCode::NOT_FOUND, format!("model '{model_id}' not found")).into_response(),
        }
    }
    .instrument(span)
    .await
}

// Copied from chat.rs for error event formatting
use serde::Serialize;
#[derive(Serialize)]
pub struct ApiError {
    message: String,
}

#[derive(Serialize)]
pub struct OpenaiErrorEvent {
    r#type: String,
    error: ApiError,
}

impl OpenaiErrorEvent {
    pub fn new(err: impl Into<String>) -> Self {
        Self {
            r#type: "error".to_string(),
            error: ApiError {
                message: err.into(),
            },
        }
    }
}

fn to_openai_error_event(err: impl Into<String>) -> Event {
    Event::default()
        .event("error")
        .json_data(OpenaiErrorEvent::new(err))
        .unwrap_or_default()
}

/// Create a SSE [`axum::response::Response`] from a [`ResponseStream`].
async fn create_response_sse_response(
    model: &Arc<dyn Responses>,
    req: CreateResponse,
    span: Span,
) -> Response {
    let mut strm: ResponseStream = match model.responses_stream(req).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            tracing::error!("Error from v1/responses: {e}");
            return openai_error_to_response(e);
        }
    };

    let sse_stream = stream! {
        let mut captured_output = String::new();
        let mut id: Option<u64> = None;
        while let Some(msg) = strm.next().instrument(span.clone()).await {
            match msg {
                Ok(response_event) => {
                    let should_break = match &response_event {
                        ResponseEvent::ResponseOutputTextDelta(delta) => {
                            captured_output.push_str(&delta.delta);
                            false
                        },
                        ResponseEvent::ResponseCompleted(resp) => {
                            if id.is_none() {
                                id = Some(resp.sequence_number);
                            }
                            true
                        },
                        ResponseEvent::ResponseIncomplete(resp) => {
                            if id.is_none() {
                                id = Some(resp.sequence_number);
                            }
                            true
                        },
                        ResponseEvent::ResponseFailed(_) => true,
                        _ => false
                    };

                    yield Ok::<Event, Infallible>(Event::default().json_data(response_event).unwrap_or_else(|e| {
                        tracing::error!("Failed to serialize response event: {e}");
                        to_openai_error_event(e.to_string())
                    }));

                    if should_break {
                        break;
                    }
                },
                Err(e) => {
                    tracing::error!("Error encountered in response stream: {e}");
                    yield Ok(to_openai_error_event(e.to_string()));
                    break;
                }
            }
        };
        tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
        if let Some(id) = id {
            tracing::info!(target: "task_history", parent: &span, id = %id, "labels");
        }
        drop(span);
    };

    Sse::new(Box::pin(sse_stream))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(30)))
        .into_response()
}
