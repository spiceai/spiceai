/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use core::time;
use std::{convert::Infallible, sync::Arc, time::Duration};

use crate::{http::traceparent::override_task_history_with_traceparent, model::LLMModelStore};
#[cfg(feature = "openapi")]
use async_openai::types::CreateChatCompletionResponse;
use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatCompletionResponseMessage, ChatCompletionResponseStream,
        CreateChatCompletionRequest,
    },
};
use async_stream::stream;
use axum::{
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Extension, Json,
};
use futures::StreamExt;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

/// Create Chat Completion
///
/// Creates a model response for the given chat conversation.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/chat/completions",
    operation_id = "post_chat_completions",
    tag = "AI",
    request_body(
        description = "Create a chat completion request using a language model.",
        content((
            CreateChatCompletionRequest = "application/json",
            example = json!({
                "model": "gpt-4o",
                "messages": [
                    { "role": "developer", "content": "You are a helpful assistant." },
                    { "role": "user", "content": "Hello!" }
                ],
                "stream": false
            })
        ))
    ),
    responses(
        (status = 200, description = "Chat completion generated successfully", content((
            CreateChatCompletionResponse = "application/json",
            example = json!({
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "created": 1_677_652_288,
                "model": "gpt-4o-mini",
                "system_fingerprint": "fp_44709d6fcb",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "\n\nHello there, how may I assist you today?"
                    },
                    "logprobs": null,
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21,
                    "completion_tokens_details": {
                        "reasoning_tokens": 0,
                        "accepted_prediction_tokens": 0,
                        "rejected_prediction_tokens": 0
                    }
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
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    headers: HeaderMap,
    Json(req): Json<CreateChatCompletionRequest>,
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
        match llms.read().await.get(&model_id) {
            Some(model) => {
                if req.stream.unwrap_or_default() {
                    match model.chat_stream(req).await {
                        Ok(strm) => {
                            create_sse_response(strm, time::Duration::from_secs(30), span_clone)
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", parent: &span_clone, "{e}");
                            tracing::error!("Error from v1/chat: {e}");

                            openai_error_to_response(e)
                        }
                    }
                } else {
                    match model.chat_request(req).await {
                        Ok(response) => {
                            if let Some(ChatChoice{message: ChatCompletionResponseMessage{
                                content: Some(content),..
                            },..}) = response
                                .choices
                                .first() {
                                    tracing::info!(target: "task_history", parent: &span_clone, captured_output = %content);
                                }
                                tracing::info!(target: "task_history", parent: &span_clone,  id = %response.id, "labels");

                            Json(response).into_response()
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", parent: &span_clone, "{e}");
                            tracing::error!("Error from v1/chat: {e}");

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

/// Create a SSE [`axum::response::Response`] from a [`ChatCompletionResponseStream`].
fn create_sse_response(
    mut strm: ChatCompletionResponseStream,
    keep_alive_interval: Duration,
    span: Span,
) -> Response {
    Sse::new(Box::pin(stream! {
        let mut chat_output = String::new();
        let mut id: Option<String> = None;
        while let Some(msg) = strm.next().instrument(span.clone()).await {
            match msg {
                Ok(resp) => {
                    if id.is_none() {
                        id = Some(resp.id.clone());
                    }
                    if let Some(choice) = resp.choices.first() {
                        if let Some(intermediate_chat_output) = &choice.delta.content {
                            chat_output.push_str(intermediate_chat_output);
                        }
                    }

                    yield Ok::<Event, Infallible>(Event::default().json_data(resp).unwrap_or_else(|e| {
                        tracing::error!("Failed to serialize chat completion message: {e}");
                        to_openai_error_event(e.to_string())
                    }));
                },
                Err(e) => {
                    tracing::error!("Error encountered in chat completion stream: {e}");
                    yield Ok(to_openai_error_event(e.to_string()));
                    break;
                }
            }
        };
        tracing::info!(target: "task_history", parent: &span, captured_output = %chat_output);
        if let Some(id) = id {
            tracing::info!(target: "task_history", parent: &span, id = %id, "labels");
        }
        drop(span);
    }))
    .keep_alive(KeepAlive::new().interval(keep_alive_interval))
    .into_response()
}

/// Create an [`Event`] that corresponds to an `OpenAI` error event.
///
/// `https://platform.openai.com/docs/api-reference/realtime-server-events/error`
fn to_openai_error_event(err: impl Into<String>) -> Event {
    Event::default()
        .event("error")
        .json_data(OpenaiErrorEvent::new(err))
        .unwrap_or_default()
}

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

/// Converts `OpenAI` errors to HTTP responses
/// Preserve the original `OpenAI` error structure to maintain compatibility with `OpenAI` documentation
#[must_use]
pub fn openai_error_to_response(e: OpenAIError) -> Response {
    match e {
        OpenAIError::InvalidArgument(_) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        OpenAIError::ApiError(api_error) => {
            let error_response = serde_json::json!({
                "message": api_error.message,
                "type": api_error.r#type,
                "param": api_error.param,
                "code": api_error.code
            });

            let status_code = match api_error.code.as_deref() {
                Some("invalid_request_error") => StatusCode::BAD_REQUEST,
                Some("invalid_api_key") => StatusCode::UNAUTHORIZED,
                Some("insufficient_quota") => StatusCode::PAYMENT_REQUIRED,
                Some("rate_limit_exceeded") => StatusCode::TOO_MANY_REQUESTS,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            (status_code, Json(error_response)).into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
