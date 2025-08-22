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
use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{
    http::traceparent::override_task_history_with_traceparent, model::LLMChatCompletionsModelStore,
};
#[cfg(feature = "openapi")]
use async_openai::types::CreateChatCompletionResponse;
use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatChoiceStream, ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CreateChatCompletionRequest,
        CreateChatCompletionStreamResponse, Role,
    },
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
use event_stream::get_event_stream;
use futures::StreamExt;
use http::HeaderValue;
use llms::chat::Chat;
use serde::Serialize;
use tokio::{
    select,
    sync::{RwLock, mpsc::channel, oneshot},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, Span};

static SPICE_COMPLETION_PROGRESS_HEADER: &str = "x-spiceai-completion-progress";
pub static KEEP_ALIVE_INTERVAL: u64 = 30;

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
    Extension(llms): Extension<Arc<RwLock<LLMChatCompletionsModelStore>>>,
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
                    let include_stream_events = headers.get(SPICE_COMPLETION_PROGRESS_HEADER)
                        .is_some_and(|v| v == HeaderValue::from_static("enabled"));
                    handle_streaming(model, req, include_stream_events).instrument(span_clone).await
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

/// Handle the SSE logic for when `v1/chat/completion` endpoint sets `"stream": true`.
/// Expect the [`tracing::Span`] involved in the chat completion to be already [`Span::entered`].
async fn handle_streaming(
    model: &Arc<dyn Chat>,
    req: CreateChatCompletionRequest,
    include_stream_events: bool,
) -> Response {
    let span = Span::current();
    let (tx, rx) = channel(100);
    let tx = Arc::new(tx);
    let (end_completion, completion_done) = oneshot::channel::<()>();

    // Get span event stream and setup background thread to read events and write to `tx`.
    if include_stream_events {
        let mut events = match get_event_stream() {
            Ok(o) => o,
            Err(e) => {
                return openai_error_to_response(OpenAIError::StreamError(format!(
                    "An error occurred in reading progress: {e}"
                )));
            }
        };

        let tx_clone = Arc::clone(&tx);
        tokio::spawn(async move {
            let mut chat_completion_ended = Box::pin(futures::stream::once(async move {
                let _ = completion_done.await;
            }));
            loop {
                select! {
                    Some(evnt) = events.next() =>  {
                        let _ = tx_clone.send(create_working_stream_payload(evnt)).await;
                    },
                    _ = chat_completion_ended.next() => {
                        break;
                    },
                };
            }
        });
    }

    let mut stream: ChatCompletionResponseStream = match model.chat_stream(req).await {
        Ok(strm) => strm,
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            tracing::error!("Error from v1/chat: {e}");

            return openai_error_to_response(e);
        }
    };

    // Merge [`ChatCompletionResponseStream`] into joint event & LLM stream.
    tokio::spawn(async move {
        while let Some(pkt) = stream.next().await {
            let _ = tx.send(pkt).await;
        }
        // Signal event stream to close.
        let _ = end_completion.send(());
    });

    create_sse_response(
        Box::pin(ReceiverStream::new(rx)),
        time::Duration::from_secs(KEEP_ALIVE_INTERVAL),
        span,
    )
}

#[allow(clippy::cast_possible_truncation, deprecated)]
pub(crate) fn create_working_stream_payload(
    content: String,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let created = u32::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .as_secs(),
    )
    .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    Ok(CreateChatCompletionStreamResponse {
        created,
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage: None,
        model: String::new(),
        id: String::new(),
        choices: vec![ChatChoiceStream {
            index: 0,
            finish_reason: None,
            logprobs: None,
            delta: ChatCompletionStreamResponseDelta {
                content: Some(content),
                function_call: None,
                tool_calls: None,
                role: Some(Role::Assistant),
                refusal: None,
            },
        }],
    })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        http::v1::chat::{SPICE_COMPLETION_PROGRESS_HEADER, post},
        model::LLMChatCompletionsModelStore,
    };
    use async_openai::{
        error::OpenAIError,
        types::{
            ChatCompletionResponseStream, CreateChatCompletionRequest,
            CreateChatCompletionStreamResponse,
        },
    };
    use tracing::{Level, span};
    use tracing_futures::Instrument;

    use super::create_working_stream_payload;
    use async_trait::async_trait;
    use axum::{
        extract::{Extension, Json},
        http::{HeaderMap, HeaderValue},
    };
    use http_body_util::BodyExt;
    use llms::chat::{Chat, nsql::SqlGeneration};
    use serde_json::json;
    use tokio::sync::RwLock;
    use tracing_subscriber::layer::SubscriberExt;

    pub struct DummyChat;

    #[async_trait]
    impl Chat for DummyChat {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }

        async fn chat_stream(
            &self,
            _req: CreateChatCompletionRequest,
        ) -> Result<ChatCompletionResponseStream, OpenAIError> {
            tracing::info!(
                target: "task_history",
                progress = "A nice little test",
            );
            tracing::info!(
                target: "task_history",
                progress = "Another nice little test",
            );
            Ok(Box::pin(futures::stream::once(async move {
                create_working_stream_payload("payload".to_string())
            })))
        }
    }

    async fn run_post(progress_header: Option<&'static str>) -> Vec<String> {
        let mut store = LLMChatCompletionsModelStore::new();
        store.insert("dummy".to_string(), Arc::new(DummyChat {}));
        let llms = Arc::new(RwLock::new(store));

        let mut headers = HeaderMap::new();
        if let Some(v) = progress_header {
            headers.insert(
                SPICE_COMPLETION_PROGRESS_HEADER,
                HeaderValue::from_static(v),
            );
        }

        let req_payload: CreateChatCompletionRequest = serde_json::from_value(json!({
            "model": "dummy",
            "stream": true,
            "messages": [
                {"role": "user", "content": "hello"}
            ]
        }))
        .expect("Failed to make test request payload.");

        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(event_stream::EventStreamLayer::new("progress")),
        );
        let span = span!(Level::INFO, "test_span");

        let _enter = span.enter();

        let response = post(Extension(llms), headers, Json(req_payload))
            .instrument(span.clone())
            .await;

        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("Failed to collect SSE response from 'post'.");

        let body_str = String::from_utf8(body_bytes.to_bytes().to_vec()).expect("Invalid utf8");
        body_str
            .split("\n\n")
            .filter_map(|e| {
                let resp: CreateChatCompletionStreamResponse =
                    serde_json::from_str(e.strip_prefix("data: ")?)
                        .expect("Failed to deserialise SSE event");
                resp.choices
                    .first()
                    .expect("Expected a choice in SSE event")
                    .delta
                    .content
                    .clone()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_post_streaming_enabled() {
        assert_eq!(
            run_post(Some("enabled")).await,
            vec![
                "A nice little test".to_string(),       // From the event stream
                "Another nice little test".to_string(), // From the event stream
                "payload".to_string()                   // From the LLM stream.
            ]
        );
    }

    #[tokio::test]
    async fn test_post_streaming_disabled() {
        assert_eq!(
            run_post(Some("disabled")).await,
            vec![
                "payload".to_string() // From the LLM stream.
            ]
        );
    }

    #[tokio::test]
    async fn test_post_streaming_none() {
        assert_eq!(
            run_post(None).await,
            vec![
                "payload".to_string() // From the LLM stream.
            ]
        );
    }
}
