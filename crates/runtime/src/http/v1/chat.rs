/*
Copyright 2024 The Spice.ai OSS Authors

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

use async_openai::types::{ChatCompletionResponseStream, CreateChatCompletionRequest};
use async_stream::stream;
use axum::{
    http::StatusCode,
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

use crate::model::LLMModelStore;

pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Json(req): Json<CreateChatCompletionRequest>,
) -> Response {
    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_chat", input = %serde_json::to_string(&req).unwrap_or_default());
    span.in_scope(|| tracing::info!(target: "task_history", model = %req.model, "labels"));

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
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                } else {
                    match model.chat_request(req).await {
                        Ok(response) => {
                            let preview = response
                                .choices
                                .first()
                                .map(|s| serde_json::to_string(s).unwrap_or_default())
                                .unwrap_or_default();

                            tracing::info!(target: "task_history", parent: &span_clone, captured_output = %preview);
                            Json(response).into_response()
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", parent: &span_clone, "{e}");
                            tracing::error!("Error from v1/chat: {e}");
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                }
            }
            None => StatusCode::NOT_FOUND.into_response(),
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
        while let Some(msg) = strm.next().instrument(span.clone()).await {
            match msg {
                Ok(resp) => {
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
