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
use std::{sync::Arc, time::Duration};

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
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::model::LLMModelStore;

pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Json(req): Json<CreateChatCompletionRequest>,
) -> Response {
    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_chat", input = %serde_json::to_string(&req).unwrap_or_default());
    span.in_scope(|| tracing::info!(name: "labels", target: "task_history", model = %req.model));

    let span_clone = span.clone();
    async move {
        let model_id = req.model.clone();
        match llms.read().await.get(&model_id) {
            Some(model) => {
                if req.stream.unwrap_or_default() {
                    match model.write().await.chat_stream(req).await {
                        Ok(strm) => {
                            create_sse_response(strm, time::Duration::from_secs(30), span_clone)
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", "{e}");
                            tracing::debug!("Error from v1/chat: {e}");
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                } else {
                    match model.write().await.chat_request(req).await {
                        Ok(response) => {
                            let preview = response
                                .choices
                                .first()
                                .map(|s| serde_json::to_string(s).unwrap_or_default())
                                .unwrap_or_default();

                            tracing::info!(name: "labels", target: "task_history", truncated_output = %preview);
                            Json(response).into_response()
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", "{e}");
                            tracing::debug!("Error from v1/chat: {e}");
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
        while let Some(msg) = strm.next().instrument(span.clone()).await {
            match msg {
                Ok(resp) => {
                    let y = Event::default();
                    match y.json_data(resp).map_err(axum::Error::new) {
                        Ok(a) => yield Ok(a),
                        Err(e) => yield Err(e),
                    }
                },
                Err(e) => {
                    yield Err(axum::Error::new(e.to_string()));
                    break;
                }
            }
        };
        drop(span);
    }))
    .keep_alive(KeepAlive::new().interval(keep_alive_interval))
    .into_response()
}
