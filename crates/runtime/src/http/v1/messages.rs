/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `POST /v1/messages` and `POST /v1/messages/count_tokens`: the Anthropic Messages API,
//! answered by any chat model in the Spicepod. Anthropic SDKs and tools call it by pointing
//! their base URL at Spice.
//!
//! A model with `from: anthropic` serves it natively: the request reaches Anthropic as the
//! client sent it. Every other chat model serves it by translation through its chat completions.
//! Either way the answer, and every error, is in the Messages API's own shape.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Extension, Json,
    body::{Body, Bytes},
    extract::{MatchedPath, Request},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::StreamExt;
use messages_api::stream::event_type;
use messages_api::{Messages, MessagesError, MessagesRequest};
use messages_chat::ChatMessages;
use model_capabilities::{
    Capability, CapabilityStore, MessagesBackend, ModelCapabilities, Refusal, require,
};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde_json::{Map, Value};
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::http::v1::chat::KEEP_ALIVE_INTERVAL;
use crate::model::LLMChatCompletionsModelStore;
use crate::model::LLMMessagesModelStore;
use crate::status::RuntimeStatus;

/// The model stores a Messages API request is answered from.
#[derive(Clone)]
pub(crate) struct MessagesModels {
    /// The handlers built when a model loaded: a native model's, and a chat model's that runs
    /// Spice's tools.
    pub(crate) messages: Arc<RwLock<LLMMessagesModelStore>>,
    pub(crate) chat: Arc<RwLock<LLMChatCompletionsModelStore>>,
    pub(crate) capabilities: Arc<RwLock<CapabilityStore>>,
}

impl MessagesModels {
    /// The handler that answers Messages API requests for `model`: the one built when it loaded,
    /// or one that translates them through its chat completions.
    async fn handler(&self, model: &str) -> Result<Arc<dyn Messages>, Refusal> {
        if let Some(handler) = self.messages.read().await.get(model) {
            return Ok(Arc::clone(handler));
        }
        let capabilities = self.capabilities.read().await;
        match capabilities
            .get(model)
            .and_then(ModelCapabilities::messages)
        {
            Some(MessagesBackend::Native) => require(
                &capabilities,
                &*self.messages.read().await,
                model,
                Capability::Messages,
            ),
            Some(MessagesBackend::ViaChat) | None => require(
                &capabilities,
                &*self.chat.read().await,
                model,
                Capability::Chat,
            )
            .map(|chat| Arc::new(ChatMessages::new(chat)) as Arc<dyn Messages>),
        }
    }
}

pub(crate) const MESSAGES_PATH: &str = "/v1/messages";
pub(crate) const COUNT_TOKENS_PATH: &str = "/v1/messages/count_tokens";

/// Marks a response already in the Messages API's error envelope, which [`encode_layer_errors`]
/// leaves alone.
#[derive(Debug, Clone, Copy)]
struct MessagesEncoded;

/// Largest error body read back from a layer in front of the handlers to reuse as the message.
const MAX_LAYER_ERROR_BODY_BYTES: usize = 64 * 1024;

/// `error` as a response: its status and the Messages API's error envelope.
fn error_response(error: &MessagesError) -> Response {
    let status = StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Json(error.to_envelope())).into_response();
    response.extensions_mut().insert(MessagesEncoded);
    response
}

/// Middleware for the Messages routes that re-encodes errors from the layers in front of the
/// handlers (API key auth, the mTLS gate, the body limit, shutdown, extractor rejections) as the
/// Messages API's error envelope, typed by status as Anthropic's are, since Anthropic SDKs read
/// that envelope on every error response. Other routes, successes, and responses already in the
/// envelope pass through unchanged.
pub(crate) async fn encode_layer_errors(req: Request<Body>, next: Next) -> Response {
    let is_messages_path = |path: &str| path == MESSAGES_PATH || path == COUNT_TOKENS_PATH;
    let is_messages = req.extensions().get::<MatchedPath>().map_or_else(
        || is_messages_path(req.uri().path()),
        |path| is_messages_path(path.as_str()),
    );
    if !is_messages {
        return next.run(req).await;
    }
    let response = next.run(req).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error())
        || response.extensions().get::<MessagesEncoded>().is_some()
    {
        return response;
    }
    let (parts, body) = response.into_parts();
    let text = axum::body::to_bytes(body, MAX_LAYER_ERROR_BODY_BYTES)
        .await
        .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
        .unwrap_or_default();
    let message = if text.is_empty() {
        status
            .canonical_reason()
            .unwrap_or("Request failed")
            .to_string()
    } else {
        text
    };
    let mut encoded = error_response(&MessagesError::from_response(
        status.as_u16(),
        message.as_bytes(),
    ));
    // Keep headers such as `WWW-Authenticate`; the body and its type are replaced.
    for (name, value) in &parts.headers {
        if *name != header::CONTENT_TYPE && *name != header::CONTENT_LENGTH {
            encoded.headers_mut().append(name, value.clone());
        }
    }
    encoded
}

/// The error for a request naming a model that cannot answer it.
fn refusal_error(model: &str, refusal: &Refusal, status: &RuntimeStatus) -> MessagesError {
    match refusal {
        Refusal::NotFound => MessagesError::not_found(
            status
                .unavailable_model_reason(model)
                .unwrap_or_else(|| format!("model '{model}' not found")),
        ),
        Refusal::Unavailable => MessagesError::api(
            status
                .unavailable_model_reason(model)
                .unwrap_or_else(|| format!("model '{model}' is unavailable via /v1/messages")),
        )
        .with_status(503),
        Refusal::Unsupported(capabilities) => {
            let provider = capabilities.provider();
            MessagesError::invalid_request(format!(
                "Model '{model}' uses provider '{provider}' which is a decision model and does not support the Messages API or chat completions. Use POST /v1/decisions, or the SQL decision functions such as ai_if, instead."
            ))
        }
    }
}

/// The request as the client sent it: its body, and its `anthropic-beta` and
/// `anthropic-version` headers.
fn messages_request(headers: &HeaderMap, body: &Bytes) -> Result<MessagesRequest, MessagesError> {
    let body: Map<String, Value> = serde_json::from_slice(body).map_err(|e| {
        MessagesError::invalid_request(format!("The request body is not a JSON object: {e}"))
    })?;
    let betas = headers
        .get_all("anthropic-beta")
        .iter()
        .filter_map(|value| value.to_str().ok());
    let version = headers
        .get("anthropic-version")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let mut request = MessagesRequest::new(body)
        .with_beta_headers(betas)
        .with_version(version);
    crate::model::messages_tool_history::replay_spice_tool_calls(&mut request.body);
    Ok(request)
}

/// The model a request names.
fn model_of(request: &MessagesRequest) -> Result<String, MessagesError> {
    request
        .model()
        .filter(|model| !model.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| MessagesError::invalid_request("model: Field required"))
}

/// The text of a Message's text blocks.
fn text_of(message: &Value) -> String {
    message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect()
}

/// Create a message
///
/// Answers a conversation in the shape of the Anthropic Messages API, so Anthropic SDKs and
/// tools work by pointing their base URL at Spice. `model` names any chat model in the
/// Spicepod: a model with `from: anthropic` receives the request as sent, and any other model
/// receives it translated into a chat completion. The request and the response, streamed
/// (`"stream": true`) or not, follow the Messages API reference:
/// <https://docs.claude.com/en/api/messages>.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/messages",
    operation_id = "post_messages",
    tag = "AI",
    params(
        ("anthropic-version" = Option<String>, Header, description = "The Messages API version. Accepted for compatibility; `2023-06-01` is the only version."),
        ("anthropic-beta" = Option<String>, Header, description = "Comma-separated beta features, forwarded to models with `from: anthropic`."),
    ),
    request_body(
        description = "A Messages API request.",
        content((
            serde_json::Value = "application/json",
            example = json!({
                "model": "claude",
                "max_tokens": 1024,
                "messages": [
                    {"role": "user", "content": "Hello!"}
                ]
            })
        ))
    ),
    responses(
        (status = 200, description = "The model's message, or with `\"stream\": true`, its stream of server-sent events", content((
            serde_json::Value = "application/json",
            example = json!({
                "id": "msg_01XFDUDYJgAACzvnptvVoYEL",
                "type": "message",
                "role": "assistant",
                "model": "claude",
                "content": [{"type": "text", "text": "Hello! How can I help you today?"}],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {"input_tokens": 10, "output_tokens": 12}
            })
        ))),
        (status = 400, description = "The request is invalid, or asks for something the model cannot serve", content((
            serde_json::Value = "application/json",
            example = json!({
                "type": "error",
                "error": {"type": "invalid_request_error", "message": "model: Field required"}
            })
        ))),
        (status = 404, description = "The model was not found", content((
            serde_json::Value = "application/json",
            example = json!({
                "type": "error",
                "error": {"type": "not_found_error", "message": "model 'claude' not found"}
            })
        ))),
        (status = 503, description = "The model failed to load"),
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<MessagesModels>,
    Extension(status): Extension<Arc<RuntimeStatus>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let request = match messages_request(&headers, &body) {
        Ok(request) => request,
        Err(error) => return error_response(&error),
    };
    let model = match model_of(&request) {
        Ok(model) => model,
        Err(error) => return error_response(&error),
    };

    let input = Value::Object(request.body.clone()).to_string();
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_chat",
        input = %input
    );
    span.in_scope(
        || tracing::info!(target: "task_history", model = %model, api = "messages", "labels"),
    );
    crate::task_history::correlation::record_task_history_trace_id(&span, &context);

    let span_clone = span.clone();
    async move {
        let handler = match models.handler(&model).await {
            Ok(handler) => handler,
            Err(refusal) => return error_response(&refusal_error(&model, &refusal, &status)),
        };
        if request.is_stream() {
            match handler.create_stream(request).await {
                Ok(events) => sse_response(events, span_clone),
                Err(error) => {
                    tracing::error!(target: "task_history", parent: &span_clone, "{error}");
                    error_response(&error)
                }
            }
        } else {
            match handler.create(request).await {
                Ok(message) => {
                    tracing::info!(target: "task_history", parent: &span_clone, captured_output = %text_of(&message));
                    if let Some(id) = message.get("id").and_then(Value::as_str) {
                        tracing::info!(target: "task_history", parent: &span_clone, id = %id, "labels");
                    }
                    Json(message).into_response()
                }
                Err(error) => {
                    tracing::error!(target: "task_history", parent: &span_clone, "{error}");
                    error_response(&error)
                }
            }
        }
    }
    .instrument(span)
    .await
}

/// The stream of a message as server-sent events, each named by its `type`. An error is sent
/// as an `error` event, and ends the stream.
fn sse_response(events: messages_api::MessageStream, span: Span) -> Response {
    struct State {
        events: messages_api::MessageStream,
        span: Span,
        output: String,
        ended: bool,
    }
    let state = State {
        events,
        span,
        output: String::new(),
        ended: false,
    };
    let stream = futures::stream::unfold(state, |mut state| async move {
        if state.ended {
            return None;
        }
        let next = state.events.next().instrument(state.span.clone()).await;
        let event = match next {
            Some(Ok(event)) => {
                match event_type(&event) {
                    Some("message_start") => {
                        if let Some(id) = event.pointer("/message/id").and_then(Value::as_str) {
                            tracing::info!(target: "task_history", parent: &state.span, id = %id, "labels");
                        }
                    }
                    Some("content_block_delta") => {
                        if event.pointer("/delta/type").and_then(Value::as_str)
                            == Some("text_delta")
                            && let Some(text) = event.pointer("/delta/text").and_then(Value::as_str)
                        {
                            state.output.push_str(text);
                        }
                    }
                    _ => {}
                }
                Event::default()
                    .event(event_type(&event).unwrap_or("message"))
                    .data(event.to_string())
            }
            Some(Err(error)) => {
                tracing::error!(target: "task_history", parent: &state.span, "{error}");
                state.ended = true;
                Event::default()
                    .event("error")
                    .data(error.to_envelope().to_string())
            }
            None => {
                tracing::info!(target: "task_history", parent: &state.span, captured_output = %state.output);
                return None;
            }
        };
        Some((Ok::<Event, Infallible>(event), state))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(KEEP_ALIVE_INTERVAL)))
        .into_response()
}

/// Count tokens in a message
///
/// Counts the input tokens of a Messages API request, as the model would be sent it, Spice's
/// system prompt and tools included. Only a model with `from: anthropic` counts tokens: a count
/// is specific to the model's tokenizer. The request and response follow the Messages API
/// reference: <https://docs.claude.com/en/api/messages-count-tokens>.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/messages/count_tokens",
    operation_id = "post_messages_count_tokens",
    tag = "AI",
    request_body(
        description = "A Messages API request, without `max_tokens`.",
        content((
            serde_json::Value = "application/json",
            example = json!({
                "model": "claude",
                "messages": [{"role": "user", "content": "Hello!"}]
            })
        ))
    ),
    responses(
        (status = 200, description = "The input token count", content((
            serde_json::Value = "application/json",
            example = json!({"input_tokens": 10})
        ))),
        (status = 400, description = "The request is invalid, or the model cannot count tokens"),
        (status = 404, description = "The model was not found"),
    )
))]
pub(crate) async fn count_tokens(
    Extension(models): Extension<MessagesModels>,
    Extension(status): Extension<Arc<RuntimeStatus>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = match messages_request(&headers, &body) {
        Ok(request) => request,
        Err(error) => return error_response(&error),
    };
    let model = match model_of(&request) {
        Ok(model) => model,
        Err(error) => return error_response(&error),
    };
    let handler = match models.handler(&model).await {
        Ok(handler) => handler,
        Err(refusal) => return error_response(&refusal_error(&model, &refusal, &status)),
    };
    match handler.count_tokens(request).await {
        Ok(count) => Json(count).into_response(),
        Err(error) => error_response(&error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn json_body(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, serde_json::from_slice(&bytes).expect("a JSON body"))
    }

    #[tokio::test]
    async fn errors_are_in_the_messages_api_envelope() {
        let invalid = messages_request(&HeaderMap::new(), &Bytes::from_static(b"[1, 2]"))
            .expect_err("an array is not a request");
        assert_eq!(
            json_body(error_response(&invalid)).await,
            (
                StatusCode::BAD_REQUEST,
                serde_json::json!({"type": "error", "error": {"type": "invalid_request_error", "message": "The request body is not a JSON object: invalid type: sequence, expected a map at line 1 column 0"}})
            )
        );

        let unavailable = refusal_error("claude", &Refusal::Unavailable, &RuntimeStatus::new());
        assert_eq!(
            json_body(error_response(&unavailable)).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"type": "error", "error": {"type": "api_error", "message": "model 'claude' is unavailable via /v1/messages"}})
            )
        );
    }

    #[test]
    fn beta_and_version_headers_are_read() {
        let mut headers = HeaderMap::new();
        headers.append("anthropic-beta", "a-1,b-2".parse().expect("a header"));
        headers.append("anthropic-beta", "c-3".parse().expect("a header"));
        headers.insert("anthropic-version", "2023-06-01".parse().expect("a header"));
        let request = messages_request(&headers, &Bytes::from_static(br#"{"model": "m"}"#))
            .expect("a request");
        assert_eq!(
            (request.betas, request.version.as_deref()),
            (
                vec!["a-1".to_string(), "b-2".to_string(), "c-3".to_string()],
                Some("2023-06-01")
            )
        );
    }
}
