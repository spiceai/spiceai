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

//! `/v1beta/models/...`: the Gemini API, answered by any chat model in the Spicepod. Gemini
//! clients, such as the Antigravity CLI's LLM gateway mode, call it by pointing their base URL
//! at Spice.
//!
//! Every model serves it by translation through its chat completions. The answer, and every
//! error, is in the Gemini API's own shape.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Extension, Json,
    body::{Body, Bytes},
    extract::{MatchedPath, Path, Query, Request},
    http::{StatusCode, header},
    middleware::Next,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::StreamExt;
use gemini_api::{
    CountTokensRequest, GeminiError, GeminiStatus, GenerateContentRequest, GenerateContentResponse,
    GenerateContentStream, ListModelsResponse,
};
use gemini_chat::{GeminiChat, model_listing};
use model_capabilities::{Capability, CapabilityStore, Refusal, require};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{Instrument, Span};

use crate::http::v1::chat::KEEP_ALIVE_INTERVAL;
use crate::model::LLMChatCompletionsModelStore;
use crate::status::RuntimeStatus;

/// `GET` lists the models; under it, `{model}` and `{model}:{method}`.
pub(crate) const MODELS_PATH: &str = "/v1beta/models";
pub(crate) const MODEL_PATH: &str = "/v1beta/models/{*model}";

/// The model stores a Gemini API request is answered from.
#[derive(Clone)]
pub(crate) struct GeminiModels {
    pub(crate) chat: Arc<RwLock<LLMChatCompletionsModelStore>>,
    pub(crate) capabilities: Arc<RwLock<CapabilityStore>>,
}

impl GeminiModels {
    /// The handler that answers Gemini API requests for `model`: its chat completions.
    async fn handler(&self, model: &str) -> Result<GeminiChat, Refusal> {
        let capabilities = self.capabilities.read().await;
        require(
            &capabilities,
            &*self.chat.read().await,
            model,
            Capability::Chat,
        )
        .map(GeminiChat::new)
    }

    /// The models a Gemini API request can name, sorted by id.
    async fn models(&self) -> Vec<(String, Arc<dyn llms::chat::Chat>)> {
        let mut models: Vec<(String, Arc<dyn llms::chat::Chat>)> = self
            .chat
            .read()
            .await
            .iter()
            .map(|(id, chat)| (id.clone(), Arc::clone(chat)))
            .collect();
        models.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        models
    }
}

/// A method of `models/{model}:{method}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Method {
    GenerateContent,
    StreamGenerateContent,
    CountTokens,
}

/// The model and the method a `{model}:{method}` path segment names.
fn model_and_method(path: &str) -> Result<(&str, Method), GeminiError> {
    let Some((model, method)) = path.rsplit_once(':') else {
        return Err(GeminiError::not_found(format!(
            "No method in '{path}'. POST to `models/{{model}}:generateContent`, `:streamGenerateContent` or `:countTokens`."
        )));
    };
    let method = match method {
        "generateContent" => Method::GenerateContent,
        "streamGenerateContent" => Method::StreamGenerateContent,
        "countTokens" => Method::CountTokens,
        other => {
            return Err(GeminiError::not_found(format!(
                "Method '{other}' is not supported. POST to `models/{{model}}:generateContent`, `:streamGenerateContent` or `:countTokens`."
            )));
        }
    };
    if model.is_empty() {
        return Err(GeminiError::invalid_argument(
            "The URL names no model: use `models/{model}:{method}`.",
        ));
    }
    Ok((model, method))
}

/// Marks a response already in the Gemini API's error body, which [`encode_layer_errors`]
/// leaves alone.
#[derive(Debug, Clone, Copy)]
struct GeminiEncoded;

/// Largest error body read back from a layer in front of the handlers to reuse as the message.
const MAX_LAYER_ERROR_BODY_BYTES: usize = 64 * 1024;

/// `error` as a response: its status and the Gemini API's error body.
fn error_response(error: &GeminiError) -> Response {
    let status =
        StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Json(error.body())).into_response();
    response.extensions_mut().insert(GeminiEncoded);
    response
}

/// The Gemini status a layer's HTTP status stands for.
fn status_of(http: StatusCode) -> GeminiStatus {
    match http.as_u16() {
        401 => GeminiStatus::Unauthenticated,
        403 => GeminiStatus::PermissionDenied,
        404 => GeminiStatus::NotFound,
        409 => GeminiStatus::Aborted,
        429 => GeminiStatus::ResourceExhausted,
        499 => GeminiStatus::Cancelled,
        501 => GeminiStatus::Unimplemented,
        503 => GeminiStatus::Unavailable,
        504 => GeminiStatus::DeadlineExceeded,
        400..=499 => GeminiStatus::InvalidArgument,
        _ => GeminiStatus::Internal,
    }
}

fn is_gemini_path(path: &str) -> bool {
    path == MODELS_PATH || path == MODEL_PATH || path.starts_with("/v1beta/models/")
}

/// Whether `req` is for a Gemini route: by the route it matched, else by its path.
fn is_gemini_request(req: &Request<Body>) -> bool {
    req.extensions().get::<MatchedPath>().map_or_else(
        || is_gemini_path(req.uri().path()),
        |path| is_gemini_path(path.as_str()),
    )
}

/// The header Google's SDKs, the Gemini CLI and Google's REST examples send the API key in.
const GOOGLE_API_KEY_HEADER: &str = "x-goog-api-key";

/// What a Gemini route answers a request without a valid runtime API key, naming every
/// header the key is read from.
const UNAUTHENTICATED_MESSAGE: &str = "Missing or invalid API key. Send the runtime API key in `x-goog-api-key`, `X-API-Key`, or `Authorization: Bearer`. See: https://spiceai.org/docs/reference/spicepod/runtime#runtimeauth";

/// Middleware for the Gemini routes that accepts the runtime API key in `x-goog-api-key`,
/// as Google's clients send it, by handing it to API key authentication as `X-API-Key`. It
/// runs in front of authentication, and a request that already carries `X-API-Key` keeps
/// it. Other routes do not read the header.
pub(crate) async fn accept_google_api_key(mut req: Request<Body>, next: Next) -> Response {
    if is_gemini_request(&req)
        && !req.headers().contains_key("x-api-key")
        && let Some(key) = req.headers().get(GOOGLE_API_KEY_HEADER).cloned()
    {
        req.headers_mut().insert("x-api-key", key);
    }
    next.run(req).await
}

/// Middleware for the Gemini routes that re-encodes errors from the layers in front of the
/// handlers (API key auth, the mTLS gate, the body limit, shutdown, extractor rejections) as
/// the Gemini API's error body, which Gemini clients read on every error response. Other
/// routes, successes, and responses already in that body pass through unchanged.
pub(crate) async fn encode_layer_errors(req: Request<Body>, next: Next) -> Response {
    if !is_gemini_request(&req) {
        return next.run(req).await;
    }
    let response = next.run(req).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error())
        || response.extensions().get::<GeminiEncoded>().is_some()
    {
        return response;
    }
    let (parts, body) = response.into_parts();
    let text = axum::body::to_bytes(body, MAX_LAYER_ERROR_BODY_BYTES)
        .await
        .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
        .unwrap_or_default();
    let message = if status == StatusCode::UNAUTHORIZED
        && (text.is_empty() || text.eq_ignore_ascii_case("unauthorized"))
    {
        UNAUTHENTICATED_MESSAGE.to_string()
    } else if text.is_empty() {
        status
            .canonical_reason()
            .unwrap_or("Request failed")
            .to_string()
    } else {
        text
    };
    let mut encoded = error_response(&GeminiError::new(status_of(status), message));
    // Keep the layer's own status, such as 413 for an oversized body, and headers such as
    // `WWW-Authenticate`; the body and its type are replaced.
    *encoded.status_mut() = status;
    for (name, value) in &parts.headers {
        if *name != header::CONTENT_TYPE && *name != header::CONTENT_LENGTH {
            encoded.headers_mut().append(name, value.clone());
        }
    }
    encoded
}

/// The error for a request naming a model that cannot answer it.
fn refusal_error(model: &str, refusal: &Refusal, status: &RuntimeStatus) -> GeminiError {
    match refusal {
        Refusal::NotFound => GeminiError::not_found(
            status
                .unavailable_model_reason(model)
                .unwrap_or_else(|| format!("Model '{model}' not found.")),
        ),
        Refusal::Unavailable => GeminiError::unavailable(
            status
                .unavailable_model_reason(model)
                .unwrap_or_else(|| format!("Model '{model}' is unavailable.")),
        ),
        Refusal::Unsupported(capabilities) => {
            let provider = capabilities.provider();
            GeminiError::invalid_argument(format!(
                "Model '{model}' uses provider '{provider}' which is a decision model and does not support the Gemini API or chat completions. Use POST /v1/decisions, or the SQL decision functions such as ai_if, instead."
            ))
        }
    }
}

fn parse<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, GeminiError> {
    serde_json::from_slice(body).map_err(|e| {
        GeminiError::invalid_argument(format!("The request body is not a valid request: {e}"))
    })
}

/// The text of a response's candidates, without the model's thoughts.
fn text_of(response: &GenerateContentResponse) -> String {
    response
        .candidates
        .iter()
        .filter_map(|candidate| candidate.content.as_ref())
        .flat_map(|content| &content.parts)
        .filter(|part| part.thought != Some(true))
        .filter_map(|part| part.text.as_deref())
        .collect()
}

/// List models
///
/// Lists the Spicepod's chat models in the shape of the Gemini API's `models.list`, so Gemini
/// clients can discover them.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1beta/models",
    operation_id = "get_gemini_models",
    tag = "AI",
    responses(
        (status = 200, description = "The chat models, as Gemini API models", content((
            serde_json::Value = "application/json",
            example = json!({
                "models": [{
                    "name": "models/my-model",
                    "displayName": "my-model",
                    "supportedGenerationMethods": ["generateContent"]
                }]
            })
        ))),
    )
))]
pub(crate) async fn list_models(Extension(models): Extension<GeminiModels>) -> Response {
    let listed = ListModelsResponse {
        models: super::models::token_limits_of(models.models().await)
            .await
            .iter()
            .map(|(id, limits)| model_listing(id, *limits))
            .collect(),
        ..ListModelsResponse::default()
    };
    Json(listed).into_response()
}

/// Get a model
///
/// One of the Spicepod's chat models, in the shape of the Gemini API's `models.get`.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1beta/models/{model}",
    operation_id = "get_gemini_model",
    tag = "AI",
    params(("model" = String, Path, description = "The model's name, as `spice models` lists it.")),
    responses(
        (status = 200, description = "The model", content((
            serde_json::Value = "application/json",
            example = json!({
                "name": "models/my-model",
                "displayName": "my-model",
                "supportedGenerationMethods": ["generateContent"]
            })
        ))),
        (status = 404, description = "The model was not found"),
    )
))]
pub(crate) async fn get_model(
    Extension(models): Extension<GeminiModels>,
    Path(model): Path<String>,
) -> Response {
    let model = model.as_str();
    let chat = models.chat.read().await.get(model).map(Arc::clone);
    if let Some(chat) = chat {
        return Json(model_listing(model, chat.token_limits().await)).into_response();
    }
    error_response(&GeminiError::not_found(format!(
        "Model '{model}' not found."
    )))
}

/// Generate content
///
/// Answers `models/{model}:generateContent`, `:streamGenerateContent` (with `?alt=sse`, a
/// stream of server-sent events) and `:countTokens` in the shape of the Gemini API, so Gemini
/// clients work by pointing their base URL at Spice. `model` names any chat model in the
/// Spicepod; the request is translated into a chat completion. The request and the response
/// follow the Gemini API reference: <https://ai.google.dev/api/generate-content>.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1beta/models/{model}:generateContent",
    operation_id = "post_gemini_generate_content",
    tag = "AI",
    params(
        ("model" = String, Path, description = "The model's name, as `spice models` lists it. The method follows it: `:generateContent`, `:streamGenerateContent` or `:countTokens`."),
        ("alt" = Option<String>, Query, description = "`sse` streams `:streamGenerateContent` as server-sent events; without it the stream is sent as one JSON array."),
    ),
    request_body(
        description = "A Gemini API `generateContent` request.",
        content((
            serde_json::Value = "application/json",
            example = json!({
                "contents": [{"role": "user", "parts": [{"text": "Hello!"}]}]
            })
        ))
    ),
    responses(
        (status = 200, description = "The model's response, or for `:streamGenerateContent`, its stream of responses", content((
            serde_json::Value = "application/json",
            example = json!({
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "Hello! How can I help?"}]},
                    "finishReason": "STOP",
                    "index": 0
                }],
                "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 6, "totalTokenCount": 9},
                "modelVersion": "my-model"
            })
        ))),
        (status = 400, description = "The request is invalid, or asks for something the model cannot serve", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": {"code": 400, "message": "The request body is not a valid request: missing field `contents`", "status": "INVALID_ARGUMENT"}
            })
        ))),
        (status = 404, description = "The model or method was not found"),
        (status = 503, description = "The model failed to load"),
    )
))]
pub(crate) async fn post(
    Extension(models): Extension<GeminiModels>,
    Extension(status): Extension<Arc<RuntimeStatus>>,
    Path(path): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let (model, method) = match model_and_method(&path) {
        Ok(found) => found,
        Err(error) => return error_response(&error),
    };
    let handler = match models.handler(model).await {
        Ok(handler) => handler,
        Err(refusal) => return error_response(&refusal_error(model, &refusal, &status)),
    };

    if method == Method::CountTokens {
        let request: CountTokensRequest = match parse(&body) {
            Ok(request) => request,
            Err(error) => return error_response(&error),
        };
        return match handler.count_tokens(model, request).await {
            Ok(count) => Json(count).into_response(),
            Err(error) => error_response(&error),
        };
    }

    let request: GenerateContentRequest = match parse(&body) {
        Ok(request) => request,
        Err(error) => return error_response(&error),
    };
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "ai_chat",
        input = %String::from_utf8_lossy(&body)
    );
    span.in_scope(
        || tracing::info!(target: "task_history", model = %model, api = "gemini", "labels"),
    );
    crate::task_history::correlation::record_task_history_trace_id(&span, &context);

    let span_clone = span.clone();
    let sse = query.get("alt").is_some_and(|alt| alt == "sse");
    async move {
        match method {
            Method::GenerateContent => match handler.generate(model, request).await {
                Ok(response) => {
                    tracing::info!(target: "task_history", parent: &span_clone, captured_output = %text_of(&response));
                    Json(response).into_response()
                }
                Err(error) => {
                    tracing::error!(target: "task_history", parent: &span_clone, "{error}");
                    error_response(&error)
                }
            },
            Method::StreamGenerateContent => match handler.generate_stream(model, request).await {
                Ok(responses) if sse => sse_response(responses, span_clone),
                Ok(responses) => json_array_response(responses, &span_clone).await,
                Err(error) => {
                    tracing::error!(target: "task_history", parent: &span_clone, "{error}");
                    error_response(&error)
                }
            },
            Method::CountTokens => error_response(&GeminiError::internal(
                "countTokens was not answered before the request was read.",
            )),
        }
    }
    .instrument(span)
    .await
}

/// The stream as server-sent events, one `data:` event per response, as the Gemini API sends
/// it with `alt=sse`. An error is sent as the error body, and ends the stream.
fn sse_response(responses: GenerateContentStream, span: Span) -> Response {
    struct State {
        responses: GenerateContentStream,
        span: Span,
        output: String,
        ended: bool,
    }
    let state = State {
        responses,
        span,
        output: String::new(),
        ended: false,
    };
    let stream = futures::stream::unfold(state, |mut state| async move {
        if state.ended {
            return None;
        }
        let next = state.responses.next().instrument(state.span.clone()).await;
        let event = match next {
            Some(Ok(response)) => {
                state.output.push_str(&text_of(&response));
                match serde_json::to_string(&response) {
                    Ok(data) => Event::default().data(data),
                    Err(e) => {
                        state.ended = true;
                        let error =
                            GeminiError::internal(format!("Failed to serialize a response: {e}"));
                        tracing::error!(target: "task_history", parent: &state.span, "{error}");
                        Event::default().data(error.body().to_string())
                    }
                }
            }
            Some(Err(error)) => {
                tracing::error!(target: "task_history", parent: &state.span, "{error}");
                state.ended = true;
                Event::default().data(error.body().to_string())
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

/// The stream as one JSON array of responses, as the Gemini API sends `streamGenerateContent`
/// without `alt=sse`. An error ends the array with the error body.
async fn json_array_response(mut responses: GenerateContentStream, span: &Span) -> Response {
    let mut items = Vec::new();
    let mut output = String::new();
    while let Some(next) = responses.next().instrument(span.clone()).await {
        match next {
            Ok(response) => {
                output.push_str(&text_of(&response));
                match serde_json::to_value(&response) {
                    Ok(value) => items.push(value),
                    Err(e) => {
                        let error =
                            GeminiError::internal(format!("Failed to serialize a response: {e}"));
                        tracing::error!(target: "task_history", parent: span, "{error}");
                        items.push(error.body());
                        break;
                    }
                }
            }
            Err(error) => {
                tracing::error!(target: "task_history", parent: span, "{error}");
                items.push(error.body());
                break;
            }
        }
    }
    tracing::info!(target: "task_history", parent: span, captured_output = %output);
    Json(Value::Array(items)).into_response()
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

    #[test]
    fn the_path_names_the_model_and_the_method() {
        assert_eq!(
            model_and_method("qwen:streamGenerateContent").expect("a method"),
            ("qwen", Method::StreamGenerateContent)
        );
        // A model id may itself contain a colon; the method is after the last one.
        assert_eq!(
            model_and_method("qwen3:4b:generateContent").expect("a method"),
            ("qwen3:4b", Method::GenerateContent)
        );
        assert_eq!(
            model_and_method("qwen:countTokens").expect("a method"),
            ("qwen", Method::CountTokens)
        );
        let unknown = model_and_method("qwen:embedContent").expect_err("not served");
        assert_eq!(unknown.http_status(), 404);
        assert_eq!(
            model_and_method(":generateContent")
                .expect_err("no model")
                .http_status(),
            400
        );
    }

    #[tokio::test]
    async fn errors_are_in_the_gemini_api_body() {
        let invalid = parse::<GenerateContentRequest>(&Bytes::from_static(b"[1, 2]"))
            .expect_err("an array is not a request");
        let (status, body) = json_body(error_response(&invalid)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], 400);
        assert_eq!(body["error"]["status"], "INVALID_ARGUMENT");
        assert!(
            body["error"]["message"].as_str().is_some_and(
                |message| message.starts_with("The request body is not a valid request")
            ),
            "{body}"
        );

        let unavailable = refusal_error("qwen", &Refusal::Unavailable, &RuntimeStatus::new());
        assert_eq!(
            json_body(error_response(&unavailable)).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"error": {"code": 503, "message": "Model 'qwen' is unavailable.", "status": "UNAVAILABLE"}})
            )
        );
    }

    #[test]
    fn a_layers_http_status_reads_as_its_gemini_status() {
        assert_eq!(
            status_of(StatusCode::UNAUTHORIZED),
            GeminiStatus::Unauthenticated
        );
        assert_eq!(
            status_of(StatusCode::PAYLOAD_TOO_LARGE),
            GeminiStatus::InvalidArgument
        );
        assert_eq!(
            status_of(StatusCode::SERVICE_UNAVAILABLE),
            GeminiStatus::Unavailable
        );
        assert_eq!(
            status_of(StatusCode::INTERNAL_SERVER_ERROR),
            GeminiStatus::Internal
        );
    }
}
