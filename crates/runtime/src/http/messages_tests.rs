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

//! `/v1/messages` through the runtime's own routes, for models loaded from a Spicepod against
//! mock providers: an Anthropic model, which receives the request as sent, and an `OpenAI`
//! model, which receives it as a chat completion.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use app::AppBuilder;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use spicepod::component::model::Model;
use spicepod::component::runtime::{ApiKey, CorsConfig};
use spicepod::component::worker::Worker;
use tower::ServiceExt;

use crate::{Runtime, config::Config};

/// A request a mock provider received.
#[derive(Clone, Debug)]
pub(super) struct Received {
    pub(super) path: String,
    pub(super) headers: HeaderMap,
    pub(super) body: Value,
}

pub(super) type Seen = Arc<Mutex<Vec<Received>>>;

fn record(seen: &Seen, path: &str, headers: HeaderMap, body: &Value) {
    seen.lock().expect("seen lock").push(Received {
        path: path.to_string(),
        headers,
        body: body.clone(),
    });
}

/// The message the mock Anthropic answers `body` with: a call to `list_datasets` when it is
/// offered and has not been answered yet, else text.
fn anthropic_message(body: &Value) -> Value {
    let offers_list_datasets = body["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "list_datasets"));
    let answered = body["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .and_then(|message| message["content"].as_array())
        .is_some_and(|content| content.iter().any(|block| block["type"] == "tool_result"));
    let (content, stop_reason) = if offers_list_datasets && !answered {
        (
            json!([
                {"type": "text", "text": "Checking."},
                {"type": "tool_use", "id": "toolu_ds", "name": "list_datasets", "input": {}},
            ]),
            "tool_use",
        )
    } else if answered {
        (
            json!([{"type": "text", "text": "There are no datasets."}]),
            "end_turn",
        )
    } else {
        (json!([{"type": "text", "text": "pong"}]), "end_turn")
    };
    json!({
        "id": "msg_01upstream",
        "type": "message",
        "role": "assistant",
        "model": body["model"],
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {"input_tokens": 12, "cache_read_input_tokens": 4, "output_tokens": 3, "service_tier": "standard"},
    })
}

/// `message` as Anthropic streams it, with a `ping` and an event type no client knows of.
fn anthropic_events(message: &Value) -> String {
    let mut start = message.clone();
    start["content"] = json!([]);
    start["stop_reason"] = Value::Null;
    start["usage"]["output_tokens"] = json!(1);
    let mut events = vec![
        json!({"type": "message_start", "message": start}),
        json!({"type": "ping"}),
    ];
    for (index, block) in message["content"]
        .as_array()
        .expect("content")
        .iter()
        .enumerate()
    {
        match block["type"].as_str() {
            Some("text") => {
                events.push(json!({"type": "content_block_start", "index": index, "content_block": {"type": "text", "text": ""}}));
                events.push(json!({"type": "content_block_delta", "index": index, "delta": {"type": "text_delta", "text": block["text"]}}));
            }
            Some("tool_use") => {
                events.push(json!({"type": "content_block_start", "index": index, "content_block": {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}}}));
                events.push(json!({"type": "content_block_delta", "index": index, "delta": {"type": "input_json_delta", "partial_json": block["input"].to_string()}}));
            }
            other => panic!("no events for {other:?}"),
        }
        events.push(json!({"type": "content_block_stop", "index": index}));
    }
    events.push(json!({"type": "future_event", "detail": "passed through"}));
    events.push(json!({"type": "message_delta", "delta": {"stop_reason": message["stop_reason"], "stop_sequence": null}, "usage": {"output_tokens": message["usage"]["output_tokens"]}}));
    events.push(json!({"type": "message_stop"}));
    events.iter().fold(String::new(), |mut sse, event| {
        let name = event["type"].as_str().expect("a type");
        write!(sse, "event: {name}\ndata: {event}\n\n").expect("writing to a String");
        sse
    })
}

async fn anthropic(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    record(&seen, "/v1/messages", headers, &body);
    if body["metadata"]["user_id"] == "retire-me" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"type": "error", "error": {"type": "not_found_error", "message": "model: claude-sonnet-4-6"}, "request_id": "req_404"})),
        )
            .into_response();
    }
    if !body["temperature"].is_null() && body["thinking"].is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"type": "error", "error": {"type": "invalid_request_error", "message": "`temperature` is deprecated for this model."}})),
        )
            .into_response();
    }
    if body.to_string().contains("file_missing") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"type": "error", "error": {"type": "not_found_error", "message": "File not found: file_missing"}})),
        )
            .into_response();
    }
    if body["metadata"]["user_id"] == "overload-me" {
        return (
            StatusCode::from_u16(529).expect("a status"),
            Json(json!({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}, "request_id": "req_529"})),
        )
            .into_response();
    }
    let message = anthropic_message(&body);
    if body["stream"] == json!(true) {
        return (
            [(header::CONTENT_TYPE, "text/event-stream")],
            anthropic_events(&message),
        )
            .into_response();
    }
    Json(message).into_response()
}

async fn anthropic_count_tokens(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    record(&seen, "/v1/messages/count_tokens", headers, &body);
    Json(json!({"input_tokens": 42}))
}

/// An `OpenAI` chat completion: a call to `get_weather` when it is offered and has not been
/// answered yet, else `pong`.
async fn openai_chat(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    record(&seen, "/v1/chat/completions", headers, &body);
    let offers_weather = body["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool["function"]["name"] == "get_weather")
    });
    let answered = body["messages"].as_array().is_some_and(|messages| {
        messages
            .iter()
            .any(|message| message["role"] == "tool" && message["tool_call_id"] == "call_w")
    });
    let offers_list_datasets = body["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool["function"]["name"] == "list_datasets")
    });
    let listed = body["messages"].as_array().is_some_and(|messages| {
        messages
            .iter()
            .any(|message| message["role"] == "tool" && message["tool_call_id"] == "call_ds")
    });
    let (message, finish) = if offers_weather && !answered {
        (
            json!({"role": "assistant", "content": null, "tool_calls": [{"id": "call_w", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}]}),
            "tool_calls",
        )
    } else if offers_list_datasets && !offers_weather && !listed {
        (
            json!({"role": "assistant", "content": null, "tool_calls": [{"id": "call_ds", "type": "function", "function": {"name": "list_datasets", "arguments": "{}"}}]}),
            "tool_calls",
        )
    } else {
        (json!({"role": "assistant", "content": "pong"}), "stop")
    };
    let usage = json!({"prompt_tokens": 20, "completion_tokens": 2, "total_tokens": 22, "prompt_tokens_details": {"cached_tokens": 8}});
    if body["stream"] == json!(true) {
        let chunk = |delta: Value, finish: Value| json!({"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 0, "model": body["model"], "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
        let mut chunks = vec![chunk(json!({"role": "assistant"}), Value::Null)];
        if let Some(content) = message["content"].as_str() {
            chunks.push(chunk(json!({"content": content}), Value::Null));
        }
        if let Some(calls) = message["tool_calls"].as_array() {
            for (index, call) in calls.iter().enumerate() {
                chunks.push(chunk(json!({"tool_calls": [{"index": index, "id": call["id"], "type": "function", "function": {"name": call["function"]["name"], "arguments": call["function"]["arguments"]}}]}), Value::Null));
            }
        }
        chunks.push(chunk(json!({}), json!(finish)));
        chunks.push(json!({"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 0, "model": body["model"], "choices": [], "usage": usage}));
        let events: String = chunks
            .iter()
            .map(|chunk| format!("data: {chunk}\n\n"))
            .chain(std::iter::once("data: [DONE]\n\n".to_string()))
            .collect();
        return ([(header::CONTENT_TYPE, "text/event-stream")], events).into_response();
    }
    Json(json!({
        "id": "chatcmpl-1", "object": "chat.completion", "created": 0, "model": body["model"],
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": usage,
    }))
    .into_response()
}

/// An OpenAI-compatible model listing, with the limits gateways report: `gpt-4o`'s window
/// and output cap in `OpenRouter`'s shape.
async fn openai_models() -> Json<Value> {
    Json(json!({"object": "list", "data": [
        {"id": "gpt-4o", "object": "model", "context_length": 128_000, "top_provider": {"max_completion_tokens": 16_384}},
    ]}))
}

/// Anthropic's Models API, for one model.
async fn anthropic_model(Path(model): Path<String>) -> Json<Value> {
    Json(json!({"type": "model", "id": model, "max_input_tokens": 200_000, "max_tokens": 64_000}))
}

pub(super) async fn start_provider(seen: &Seen) -> String {
    let app = Router::new()
        .route("/v1/models", get(openai_models))
        .route("/v1/models/{model}", get(anthropic_model))
        .route("/v1/messages", post(anthropic))
        .route("/v1/messages/count_tokens", post(anthropic_count_tokens))
        .route("/v1/chat/completions", post(openai_chat))
        .with_state(Arc::clone(seen));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the mock provider");
    let address = listener.local_addr().expect("mock provider address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{address}")
}

pub(super) fn model(name: &str, from: &str, endpoint: &str, extra: &[(&str, &str)]) -> Model {
    let mut model = Model::new(from, name);
    let params: Vec<(&str, String)> = match from.split(':').next() {
        Some("anthropic") => vec![
            ("endpoint", format!("{endpoint}/v1")),
            ("anthropic_api_key", "spice-anthropic-key".to_string()),
        ],
        Some("openai") => vec![
            ("endpoint", format!("{endpoint}/v1")),
            ("openai_api_key", "spice-openai-key".to_string()),
        ],
        other => panic!("no mock params for {other:?}"),
    };
    for (key, value) in params.into_iter().chain(
        extra
            .iter()
            .map(|(key, value)| (*key, (*value).to_string())),
    ) {
        model.params.insert(key.to_string(), Value::String(value));
    }
    model
}

pub(super) async fn runtime_with(models: Vec<Model>) -> Router {
    runtime_with_cors(models, &CorsConfig::default()).await
}

async fn runtime_with_cors(models: Vec<Model>, cors: &CorsConfig) -> Router {
    runtime_with_options(models, Vec::new(), cors, None).await
}

/// A runtime serving `models` and the router `workers` over them.
pub(super) async fn runtime_with_workers(models: Vec<Model>, workers: Vec<Worker>) -> Router {
    runtime_with_options(models, workers, &CorsConfig::default(), None).await
}

/// A runtime whose routes accept only requests carrying `api_key`, as `runtime.auth.api_key`
/// configures.
pub(super) async fn runtime_with_api_key(models: Vec<Model>, api_key: &str) -> Router {
    runtime_with_cors_and_api_key(models, &CorsConfig::default(), api_key).await
}

/// A runtime that answers browsers as `cors` configures, and whose routes accept only
/// requests carrying `api_key`.
pub(super) async fn runtime_with_cors_and_api_key(
    models: Vec<Model>,
    cors: &CorsConfig,
    api_key: &str,
) -> Router {
    let auth =
        runtime_auth::api_key::ApiKeyAuth::new(vec![ApiKey::parse_str(&format!("{api_key}:rw"))]);
    runtime_with_options(
        models,
        Vec::new(),
        cors,
        Some(runtime_auth::layer::http::AuthLayer::new(Arc::new(auth))),
    )
    .await
}

async fn runtime_with_options(
    models: Vec<Model>,
    workers: Vec<Worker>,
    cors: &CorsConfig,
    auth: Option<runtime_auth::layer::http::AuthLayer>,
) -> Router {
    let mut app = AppBuilder::new("messages");
    for model in models {
        app = app.with_model(model);
    }
    for worker in workers {
        app = app.with_worker(worker);
    }
    let rt = Arc::new(Runtime::builder().with_app(app.build()).build().await);
    Arc::clone(&rt).load_models().await;
    Arc::clone(&rt).load_workers().await;
    let search = Arc::new(runtime_search::search_engine::SearchEngine::new(
        Arc::clone(&rt.df) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
        runtime_search::search_engine::parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
        crate::search::util::RuntimeTableProviderExplorer,
    ));
    super::routes::routes(
        &rt,
        Arc::new(Config::new()),
        search,
        auth,
        cors,
        false,
        #[cfg(feature = "mcp")]
        None,
    )
}

/// What a route answered: the status, the headers, and the body.
pub(super) async fn send(
    routes: &Router,
    path: &str,
    headers: &[(&str, &str)],
    body: &Value,
) -> (StatusCode, HeaderMap, String) {
    let mut request = Request::post(path).header(header::CONTENT_TYPE, "application/json");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = routes
        .clone()
        .oneshot(
            request
                .body(Body::from(body.to_string()))
                .expect("a request"),
        )
        .await
        .expect("a response");
    let status = response.status();
    let response_headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    (
        status,
        response_headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

/// The events of an SSE body, as (name, data) pairs.
pub(super) fn sse_events(body: &str) -> Vec<(String, Value)> {
    body.split("\n\n")
        .filter(|block| block.lines().any(|line| line.starts_with("data:")))
        .map(|block| {
            let mut name = String::new();
            let mut data = String::new();
            for line in block.lines() {
                if let Some(value) = line.strip_prefix("event:") {
                    name = value.trim().to_string();
                } else if let Some(value) = line.strip_prefix("data:") {
                    data = value.trim().to_string();
                }
            }
            (name, serde_json::from_str(&data).expect("JSON data"))
        })
        .collect()
}

fn header_of<'a>(received: &'a Received, name: &str) -> Option<&'a str> {
    received
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
}

pub(super) fn taken(seen: &Seen) -> Vec<Received> {
    std::mem::take(&mut *seen.lock().expect("seen lock"))
}

#[tokio::test]
async fn an_anthropic_model_receives_the_request_as_the_client_sent_it() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[],
    )])
    .await;
    taken(&seen);

    // Fields, blocks and tools no part of Spice knows of reach Anthropic unchanged.
    let request = json!({
        "model": "claude",
        "max_tokens": 100,
        "inference_geo": "us",
        "speed": "fast",
        "context_management": {"edits": [{"type": "clear_tool_uses_20250919"}]},
        "tools": [{"type": "web_search_20260209", "name": "web_search"}],
        "messages": [{"role": "user", "content": [
            {"type": "search_result", "source": "https://example.com", "title": "t", "content": [{"type": "text", "text": "x"}]},
            {"type": "text", "text": "ping", "cache_control": {"type": "ephemeral", "ttl": "1h"}},
        ]}],
    });
    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[
            ("x-api-key", "client-key"),
            ("anthropic-version", "2023-06-01"),
            (
                "anthropic-beta",
                "fast-mode-2026-02-01, context-management-2025-06-27",
            ),
        ],
        &request,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let received = taken(&seen);
    assert_eq!(received.len(), 1);
    let mut expected_upstream = request.clone();
    expected_upstream["model"] = json!("claude-sonnet-4-6");
    assert_eq!(received[0].path, "/v1/messages");
    assert_eq!(received[0].body, expected_upstream);
    assert_eq!(
        (
            header_of(&received[0], "x-api-key"),
            header_of(&received[0], "anthropic-version"),
            header_of(&received[0], "anthropic-beta"),
        ),
        (
            Some("spice-anthropic-key"),
            Some("2023-06-01"),
            Some("fast-mode-2026-02-01,context-management-2025-06-27"),
        ),
        "the model's own key, and the client's beta features only"
    );

    let mut expected_answer = anthropic_message(&expected_upstream);
    expected_answer["model"] = json!("claude");
    assert_eq!(
        serde_json::from_str::<Value>(&body).expect("a message"),
        expected_answer
    );
}

#[tokio::test]
async fn an_anthropic_models_stream_reaches_the_client_as_anthropic_sent_it() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[],
    )])
    .await;
    taken(&seen);

    let (status, headers, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "claude", "max_tokens": 100, "stream": true, "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );

    let mut upstream = anthropic_message(&taken(&seen)[0].body);
    upstream["model"] = json!("claude");
    let expected = sse_events(&anthropic_events(&upstream));
    assert_eq!(sse_events(&body), expected);
}

#[tokio::test]
async fn anthropic_errors_reach_the_client_in_their_own_shape() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[],
    )])
    .await;

    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "claude", "max_tokens": 100, "metadata": {"user_id": "overload-me"}, "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(
        (
            status.as_u16(),
            serde_json::from_str::<Value>(&body).expect("an envelope")
        ),
        (
            529,
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}, "request_id": "req_529"})
        )
    );

    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "claude", "max_tokens": 100, "metadata": {"user_id": "retire-me"}, "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(
        (
            status,
            serde_json::from_str::<Value>(&body).expect("an envelope")
        ),
        (
            StatusCode::NOT_FOUND,
            json!({"type": "error", "error": {"type": "not_found_error", "message": "Failed to answer a Messages API request with Anthropic model 'claude-sonnet-4-6': the API at the configured `endpoint` reports no such model. Check both `from: anthropic:<model_id>` and `endpoint` — an endpoint that does not route `/messages` answers with this same error. See: https://spiceai.org/docs/components/models/anthropic"}, "request_id": "req_404"})
        )
    );

    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "nonexistent", "max_tokens": 100, "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(
        (
            status,
            serde_json::from_str::<Value>(&body).expect("an envelope")
        ),
        (
            StatusCode::NOT_FOUND,
            json!({"type": "error", "error": {"type": "not_found_error", "message": "model 'nonexistent' not found"}})
        )
    );
}

#[tokio::test]
async fn the_spicepod_system_prompt_and_defaults_reach_anthropic() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let mut claude = model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[("system_prompt", "You answer about Spice.")],
    );
    claude.params.insert("temperature".to_string(), json!(0.3));
    let routes = runtime_with(vec![claude]).await;
    taken(&seen);

    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "claude", "max_tokens": 100, "system": "Be brief.", "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let received = taken(&seen);
    assert_eq!(
        (
            &received[0].body["system"],
            &received[0].body["temperature"]
        ),
        (&json!("You answer about Spice.\n\nBe brief."), &json!(0.3))
    );
}

#[tokio::test]
async fn an_anthropic_model_runs_spice_tools_in_one_message() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[("tools", "list_datasets")],
    )])
    .await;
    taken(&seen);

    let request = json!({"model": "claude", "max_tokens": 100, "messages": [{"role": "user", "content": "Which datasets are there?"}]});
    let (status, _, body) = send(&routes, "/v1/messages", &[], &request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let message: Value = serde_json::from_str(&body).expect("a message");
    assert_eq!(
        message["content"],
        json!([
            {"type": "text", "text": "Checking."},
            {"type": "mcp_tool_use", "id": "toolu_ds", "name": "list_datasets", "input": {}, "server_name": "spice"},
            {"type": "mcp_tool_result", "tool_use_id": "toolu_ds", "is_error": false, "content": [{"type": "text", "text": "[]"}]},
            {"type": "text", "text": "There are no datasets."},
        ])
    );
    assert_eq!(
        (
            &message["stop_reason"],
            &message["usage"]["input_tokens"],
            &message["usage"]["output_tokens"]
        ),
        (&json!("end_turn"), &json!(24), &json!(6)),
        "one message, with the usage of both rounds"
    );
    let rounds = taken(&seen);
    assert_eq!(rounds.len(), 2);
    assert_eq!(
        rounds[1].body["messages"][2],
        json!({"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_ds", "content": [{"type": "text", "text": "[]"}]}]})
    );

    // Streamed, the client builds the same message from the events.
    let mut streamed = request.clone();
    streamed["stream"] = json!(true);
    let (status, _, body) = send(&routes, "/v1/messages", &[], &streamed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let events = sse_events(&body);
    let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names
            .iter()
            .filter(|name| **name == "message_start")
            .count(),
        1
    );
    assert_eq!(names.last(), Some(&"message_stop"));
    let starts: Vec<&Value> = events
        .iter()
        .filter(|(name, _)| name == "content_block_start")
        .map(|(_, data)| &data["content_block"])
        .collect();
    assert_eq!(
        starts
            .iter()
            .map(|block| block["type"].clone())
            .collect::<Vec<_>>(),
        [
            json!("text"),
            json!("mcp_tool_use"),
            json!("mcp_tool_result"),
            json!("text")
        ]
    );
    let indexes: Vec<u64> = events
        .iter()
        .filter(|(name, _)| name == "content_block_start")
        .filter_map(|(_, data)| data["index"].as_u64())
        .collect();
    assert_eq!(
        indexes,
        [0, 1, 2, 3],
        "block indexes continue across rounds"
    );
}

#[tokio::test]
async fn any_other_chat_model_answers_through_its_chat_completions() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model("gpt", "openai:gpt-4o-mini", &endpoint, &[])]).await;
    taken(&seen);

    let request = json!({
        "model": "gpt",
        "max_tokens": 100,
        "system": [{"type": "text", "text": "Be brief.", "cache_control": {"type": "ephemeral"}}],
        "tools": [{"name": "get_weather", "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}}}],
        "messages": [{"role": "user", "content": "Weather in Paris?"}],
    });
    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[("x-api-key", "client-key")],
        &request,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let message: Value = serde_json::from_str(&body).expect("a message");
    assert_eq!(
        message,
        json!({
            "id": "msg_1", "type": "message", "role": "assistant", "model": "gpt",
            "content": [{"type": "tool_use", "id": "call_w", "name": "get_weather", "input": {"city": "Paris"}}],
            "stop_reason": "tool_use", "stop_sequence": null,
            "usage": {"input_tokens": 12, "output_tokens": 2, "cache_read_input_tokens": 8},
        })
    );
    let received = taken(&seen);
    assert_eq!(
        (
            received[0].path.as_str(),
            &received[0].body["model"],
            header_of(&received[0], "authorization")
        ),
        (
            "/v1/chat/completions",
            &json!("gpt-4o-mini"),
            Some("Bearer spice-openai-key")
        )
    );
    assert_eq!(
        received[0].body["messages"],
        json!([
            {"role": "system", "content": "Be brief."},
            {"role": "user", "content": "Weather in Paris?"},
        ])
    );

    // The tool result goes back as a tool message, and the stream carries the same answer.
    let mut follow_up = request.clone();
    follow_up["stream"] = json!(true);
    follow_up["messages"] = json!([
        {"role": "user", "content": "Weather in Paris?"},
        {"role": "assistant", "content": message["content"]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_w", "content": "18C, sunny"}]},
    ]);
    let (status, _, body) = send(&routes, "/v1/messages", &[], &follow_up).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let events = sse_events(&body);
    assert_eq!(
        events
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop"
        ]
    );
    assert_eq!(
        events[2].1["delta"],
        json!({"type": "text_delta", "text": "pong"})
    );
    assert_eq!(
        events[4].1,
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null}, "usage": {"output_tokens": 2, "input_tokens": 12, "cache_read_input_tokens": 8}})
    );
    let received = taken(&seen);
    assert_eq!(
        received[0].body["messages"].as_array().expect("messages")[2..],
        [
            json!({"role": "assistant", "tool_calls": [{"id": "call_w", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}]}),
            json!({"role": "tool", "content": "18C, sunny", "tool_call_id": "call_w"}),
        ]
    );
}

/// With `runtime.auth` on, both Messages routes refuse a request without a valid API key with the
/// Messages API's `authentication_error` envelope, which Anthropic SDKs read on every error, and
/// answer one with the key; other routes keep their own error bodies.
#[tokio::test]
async fn a_request_without_a_valid_api_key_is_refused_with_the_messages_error_envelope() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with_api_key(
        vec![model(
            "claude",
            "anthropic:claude-sonnet-4-6",
            &endpoint,
            &[],
        )],
        "spice-api-key",
    )
    .await;
    taken(&seen);
    let request = json!({"model": "claude", "max_tokens": 10, "messages": [{"role": "user", "content": "ping"}]});

    for path in ["/v1/messages", "/v1/messages/count_tokens"] {
        let refused: [&[(&str, &str)]; 3] = [
            &[],
            &[("x-api-key", "not-the-key")],
            &[("authorization", "Bearer not-the-key")],
        ];
        for headers in refused {
            let (status, response_headers, body) = send(&routes, path, headers, &request).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} {headers:?}");
            assert_eq!(
                response_headers[header::CONTENT_TYPE],
                "application/json",
                "{path} {headers:?}"
            );
            assert_eq!(
                serde_json::from_str::<Value>(&body).expect("an envelope"),
                json!({"type": "error", "error": {"type": "authentication_error", "message": "Unauthorized"}}),
                "{path} {headers:?}"
            );
        }
        let (status, _, body) =
            send(&routes, path, &[("x-api-key", "spice-api-key")], &request).await;
        assert_eq!(status, StatusCode::OK, "{path} with the API key: {body}");
    }
    assert_eq!(
        taken(&seen)
            .iter()
            .map(|received| received.path.as_str())
            .collect::<Vec<_>>(),
        ["/v1/messages", "/v1/messages/count_tokens"],
        "only the requests with the API key reach the provider"
    );

    let (status, _, body) = send(&routes, "/v1/chat/completions", &[], &request).await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::UNAUTHORIZED, "Unauthorized")
    );
}

#[tokio::test]
async fn only_an_anthropic_model_counts_tokens() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![
        model("claude", "anthropic:claude-sonnet-4-6", &endpoint, &[]),
        model("gpt", "openai:gpt-4o-mini", &endpoint, &[]),
    ])
    .await;
    taken(&seen);

    let request =
        |model: &str| json!({"model": model, "messages": [{"role": "user", "content": "ping"}]});
    let (status, _, body) = send(
        &routes,
        "/v1/messages/count_tokens",
        &[],
        &request("claude"),
    )
    .await;
    assert_eq!(
        (status, body.as_str()),
        (StatusCode::OK, r#"{"input_tokens":42}"#)
    );
    let received = taken(&seen);
    assert_eq!(
        (received[0].path.as_str(), &received[0].body["model"]),
        ("/v1/messages/count_tokens", &json!("claude-sonnet-4-6"))
    );

    let (status, _, body) = send(&routes, "/v1/messages/count_tokens", &[], &request("gpt")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_str::<Value>(&body).expect("an envelope")["error"]["type"],
        "invalid_request_error"
    );
    assert!(taken(&seen).is_empty(), "the chat model is never asked");
}

#[tokio::test]
async fn a_chat_model_with_spice_tools_streams_the_clients_tool_calls() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "gpt",
        "openai:gpt-4o-mini",
        &endpoint,
        &[("tools", "list_datasets")],
    )])
    .await;
    taken(&seen);

    let request = |stream: bool| {
        json!({
            "model": "gpt",
            "max_tokens": 100,
            "stream": stream,
            "tools": [{"name": "get_weather", "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}}}],
            "messages": [{"role": "user", "content": "Weather in Paris?"}],
        })
    };
    let expected_call = json!({"type": "tool_use", "id": "call_w", "name": "get_weather", "input": {"city": "Paris"}});

    let (status, _, body) = send(&routes, "/v1/messages", &[], &request(false)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let message: Value = serde_json::from_str(&body).expect("a message");
    assert_eq!(
        (&message["content"], &message["stop_reason"]),
        (&json!([expected_call]), &json!("tool_use"))
    );

    let (status, _, body) = send(&routes, "/v1/messages", &[], &request(true)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let events = sse_events(&body);
    let starts: Vec<Value> = events
        .iter()
        .filter(|(name, _)| name == "content_block_start")
        .map(|(_, data)| data["content_block"].clone())
        .collect();
    let input: String = events
        .iter()
        .filter(|(name, _)| name == "content_block_delta")
        .filter_map(|(_, data)| data["delta"]["partial_json"].as_str())
        .collect();
    assert_eq!(
        (starts, input.as_str()),
        (
            vec![json!({"type": "tool_use", "id": "call_w", "name": "get_weather", "input": {}})],
            r#"{"city":"Paris"}"#
        ),
        "the streamed tool call is the client's, whole: {events:?}"
    );
}

// The chat completions stream that `/v1/messages` translates: a chat model with Spice tools
// streams the client's own tool calls too, not only the `tool_calls` finish.
#[tokio::test]
async fn chat_completions_stream_the_clients_tool_calls_beside_spice_tools() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "gpt",
        "openai:gpt-4o-mini",
        &endpoint,
        &[("tools", "list_datasets")],
    )])
    .await;

    let (status, _, body) = send(
        &routes,
        "/v1/chat/completions",
        &[],
        &json!({
            "model": "gpt",
            "stream": true,
            "tools": [{"type": "function", "function": {"name": "get_weather", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}}],
            "messages": [{"role": "user", "content": "Weather in Paris?"}],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let chunks: Vec<Value> = body
        .split("\n\n")
        .filter_map(|event| event.strip_prefix("data: "))
        .filter(|data| *data != "[DONE]")
        .map(|data| serde_json::from_str(data).expect("a chunk"))
        .collect();
    let calls: Vec<(Value, Value, String)> = chunks
        .iter()
        .flat_map(|chunk| chunk["choices"].as_array().cloned().unwrap_or_default())
        .flat_map(|choice| {
            choice["delta"]["tool_calls"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .map(|call| {
            (
                call["id"].clone(),
                call["function"]["name"].clone(),
                call["function"]["arguments"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
        })
        .collect();
    assert_eq!(
        calls,
        [(
            json!("call_w"),
            json!("get_weather"),
            r#"{"city":"Paris"}"#.to_string()
        )],
        "{chunks:?}"
    );
}

#[tokio::test]
async fn tokens_are_counted_without_the_defaults_of_a_messages_request() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let mut claude = model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[("system_prompt", "You answer about Spice.")],
    );
    claude.params.insert("temperature".to_string(), json!(0.3));
    claude
        .params
        .insert("max_completion_tokens".to_string(), json!(4096));
    let routes = runtime_with(vec![claude]).await;
    taken(&seen);

    let (status, _, body) = send(
        &routes,
        "/v1/messages/count_tokens",
        &[],
        &json!({"model": "claude", "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        taken(&seen)[0].body,
        json!({"model": "claude-sonnet-4-6", "system": "You answer about Spice.", "messages": [{"role": "user", "content": "ping"}]}),
        "count_tokens takes no sampling or output fields"
    );
}

#[tokio::test]
async fn a_rejected_spicepod_default_names_the_param_that_sent_it() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let mut claude = model("claude", "anthropic:claude-sonnet-4-6", &endpoint, &[]);
    claude.params.insert("temperature".to_string(), json!(0.3));
    let routes = runtime_with(vec![claude]).await;

    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "claude", "max_tokens": 100, "thinking": {"type": "adaptive"}, "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(
        (
            status,
            serde_json::from_str::<Value>(&body).expect("an envelope")
        ),
        (
            StatusCode::BAD_REQUEST,
            json!({"type": "error", "error": {"type": "invalid_request_error", "message": "Failed to answer a Messages API request with Anthropic model 'claude-sonnet-4-6': the model does not accept the `temperature` parameter, which model 'claude' sets in its `params` (as `temperature`, `anthropic_temperature` or `openai_temperature`) for every request that leaves it unset. Remove `temperature` from the model's `params`, or set `from: anthropic:<model_id>` to a model that accepts it. Cause: `temperature` is deprecated for this model. See: https://spiceai.org/docs/components/models/anthropic"}})
        )
    );
}

#[tokio::test]
async fn a_not_found_error_about_something_else_than_the_model_is_left_as_is() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[],
    )])
    .await;

    let (status, _, body) = send(
        &routes,
        "/v1/messages",
        &[],
        &json!({"model": "claude", "max_tokens": 100, "messages": [{"role": "user", "content": [
            {"type": "document", "source": {"type": "file", "file_id": "file_missing"}},
            {"type": "text", "text": "Summarize it."},
        ]}]}),
    )
    .await;
    assert_eq!(
        (
            status,
            serde_json::from_str::<Value>(&body).expect("an envelope")
        ),
        (
            StatusCode::NOT_FOUND,
            json!({"type": "error", "error": {"type": "not_found_error", "message": "File not found: file_missing"}})
        )
    );
}

/// Spice's tools look the same on every model: a chat model's calls to them come back as
/// `mcp_tool_use` and `mcp_tool_result` blocks, as an Anthropic model's do.
#[tokio::test]
async fn a_chat_model_runs_spice_tools_in_one_message() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "gpt",
        "openai:gpt-4o-mini",
        &endpoint,
        &[("tools", "list_datasets")],
    )])
    .await;
    taken(&seen);

    let request = json!({"model": "gpt", "max_tokens": 100, "messages": [{"role": "user", "content": "Which datasets are there?"}]});
    let (status, _, body) = send(&routes, "/v1/messages", &[], &request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let message: Value = serde_json::from_str(&body).expect("a message");
    assert_eq!(
        (&message["content"], &message["stop_reason"]),
        (
            &json!([
                {"type": "mcp_tool_use", "id": "call_ds", "name": "list_datasets", "input": {}, "server_name": "spice"},
                {"type": "mcp_tool_result", "tool_use_id": "call_ds", "is_error": false, "content": [{"type": "text", "text": "[]"}]},
                {"type": "text", "text": "pong"},
            ]),
            &json!("end_turn")
        )
    );
    assert_eq!(taken(&seen).len(), 2, "two rounds");

    let mut streamed = request.clone();
    streamed["stream"] = json!(true);
    let (status, _, body) = send(&routes, "/v1/messages", &[], &streamed).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let starts: Vec<Value> = sse_events(&body)
        .iter()
        .filter(|(name, _)| name == "content_block_start")
        .map(|(_, data)| data["content_block"]["type"].clone())
        .collect();
    assert_eq!(
        starts,
        [
            json!("mcp_tool_use"),
            json!("mcp_tool_result"),
            json!("text")
        ]
    );
}

/// A browser calling the Messages API sends its `anthropic-*` headers, which a CORS preflight
/// has to allow for the call to be made at all.
#[tokio::test]
async fn a_browser_preflight_allows_the_messages_api_headers() {
    let routes = runtime_with_cors(
        Vec::new(),
        &CorsConfig {
            enabled: true,
            allowed_origins: vec!["https://app.example.com".to_string()],
        },
    )
    .await;
    let requested = "content-type, x-api-key, anthropic-version, anthropic-beta, anthropic-dangerous-direct-browser-access";
    for path in ["/v1/messages", "/v1/messages/count_tokens"] {
        let response = routes
            .clone()
            .oneshot(
                Request::builder()
                    .method(axum::http::Method::OPTIONS)
                    .uri(path)
                    .header(header::ORIGIN, "https://app.example.com")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, requested)
                    .body(Body::empty())
                    .expect("a preflight"),
            )
            .await
            .expect("a response");
        let header_of = |name| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_ascii_lowercase)
        };
        assert_eq!(
            header_of(header::ACCESS_CONTROL_ALLOW_ORIGIN).as_deref(),
            Some("https://app.example.com"),
            "{path}"
        );
        let allowed = header_of(header::ACCESS_CONTROL_ALLOW_HEADERS).unwrap_or_default();
        let allowed: Vec<&str> = allowed.split(',').map(str::trim).collect();
        for name in requested.split(',').map(str::trim) {
            assert!(
                allowed.contains(&name),
                "{path}: {name} is not allowed: {allowed:?}"
            );
        }
    }
}

/// A model's Spicepod `tool_choice` forces the first round only, as a request's own does: a
/// loop that called a Spice tool tells the next round `auto`, which the default does not
/// override.
#[tokio::test]
async fn a_spicepod_tool_choice_default_forces_only_the_first_round() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-4-6",
        &endpoint,
        &[("tools", "list_datasets"), ("tool_choice", "required")],
    )])
    .await;
    taken(&seen);

    for stream in [false, true] {
        let request = json!({"model": "claude", "max_tokens": 100, "stream": stream, "messages": [{"role": "user", "content": "Which datasets are there?"}]});
        let (status, _, body) = send(&routes, "/v1/messages", &[], &request).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rounds = taken(&seen);
        assert_eq!(
            rounds
                .iter()
                .map(|round| round.body["tool_choice"].clone())
                .collect::<Vec<_>>(),
            [json!({"type": "any"}), json!({"type": "auto"})],
            "stream: {stream}"
        );
    }
}
