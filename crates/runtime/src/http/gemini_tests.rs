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

//! The Gemini API (`/v1beta/models/...`) and the Responses API of a model whose provider
//! has none, through the runtime's own routes, for models loaded from a Spicepod against the
//! mock providers of [`super::messages_tests`]. Both are answered through the model's chat.

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::messages_tests::{
    Seen, model, runtime_with, runtime_with_api_key, send, sse_events, start_provider, taken,
};

async fn get(routes: &Router, path: &str) -> (StatusCode, Value) {
    let response = routes
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).expect("a request"))
        .await
        .expect("a response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    (status, serde_json::from_slice(&bytes).expect("a JSON body"))
}

fn parsed(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"))
}

/// The text of a chat message's content, whether a string or a list of text parts.
fn text_of(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect(),
        _ => String::new(),
    }
}

fn ping() -> Value {
    json!({"contents": [{"role": "user", "parts": [{"text": "ping"}]}]})
}

#[tokio::test]
async fn a_chat_model_answers_generate_content_through_its_chat_completions() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model("gpt", "openai:gpt-4o", &endpoint, &[])]).await;
    taken(&seen);

    let (status, _, body) = send(&routes, "/v1beta/models/gpt:generateContent", &[], &ping()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body = parsed(&body);
    assert_eq!(
        body["candidates"][0]["content"],
        json!({"role": "model", "parts": [{"text": "pong"}]})
    );
    assert_eq!(body["candidates"][0]["finishReason"], "STOP");
    assert_eq!(
        body["usageMetadata"],
        json!({"promptTokenCount": 20, "candidatesTokenCount": 2, "totalTokenCount": 22, "cachedContentTokenCount": 8})
    );

    let received = taken(&seen);
    assert_eq!(
        received.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        ["/v1/chat/completions"],
        "the request is answered by the model's chat completions"
    );
    let messages = received[0].body["messages"]
        .as_array()
        .expect("chat messages");
    let last = messages.last().expect("a message");
    assert_eq!(
        (last["role"].as_str(), text_of(&last["content"])),
        (Some("user"), "ping".to_string())
    );
}

#[tokio::test]
async fn a_streamed_answer_is_sent_as_server_sent_events() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model("gpt", "openai:gpt-4o", &endpoint, &[])]).await;
    taken(&seen);

    let (status, headers, body) = send(
        &routes,
        "/v1beta/models/gpt:streamGenerateContent?alt=sse",
        &[],
        &ping(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );
    let events = sse_events(&body);
    let text: String = events
        .iter()
        .flat_map(|(_, data)| data["candidates"][0]["content"]["parts"].as_array().cloned().unwrap_or_default())
        .filter_map(|part| part["text"].as_str().map(str::to_string))
        .collect();
    assert_eq!(text, "pong");
    let last = &events.last().expect("an event").1;
    assert_eq!(last["candidates"][0]["finishReason"], "STOP", "{body}");
    assert_eq!(last["usageMetadata"]["totalTokenCount"], 22, "{body}");
    assert!(
        taken(&seen)
            .iter()
            .all(|r| r.path == "/v1/chat/completions" && r.body["stream"] == json!(true)),
        "the stream is a streamed chat completion"
    );

    // Without `alt=sse`, the same stream is one JSON array.
    let (status, _, body) =
        send(&routes, "/v1beta/models/gpt:streamGenerateContent", &[], &ping()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let responses = parsed(&body);
    assert!(
        responses.as_array().is_some_and(|items| !items.is_empty()),
        "{body}"
    );
}

#[tokio::test]
async fn a_function_call_reaches_the_client_as_a_function_call_part() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model("gpt", "openai:gpt-4o", &endpoint, &[])]).await;
    taken(&seen);

    let request = json!({
        "contents": [{"role": "user", "parts": [{"text": "Weather in Paris?"}]}],
        "tools": [{"functionDeclarations": [{
            "name": "get_weather",
            "description": "The weather in a city",
            "parameters": {"type": "OBJECT", "properties": {"city": {"type": "STRING"}}, "required": ["city"]}
        }]}]
    });
    let (status, _, body) = send(&routes, "/v1beta/models/gpt:generateContent", &[], &request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body = parsed(&body);
    let parts = body["candidates"][0]["content"]["parts"]
        .as_array()
        .expect("parts");
    let call = parts
        .iter()
        .find_map(|part| part.get("functionCall"))
        .unwrap_or_else(|| panic!("a function call part: {body}"));
    assert_eq!(
        (&call["name"], &call["args"]),
        (&json!("get_weather"), &json!({"city": "Paris"}))
    );

    let received = taken(&seen);
    let tools = received[0].body["tools"].as_array().expect("chat tools");
    assert_eq!(tools[0]["function"]["name"], "get_weather");
    assert_eq!(
        tools[0]["function"]["parameters"]["properties"]["city"]["type"],
        "string",
        "Gemini's schema types are translated to JSON Schema's"
    );
}

#[tokio::test]
async fn errors_are_in_the_gemini_api_error_body() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model("gpt", "openai:gpt-4o", &endpoint, &[])]).await;

    let (status, _, body) =
        send(&routes, "/v1beta/models/missing:generateContent", &[], &ping()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(
        parsed(&body),
        json!({"error": {"code": 404, "message": "Model 'missing' not found.", "status": "NOT_FOUND"}})
    );

    let (status, _, body) = send(&routes, "/v1beta/models/gpt:embedContent", &[], &ping()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(parsed(&body)["error"]["status"], "NOT_FOUND");

    let (status, _, body) = send(
        &routes,
        "/v1beta/models/gpt:generateContent",
        &[],
        &json!({"contents": "not a list"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(parsed(&body)["error"]["status"], "INVALID_ARGUMENT");
}

#[tokio::test]
async fn a_request_without_a_valid_api_key_is_refused_with_the_gemini_error_body() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with_api_key(
        vec![model("gpt", "openai:gpt-4o", &endpoint, &[])],
        "spice-key",
    )
    .await;
    taken(&seen);

    let (status, _, body) = send(&routes, "/v1beta/models/gpt:generateContent", &[], &ping()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    let body = parsed(&body);
    assert_eq!(
        (body["error"]["code"].as_u64(), body["error"]["status"].as_str()),
        (Some(401), Some("UNAUTHENTICATED")),
        "{body}"
    );
    assert!(taken(&seen).is_empty(), "the model is never called");

    // The Antigravity CLI's gateway mode sends the key as a bearer token.
    let (status, _, body) = send(
        &routes,
        "/v1beta/models/gpt:generateContent",
        &[("authorization", "Bearer spice-key")],
        &ping(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn models_are_listed_in_the_gemini_api_shape() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![
        model("gpt", "openai:gpt-4o", &endpoint, &[]),
        model("claude", "anthropic:claude-sonnet-5", &endpoint, &[]),
    ])
    .await;

    let (status, body) = get(&routes, "/v1beta/models").await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = body["models"]
        .as_array()
        .expect("models")
        .iter()
        .filter_map(|model| model["name"].as_str())
        .collect();
    assert_eq!(names, ["models/claude", "models/gpt"]);

    let (status, body) = get(&routes, "/v1beta/models/gpt").await;
    assert_eq!((status, &body["name"]), (StatusCode::OK, &json!("models/gpt")));
    let (status, body) = get(&routes, "/v1beta/models/missing").await;
    assert_eq!((status, &body["error"]["status"]), (StatusCode::NOT_FOUND, &json!("NOT_FOUND")));
}

/// A provider without a Responses API of its own answers `/v1/responses` through its chat:
/// for an Anthropic model, through Anthropic's Messages API.
#[tokio::test]
async fn a_model_without_a_native_responses_api_answers_it_through_its_chat() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-5",
        &endpoint,
        &[],
    )])
    .await;
    taken(&seen);

    let (status, _, body) = send(
        &routes,
        "/v1/responses",
        &[],
        &json!({"model": "claude", "input": "ping"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body = parsed(&body);
    assert_eq!(body["object"], "response");
    assert_eq!(body["status"], "completed");
    let text: String = body["output"]
        .as_array()
        .expect("output")
        .iter()
        .filter(|item| item["type"] == "message")
        .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
        .filter_map(|part| part["text"].as_str().map(str::to_string))
        .collect();
    assert_eq!(text, "pong");
    assert_eq!(
        taken(&seen).iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        ["/v1/messages"],
        "the chat of an Anthropic model calls Anthropic's Messages API"
    );

    let (status, _, body) = send(
        &routes,
        "/v1/responses",
        &[],
        &json!({"model": "claude", "input": "ping", "stream": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let events = sse_events(&body);
    assert_eq!(
        events.first().map(|(name, _)| name.as_str()),
        Some("response.created"),
        "{body}"
    );
    let (name, completed) = events.last().expect("an event");
    assert_eq!(name, "response.completed", "{body}");
    assert_eq!(completed["response"]["status"], "completed");
}

/// Previously refused, a feature that only a native Responses API can carry is still refused,
/// with a 400 that says why, rather than answered without it.
#[tokio::test]
async fn a_stateful_responses_request_to_a_chat_model_is_refused() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let routes = runtime_with(vec![model(
        "claude",
        "anthropic:claude-sonnet-5",
        &endpoint,
        &[],
    )])
    .await;
    taken(&seen);

    let (status, _, body) = send(
        &routes,
        "/v1/responses",
        &[],
        &json!({"model": "claude", "input": "ping", "previous_response_id": "resp_1"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("previous_response_id"), "{body}");
    assert!(taken(&seen).is_empty(), "the model is never called");
}
