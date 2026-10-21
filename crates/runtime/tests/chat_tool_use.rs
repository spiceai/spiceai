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

//! `/v1/chat/completions` through the Spice tool-use loop, against an in-process mock of the
//! Anthropic Messages API that records every request the runtime sends it.
//!
//! The mock's model writes [`PREAMBLE`] and calls the `sql` tool, then answers with [`ANSWER`]
//! once it has the tool result.

#![expect(
    clippy::expect_used,
    reason = "set-up and helpers outside `#[test]` functions fail fast with `expect`"
)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use app::AppBuilder;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use parking_lot::Mutex;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use serde_json::{Value, json};
use spicepod::component::model::Model;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const QUESTION: &str = "How many rows are there?";
/// A question the mock's model answers after two `sql` calls rather than one.
const QUESTION_TWO_CALLS: &str = "How many rows are there? Check twice.";
const PREAMBLE: &str = "Let me check.";
const ANSWER: &str = "There are 3 rows.";
/// The answer to a request for structured output, in the shape `/v1/nsql` asks for.
const NSQL_ANSWER: &str = r#"{"sql": "SELECT 3 AS n"}"#;

/// The requests the runtime sent to the mock, in order.
#[derive(Clone, Default)]
struct Recorded(Arc<Mutex<Vec<Value>>>);

/// Whether the last message of `request` carries a tool result.
fn has_tool_result(request: &Value) -> bool {
    request["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .and_then(|last| last["content"].as_array())
        .is_some_and(|blocks| blocks.iter().any(|b| b["type"] == "tool_result"))
}

/// The number of tool results across the messages of `request`.
fn tool_results(request: &Value) -> usize {
    request["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|message| message["content"].as_array())
        .flatten()
        .filter(|block| block["type"] == "tool_result")
        .count()
}

/// Whether `request` asks [`QUESTION_TWO_CALLS`].
fn asks_twice(request: &Value) -> bool {
    request["messages"][0]["content"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|block| block["text"] == QUESTION_TWO_CALLS)
}

/// The text and the tool call, if any, the mock's model replies with: a plain reply to a request
/// without tools (the runtime's health check); [`PREAMBLE`] and a `sql` call until it has the
/// results of one call, or of two for [`QUESTION_TWO_CALLS`]; then [`NSQL_ANSWER`] to a request
/// for structured output and [`ANSWER`] to any other.
fn reply(request: &Value) -> (&'static str, Option<Value>) {
    if request["tools"].as_array().is_none_or(Vec::is_empty) {
        return ("ok", None);
    }
    let calls = tool_results(request);
    if calls >= if asks_twice(request) { 2 } else { 1 } {
        let structured = request
            .get("output_format")
            .is_some_and(|format| !format.is_null());
        return (if structured { NSQL_ANSWER } else { ANSWER }, None);
    }
    (
        PREAMBLE,
        Some(json!({
            "id": format!("toolu_sql_{calls}"), "name": "sql",
            "input": { "query": "SELECT 3 AS n" },
        })),
    )
}

fn sse_event(event: &str, data: &Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

/// The Messages API stream for `text` followed by `tool_use`.
fn sse_body(model: &Value, text: &str, tool_use: Option<&Value>) -> String {
    let mut body = sse_event(
        "message_start",
        &json!({ "type": "message_start", "message": {
            "id": "msg_mock", "type": "message", "role": "assistant", "content": [],
            "model": model, "stop_reason": null, "stop_sequence": null,
            "usage": { "input_tokens": 10, "output_tokens": 1 },
        }}),
    );
    body += &sse_event(
        "content_block_start",
        &json!({ "type": "content_block_start", "index": 0,
                 "content_block": { "type": "text", "text": "" } }),
    );
    body += &sse_event(
        "content_block_delta",
        &json!({ "type": "content_block_delta", "index": 0,
                 "delta": { "type": "text_delta", "text": text } }),
    );
    body += &sse_event(
        "content_block_stop",
        &json!({ "type": "content_block_stop", "index": 0 }),
    );
    if let Some(tool_use) = tool_use {
        body += &sse_event(
            "content_block_start",
            &json!({ "type": "content_block_start", "index": 1, "content_block": {
                "type": "tool_use", "id": tool_use["id"], "name": tool_use["name"], "input": {},
            }}),
        );
        body += &sse_event(
            "content_block_delta",
            &json!({ "type": "content_block_delta", "index": 1, "delta": {
                "type": "input_json_delta", "partial_json": tool_use["input"].to_string(),
            }}),
        );
        body += &sse_event(
            "content_block_stop",
            &json!({ "type": "content_block_stop", "index": 1 }),
        );
    }
    let stop_reason = if tool_use.is_some() {
        "tool_use"
    } else {
        "end_turn"
    };
    body += &sse_event(
        "message_delta",
        &json!({ "type": "message_delta",
                 "delta": { "stop_reason": stop_reason, "stop_sequence": null },
                 "usage": { "output_tokens": 5 } }),
    );
    body += &sse_event("message_stop", &json!({ "type": "message_stop" }));
    body
}

async fn messages(State(recorded): State<Recorded>, Json(request): Json<Value>) -> Response {
    recorded.0.lock().push(request.clone());
    let (text, tool_use) = reply(&request);
    if request["stream"] == true {
        return (
            [(CONTENT_TYPE, "text/event-stream")],
            sse_body(&request["model"], text, tool_use.as_ref()),
        )
            .into_response();
    }
    let mut content = vec![json!({ "type": "text", "text": text })];
    if let Some(tool_use) = &tool_use {
        let mut block = tool_use.clone();
        block["type"] = json!("tool_use");
        content.push(block);
    }
    Json(json!({
        "id": "msg_mock", "type": "message", "role": "assistant", "content": content,
        "model": request["model"],
        "stop_reason": if tool_use.is_some() { "tool_use" } else { "end_turn" },
        "stop_sequence": null, "usage": { "input_tokens": 10, "output_tokens": 5 },
    }))
    .into_response()
}

/// Starts the mock Messages API, and returns its base URL and the requests it records.
async fn start_mock_anthropic() -> anyhow::Result<(String, Recorded)> {
    let recorded = Recorded::default();
    let app = Router::new()
        .route("/v1/messages", post(messages))
        .with_state(recorded.clone());
    let listener = tokio::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((format!("http://127.0.0.1:{port}/v1"), recorded))
}

/// Starts a runtime serving model `claude` from the mock at `endpoint` with the `sql` tool, and
/// returns its HTTP base URL once it reports ready.
async fn start_runtime(endpoint: &str) -> anyhow::Result<String> {
    // Both listeners are bound before either is dropped, so the two ports differ.
    let flight_listener = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))?;
    let http_listener = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))?;
    let flight_port = flight_listener.local_addr()?.port();
    let http_port = http_listener.local_addr()?.port();
    drop(flight_listener);
    drop(http_listener);

    let mut model = Model::new("anthropic:claude-sonnet-4-6", "claude");
    model
        .params
        .insert("anthropic_api_key".to_string(), "test".into());
    model.params.insert("endpoint".to_string(), endpoint.into());
    model.params.insert("tools".to_string(), "sql".into());
    let app = AppBuilder::new("chat_tool_use").with_model(model).build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    Arc::clone(&rt).load_components().await;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));
    tokio::spawn(async move {
        Box::pin(rt.start_servers(api_config, None, EndpointAuth::default())).await
    });

    let base_url = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = client
            .get(format!("{base_url}/v1/ready"))
            .send()
            .await
            .map(|r| r.status());
        match status {
            Ok(status) if status.is_success() => return Ok(base_url),
            last if Instant::now() >= deadline => {
                anyhow::bail!("runtime was not ready within 60s; last /v1/ready: {last:?}")
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

async fn chat_completion(base_url: &str, stream: bool) -> anyhow::Result<String> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/chat/completions"))
        .json(&json!({
            "model": "claude", "stream": stream,
            "messages": [{ "role": "user", "content": QUESTION }],
        }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(
        status.is_success(),
        "chat completion failed: {status} {body}"
    );
    Ok(body)
}

/// The text a streaming client assembles from the `data:` events of `body`.
fn streamed_text(body: &str) -> anyhow::Result<String> {
    let mut text = String::new();
    for data in body.lines().filter_map(|line| line.strip_prefix("data: ")) {
        if data == "[DONE]" {
            continue;
        }
        let chunk: Value = serde_json::from_str(data)?;
        for choice in chunk["choices"].as_array().into_iter().flatten() {
            text.push_str(choice["delta"]["content"].as_str().unwrap_or_default());
        }
    }
    Ok(text)
}

/// Each block of the assistant turn the runtime replayed to the model with the `sql` result, as
/// its type and its text or tool name.
fn replayed_assistant_turn(recorded: &Recorded) -> Vec<(String, String)> {
    let requests = recorded.0.lock();
    let follow_up = requests
        .iter()
        .find(|r| has_tool_result(r))
        .expect("the runtime sends the model the sql result");
    let messages = follow_up["messages"]
        .as_array()
        .expect("a request carries messages");
    let assistant = &messages[messages.len() - 2];
    assert_eq!(assistant["role"], "assistant");
    assistant["content"]
        .as_array()
        .expect("an assistant turn carries content blocks")
        .iter()
        .map(|block| {
            let detail = block["text"].as_str().or(block["name"].as_str());
            (
                block["type"].as_str().unwrap_or_default().to_string(),
                detail.unwrap_or_default().to_string(),
            )
        })
        .collect()
}

fn the_turn_as_written() -> Vec<(String, String)> {
    vec![
        ("text".to_string(), PREAMBLE.to_string()),
        ("tool_use".to_string(), "sql".to_string()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streamed_answer_keeps_the_text_beside_a_tool_call_and_sets_it_apart()
-> anyhow::Result<()> {
    let (endpoint, recorded) = start_mock_anthropic().await?;
    let base_url = start_runtime(&endpoint).await?;

    let body = chat_completion(&base_url, true).await?;

    assert_eq!(streamed_text(&body)?, format!("{PREAMBLE}\n\n{ANSWER}"));
    assert_eq!(replayed_assistant_turn(&recorded), the_turn_as_written());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_non_streamed_answer_includes_the_text_beside_a_tool_call() -> anyhow::Result<()> {
    let (endpoint, recorded) = start_mock_anthropic().await?;
    let base_url = start_runtime(&endpoint).await?;

    let body: Value = serde_json::from_str(&chat_completion(&base_url, false).await?)?;

    assert_eq!(
        body["choices"][0]["message"]["content"],
        format!("{PREAMBLE}\n\n{ANSWER}")
    );
    assert_eq!(replayed_assistant_turn(&recorded), the_turn_as_written());
    Ok(())
}

/// `/v1/nsql` asks for structured output and parses the answer as JSON, so the text the model
/// wrote beside its tool call must not reach the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nsql_answers_when_the_model_writes_text_beside_a_tool_call() -> anyhow::Result<()> {
    let (endpoint, recorded) = start_mock_anthropic().await?;
    let base_url = start_runtime(&endpoint).await?;

    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/nsql"))
        .json(&json!({ "query": QUESTION, "model": "claude" }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(serde_json::from_str::<Value>(&body)?, json!([{ "n": 3 }]));
    assert_eq!(replayed_assistant_turn(&recorded), the_turn_as_written());
    Ok(())
}

/// Each round of a tool-using answer keeps what Anthropic's prompt caching reuses: every request
/// asks for caching, and starts with the whole of the request before it, so the text the model
/// wrote beside its first tool call is part of the prefix the third round can read from cache.
/// <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_tool_round_extends_the_request_before_it() -> anyhow::Result<()> {
    let (endpoint, recorded) = start_mock_anthropic().await?;
    let base_url = start_runtime(&endpoint).await?;

    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/chat/completions"))
        .json(&json!({
            "model": "claude", "prompt_cache_key": "chat_tool_use",
            "messages": [{ "role": "user", "content": QUESTION_TWO_CALLS }],
        }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(
        status.is_success(),
        "chat completion failed: {status} {body}"
    );

    let rounds: Vec<Value> = recorded
        .0
        .lock()
        .iter()
        .filter(|request| asks_twice(request))
        .cloned()
        .collect();
    assert_eq!(rounds.len(), 3, "two tool calls, then the answer");
    for (i, round) in rounds.iter().enumerate() {
        assert_eq!(
            round["cache_control"],
            json!({ "type": "ephemeral" }),
            "round {i}"
        );
    }
    for (i, (before, after)) in rounds.iter().zip(rounds.iter().skip(1)).enumerate() {
        assert_eq!(after["tools"], before["tools"], "round {}", i + 1);
        assert_eq!(after["system"], before["system"], "round {}", i + 1);
        let before = before["messages"]
            .as_array()
            .expect("a request carries messages");
        let after = after["messages"]
            .as_array()
            .expect("a request carries messages");
        assert_eq!(
            after.get(..before.len()),
            Some(before.as_slice()),
            "round {} starts with the request before it",
            i + 1
        );
    }
    assert_eq!(
        rounds[2]["messages"][1]["content"][0],
        json!({ "type": "text", "text": PREAMBLE }),
        "the third round's prefix carries the text the model wrote beside its first call"
    );
    Ok(())
}
