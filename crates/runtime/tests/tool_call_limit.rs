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

//! The most Spice tool calls Spice runs from one model response, on `/v1/chat/completions` and
//! `/v1/responses`, streamed and not, against an in-process mock of the `OpenAI` API.
//!
//! The mock's model makes as many `sql` calls in one response as the question asks for, then
//! answers [`ANSWER`] once it has their results. Which calls ran shows in the request that
//! carries the results: a call that ran has the query's result, and one that did not has the
//! message that says so.

use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use rstest::rstest;
use serde_json::{Value, json};
use spicepod::component::model::Model;

use super::{LOCALHOST, Recorded, start_runtime_with, streamed_text};
use std::net::SocketAddr;

/// The most Spice tool calls Spice runs from one model response.
const LIMIT: usize = 64;
const ANSWER: &str = "done";
const QUERY: &str = "SELECT 1 AS n";
/// What the `sql` tool returns for [`QUERY`].
const QUERY_RESULT: &str = r#"[{"n":1}]"#;

/// What the model is told for a call Spice did not run, in a response that made `calls` calls.
fn not_run(calls: usize) -> String {
    format!(
        "Spice did not run this tool call: the response made {calls} tool calls, and Spice runs at most {LIMIT} from one response. Make fewer tool calls at once."
    )
}

/// The question that makes the mock's model make `calls` `sql` calls in one response.
fn question(calls: usize) -> String {
    format!("Run the query. CALLS={calls}")
}

/// The number of calls the question in `request` asks for, if it asks for any.
fn requested_calls(request: &Value) -> Option<usize> {
    let request = request.to_string();
    let (_, rest) = request.split_once("CALLS=")?;
    rest.chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

#[derive(Clone, Copy, Debug)]
enum Api {
    ChatCompletions,
    Responses,
}

impl Api {
    /// A tool result `text` as `self` sends it to the model: a chat completions tool message
    /// carries it as a `JSON` string.
    fn sent(self, text: &str) -> String {
        match self {
            Self::ChatCompletions => Value::String(text.to_string()).to_string(),
            Self::Responses => text.to_string(),
        }
    }
}

/// The tool results in `request`, in order, as each call's ID and result.
fn tool_results(api: Api, request: &Value) -> Vec<(String, String)> {
    let (items, kind, id, output) = match api {
        Api::ChatCompletions => (&request["messages"], "tool", "tool_call_id", "content"),
        Api::Responses => (
            &request["input"],
            "function_call_output",
            "call_id",
            "output",
        ),
    };
    let kind_key = match api {
        Api::ChatCompletions => "role",
        Api::Responses => "type",
    };
    items
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item[kind_key] == kind)
        .map(|item| {
            (
                item[id].as_str().unwrap_or_default().to_string(),
                item[output].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// The `sql` calls the mock's model makes in reply to `request`: as many as its question asks for
/// until it has tool results, and none to a request without tools (the runtime's health check)
/// or one that has them.
fn calls_to_make(api: Api, request: &Value) -> usize {
    if request["tools"].as_array().is_none_or(Vec::is_empty)
        || !tool_results(api, request).is_empty()
    {
        return 0;
    }
    requested_calls(request).unwrap_or_default()
}

fn arguments() -> String {
    json!({ "query": QUERY }).to_string()
}

fn sse(event: Option<&str>, data: &Value) -> String {
    match event {
        Some(event) => format!("event: {event}\ndata: {data}\n\n"),
        None => format!("data: {data}\n\n"),
    }
}

async fn chat_completions(
    State(recorded): State<Recorded>,
    Json(request): Json<Value>,
) -> Response {
    recorded.0.lock().push(request.clone());
    let calls = calls_to_make(Api::ChatCompletions, &request);
    let tool_call = |i: usize| {
        json!({
            "id": format!("call_{i}"), "type": "function",
            "function": { "name": "sql", "arguments": arguments() },
        })
    };
    let finish_reason = if calls > 0 { "tool_calls" } else { "stop" };
    if request["stream"] == true {
        let chunk = |delta: Value, finish_reason: Option<&str>| {
            sse(
                None,
                &json!({
                    "id": "chatcmpl-mock", "object": "chat.completion.chunk", "created": 0,
                    "model": request["model"],
                    "choices": [{ "index": 0, "delta": delta, "finish_reason": finish_reason }],
                }),
            )
        };
        let mut body = chunk(json!({ "role": "assistant", "content": "" }), None);
        if calls == 0 {
            body += &chunk(json!({ "content": ANSWER }), None);
        }
        for i in 0..calls {
            let mut call = tool_call(i);
            call["index"] = json!(i);
            body += &chunk(json!({ "tool_calls": [call] }), None);
        }
        body += &chunk(json!({}), Some(finish_reason));
        body += "data: [DONE]\n\n";
        return ([(CONTENT_TYPE, "text/event-stream")], body).into_response();
    }
    let message = if calls == 0 {
        json!({ "role": "assistant", "content": ANSWER })
    } else {
        json!({ "role": "assistant", "content": null, "tool_calls": (0..calls).map(tool_call).collect::<Vec<_>>() })
    };
    Json(json!({
        "id": "chatcmpl-mock", "object": "chat.completion", "created": 0,
        "model": request["model"],
        "choices": [{ "index": 0, "message": message, "finish_reason": finish_reason }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 },
    }))
    .into_response()
}

/// Output item `index` of the mock's response: a call to `sql`, or the answer.
fn output_item(index: usize, is_call: bool) -> Value {
    if is_call {
        json!({
            "type": "function_call", "id": format!("fc_{index}"), "call_id": format!("call_{index}"),
            "name": "sql", "arguments": arguments(), "status": "completed",
        })
    } else {
        json!({
            "type": "message", "id": "msg_0", "role": "assistant", "status": "completed",
            "content": [{ "type": "output_text", "text": ANSWER, "annotations": [], "logprobs": null }],
        })
    }
}

fn response_object(request: &Value, status: &str, output: &[Value]) -> Value {
    json!({
        "id": "resp_mock", "object": "response", "created_at": 0, "model": request["model"],
        "status": status, "output": output,
        "usage": {
            "input_tokens": 1, "input_tokens_details": { "cached_tokens": 0 },
            "output_tokens": 1, "output_tokens_details": { "reasoning_tokens": 0 },
            "total_tokens": 2,
        },
    })
}

async fn responses(State(recorded): State<Recorded>, Json(request): Json<Value>) -> Response {
    recorded.0.lock().push(request.clone());
    let calls = calls_to_make(Api::Responses, &request);
    let output: Vec<Value> = if calls == 0 {
        vec![output_item(0, false)]
    } else {
        (0..calls).map(|i| output_item(i, true)).collect()
    };
    if request["stream"] != true {
        return Json(response_object(&request, "completed", &output)).into_response();
    }
    let mut events = vec![(
        "response.created",
        json!({ "response": response_object(&request, "in_progress", &[]) }),
    )];
    for (index, item) in output.iter().enumerate() {
        let item_id = item["id"].clone();
        events.push((
            "response.output_item.added",
            json!({ "output_index": index, "item": item }),
        ));
        if item["type"] == "function_call" {
            events.push((
                "response.function_call_arguments.delta",
                json!({ "item_id": item_id, "output_index": index, "delta": arguments() }),
            ));
            events.push((
                "response.function_call_arguments.done",
                json!({ "item_id": item_id, "output_index": index, "arguments": arguments() }),
            ));
        } else {
            events.push((
                "response.output_text.delta",
                json!({ "item_id": item_id, "output_index": index, "content_index": 0, "delta": ANSWER }),
            ));
        }
        events.push((
            "response.output_item.done",
            json!({ "output_index": index, "item": item }),
        ));
    }
    events.push((
        "response.completed",
        json!({ "response": response_object(&request, "completed", &output) }),
    ));
    let body: String = events
        .into_iter()
        .enumerate()
        .map(|(sequence_number, (event, mut data))| {
            data["type"] = json!(event);
            data["sequence_number"] = json!(sequence_number);
            sse(Some(event), &data)
        })
        .collect();
    ([(CONTENT_TYPE, "text/event-stream")], body).into_response()
}

/// Starts the mock `OpenAI` API, and returns its base URL and the requests it records.
async fn start_mock_openai() -> anyhow::Result<(String, Recorded)> {
    let recorded = Recorded::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
        .with_state(recorded.clone());
    let listener = tokio::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((format!("http://127.0.0.1:{port}/v1"), recorded))
}

/// Sends the question that asks for `calls` calls to model `gpt` on `api`, and returns the text
/// of the answer.
async fn ask(base_url: &str, api: Api, stream: bool, calls: usize) -> anyhow::Result<String> {
    let (path, request) = match api {
        Api::ChatCompletions => (
            "chat/completions",
            json!({
                "model": "gpt", "stream": stream,
                "messages": [{ "role": "user", "content": question(calls) }],
            }),
        ),
        Api::Responses => (
            "responses",
            json!({ "model": "gpt", "stream": stream, "input": question(calls) }),
        ),
    };
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/{path}"))
        .json(&request)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(status.is_success(), "{path} failed: {status} {body}");
    match (api, stream) {
        (Api::ChatCompletions, true) => streamed_text(&body),
        (Api::ChatCompletions, false) => {
            let body: Value = serde_json::from_str(&body)?;
            Ok(body["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or_default()
                .to_string())
        }
        (Api::Responses, true) => Ok(body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .filter(|event| event["type"] == "response.output_text.delta")
            .filter_map(|event| event["delta"].as_str().map(str::to_string))
            .collect()),
        (Api::Responses, false) => {
            let body: Value = serde_json::from_str(&body)?;
            Ok(body["output"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|item| item["type"] == "message")
                .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
                .filter_map(|content| content["text"].as_str().map(str::to_string))
                .collect())
        }
    }
}

/// Asks model `gpt` on `api` for `calls` calls in one response, and returns the tool results of
/// the request that carried them to the model.
async fn tool_results_for(
    api: Api,
    stream: bool,
    calls: usize,
) -> anyhow::Result<Vec<(String, String)>> {
    let (endpoint, recorded) = start_mock_openai().await?;
    let mut model = Model::new("openai:gpt-4o-mini", "gpt");
    model
        .params
        .insert("openai_api_key".to_string(), "test".into());
    model.params.insert("endpoint".to_string(), endpoint.into());
    model.params.insert("tools".to_string(), "sql".into());
    let base_url = start_runtime_with(model).await?;

    let answer = ask(&base_url, api, stream, calls).await?;
    assert_eq!(answer, ANSWER, "{api:?}, stream: {stream}");

    let requests = recorded.0.lock();
    let with_results: Vec<_> = requests
        .iter()
        .filter(|request| requested_calls(request).is_some())
        .map(|request| tool_results(api, request))
        .filter(|results| !results.is_empty())
        .collect();
    assert_eq!(
        with_results.len(),
        1,
        "{api:?}, stream: {stream}: one request carries the tool results"
    );
    Ok(with_results.into_iter().next().unwrap_or_default())
}

fn call_id(i: usize) -> String {
    format!("call_{i}")
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_calls_past_the_limit_in_one_response_do_not_run(
    #[values(Api::ChatCompletions, Api::Responses)] api: Api,
    #[values(false, true)] stream: bool,
) -> anyhow::Result<()> {
    const CALLS: usize = 1000;
    let results = tool_results_for(api, stream, CALLS).await?;

    // Every call gets a result, in the order the model made them: the first `LIMIT` ran, and the
    // model is told the rest did not.
    let expected: Vec<(String, String)> = (0..CALLS)
        .map(|i| {
            let result = if i < LIMIT {
                QUERY_RESULT.to_string()
            } else {
                not_run(CALLS)
            };
            (call_id(i), api.sent(&result))
        })
        .collect();
    assert_eq!(results, expected, "{api:?}, stream: {stream}");
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_tool_call_runs_at_the_limit(
    #[values(Api::ChatCompletions, Api::Responses)] api: Api,
    #[values(false, true)] stream: bool,
) -> anyhow::Result<()> {
    let results = tool_results_for(api, stream, LIMIT).await?;

    let expected: Vec<(String, String)> = (0..LIMIT)
        .map(|i| (call_id(i), api.sent(QUERY_RESULT)))
        .collect();
    assert_eq!(results, expected, "{api:?}, stream: {stream}");
    Ok(())
}
