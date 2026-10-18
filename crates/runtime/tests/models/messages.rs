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

//! `/v1/messages` against the real providers: an Anthropic model, which receives each request
//! as sent, and an `OpenAI` model, which receives it as a chat completion.

use std::sync::Arc;

use app::AppBuilder;
use reqwest::Client;
use runtime::{Runtime, auth::EndpointAuth};
use serde_json::{Value, json};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;

use crate::{
    init_tracing,
    models::{create_api_bindings_config, get_anthropic_model, openai::get_openai_model},
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};

const CLAUDE: &str = "claude-haiku-4-5-20251001";

const ORDERS_CSV: &str = "order_id,region,quantity,unit_price
1,north,3,9.50
2,south,1,24.00
3,north,2,24.00
4,south,4,9.50
";

/// Starts a runtime with `app` and returns its HTTP base URL.
async fn start(app: app::App) -> Result<String, anyhow::Error> {
    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(http_base_url)
}

/// Sends `request` to `path`, and returns the status and the message: the JSON body, or the
/// message a streamed answer's events build, with the events.
async fn post(
    base_url: &str,
    path: &str,
    request: &Value,
) -> Result<(u16, Value, Vec<Value>), anyhow::Error> {
    let response = Client::new()
        .post(format!("{base_url}{path}"))
        .header("anthropic-version", "2023-06-01")
        .json(request)
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    if request["stream"] != json!(true) || status != 200 {
        return Ok((status, serde_json::from_str(&body)?, Vec::new()));
    }
    let events: Vec<Value> = body
        .split("\n\n")
        .filter_map(|event| event.lines().find_map(|line| line.strip_prefix("data: ")))
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    Ok((status, accumulate(&events), events))
}

/// The message a client builds from `events`, as the Anthropic SDKs accumulate a stream.
fn accumulate(events: &[Value]) -> Value {
    let mut message = Value::Null;
    let mut partial: std::collections::BTreeMap<usize, String> = std::collections::BTreeMap::new();
    for event in events {
        let index = event["index"].as_u64().and_then(|i| usize::try_from(i).ok());
        match event["type"].as_str().unwrap_or_default() {
            "message_start" => message = event["message"].clone(),
            "content_block_start" => {
                if let Some(content) = message["content"].as_array_mut() {
                    content.push(event["content_block"].clone());
                }
            }
            "content_block_delta" => {
                let Some(index) = index else { continue };
                let delta = &event["delta"];
                let block = &mut message["content"][index];
                match delta["type"].as_str().unwrap_or_default() {
                    "text_delta" => {
                        let text = format!("{}{}", block["text"].as_str().unwrap_or_default(), delta["text"].as_str().unwrap_or_default());
                        block["text"] = json!(text);
                    }
                    "thinking_delta" => {
                        let thinking = format!("{}{}", block["thinking"].as_str().unwrap_or_default(), delta["thinking"].as_str().unwrap_or_default());
                        block["thinking"] = json!(thinking);
                    }
                    "signature_delta" => block["signature"] = delta["signature"].clone(),
                    "input_json_delta" => partial
                        .entry(index)
                        .or_default()
                        .push_str(delta["partial_json"].as_str().unwrap_or_default()),
                    _ => {}
                }
            }
            "content_block_stop" => {
                if let Some(json) = index.and_then(|index| partial.remove(&index))
                    && let Ok(input) = serde_json::from_str::<Value>(&json)
                    && let Some(index) = index
                {
                    message["content"][index]["input"] = input;
                }
            }
            "message_delta" => {
                message["stop_reason"] = event["delta"]["stop_reason"].clone();
                message["usage"] = event["usage"].clone();
            }
            _ => {}
        }
    }
    message
}

fn text_of(message: &Value) -> String {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "text")
        .filter_map(|block| block["text"].as_str())
        .collect()
}

fn blocks_of<'a>(message: &'a Value, block_type: &str) -> Vec<&'a Value> {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == block_type)
        .collect()
}

#[tokio::test]
async fn messages_on_an_anthropic_model_reach_anthropic_as_sent() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_ANTHROPIC_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            let base_url = start(
                AppBuilder::new("messages_native")
                    .with_model(get_anthropic_model(CLAUDE, "claude"))
                    .build(),
            )
            .await?;

            for stream in [false, true] {
                // Extended thinking returns signed thinking blocks, which only Anthropic writes.
                let (status, message, events) = post(
                    &base_url,
                    "/v1/messages",
                    &json!({
                        "model": "claude",
                        "max_tokens": 2048,
                        "stream": stream,
                        "thinking": {"type": "enabled", "budget_tokens": 1024},
                        "messages": [{"role": "user", "content": "What is 17 * 23? Answer with the number only."}],
                    }),
                )
                .await?;
                assert_eq!(status, 200, "stream: {stream}: {message:#}");
                assert_eq!(message["model"], "claude", "stream: {stream}: {message:#}");
                assert_eq!(message["stop_reason"], "end_turn", "stream: {stream}: {message:#}");
                assert!(text_of(&message).contains("391"), "stream: {stream}: {message:#}");
                let thinking = blocks_of(&message, "thinking");
                assert!(
                    !thinking.is_empty()
                        && thinking
                            .iter()
                            .all(|block| block["signature"].as_str().is_some_and(|s| !s.is_empty())),
                    "stream: {stream}: {message:#}"
                );
                if stream {
                    assert_eq!(events.first().map(|e| &e["type"]), Some(&json!("message_start")));
                    assert_eq!(events.last().map(|e| &e["type"]), Some(&json!("message_stop")));
                }
            }

            let (status, counted, _) = post(
                &base_url,
                "/v1/messages/count_tokens",
                &json!({"model": "claude", "messages": [{"role": "user", "content": "Hello, Claude"}]}),
            )
            .await?;
            assert_eq!(status, 200, "{counted:#}");
            assert!(counted["input_tokens"].as_u64().is_some_and(|n| n > 0), "{counted:#}");
            Ok(())
        })
        .await
}

/// A client that keeps its own history sends a message's content back. Spice's tool calls in it
/// must give the model, on the next turn, the calls and results it saw: Anthropic accepts the
/// request, and the model answers from them.
#[tokio::test]
async fn messages_on_an_anthropic_model_run_spice_tools_and_replay_them() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_ANTHROPIC_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            let data_dir = tempfile::tempdir()?;
            let orders_path = data_dir.path().join("orders.csv");
            std::fs::write(&orders_path, ORDERS_CSV)?;
            let mut orders = Dataset::new(format!("file:{}", orders_path.display()), "orders");
            orders.params = Some(Params::from_string_map(
                [("file_format".to_string(), "csv".to_string())].into(),
            ));
            let mut claude = get_anthropic_model(CLAUDE, "claude");
            claude.params.insert("tools".into(), Value::String("sql".into()));
            let base_url = start(
                AppBuilder::new("messages_spice_tools")
                    .with_dataset(orders)
                    .with_model(claude)
                    .build(),
            )
            .await?;

            let question = json!({
                "role": "user",
                "content": "Using the `orders` table (columns: region, quantity, unit_price), what is the total revenue, quantity * unit_price, of the north region? Answer with the number.",
            });
            for stream in [false, true] {
                let (status, turn_1, _) = post(
                    &base_url,
                    "/v1/messages",
                    &json!({
                        "model": "claude",
                        "max_tokens": 1024,
                        "stream": stream,
                        "tool_choice": {"type": "tool", "name": "sql"},
                        "messages": [question],
                    }),
                )
                .await?;
                assert_eq!(status, 200, "stream: {stream}: {turn_1:#}");
                let calls = blocks_of(&turn_1, "mcp_tool_use");
                let results = blocks_of(&turn_1, "mcp_tool_result");
                assert!(
                    !calls.is_empty()
                        && calls.iter().all(|call| call["server_name"] == "spice" && call["name"] == "sql"),
                    "stream: {stream}: {turn_1:#}"
                );
                assert_eq!(calls.len(), results.len(), "stream: {stream}: {turn_1:#}");
                assert!(blocks_of(&turn_1, "tool_use").is_empty(), "stream: {stream}: {turn_1:#}");
                // north: 3 * 9.50 + 2 * 24.00 = 76.50
                assert!(text_of(&turn_1).contains("76.5"), "stream: {stream}: {turn_1:#}");
                assert_eq!(turn_1["stop_reason"], "end_turn", "stream: {stream}: {turn_1:#}");

                let (status, turn_2, _) = post(
                    &base_url,
                    "/v1/messages",
                    &json!({
                        "model": "claude",
                        "max_tokens": 1024,
                        "stream": stream,
                        "messages": [
                            question,
                            {"role": "assistant", "content": turn_1["content"]},
                            {"role": "user", "content": "What exact SQL query did you run? Quote it verbatim."},
                        ],
                    }),
                )
                .await?;
                assert_eq!(status, 200, "stream: {stream}: {turn_2:#}");
                let quoted = text_of(&turn_2).to_lowercase();
                assert!(
                    quoted.contains("select") && quoted.contains("orders"),
                    "stream: {stream}: {turn_2:#}"
                );
            }
            Ok(())
        })
        .await
}

#[tokio::test]
async fn messages_on_an_openai_model_are_translated_to_chat_completions() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;
            let base_url = start(
                AppBuilder::new("messages_translated")
                    .with_model(get_openai_model("gpt-4o-mini", "gpt"))
                    .build(),
            )
            .await?;

            let weather = json!({
                "name": "get_weather",
                "description": "Gets the current weather of a city.",
                "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
            });
            let question = json!({"role": "user", "content": "What is the weather in Paris? Use the tool."});
            for stream in [false, true] {
                let (status, call, _) = post(
                    &base_url,
                    "/v1/messages",
                    &json!({"model": "gpt", "max_tokens": 256, "stream": stream, "tools": [weather], "messages": [question]}),
                )
                .await?;
                assert_eq!(status, 200, "stream: {stream}: {call:#}");
                let uses = blocks_of(&call, "tool_use");
                assert_eq!(call["stop_reason"], "tool_use", "stream: {stream}: {call:#}");
                assert!(
                    uses.len() == 1
                        && uses[0]["name"] == "get_weather"
                        && uses[0]["input"]["city"].as_str().is_some_and(|c| c.to_lowercase().contains("paris")),
                    "stream: {stream}: {call:#}"
                );

                let (status, answer, _) = post(
                    &base_url,
                    "/v1/messages",
                    &json!({
                        "model": "gpt",
                        "max_tokens": 256,
                        "stream": stream,
                        "tools": [weather],
                        "messages": [
                            question,
                            {"role": "assistant", "content": call["content"]},
                            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": uses[0]["id"], "content": "It is 18 degrees Celsius and sunny."}]},
                        ],
                    }),
                )
                .await?;
                assert_eq!(status, 200, "stream: {stream}: {answer:#}");
                assert_eq!(answer["stop_reason"], "end_turn", "stream: {stream}: {answer:#}");
                assert!(text_of(&answer).contains("18"), "stream: {stream}: {answer:#}");
                assert!(
                    answer["usage"]["input_tokens"].as_u64().is_some_and(|n| n > 0)
                        && answer["usage"]["output_tokens"].as_u64().is_some_and(|n| n > 0),
                    "stream: {stream}: {answer:#}"
                );
            }

            let (status, refused, _) = post(
                &base_url,
                "/v1/messages",
                &json!({"model": "gpt", "max_tokens": 16, "top_k": 5, "messages": [question]}),
            )
            .await?;
            assert_eq!(
                (status, &refused["error"]["type"]),
                (400, &json!("invalid_request_error")),
                "{refused:#}"
            );
            Ok(())
        })
        .await
}
