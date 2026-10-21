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

//! Chat completions through Spice's tool loop against a real Anthropic model. Needs
//! `SPICE_ANTHROPIC_API_KEY`.

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use app::AppBuilder;
use arrow::util::display::array_value_to_string;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures::TryStreamExt;
use parking_lot::Mutex;
use runtime::{Runtime, auth::EndpointAuth};
use serde_json::{Value, json};

use crate::{
    init_tracing,
    models::{create_api_bindings_config, get_anthropic_model},
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};

/// Its minimum cacheable prompt is 1,024 tokens:
/// <https://platform.claude.com/docs/en/build-with-claude/prompt-caching>.
const MODEL: &str = "claude-sonnet-4-6";
const MIN_CACHEABLE_TOKENS: u64 = 1024;
const ANTHROPIC_MESSAGES_URL: &str = "https://api.anthropic.com/v1/messages";
const DIGEST_QUERY: &str = "SELECT md5('spice prompt caching') AS digest";
const PROMPT_CACHE_KEY: &str = "spice-integration-anthropic-prompt-caching";
/// The text the model is told to write before its tool call.
const PREAMBLE: &str = "Running the query.";

/// A system prompt several times longer than [`MIN_CACHEABLE_TOKENS`], so that what the model
/// reads before the conversation is cacheable on its own.
fn long_system_prompt() -> String {
    let mut prompt =
        String::from("You answer questions about a warehouse catalog. Its entries follow.\n");
    for i in 1..=300 {
        writeln!(
            prompt,
            "Entry {i}: bin {i} holds {} units of part P-{i:04}.",
            i * 7
        )
        .expect("writing to a String does not fail");
    }
    prompt
}

/// One request Spice sent to Anthropic, and Anthropic's response.
struct Exchange {
    request: Value,
    response: Value,
}

/// The exchanges a [`start_recording_proxy`] has forwarded, in order.
#[derive(Clone, Default)]
struct Recorded(Arc<Mutex<Vec<Exchange>>>);

impl Recorded {
    fn take(&self) -> Vec<Exchange> {
        std::mem::take(&mut *self.0.lock())
    }
}

/// Forwards a Messages API request to Anthropic with its credential and version headers, and
/// records it with the response.
async fn forward(State(recorded): State<Recorded>, headers: HeaderMap, body: Bytes) -> Response {
    let mut upstream = reqwest::Client::new()
        .post(ANTHROPIC_MESSAGES_URL)
        .body(body.clone());
    for (name, value) in &headers {
        let name = name.as_str();
        if name == "x-api-key"
            || name == "authorization"
            || name == "content-type"
            || name.starts_with("anthropic-")
        {
            upstream = upstream.header(name, value);
        }
    }
    let (status, response) = match upstream.send().await {
        Ok(response) => (
            response.status().as_u16(),
            response.bytes().await.unwrap_or_default(),
        ),
        Err(e) => return (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    };
    if let (Ok(request), Ok(parsed)) = (
        serde_json::from_slice::<Value>(&body),
        serde_json::from_slice::<Value>(&response),
    ) {
        recorded.0.lock().push(Exchange {
            request,
            response: parsed,
        });
    }
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        [(CONTENT_TYPE, "application/json")],
        response,
    )
        .into_response()
}

/// Starts a proxy in front of the Anthropic Messages API, and returns the `endpoint` that routes a
/// model through it and the exchanges it records.
async fn start_recording_proxy() -> anyhow::Result<(String, Recorded)> {
    let recorded = Recorded::default();
    let app = Router::new()
        .route("/v1/messages", post(forward))
        .with_state(recorded.clone());
    let listener =
        tokio::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((format!("http://127.0.0.1:{port}/v1"), recorded))
}

/// The result of `query`, a single value, as text.
async fn query_single_value(rt: &Arc<Runtime>, query: &str) -> anyhow::Result<String> {
    let batches = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await?
        .data
        .try_collect::<Vec<_>>()
        .await?;
    anyhow::ensure!(
        batches.len() == 1 && batches[0].num_rows() == 1,
        "expected one row from {query}"
    );
    Ok(array_value_to_string(batches[0].column(0), 0)?)
}

/// A non-streamed chat completion that has the model write [`PREAMBLE`], call the `sql` tool once,
/// then answer.
async fn chat(base_url: &str, prompt_cache_key: Option<&str>) -> anyhow::Result<Value> {
    let mut body = json!({
        "model": "claude",
        "messages": [
            { "role": "system", "content": long_system_prompt() },
            {
                "role": "user",
                "content": format!(
                    "Before calling any tool, write this sentence and nothing else: {PREAMBLE}\n\
                     Then use the sql tool to run exactly `{DIGEST_QUERY}`. Once you have its \
                     result, reply with only the digest it returns."
                ),
            },
        ],
    });
    if let Some(key) = prompt_cache_key {
        body["prompt_cache_key"] = json!(key);
    }
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/chat/completions"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    anyhow::ensure!(
        status.is_success(),
        "chat completion failed: {status} {text}"
    );
    Ok(serde_json::from_str(&text)?)
}

/// Each content block as its type with its text, or its id, name and input.
fn blocks(content: &Value) -> Vec<Value> {
    content
        .as_array()
        .into_iter()
        .flatten()
        .map(|block| match block["type"].as_str() {
            Some("text") => json!({ "type": "text", "text": block["text"] }),
            _ => json!({
                "type": block["type"], "id": block["id"], "name": block["name"],
                "input": block["input"],
            }),
        })
        .collect()
}

/// Checks one chat completion's exchanges with Anthropic, and returns each round's
/// `cache_read_input_tokens`.
///
/// The model's first reply is [`PREAMBLE`] and a `sql` call. Spice sends that reply back to the
/// model in the next round exactly as the model wrote it, text included, and the client's answer
/// starts with that text and carries the digest only the tool can produce.
fn check_round_trip(name: &str, exchanges: &[Exchange], answer: &Value, digest: &str) -> Vec<u64> {
    assert_eq!(exchanges.len(), 2, "{name}: one tool call, then the answer");
    let reply = blocks(&exchanges[0].response["content"]);
    assert!(
        reply
            .first()
            .and_then(|block| block["text"].as_str())
            .is_some_and(|text| text.starts_with(PREAMBLE))
            && reply
                .iter()
                .any(|block| block["type"] == "tool_use" && block["name"] == "sql"),
        "{name}: the model wrote {PREAMBLE:?} beside a sql call: {reply:?}"
    );
    let messages = exchanges[1].request["messages"]
        .as_array()
        .expect("a request carries messages");
    let replayed = &messages[messages.len() - 2];
    assert_eq!(replayed["role"], "assistant", "{name}");
    assert_eq!(
        blocks(&replayed["content"]),
        reply,
        "{name}: the second round replays the model's turn as it wrote it"
    );
    let content = answer["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or_default();
    assert!(
        content.starts_with(PREAMBLE) && content.contains(digest),
        "{name}: the answer starts with {PREAMBLE:?} and carries {digest}: {answer}"
    );
    exchanges
        .iter()
        .map(|exchange| {
            exchange.response["usage"]["cache_read_input_tokens"]
                .as_u64()
                .unwrap_or_default()
        })
        .collect()
}

/// Prompt caching keeps working when Spice replays the text a model wrote beside its tool call.
///
/// A proxy between Spice and Anthropic records every round. In each request the model writes
/// [`PREAMBLE`] beside a `sql` call, and the next round replays that turn as written. With a
/// `prompt_cache_key`, the repeated request reads from Anthropic's cache in both rounds,
/// including the round that carries the replayed text. Without one, Spice asks for no caching and
/// nothing is read.
#[tokio::test]
async fn anthropic_prompt_caching_through_tool_calls() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_ANTHROPIC_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let (endpoint, recorded) = start_recording_proxy().await?;
            let mut model = get_anthropic_model(MODEL, "claude");
            model.params.insert("endpoint".to_string(), endpoint.into());
            model.params.insert("tools".to_string(), "sql".into());
            let app = AppBuilder::new("anthropic_prompt_caching")
                .with_model(model)
                .build();

            let api_config = create_api_bindings_config();
            let base_url = format!("http://{}", api_config.http_bind_address);
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

            let digest = query_single_value(&rt, DIGEST_QUERY).await?;
            recorded.take();

            let mut cache_reads = Vec::new();
            for (name, key) in [
                ("uncached", None),
                ("warm", Some(PROMPT_CACHE_KEY)),
                ("cached", Some(PROMPT_CACHE_KEY)),
            ] {
                let answer = chat(&base_url, key).await?;
                let exchanges = recorded.take();
                let reads = check_round_trip(name, &exchanges, &answer, &digest);
                eprintln!("{name}: cache_read_input_tokens per round = {reads:?}");
                let cache_control = key.map_or(Value::Null, |_| json!({ "type": "ephemeral" }));
                for exchange in &exchanges {
                    assert_eq!(exchange.request["cache_control"], cache_control, "{name}");
                }
                cache_reads.push(reads);
            }

            assert_eq!(
                cache_reads[0],
                [0, 0],
                "no prompt_cache_key, no cache reads"
            );
            assert!(
                cache_reads[2]
                    .iter()
                    .all(|&read| read >= MIN_CACHEABLE_TOKENS),
                "the repeated request reads from the cache in both rounds, the replayed one \
                 included: {:?}",
                cache_reads[2]
            );
            Ok(())
        })
        .await
}
