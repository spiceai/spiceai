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

//! End-to-end tests of the tools that Spice runs for a model, for each provider and both APIs.
//!
//! Spice runs its own tools (here `sql`) inside one client request. It sends the request to the
//! provider, runs the tool calls that the model makes, and then sends the provider a new request
//! with the calls and their results. The client gets only the last answer. Each test has this
//! conversation with Spice:
//!
//! ```text
//!  turn 1   client sends A           Spice sends the provider:
//!                                      round 1: A               -> the model calls `sql` (B)
//!                                      Spice runs B             -> result C
//!                                      round 2: A, B, C         -> answer D
//!           client gets D
//!  turn 2   client sends A, D, E       round 3: A, B, C, D, E   -> answer F
//! ```
//!
//! The tests check two things that the client cannot see:
//!
//! - `*_send_earlier_tool_calls`: round 3 holds B and C, the steps that Spice ran in turn 1, at
//!   the place where the model saw them.
//! - `*_reuse_the_prompt_cache`: each round starts with all of the round before it, so the
//!   provider reads that part from its prompt cache. From one round to the next, Spice must not
//!   change the tools, the system prompt, or an earlier message.
//!
//! The request that Spice sent the provider in each round, and the provider's usage for it, come
//! from `runtime.task_history`: one task for each round.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use app::AppBuilder;
use arrow::array::{Array, StringArray};
use async_openai::Client as OpenAIClient;
use async_openai::config::OpenAIConfig;
use async_openai::types::chat::CreateChatCompletionRequest;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use opentelemetry_sdk::trace::SdkTracerProvider;
use rstest::rstest;
use runtime::{Runtime, auth::EndpointAuth};
use serde_json::{Value, json};
use spicepod::component::runtime::TaskHistoryCapturedContext;
use spicepod::component::{dataset::Dataset, model::Model};
use spicepod::param::Params;
use tracing::subscriber::DefaultGuard;

use crate::models::openai::{create_response, extract_text, get_openai_model, normalized_sql};
use crate::models::{create_api_bindings_config, get_anthropic_model, get_xai_model};
use crate::utils::{
    init_tracing_with_task_history_captured_context, runtime_ready_check, test_request_context,
    verify_env_secret_exists,
};
use crate::{DEFAULT_TRACING_MODELS, init_tracing};

/// The name of the model in the Spicepod.
const MODEL_NAME: &str = "tool_model";

/// The rows of the `orders` dataset. With this many, the result of `sql` (C) is the largest part
/// of the conversation, so a round that loses the cache before C loses thousands of tokens.
const ORDER_ROWS: usize = 200;

/// The question of turn 1 (A). It makes the model call `sql`, and keeps the answer (D) short.
const QUESTION: &str = "Use the sql tool to run exactly this query: \
    SELECT * FROM orders ORDER BY order_id. \
    Then reply with only the number of rows that it returned.";

/// The question of turn 2 (E). Only the `sql` call of turn 1 holds the answer.
const FOLLOW_UP: &str = "What exact SQL query did you run? Quote it verbatim.";

/// The most prompt tokens of a round that the next round can fail to read from the cache when
/// Spice keeps the round whole. `OpenAI` and xAI cache a prompt in blocks of 128 tokens. Measured
/// on 2026-10-15, `OpenAI` read up to 305 tokens fewer than a request had, from a later request that
/// repeats all of it. A round that loses the cache before the `sql` result loses thousands.
const CACHE_SLACK_TOKENS: u64 = 512;

/// The provider of the model that Spice runs its tools for.
#[derive(Clone, Copy, Debug)]
enum Provider {
    OpenAi,
    /// A reasoning model of `OpenAI`. On the Responses API, it gives each tool call a reasoning
    /// item, which Spice replays with the call.
    OpenAiReasoning,
    Anthropic,
    Xai,
}

impl Provider {
    /// How many conversations the cache test can have to get one in which each round reads the
    /// round before it from the cache.
    ///
    /// xAI's cache misses a whole conversation at times. Measured on 2026-10-15, a round read the
    /// round before it from xAI's cache in 15 of 19 conversations through Spice, and about as often
    /// when the same requests went to xAI directly.
    fn cache_attempts(self) -> usize {
        match self {
            Self::Xai => 3,
            Self::OpenAi | Self::OpenAiReasoning | Self::Anthropic => 1,
        }
    }

    /// The secret that holds the provider's API key.
    fn api_key_secret(self) -> &'static str {
        match self {
            Self::OpenAi | Self::OpenAiReasoning => "SPICE_OPENAI_API_KEY",
            Self::Anthropic => "SPICE_ANTHROPIC_API_KEY",
            Self::Xai => "SPICE_XAI_API_KEY",
        }
    }

    /// The Spicepod model: the provider's model, with Spice's tools and `system_prompt`.
    fn model(self, system_prompt: String) -> Model {
        let mut model = match self {
            Self::OpenAi => get_openai_model("gpt-4o-mini", MODEL_NAME),
            Self::OpenAiReasoning => get_openai_model("gpt-5-mini", MODEL_NAME),
            Self::Anthropic => get_anthropic_model("claude-haiku-4-5-20251001", MODEL_NAME),
            Self::Xai => get_xai_model("grok-4.20-non-reasoning", MODEL_NAME),
        };
        // `auto` includes `list_datasets`, which Spice also calls itself to start each Chat
        // Completions request.
        model.params.insert("tools".to_string(), "auto".into());
        model
            .params
            .insert("system_prompt".to_string(), system_prompt.into());
        model
    }
}

/// The API that the client uses.
#[derive(Clone, Copy, Debug)]
enum Api {
    ChatCompletions,
    Responses,
}

impl Api {
    /// The task in `runtime.task_history` of each request that Spice sends the provider.
    fn round_task(self) -> &'static str {
        match self {
            Self::ChatCompletions => "ai_completion",
            Self::Responses => "responses",
        }
    }

    /// Sends `history`, the conversation so far, to Spice. Returns the text of the answer, and the
    /// answer as the items that the client adds to its history.
    async fn send(
        self,
        client: &OpenAIClient<OpenAIConfig>,
        history: &[Value],
        prompt_cache_key: &str,
        stream: bool,
    ) -> Result<(String, Vec<Value>), anyhow::Error> {
        match self {
            Self::ChatCompletions => {
                let request: CreateChatCompletionRequest = serde_json::from_value(json!({
                    "model": MODEL_NAME,
                    "messages": history,
                    "prompt_cache_key": prompt_cache_key,
                    "stream": stream,
                }))?;
                let text = if stream {
                    let mut chunks = client.chat().create_stream(request).await?;
                    let mut text = String::new();
                    while let Some(chunk) = chunks.next().await {
                        for choice in chunk?.choices {
                            text.extend(choice.delta.content);
                        }
                    }
                    text
                } else {
                    let response = client.chat().create(request).await?;
                    response
                        .choices
                        .into_iter()
                        .next()
                        .and_then(|choice| choice.message.content)
                        .unwrap_or_default()
                };
                let answer = json!({ "role": "assistant", "content": text });
                Ok((text, vec![answer]))
            }
            Self::Responses => {
                let (response, _) = create_response(
                    client,
                    json!({
                        "model": MODEL_NAME,
                        "input": history,
                        "prompt_cache_key": prompt_cache_key,
                        "stream": stream,
                    }),
                )
                .await?;
                let items = serde_json::to_value(&response.output)?
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                Ok((extract_text(&response).unwrap_or_default(), items))
            }
        }
    }
}

/// One request that Spice sent the provider, and the provider's usage for it.
#[derive(Debug)]
struct Round {
    request: Value,
    /// The tokens of the request.
    prompt_tokens: u64,
    /// The tokens of the request that the provider read from its prompt cache.
    cached_tokens: u64,
}

/// One turn: the text of the answer that the client got, and the requests that Spice sent the
/// provider for it.
struct Turn {
    answer: String,
    rounds: Vec<Round>,
}

/// The conversation of the module documentation, with the model of one provider, on one API.
struct Conversation {
    /// Names the provider, API and mode in a failure.
    label: String,
    api: Api,
    turns: [Turn; 2],
}

impl Conversation {
    /// Starts Spice with `provider`'s model and has the conversation on `api`.
    async fn run(provider: Provider, api: Api, stream: bool) -> Result<Self, anyhow::Error> {
        verify_env_secret_exists(provider.api_key_secret())
            .await
            .map_err(anyhow::Error::msg)?;

        let data_dir = tempfile::tempdir()?;
        let spice = Spice::start(
            orders_dataset(data_dir.path())?,
            provider.model(system_prompt()),
        )
        .await?;

        // The key routes each round to the same cache, and is what turns Anthropic's prompt
        // caching on. Spice sends it to xAI as `x-grok-conv-id`.
        let prompt_cache_key = format!("spice-tool-turns-{:016x}", rand::random::<u64>());
        let question = json!({ "role": "user", "content": QUESTION });

        let start = SystemTime::now();
        let (answer, answer_items) = api
            .send(
                &spice.client,
                std::slice::from_ref(&question),
                &prompt_cache_key,
                stream,
            )
            .await?;
        let turn_1 = Turn {
            answer,
            rounds: spice.rounds(api, start).await?,
        };

        let mut history = vec![question];
        history.extend(answer_items);
        history.push(json!({ "role": "user", "content": FOLLOW_UP }));
        let start = SystemTime::now();
        let (answer, _) = api
            .send(&spice.client, &history, &prompt_cache_key, stream)
            .await?;
        let turn_2 = Turn {
            answer,
            rounds: spice.rounds(api, start).await?,
        };

        Ok(Self {
            label: format!("{provider:?} on {api:?} (stream: {stream})"),
            api,
            turns: [turn_1, turn_2],
        })
    }

    /// Every round of the conversation, in order.
    fn rounds(&self) -> impl Iterator<Item = &Round> {
        self.turns.iter().flat_map(|turn| &turn.rounds)
    }

    /// The query of the `sql` call that Spice ran in turn 1.
    fn turn_1_query(&self) -> String {
        let label = &self.label;
        let [turn_1, _] = &self.turns;
        assert!(
            turn_1.rounds.len() >= 2,
            "{label}: Spice ran no tool in turn 1, so the conversation tests nothing: {:#?}",
            turn_1.rounds
        );
        let last = turn_1.rounds.last().map(|round| &round.request);
        let query = last.and_then(|request| sql_call_query(self.api, request));
        query.unwrap_or_else(|| {
            panic!("{label}: no `sql` call with its result in turn 1: {last:#?}")
        })
    }

    /// Asserts that Spice sent the provider the tool calls of turn 1 again in turn 2, where the
    /// model saw them, and that the model answered from them.
    fn assert_turn_2_has_the_tool_calls_of_turn_1(&self) {
        let label = &self.label;
        let query = self.turn_1_query();
        let [turn_1, turn_2] = &self.turns;

        // One round: the model answered from the history instead of running `sql` again.
        let [round_3] = turn_2.rounds.as_slice() else {
            panic!(
                "{label}: turn 2 sent the provider other than one request: {:#?}",
                turn_2.rounds
            );
        };
        if let Some(round_2) = turn_1.rounds.last() {
            assert_starts_with(label, self.api, round_2, round_3);
        }

        assert!(
            normalized_sql(&turn_2.answer).contains(&normalized_sql(&query)),
            "{label}: the answer of turn 2 does not quote {query:?}: {}",
            turn_2.answer
        );
    }

    /// Asserts that each round starts with all of the round before it.
    fn assert_each_round_starts_with_the_last(&self) {
        // The conversation must include a tool round, or there is no round to compare.
        self.turn_1_query();

        let rounds = self.rounds().collect::<Vec<_>>();
        for (before, after) in rounds.iter().zip(rounds.iter().skip(1)) {
            assert_starts_with(&self.label, self.api, before, after);
        }
    }

    /// Whether the provider read each round after the first from its cache, up to the end of the
    /// round before it. Each round, as `(prompt_tokens, cached_tokens)`, when it did not.
    fn cache_misses(&self) -> Result<(), Vec<(u64, u64)>> {
        let rounds = self.rounds().collect::<Vec<_>>();
        let missed = rounds
            .iter()
            .zip(rounds.iter().skip(1))
            .any(|(before, after)| after.cached_tokens + CACHE_SLACK_TOKENS < before.prompt_tokens);
        if missed {
            Err(rounds
                .iter()
                .map(|round| (round.prompt_tokens, round.cached_tokens))
                .collect())
        } else {
            Ok(())
        }
    }
}

/// Has the conversation with `provider`'s model on `api` until the provider reads each round
/// after the first from its cache, up to the end of the round before it. Each conversation must
/// keep each round whole in the next.
async fn assert_reuses_the_prompt_cache(
    provider: Provider,
    api: Api,
    stream: bool,
) -> Result<(), anyhow::Error> {
    let mut misses = Vec::new();
    for _ in 0..provider.cache_attempts() {
        let conversation = Conversation::run(provider, api, stream).await?;
        conversation.assert_each_round_starts_with_the_last();
        match conversation.cache_misses() {
            Ok(()) => return Ok(()),
            Err(rounds) => misses.push(rounds),
        }
    }
    panic!(
        "{provider:?} on {api:?} (stream: {stream}): in each conversation, the provider did not \
         read a request from its cache up to the end of the request before it, all of which the \
         request repeats. Each round as (prompt tokens, cached tokens): {misses:?}"
    )
}

/// Spice, running the `orders` dataset and one model, and serving HTTP to `client`.
struct Spice {
    rt: Arc<Runtime>,
    trace_provider: SdkTracerProvider,
    client: OpenAIClient<OpenAIConfig>,
    _tracing: DefaultGuard,
}

impl Spice {
    async fn start(dataset: Dataset, model: Model) -> Result<Self, anyhow::Error> {
        let app = AppBuilder::new("spice_tool_turns")
            .with_dataset(dataset)
            .with_model(model)
            .build();

        let api_config = create_api_bindings_config();
        let http_base_url = format!("http://{}", api_config.http_bind_address);
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        // `Full`: the request of each round, whole.
        let (tracing, trace_provider) = init_tracing_with_task_history_captured_context(
            DEFAULT_TRACING_MODELS,
            &rt,
            TaskHistoryCapturedContext::Full,
        );

        let rt_ref_copy = Arc::clone(&rt);
        tokio::spawn(async move {
            Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
        });

        tokio::select! {
            () = tokio::time::sleep(Duration::from_mins(1)) => {
                return Err(anyhow::anyhow!("Timed out waiting for components to load"));
            }
            () = Arc::clone(&rt).load_components() => {}
        }
        runtime_ready_check(&rt).await;

        let client = OpenAIClient::with_config(
            OpenAIConfig::default().with_api_base(format!("{http_base_url}/v1")),
        );
        Ok(Self {
            rt,
            trace_provider,
            client,
            _tracing: tracing,
        })
    }

    /// The requests that Spice sent the provider since `since`, in order.
    ///
    /// A round's task is written when the round ends, which for the last round of a streamed turn
    /// can be after the client has the whole answer. So this waits until two reads in a row agree.
    async fn rounds(&self, api: Api, since: SystemTime) -> Result<Vec<Round>, anyhow::Error> {
        let mut last_read = None;
        for _ in 0..30 {
            let _ = self.trace_provider.force_flush();
            let read = self.read_rounds(api, since).await?;
            if !read.is_empty() && last_read.as_ref() == Some(&read) {
                return read
                    .into_iter()
                    .map(|(request, prompt_tokens, cached_tokens)| {
                        Ok(Round {
                            request: serde_json::from_str(&request)?,
                            prompt_tokens: token_label(prompt_tokens.as_deref())?,
                            cached_tokens: token_label(cached_tokens.as_deref())?,
                        })
                    })
                    .collect();
            }
            last_read = Some(read);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(anyhow::anyhow!(
            "the `{}` tasks since {since:?} did not settle: {last_read:#?}",
            api.round_task()
        ))
    }

    /// Each round's request, and its `prompt_tokens` and `cached_tokens` labels.
    async fn read_rounds(
        &self,
        api: Api,
        since: SystemTime,
    ) -> Result<Vec<(String, Option<String>, Option<String>)>, anyhow::Error> {
        let since = DateTime::<Utc>::from(since).to_rfc3339();
        let batches = self
            .rt
            .datafusion()
            .query_builder(&format!(
                "SELECT input, labels['prompt_tokens'], labels['cached_tokens'] \
                 FROM runtime.task_history \
                 WHERE task = '{}' AND start_time >= '{since}' ORDER BY start_time",
                api.round_task()
            ))
            .build()
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .data
            .try_collect::<Vec<_>>()
            .await?;

        let mut rounds = Vec::new();
        for batch in &batches {
            let [input, prompt_tokens, cached_tokens] = [0, 1, 2]
                .map(|i| arrow::compute::cast(batch.column(i), &arrow::datatypes::DataType::Utf8));
            let (input, prompt_tokens, cached_tokens) = (input?, prompt_tokens?, cached_tokens?);
            let [input, prompt_tokens, cached_tokens] = [&input, &prompt_tokens, &cached_tokens]
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .context("a task history column is not a string column")
                });
            let (input, prompt_tokens, cached_tokens) = (input?, prompt_tokens?, cached_tokens?);
            for row in 0..batch.num_rows() {
                rounds.push((
                    input.value(row).to_string(),
                    prompt_tokens
                        .is_valid(row)
                        .then(|| prompt_tokens.value(row).to_string()),
                    cached_tokens
                        .is_valid(row)
                        .then(|| cached_tokens.value(row).to_string()),
                ));
            }
        }
        Ok(rounds)
    }
}

/// A token count from a task's labels, which Spice records for each request to a provider.
fn token_label(label: Option<&str>) -> Result<u64, anyhow::Error> {
    label
        .context("a provider request has no token count in its task's labels")?
        .parse()
        .context("a token count label is not a number")
}

/// The `orders` dataset: a CSV file in `dir` with [`ORDER_ROWS`] rows.
fn orders_dataset(dir: &Path) -> Result<Dataset, anyhow::Error> {
    const REGIONS: [&str; 4] = ["north", "south", "east", "west"];
    const PRODUCTS: [(&str, &str); 3] =
        [("widget", "9.50"), ("gadget", "24.00"), ("gizmo", "120.00")];

    let rows = (1..=ORDER_ROWS).map(|id| {
        let (product, unit_price) = PRODUCTS[id % PRODUCTS.len()];
        let region = REGIONS[id % REGIONS.len()];
        format!("{id},{region},{product},{},{unit_price}", id % 7 + 1)
    });
    let csv = std::iter::once("order_id,region,product,quantity,unit_price".to_string())
        .chain(rows)
        .collect::<Vec<_>>()
        .join("\n");
    let path = dir.join("orders.csv");
    std::fs::write(&path, csv)?;

    let mut orders = Dataset::new(format!("file:{}", path.display()), "orders");
    orders.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())].into(),
    ));
    Ok(orders)
}

/// A system prompt that is unique to this run, and long enough for each provider to cache.
///
/// A provider caches a prompt only above a minimum length (`OpenAI`: 1,024 tokens), and round 2
/// can read round 1 from the cache only if round 1 is above it. The run ID comes first, so that no
/// request of this run matches a request of an earlier run: each cache hit is then of an earlier
/// round of this run.
fn system_prompt() -> String {
    let run_id = format!("{:016x}", rand::random::<u64>());
    let notes = (1..=300).map(|warehouse| {
        format!(
            "Note {warehouse}: warehouse W{warehouse:03} ships the orders of its region with \
             carrier code C{:03}.",
            warehouse * 7 % 1000
        )
    });
    std::iter::once(format!(
        "Run {run_id}. You answer questions about the `orders` table. \
         These notes are for reference only:"
    ))
    .chain(notes)
    .collect::<Vec<_>>()
    .join("\n")
}

/// The parts of `request` that the provider reads as one prefix, in order: the tools, Responses'
/// `instructions` (Spice's system prompt), and then each message or item of the conversation.
fn prompt_parts(api: Api, request: &Value) -> Vec<Value> {
    let items = match api {
        Api::ChatCompletions => &request["messages"],
        Api::Responses => &request["input"],
    };
    [&request["tools"], &request["instructions"]]
        .into_iter()
        .chain(items.as_array().into_iter().flatten())
        .cloned()
        .collect()
}

/// Asserts that the request of `after` starts with all of the request of `before`.
fn assert_starts_with(label: &str, api: Api, before: &Round, after: &Round) {
    let (before, after) = (
        prompt_parts(api, &before.request),
        prompt_parts(api, &after.request),
    );
    let difference = before
        .iter()
        .zip(&after)
        .position(|(before, after)| before != after);
    assert!(
        difference.is_none() && before.len() <= after.len(),
        "{label}: a request does not start with the request before it. Part {difference:?} (0 is \
         the tools, 1 the instructions, then the conversation) changed.\n\
         before: {before:#?}\nafter: {after:#?}"
    );
}

/// The query of the last `sql` call in `request` whose result follows it.
fn sql_call_query(api: Api, request: &Value) -> Option<String> {
    let calls: Vec<(&Value, &Value)> = match api {
        Api::ChatCompletions => request["messages"]
            .as_array()?
            .iter()
            .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
            .filter(|call| call["function"]["name"] == "sql")
            .map(|call| (&call["id"], &call["function"]["arguments"]))
            .collect(),
        Api::Responses => request["input"]
            .as_array()?
            .iter()
            .filter(|item| item["type"] == "function_call" && item["name"] == "sql")
            .map(|item| (&item["call_id"], &item["arguments"]))
            .collect(),
    };
    let has_result = |id: &Value| match api {
        Api::ChatCompletions => request["messages"]
            .as_array()
            .is_some_and(|messages| messages.iter().any(|m| m["tool_call_id"] == *id)),
        Api::Responses => request["input"].as_array().is_some_and(|items| {
            items
                .iter()
                .any(|item| item["type"] == "function_call_output" && item["call_id"] == *id)
        }),
    };
    calls
        .into_iter()
        .rev()
        .find(|(id, _)| has_result(id))
        .and_then(|(_, arguments)| serde_json::from_str::<Value>(arguments.as_str()?).ok())
        .and_then(|arguments| arguments["query"].as_str().map(ToString::to_string))
}

#[rstest]
#[tokio::test]
async fn chat_completions_send_earlier_tool_calls(
    #[values(Provider::OpenAi, Provider::Anthropic, Provider::Xai)] provider: Provider,
    #[values(false, true)] stream: bool,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            Conversation::run(provider, Api::ChatCompletions, stream)
                .await?
                .assert_turn_2_has_the_tool_calls_of_turn_1();
            Ok(())
        })
        .await
}

#[rstest]
#[tokio::test]
async fn chat_completions_reuse_the_prompt_cache(
    #[values(Provider::OpenAi, Provider::Anthropic, Provider::Xai)] provider: Provider,
    #[values(false, true)] stream: bool,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            assert_reuses_the_prompt_cache(provider, Api::ChatCompletions, stream).await
        })
        .await
}

#[rstest]
#[tokio::test]
async fn responses_send_earlier_tool_calls(
    #[values(Provider::OpenAi, Provider::OpenAiReasoning, Provider::Xai)] provider: Provider,
    #[values(false, true)] stream: bool,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            Conversation::run(provider, Api::Responses, stream)
                .await?
                .assert_turn_2_has_the_tool_calls_of_turn_1();
            Ok(())
        })
        .await
}

/// Not for `OpenAI`'s reasoning model: `OpenAI` leaves out the reasoning items of earlier turns, so
/// a later turn can read the cache only up to the first of them, whatever Spice sends. Measured on
/// 2026-10-15 with `gpt-5-mini`: turn 2 had fewer input tokens than the round before it (12,792 vs
/// 12,820), and read 5,632 of them from the cache.
#[rstest]
#[tokio::test]
async fn responses_reuse_the_prompt_cache(
    #[values(Provider::OpenAi, Provider::Xai)] provider: Provider,
    #[values(false, true)] stream: bool,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async { assert_reuses_the_prompt_cache(provider, Api::Responses, stream).await })
        .await
}
