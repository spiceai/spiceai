/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]

use async_openai::types::chat::{
    ChatCompletionMessageToolCalls, ChatCompletionStreamOptions, CreateChatCompletionRequest,
    CreateChatCompletionResponse,
};
use jsonpath_rust::JsonPath;
use llms::{accumulate::accumulate, chat::Chat};
use rstest::rstest;
use serde_json::json;
use std::{
    future::Future,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};
use tokio::runtime::Runtime;

use crate::{TEST_ARGS, init_tracing};

mod create;
mod streaming_tests;

/// Async function that creates a model instance
type AsyncModelCreator = Box<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Arc<dyn Chat>, anyhow::Error>> + Send>,
        > + Send
        + Sync,
>;

/// A given model to test - cached after first creation
type ModelCache = Mutex<Option<Arc<dyn Chat>>>;

/// A multi-thread runtime that lives for as long as the test binary.
///
/// Every test here is a `#[tokio::test]`, so each owns a runtime that is dropped when that test
/// ends, while [`MODEL_CACHES`] hands the same model — and so the same HTTP client — to later
/// tests. A client's connection task is spawned by whichever runtime first drove a request over
/// it, so once that runtime is gone the pooled connection is dead and the next test to reuse the
/// client fails with `hyper::Error(User(DispatchGone), "runtime dropped the dispatch task")`.
/// Building and driving every cached client here keeps those connection tasks alive for as long
/// as the cache that hands them out.
static SHARED_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build the shared test runtime")
});

/// Drives `future` on [`SHARED_RUNTIME`] and awaits its result from the caller's runtime.
///
/// A panic inside `future` is re-raised here, so a failing request still reports the message
/// [`run_test`] panicked with rather than an opaque join error.
async fn on_shared_runtime<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match SHARED_RUNTIME.spawn(future).await {
        Ok(output) => output,
        Err(err) => match err.try_into_panic() {
            Ok(payload) => std::panic::resume_unwind(payload),
            Err(err) => panic!("shared test runtime task did not complete: {err}"),
        },
    }
}

/// Loads `.env` once per process; repeated loads would re-run `set_var` while
/// other test threads are active.
static DOTENV: LazyLock<()> = LazyLock::new(|| {
    // SAFETY: `.env` loading mutates the process environment; it happens once,
    // and tests only read these variables afterwards.
    let _ = unsafe { dotenv::from_filename(".env") }.expect("failed to load .env file");
});

static TEST_MODEL_CREATORS: LazyLock<Vec<(&'static str, AsyncModelCreator)>> = LazyLock::new(
    || {
        vec![
            (
                "bedrock",
                Box::new(|| {
                    Box::pin(async {
                        create::create_bedrock("us.amazon.nova-lite-v1:0")
                            .await
                            .map_err(|e| anyhow::anyhow!("failed to create bedrock model: {e}"))
                    })
                }),
            ),
            (
                "anthropic",
                Box::new(|| {
                    Box::pin(async {
                        create::create_anthropic(None)
                            .map_err(|e| anyhow::anyhow!("failed to create anthropic model: {e}"))
                    })
                }),
            ),
            (
                "google",
                Box::new(|| {
                    Box::pin(async {
                        create::create_google("gemini-2.0-flash")
                            .await
                            .map_err(|e| anyhow::anyhow!("failed to create google model: {e}"))
                    })
                }),
            ),
            (
                "openai",
                Box::new(|| Box::pin(async { Ok(create::create_openai("gpt-4o-mini")) })),
            ),
            (
                "xai",
                Box::new(|| {
                    Box::pin(async {
                        create::create_xai("grok-4.3").map_err(|e| {
                            anyhow::anyhow!("failed to create 'grok-4.3' from xAI: {e}")
                        })
                    })
                }),
            ),
            (
                "hf_phi3",
                Box::new(|| {
                    Box::pin(async {
                        create::create_hf("microsoft/Phi-3-mini-4k-instruct")
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to create 'microsoft/Phi-3-mini-4k-instruct' from HF: {e}"))
                    })
                }),
            ),
            (
                "local_phi3",
                Box::new(|| {
                    Box::pin(async {
                        create::create_local("microsoft/Phi-3-mini-4k-instruct")
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to create 'microsoft/Phi-3-mini-4k-instruct' from local system: {e}"))
                    })
                }),
            ),
        ]
    },
);

static MODEL_CACHES: LazyLock<Vec<(&'static str, ModelCache)>> = LazyLock::new(|| {
    TEST_MODEL_CREATORS
        .iter()
        .filter_map(|(name, _)| {
            if TEST_ARGS.skip_model(name) {
                None
            } else {
                Some((*name, Mutex::new(None)))
            }
        })
        .collect()
});

/// Get or create a model instance for the given name
async fn get_or_create_model(model_name: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let (_, model_cache) = MODEL_CACHES
        .iter()
        .find(|(name, _)| *name == model_name)
        .ok_or_else(|| anyhow::anyhow!("model {model_name} not found in MODEL_CACHES"))?;

    // Check if model is already cached
    {
        let guard = model_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("model cache could not be unlocked"))?;
        if let Some(model) = guard.as_ref() {
            return Ok(Arc::clone(model));
        }
    }

    // Model not cached, create it
    let (_, creator) = TEST_MODEL_CREATORS
        .iter()
        .find(|(name, _)| *name == model_name)
        .ok_or_else(|| anyhow::anyhow!("model creator {model_name} not found"))?;

    let model = creator().await?;

    // Cache the model
    {
        let mut guard = model_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("model cache could not be locked"))?;
        *guard = Some(Arc::clone(&model));
    }

    Ok(model)
}

async fn run_test(
    model_name: &str,
    test_name: &str,
    req: CreateChatCompletionRequest,
    as_stream: bool,
    json_path_checks: Vec<(&str, &str)>,
) -> Result<Option<CreateChatCompletionResponse>, anyhow::Error> {
    LazyLock::force(&DOTENV);
    init_tracing(None);

    if TEST_ARGS.skip_model(model_name) {
        tracing::debug!("Skipping test {model_name}/{test_name}");
        return Ok(None);
    }

    let model = on_shared_runtime({
        let model_name = model_name.to_string();
        async move { get_or_create_model(&model_name).await }
    })
    .await
    .unwrap_or_else(|e| panic!("failed to get or create model {model_name}: {e}"));

    tracing::info!("Running test {test_name}/{model_name} with {req:?}");

    let actual_resp = on_shared_runtime({
        let (test_name, model_name) = (test_name.to_string(), model_name.to_string());
        async move {
            if as_stream {
                let mut req = req;
                req.stream = Some(true);
                req.stream_options = Some(ChatCompletionStreamOptions {
                    include_usage: Some(true),
                    include_obfuscation: None,
                });
                accumulate(model.chat_stream(req).await.unwrap_or_else(|e| {
                    panic!("For test {test_name}/{model_name}, chat_stream failed. Error: {e:#?}")
                }))
                .await
            } else {
                model.chat_request(req).await.unwrap_or_else(|e| {
                    panic!("For test {test_name}/{model_name}, chat_request failed. Error: {e:#?}")
                })
            }
        }
    })
    .await;
    tracing::debug!("Response for {test_name}/{model_name}: {actual_resp:?}");

    let resp_value =
        serde_json::to_value(&actual_resp).expect("failed to serialize response to JSON");
    for (id, json_ptr) in &json_path_checks {
        let resp_ptr = JsonPath::from_str(json_ptr)
            .expect("invalid JSONPath selector")
            .find(&resp_value);
        insta::assert_snapshot!(
            format!("{test_name}_{model_name}_{id}"),
            serde_json::to_string_pretty(&resp_ptr).expect("Failed to serialize snapshot")
        );
    }
    Ok(Some(actual_resp))
}

/// Drives a body that spawns a background task, drops the runtime the test itself owns, and
/// reports whether the task then ran to completion.
///
/// The background task stands in for a connection task: spawned as a side effect of driving a
/// request, and needed again by whichever test reuses the cached client next. With
/// `via_shared_runtime` the body runs on [`SHARED_RUNTIME`], as [`run_test`] now drives every
/// request; otherwise it runs on the per-test runtime, which is what produced #13575.
fn spawned_work_outlives_per_test_runtime(via_shared_runtime: bool) -> bool {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel::<()>();
    let per_test = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build the per-test runtime");

    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

    let body = async move {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = started_tx.send(());
            // Wait to be released rather than for a fixed time. The release is sent only after
            // the per-test runtime is dropped, so the drop always happens first however the test
            // thread is scheduled; a timer here would let a preempted thread finish the task
            // early and report the control case as surviving.
            let _ = release_rx.await;
            let _ = finished_tx.send(());
        });
        // Both cases must measure survival rather than whether the task ever got going, so let
        // it start before the runtime that spawned it can go away.
        started_rx.await.expect("the background task never started");
    };

    if via_shared_runtime {
        per_test.block_on(on_shared_runtime(body));
    } else {
        per_test.block_on(body);
    }
    drop(per_test);
    // Released only now: a task that died with the runtime can never observe this, so the two
    // cases are separated by what survives the drop rather than by elapsed time.
    let _ = release_tx.send(());

    // Dropping a runtime drops its tasks, which drops `finished_tx` and disconnects the channel,
    // so a task that died is reported immediately rather than by waiting out the timeout.
    finished_rx.recv_timeout(Duration::from_secs(30)).is_ok()
}

/// Work driven through [`on_shared_runtime`] must survive the test that started it, because the
/// model cache hands the same client to a later test whose own runtime did not create it.
///
/// Regression test for #13575.
#[test]
fn shared_runtime_keeps_spawned_work_alive_past_the_test_that_started_it() {
    // Control first: on the per-test runtime the task does not survive. Without this the
    // assertion below would pass even if `on_shared_runtime` did nothing at all.
    assert!(
        !spawned_work_outlives_per_test_runtime(false),
        "a task spawned on the per-test runtime outlived it, so this test can no longer tell whether the shared runtime is doing anything"
    );

    assert!(
        spawned_work_outlives_per_test_runtime(true),
        "work driven through `on_shared_runtime` died with the per-test runtime; a cached client's connection task would die with it too, failing the next test with `DispatchGone` (#13575)"
    );
}

/// [`run_test`] reports a provider failure by panicking with the test and model names, so
/// [`on_shared_runtime`] has to re-raise that panic rather than replace it with a join error.
#[test]
fn on_shared_runtime_re_raises_the_original_panic() {
    let per_test = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build the per-test runtime");

    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        per_test.block_on(on_shared_runtime(async {
            panic!("For test basic/xai, chat_request failed.");
        }));
    }))
    .expect_err("the panic did not propagate out of `on_shared_runtime`");

    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_else(|| panic!("the propagated panic carried no string payload"));
    assert!(
        message.contains("For test basic/xai, chat_request failed."),
        "the panic reached the test as {message:?}, losing the diagnostic `run_test` panicked with"
    );
}

#[rstest]
#[tokio::test]
async fn test_basic(
    #[values(
        "anthropic",
        "openai",
        "xai",
        "local_phi3",
        "hf_phi3",
        "bedrock",
        "google"
    )]
    model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req: CreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "not_needed",
        "messages": [
            {
                "role": "user",
                "content": "Say Hello"
            }
        ]
    }))
    .expect("failed to create request");

    let _ = run_test(
        model_name,
        "basic",
        req,
        as_stream,
        vec![(
            "replied_appropriately",
            "$.choices[*].message[?(@.content ~= 'Hello')].length()",
        )],
    )
    .await
    .expect("test failed");
}

#[rstest]
#[tokio::test]
async fn test_usage(
    #[values(
        "anthropic",
        "openai",
        "xai",
        "local_phi3",
        "hf_phi3",
        "bedrock",
        "google"
    )]
    model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req: CreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "not_needed",
        "messages": [
            {
                "role": "user",
                "content": "Say Hello"
            }
        ]
    }))
    .expect("failed to create request");

    run_test(
        model_name,
        "usage",
        req,
        as_stream,
        vec![
            (
                "has_prompt_tokens",
                "$.usage[?(@.prompt_tokens > 0)].length()",
            ),
            (
                "has_completion_tokens",
                "$.usage[?(@.completion_tokens > 0)].length()",
            ),
            (
                "has_total_tokens",
                "$.usage[?(@.total_tokens > 0)].length()",
            ),
            (
                "total_tokens_gt_prompt_tokens",
                "$.usage[?(@.total_tokens >= @.prompt_tokens)].length()",
            ),
            (
                "total_tokens_gt_completion_tokens",
                "$.usage[?(@.total_tokens >= @.completion_tokens)].length()",
            ),
        ],
    )
    .await
    .expect("test failed");
}

#[rstest]
#[tokio::test]
async fn test_system_prompt(
    #[values("anthropic", "openai", "xai", "local_phi3", "hf_phi3", "google")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req: CreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "not_needed",
        "messages": [
            {
                "role": "system",
                "content": "Quote back the exact message from the user"
            },
            {
                "role": "user",
                "content": "pong"
            }
        ],
        "max_completion_tokens": 100,
    }))
    .expect("failed to create request");
    run_test(
        model_name,
        "system_prompt",
        req,
        as_stream,
        vec![
            (
                "assistant_response",
                "$.choices[*].message[?(@.role == 'assistant' && @.content ~= 'pong')].length()",
            ),
            (
                "replied_appropriately",
                "$.choices[*].message[?(@.content ~= '(?i)pong')].length()",
            ),
        ],
    )
    .await
    .expect("test failed");
}

#[rstest]
#[tokio::test]
async fn test_supports_basic_message_roles(
    #[values(
        "anthropic",
        "openai",
        "xai",
        "local_phi3",
        "hf_phi3",
        "bedrock",
        "google"
    )]
    model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req: CreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "not_needed",
        "messages": [
            {
                "role": "system",
                "content": "Quote back the exact message from the user"
            },
            {
                "role": "user",
                "content": "call a tool"
            },
            {
                "role": "assistant",
                "content": "Sorry I, can't call a tool. ",
            },
            {
                "role": "user",
                "content": "That's fine. Tell me a joke."
            }
        ],
    }))
    .expect("failed to create request");

    run_test(
        model_name,
        "supports_basic_message_roles",
        req,
        as_stream,
        vec![],
    )
    .await
    .expect("test failed");
}

#[rstest]
#[tokio::test]
async fn test_supports_all_message_roles(
    #[values("anthropic", "openai", "xai", "bedrock", "google")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req: CreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "not_needed",
        "messages": [
            {
                "role": "system",
                "content": "Quote back the exact message from the user"
            },
            {
                "role": "user",
                "content": "call a tool"
            },
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "1",
                        "type": "function",
                        "function": {
                            "name": "get_current_weather",
                            "arguments": "{\"location\": \"San Francisco, CA\"}"
                        }
                    }
                ]
            },
            {
                "role": "tool",
                "content": "72",
                "tool_call_id": "1"
            }
        ],
        "tools": [
          {
            "type": "function",
            "function": {
              "name": "get_current_weather",
              "parameters": {
                "type": "object",
                "properties": {},
                "required": []
              }
            }
          }
        ]
    }))
    .expect("failed to create request");

    run_test(
        model_name,
        "supports_all_message_roles",
        req,
        as_stream,
        vec![],
    )
    .await
    .expect("test failed");
}

#[rstest]
#[tokio::test]
async fn test_tool_use(
    #[values("anthropic", "openai", "google", "xai", "bedrock")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    // serde_json::from_value(
    let req: CreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "not_needed",
        "messages": [
            {
              "role": "user",
              "content": "What's the weather like in Boston today?"
            }
        ],
        "tool_choice": {"type": "function", "function": {"name": "get_current_weather"}},
        "tools": [
          {
            "type": "function",
            "function": {
              "name": "get_current_weather",
              "description": "Get the current weather in a given location, in Celsius",
              "parameters": {
                "type": "object",
                "properties": {
                  "location": {
                    "type": "string",
                    "description": "The city and state, e.g. San Francisco."
                  },
                  "unit": {
                    "type": "string",
                    "enum": ["celsius", "fahrenheit"]
                  }
                },
                "required": ["location", "unit"]
              }
            }
          }
        ]
    }))
    .expect("failed to create request");

    let resp = run_test(
        model_name,
        "tool_use",
        req,
        as_stream,
        vec![
            ("finish_reason", "$.choices[0].finish_reason"),
            (
                "tool_choice",
                "$.choices[0].message.tool_calls[0].function.name",
            ),
        ],
    )
    .await
    .expect("test failed");

    let Some(resp) = resp else {
        // Test was skipped
        return;
    };

    // JSON Parse the function arguments to ensure robust to ordering.
    let tool_calls = resp
        .choices
        .first()
        .expect("no choices in response")
        .message
        .tool_calls
        .as_ref()
        .expect("no tool calls in message");

    let first_tool_call = tool_calls.first().expect("no tool calls");
    let function = match first_tool_call {
        ChatCompletionMessageToolCalls::Function(f) => &f.function,
        ChatCompletionMessageToolCalls::Custom(_) => panic!("unexpected custom tool call"),
    };

    let args: serde_json::Value = serde_json::from_str(function.arguments.as_str())
        .expect("failed to parse tool call arguments");

    insta::assert_json_snapshot!(format!("tool_use_{model_name}_valid_function_args"), args);
}

/// The built-in Anthropic default has to satisfy two things no unit test can check: Anthropic
/// still serves it, and it still accepts the sampling controls the adapter forwards. Anthropic's
/// newest generation answers ``temperature` is deprecated for this model.` and rejects the request
/// outright (#13564), so a default bumped onto such a model would turn every configuration that
/// sets `temperature` into a guaranteed 400 — while every test that sets none of these stays green.
/// This is the guard for that, and it is why the default trails Anthropic's newest model.
///
/// One control per request: every Claude 4+ model rejects `temperature` and `top_p` set *together*,
/// so a request carrying both asserts nothing about the default — it 400s on any model this constant
/// may name. #13579 tracks the adapter forwarding that combination.
///
/// This builds its own model rather than going through `run_test`/`get_or_create_model`, which is
/// the one deviation from every other test in this file and is load-bearing. That cache holds one
/// client per provider for the whole binary while each `#[tokio::test]` owns its own runtime, so a
/// request can be issued on a client whose hyper dispatch task was spawned on a runtime that has
/// since been dropped — it then fails with `User(DispatchGone)` before reaching Anthropic (#13575).
/// Measured, not assumed: that is what took this guard in
/// [run 32982484882](https://github.com/spiceai/spiceai/actions/runs/32982484882) and again in
/// [run 33017678054](https://github.com/spiceai/spiceai/actions/runs/33017678054) after it had been
/// narrowed to a single test, because the exposure comes from *which* runtime created the cached
/// client, not from how many tests or requests use it. Owning the client keeps its dispatch task on
/// this test's runtime, so a failure here is Anthropic's answer rather than a dead client.
///
/// No snapshots: the assertion is that each request was accepted at all. Passing `None` as the model
/// id is what makes this exercise whatever the default currently is rather than a pinned copy of it.
#[tokio::test]
async fn default_anthropic_model_accepts_forwarded_sampling_controls() {
    LazyLock::force(&DOTENV);
    let _tracing = init_tracing(None);

    if TEST_ARGS.skip_model("anthropic") {
        tracing::debug!("Skipping test anthropic/default_sampling_controls");
        return;
    }

    let model = create::create_anthropic(None)
        .unwrap_or_else(|e| panic!("failed to build the default Anthropic model: {e}"));

    // Every control `crates/llms/src/anthropic/chat.rs` forwards to Anthropic: `temperature` and
    // `top_p` pass through, and `top_logprobs` becomes Anthropic's `top_k`. That last translation
    // is between unrelated parameters and is itself a defect (#13581) — this asserts only that the
    // default model accepts what the converter currently sends, not that it should send it.
    for (control, value) in [
        ("temperature", json!(0.5)),
        ("top_p", json!(0.9)),
        ("top_logprobs", json!(5)),
    ] {
        let req: CreateChatCompletionRequest = serde_json::from_value(json!({
            "model": "not_needed",
            "messages": [{"role": "user", "content": "Say Hello"}],
            "max_completion_tokens": 16,
            control: value,
        }))
        .unwrap_or_else(|e| panic!("failed to create the {control} request: {e}"));

        model
            .chat_request(req)
            .await
            .unwrap_or_else(|e| panic!("the default model rejected {control}: {e:#?}"));
    }
}
