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
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

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

/// A model fixture: the name tests select it by, how to create it, and the artifact repository
/// its creation fetches from.
struct TestModel {
    /// Name tests select this fixture by.
    name: &'static str,
    /// Hugging Face repository this fixture's creation downloads artifacts from, if any.
    ///
    /// `hf_hub` guards each blob it downloads with a lock file and fails outright rather than
    /// waiting for a download already in flight, so two creations fetching the same repository
    /// concurrently lose the race. [`MODEL_CACHES`] cannot serialize them: it keys a mutex per
    /// fixture, and `hf_phi3` and `local_phi3` are two fixtures over one repository. Naming the
    /// repository here is what puts both fixtures on one [`TestModel::creation_key`], and so on
    /// one creation lock (#13560).
    fetch_repo: Option<&'static str>,
    create: AsyncModelCreator,
}

impl TestModel {
    /// The resource this fixture's creation must not share with a concurrent creation.
    ///
    /// Its artifact repository when it downloads one, since two fixtures over one repository
    /// contend for its blob locks; otherwise the fixture itself, so two callers of one fixture
    /// still cannot create it twice.
    fn creation_key(&self) -> &'static str {
        self.fetch_repo.unwrap_or(self.name)
    }
}

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

/// Hugging Face repository behind both phi3 fixtures.
///
/// Named once so the two fixtures cannot drift onto different spellings of the repository they
/// share, which is what their [`TestModel::fetch_repo`] serialization keys on.
const PHI3_REPO: &str = "microsoft/Phi-3-mini-4k-instruct";

static TEST_MODELS: LazyLock<Vec<TestModel>> = LazyLock::new(|| {
    vec![
        TestModel {
            name: "bedrock",
            fetch_repo: None,
            create: Box::new(|| {
                Box::pin(async {
                    create::create_bedrock("us.amazon.nova-lite-v1:0")
                        .await
                        .map_err(|e| anyhow::anyhow!("failed to create bedrock model: {e}"))
                })
            }),
        },
        TestModel {
            name: "anthropic",
            fetch_repo: None,
            create: Box::new(|| {
                Box::pin(async {
                    create::create_anthropic(None)
                        .map_err(|e| anyhow::anyhow!("failed to create anthropic model: {e}"))
                })
            }),
        },
        TestModel {
            name: "google",
            fetch_repo: None,
            create: Box::new(|| {
                Box::pin(async {
                    create::create_google("gemini-2.0-flash")
                        .await
                        .map_err(|e| anyhow::anyhow!("failed to create google model: {e}"))
                })
            }),
        },
        TestModel {
            name: "openai",
            fetch_repo: None,
            create: Box::new(|| Box::pin(async { Ok(create::create_openai("gpt-4o-mini")) })),
        },
        TestModel {
            name: "xai",
            fetch_repo: None,
            create: Box::new(|| {
                Box::pin(async {
                    create::create_xai("grok-4.3")
                        .map_err(|e| anyhow::anyhow!("failed to create 'grok-4.3' from xAI: {e}"))
                })
            }),
        },
        TestModel {
            name: "hf_phi3",
            fetch_repo: Some(PHI3_REPO),
            create: Box::new(|| {
                Box::pin(async {
                    create::create_hf(PHI3_REPO)
                        .await
                        .map_err(|e| anyhow::anyhow!("failed to create '{PHI3_REPO}' from HF: {e}"))
                })
            }),
        },
        TestModel {
            name: "local_phi3",
            fetch_repo: Some(PHI3_REPO),
            create: Box::new(|| {
                Box::pin(async {
                    create::create_local(PHI3_REPO).await.map_err(|e| {
                        anyhow::anyhow!("failed to create '{PHI3_REPO}' from local system: {e}")
                    })
                })
            }),
        },
    ]
});

static MODEL_CACHES: LazyLock<Vec<(&'static str, ModelCache)>> = LazyLock::new(|| {
    TEST_MODELS
        .iter()
        .filter_map(|model| {
            if TEST_ARGS.skip_model(model.name) {
                None
            } else {
                Some((model.name, Mutex::new(None)))
            }
        })
        .collect()
});

/// Whether the model is served in-process by mistral.rs rather than by a hosted provider.
fn is_local_model(model_name: &str) -> bool {
    matches!(model_name, "local_phi3" | "hf_phi3")
}

/// Matches any non-empty content.
///
/// A small local model cannot be relied on to follow an instruction: asked to quote back
/// `pong`, Phi-3-mini answers with the wrong word or with an essay about the arcade game.
/// Its reply is therefore asserted for shape, not for wording — the same split
/// `normalize_chat_completion_response` draws in `crates/runtime/tests/models/mod.rs`,
/// where local models pass `normalize_message_content: true` and hosted models `false`.
/// See <https://github.com/spiceai/spiceai/issues/3426>.
const ANY_CONTENT: &str = "(?s).";

/// The content regex a check should use for `model_name`: what a hosted provider was asked
/// to say, or mere presence for a local model.
fn content_pattern<'a>(model_name: &str, hosted: &'a str) -> &'a str {
    if is_local_model(model_name) {
        ANY_CONTENT
    } else {
        hosted
    }
}

/// One lock per [`TestModel::creation_key`], shared by every fixture with that key.
static CREATION_LOCKS: LazyLock<Vec<(&'static str, tokio::sync::Mutex<()>)>> =
    LazyLock::new(|| {
        let mut keys: Vec<&'static str> = TEST_MODELS.iter().map(TestModel::creation_key).collect();
        keys.sort_unstable();
        keys.dedup();
        keys.into_iter()
            .map(|key| (key, tokio::sync::Mutex::new(())))
            .collect()
    });

/// The lock serializing creations under `creation_key`.
fn creation_lock(creation_key: &str) -> Result<&'static tokio::sync::Mutex<()>, anyhow::Error> {
    CREATION_LOCKS
        .iter()
        .find(|(key, _)| *key == creation_key)
        .map(|(_, lock)| lock)
        .ok_or_else(|| anyhow::anyhow!("no creation lock registered for {creation_key}"))
}

/// Returns `cache`'s value, awaiting `create` and caching the result if it is empty.
///
/// Creations sharing a `creation_key` run one at a time, and each caches its result *before*
/// releasing the lock. Both halves matter: the lock keeps two fixtures over one Hugging Face
/// repository from contending for its blob locks, and caching inside it means a caller that
/// waited finds the value the winner cached rather than creating a second copy of the same model
/// (#13560).
async fn get_or_create_cached<T: Clone>(
    cache: &Mutex<Option<T>>,
    lock: &tokio::sync::Mutex<()>,
    create: impl Future<Output = Result<T, anyhow::Error>>,
) -> Result<T, anyhow::Error> {
    if let Some(cached) = cached(cache)? {
        return Ok(cached);
    }

    let creation_guard = lock.lock().await;

    // Re-checked under the lock: whoever held it before may have been creating this very value.
    if let Some(cached) = cached(cache)? {
        return Ok(cached);
    }

    let created = create.await?;
    store_under_creation_lock(&creation_guard, cache, &created)?;

    Ok(created)
}

/// Caches `value`, taking the creation guard by reference so that it cannot be cached outside the
/// lock.
///
/// The ordering is the whole point of the lock — see [`get_or_create_cached`] — and no test can
/// establish it. `#[tokio::test]` builds a current-thread runtime and there is no `.await` between
/// releasing the guard and storing, so a store moved after the release still lands before the
/// waiting caller is polled: it finds the value either way and the run count is 1 either way. Nor
/// does a probe at the store site help, because tokio's mutex is fair — releasing a guard with a
/// caller already queued hands the permit straight to that caller, so the lock never *looks* free
/// from here even when the store has escaped it.
///
/// So the guarantee is carried by this signature instead. Moving the store after the guard is
/// dropped leaves nothing to pass, and it stops compiling rather than silently regressing.
fn store_under_creation_lock<T: Clone>(
    _creation_guard: &tokio::sync::MutexGuard<'_, ()>,
    cache: &Mutex<Option<T>>,
    value: &T,
) -> Result<(), anyhow::Error> {
    *cache
        .lock()
        .map_err(|_| anyhow::anyhow!("cache could not be locked"))? = Some(value.clone());
    Ok(())
}

/// Reads `cache` without holding its lock past the read, so it is never held across an `.await`.
fn cached<T: Clone>(cache: &Mutex<Option<T>>) -> Result<Option<T>, anyhow::Error> {
    let guard = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("cache could not be locked"))?;
    Ok(guard.clone())
}

/// Get or create a model instance for the given name
async fn get_or_create_model(model_name: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let (_, model_cache) = MODEL_CACHES
        .iter()
        .find(|(name, _)| *name == model_name)
        .ok_or_else(|| anyhow::anyhow!("model {model_name} not found in MODEL_CACHES"))?;

    let model_fixture = TEST_MODELS
        .iter()
        .find(|model| model.name == model_name)
        .ok_or_else(|| anyhow::anyhow!("model creator {model_name} not found"))?;

    get_or_create_cached(
        model_cache,
        creation_lock(model_fixture.creation_key())?,
        (model_fixture.create)(),
    )
    .await
}

async fn run_test(
    model_name: &str,
    test_name: &str,
    req: CreateChatCompletionRequest,
    as_stream: bool,
    json_path_checks: Vec<(&str, &str)>,
) -> Result<Option<CreateChatCompletionResponse>, anyhow::Error> {
    LazyLock::force(&DOTENV);
    // Hold the guard for the body: `set_default` installs the subscriber only for the
    // guard's lifetime, so dropping it here would discard every event this test logs.
    let _tracing_guard = init_tracing(None);

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

    let replied_appropriately = format!(
        "$.choices[*].message[?(@.content ~= '{}')].length()",
        content_pattern(model_name, "Hello")
    );

    let _ = run_test(
        model_name,
        "basic",
        req,
        as_stream,
        vec![("replied_appropriately", replied_appropriately.as_str())],
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
    let assistant_response = format!(
        "$.choices[*].message[?(@.role == 'assistant' && @.content ~= '{}')].length()",
        content_pattern(model_name, "pong")
    );
    let replied_appropriately = format!(
        "$.choices[*].message[?(@.content ~= '{}')].length()",
        content_pattern(model_name, "(?i)pong")
    );

    run_test(
        model_name,
        "system_prompt",
        req,
        as_stream,
        vec![
            ("assistant_response", assistant_response.as_str()),
            ("replied_appropriately", replied_appropriately.as_str()),
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

/// The creation lock fixture `name` resolves to.
fn fixture_creation_lock(name: &str) -> &'static tokio::sync::Mutex<()> {
    let key = TEST_MODELS
        .iter()
        .find(|model| model.name == name)
        .unwrap_or_else(|| panic!("fixture {name} is not registered in TEST_MODELS"))
        .creation_key();
    creation_lock(key).unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}

/// `hf_phi3` and `local_phi3` fetch one Hugging Face repository, so they must resolve to one
/// creation lock: `hf_hub` fails a blob whose lock file is already taken instead of waiting for
/// the download in flight, so two unserialized creations race and the loser panics.
///
/// Regression test for #13560.
#[test]
fn the_two_phi3_fixtures_share_one_creation_lock() {
    // Control first: a fixture that downloads nothing gets a lock of its own, so this is not
    // merely observing that every fixture shares one global lock.
    assert!(
        !std::ptr::eq(
            fixture_creation_lock("hf_phi3"),
            fixture_creation_lock("openai")
        ),
        "'hf_phi3' and 'openai' share a creation lock; they fetch nothing in common, so this test can no longer tell whether the lock is keyed on the artifact repository at all"
    );

    assert!(
        std::ptr::eq(
            fixture_creation_lock("hf_phi3"),
            fixture_creation_lock("local_phi3")
        ),
        "'hf_phi3' and 'local_phi3' resolve to different creation locks, so their creations can run concurrently; they fetch one Hugging Face repository, and both would race on its blob lock (#13560)"
    );
}

/// A stand-in creation that reports when it runs, counts its runs on `runs`, and waits to be
/// released before returning `value`.
///
/// Returns the "it ran" receiver, the release sender, and the creation itself. A creation that
/// should not block is released before it is spawned; a `oneshot` delivers a value sent ahead of
/// the receiver, so the creation returns as soon as it runs.
fn probe_creation(
    runs: Arc<AtomicUsize>,
    value: &'static str,
) -> (
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    impl Future<Output = Result<&'static str, anyhow::Error>>,
) {
    let (ran_tx, ran) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();

    let create = async move {
        runs.fetch_add(1, Ordering::SeqCst);
        let _ = ran_tx.send(());
        let _ = release_rx.await;
        Ok(value)
    };

    (ran, release, create)
}

/// Waits, bounded, for the creation behind `ran` to start, and reports whether it did.
///
/// Bounded rather than polled because one caller below is establishing the *absence* of a
/// creation, which can only be established by waiting for one. The caller that expects the
/// creation returns as soon as it runs, so the timeout is only ever spent proving a negative.
async fn creation_starts(ran: oneshot::Receiver<()>) -> bool {
    tokio::time::timeout(Duration::from_secs(5), ran)
        .await
        .is_ok_and(|started| started.is_ok())
}

/// A caller that waits for the creation lock must find the value the winner cached, not create a
/// second copy — for `local_phi3` a second copy is another set of model weights resident in the
/// test process. That holds only because the value is cached *before* the lock is released.
///
/// This test does not establish that ordering, and cannot: see
/// [`store_under_creation_lock`], whose signature is what holds it. What this covers is the
/// behaviour on top of it — that a caller which waits is served the cached value rather than
/// creating its own, and that it is served the *same* value.
///
/// Uses a lock of its own rather than a fixture's: the registry's locks are shared with the model
/// creations the rest of this binary performs, and blocking one for the duration of this test
/// would stall them.
#[tokio::test]
async fn a_caller_that_waits_for_a_creation_takes_its_cached_value() {
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let cache = Arc::new(Mutex::new(None));
    let runs = Arc::new(AtomicUsize::new(0));

    let (first_ran, first_release, first_create) = probe_creation(Arc::clone(&runs), "created");
    let (second_ran, second_release, second_create) =
        probe_creation(Arc::clone(&runs), "created again");
    // The second creation is under test for whether it *starts*, so it must not block once it has.
    let _ = second_release.send(());

    let (first_cache, first_lock) = (Arc::clone(&cache), Arc::clone(&lock));
    let first_call =
        tokio::spawn(
            async move { get_or_create_cached(&first_cache, &first_lock, first_create).await },
        );

    // Also the control for the assertion below: it shows `creation_starts` can observe a creation
    // that does start, so the negative there is not vacuous.
    assert!(
        creation_starts(first_ran).await,
        "the first creation never started"
    );

    // Spawned while the first still holds the lock, so it reaches the cache only after the first
    // has released — which is the handoff under test.
    let (second_cache, second_lock) = (Arc::clone(&cache), Arc::clone(&lock));
    let second_call = tokio::spawn(async move {
        get_or_create_cached(&second_cache, &second_lock, second_create).await
    });

    // If the second caller could get past the lock now, it would observe the empty cache and
    // create its own copy.
    assert!(
        !creation_starts(second_ran).await,
        "a second creation started while the first held the creation lock, so the two are not serialized at all"
    );

    let _ = first_release.send(());
    let first_value = first_call
        .await
        .expect("the first call panicked")
        .expect("the first creation failed");
    let second_value = second_call
        .await
        .expect("the second call panicked")
        .expect("the second call failed");

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the waiting caller created a second value instead of taking the one already cached; caching happens inside the creation lock precisely so it cannot (#13560)"
    );
    assert_eq!(
        second_value, first_value,
        "the waiting caller returned a different value than the one that was cached"
    );
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
