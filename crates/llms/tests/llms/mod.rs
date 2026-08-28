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

    let _creation_guard = lock.lock().await;

    // Re-checked under the lock: whoever held it before may have been creating this very value.
    if let Some(cached) = cached(cache)? {
        return Ok(cached);
    }

    let created = create.await?;
    *cache
        .lock()
        .map_err(|_| anyhow::anyhow!("cache could not be locked"))? = Some(created.clone());

    Ok(created)
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
    init_tracing(None);

    if TEST_ARGS.skip_model(model_name) {
        tracing::debug!("Skipping test {model_name}/{test_name}");
        return Ok(None);
    }

    let model = get_or_create_model(model_name)
        .await
        .unwrap_or_else(|e| panic!("failed to get or create model {model_name}: {e}"));

    tracing::info!("Running test {test_name}/{model_name} with {req:?}");

    let actual_resp = if as_stream {
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
    };
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
