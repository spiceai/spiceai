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

use async_openai::types::chat::{
    ChatCompletionMessageToolCalls, ChatCompletionStreamOptions, CreateChatCompletionRequest,
    CreateChatCompletionResponse,
};
use jsonpath_rust::JsonPath;
use llms::{accumulate::accumulate, chat::Chat};
use rstest::rstest;
use serde_json::json;
use std::{
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
};

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
                        create::create_xai("grok-3")
                            .map_err(|e| anyhow::anyhow!("failed to create 'grok-3' from xAI: {e}"))
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
            (
                "perplexity",
                Box::new(|| {
                    Box::pin(async {
                        create::create_perplexity()
                            .map_err(|e| anyhow::anyhow!("failed to create perplexity model: {e}"))
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
    let _ = dotenvy::from_filename(".env").expect("failed to load .env file");
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
        "perplexity",
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
        "perplexity",
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
