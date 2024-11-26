/*
Copyright 2024 The Spice.ai OSS Authors

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
use async_openai::types::CreateChatCompletionRequest;
use jsonpath_rust::JsonPath;
use llms::chat::Chat;
use serde_json::json;
use std::{
    str::FromStr,
    sync::{Arc, LazyLock},
};

use crate::{init_tracing, TEST_ARGS};

mod create;

#[derive(Clone)]
pub struct TestCase {
    pub name: &'static str,
    pub req: CreateChatCompletionRequest,

    /// Maps (id, `JSONPath` selector), where the selector is into the [`CreateChatCompletionResponse`].
    /// This is used in snapshot testing to assert certain properties of the response.
    pub json_path: Vec<(&'static str, &'static str)>,
}
/// Creates [`TestCase`] instances from request/response that JSON serialize to
/// [`CreateChatCompletionRequest`] and [`CreateChatCompletionResponse`].
#[macro_export]
macro_rules! test_case {
    ($name:expr, $req:expr, $jsonpaths:expr) => {
        TestCase {
            name: $name,
            req: serde_json::from_value($req)
                .expect(&format!("Failed to parse request in test case '{}'", $name)),
            json_path: $jsonpaths,
        }
    };
}

/// Test case parameters (for [`run_test_case`]) to run for each model.
static TEST_CASES: LazyLock<Vec<TestCase>> = LazyLock::new(|| {
    vec![
        test_case!(
            "basic",
            json!({
                "model": "not_needed",
                "messages": [
                    {
                        "role": "user",
                        "content": "Say Hello"
                    }
                ]
            }),
            vec![
                (
                    "message_keys",
                    "$.choices[*].message['role', 'tool_calls', 'refusal']"
                ),
                (
                    "replied_appropriately",
                    "$.choices[*].message[?(@.content ~= 'Hello')].length()"
                )
            ]
        ),
        test_case!(
            "system_prompt",
            json!({
                "model": "not_needed",
                "messages": [
                    {
                        "role": "system",
                        "content": "Repeat back any user message."
                    },
                    {
                        "role": "user",
                        "content": "Hi"
                    }
                ]
            }),
            vec![
                (
                    "assistant_response",
                    "$.choices[*].message[?(@.role == 'assistant' && @.content ~= 'Hi')].length()"
                ),
                (
                    "replied_appropriately",
                    "$.choices[*].message[?(@.content ~= 'Hi')].length()"
                )
            ]
        ),
        test_case!(
            "tool_use",
            json!({
                "model": "not_needed",
                "messages": [
                    {
                      "role": "user",
                      "content": "What'\''s the weather like in Boston today?"
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
                            "description": "The city and state, e.g. San Francisco, CA"
                          },
                          "unit": {
                            "type": "string",
                            "enum": ["celsius", "fahrenheit"]
                          }
                        },
                        "required": ["location"]
                      }
                    }
                  }
                ]
            }),
            vec![
                ("finish_reason", "$.choices[0].finish_reason"),
                (
                    "tool_choice",
                    "$.choices[0].message.tool_calls[0].function.name"
                ),
                (
                    "valid_function_args",
                    "$.choices[0].message.tool_calls[0].function.arguments"
                )
            ]
        ),
    ]
});

/// For a given mode name, a function that instantiates the model..
type ModelFn<'a> = (&'a str, Box<dyn Fn() -> Arc<Box<dyn Chat>>>);

/// A given model to test.
type ModelDef<'a> = (&'a str, Arc<Box<dyn Chat>>);
#[allow(clippy::expect_used)]
static TEST_MODELS: LazyLock<Vec<ModelDef>> = LazyLock::new(|| {
    let model_creators: [ModelFn; 4] = [
        (
            "anthropic",
            Box::new(|| create::create_anthropic(None).expect("failed to create anthropic model")),
        ),
        ("openai", Box::new(|| create::create_openai("gpt-4o-mini"))),
        (
            "hf/phi3",
            Box::new(|| {
                create::create_hf("microsoft/Phi-3-mini-4k-instruct")
                    .expect("failed to create 'microsoft/Phi-3-mini-4k-instruct' from HF")
            }),
        ),
        (
            "local/phi3",
            Box::new(|| {
                create::create_local("microsoft/Phi-3-mini-4k-instruct")
                    .expect("failed to create 'microsoft/Phi-3-mini-4k-instruct' from local system")
            }),
        ),
    ];

    model_creators
        .iter()
        .filter_map(|(name, creator)| {
            if TEST_ARGS.skip_model(name) {
                None
            } else {
                Some((*name, creator()))
            }
        })
        .collect()
});

/// A mapping of model names (in [`TEST_MODELS`]) and test names (in [`TEST_CASES`]) to skip.
static TEST_DENY_LIST: LazyLock<Vec<(&'static str, &'static str)>> =
    LazyLock::new(|| vec![("hf/phi3", "tool_use"), ("local/phi3", "tool_use")]);

/// Run a single [`TestCase`] for a model.
#[allow(clippy::expect_used, clippy::expect_fun_call)]
async fn run_test_case(
    test: &TestCase,
    model_name: &'static str,
    model: Arc<Box<dyn Chat>>,
) -> Result<(), anyhow::Error> {
    let test_name = test.name;
    tracing::info!("Running test {test_name}/{model_name} with {:?}", test.req);

    let actual_resp = model
        .chat_request(test.req.clone())
        .await
        .expect(format!("For test {test_name}/{model_name}, chat_request failed").as_str());

    tracing::trace!("Response for {test_name}/{model_name}: {actual_resp:?}");
    // Convert to [`serde_json::Value`] for JSONPath testing.
    let resp_value = serde_json::to_value(&actual_resp).expect(
        format!("For test {test_name}/{model_name}, failed to serialize response to JSON").as_str(),
    );
    for (id, json_ptr) in &test.json_path {
        let resp_ptr = JsonPath::from_str(json_ptr)
            .expect(format!("For test {test_name}, invalid JSONPath selector for id={id}").as_str())
            .find(&resp_value);
        insta::assert_snapshot!(
            format!("{test_name}_{model_name}_{id}"),
            serde_json::to_string_pretty(&resp_ptr).expect("Failed to serialize snapshot")
        );
    }
    Ok(())
}

#[tokio::test]
#[allow(clippy::expect_used, clippy::expect_fun_call)]
async fn run_all_tests() {
    // Set ENV variables before we lazy load `TEST_MODELS`.
    let _ = dotenvy::from_filename(".env").expect("failed to load .env file");
    init_tracing(None);

    for ts in TEST_CASES.iter() {
        for (model_name, model) in TEST_MODELS.iter() {
            if crate::llms::TEST_DENY_LIST
                .iter()
                .any(|(m, t)| m == model_name && *t == ts.name)
            {
                tracing::info!("Skipping test {model_name}/{}", ts.name);
                continue;
            }

            run_test_case(ts, model_name, Arc::clone(model))
                .await
                .expect(format!("Failed to run test {model_name}/{}", ts.name).as_str());
        }
    }
}
