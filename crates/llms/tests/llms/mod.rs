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
use async_openai::types::{CreateChatCompletionRequest, CreateChatCompletionResponse};
use jsonpath_rust::JsonPath;
use lazy_static::lazy_static;
use llms::chat::Chat;
use serde_json::json;
use std::{str::FromStr, sync::Arc};

mod anthropic;

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

lazy_static! {
    /// Test case parameters (for [`run_test_case`]) to run for each model.
    static ref TEST_CASES: Vec<TestCase> = vec![
        test_case!("basic", json!({
            "model": "not_needed",
            "messages": [
                {
                    "role": "user",
                    "content": "Say Hi"
                }
            ]
        }), vec![
            ("message_keys", "$.choices[*].message['role', 'tool_calls', 'refusal', 'function_calls']"),
            ("replied_appropriately", "$.choices[*].message[?(@.content ~= 'Hi')].length()")
        ]),
    ];

    /// Model instantiations to test.
    static ref TEST_MODELS: Vec<(&'static str, Arc<dyn Chat>)> =
        vec![("anthropic", anthropic::create_chat(None).expect("failed to create anthropic model"))];

    /// A mapping of model names (in [`TEST_MODELS`]) and test names (in [`TEST_CASES`]) to skip.
    static ref TEST_DENY_LIST: Vec<(&'static str, &'static str)> = vec![("anthropic", "advanced")];
}

/// Run a single [`TestCase`] for a model.
#[allow(clippy::expect_used, clippy::expect_fun_call)]
async fn run_test_case(
    test: &TestCase,
    model_name: &'static str,
    model: Arc<dyn Chat>,
) -> Result<(), anyhow::Error> {
    let test_name = test.name;
    println!("Running test {test_name}/{model_name} with {:?}", test.req);

    let actual_resp = model
        .chat_request(test.req.clone())
        .await
        .expect(format!("For test {test_name}/{model_name}, chat_request failed").as_str());

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
