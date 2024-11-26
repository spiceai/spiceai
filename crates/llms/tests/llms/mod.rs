/*
Copyright 2024 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0
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
        // ... other test cases ...
    ]
});

// Macro to create test module and functions
#[macro_export]
macro_rules! generate_model_tests {
    () => {
        mod model_tests {
            use super::*;

            // Generate a test function for each model/test combination
            macro_rules! test_model_case {
                ($model_name_expr:expr, $test_case_expr:expr) => {
                    paste::paste! {
                        #[tokio::test]
                        async fn [<test_ $model_name_expr _ $test_case_expr>]() {
                            let model_name = stringify!($model_name_expr);
                            let test_case = stringify!($test_case_expr);
                            println!("Running test {}/{}", model_name, test_case);

                            let _ = dotenvy::from_filename(".env").expect("failed to load .env file");
                            init_tracing(None);

                            if TEST_DENY_LIST
                                .iter()
                                .any(|(m, t)| *m == model_name && *t == test_case)
                            {
                                return;
                            }

                            // Get test case
                            let test = TEST_CASES
                                .iter()
                                .find(|t| t.name == test_case)
                                .expect("test case not found");

                            let (_, model) = TEST_MODELS
                                .iter()
                                .find(|(name, _)| *name == model_name)
                                .expect("model not found");

                            // Run test
                            run_single_test(test, model_name, Arc::clone(model)).await
                                .expect("test failed");
                        }
                    }
                };
            }

            test_model_case!(anthropic, basic);
            test_model_case!(openai, basic);
            // test_model_case!(hf_phi3, basic);
            // test_model_case!(local_phi3, basic);
        }

        async fn run_single_test(
            test: &TestCase,
            model_name: &str,
            model: Arc<Box<dyn Chat>>,
        ) -> Result<(), anyhow::Error> {
            tracing::info!(
                "Running test {}/{} with {:?}",
                test.name,
                model_name,
                test.req
            );

            let actual_resp = model.chat_request(test.req.clone()).await.expect(&format!(
                "For test {}/{}, chat_request failed",
                test.name, model_name
            ));

            let resp_value =
                serde_json::to_value(&actual_resp).expect("failed to serialize response to JSON");

            for (id, json_ptr) in &test.json_path {
                let resp_ptr = JsonPath::from_str(json_ptr)
                    .expect("invalid JSONPath selector")
                    .find(&resp_value);
                insta::assert_snapshot!(
                    format!("{}_{model_name}_{id}", test.name),
                    serde_json::to_string_pretty(&resp_ptr).expect("Failed to serialize snapshot")
                );
            }
            Ok(())
        }
    };
}

generate_model_tests!();

// #[cfg(test)]
// mod tests {
//     use super::*;
// }
