use std::sync::Arc;

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
use lazy_static::lazy_static;
use llms::chat::Chat;
use paste::paste;
use serde_json::json;

mod anthropic;

#[derive(Clone)]
pub struct TestCase {
    pub name: &'static str,
    pub req: CreateChatCompletionRequest,
    pub res: CreateChatCompletionResponse,
}
/// Creates [`TestCase`] instances from request/response that JSON serialize to
/// [`CreateChatCompletionRequest`] and [`CreateChatCompletionResponse`].
#[macro_export]
macro_rules! test_case {
    ($name:expr, $req:expr, $res:expr) => {
        TestCase {
            name: $name,
            req: serde_json::from_value($req)
                .expect(&format!("Failed to parse request in test case '{}'", $name)),
            res: serde_json::from_value($res).expect(&format!(
                "Failed to parse response in test case '{}'",
                $name
            )),
        }
    };
}

// You could also make a more explicit version that lets you specify the sources:
#[macro_export]
macro_rules! generate_model_tests_from {
    ($cases:expr, $models:expr, $deny_list:expr) => {
        paste::paste! {
            // Generate individual tests for each combination
            for test_case in $cases.iter() {
                for (model_name, model) in $models.iter() {
                    #[tokio::test]
                    async fn [<test_ $model_name _ $test_case.name>]() -> Result<(), Error> {
                        if !$deny_list.iter().any(|(m, t)| *m == model_name && *t == test_case.name) {
                            run_test_case(
                                test_case.name,
                                test_case.clone(),
                                model_name,
                                model.clone()
                            ).await?;
                            Ok(())
                        } else {
                            println!("Test {}/{} skipped (in deny list)", model_name, test_case.name);
                            Ok(())
                        }
                    }
                }
            }
        }
    };
}

lazy_static! {
    /// Test case parameters (for [`run_test_case`]) to run for each model.
    static ref TEST_CASES: Vec<TestCase> = vec![
        test_case!("basic", json!({"x": 1}), json!({"y": 2})),
        test_case!("advanced", json!({"x": 2}), json!({"y": 3})),
    ];

    /// Model instantiations to test.
    static ref TEST_MODELS: Vec<(&'static str, Arc<dyn Chat>)> =
        vec![("anthropic", anthropic::create_chat(None).expect("failed to create anthropic model"))];

    /// A mapping of model names (in [`TEST_MODELS`]) and test names (in [`TEST_CASES`]) to skip.
    static ref TEST_DENY_LIST: Vec<(&'static str, &'static str)> = vec![("anthropic", "advanced")];
}

/// Run a single [`TestCase`] for a model.
fn run_test_case(
    test_name: &'static str,
    test: TestCase,
    model_name: &'static str,
    model: Arc<dyn Chat>,
) -> Result<(), anyhow::Error> {
    println!(
        "Running test {}/{}. Request: {:?}, Expected response: {:?}",
        model_name, test_name, test.req, test.res
    );
    Ok(())
}

// Usage with explicit sources:
#[cfg(test)]
mod tests {

    generate_model_tests_from!(TEST_CASES, TEST_MODELS, TEST_DENY_LIST);
}
