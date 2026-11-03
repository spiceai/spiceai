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

//! Every provider that answers decisions, held to the `evaluate-api` conformance suite.
//!
//! Chat models answer through `ChatEvaluator`; `OpenAI` decision models and `TypeSafe` Jev
//! have native decisions APIs.

use std::sync::{Arc, LazyLock};

use evaluate_api::{Evaluate, conformance};
use evaluate_chat::ChatEvaluator;
use llms::{openai::decisions::OpenAiDecisions, typesafe::TypeSafe};
use rstest::rstest;

use super::{DOTENV, TEST_ARGS, get_or_create_model, on_shared_runtime};
use crate::init_tracing;

/// Fixture name `MODEL_ALLOWLIST` and `MODEL_SKIPLIST` select the `OpenAI` decision model by.
const OPENAI_DECISIONS: &str = "openai_decisions";
const OPENAI_DECISION_MODEL: &str = "gpt-6-luna";
const OPENAI_ENDPOINT: &str = "https://api.openai.com/v1";

/// Fixture name the `TypeSafe` Jev model is selected by.
const TYPESAFE: &str = "typesafe";

/// Runs the conformance suite on `model` on the shared runtime, which also serves the
/// provider clients' connection pools, and fails the test with every failed case.
async fn assert_conforms(name: &str, model: Arc<dyn Evaluate>) {
    let label = name.to_string();
    let failures =
        on_shared_runtime(async move { conformance::run_all(model.as_ref(), &label).await }).await;
    assert!(
        failures.is_empty(),
        "{name} breaks the evaluate contract:\n{}",
        conformance::report(&failures)
    );
}

fn required_env(name: &str, variable: &str) -> String {
    std::env::var(variable).unwrap_or_else(|_| panic!("{name} needs `{variable}` to be set"))
}

/// The hosted chat models. The local models are left out: a small model does not follow a
/// structured-output instruction reliably enough to hold to an exact answer.
#[rstest]
#[tokio::test]
async fn chat_model_answers_decisions(
    #[values("anthropic", "openai", "xai", "bedrock", "google")] model_name: &str,
) {
    LazyLock::force(&DOTENV);
    let _tracing_guard = init_tracing(None);
    if TEST_ARGS.skip_model(model_name) {
        return;
    }

    let chat = on_shared_runtime({
        let model_name = model_name.to_string();
        async move { get_or_create_model(&model_name).await }
    })
    .await
    .unwrap_or_else(|e| panic!("failed to get or create model {model_name}: {e}"));

    assert_conforms(model_name, Arc::new(ChatEvaluator::new(model_name, chat))).await;
}

#[tokio::test]
async fn openai_decision_model_conforms() {
    LazyLock::force(&DOTENV);
    let _tracing_guard = init_tracing(None);
    if TEST_ARGS.skip_model(OPENAI_DECISIONS) {
        return;
    }

    let model = OpenAiDecisions::try_new(
        OPENAI_DECISIONS,
        OPENAI_DECISION_MODEL,
        OPENAI_ENDPOINT,
        Some(required_env(OPENAI_DECISIONS, "SPICE_OPENAI_API_KEY")),
    )
    .unwrap_or_else(|e| panic!("failed to create {OPENAI_DECISIONS}: {e}"));

    assert_conforms(OPENAI_DECISIONS, Arc::new(model)).await;
}

#[tokio::test]
async fn typesafe_jev_conforms() {
    LazyLock::force(&DOTENV);
    let _tracing_guard = init_tracing(None);
    if TEST_ARGS.skip_model(TYPESAFE) {
        return;
    }

    let model = TypeSafe::try_new(TYPESAFE, None, required_env(TYPESAFE, "TYPESAFE_API_KEY"))
        .unwrap_or_else(|e| panic!("failed to create {TYPESAFE}: {e}"));

    assert_conforms(TYPESAFE, Arc::new(model)).await;
}
