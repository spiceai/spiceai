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

#![expect(
    clippy::expect_used,
    reason = "set-up and helpers outside `#[test]` functions fail fast with `expect`"
)]

pub mod llms;

// Credential-free tests that run in `make nextest`; `llms` calls live provider
// APIs and runs in `integration_llms.yml`.
mod anthropic_stream_errors;
#[cfg(feature = "local_llm")]
mod gguf_hf_cache;
mod list_models_errors;
#[cfg(feature = "local_embed")]
mod model2vec_hf_cache;
#[cfg(feature = "local_llm")]
mod safetensors_hf_cache;
mod xai_errors;

use std::{collections::HashSet, sync::LazyLock};

use tracing_subscriber::EnvFilter;

static TEST_ARGS: LazyLock<TestArgs> = LazyLock::new(|| {
    let args = TestArgs::from_env();
    args.validate();
    args
});

#[derive(Debug)]
struct TestArgs {
    // Model names to skip from testing.
    model_skiplist: Option<Vec<String>>,

    /// Models to test. If provided, only these models will be tested.
    model_allow_list: Option<Vec<String>>,
}

impl TestArgs {
    fn from_env() -> Self {
        let model_skiplist: Option<Vec<String>> = std::env::var("MODEL_SKIPLIST")
            .ok()
            .map(|s| s.split(',').map(ToString::to_string).collect());

        let model_allow_list: Option<Vec<String>> = std::env::var("MODEL_ALLOWLIST")
            .ok()
            .map(|s| s.split(',').map(ToString::to_string).collect());

        TestArgs {
            model_skiplist,
            model_allow_list,
        }
    }

    fn skip_model(&self, model_name: &str) -> bool {
        // If allow list set, check if model is in it.
        if let Some(ref allow_list) = self.model_allow_list {
            return !allow_list.contains(&model_name.to_string());
        }

        // If deny list set, check if model is not in it.
        self.model_skiplist
            .as_ref()
            .is_some_and(|skip_list| skip_list.contains(&model_name.to_string()))
    }

    fn validate(&self) {
        let skip: HashSet<_> = self.model_skiplist.iter().collect();
        let allow: HashSet<_> = self.model_allow_list.iter().collect();
        let overlap = skip.intersection(&allow);

        if overlap.clone().count() > 0 {
            tracing::warn!("Model allowlist and skiplist have overlapping models: {overlap:?}");
        }
    }
}

fn init_tracing(default_level: Option<&str>) -> tracing::subscriber::DefaultGuard {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new("llms=TRACE,DEBUG"),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_default(subscriber)
}

/// Fails the calling test unless it runs in a process of its own.
///
/// nextest runs every test in its own process; plain `cargo test` runs a
/// binary's tests on threads of one process. A test that changes process-global
/// state calls this before changing it, so under `cargo test` it fails with
/// instructions instead of changing the state every neighbouring test runs under.
/// `why` names that state.
fn require_process_per_test(why: &str) {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "{why}, so it must run in a process of its own. Run it with \
         `cargo nextest run -p llms --test integration <test name>`: plain \
         `cargo test` runs every test in the binary on threads of one process"
    );
}
