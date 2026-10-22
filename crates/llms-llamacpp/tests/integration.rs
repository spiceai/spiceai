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

//! The llama.cpp engine's integration tests. `load_errors` needs no network and runs in
//! `make nextest`; `model` and `differential` download Qwen3-0.6B (639 MB) from Hugging Face
//! and run in `integration_llms.yml`.

// The engine registers itself through a `linkme` slice, which only a linked crate fills.
use llms_llamacpp as _;

mod differential;
mod load_errors;
mod model;

use std::path::PathBuf;
use std::sync::Arc;

use async_openai::types::chat::CreateChatCompletionRequest;
use llms::chat::{Chat, ReasoningDefault};

/// Qwen's own GGUF of Qwen3-0.6B, at a pinned revision: it reasons and calls tools, and
/// decodes fast enough on a CPU.
pub(crate) const MODEL: &str = "Qwen/Qwen3-0.6B-GGUF";
pub(crate) const REVISION: &str = "23749fefcc72300e3a2ad315e1317431b06b590a";
pub(crate) const FILE: &str = "Qwen3-0.6B-Q8_0.gguf";

/// The model through the runtime's own path: `create_hf_model` downloads the GGUF and hands
/// it to the registered llama.cpp engine.
pub(crate) async fn qwen3() -> Arc<dyn Chat> {
    let file = [PathBuf::from(FILE)];
    assert_eq!(
        llms::chat::engine::engine_for(&file).map(|engine| engine.name),
        Some("llama.cpp"),
        "the llama.cpp engine must be registered, or the model would load on mistral.rs"
    );
    llms::chat::create_hf_model(
        &format!("{MODEL}:{REVISION}"),
        None,
        &file,
        llms::chat::HubParams::default(),
        None,
        None,
        ReasoningDefault {
            model_name: "qwen3-0.6b",
            effort: None,
        },
    )
    .await
    .expect("Qwen3-0.6B loads on llama.cpp")
}

pub(crate) fn request(body: serde_json::Value) -> CreateChatCompletionRequest {
    serde_json::from_value(body).expect("the request is a valid chat completion request")
}
