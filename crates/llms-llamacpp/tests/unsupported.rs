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

//! Architectures llama.cpp loads that this engine cannot serve, refused while they load.

use llms::chat::{Error, LocalModelOptions};

/// A `Q8_0` GGUF of `google/flan-t5-small` (83 MB), an encoder-decoder model, at a pinned
/// revision.
const T5: &str = "cyanic-selkie/flan-t5-small-Q8_0-GGUF";
const T5_REVISION: &str = "4a3fbcbbf5a0f7b2d97790ae1c987b098d64cd54";
const T5_FILE: &str = "flan-t5-small-q8_0.gguf";

/// The GGUF carries no chat template, so the model sets one, as a Spicepod would with
/// `chat_template`. Without the refusal, the model loads with it, and its first request
/// aborts the process in llama.cpp, whose decoder needs the encoder pass this engine never
/// runs.
#[tokio::test]
async fn an_encoder_decoder_model_is_refused_as_an_unsupported_architecture() {
    let dir = llms::chat::hf_files::download_model(
        &format!("{T5}:{T5_REVISION}"),
        &[T5_FILE],
        llms::hf::HubParams::default(),
        "flan-t5-small",
    )
    .await
    .expect("flan-t5-small downloads");
    let weights = [dir.join(T5_FILE)];
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let options = LocalModelOptions {
        chat_template_literal: Some("{% for m in messages %}{{ m.content }}{% endfor %}"),
        ..LocalModelOptions::default()
    };

    let Err(error) = (engine.load)(&weights, options).await else {
        panic!("flan-t5-small loaded");
    };

    assert!(
        matches!(&error, Error::UnsupportedArchitecture { architecture, file }
            if architecture == "t5" && file == T5_FILE),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "Architecture 't5' in flan-t5-small-q8_0.gguf is not supported by this Spice release. Upgrade Spice, or use a GGUF of a supported architecture: https://spiceai.org/docs/components/models#gguf-architectures"
    );
}
