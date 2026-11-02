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

//! The same GGUF and prompts through the llama.cpp and mistral.rs engines, greedy. The two
//! engines share prompt rendering, so they must give the same reply to a question with one
//! right answer, and that reply must hold it; their arithmetic differs, so long free text
//! may diverge at a near-tie.

use std::path::PathBuf;
use std::sync::Arc;

use llms::chat::mistral::MistralLlama;
use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};
use serde_json::json;

use crate::{FILE, MODEL, REVISION, request};

/// The GGUF in the Hugging Face cache, downloaded the way `create_hf_model` downloads it,
/// so it waits for, rather than races, another test's download of the same file.
async fn local_gguf() -> PathBuf {
    llms::chat::hf_files::download_model(
        &format!("{MODEL}:{REVISION}"),
        &[FILE],
        llms::chat::hf_files::Budget::Unmeasured,
        llms::hf::HubParams::default(),
        "qwen3-0.6b",
    )
    .await
    .expect("the GGUF downloads")
    .dir
    .join(FILE)
}

/// Questions with one right answer, and that answer, which a reply must end with.
const CORPUS: &[(&str, &str)] = &[
    (
        "What is the capital of France? Reply with one word.",
        "Paris",
    ),
    ("What is 12 times 12? Reply with only the number.", "144"),
    ("What is 2 plus 3? Reply with only the number.", "5"),
    ("What is the opposite of hot? Reply with one word.", "Cold"),
    (
        "Which planet is closest to the Sun? Reply with one word.",
        "Mercury",
    ),
];

async fn answer(model: &dyn Chat, prompt: &str) -> String {
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": prompt }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 16,
        })))
        .await
        .expect("a reply");
    reply.choices[0]
        .message
        .content
        .clone()
        .expect("content")
        .trim()
        .trim_end_matches('.')
        .to_string()
}

/// The reply's last word: `2 + 3 = 5` answers `5`, and `15` does not.
fn last_word(reply: &str) -> &str {
    reply
        .split(|c: char| !c.is_alphanumeric())
        .rfind(|word| !word.is_empty())
        .unwrap_or_default()
}

#[tokio::test]
async fn both_engines_answer_the_corpus_alike_and_right() {
    let path = local_gguf().await;
    let weights = [path];
    let options = LocalModelOptions {
        reasoning: ReasoningDefault {
            model_name: "qwen3-0.6b",
            effort: None,
        },
        ..LocalModelOptions::default()
    };
    let engine = llms::chat::engine::engine_for(&weights).expect("a .gguf goes to llama.cpp");
    let llama: Arc<dyn Chat> = (engine.load)(&weights, options)
        .await
        .expect("llama.cpp loads it");
    let mistral = MistralLlama::from(&weights, None, None, None, None, options, None)
        .await
        .expect("mistral.rs loads it");

    for &(prompt, expected) in CORPUS {
        let (from_llama, from_mistral) = (
            answer(&*llama, prompt).await,
            answer(&mistral, prompt).await,
        );
        assert_eq!(
            from_llama, from_mistral,
            "the engines' answers to {prompt:?}"
        );
        assert_eq!(
            last_word(&from_llama),
            expected,
            "{from_llama:?} answers {prompt:?}"
        );
    }
}
