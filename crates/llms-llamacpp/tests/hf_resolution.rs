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

//! A GGUF repository's files resolved from the repository itself: a quantization by name, and
//! with no `files`, the largest one that fits. unsloth's Qwen3-0.6B repository offers 26 single-file
//! quantizations from 215 MB to 1.2 GB, so each case downloads one file of at most 397 MB, except
//! the load with nothing named, which takes the largest the machine holds (the 1.2 GB `BF16` on
//! any machine with 6 GiB free).

use llms::chat::hf_files::{Budget, Downloaded, download_model};
use llms::chat::{Chat, ReasoningDefault, create_hf_model};
use llms::hf::HubParams;
use serde_json::json;

use crate::{numbers, request};

/// unsloth's GGUFs of Qwen3-0.6B, one file per quantization, at a pinned revision.
const REPO: &str = "unsloth/Qwen3-0.6B-GGUF:50968a4468ef4233ed78cd7c3de230dd1d61a56b";

/// Memory whose weights budget (a tenth and 4 GiB left for the context) is 400 MB: between
/// `Q4_K_M` (397 MB) and the next larger, `UD-Q4_K_XL` (405 MB).
const MEMORY_FOR_400_MB: u64 = ((4 << 30) + 400_000_000) * 10 / 9;

async fn resolve(files: &[&str], budget: Budget) -> Result<Downloaded, String> {
    download_model(REPO, files, budget, HubParams::default(), "qwen3-0.6b-hf")
        .await
        .map_err(|error| error.to_string())
}

async fn twelve_times_twelve(model: &dyn Chat) -> Vec<u32> {
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3-0.6b-hf",
            "messages": [{ "role": "user", "content": "What is 12 times 12? Reply with only the number." }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 16,
        })))
        .await
        .expect("a reply");
    numbers(&reply.choices[0].message.content.clone().expect("content"))
}

async fn load(files: &[&str]) -> std::sync::Arc<dyn Chat> {
    create_hf_model(
        REPO,
        None,
        files,
        HubParams::default(),
        None,
        None,
        ReasoningDefault {
            model_name: "qwen3-0.6b-hf",
            effort: None,
        },
    )
    .await
    .expect("the model loads")
}

/// A quantization named by its tag is that quantization's file, and the model loads and answers
/// from it as from the file named exactly.
#[tokio::test]
async fn a_quantization_by_name_is_its_file() {
    let by_name = resolve(&["Q4_K_M"], Budget::Unmeasured)
        .await
        .expect("Q4_K_M resolves");
    assert_eq!(by_name.gguf, vec!["Qwen3-0.6B-Q4_K_M.gguf"]);
    let by_file = resolve(&["Qwen3-0.6B-Q4_K_M.gguf"], Budget::Unmeasured)
        .await
        .expect("the file resolves");
    assert_eq!(by_file, by_name);

    let named = load(&["q4_k_m"]).await;
    assert_eq!(twelve_times_twelve(named.as_ref()).await, vec![144]);
}

/// With nothing named, the largest quantization whose weights fit the memory is chosen: here a
/// budget just under `UD-Q4_K_XL` takes `Q4_K_M`.
#[tokio::test]
async fn with_nothing_named_the_largest_that_fits_is_chosen() {
    let chosen = resolve(&[], Budget::Free(MEMORY_FOR_400_MB))
        .await
        .expect("a quantization fits");
    assert_eq!(chosen.gguf, vec!["Qwen3-0.6B-Q4_K_M.gguf"]);
}

/// What resolution cannot decide or find is named in the error, with the fix.
#[tokio::test]
async fn what_cannot_be_chosen_says_how_to_choose() {
    let unmeasured = resolve(&[], Budget::Unmeasured)
        .await
        .expect_err("no memory to choose by");
    assert!(
        unmeasured.contains("it offers 26 GGUF quantizations (BF16 (1.2 GB), UD-Q8_K_XL (0.8 GB), Q8_0 (0.6 GB),")
            && unmeasured.contains(
                "this build cannot measure the memory to choose one. Name one in the model's `files`, as `files: [{path: UD-IQ1_S}]`"
            ),
        "{unmeasured}"
    );
    let split = resolve(&[], Budget::Split)
        .await
        .expect_err("a split model names its quantization");
    assert!(
        split.contains(
            "and a model split across machines names its quantization, so that every machine loads the same weights. Name one in the model's `files`, as `files: [{path: UD-IQ1_S}]`"
        ),
        "{split}"
    );
    let too_small = resolve(&[], Budget::Free(4 << 30))
        .await
        .expect_err("nothing fits");
    assert!(
        too_small.contains("none of its GGUF quantizations fits the 4.0 GiB of memory free for the model with room for the context; the smallest, UD-IQ1_S, is 0.2 GB. Name one in the model's `files` to load it anyway, across machines with `nodes` if one cannot hold it:"),
        "{too_small}"
    );
    let unknown = resolve(&["Q9_K"], Budget::Unmeasured)
        .await
        .expect_err("no such quantization");
    assert!(
        unknown.contains("the repository has no file or quantization 'Q9_K'; its quantizations are BF16, UD-Q8_K_XL, Q8_0,"),
        "{unknown}"
    );
}

/// Nothing named on this machine: the engine's measured memory picks the quantization, and the
/// model it loads answers.
#[tokio::test]
async fn with_nothing_named_the_model_loads_on_this_machine() {
    let memory = llms::chat::engine::free_memory().expect("the llama.cpp engine measures memory");
    let chosen = resolve(&[], Budget::Free(memory))
        .await
        .expect("a quantization fits");
    assert_eq!(chosen.gguf.len(), 1, "{chosen:?}");
    let model = load(&[]).await;
    // A larger quantization may answer in a sentence ("12 times 12 is 144"): its last number
    // is the answer.
    assert_eq!(twelve_times_twelve(model.as_ref()).await.last(), Some(&144));
}
