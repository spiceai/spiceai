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

//! Qwen3.5-0.8B, whose GGUF carries an MTP (`NextN`) layer the model drafts its own tokens with.

use std::path::PathBuf;

use llama_cpp::{Backend, Model, ModelParams};
use llms::chat::{LocalModelOptions, ReasoningDefault};
use llms::hf::HubParams;
use serde_json::json;

use crate::{fixture, numbers, request};

/// Unsloth's GGUF of Qwen3.5-0.8B with its MTP layer, at a pinned revision.
const MODEL: &str = "unsloth/Qwen3.5-0.8B-MTP-GGUF";
const REVISION: &str = "cf8a611f6ed2c2060046219a19f12cd3d5ecd67c";
const FILE: &str = "Qwen3.5-0.8B-Q4_K_M.gguf";

/// The downloaded Qwen3.5-0.8B GGUF, MTP layer included.
pub(crate) async fn qwen35_gguf() -> PathBuf {
    llms::chat::hf_files::download_model(
        &format!("{MODEL}:{REVISION}"),
        &[FILE],
        llms::chat::hf_files::Budget::Unmeasured,
        HubParams::default(),
        "qwen3.5",
    )
    .await
    .expect("Qwen3.5-0.8B downloads")
    .dir
    .join(FILE)
}

/// A fused query, key and value projection for the MTP layer, in its shape: the 1024-wide
/// embedding projected to the 4096-wide gated query and the 512-wide key and value.
const FUSED_QKV: &str = "blk.24.attn_qkv.weight";
const FUSED_QKV_SHAPE: [u64; 2] = [1024, 4096 + 512 + 512];

/// A model whose MTP layer does not load still loads without it, and decodes without drafting.
/// Here the layer fails because the copy adds a fused QKV projection to it beside its separate
/// ones: llama.cpp loading the layer takes the fused one and leaves the separate ones unread,
/// which it refuses, while loading without the layer skips all of them. It stands in for a
/// layer that no longer fits in memory.
#[tokio::test]
async fn a_model_whose_mtp_layer_does_not_load_decodes_without_drafting() {
    let downloaded = qwen35_gguf().await;
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = fixture::copy_with_tensor(&downloaded, scratch.path(), FUSED_QKV, FUSED_QKV_SHAPE);

    // llama.cpp refuses the copy with its MTP layer, loads it without, and still reports the
    // layer from the file's header.
    let backend = Backend::get();
    let Err(with_mtp) = Model::load(backend, &gguf, &ModelParams::default().with_load_mtp(true))
    else {
        panic!("the MTP layer should not load");
    };
    assert!(
        with_mtp
            .to_string()
            .contains("done_getting_tensors: wrong number of tensors; expected 336, got 333"),
        "{with_mtp}"
    );
    let plain = Model::load(backend, &gguf, &ModelParams::default()).expect("loads without MTP");
    assert_eq!(plain.n_layer_nextn(), 1);
    drop(plain);

    let path = gguf.to_str().expect("a UTF-8 path").to_string();
    let model = llms::chat::create_local_model(
        &[path],
        None,
        None,
        None,
        None,
        None,
        LocalModelOptions {
            reasoning: ReasoningDefault {
                model_name: "qwen3.5",
                effort: None,
            },
            ..LocalModelOptions::default()
        },
    )
    .await
    .expect("the model loads without its MTP layer");
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3.5",
            "messages": [{ "role": "user", "content": "Count from 1 to 10. Reply with only the numbers, separated by commas." }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 64,
        })))
        .await
        .expect("a reply");
    let content = reply.choices[0].message.content.clone().expect("content");
    assert_eq!(numbers(&content), (1..=10).collect::<Vec<_>>(), "{content}");
}
