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

//! llama.cpp parses GGUF files from the Hugging Face Hub, which Spice does not control. A
//! corrupted or hostile file must fail its load with an error, not take the process down.
//! This feeds seeded mutations of a small valid GGUF to the loader: every one must return
//! an error or load, and the test process must survive all of them.

use std::collections::BTreeSet;
use std::path::Path;

use llama_cpp::{Backend, Model, ModelParams};

/// A deterministic generator, so a failing case can be replayed from its seed.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n.max(1)).unwrap_or(u64::MAX)).unwrap_or(0)
    }
}

fn put_string(bytes: &mut Vec<u8>, text: &str) {
    bytes.extend_from_slice(&(text.len() as u64).to_le_bytes());
    bytes.extend_from_slice(text.as_bytes());
}

/// A GGUF v3 file for a `llama` model with a few hyperparameters and one small F32 tensor.
fn seed_gguf() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"GGUF");
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes()); // tensors
    bytes.extend_from_slice(&4u64.to_le_bytes()); // key-value pairs
    put_string(&mut bytes, "general.architecture");
    bytes.extend_from_slice(&8u32.to_le_bytes());
    put_string(&mut bytes, "llama");
    put_string(&mut bytes, "llama.block_count");
    bytes.extend_from_slice(&4u32.to_le_bytes()); // uint32
    bytes.extend_from_slice(&1u32.to_le_bytes());
    put_string(&mut bytes, "llama.embedding_length");
    bytes.extend_from_slice(&4u32.to_le_bytes());
    bytes.extend_from_slice(&8u32.to_le_bytes());
    put_string(&mut bytes, "general.alignment");
    bytes.extend_from_slice(&4u32.to_le_bytes());
    bytes.extend_from_slice(&32u32.to_le_bytes());
    // Tensor info: name, dimensions, type F32, offset 0.
    put_string(&mut bytes, "token_embd.weight");
    bytes.extend_from_slice(&2u32.to_le_bytes());
    bytes.extend_from_slice(&8u64.to_le_bytes());
    bytes.extend_from_slice(&4u64.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    while !bytes.len().is_multiple_of(32) {
        bytes.push(0);
    }
    bytes.extend(std::iter::repeat_n(0u8, 8 * 4 * 4));
    bytes
}

/// A GGUF metadata value of one of the types the headers below need.
enum Kv<'a> {
    Str(&'a str),
    U32(u32),
    F32(f32),
    Bool(bool),
}

/// A GGUF header of `pairs` of keys and values, and no tensors.
fn gguf_header(pairs: &[(&str, Kv)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"GGUF");
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(pairs.len() as u64).to_le_bytes());
    for (key, value) in pairs {
        put_string(&mut bytes, key);
        match value {
            Kv::Str(text) => {
                bytes.extend_from_slice(&8u32.to_le_bytes());
                put_string(&mut bytes, text);
            }
            Kv::U32(number) => {
                bytes.extend_from_slice(&4u32.to_le_bytes());
                bytes.extend_from_slice(&number.to_le_bytes());
            }
            Kv::F32(number) => {
                bytes.extend_from_slice(&6u32.to_le_bytes());
                bytes.extend_from_slice(&number.to_le_bytes());
            }
            Kv::Bool(flag) => {
                bytes.extend_from_slice(&7u32.to_le_bytes());
                bytes.push(u8::from(*flag));
            }
        }
    }
    bytes
}

/// Regression test for an abort found reading llama.cpp's loader: an unknown classifier
/// activation fails a `GGML_ASSERT`, which aborted the process before the fork made libllama's
/// failed checks throw.
#[test]
fn an_unknown_classifier_activation_fails_the_load_instead_of_aborting() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("classifier.gguf");
    let bytes = gguf_header(&[
        ("general.architecture", Kv::Str("llama")),
        ("tokenizer.ggml.model", Kv::Str("none")),
        ("llama.classifier.activation", Kv::Str("bogus")),
        ("llama.context_length", Kv::U32(64)),
        ("llama.embedding_length", Kv::U32(8)),
        ("llama.block_count", Kv::U32(1)),
    ]);
    std::fs::write(&path, bytes).expect("the file is written");

    let refused = refusal(&path).expect("the file does not load");

    assert!(
        refused.contains("unsupported classifier activation"),
        "the failed check is the reason: {refused}"
    );
}

/// The fork checks DeepSeek-V4's `attention.output_group_count` while loading its
/// hyperparameters, since the attention heads are divided among that many groups: a GGUF that
/// gives 0 fails its load there, and never reaches a division by zero.
#[test]
fn a_zero_output_group_count_fails_a_deepseek_load() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("deepseek41.gguf");
    let bytes = gguf_header(&[
        ("general.architecture", Kv::Str("deepseek41")),
        ("tokenizer.ggml.model", Kv::Str("none")),
        ("deepseek41.context_length", Kv::U32(64)),
        ("deepseek41.embedding_length", Kv::U32(8)),
        ("deepseek41.block_count", Kv::U32(1)),
        ("deepseek41.attention.head_count", Kv::U32(2)),
        ("deepseek41.attention.layer_norm_rms_epsilon", Kv::F32(1e-6)),
        ("deepseek41.attention.q_lora_rank", Kv::U32(8)),
        ("deepseek41.attention.sliding_window", Kv::U32(8)),
        ("deepseek41.expert_feed_forward_length", Kv::U32(8)),
        ("deepseek41.expert_shared_count", Kv::U32(1)),
        ("deepseek41.expert_weights_scale", Kv::F32(1.0)),
        ("deepseek41.expert_weights_norm", Kv::Bool(true)),
        ("deepseek41.swiglu_clamp_exp", Kv::F32(10.0)),
        ("deepseek41.attention.indexer.head_count", Kv::U32(2)),
        ("deepseek41.attention.indexer.key_length", Kv::U32(8)),
        ("deepseek41.attention.indexer.top_k", Kv::U32(4)),
        ("deepseek41.attention.output_group_count", Kv::U32(0)),
        ("deepseek41.attention.output_lora_rank", Kv::U32(8)),
        (
            "deepseek41.attention.compress_rope_freq_base",
            Kv::F32(10_000.0),
        ),
        ("deepseek41.hyper_connection.count", Kv::U32(2)),
        (
            "deepseek41.hyper_connection.sinkhorn_iterations",
            Kv::U32(2),
        ),
        ("deepseek41.hyper_connection.epsilon", Kv::F32(1e-6)),
        ("deepseek41.hash_layer_count", Kv::U32(0)),
    ]);
    std::fs::write(&path, bytes).expect("the file is written");

    let refused = refusal(&path).expect("the file does not load");

    assert_eq!(
        reason(&refused),
        "llama_model_load: error loading model: error loading model hyperparameters: \
         DeepSeek-V4 attention.output_group_count must be > 0",
        "the zero group count is the reason: {refused}"
    );
}

/// One mutation of `bytes`: a flipped byte, a truncation, a run of 0xFF that turns a count or
/// length into a huge one, or a run of random bytes.
fn mutate(bytes: &mut Vec<u8>, rng: &mut XorShift) {
    match rng.below(4) {
        0 => {
            let at = rng.below(bytes.len());
            bytes[at] ^= u8::try_from(rng.below(255) + 1).unwrap_or(1);
        }
        1 => bytes.truncate(rng.below(bytes.len())),
        2 => {
            let at = rng.below(bytes.len());
            let end = (at + 8).min(bytes.len());
            bytes[at..end].fill(0xFF);
        }
        _ => {
            let at = rng.below(bytes.len());
            let end = (at + 1 + rng.below(16)).min(bytes.len());
            for byte in &mut bytes[at..end] {
                *byte = u8::try_from(rng.next() & 0xFF).unwrap_or(0);
            }
        }
    }
}

/// Why llama.cpp refused the file, or `None` when it loaded (and the model is dropped).
fn refusal(path: &Path) -> Option<String> {
    Model::load(
        Backend::get(),
        path,
        &ModelParams::default().with_gpu_layers(0),
    )
    .err()
    .map(|e| e.to_string())
}

/// The check that failed, from a refusal: llama.cpp's first error line, without the path.
fn reason(refusal: &str) -> String {
    let after_path = refusal.split_once("': ").map_or(refusal, |(_, r)| r);
    after_path
        .split(';')
        .next()
        .unwrap_or(after_path)
        .trim()
        .to_string()
}

#[test]
fn a_corrupted_gguf_fails_its_load_without_taking_the_process_down() {
    const CASES: u64 = 400;
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("mutated.gguf");
    let seed = seed_gguf();
    std::fs::write(&path, &seed).expect("the seed is written");
    let unmutated = refusal(&path).expect("the seed is not a complete model, so it does not load");
    assert!(
        unmutated.contains("error loading model hyperparameters"),
        "the seed's GGUF container parses, and llama's own checks refuse it: {unmutated}"
    );
    let mut loaded = 0;
    let mut reasons = BTreeSet::new();
    for case in 0..CASES {
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15 ^ (case + 1));
        let mut bytes = seed.clone();
        for _ in 0..=rng.below(3) {
            mutate(&mut bytes, &mut rng);
        }
        std::fs::write(&path, &bytes).expect("the mutation is written");
        match refusal(&path) {
            Some(refused) => {
                reasons.insert(reason(&refused));
            }
            None => loaded += 1,
        }
    }
    // The seed is not a complete llama model, so a load that succeeds would mean a mutation
    // produced one: none should.
    assert_eq!(loaded, 0, "{loaded} of {CASES} mutations loaded");
    // Mutations that only ever tripped the magic check would test nothing past it.
    assert!(
        reasons.len() >= 5,
        "the mutations reach several of the parser's checks: {reasons:#?}"
    );
    eprintln!("{} distinct refusals: {reasons:#?}", reasons.len());
}
