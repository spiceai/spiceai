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

use std::num::NonZeroU32;
use std::path::PathBuf;

use llama_cpp::{
    Backend, Batch, ContextParams, DraftRequest, Model, ModelParams, Mtp, MtpParams, Sampler, Token,
};
use llms::chat::{LocalModelOptions, ReasoningDefault};
use llms::hf::HubParams;
use serde_json::json;

use crate::{fixture, numbers, request};

/// Unsloth's GGUF of Qwen3.5-0.8B with its MTP layer, at a pinned revision.
const MODEL: &str = "unsloth/Qwen3.5-0.8B-MTP-GGUF";
const REVISION: &str = "cf8a611f6ed2c2060046219a19f12cd3d5ecd67c";
const FILE: &str = "Qwen3.5-0.8B-Q4_K_M.gguf";
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
    let downloaded = llms::chat::hf_files::download_model(
        &format!("{MODEL}:{REVISION}"),
        &[FILE],
        HubParams::default(),
        "qwen3.5",
    )
    .await
    .expect("Qwen3.5-0.8B downloads");
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = fixture::copy_with_tensor(
        &downloaded.join(FILE),
        scratch.path(),
        FUSED_QKV,
        FUSED_QKV_SHAPE,
    );

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

/// Two prompts with different continuations, so a sequence drafting from the other's hidden
/// states would be offered the other's tokens.
const PROMPTS: [&str; 2] = ["The capital of France is", "1, 2, 3, 4, 5,"];
/// The start of each prompt's greedy continuation, so the comparisons compare real answers.
const CONTINUATIONS: [&str; 2] = [" Paris", " 6, 7, 8"];
/// Tokens generated per sequence.
const GENERATED: usize = 24;
/// Tokens drafted per step, as the engine drafts.
const DRAFT: i32 = 2;

async fn qwen35_gguf() -> PathBuf {
    llms::chat::hf_files::download_model(
        &format!("{MODEL}:{REVISION}"),
        &[FILE],
        HubParams::default(),
        "qwen3.5",
    )
    .await
    .expect("Qwen3.5-0.8B downloads")
    .join(FILE)
}

/// One sequence's greedy generation with MTP drafting.
#[derive(Debug, PartialEq)]
struct Drafted {
    tokens: Vec<Token>,
    /// The draft the head offered at each step.
    drafts: Vec<Vec<Token>>,
    /// How many drafted tokens the target accepted.
    accepted: usize,
}

/// Generates [`GENERATED`] tokens greedily for each prompt, each in its own sequence of one
/// context, drafting as the engine does but for every sequence at each step: one pass of the
/// head drafts for all of them, and one target batch verifies every draft.
fn generate(backend: &Backend, model: &Model, prompts: &[Vec<Token>]) -> Vec<Drafted> {
    let n_seq = u32::try_from(prompts.len()).expect("a few sequences");
    let sequences = ContextParams::default()
        .with_n_ctx(NonZeroU32::new(512))
        .with_n_seq_max(n_seq)
        .with_kv_unified(true);
    // A hybrid model's recurrent layers roll back only as many tokens as they keep snapshots of.
    let target = model
        .new_context(
            backend,
            &sequences.with_n_rs_seq(u32::try_from(DRAFT).expect("a short draft")),
        )
        .expect("a context");
    let mut mtp = Mtp::new(
        target,
        model,
        &sequences,
        MtpParams {
            n_max: DRAFT,
            n_min: 0,
            p_min: 0.0,
        },
    )
    .expect("MTP drafting starts");
    let mut greedy = Sampler::greedy();

    // The prompts, each sequence's tokens together; the last of each predicts its first token.
    let mut batch = Batch::new(256);
    let mut last = Vec::new();
    for (seq, prompt) in (0..).zip(prompts) {
        for (pos, &token) in (0..).zip(prompt) {
            let output = usize::try_from(pos).expect("a position") + 1 == prompt.len();
            batch
                .add(token, pos, seq, output)
                .expect("the batch has room");
        }
        last.push(batch.n_tokens() - 1);
    }
    mtp.target_mut().decode(&batch).expect("the prompts decode");
    mtp.process(&batch).expect("the head reads the prompts");
    for (seq, prompt) in (0..).zip(prompts) {
        mtp.begin(seq, prompt).expect("drafting begins");
    }
    let mut next: Vec<Token> = last
        .iter()
        .map(|&index| greedy.sample(mtp.target(), index))
        .collect();
    let mut n_past: Vec<i32> = prompts
        .iter()
        .map(|prompt| i32::try_from(prompt.len()).expect("a short prompt"))
        .collect();
    let mut history = prompts.to_vec();
    let mut out: Vec<Drafted> = prompts
        .iter()
        .map(|_| Drafted {
            tokens: Vec::new(),
            drafts: Vec::new(),
            accepted: 0,
        })
        .collect();

    loop {
        let mut active = Vec::new();
        for (seq, drafted) in out.iter_mut().enumerate() {
            if drafted.tokens.len() < GENERATED {
                drafted.tokens.push(next[seq]);
                if drafted.tokens.len() < GENERATED {
                    active.push(seq);
                }
            }
        }
        if active.is_empty() {
            break;
        }
        let requests: Vec<DraftRequest<'_>> = active
            .iter()
            .map(|&seq| DraftRequest {
                seq: i32::try_from(seq).expect("a sequence"),
                n_past: n_past[seq],
                last: next[seq],
                history: &history[seq],
            })
            .collect();
        let drafts = mtp
            .draft_many(&requests)
            .expect("the head drafts for every sequence");

        // Drafting wrote its positions into the head's cache, and processing the verify batch
        // writes them again, so they go first.
        batch.clear();
        let mut offsets = Vec::new();
        for (&seq, draft) in active.iter().zip(&drafts) {
            let id = i32::try_from(seq).expect("a sequence");
            assert!(
                mtp.draft_mut()
                    .seq_rm(id, n_past[seq], -1)
                    .expect("a known sequence")
            );
            offsets.push(batch.n_tokens());
            let room = GENERATED - out[seq].tokens.len();
            batch
                .add(next[seq], n_past[seq], id, true)
                .expect("the batch has room");
            for (at, &token) in (n_past[seq] + 1..).zip(draft.iter().take(room)) {
                batch.add(token, at, id, true).expect("the batch has room");
            }
        }
        mtp.target_mut()
            .decode(&batch)
            .expect("the verify batch decodes");
        mtp.process(&batch)
            .expect("the head reads the verify batch");

        for ((&seq, draft), offset) in active.iter().zip(drafts).zip(offsets) {
            let id = i32::try_from(seq).expect("a sequence");
            history[seq].push(next[seq]);
            let room = GENERATED - out[seq].tokens.len();
            let mut accepted = 0;
            next[seq] = loop {
                let index = offset + i32::try_from(accepted).expect("a short draft");
                let token = greedy.sample(mtp.target(), index);
                if accepted < draft.len().min(room) && token == draft[accepted] {
                    accepted += 1;
                    out[seq].tokens.push(token);
                    history[seq].push(token);
                } else {
                    break token;
                }
            };
            n_past[seq] += 1 + i32::try_from(accepted).expect("a short draft");
            if !draft.is_empty() {
                mtp.accept(id, u16::try_from(accepted).expect("a short draft"))
                    .expect("the head takes the accepted count");
            }
            out[seq].accepted += accepted;
            out[seq].drafts.push(draft);
            // The rejected drafts leave both caches.
            assert!(
                mtp.target_mut()
                    .seq_rm(id, n_past[seq], -1)
                    .expect("a known sequence")
            );
            assert!(
                mtp.draft_mut()
                    .seq_rm(id, n_past[seq], -1)
                    .expect("a known sequence")
            );
        }
    }
    out
}

/// Two sequences draft together, one pass of the head per step, each from its own hidden
/// states: each is offered the drafts, and generates the tokens, it does drafting alone.
/// Decoding several requests together with MTP drafting rests on this.
#[tokio::test]
async fn two_sequences_drafting_together_each_draft_as_they_do_alone() {
    let path = qwen35_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default().with_load_mtp(true))
        .expect("Qwen3.5-0.8B loads with its MTP layer");
    let vocab = model.vocab();
    let prompts: Vec<Vec<Token>> = PROMPTS
        .iter()
        .map(|prompt| {
            vocab
                .tokenize(prompt, true, false)
                .expect("the prompt tokenizes")
        })
        .collect();
    let text = |tokens: &[Token]| {
        tokens
            .iter()
            .map(|&token| String::from_utf8_lossy(&vocab.token_to_piece(token, false)).into_owned())
            .collect::<String>()
    };

    let alone: Vec<Drafted> = prompts
        .iter()
        .map(|prompt| {
            generate(backend, &model, std::slice::from_ref(prompt))
                .pop()
                .expect("one sequence")
        })
        .collect();
    for (drafted, continuation) in alone.iter().zip(CONTINUATIONS) {
        let generated = text(&drafted.tokens);
        assert!(
            generated.starts_with(continuation),
            "the greedy continuation starts with {continuation:?}: {generated:?}"
        );
        assert!(
            drafted.accepted > 0,
            "the target accepted some of the head's drafts, so the comparison below compares drafting: {drafted:?}"
        );
    }

    let together = generate(backend, &model, &prompts);
    for (seq, (together, alone)) in together.iter().zip(&alone).enumerate() {
        assert_eq!(
            together, alone,
            "sequence {seq} drafting beside the other drafts and generates as it does alone"
        );
    }
}

/// A draft call naming one sequence twice would hand both requests the same draft, so it fails
/// before drafting, and owes no accept.
#[tokio::test]
async fn a_draft_call_naming_a_sequence_twice_fails() {
    let path = qwen35_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default().with_load_mtp(true))
        .expect("Qwen3.5-0.8B loads with its MTP layer");
    let params = ContextParams::default()
        .with_n_ctx(NonZeroU32::new(256))
        .with_n_seq_max(2)
        .with_kv_unified(true);
    let target = model.new_context(backend, &params).expect("a context");
    let mut mtp = Mtp::new(
        target,
        &model,
        &params,
        MtpParams {
            n_max: DRAFT,
            n_min: 0,
            p_min: 0.0,
        },
    )
    .expect("MTP drafting starts");
    let prompt = model
        .vocab()
        .tokenize(PROMPTS[0], true, false)
        .expect("the prompt tokenizes");
    let mut batch = Batch::new(prompt.len());
    for (pos, &token) in (0..).zip(&prompt) {
        batch.add(token, pos, 0, true).expect("the batch has room");
    }
    mtp.target_mut().decode(&batch).expect("the prompt decodes");
    mtp.process(&batch).expect("the head reads the prompt");
    mtp.begin(0, &prompt).expect("drafting begins");
    let n_past = i32::try_from(prompt.len()).expect("a short prompt");
    let last = Sampler::greedy().sample(mtp.target(), n_past - 1);
    let request = DraftRequest {
        seq: 0,
        n_past,
        last,
        history: &prompt,
    };

    let error = mtp
        .draft_many(&[request, request])
        .expect_err("one sequence twice");
    assert_eq!(
        error.to_string(),
        "llama.cpp MTP drafting failed: two draft requests name the same sequence"
    );
    let error = mtp.accept(0, 0).expect_err("no draft is owed an accept");
    assert_eq!(
        error.to_string(),
        "llama.cpp MTP drafting failed: the sequence has no draft to accept"
    );
    assert!(
        !mtp.draft(0, n_past, last, &prompt)
            .expect("one request drafts")
            .is_empty(),
        "the failed call left the sequence able to draft"
    );
}

/// The head caches every token the target does, so a draft context with fewer cells than the
/// target, though each of its sequences is as long, is refused before it runs out of room.
#[tokio::test]
async fn a_draft_context_smaller_than_the_target_is_refused() {
    let path = qwen35_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default().with_load_mtp(true))
        .expect("Qwen3.5-0.8B loads with its MTP layer");
    // A cache per sequence: two of 256 tokens, 512 in all.
    let target = model
        .new_context(
            backend,
            &ContextParams::default()
                .with_n_ctx(NonZeroU32::new(512))
                .with_n_seq_max(2)
                .with_kv_unified(false),
        )
        .expect("a target context");
    // One shared cache of 256 tokens, which each sequence may fill.
    let draft = ContextParams::default()
        .with_n_ctx(NonZeroU32::new(256))
        .with_n_seq_max(2)
        .with_kv_unified(true);
    let error = Mtp::new(
        target,
        &model,
        &draft,
        MtpParams {
            n_max: DRAFT,
            n_min: 0,
            p_min: 0.0,
        },
    )
    .expect_err("a draft context half the target's size");
    assert_eq!(
        error.to_string(),
        "llama.cpp could not start MTP drafting with this model and its draft head: the draft context holds 2 sequences of 256 tokens, 256 in all, and the target 2 of 256, 512 in all"
    );
}
