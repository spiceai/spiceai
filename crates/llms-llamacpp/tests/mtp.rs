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
    Backend, Batch, ContextParams, DraftKind, Model, ModelParams, Mtp, MtpParams, Sampler, Token,
};
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

/// The draft head's state does not outlive the request that left it: a second request, read into
/// a cache emptied first as a finished request's cache is emptied, drafts exactly what the same request drafts on a freshly created context.
/// llama.cpp's MTP head pairs each token with the target's hidden state at the token before it, and
/// the first token of a batch with the state the head kept from the sequence's previous batch
/// (`pending_h`), which on a fresh context is zero.
#[tokio::test]
async fn a_second_request_drafts_as_on_a_fresh_context() {
    let gguf = qwen35_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &gguf, &ModelParams::default().with_load_mtp(true))
        .expect("Qwen3.5-0.8B loads with its MTP layer");
    let chat = |text: &str| {
        model
            .vocab()
            .tokenize(
                &format!("<|im_start|>user\n{text}<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"),
                false,
                true,
            )
            .expect("the prompt tokenizes")
    };
    let first = chat("Name the planets of the solar system in order, one per line.");
    // A stale pairing changes one entry of the head's cache, which moves its logits a little, so
    // whether a draft flips within a request depends on the backend's numerics; across these
    // requests, one does on every backend this has run on when the state outlives the request.
    let seconds = [
        "Write a short paragraph about the history of the bicycle.",
        "List five prime numbers and say why each one is prime.",
        "Translate 'good morning, how are you?' into French, Spanish and German.",
        "Describe how a refrigerator keeps food cold.",
        "Explain what a hash table is to a new programmer.",
        "Give three tips for keeping a houseplant alive.",
        "Summarize the plot of a heist story in four sentences.",
        "Why is the sky blue? Answer in two sentences.",
    ];

    let mut differing = Vec::new();
    for text in seconds {
        let second = chat(text);
        let after_another = {
            let mut spec = drafting(&model, backend);
            let (_, served) = generate(&mut spec, &first, 24);
            // the finished request leaves the cache as a new request finds it: empty
            assert!(
                spec.target_mut().truncate(0) && spec.draft_mut().truncate(0),
                "the cache empties after {} tokens",
                served.len()
            );
            generate(&mut spec, &second, 64)
        };
        let fresh = generate(&mut drafting(&model, backend), &second, 64);

        // the reply is the target's greedy tokens either way, as far as both runs got in their
        // steps
        let shared = after_another.1.len().min(fresh.1.len());
        assert_eq!(
            after_another.1[..shared],
            fresh.1[..shared],
            "the reply to {text:?}"
        );
        // and the drafts, so the steps, must be the same too
        let first_difference = after_another
            .0
            .iter()
            .zip(&fresh.0)
            .position(|(a, b)| a != b);
        eprintln!(
            "{text:?} after another request: {} reply tokens in {} steps; fresh: {} in {}; first draft that differs: {first_difference:?}",
            after_another.1.len(),
            after_another.0.len(),
            fresh.1.len(),
            fresh.0.len()
        );
        if let Some(step) = first_difference {
            differing.push((
                text,
                step,
                after_another.0[step].clone(),
                fresh.0[step].clone(),
            ));
        } else {
            // the same drafts at every step make the same steps, so the whole replies match,
            // including the drafts the final step accepted, which no later step's token follows
            assert_eq!(
                after_another.1, fresh.1,
                "the reply to {text:?}, whose drafts match at every step"
            );
        }
    }
    assert!(
        differing.is_empty(),
        "requests whose drafts after another request differ from a fresh context's (request, step, after another, fresh): {differing:?}"
    );
}

/// A target context and Qwen3.5-0.8B's MTP layer drafting three tokens for it,
/// with as many recurrent state snapshots as a verify batch rolls back over.
fn drafting<'m>(model: &'m Model, backend: &Backend) -> Mtp<'m> {
    let params = ContextParams::default()
        .with_n_ctx(NonZeroU32::new(2048))
        .with_n_batch(512)
        .with_n_rs_seq(3);
    let target = model
        .new_context(backend, &params)
        .expect("a target context");
    Mtp::new(
        target,
        model,
        &ContextParams::default()
            .with_n_ctx(NonZeroU32::new(2048))
            .with_n_batch(512),
        MtpParams {
            kind: DraftKind::Mtp,
            n_max: 3,
            n_min: 0,
            p_min: 0.0,
        },
    )
    .expect("drafting starts")
}

/// Reads `prompt` and generates `steps` verify steps greedily, as spiced's MTP
/// loop does: draft, verify the last token and the draft in one batch, keep the drafts the target
/// would have picked. Returns every draft and the reply.
fn generate(spec: &mut Mtp<'_>, prompt: &[Token], steps: usize) -> (Vec<Vec<Token>>, Vec<Token>) {
    let position = |n: usize| i32::try_from(n).expect("a position");
    let mut batch = Batch::new(512);
    for (pos, &token) in prompt.iter().enumerate() {
        batch
            .add(token, position(pos), pos + 1 == prompt.len())
            .expect("the prompt fits a batch");
    }
    spec.target_mut()
        .decode(&batch)
        .expect("the prompt decodes");
    spec.process(&batch).expect("the head reads the prompt");
    spec.begin(0, prompt).expect("drafting begins");
    let mut sampler = Sampler::greedy();
    let mut history = prompt.to_vec();
    let mut id = sampler.sample(spec.target(), batch.n_tokens() - 1);
    let mut drafts = Vec::new();
    let mut reply = Vec::new();
    for _ in 0..steps {
        let n_past = position(history.len());
        let draft = spec.draft(0, n_past, id, &history).expect("a draft");
        // drafting wrote its positions into the head's cache, and the verify batch writes them again
        assert!(
            spec.draft_mut().truncate(n_past),
            "the head's cache rolls back"
        );
        batch.clear();
        batch.add(id, n_past, true).expect("fits");
        for (i, &token) in draft.iter().enumerate() {
            batch
                .add(token, n_past + 1 + position(i), true)
                .expect("fits");
        }
        spec.target_mut()
            .decode(&batch)
            .expect("the verify batch decodes");
        spec.process(&batch)
            .expect("the head reads the verify batch");
        history.push(id);
        reply.push(id);
        let mut accepted = 0;
        let next = loop {
            let token = sampler.sample(spec.target(), position(accepted));
            if accepted < draft.len() && token == draft[accepted] {
                history.push(token);
                reply.push(token);
                accepted += 1;
            } else {
                break token;
            }
        };
        if !draft.is_empty() {
            spec.accept(0, u16::try_from(accepted).expect("a draft length"))
                .expect("the draft is accepted");
        }
        let kept = position(history.len());
        assert!(
            spec.target_mut().truncate(kept) && spec.draft_mut().truncate(kept),
            "the rejected drafts leave both caches"
        );
        drafts.push(draft);
        id = next;
    }
    assert!(
        drafts.iter().any(|d| !d.is_empty()),
        "at least one draft must be non-empty so the regression guards exercise drafting"
    );
    (drafts, reply)
}

/// A turn that reuses the previous turn's prompt drafts exactly what the same prompt drafts read
/// whole on a freshly created context: the multi-turn shape. As spiced's prompt cache does for a
/// model with recurrent layers, turn 1 is read in two spans split four tokens before its end, the
/// recurrent state saved between them, and turn 2 restores that state, cuts both caches back to
/// the split, and reads only its own tokens from there. The fresh context reads turn 2 in the same
/// two spans, so the target computes the same bits in both, and only the draft head's state at
/// the split can differ: the token there must pair with the hidden state of the token before it,
/// which spiced saves with the checkpoint (`Mtp::state`) and restores with it.
#[tokio::test]
async fn a_reused_prefix_drafts_as_on_a_fresh_context() {
    let gguf = qwen35_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &gguf, &ModelParams::default().with_load_mtp(true))
        .expect("Qwen3.5-0.8B loads with its MTP layer");
    let tokens = |text: &str| {
        model
            .vocab()
            .tokenize(text, false, true)
            .expect("the prompt tokenizes")
    };
    let question = "Name the planets of the solar system in order, one per line.";
    let answer = "Mercury\nVenus\nEarth\nMars\nJupiter\nSaturn\nUranus\nNeptune";
    let turn1 = tokens(&format!(
        "<|im_start|>user\n{question}<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    ));
    let turn2 = tokens(&format!(
        "<|im_start|>user\n{question}<|im_end|>\n<|im_start|>assistant\n{answer}<|im_end|>\n<|im_start|>user\nNow describe each of them in one sentence.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    ));
    // spiced's split: the checkpoint four tokens before the prompt's end, before its generation
    // prompt's empty think block, which the next turn's prompt does not repeat
    let at = turn1.len() - 4;
    assert_eq!(
        turn1[..at],
        turn2[..at],
        "turn 2 repeats turn 1 up to the split"
    );

    let reused = {
        let mut spec = drafting(&model, backend);
        read(&mut spec, &turn1, 0..at);
        let checkpoint = spec
            .target()
            .partial_state()
            .expect("the recurrent state saves");
        let head = spec.state(0).expect("the head's state");
        read(&mut spec, &turn1, at..turn1.len());
        let _ = generate_after_read(&mut spec, &turn1, 24);
        // turn 2: restore the states saved at the split, then cut both caches back to it
        assert!(
            spec.target_mut().restore_partial_state(&checkpoint),
            "the checkpoint restores"
        );
        if let Some(head) = &head {
            spec.restore_state(0, head)
                .expect("the head's state restores");
        }
        assert!(
            spec.target_mut().truncate(position(at)) && spec.draft_mut().truncate(position(at)),
            "both caches cut back to the split"
        );
        read(&mut spec, &turn2, at..turn2.len());
        generate_after_read(&mut spec, &turn2, 48)
    };
    let fresh = {
        let mut spec = drafting(&model, backend);
        read(&mut spec, &turn2, 0..at);
        read(&mut spec, &turn2, at..turn2.len());
        generate_after_read(&mut spec, &turn2, 48)
    };

    let shared = reused.1.len().min(fresh.1.len());
    assert_eq!(reused.1[..shared], fresh.1[..shared], "turn 2's reply");
    let first_difference = reused.0.iter().zip(&fresh.0).position(|(a, b)| a != b);
    eprintln!(
        "turn 2 after turn 1: {} reply tokens in {} steps; read whole on a fresh context: {} in {}; first draft that differs: {first_difference:?}",
        reused.1.len(),
        reused.0.len(),
        fresh.1.len(),
        fresh.0.len()
    );
    assert_eq!(
        first_difference, None,
        "turn 2's drafts after turn 1 {:?}\nread whole on a fresh context {:?}",
        reused.0, fresh.0
    );
    // the same drafts at every step make the same steps, so the whole replies match, including
    // the drafts the final step accepted, which no later step's token follows
    assert_eq!(
        reused.1, fresh.1,
        "turn 2's reply, whose drafts match at every step"
    );
}

fn position(n: usize) -> i32 {
    i32::try_from(n).expect("a position")
}

/// Decodes `prompt[span]` in one batch, the prompt's last token an output, and
/// hands the batch to the draft head, as spiced reads a prompt span.
fn read(spec: &mut Mtp<'_>, prompt: &[Token], span: std::ops::Range<usize>) {
    let mut batch = Batch::new(512);
    for pos in span {
        batch
            .add(prompt[pos], position(pos), pos + 1 == prompt.len())
            .expect("the span fits a batch");
    }
    spec.target_mut().decode(&batch).expect("the span decodes");
    spec.process(&batch).expect("the head reads the span");
}

/// After `prompt` is read whole: `steps` verify steps, as `generate`'s loop.
fn generate_after_read(
    spec: &mut Mtp<'_>,
    prompt: &[Token],
    steps: usize,
) -> (Vec<Vec<Token>>, Vec<Token>) {
    spec.begin(0, prompt).expect("drafting begins");
    let mut sampler = Sampler::greedy();
    let mut history = prompt.to_vec();
    // the last read batch's last row is the prompt's last token
    let mut id = sampler.sample(spec.target(), -1);
    let mut batch = Batch::new(512);
    let mut drafts = Vec::new();
    let mut reply = Vec::new();
    for _ in 0..steps {
        let n_past = position(history.len());
        let draft = spec.draft(0, n_past, id, &history).expect("a draft");
        assert!(
            spec.draft_mut().truncate(n_past),
            "the head's cache rolls back"
        );
        batch.clear();
        batch.add(id, n_past, true).expect("fits");
        for (i, &token) in draft.iter().enumerate() {
            batch
                .add(token, n_past + 1 + position(i), true)
                .expect("fits");
        }
        spec.target_mut()
            .decode(&batch)
            .expect("the verify batch decodes");
        spec.process(&batch)
            .expect("the head reads the verify batch");
        history.push(id);
        reply.push(id);
        let mut accepted = 0;
        let next = loop {
            let token = sampler.sample(spec.target(), position(accepted));
            if accepted < draft.len() && token == draft[accepted] {
                history.push(token);
                reply.push(token);
                accepted += 1;
            } else {
                break token;
            }
        };
        if !draft.is_empty() {
            spec.accept(0, u16::try_from(accepted).expect("a draft length"))
                .expect("the draft is accepted");
        }
        let kept = position(history.len());
        assert!(
            spec.target_mut().truncate(kept) && spec.draft_mut().truncate(kept),
            "the rejected drafts leave both caches"
        );
        drafts.push(draft);
        id = next;
    }
    assert!(
        drafts.iter().any(|d| !d.is_empty()),
        "at least one draft must be non-empty so the regression guards exercise drafting"
    );
    (drafts, reply)
}
