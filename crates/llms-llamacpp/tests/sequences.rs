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

//! Several sequences in one context, decoded and drafted for together, on Qwen3.5-0.8B with its
//! MTP layer: a hybrid model, so each sequence also carries its own recurrent state.

use std::num::NonZeroU32;

use llama_cpp::{
    Backend, Batch, ContextParams, DraftKind, DraftRequest, Model, ModelParams, Mtp, MtpParams,
    Sampler, Token,
};

use crate::mtp::qwen35_gguf;

/// The most tokens one draft proposes.
const N_DRAFT: usize = 3;

/// One sequence's drafts and reply over a lockstep generation.
type Generation = (Vec<Vec<Token>>, Vec<Token>);

/// A sequence's reply depends only on its own tokens: sequence 0 drafts and replies exactly the
/// same whether sequence 1, decoded in the same batches, holds one prompt or another. Every batch
/// has the same shape in both runs (the two prompts for sequence 1 are the same length, and each
/// sequence's verify rows are padded to a full draft), so the kernels compute the same bits for
/// sequence 0 unless another sequence's tokens reach it, through the attention cache, the
/// recurrent state or the draft head.
#[tokio::test]
async fn a_sequence_replies_the_same_whatever_another_sequence_holds() {
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
    let mut one = chat("Write a short paragraph about the history of the bicycle.");
    let mut other = chat("Explain how a refrigerator keeps food cold, step by step.");
    let shared = one.len().min(other.len());
    one.truncate(shared);
    other.truncate(shared);
    assert_ne!(one, other, "sequence 1's two prompts differ");

    let with_one = generate_together(&model, backend, [&first, &one], 24);
    let with_other = generate_together(&model, backend, [&first, &other], 24);

    assert_ne!(
        with_one[1].1, with_other[1].1,
        "sequence 1 replies to its two prompts differently, so it held different tokens"
    );
    assert!(
        with_one[0].0.iter().any(|draft| !draft.is_empty()),
        "sequence 0 drafted, so the draft head's per-sequence state is exercised"
    );
    assert_eq!(
        with_one[0].0, with_other[0].0,
        "sequence 0's drafts beside two different sequences"
    );
    assert_eq!(
        with_one[0].1, with_other[0].1,
        "sequence 0's reply beside two different sequences"
    );
}

/// The draft head and the context refuse a sequence they do not hold, and a batch whose
/// sequences are interleaved, with an error instead of the assertion that would end the process
/// inside llama.cpp.
#[tokio::test]
async fn a_sequence_outside_the_context_is_refused_not_asserted() {
    let gguf = qwen35_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &gguf, &ModelParams::default().with_load_mtp(true))
        .expect("Qwen3.5-0.8B loads with its MTP layer");
    let mut spec = drafting(&model, backend, 2);
    let prompt = model
        .vocab()
        .tokenize("Hello", false, true)
        .expect("the prompt tokenizes");

    assert_eq!(
        spec.begin(2, &prompt).map_err(|e| e.to_string()),
        Err(
            "llama.cpp MTP drafting failed: the sequence is not one the MTP state drafts for"
                .to_string()
        ),
    );
    assert_eq!(
        spec.accept(1, 0).map_err(|e| e.to_string()),
        Err("llama.cpp MTP drafting failed: the sequence has no draft to accept".to_string()),
    );
    let history = prompt.clone();
    let requests = [
        DraftRequest {
            seq: 0,
            n_past: 1,
            last: prompt[0],
            history: &history,
        },
        DraftRequest {
            seq: 0,
            n_past: 1,
            last: prompt[0],
            history: &history,
        },
    ];
    assert_eq!(
        spec.draft_many(&requests).map_err(|e| e.to_string()),
        Err("llama.cpp MTP drafting failed: two draft requests name the same sequence".to_string()),
    );

    let mut interleaved = Batch::new(8);
    for (pos, seq) in [(0, 0), (0, 1), (1, 0)] {
        interleaved
            .add_seq(prompt[0], pos, seq, true)
            .expect("fits");
    }
    spec.target_mut()
        .decode(&interleaved)
        .expect("the target decodes an interleaved batch");
    assert_eq!(
        spec.process(&interleaved).map_err(|e| e.to_string()),
        Err(
            "llama.cpp MTP drafting failed: each sequence's tokens must be contiguous in the batch"
                .to_string()
        ),
    );

    assert!(
        !spec.target_mut().truncate_seq(2, 0),
        "a sequence the context does not hold is not truncated"
    );
    assert_eq!(
        spec.target().partial_state_seq(2),
        None,
        "a sequence the context does not hold has no state"
    );
}

/// A target context holding `n_seq` sequences on one shared cache, and Qwen3.5-0.8B's MTP layer
/// drafting up to [`N_DRAFT`] tokens for each, with as many recurrent state snapshots per
/// sequence as a verify batch rolls back over.
fn drafting<'m>(model: &'m Model, backend: &Backend, n_seq: u32) -> Mtp<'m> {
    let params = |n_rs_seq: u32| {
        ContextParams::default()
            .with_n_ctx(NonZeroU32::new(2048))
            .with_n_batch(512)
            .with_n_seq_max(n_seq)
            .with_kv_unified(true)
            .with_n_rs_seq(n_rs_seq)
    };
    let target = model
        .new_context(
            backend,
            &params(u32::try_from(N_DRAFT).expect("a draft length")),
        )
        .expect("a target context");
    Mtp::new(
        target,
        model,
        &params(0),
        MtpParams {
            kind: DraftKind::Mtp,
            n_max: i32::try_from(N_DRAFT).expect("a draft length"),
            n_min: 0,
            p_min: 0.0,
        },
    )
    .expect("drafting starts")
}

/// Reads each prompt into its own sequence in one batch, then generates `steps` verify steps
/// greedily for both at once, as a batching engine would: one draft pass for both sequences, one
/// verify batch holding each sequence's last token and its draft, padded to a full draft so the
/// batch's shape never depends on how much a sequence drafted, and each sequence keeping the
/// drafts the target would have picked. Returns each sequence's drafts and reply.
fn generate_together(
    model: &Model,
    backend: &Backend,
    prompts: [&[Token]; 2],
    steps: usize,
) -> [Generation; 2] {
    let position = |n: usize| i32::try_from(n).expect("a position");
    let mut spec = drafting(model, backend, 2);
    let mut batch = Batch::new(512);
    let mut last_row = [0i32; 2];
    for (seq, prompt) in prompts.iter().enumerate() {
        for (pos, &token) in prompt.iter().enumerate() {
            batch
                .add_seq(token, position(pos), position(seq), pos + 1 == prompt.len())
                .expect("the prompts fit a batch");
        }
        last_row[seq] = batch.n_tokens() - 1;
    }
    spec.target_mut()
        .decode(&batch)
        .expect("the prompts decode");
    spec.process(&batch).expect("the head reads the prompts");
    let mut sampler = Sampler::greedy();
    let mut history: [Vec<Token>; 2] = [prompts[0].to_vec(), prompts[1].to_vec()];
    let mut id = [Token(0); 2];
    for seq in 0..2 {
        spec.begin(position(seq), &history[seq])
            .expect("drafting begins");
        id[seq] = sampler.sample(spec.target(), last_row[seq]);
    }
    let mut generations: [Generation; 2] = Default::default();
    for _ in 0..steps {
        let n_past = [position(history[0].len()), position(history[1].len())];
        let drafts = {
            let requests: Vec<DraftRequest<'_>> = (0..2)
                .map(|seq| DraftRequest {
                    seq: position(seq),
                    n_past: n_past[seq],
                    last: id[seq],
                    history: &history[seq],
                })
                .collect();
            spec.draft_many(&requests).expect("the drafts")
        };
        // drafting wrote its positions into the head's cache, and the verify batch writes them again
        for (seq, &past) in n_past.iter().enumerate() {
            assert!(
                spec.draft_mut().truncate_seq(position(seq), past),
                "the head's cache rolls back"
            );
        }
        batch.clear();
        let mut first_row = [0i32; 2];
        for seq in 0..2 {
            first_row[seq] = batch.n_tokens();
            batch
                .add_seq(id[seq], n_past[seq], position(seq), true)
                .expect("fits");
            for i in 0..N_DRAFT {
                // a short draft is padded with its sequence's last token, whose rows are read by
                // nothing and cut back below
                let token = drafts[seq].get(i).copied().unwrap_or(id[seq]);
                batch
                    .add_seq(token, n_past[seq] + 1 + position(i), position(seq), true)
                    .expect("fits");
            }
        }
        spec.target_mut()
            .decode(&batch)
            .expect("the verify batch decodes");
        spec.process(&batch)
            .expect("the head reads the verify batch");
        for seq in 0..2 {
            let draft = &drafts[seq];
            history[seq].push(id[seq]);
            generations[seq].1.push(id[seq]);
            let mut accepted = 0;
            let next = loop {
                let token = sampler.sample(spec.target(), first_row[seq] + position(accepted));
                if accepted < draft.len() && token == draft[accepted] {
                    history[seq].push(token);
                    generations[seq].1.push(token);
                    accepted += 1;
                } else {
                    break token;
                }
            };
            if !draft.is_empty() {
                spec.accept(
                    position(seq),
                    u16::try_from(accepted).expect("a draft length"),
                )
                .expect("the draft is accepted");
            }
            let kept = position(history[seq].len());
            assert!(
                spec.target_mut().truncate_seq(position(seq), kept)
                    && spec.draft_mut().truncate_seq(position(seq), kept),
                "the rejected drafts leave both caches"
            );
            generations[seq].0.push(draft.clone());
            id[seq] = next;
        }
    }
    generations
}
