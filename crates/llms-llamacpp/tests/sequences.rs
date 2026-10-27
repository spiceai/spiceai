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

//! The bindings' sequences, on Qwen3-0.6B: a batch carries the tokens of several sequences,
//! each sequence's cache entries stay its own, and two sequences decoded in one batch each get
//! the token that one sequence alone gets, which is what decoding several requests together rests
//! on. A full cache returns its own error, so a caller can free room and decode the batch again,
//! and a sequence call llama.cpp would end the process on returns an error instead.

use std::num::NonZeroU32;
use std::path::PathBuf;

use llama_cpp::{Backend, Batch, ContextParams, Error, Model, ModelParams, Sampler, Token};

use crate::{FILE, MODEL, REVISION};

/// A prompt whose next token is not a near tie, so the batch's shape cannot flip it.
const PROMPT: &str = "The capital of France is";

async fn local_gguf() -> PathBuf {
    llms::chat::hf_files::download_model(
        &format!("{MODEL}:{REVISION}"),
        &[FILE],
        llms::hf::HubParams::default(),
        "qwen3-0.6b",
    )
    .await
    .expect("the GGUF downloads")
    .join(FILE)
}

#[tokio::test]
async fn two_sequences_in_one_batch_each_decode_as_one_sequence_alone() {
    let path = local_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default()).expect("Qwen3-0.6B loads");
    let vocab = model.vocab();
    let prompt = vocab
        .tokenize(PROMPT, true, false)
        .expect("the prompt tokenizes");
    let n = i32::try_from(prompt.len()).expect("a short prompt");
    let last = n - 1;
    let n_ctx = NonZeroU32::new(256);
    let piece =
        |token: Token| String::from_utf8_lossy(&vocab.token_to_piece(token, false)).into_owned();

    // One sequence alone, as the engine decodes today.
    let mut alone = model
        .new_context(backend, &ContextParams::default().with_n_ctx(n_ctx))
        .expect("a context");
    let mut batch = Batch::new(prompt.len() * 2);
    for (pos, &token) in (0..).zip(&prompt) {
        batch
            .add(token, pos, 0, pos == last)
            .expect("the batch has room");
    }
    alone.decode(&batch).expect("the prompt decodes");
    let expected = Sampler::greedy().sample(&alone, last);
    assert_eq!(
        piece(expected),
        " Paris",
        "the prompt's greedy continuation, so the comparison below compares a real answer"
    );

    // The same prompt in sequences 0 and 1 of one batch: sequence 0's tokens, then sequence 1's.
    let mut both = model
        .new_context(
            backend,
            &ContextParams::default()
                .with_n_ctx(n_ctx)
                .with_n_seq_max(2)
                .with_kv_unified(true),
        )
        .expect("a two-sequence context");
    assert_eq!(both.n_seq_max(), 2, "the context holds two sequences");
    batch.clear();
    for seq in 0..2 {
        for (pos, &token) in (0..).zip(&prompt) {
            batch
                .add(token, pos, seq, pos == last)
                .expect("the batch has room");
        }
    }
    both.decode(&batch)
        .expect("both sequences decode in one batch");
    let first = Sampler::greedy().sample(&both, last);
    let second = Sampler::greedy().sample(&both, 2 * n - 1);
    assert_eq!(
        (piece(first), piece(second)),
        (piece(expected), piece(expected)),
        "each sequence's next token is the one a sequence alone gets"
    );

    // Each sequence's cache entries are its own.
    let positions = |context: &llama_cpp::Context<'_>| {
        (
            context.seq_pos_max(0).expect("sequence 0"),
            context.seq_pos_max(1).expect("sequence 1"),
        )
    };
    assert_eq!(
        positions(&both),
        (last, last),
        "each sequence holds the whole prompt"
    );
    assert!(
        both.seq_rm(1, 0, -1).expect("sequence 1"),
        "sequence 1 can be removed whole"
    );
    assert_eq!(
        positions(&both),
        (last, -1),
        "removing sequence 1 leaves sequence 0"
    );
    both.seq_cp(0, 1, 2, -1)
        .expect("one shared cache copies part of a sequence");
    assert_eq!(
        positions(&both),
        (last, last),
        "sequence 1 shares sequence 0's entries from position 2 on"
    );
}

/// Two sequences with a KV cache each: llama.cpp copies between them only whole sequences, and
/// ends the process on a partial copy, so the bindings refuse one with an error.
#[tokio::test]
async fn sequences_with_separate_caches_copy_only_whole() {
    let path = local_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default()).expect("Qwen3-0.6B loads");
    let mut context = model
        .new_context(
            backend,
            &ContextParams::default()
                .with_n_ctx(NonZeroU32::new(512))
                .with_n_seq_max(2)
                .with_kv_unified(false),
        )
        .expect("a context with a cache per sequence");
    assert_eq!(
        (context.n_ctx(), context.n_ctx_seq()),
        (512, 256),
        "each sequence's cache holds half the context"
    );
    let prompt = model
        .vocab()
        .tokenize(PROMPT, true, false)
        .expect("the prompt tokenizes");
    let last = i32::try_from(prompt.len()).expect("a short prompt") - 1;
    let mut batch = Batch::new(prompt.len());
    for (pos, &token) in (0..).zip(&prompt) {
        batch.add(token, pos, 0, false).expect("the batch has room");
    }
    context.decode(&batch).expect("the prompt decodes");

    let error = context
        .seq_cp(0, 1, 2, -1)
        .expect_err("a partial copy between caches");
    assert_eq!(
        error.to_string(),
        "Sequence 0 cannot be copied to sequence 1 in part: each has a KV cache of its own, and llama.cpp copies between caches only whole sequences"
    );
    assert_eq!(
        context.seq_pos_max(1).expect("sequence 1"),
        -1,
        "the refused copy left sequence 1 empty"
    );
    context
        .seq_cp(0, 1, -1, -1)
        .expect("a whole sequence copies between caches");
    assert_eq!(
        context.seq_pos_max(1).expect("sequence 1"),
        last,
        "sequence 1 holds sequence 0's prompt"
    );
}

/// llama.cpp ends the process on a sequence its context does not hold, so the bindings return an
/// error for one instead; -1 still names every sequence.
#[tokio::test]
async fn a_sequence_the_context_does_not_hold_is_an_error() {
    let path = local_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default()).expect("Qwen3-0.6B loads");
    let mut context = model
        .new_context(
            backend,
            &ContextParams::default()
                .with_n_ctx(NonZeroU32::new(512))
                .with_n_seq_max(2),
        )
        .expect("a two-sequence context");
    let unknown = "Sequence 2 is not one of the context's 2 sequences";
    assert_eq!(
        context
            .seq_rm(2, 0, -1)
            .expect_err("sequence 2")
            .to_string(),
        unknown
    );
    assert_eq!(
        context.seq_pos_max(2).expect_err("sequence 2").to_string(),
        unknown
    );
    assert_eq!(
        context
            .seq_cp(0, 2, -1, -1)
            .expect_err("sequence 2")
            .to_string(),
        unknown
    );
    assert_eq!(
        context
            .seq_pos_max(-1)
            .expect_err("sequence -1")
            .to_string(),
        "Sequence -1 is not one of the context's 2 sequences"
    );
    assert!(
        context.seq_rm(-1, 0, -1).expect("every sequence"),
        "-1 removes every sequence"
    );
}

#[tokio::test]
async fn a_batch_the_full_cache_cannot_hold_returns_no_room() {
    let path = local_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &path, &ModelParams::default()).expect("Qwen3-0.6B loads");
    let mut context = model
        .new_context(
            backend,
            &ContextParams::default().with_n_ctx(NonZeroU32::new(256)),
        )
        .expect("a context");
    let n_ctx = i32::try_from(context.n_ctx()).expect("a small context");
    let token = model
        .vocab()
        .tokenize(" the", false, false)
        .expect("a token")[0];

    let mut batch = Batch::new(usize::try_from(n_ctx).expect("a small context"));
    for pos in 0..n_ctx {
        batch.add(token, pos, 0, false).expect("the batch has room");
    }
    context
        .decode(&batch)
        .expect("a context's worth of tokens decodes");
    assert_eq!(
        context.seq_pos_max(0).expect("sequence 0"),
        n_ctx - 1,
        "the cache is full"
    );

    batch.clear();
    batch
        .add(token, n_ctx, 0, true)
        .expect("the batch has room");
    let error = context
        .decode(&batch)
        .expect_err("a full cache has no room for another token");
    assert!(
        matches!(error, Error::NoRoom),
        "a full cache is its own error, not a generic decode failure: {error}"
    );
}
