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

//! A sampler chain that is greedy alone picks from a context's logits row itself, where any
//! other chain hands the row to llama.cpp's `llama_sampler_sample`. Over Qwen3-0.6B's own
//! logits, both must pick the same token.

use std::num::NonZeroU32;

use llama_cpp::{Backend, Batch, ContextParams, Model, ModelParams, Sampler, Token};

use crate::qwen3_gguf;

const PROMPT: &str =
    "<|im_start|>user\nWrite a short poem about the sea.<|im_end|>\n<|im_start|>assistant\n";

/// Tokens of greedy continuation checked after the prompt.
const STEPS: usize = 96;

/// Each row of a prompt decoded with every position output, as an MTP verify batch outputs
/// each drafted position, then the row of each step of a greedy continuation. At each row:
/// - the greedy chain, which reads the row itself, picks what llama.cpp's greedy sampler picks
///   through `llama_sampler_sample`, here behind a logit bias that changes no logit;
/// - a chain that rules that token out picks another, so a chain with anything ahead of its
///   greedy sampler still goes through llama.cpp.
#[tokio::test]
async fn a_greedy_chain_picks_what_llama_cpp_greedy_sampling_picks() {
    let gguf = qwen3_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &gguf, &ModelParams::default()).expect("Qwen3-0.6B loads");
    let vocab = model.vocab();
    let mut context = model
        .new_context(
            backend,
            &ContextParams::default().with_n_ctx(NonZeroU32::new(512)),
        )
        .expect("a context");

    let mut greedy = Sampler::chain([Sampler::greedy()]);
    let mut through_llama_cpp =
        Sampler::chain([Sampler::logit_bias(&vocab, &[]), Sampler::greedy()]);
    let mut check = |context: &llama_cpp::Context<'_>, row: i32, at: &str| -> Token {
        let picked = greedy.sample(context, row);
        assert_eq!(picked, through_llama_cpp.sample(context, row), "{at}");
        let mut ruled_out = Sampler::chain([
            Sampler::logit_bias(&vocab, &[(picked, f32::NEG_INFINITY)]),
            Sampler::greedy(),
        ]);
        assert_ne!(
            picked,
            ruled_out.sample(context, row),
            "{at}: the ruled-out token"
        );
        picked
    };

    let prompt = vocab
        .tokenize(PROMPT, true, true)
        .expect("the prompt tokenizes");
    let mut batch = Batch::new(prompt.len());
    for (pos, &token) in prompt.iter().enumerate() {
        let pos = i32::try_from(pos).expect("a position fits i32");
        batch.add(token, pos, true).expect("the batch has room");
    }
    context.decode(&batch).expect("the prompt decodes");
    let mut picks = Vec::new();
    for row in 0..batch.n_tokens() {
        picks.push(check(&context, row, &format!("prompt row {row}")));
    }

    let mut last = *picks.last().expect("the prompt has a row");
    let mut generated = Vec::new();
    for step in 0..STEPS {
        let pos = i32::try_from(prompt.len() + step).expect("a position fits i32");
        batch.clear();
        batch.add(last, pos, true).expect("the batch has room");
        context.decode(&batch).expect("the continuation decodes");
        last = check(&context, 0, &format!("step {step}"));
        generated.push(last);
    }

    // The rows were the model's own: its continuation varies, rather than one token repeated.
    picks.extend(&generated);
    let mut distinct = picks.iter().map(|t| t.0).collect::<Vec<_>>();
    distinct.sort_unstable();
    distinct.dedup();
    assert!(
        distinct.len() > 40,
        "only {} distinct tokens: {}",
        distinct.len(),
        String::from_utf8_lossy(
            &generated
                .iter()
                .flat_map(|&t| vocab.token_to_piece(t, false))
                .collect::<Vec<_>>()
        )
    );
}
