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

//! A decode llama.cpp refuses says why in llama.cpp's own words, on Qwen3-0.6B.

use std::num::NonZeroU32;

use llama_cpp::{Backend, Batch, ContextParams, Model, ModelParams, Token};

use crate::qwen3_gguf;

/// What `llama_decode` logs at error level for a batch whose first token is `i32::MAX`, after
/// the summary of its return value: `llama_batch_allocr::init` names the token, then
/// `llama_context::decode` and `llama_decode` each say they failed.
const REFUSED: &str = "llama.cpp could not decode the batch: the batch is invalid: \
     init: invalid token[0] = 2147483647; decode: failed to initialize batch; \
     llama_decode: failed to decode, ret = -1";

/// A token outside the vocabulary fails the decode with the lines llama.cpp logged about it,
/// each time: a refusal takes its own lines, so the next one does not carry them again, and
/// a decode that succeeds in between adds none.
#[tokio::test]
async fn a_refused_decode_carries_what_llama_cpp_logged() {
    let gguf = qwen3_gguf().await;
    let backend = Backend::get();
    let model = Model::load(backend, &gguf, &ModelParams::default()).expect("Qwen3-0.6B loads");
    let mut context = model
        .new_context(
            backend,
            &ContextParams::default().with_n_ctx(NonZeroU32::new(64)),
        )
        .expect("a context");
    let mut outside = Batch::new(1);
    outside
        .add(Token(i32::MAX), 0, true)
        .expect("the batch has room");

    assert_eq!(
        context.decode(&outside).map_err(|e| e.to_string()),
        Err(REFUSED.to_string()),
        "the first refusal"
    );
    assert_eq!(
        context.decode(&outside).map_err(|e| e.to_string()),
        Err(REFUSED.to_string()),
        "a second refusal carries only its own lines"
    );

    let hello = model
        .vocab()
        .tokenize("Hello", false, true)
        .expect("the prompt tokenizes");
    let mut first = Batch::new(1);
    first.add(hello[0], 0, true).expect("the batch has room");
    context
        .decode(&first)
        .expect("a token of the vocabulary decodes");

    assert_eq!(
        context.decode(&outside).map_err(|e| e.to_string()),
        Err(REFUSED.to_string()),
        "a refusal after a decode that succeeded"
    );
}
