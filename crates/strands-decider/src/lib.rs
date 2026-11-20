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

//! The readout of Strands decider models (`strands-labs/strands-decider`): decision models
//! that answer typed questions (noul, choice, score) about a state with calibrated
//! probabilities. A checkpoint is a `LoRA` adapter on a base decoder plus a small pointer head.
//!
//! ```text
//! state + question ──render──► prompt text ──tokenize──► tokens
//!                                                          │
//!            decoder + LoRA (a [`HiddenStates`] backend) ◄─┘
//!                          │ final hidden states at each option's last token and at <answer>
//!                          ▼
//!            PointerHead ──► one logit per option ──softmax / temperature──► Answer
//! ```
//!
//! This crate ports upstream's `prompting.py`, `schema.py` and the readout of `infer.py` and
//! `hidden_engine.py`. It runs the head; a backend runs the decoder. With the `llama` feature,
//! [`llama::LlamaHiddenStates`] runs it on llama.cpp.
#![expect(
    clippy::missing_errors_doc,
    reason = "every fallible call returns this crate's `Error`, whose variants say what failed"
)]

mod config;
mod engine;
mod head;
#[cfg(feature = "llama")]
pub mod llama;
mod prompting;
mod pyjson;
mod schema;
mod tokenizer;

use std::path::PathBuf;

use snafu::Snafu;

pub use config::{CONFIG_NAME, DeciderConfig, LEGACY_CONFIG_NAME};
pub use engine::{BoxError, Decider, Evaluation, HiddenRows, HiddenStates};
pub use head::{PointerHead, softmax};
pub use prompting::{
    RenderedQuestion, option_token_index, render_content, render_question, render_state, round4,
    to_answer,
};
pub use schema::{
    Answer, ChoiceOptions, Content, Kind, MAX_CHOICE_OPTIONS, MAX_SCORE_LEVELS, MIN_SCORE_LEVELS,
    Meaning, NoulCriteria, Question, ScoreLevels, derive_confidence, derive_score_confidence,
};
pub use tokenizer::{DeciderTokenizer, Fitted, bos_token_from_config};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid question: {message}"))]
    InvalidQuestion { message: String },

    #[snafu(display("A request needs at least one question"))]
    NoQuestions,

    #[snafu(display(
        "The prompt of {} tokens ({state} state + {question} question) exceeds the model's window of {max_length} tokens",
        state + question
    ))]
    PromptTooLong {
        state: usize,
        question: usize,
        max_length: usize,
    },

    #[snafu(display(
        "The option at bytes {start}..{end} of its question has no tokens; the prompt was truncated through its option list"
    ))]
    OptionTruncated { start: usize, end: usize },

    #[snafu(display("The tokenizer failed: {message}: {source}"))]
    Tokenizer {
        message: String,
        source: tokenizers::Error,
    },

    #[snafu(display("The tokenizer cannot add its BOS token: {message}"))]
    Bos { message: String },

    #[snafu(display("The head weights cannot be used: {message}"))]
    Head { message: String },

    #[snafu(display("The decider config cannot be used: {message}"))]
    Config { message: String },

    #[snafu(display("Could not read '{}': {source}", path.display()))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("The hidden-state backend failed: {source}"))]
    Backend { source: BoxError },

    #[snafu(display("The hidden-state backend returned {message}"))]
    BackendOutput { message: String },

    #[cfg(feature = "llama")]
    #[snafu(display("llama.cpp failed: {source}"))]
    Llama { source: llama_cpp::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A count or index as a float. Counts here are far below 2^32, where the conversion is exact.
pub(crate) fn as_f64(n: usize) -> f64 {
    f64::from(u32::try_from(n).unwrap_or(u32::MAX))
}
