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

//! Safe bindings to the parts of llama.cpp that Spice's GGUF engine uses: loading a model,
//! decoding batches into a context, sampling, and MTP speculative decoding.
//!
//! llama.cpp is built from the `spiceai/llama.cpp` fork at a tagged upstream release
//! ([`RELEASE`]), with CUDA or Metal when the `cuda` or `metal` feature is on.
#![expect(
    clippy::missing_errors_doc,
    reason = "every fallible call returns this crate's `Error`, whose variants say what failed"
)]

mod context;
mod log;
mod model;
mod mtp;
mod sampler;

use std::sync::OnceLock;

use snafu::Snafu;

pub use context::{Batch, Context, ContextParams};
pub use model::{Model, ModelParams, Vocab};
pub use mtp::{Mtp, MtpParams};
pub use sampler::Sampler;

/// The llama.cpp release this crate is built from, such as `b11803`.
pub const RELEASE: &str = env!("LLAMA_CPP_RELEASE");

mod sys {
    //! The raw bindings bindgen generates from `llama.h` and the MTP shim.
    // Generated code, named and shaped by the C headers.
    #![allow(
        dead_code,
        non_camel_case_types,
        non_snake_case,
        non_upper_case_globals,
        unsafe_op_in_unsafe_fn,
        clippy::pedantic,
        clippy::allow_attributes,
        clippy::upper_case_acronyms
    )]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("llama.cpp could not load the model '{path}'"))]
    LoadModel { path: String },

    #[snafu(display("The model path '{path}' cannot be passed to llama.cpp: {source}"))]
    ModelPath {
        path: String,
        source: std::ffi::NulError,
    },

    #[snafu(display("llama.cpp could not create a context for the model"))]
    CreateContext,

    #[snafu(display("llama.cpp could not decode the batch: {reason}"))]
    Decode { reason: String },

    #[snafu(display("The batch is full: it holds {capacity} tokens"))]
    BatchFull { capacity: usize },

    #[snafu(display("The text is too long for llama.cpp to tokenize: {bytes} bytes"))]
    TextTooLong { bytes: usize },

    #[snafu(display("llama.cpp could not start MTP drafting with this model and its draft head"))]
    MtpInit,

    #[snafu(display("llama.cpp MTP drafting failed: {message}"))]
    Mtp { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A token id in a model's vocabulary.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token(pub i32);

/// llama.cpp's process-wide backend. It is initialized on first use and lives for the
/// process; its log goes to `tracing`.
#[derive(Debug)]
pub struct Backend {
    _private: (),
}

static BACKEND: OnceLock<Backend> = OnceLock::new();

impl Backend {
    pub fn get() -> &'static Backend {
        BACKEND.get_or_init(|| {
            log::forward_to_tracing();
            // SAFETY: called once, before any model is loaded; the backend is never freed.
            unsafe { sys::llama_backend_init() };
            Backend { _private: () }
        })
    }
}
