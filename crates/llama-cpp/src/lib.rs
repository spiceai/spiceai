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
pub mod rpc;
mod sampler;

use std::sync::OnceLock;

use snafu::Snafu;

pub use context::{Batch, Context, ContextParams};
pub use model::{Model, ModelParams, Vocab};
pub use mtp::{DraftKind, Mtp, MtpParams};
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
    #[snafu(display("llama.cpp could not load the model '{path}': {reason}"))]
    LoadModel { path: String, reason: String },

    #[snafu(display("The model path '{path}' cannot be passed to llama.cpp: {source}"))]
    ModelPath {
        path: String,
        source: std::ffi::NulError,
    },

    #[snafu(display("llama.cpp could not create a context for the model: {reason}"))]
    CreateContext { reason: String },

    #[snafu(display("llama.cpp could not decode the batch: {reason}"))]
    Decode { reason: String },

    #[snafu(display("The batch is full: it holds {capacity} tokens"))]
    BatchFull { capacity: usize },

    #[snafu(display("The text is too long for llama.cpp to tokenize: {bytes} bytes"))]
    TextTooLong { bytes: usize },

    #[snafu(display(
        "llama.cpp could not start MTP drafting with this model and its draft head: {reason}"
    ))]
    MtpInit { reason: String },

    #[snafu(display("llama.cpp MTP drafting failed: {message}"))]
    Mtp { message: String },

    #[snafu(display("No GPU server answered at '{endpoint}'"))]
    RpcConnect { endpoint: String },

    #[snafu(display("The address '{endpoint}' cannot be passed to llama.cpp: {source}"))]
    RpcEndpoint {
        endpoint: String,
        source: std::ffi::NulError,
    },

    #[snafu(display("llama.cpp could not serve this node's GPUs at '{endpoint}': {reason}"))]
    RpcServe { endpoint: String, reason: String },

    #[snafu(display("llama.cpp could not use the grammar: {reason}"))]
    Grammar { reason: String },

    #[snafu(display("The grammar cannot be passed to llama.cpp: {source}"))]
    GrammarNul { source: std::ffi::NulError },

    #[snafu(display("llama.cpp could not turn the JSON schema into a grammar: {reason}"))]
    JsonSchema { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The GBNF grammar, with start rule `root`, that accepts exactly the JSON documents
/// `schema` (a JSON Schema, as JSON text) describes, converted by llama.cpp.
pub fn json_schema_to_grammar(schema: &str) -> Result<String> {
    let c_schema = std::ffi::CString::new(schema).map_err(|e| Error::JsonSchema {
        reason: e.to_string(),
    })?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    // SAFETY: `c_schema` is NUL-terminated for the call, and `error` takes an owned string.
    let grammar = unsafe { sys::spice_json_schema_to_grammar(c_schema.as_ptr(), &raw mut error) };
    // SAFETY: each pointer is null or a string the shim allocated, read once, then freed.
    let take = |text: *mut std::ffi::c_char| -> Option<String> {
        if text.is_null() {
            return None;
        }
        let owned = unsafe { std::ffi::CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned();
        unsafe { sys::spice_string_free(text) };
        Some(owned)
    };
    let reason = take(error);
    take(grammar).ok_or_else(|| Error::JsonSchema {
        reason: reason.unwrap_or_else(|| "llama.cpp gave no reason".to_string()),
    })
}

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
