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
//! Loading a GGUF model, and its vocabulary.

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::NonNull;

use snafu::{OptionExt, ResultExt};

use crate::context::{Context, ContextParams};
use crate::rpc::Device;
use crate::{
    Backend, CreateContextSnafu, LoadModelSnafu, ModelPathSnafu, Result, TextTooLongSnafu, Token,
    sys,
};

/// How a model is loaded.
#[derive(Debug, Clone, Default)]
pub struct ModelParams {
    gpu_layers: Option<u32>,
    load_mtp: bool,
    devices: Vec<Device>,
}

impl ModelParams {
    /// Offloads this many layers to the GPU; every layer when unset.
    #[must_use]
    pub fn with_gpu_layers(mut self, layers: u32) -> Self {
        self.gpu_layers = Some(layers);
        self
    }

    /// Loads the model's multi-token-prediction layers, as an MTP draft head needs.
    #[must_use]
    pub fn with_load_mtp(mut self, load_mtp: bool) -> Self {
        self.load_mtp = load_mtp;
        self
    }

    /// Places the model on `devices` instead of this node's GPUs. One device holds all of it;
    /// across two or more, every tensor is split tensor-parallel, and each device holds and
    /// reads its share of every weight.
    #[must_use]
    pub fn with_devices(mut self, devices: Vec<Device>) -> Self {
        self.devices = devices;
        self
    }
}

/// A loaded GGUF model.
#[derive(Debug)]
pub struct Model {
    raw: NonNull<sys::llama_model>,
}

// SAFETY: llama.cpp never mutates a loaded model; per-request state lives in contexts.
unsafe impl Send for Model {}
// SAFETY: as above, so shared references from several threads only read it.
unsafe impl Sync for Model {}

impl Model {
    /// Loads a GGUF file. For a sharded GGUF, `path` is the first shard and llama.cpp opens
    /// the rest beside it.
    pub fn load(_backend: &Backend, path: &Path, params: &ModelParams) -> Result<Self> {
        let display = path.display().to_string();
        let c_path = CString::new(path.as_os_str().as_encoded_bytes()).context(ModelPathSnafu {
            path: display.clone(),
        })?;
        // SAFETY: returns a plain struct of defaults.
        let mut raw_params = unsafe { sys::llama_model_default_params() };
        // A negative count offloads every layer.
        raw_params.n_gpu_layers = params
            .gpu_layers
            .and_then(|layers| i32::try_from(layers).ok())
            .unwrap_or(-1);
        raw_params.load_mtp = params.load_mtp;
        // llama.cpp reads the list up to its null entry while it loads, and keeps a copy.
        let mut devices: Vec<sys::ggml_backend_dev_t> =
            params.devices.iter().map(|device| device.0).collect();
        if !devices.is_empty() {
            if devices.len() > 1 {
                raw_params.split_mode = sys::llama_split_mode_LLAMA_SPLIT_MODE_TENSOR;
            }
            devices.push(std::ptr::null_mut());
            raw_params.devices = devices.as_mut_ptr();
        }
        crate::log::clear_last_error();
        // SAFETY: `c_path` is a NUL-terminated path and `devices` a null-terminated list, both
        // of which outlive the call.
        let raw = unsafe { sys::llama_model_load_from_file(c_path.as_ptr(), raw_params) };
        let raw = NonNull::new(raw).with_context(|| LoadModelSnafu {
            path: display,
            reason: crate::log::last_error(),
        })?;
        Ok(Self { raw })
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::llama_model {
        self.raw.as_ptr()
    }

    #[must_use]
    pub fn vocab(&self) -> Vocab<'_> {
        // SAFETY: a loaded model always has a vocabulary, which lives as long as the model.
        let raw = unsafe { sys::llama_model_get_vocab(self.raw.as_ptr()) };
        Vocab {
            raw,
            _model: PhantomData,
        }
    }

    /// How many multi-token-prediction (`NextN`) layers the model carries; a model with one
    /// can draft for itself.
    #[must_use]
    pub fn n_layer_nextn(&self) -> i32 {
        // SAFETY: reads the loaded model's hyperparameters.
        unsafe { sys::llama_model_n_layer_nextn(self.raw.as_ptr()) }
    }

    /// The chat template the GGUF file carries, if any.
    #[must_use]
    pub fn chat_template(&self) -> Option<String> {
        // SAFETY: a null name asks for the default template; the result is null or a
        // NUL-terminated string owned by the model.
        let template =
            unsafe { sys::llama_model_chat_template(self.raw.as_ptr(), std::ptr::null()) };
        if template.is_null() {
            return None;
        }
        // SAFETY: checked non-null above; it lives as long as the model.
        let template = unsafe { CStr::from_ptr(template) };
        template.to_str().ok().map(ToString::to_string)
    }

    /// The model's architecture, as its GGUF's `general.architecture` names it.
    #[must_use]
    pub fn architecture(&self) -> Option<String> {
        let mut buf = vec![0u8; 64];
        loop {
            // SAFETY: the key is NUL-terminated and `buf` holds `buf.len()` bytes; llama.cpp
            // writes at most that many, NUL included, and returns the value's full length.
            let len = unsafe {
                sys::llama_model_meta_val_str(
                    self.raw.as_ptr(),
                    c"general.architecture".as_ptr(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                )
            };
            let len = usize::try_from(len).ok()?;
            if len < buf.len() {
                buf.truncate(len);
                return String::from_utf8(buf).ok();
            }
            buf.resize(len + 1, 0);
        }
    }

    /// Creates a context to decode with.
    pub fn new_context(&self, _backend: &Backend, params: &ContextParams) -> Result<Context<'_>> {
        crate::log::clear_last_error();
        // SAFETY: the model outlives the context through the context's lifetime.
        let raw = unsafe { sys::llama_init_from_model(self.raw.as_ptr(), params.raw(None)) };
        let raw = NonNull::new(raw).with_context(|| CreateContextSnafu {
            reason: crate::log::last_error(),
        })?;
        Ok(Context::from_raw(raw))
    }
}

impl Drop for Model {
    fn drop(&mut self) {
        // SAFETY: every context borrows the model, so none is left.
        unsafe { sys::llama_model_free(self.raw.as_ptr()) };
    }
}

/// A model's vocabulary.
#[derive(Debug, Clone, Copy)]
pub struct Vocab<'m> {
    raw: *const sys::llama_vocab,
    _model: PhantomData<&'m Model>,
}

impl Vocab<'_> {
    pub(crate) fn as_ptr(&self) -> *const sys::llama_vocab {
        self.raw
    }

    /// How many tokens the vocabulary holds; token ids run from 0 to one less.
    #[must_use]
    pub fn n_tokens(&self) -> i32 {
        // SAFETY: the vocabulary lives as long as `'m`.
        unsafe { sys::llama_vocab_n_tokens(self.raw) }
    }

    /// Every token that ends generation.
    #[must_use]
    pub fn eog_tokens(&self) -> Vec<Token> {
        (0..self.n_tokens())
            .map(Token)
            .filter(|&token| self.is_eog(token))
            .collect()
    }

    #[must_use]
    pub fn bos(&self) -> Token {
        // SAFETY: the vocabulary lives as long as `'m`.
        Token(unsafe { sys::llama_vocab_bos(self.raw) })
    }

    #[must_use]
    pub fn eos(&self) -> Token {
        // SAFETY: as above.
        Token(unsafe { sys::llama_vocab_eos(self.raw) })
    }

    /// Whether `token` ends generation: end of sequence, end of turn, and the like.
    #[must_use]
    pub fn is_eog(&self, token: Token) -> bool {
        // SAFETY: as above.
        unsafe { sys::llama_vocab_is_eog(self.raw, token.0) }
    }

    /// Splits `text` into tokens. `parse_special` reads control tokens such as
    /// `<|im_start|>` in the text as themselves.
    pub fn tokenize(
        &self,
        text: &str,
        add_special: bool,
        parse_special: bool,
    ) -> Result<Vec<Token>> {
        let length = i32::try_from(text.len())
            .ok()
            .context(TextTooLongSnafu { bytes: text.len() })?;
        let mut tokens = vec![Token(0); text.len() + 2];
        let mut written =
            self.tokenize_into(text, length, &mut tokens, add_special, parse_special)?;
        if written < 0 {
            // A negative count is how many tokens the text needs.
            tokens.resize(written.unsigned_abs() as usize, Token(0));
            written = self.tokenize_into(text, length, &mut tokens, add_special, parse_special)?;
        }
        tokens.truncate(usize::try_from(written).unwrap_or(0));
        Ok(tokens)
    }

    fn tokenize_into(
        &self,
        text: &str,
        length: i32,
        tokens: &mut [Token],
        add_special: bool,
        parse_special: bool,
    ) -> Result<i32> {
        let capacity = i32::try_from(tokens.len())
            .ok()
            .context(TextTooLongSnafu { bytes: text.len() })?;
        // SAFETY: `text` holds `length` bytes, and `tokens` has room for `capacity` tokens;
        // `Token` is a transparent `llama_token`.
        Ok(unsafe {
            sys::llama_tokenize(
                self.raw,
                text.as_ptr().cast(),
                length,
                tokens.as_mut_ptr().cast(),
                capacity,
                add_special,
                parse_special,
            )
        })
    }

    /// The bytes `token` decodes to. A multi-byte character can span several tokens, so a
    /// piece is not always valid UTF-8 by itself. `special` renders control tokens as text.
    #[must_use]
    pub fn token_to_piece(&self, token: Token, special: bool) -> Vec<u8> {
        let mut buffer = vec![0u8; 32];
        let mut written = self.piece_into(token, &mut buffer, special);
        if written < 0 {
            buffer.resize(written.unsigned_abs() as usize, 0);
            written = self.piece_into(token, &mut buffer, special);
        }
        buffer.truncate(usize::try_from(written).unwrap_or(0));
        buffer
    }

    fn piece_into(&self, token: Token, buffer: &mut [u8], special: bool) -> i32 {
        let capacity = i32::try_from(buffer.len()).unwrap_or(i32::MAX);
        // SAFETY: `buffer` has room for `capacity` bytes; llama.cpp writes no NUL.
        unsafe {
            sys::llama_token_to_piece(
                self.raw,
                token.0,
                buffer.as_mut_ptr().cast(),
                capacity,
                0,
                special,
            )
        }
    }
}
