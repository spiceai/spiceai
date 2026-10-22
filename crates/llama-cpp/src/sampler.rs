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
//! Choosing the next token from a context's logits.

use std::ptr::NonNull;

use std::ffi::CString;
use std::marker::PhantomData;

use snafu::ResultExt;

use crate::context::Context;
use crate::model::Vocab;
use crate::{GrammarNulSnafu, GrammarSnafu, Result, Token, sys};

/// A llama.cpp sampler, or a chain of them applied in order. A grammar sampler reads its
/// model's vocabulary, so it lives no longer than `'v`.
#[derive(Debug)]
pub struct Sampler<'v> {
    raw: NonNull<sys::llama_sampler>,
    _vocab: PhantomData<&'v ()>,
}

// SAFETY: a sampler is used by one thread at a time (`sample` takes `&mut self`), and holds
// nothing tied to the thread that made it.
unsafe impl Send for Sampler<'_> {}

impl<'v> Sampler<'v> {
    fn from_raw(raw: *mut sys::llama_sampler) -> Self {
        // llama.cpp's initializers only return null when out of memory.
        let raw = NonNull::new(raw).unwrap_or_else(|| {
            std::alloc::handle_alloc_error(std::alloc::Layout::new::<sys::llama_sampler>())
        });
        Self {
            raw,
            _vocab: PhantomData,
        }
    }

    /// The most likely token.
    #[must_use]
    pub fn greedy() -> Self {
        // SAFETY: allocates a sampler, freed in `drop` or by the chain that takes it.
        Self::from_raw(unsafe { sys::llama_sampler_init_greedy() })
    }

    /// A draw from the distribution, seeded.
    #[must_use]
    pub fn dist(seed: u32) -> Self {
        // SAFETY: as above.
        Self::from_raw(unsafe { sys::llama_sampler_init_dist(seed) })
    }

    #[must_use]
    pub fn temp(temperature: f32) -> Self {
        // SAFETY: as above.
        Self::from_raw(unsafe { sys::llama_sampler_init_temp(temperature) })
    }

    /// Nucleus sampling: the smallest set of tokens whose probability reaches `p`, keeping
    /// at least `min_keep`.
    #[must_use]
    pub fn top_p(p: f32, min_keep: usize) -> Self {
        // SAFETY: as above.
        Self::from_raw(unsafe { sys::llama_sampler_init_top_p(p, min_keep) })
    }

    /// Adds `bias` to the logit of each token, as `-inf` rules a token out.
    #[must_use]
    pub fn logit_bias(vocab: &Vocab<'_>, biases: &[(Token, f32)]) -> Self {
        let biases: Vec<sys::llama_logit_bias> = biases
            .iter()
            .map(|&(token, bias)| sys::llama_logit_bias {
                token: token.0,
                bias,
            })
            .collect();
        let n = i32::try_from(biases.len()).unwrap_or(i32::MAX);
        // SAFETY: `biases` holds `n` entries for the call; llama.cpp copies them.
        Self::from_raw(unsafe {
            sys::llama_sampler_init_logit_bias(vocab.n_tokens(), n, biases.as_ptr())
        })
    }

    /// Keeps only tokens that continue a text the GBNF `grammar` accepts from its `root`
    /// rule. A sampler chain applies it before the sampler that picks the token.
    pub fn grammar(vocab: &Vocab<'v>, grammar: &str) -> Result<Self> {
        let c_grammar = CString::new(grammar).context(GrammarNulSnafu)?;
        crate::log::clear_last_error();
        // SAFETY: both strings are NUL-terminated and outlive the call; the vocabulary lives
        // as long as the model, which outlives the request the sampler serves.
        let raw = unsafe {
            sys::llama_sampler_init_grammar(vocab.as_ptr(), c_grammar.as_ptr(), c"root".as_ptr())
        };
        let raw = NonNull::new(raw).ok_or_else(|| {
            GrammarSnafu {
                reason: crate::log::last_error(),
            }
            .build()
        })?;
        Ok(Self {
            raw,
            _vocab: PhantomData,
        })
    }

    /// Applies `samplers` in order; the last one picks the token.
    #[must_use]
    pub fn chain(samplers: impl IntoIterator<Item = Sampler<'v>>) -> Self {
        // SAFETY: allocates an empty chain from its default parameters.
        let chain = Self::from_raw(unsafe {
            sys::llama_sampler_chain_init(sys::llama_sampler_chain_default_params())
        });
        for sampler in samplers {
            let sampler = std::mem::ManuallyDrop::new(sampler);
            // SAFETY: the chain takes ownership of the sampler and frees it with itself, so it
            // is not dropped here.
            unsafe { sys::llama_sampler_chain_add(chain.raw.as_ptr(), sampler.raw.as_ptr()) };
        }
        chain
    }

    /// Picks the token for output row `index` of the last decoded batch.
    pub fn sample(&mut self, context: &Context<'_>, index: i32) -> Token {
        // SAFETY: the context is live and its last decode marked row `index` for output.
        Token(unsafe { sys::llama_sampler_sample(self.raw.as_ptr(), context.as_ptr(), index) })
    }
}

impl Drop for Sampler<'_> {
    fn drop(&mut self) {
        // SAFETY: the sampler was not handed to a chain, which would own it.
        unsafe { sys::llama_sampler_free(self.raw.as_ptr()) };
    }
}
