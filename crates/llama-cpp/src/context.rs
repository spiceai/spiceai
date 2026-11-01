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
//! Contexts, which hold a sequence's KV cache, and the batches decoded into them.

use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ptr::NonNull;

use snafu::ensure;

use crate::mtp::DraftKind;
use crate::{BatchFullSnafu, Error, Result, Token, sys};

/// The parts of a sequence's state a truncation cannot restore: recurrent states and
/// sliding-window caches, not the full-attention cache.
const PARTIAL_ONLY: sys::llama_state_seq_flags = sys::LLAMA_STATE_SEQ_FLAGS_PARTIAL_ONLY;

/// How far a context's cache can be cut back from its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truncation {
    /// To any position: an attention cache, or a sliding-window cache kept at full size, as
    /// llama.cpp keeps one by default.
    Anywhere,
    /// Only over the last few positions its rollback snapshots keep: a recurrent state with
    /// snapshots for drafted tokens.
    Bounded,
    /// Not short of clearing it: a recurrent state without snapshots, or a compressed cache.
    Never,
}

/// How a context is created.
#[expect(
    clippy::struct_field_names,
    reason = "the names are llama.cpp's own context parameters"
)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextParams {
    n_ctx: Option<NonZeroU32>,
    n_batch: Option<u32>,
    n_rs_seq: u32,
}

impl ContextParams {
    /// The context length in tokens; the model's own when unset.
    #[must_use]
    pub fn with_n_ctx(mut self, n_ctx: Option<NonZeroU32>) -> Self {
        self.n_ctx = n_ctx;
        self
    }

    /// The most tokens one `decode` call takes.
    #[must_use]
    pub fn with_n_batch(mut self, n_batch: u32) -> Self {
        self.n_batch = Some(n_batch);
        self
    }

    /// How many recurrent-state snapshots a sequence keeps, so a hybrid model's recurrent
    /// layers can roll back that many tokens.
    #[must_use]
    pub fn with_n_rs_seq(mut self, n_rs_seq: u32) -> Self {
        self.n_rs_seq = n_rs_seq;
        self
    }

    /// The C parameters; `draft_of` makes a draft context that reads that target, an MTP
    /// context for an MTP head.
    pub(crate) fn raw(
        &self,
        draft_of: Option<(&Context<'_>, DraftKind)>,
    ) -> sys::llama_context_params {
        // SAFETY: returns a plain struct of defaults.
        let mut raw = unsafe { sys::llama_context_default_params() };
        if let Some(n_ctx) = self.n_ctx {
            raw.n_ctx = n_ctx.get();
        }
        if let Some(n_batch) = self.n_batch {
            raw.n_batch = n_batch;
        }
        raw.n_rs_seq = self.n_rs_seq;
        if let Some((target, kind)) = draft_of {
            if kind == DraftKind::Mtp {
                raw.ctx_type = sys::llama_context_type_LLAMA_CONTEXT_TYPE_MTP;
            }
            raw.ctx_other = target.as_ptr();
        }
        raw
    }
}

/// A llama.cpp context: the KV cache and compute state of the model's sequences.
///
/// `raw` is the context `llama_init_from_model` created for this value, which owns it: nothing
/// else frees it, and `Drop` frees it once. So `raw` points to a live context for as long as
/// `self` exists, which every `unsafe` call below relies on.
#[derive(Debug)]
pub struct Context<'m> {
    raw: NonNull<sys::llama_context>,
    _model: PhantomData<&'m crate::Model>,
}

// SAFETY: a context is used by one thread at a time, which `&mut self` on every call that
// changes it enforces; nothing in it is tied to the thread that created it.
unsafe impl Send for Context<'_> {}

impl Context<'_> {
    pub(crate) fn from_raw(raw: NonNull<sys::llama_context>) -> Self {
        Self {
            raw,
            _model: PhantomData,
        }
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::llama_context {
        self.raw.as_ptr()
    }

    /// The context length in tokens.
    #[must_use]
    pub fn n_ctx(&self) -> u32 {
        // SAFETY: the context is live.
        unsafe { sys::llama_n_ctx(self.raw.as_ptr()) }
    }

    /// Decodes `batch`, computing the logits of the tokens it marks for output.
    pub fn decode(&mut self, batch: &Batch) -> Result<()> {
        // SAFETY: the batch's arrays hold `n_tokens` entries; llama.cpp copies what it keeps.
        let status = unsafe { sys::llama_decode(self.raw.as_ptr(), batch.raw) };
        match status {
            0 => Ok(()),
            1 => Err(Error::Decode {
                reason: "the context has no room left for the batch".to_string(),
            }),
            2 => Err(Error::Decode {
                reason: "decoding was aborted".to_string(),
            }),
            -1 => Err(Error::Decode {
                reason: "the batch is invalid".to_string(),
            }),
            status => Err(Error::Decode {
                reason: format!("llama.cpp returned {status}"),
            }),
        }
    }

    /// The logits of output row `index` of the last decoded batch, one per token of the
    /// model's vocabulary, by token id. `None` when the last decode did not output that row,
    /// or when a backend sampler, which leaves only its own candidates, sampled it.
    pub(crate) fn logits(&self, index: i32) -> Option<&[f32]> {
        let ctx = self.raw.as_ptr();
        // SAFETY: `raw` is live while `self` is (see `Context`). Each call waits for the last
        // decode to finish, then reads where it left row `index`.
        let backend_sampled = unsafe {
            sys::llama_get_sampled_token_ith(ctx, index) != sys::LLAMA_TOKEN_NULL
                || !sys::llama_get_sampled_logits_ith(ctx, index).is_null()
                || !sys::llama_get_sampled_probs_ith(ctx, index).is_null()
        };
        if backend_sampled {
            return None;
        }
        // SAFETY: as above; null when the decode did not output row `index`.
        let row = NonNull::new(unsafe { sys::llama_get_logits_ith(ctx, index) })?;
        // SAFETY: as above; the model outlives the context, and its vocabulary the model.
        let n_vocab = unsafe {
            sys::llama_vocab_n_tokens(sys::llama_model_get_vocab(sys::llama_get_model(ctx)))
        };
        let n_vocab = usize::try_from(n_vocab).ok()?;
        // SAFETY: llama.cpp keeps a row of logits per output, one per vocabulary token. It
        // writes them while it decodes, and when the first read after a decode puts the rows in
        // batch order, which the calls above have done. Only another decode queues writes again,
        // and it takes `&mut self`, so it cannot start while the returned row borrows `self`.
        Some(unsafe { std::slice::from_raw_parts(row.as_ptr(), n_vocab) })
    }

    /// Waits for the computation queued by earlier decodes to finish: a decode returns once
    /// its computation is queued, and a time taken without waiting would leave it out.
    pub fn synchronize(&mut self) {
        // SAFETY: the context is live.
        unsafe { sys::llama_synchronize(self.raw.as_ptr()) };
    }

    /// Empties the KV cache of every sequence.
    pub fn clear(&mut self) {
        // SAFETY: the context is live; a context always has memory.
        unsafe { sys::llama_memory_clear(sys::llama_get_memory(self.raw.as_ptr()), true) };
    }

    /// Removes sequence 0's entries from position `from` on. Returns false when the context
    /// cannot roll back that far, as when a recurrent layer keeps too few state snapshots.
    pub fn truncate(&mut self, from: i32) -> bool {
        // SAFETY: as above; -1 means "to the end".
        unsafe { sys::llama_memory_seq_rm(sys::llama_get_memory(self.raw.as_ptr()), 0, from, -1) }
    }

    /// How far this context's cache can be cut back, found as llama.cpp's server finds it: a
    /// context that keeps rollback snapshots rolls back only over them; otherwise two tokens are
    /// decoded into the emptied cache, and the second removed. Leaves the cache empty.
    pub fn truncation(&mut self) -> Truncation {
        // SAFETY: `raw` is live while `self` is (see `Context`), and `llama_n_rs_seq` only reads
        // the context's parameters.
        if unsafe { sys::llama_n_rs_seq(self.raw.as_ptr()) } > 0 {
            return Truncation::Bounded;
        }
        self.clear();
        let mut batch = Batch::new(2);
        let truncated = batch.add(Token(0), 0, false).is_ok()
            && batch.add(Token(0), 1, true).is_ok()
            && self.decode(&batch).is_ok()
            && self.truncate(1);
        self.clear();
        if truncated {
            Truncation::Anywhere
        } else {
            Truncation::Never
        }
    }

    /// Sequence 0's state that a truncation cannot restore, as llama.cpp's server saves a
    /// context checkpoint: a recurrent layer's state or a sliding-window cache, never the
    /// full-attention cache. `None` when llama.cpp writes less than it sized.
    #[must_use]
    pub fn partial_state(&self) -> Option<Vec<u8>> {
        // SAFETY: `raw` is live while `self` is (see `Context`), and sizing a sequence's state
        // reads the context without changing it.
        let size = unsafe { sys::llama_state_seq_get_size_ext(self.raw.as_ptr(), 0, PARTIAL_ONLY) };
        let mut state = vec![0u8; size];
        // SAFETY: `raw` is live (see `Context`), and llama.cpp writes at most `size` bytes into
        // `state`, which holds `size` writable bytes.
        let written = unsafe {
            sys::llama_state_seq_get_data_ext(
                self.raw.as_ptr(),
                state.as_mut_ptr(),
                size,
                0,
                PARTIAL_ONLY,
            )
        };
        (size > 0 && written == size).then_some(state)
    }

    /// Restores a state `partial_state` saved, leaving the full-attention cache as it is.
    /// Returns false when llama.cpp does not read all of it.
    pub fn restore_partial_state(&mut self, state: &[u8]) -> bool {
        // SAFETY: `raw` is live (see `Context`), `&mut self` keeps every other call off the
        // context while its state is replaced, and llama.cpp reads at most `state.len()` bytes
        // from `state`, which holds that many.
        let read = unsafe {
            sys::llama_state_seq_set_data_ext(
                self.raw.as_ptr(),
                state.as_ptr(),
                state.len(),
                0,
                PARTIAL_ONLY,
            )
        };
        !state.is_empty() && read == state.len()
    }
}

impl Drop for Context<'_> {
    fn drop(&mut self) {
        // SAFETY: the context is live, and an MTP draft context that reads it is dropped first
        // (see `Mtp`).
        unsafe { sys::llama_free(self.raw.as_ptr()) };
    }
}

/// Tokens to decode in one call, all in sequence 0.
#[derive(Debug)]
pub struct Batch {
    raw: sys::llama_batch,
    capacity: usize,
}

// SAFETY: the batch owns its arrays, and nothing else points into them.
unsafe impl Send for Batch {}

impl Batch {
    /// A batch with room for `capacity` tokens.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let n = i32::try_from(capacity).unwrap_or(i32::MAX);
        // SAFETY: allocates token, position, sequence and output arrays for `n` tokens of one
        // sequence each; freed in `drop`.
        let raw = unsafe { sys::llama_batch_init(n, 0, 1) };
        Self {
            raw,
            capacity: usize::try_from(n).unwrap_or(0),
        }
    }

    /// Appends `token` at `pos` in sequence 0; `output` computes its logits.
    pub fn add(&mut self, token: Token, pos: i32, output: bool) -> Result<()> {
        let index = usize::try_from(self.raw.n_tokens).unwrap_or(usize::MAX);
        ensure!(
            index < self.capacity,
            BatchFullSnafu {
                capacity: self.capacity
            }
        );
        // SAFETY: `index` is within the arrays `llama_batch_init` sized for `capacity` tokens,
        // and each token's sequence array has room for one id.
        unsafe {
            *self.raw.token.add(index) = token.0;
            *self.raw.pos.add(index) = pos;
            *self.raw.n_seq_id.add(index) = 1;
            **self.raw.seq_id.add(index) = 0;
            *self.raw.logits.add(index) = i8::from(output);
        }
        self.raw.n_tokens += 1;
        Ok(())
    }

    pub fn clear(&mut self) {
        self.raw.n_tokens = 0;
    }

    #[must_use]
    pub fn n_tokens(&self) -> i32 {
        self.raw.n_tokens
    }

    pub(crate) fn raw(&self) -> &sys::llama_batch {
        &self.raw
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        // SAFETY: the arrays came from `llama_batch_init` and are freed once.
        unsafe { sys::llama_batch_free(self.raw) };
    }
}
