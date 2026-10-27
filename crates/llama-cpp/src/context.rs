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

use crate::{
    BatchFullSnafu, Error, PartialSequenceCopySnafu, Result, Token, UnknownSequenceSnafu, sys,
};

/// How a context is created.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextParams {
    n_ctx: Option<NonZeroU32>,
    n_batch: Option<u32>,
    n_rs_seq: u32,
    n_seq_max: Option<u32>,
    kv_unified: Option<bool>,
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

    /// How many sequences the context holds at once; llama.cpp's own default (one) when unset.
    #[must_use]
    pub fn with_n_seq_max(mut self, n_seq_max: u32) -> Self {
        self.n_seq_max = Some(n_seq_max);
        self
    }

    /// Whether the sequences share one KV cache, so a sequence's share is not fixed in advance;
    /// llama.cpp's own default when unset.
    #[must_use]
    pub fn with_kv_unified(mut self, kv_unified: bool) -> Self {
        self.kv_unified = Some(kv_unified);
        self
    }

    /// The C parameters; `mtp_of` makes an MTP draft context that reads that target's
    /// hidden states.
    pub(crate) fn raw(&self, mtp_of: Option<&Context<'_>>) -> sys::llama_context_params {
        // SAFETY: returns a plain struct of defaults.
        let mut raw = unsafe { sys::llama_context_default_params() };
        if let Some(n_ctx) = self.n_ctx {
            raw.n_ctx = n_ctx.get();
        }
        if let Some(n_batch) = self.n_batch {
            raw.n_batch = n_batch;
        }
        raw.n_rs_seq = self.n_rs_seq;
        if let Some(n_seq_max) = self.n_seq_max {
            raw.n_seq_max = n_seq_max;
        }
        if let Some(kv_unified) = self.kv_unified {
            raw.kv_unified = kv_unified;
        }
        if let Some(target) = mtp_of {
            raw.ctx_type = sys::llama_context_type_LLAMA_CONTEXT_TYPE_MTP;
            raw.ctx_other = target.as_ptr();
        }
        raw
    }
}

/// A llama.cpp context: the KV cache and compute state of the model's sequences.
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

    /// How many sequences the context holds at once.
    #[must_use]
    pub fn n_seq_max(&self) -> u32 {
        // SAFETY: the context is live.
        unsafe { sys::llama_n_seq_max(self.raw.as_ptr()) }
    }

    /// The most tokens one sequence can hold: the context length, or its share of it when the
    /// sequences do not share one KV cache.
    #[must_use]
    pub fn n_ctx_seq(&self) -> u32 {
        // SAFETY: the context is live.
        unsafe { sys::llama_n_ctx_seq(self.raw.as_ptr()) }
    }

    /// Decodes `batch`, computing the logits of the tokens it marks for output.
    ///
    /// A batch the cache has no room for returns [`Error::NoRoom`] before anything is computed,
    /// so a caller that frees room can retry the same batch.
    pub fn decode(&mut self, batch: &Batch) -> Result<()> {
        // SAFETY: the batch's arrays hold `n_tokens` entries; llama.cpp copies what it keeps.
        let status = unsafe { sys::llama_decode(self.raw.as_ptr(), batch.raw) };
        match status {
            0 => Ok(()),
            1 => Err(Error::NoRoom),
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

    /// Empties the KV cache of every sequence.
    pub fn clear(&mut self) {
        // SAFETY: the context is live; a context always has memory.
        unsafe { sys::llama_memory_clear(sys::llama_get_memory(self.raw.as_ptr()), true) };
    }

    /// Removes sequence 0's entries from position `from` on. Returns false when the context
    /// cannot roll back that far, as when a recurrent layer keeps too few state snapshots.
    pub fn truncate(&mut self, from: i32) -> bool {
        // SAFETY: the context is live, a context always has memory, and holds sequence 0.
        unsafe { sys::llama_memory_seq_rm(sys::llama_get_memory(self.raw.as_ptr()), 0, from, -1) }
    }

    /// Removes sequence `seq`'s entries in positions `[p0, p1)`; a negative `p1` means "to the
    /// end", and `seq` -1 means every sequence. Returns false when the context cannot remove
    /// that range, as when a recurrent layer keeps too few state snapshots to roll back.
    pub fn seq_rm(&mut self, seq: i32, p0: i32, p1: i32) -> Result<bool> {
        if seq != -1 {
            self.check_seq(seq)?;
        }
        // SAFETY: the context is live, a context always has memory, and holds `seq`.
        Ok(unsafe {
            sys::llama_memory_seq_rm(sys::llama_get_memory(self.raw.as_ptr()), seq, p0, p1)
        })
    }

    /// Makes sequence `dst` hold sequence `src`'s entries in positions `[p0, p1)`; a negative
    /// `p1` means "to the end". When the sequences share one KV cache they share the entries;
    /// in separate caches the entries are copied, which llama.cpp does only for a whole
    /// sequence, so a partial copy there returns [`Error::PartialSequenceCopy`].
    pub fn seq_cp(&mut self, src: i32, dst: i32, p0: i32, p1: i32) -> Result<()> {
        self.check_seq(src)?;
        self.check_seq(dst)?;
        ensure!(
            src == dst || !self.separate_caches() || (p0 <= 0 && p1 < 0),
            PartialSequenceCopySnafu { src, dst }
        );
        // SAFETY: the context is live, holds both sequences, and the copy is one llama.cpp
        // supports between their caches.
        unsafe {
            sys::llama_memory_seq_cp(sys::llama_get_memory(self.raw.as_ptr()), src, dst, p0, p1);
        }
        Ok(())
    }

    /// The largest position sequence `seq` holds, or -1 when it holds none.
    pub fn seq_pos_max(&self, seq: i32) -> Result<i32> {
        self.check_seq(seq)?;
        // SAFETY: as above; reading the positions does not change the memory.
        Ok(unsafe { sys::llama_memory_seq_pos_max(sys::llama_get_memory(self.raw.as_ptr()), seq) })
    }

    /// llama.cpp ends the process on a sequence its cache does not hold, so every sequence is
    /// checked before it is passed on.
    fn check_seq(&self, seq: i32) -> Result<()> {
        let n_seq_max = self.n_seq_max();
        ensure!(
            u32::try_from(seq).is_ok_and(|seq| seq < n_seq_max),
            UnknownSequenceSnafu { seq, n_seq_max }
        );
        Ok(())
    }

    /// Whether each sequence has a KV cache of its own: llama.cpp then splits the context
    /// length between them, and with one shared cache gives each sequence all of it.
    fn separate_caches(&self) -> bool {
        self.n_ctx_seq() < self.n_ctx()
    }
}

impl Drop for Context<'_> {
    fn drop(&mut self) {
        // SAFETY: the context is live, and an MTP draft context that reads it is dropped first
        // (see `Mtp`).
        unsafe { sys::llama_free(self.raw.as_ptr()) };
    }
}

/// Tokens to decode in one call, each in one sequence.
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

    /// Appends `token` at `pos` in sequence `seq` (below the context's `n_seq_max`); `output`
    /// computes its logits.
    pub fn add(&mut self, token: Token, pos: i32, seq: i32, output: bool) -> Result<()> {
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
            **self.raw.seq_id.add(index) = seq;
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
