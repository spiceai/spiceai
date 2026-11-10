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
use std::sync::Arc;

use snafu::ensure;

use crate::lora::LoraAdapter;
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
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextParams {
    n_ctx: Option<NonZeroU32>,
    n_batch: Option<u32>,
    n_rs_seq: u32,
    n_seq_max: Option<u32>,
    kv_unified: Option<bool>,
    hidden_states: bool,
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

    /// How many sequences the context holds at once; one when unset.
    #[must_use]
    pub fn with_n_seq_max(mut self, n_seq_max: u32) -> Self {
        self.n_seq_max = Some(n_seq_max);
        self
    }

    /// Whether the sequences share one KV cache of `n_ctx` cells, as llama.cpp's `--kv-unified`
    /// makes them, rather than each holding `n_ctx / n_seq_max` cells of its own.
    #[must_use]
    pub fn with_kv_unified(mut self, kv_unified: bool) -> Self {
        self.kv_unified = Some(kv_unified);
        self
    }

    /// Makes every decode output the final hidden state of each token it marks for output,
    /// one per token rather than pooled per sequence: the model's last layer after its final
    /// norm, the input to its output head. [`Context::hidden_state`] reads them.
    #[must_use]
    pub fn with_hidden_states(mut self, hidden_states: bool) -> Self {
        self.hidden_states = hidden_states;
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
        if let Some(n_seq_max) = self.n_seq_max {
            raw.n_seq_max = n_seq_max;
        }
        if let Some(kv_unified) = self.kv_unified {
            raw.kv_unified = kv_unified;
        }
        if self.hidden_states {
            // With no pooling, llama.cpp keeps a row per output token: the graph's `t_embd`,
            // which every decoder sets to the output-normed state before the output head.
            raw.embeddings = true;
            raw.pooling_type = sys::llama_pooling_type_LLAMA_POOLING_TYPE_NONE;
        }
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
    /// Whether the decodes that follow output each row's greedy pick instead of its logits
    /// ([`Context::set_argmax_output`]).
    argmax: bool,
    /// Whether decodes output hidden states ([`ContextParams::with_hidden_states`]).
    hidden_states: bool,
    /// The adapters llama.cpp applies to this context's decodes. It keeps only their pointers,
    /// so the context holds them until it is freed or they are replaced.
    loras: Vec<Arc<LoraAdapter<'m>>>,
}

// SAFETY: a context is used by one thread at a time, which `&mut self` on every call that
// changes it enforces; nothing in it is tied to the thread that created it.
unsafe impl Send for Context<'_> {}

impl<'m> Context<'m> {
    /// Takes ownership of `raw`, which llama.cpp created with `params`.
    pub(crate) fn from_raw(raw: NonNull<sys::llama_context>, params: &ContextParams) -> Self {
        Self {
            raw,
            _model: PhantomData,
            argmax: false,
            hidden_states: params.hidden_states,
            loras: Vec::new(),
        }
    }

    /// Applies `adapters`, each with its scale, to the decodes that follow, in place of the
    /// adapters set before; an empty list removes them. A scale of 1 applies an adapter as
    /// trained (llama.cpp multiplies it by the adapter's `alpha / rank`). The base weights are
    /// never changed, so contexts of one model can apply different adapters.
    pub fn set_lora_adapters(&mut self, adapters: &[(Arc<LoraAdapter<'m>>, f32)]) {
        let mut raw: Vec<*mut sys::llama_adapter_lora> = adapters
            .iter()
            .map(|(adapter, _)| adapter.as_ptr())
            .collect();
        let mut scales: Vec<f32> = adapters.iter().map(|&(_, scale)| scale).collect();
        let (raw_ptr, scales_ptr) = if adapters.is_empty() {
            (std::ptr::null_mut(), std::ptr::null_mut())
        } else {
            (raw.as_mut_ptr(), scales.as_mut_ptr())
        };
        // SAFETY: the context is live, both arrays hold `adapters.len()` entries (or are null
        // for none), and llama.cpp copies them. Every adapter belongs to this context's model
        // (`'m`), and `self.loras` keeps each alive for as long as the context may read it.
        unsafe {
            sys::llama_set_adapters_lora(self.raw.as_ptr(), raw_ptr, adapters.len(), scales_ptr);
        }
        self.loras = adapters
            .iter()
            .map(|(adapter, _)| Arc::clone(adapter))
            .collect();
    }

    /// The final hidden state of the token at position `index` of the last decoded batch, one
    /// value per dimension of the model's output embedding ([`crate::Model::n_embd_out`]). A
    /// negative index counts back from the batch's last output. `None` when the context was
    /// not created [`ContextParams::with_hidden_states`], or when the batch did not mark that
    /// token for output.
    #[must_use]
    pub fn hidden_state(&self, index: i32) -> Option<&[f32]> {
        if !self.hidden_states {
            return None;
        }
        let ctx = self.raw.as_ptr();
        // SAFETY: the context is live; the call waits for the last decode to finish and
        // returns null for a row it did not output.
        let row = NonNull::new(unsafe { sys::llama_get_embeddings_ith(ctx, index) })?;
        // SAFETY: the model outlives the context.
        let n_embd = unsafe { sys::llama_model_n_embd_out(sys::llama_get_model(ctx)) };
        let n_embd = usize::try_from(n_embd).ok()?;
        // SAFETY: with no pooling llama.cpp keeps `n_embd_out` floats per output row, written by
        // the decode the call above waited for. Only another decode writes them again, and it
        // takes `&mut self`, so it cannot start while the returned row borrows `self`.
        Some(unsafe { std::slice::from_raw_parts(row.as_ptr(), n_embd) })
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

    /// How many sequences the context holds.
    #[must_use]
    pub fn n_seq_max(&self) -> u32 {
        // SAFETY: the context is live.
        unsafe { sys::llama_n_seq_max(self.raw.as_ptr()) }
    }

    /// How many tokens one sequence can hold: the whole context with a unified cache.
    #[must_use]
    pub fn n_ctx_seq(&self) -> u32 {
        // SAFETY: the context is live.
        unsafe { sys::llama_n_ctx_seq(self.raw.as_ptr()) }
    }

    /// Whether `seq` is one of the context's sequences; llama.cpp asserts, ending the process,
    /// on any other.
    fn holds(&self, seq: i32) -> bool {
        u32::try_from(seq).is_ok_and(|seq| seq < self.n_seq_max())
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
        // SAFETY: `raw` is live while `self` is (see `Context`). The call waits once for the
        // last decode to finish, then returns where it left row `index`: null when the decode
        // did not output that row, or when a backend sampler sampled it. Checking the sampled
        // getters one by one and then reading the row would wait in each of them, and on a
        // model split across nodes each wait reaches every rank.
        let row = NonNull::new(unsafe { sys::llama_get_unsampled_logits_ith(ctx, index) })?;
        // SAFETY: as above; the model outlives the context, and its vocabulary the model.
        let n_vocab = unsafe {
            sys::llama_vocab_n_tokens(sys::llama_model_get_vocab(sys::llama_get_model(ctx)))
        };
        let n_vocab = usize::try_from(n_vocab).ok()?;
        // SAFETY: llama.cpp keeps a row of logits per output, one per vocabulary token. It
        // writes them while it decodes, and when the first read after a decode puts the rows in
        // batch order, which the call above has done. Only another decode queues writes again,
        // and it takes `&mut self`, so it cannot start while the returned row borrows `self`.
        Some(unsafe { std::slice::from_raw_parts(row.as_ptr(), n_vocab) })
    }

    /// Turns the argmax output on or off for the decodes that follow. With it on, a model whose
    /// output head a tensor split divides by rows (DeepSeek-V4.1) computes each output row's
    /// greedy pick on the devices that hold the head and sends back only the picks, instead of
    /// gathering every row's logits. Those decodes have no logits: [`Context::argmax`] gives the
    /// pick. Any other model computes its logits as before, and [`Context::argmax`] takes their
    /// first maximum. The pick ignores samplers, so only a request that samples greedily alone
    /// may use it. Changing the mode rebuilds the model's graph, so this calls llama.cpp only
    /// when the mode changes.
    pub fn set_argmax_output(&mut self, on: bool) {
        if on != self.argmax {
            // SAFETY: the context is live.
            unsafe { sys::llama_set_argmax_output(self.raw.as_ptr(), on) };
            self.argmax = on;
        }
    }

    /// Whether the argmax output is on ([`Context::set_argmax_output`]).
    #[must_use]
    pub fn argmax_output(&self) -> bool {
        self.argmax
    }

    /// The greedy pick of output row `index` of the last decoded batch: the token with the
    /// largest logit, the first on a tie, token 0 when the row's first logit is a NaN, as
    /// llama.cpp's greedy sampler picks it. `None` when the last decode did not output that row,
    /// or when a backend sampler sampled it.
    pub(crate) fn argmax(&self, index: i32) -> Option<Token> {
        // SAFETY: the context is live; the call waits for the last decode to finish.
        let token = unsafe { sys::llama_get_argmax_ith(self.raw.as_ptr(), index) };
        (token >= 0).then_some(Token(token))
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
        self.truncate_seq(0, from)
    }

    /// Removes sequence `seq`'s entries from position `from` on. Returns false for a sequence
    /// the context does not hold, or one it cannot roll back that far.
    pub fn truncate_seq(&mut self, seq: i32, from: i32) -> bool {
        if !self.holds(seq) {
            return false;
        }
        // SAFETY: the context is live, and `seq` is one of its sequences; -1 means "to the end".
        unsafe { sys::llama_memory_seq_rm(sys::llama_get_memory(self.raw.as_ptr()), seq, from, -1) }
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
        self.partial_state_seq(0)
    }

    /// Sequence `seq`'s state that a truncation cannot restore; see [`Context::partial_state`].
    /// `None` for a sequence the context does not hold.
    #[must_use]
    pub fn partial_state_seq(&self, seq: i32) -> Option<Vec<u8>> {
        if !self.holds(seq) {
            return None;
        }
        // SAFETY: `raw` is live while `self` is (see `Context`), `seq` is one of its sequences,
        // and sizing a sequence's state reads the context without changing it.
        let size =
            unsafe { sys::llama_state_seq_get_size_ext(self.raw.as_ptr(), seq, PARTIAL_ONLY) };
        let mut state = vec![0u8; size];
        // SAFETY: `raw` is live (see `Context`), and llama.cpp writes at most `size` bytes into
        // `state`, which holds `size` writable bytes.
        let written = unsafe {
            sys::llama_state_seq_get_data_ext(
                self.raw.as_ptr(),
                state.as_mut_ptr(),
                size,
                seq,
                PARTIAL_ONLY,
            )
        };
        (size > 0 && written == size).then_some(state)
    }

    /// Restores a state `partial_state` saved, leaving the full-attention cache as it is.
    /// Returns false when llama.cpp does not read all of it.
    pub fn restore_partial_state(&mut self, state: &[u8]) -> bool {
        self.restore_partial_state_seq(0, state)
    }

    /// Restores a state [`Context::partial_state_seq`] saved into sequence `seq`. Returns false
    /// for a sequence the context does not hold, or when llama.cpp does not read all of it.
    pub fn restore_partial_state_seq(&mut self, seq: i32, state: &[u8]) -> bool {
        if !self.holds(seq) {
            return false;
        }
        // SAFETY: `raw` is live (see `Context`), `seq` is one of its sequences, `&mut self`
        // keeps every other call off the context while its state is replaced, and llama.cpp
        // reads at most `state.len()` bytes from `state`, which holds that many.
        let read = unsafe {
            sys::llama_state_seq_set_data_ext(
                self.raw.as_ptr(),
                state.as_ptr(),
                state.len(),
                seq,
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

    /// Appends `token` at `pos` in sequence 0; `output` computes its logits.
    pub fn add(&mut self, token: Token, pos: i32, output: bool) -> Result<()> {
        self.add_seq(token, pos, 0, output)
    }

    /// Appends `token` at `pos` in sequence `seq`; `output` computes its logits.
    pub fn add_seq(&mut self, token: Token, pos: i32, seq: i32, output: bool) -> Result<()> {
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
