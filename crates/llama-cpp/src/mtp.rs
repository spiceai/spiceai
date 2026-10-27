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
//! Multi-token-prediction (MTP) speculative decoding: a draft head proposes the next tokens
//! from the target's hidden states, and one target pass verifies them.

use std::ffi::CStr;
use std::ptr::NonNull;

use snafu::{OptionExt, ensure};

use crate::context::{Batch, Context, ContextParams};
use crate::model::Model;
use crate::{CreateContextSnafu, Error, MtpInitSnafu, Result, Token, sys};

/// How the draft head drafts, as llama.cpp's `--spec-draft-*` options set it.
#[derive(Debug, Clone, Copy)]
pub struct MtpParams {
    /// The most tokens drafted per step.
    pub n_max: i32,
    /// Drafts shorter than this are dropped.
    pub n_min: i32,
    /// Drafting stops at a token whose probability is below this.
    pub p_min: f32,
}

/// What one sequence's draft follows: the token `last` at position `n_past`, after every token
/// in `history`.
#[derive(Debug, Clone, Copy)]
pub struct DraftRequest<'a> {
    pub seq: i32,
    pub n_past: i32,
    pub last: Token,
    pub history: &'a [Token],
}

/// A target context and the MTP draft context that reads it, drafting for each of the
/// target's sequences.
#[derive(Debug)]
pub struct Mtp<'m> {
    raw: NonNull<sys::spice_mtp>,
    // Fields drop in order, and the draft context reads the target's, so it goes first.
    draft: Context<'m>,
    target: Context<'m>,
    n_max: usize,
}

// SAFETY: the drafting state, like the contexts it drives, is used by one thread at a time,
// which `&mut self` on every call enforces.
unsafe impl Send for Mtp<'_> {}

impl<'m> Mtp<'m> {
    /// Creates `head`'s draft context beside `target`, with `params`, and starts drafting for
    /// each of the target's sequences. The draft context caches every sequence the target
    /// does, so `params` must give it as many sequences, each at least as long, and at least as
    /// many tokens in all.
    pub fn new(
        target: Context<'m>,
        head: &'m Model,
        params: &ContextParams,
        mtp: MtpParams,
    ) -> Result<Self> {
        crate::log::clear_last_error();
        // SAFETY: the draft context reads `target`, which `Self` keeps alive longer.
        let draft = unsafe { sys::llama_init_from_model(head.as_ptr(), params.raw(Some(&target))) };
        let draft = Context::from_raw(NonNull::new(draft).with_context(|| CreateContextSnafu {
            reason: crate::log::last_error(),
        })?);
        ensure!(
            draft.n_seq_max() == target.n_seq_max()
                && draft.n_ctx_seq() >= target.n_ctx_seq()
                && draft.n_ctx() >= target.n_ctx(),
            MtpInitSnafu {
                reason: format!(
                    "the draft context holds {} sequences of {} tokens, {} in all, and the target {} of {}, {} in all",
                    draft.n_seq_max(),
                    draft.n_ctx_seq(),
                    draft.n_ctx(),
                    target.n_seq_max(),
                    target.n_ctx_seq(),
                    target.n_ctx()
                ),
            }
        );
        // SAFETY: both contexts are live, hold `n_seq_max` sequences, and `draft` is the MTP
        // context of `target`.
        let raw = unsafe {
            sys::spice_mtp_init(
                target.as_ptr(),
                draft.as_ptr(),
                target.n_seq_max(),
                mtp.n_max,
                mtp.n_min,
                mtp.p_min,
            )
        };
        let raw = NonNull::new(raw).with_context(|| MtpInitSnafu {
            reason: crate::log::last_error(),
        })?;
        Ok(Self {
            raw,
            draft,
            target,
            n_max: usize::try_from(mtp.n_max).unwrap_or(0),
        })
    }

    #[must_use]
    pub fn target(&self) -> &Context<'m> {
        &self.target
    }

    pub fn target_mut(&mut self) -> &mut Context<'m> {
        &mut self.target
    }

    pub fn draft_mut(&mut self) -> &mut Context<'m> {
        &mut self.draft
    }

    /// Starts a generation in sequence `seq`; its prompt must already be decoded and processed.
    pub fn begin(&mut self, seq: i32, prompt: &[Token]) -> Result<()> {
        // SAFETY: `prompt` holds `len` transparent `llama_token`s for the call.
        let status = unsafe {
            sys::spice_mtp_begin(self.raw.as_ptr(), seq, prompt.as_ptr().cast(), prompt.len())
        };
        self.check(status)
    }

    /// Hands the head a batch the target just decoded. Each sequence's tokens must be
    /// contiguous in the batch.
    pub fn process(&mut self, batch: &Batch) -> Result<()> {
        // SAFETY: the batch is live for the call; the shim copies what it keeps.
        let status = unsafe { sys::spice_mtp_process(self.raw.as_ptr(), batch.raw()) };
        self.check(status)
    }

    /// Drafts the tokens to follow `last` at position `n_past` in sequence `seq`; `history`
    /// is every token before `last`. A non-empty draft must be followed by [`Mtp::accept`].
    pub fn draft(
        &mut self,
        seq: i32,
        n_past: i32,
        last: Token,
        history: &[Token],
    ) -> Result<Vec<Token>> {
        let request = DraftRequest {
            seq,
            n_past,
            last,
            history,
        };
        Ok(self.draft_many(&[request])?.pop().unwrap_or_default())
    }

    /// Drafts for every request's sequence in one pass of the head, returning the drafts in
    /// the requests' order. No two requests may name the same sequence, and each non-empty
    /// draft must be followed by [`Mtp::accept`] for its sequence.
    pub fn draft_many(&mut self, requests: &[DraftRequest<'_>]) -> Result<Vec<Vec<Token>>> {
        let mut drafts: Vec<Vec<Token>> = requests
            .iter()
            .map(|_| vec![Token(0); self.n_max])
            .collect();
        let mut raw: Vec<sys::spice_mtp_draft_request> = requests
            .iter()
            .zip(drafts.iter_mut())
            .map(|(request, out)| sys::spice_mtp_draft_request {
                seq: request.seq,
                n_past: request.n_past,
                id_last: request.last.0,
                history: request.history.as_ptr().cast(),
                n_history: request.history.len(),
                out: out.as_mut_ptr().cast(),
                capacity: out.len(),
                n_out: 0,
            })
            .collect();
        // SAFETY: each request's history and output buffer are live and as long as it says
        // for the call, and the shim writes at most `capacity` tokens to each buffer.
        let status =
            unsafe { sys::spice_mtp_draft(self.raw.as_ptr(), raw.as_mut_ptr(), raw.len()) };
        self.check(status)?;
        for (out, request) in drafts.iter_mut().zip(&raw) {
            out.truncate(request.n_out);
        }
        Ok(drafts)
    }

    /// Reports how many of sequence `seq`'s drafted tokens the target accepted.
    pub fn accept(&mut self, seq: i32, accepted: u16) -> Result<()> {
        // SAFETY: the state is live.
        let status = unsafe { sys::spice_mtp_accept(self.raw.as_ptr(), seq, accepted) };
        self.check(status)
    }

    fn check(&self, status: sys::spice_mtp_status) -> Result<()> {
        if status == sys::spice_mtp_status_SPICE_MTP_OK {
            return Ok(());
        }
        // SAFETY: the shim returns a NUL-terminated message that lives until its next call.
        let message = unsafe { CStr::from_ptr(sys::spice_mtp_error(self.raw.as_ptr())) }
            .to_string_lossy()
            .into_owned();
        Err(Error::Mtp { message })
    }
}

impl Drop for Mtp<'_> {
    fn drop(&mut self) {
        // SAFETY: freed once, before the contexts it drives (fields drop after this).
        unsafe { sys::spice_mtp_free(self.raw.as_ptr()) };
    }
}
