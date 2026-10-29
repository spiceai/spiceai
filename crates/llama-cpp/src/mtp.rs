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

use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr::NonNull;

use snafu::OptionExt;

use crate::context::{Batch, Context, ContextParams};
use crate::model::Model;
use crate::{CreateContextSnafu, Error, MtpInitSnafu, Result, Token, sys};

/// How a draft model drafts for its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DraftKind {
    /// A multi-token-prediction head, which reads the target's hidden states.
    #[default]
    Mtp,
    /// A `DFlash` drafter, which reads the target's layer inputs.
    DFlash,
    /// A `DSpark` drafter: a `DFlash` drafter with a Markov head.
    DSpark,
}

impl DraftKind {
    /// `DFlash` or `DSpark` for a GGUF whose architecture is `dflash`, from its header; `None`
    /// for any other file, or one that cannot be read.
    #[must_use]
    pub fn dflash_of(path: &Path) -> Option<Self> {
        let path = CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
        // SAFETY: a NUL-terminated path for the call; the shim reads the header and frees it.
        match unsafe { sys::spice_dflash_kind(path.as_ptr()) } {
            sys::spice_draft_kind_SPICE_DRAFT_DFLASH => Some(Self::DFlash),
            sys::spice_draft_kind_SPICE_DRAFT_DSPARK => Some(Self::DSpark),
            _ => None,
        }
    }

    fn raw(self) -> sys::spice_draft_kind {
        match self {
            Self::Mtp => sys::spice_draft_kind_SPICE_DRAFT_MTP,
            Self::DFlash => sys::spice_draft_kind_SPICE_DRAFT_DFLASH,
            Self::DSpark => sys::spice_draft_kind_SPICE_DRAFT_DSPARK,
        }
    }
}

/// How the draft head drafts, as llama.cpp's `--spec-draft-*` options set it.
#[derive(Debug, Clone, Copy)]
pub struct MtpParams {
    /// What kind of draft model drafts.
    pub kind: DraftKind,
    /// The most tokens drafted per step.
    pub n_max: i32,
    /// Drafts shorter than this are dropped.
    pub n_min: i32,
    /// Drafting stops at a token whose probability is below this.
    pub p_min: f32,
}

/// A target context and the MTP draft context that reads it, drafting for sequence 0.
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
    /// Creates `head`'s draft context beside `target`, with `params`, and starts drafting.
    pub fn new(
        target: Context<'m>,
        head: &'m Model,
        params: &ContextParams,
        mtp: MtpParams,
    ) -> Result<Self> {
        crate::log::clear_last_error();
        // SAFETY: the draft context reads `target`, which `Self` keeps alive longer.
        let draft = unsafe {
            sys::llama_init_from_model(head.as_ptr(), params.raw(Some((&target, mtp.kind))))
        };
        let draft = Context::from_raw(NonNull::new(draft).with_context(|| CreateContextSnafu {
            reason: crate::log::last_error(),
        })?);
        // SAFETY: both contexts are live, and `draft` is the MTP context of `target`.
        let raw = unsafe {
            sys::spice_mtp_init(
                target.as_ptr(),
                draft.as_ptr(),
                mtp.kind.raw(),
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

    /// Starts a generation; the prompt must already be decoded and processed.
    pub fn begin(&mut self, prompt: &[Token]) -> Result<()> {
        // SAFETY: `prompt` holds `len` transparent `llama_token`s for the call.
        let status = unsafe {
            sys::spice_mtp_begin(self.raw.as_ptr(), prompt.as_ptr().cast(), prompt.len())
        };
        self.check(status)
    }

    /// Hands the head a batch the target just decoded.
    pub fn process(&mut self, batch: &Batch) -> Result<()> {
        // SAFETY: the batch is live for the call; the shim copies what it keeps.
        let status = unsafe { sys::spice_mtp_process(self.raw.as_ptr(), batch.raw()) };
        self.check(status)
    }

    /// Drafts the tokens to follow `last` at position `n_past`; `history` is every token
    /// before `last`. A non-empty draft must be followed by [`Mtp::accept`].
    /// The most tokens one draft proposes.
    #[must_use]
    pub fn n_max(&self) -> usize {
        self.n_max
    }

    pub fn draft(&mut self, n_past: i32, last: Token, history: &[Token]) -> Result<Vec<Token>> {
        let mut out = vec![Token(0); self.n_max];
        let mut written = 0usize;
        // SAFETY: `history` and `out` are valid for their lengths, and `written` for one write.
        let status = unsafe {
            sys::spice_mtp_draft(
                self.raw.as_ptr(),
                n_past,
                last.0,
                history.as_ptr().cast(),
                history.len(),
                out.as_mut_ptr().cast(),
                out.len(),
                &raw mut written,
            )
        };
        self.check(status)?;
        out.truncate(written);
        Ok(out)
    }

    /// Reports how many drafted tokens the target accepted.
    pub fn accept(&mut self, accepted: u16) -> Result<()> {
        // SAFETY: the state is live.
        let status = unsafe { sys::spice_mtp_accept(self.raw.as_ptr(), accepted) };
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
