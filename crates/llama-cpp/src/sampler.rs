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
    /// Whether the sampler is greedy and nothing else: the greedy sampler, or a chain of it
    /// alone. Such a sampler picks a row's largest logit, which `sample` reads from the row
    /// itself instead of handing llama.cpp the row to copy into a candidate per token.
    greedy: bool,
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
            greedy: false,
            _vocab: PhantomData,
        }
    }

    /// The most likely token.
    #[must_use]
    pub fn greedy() -> Self {
        // SAFETY: allocates a sampler, freed in `drop` or by the chain that takes it.
        let mut greedy = Self::from_raw(unsafe { sys::llama_sampler_init_greedy() });
        greedy.greedy = true;
        greedy
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

    /// `OpenAI`'s frequency and presence penalties over the last `window` tokens the sampler
    /// has accepted: each token's logit loses `frequency` for every time it was accepted, and
    /// `presence` once if it was accepted at all.
    #[must_use]
    pub fn penalties(vocab: &Vocab<'_>, window: i32, frequency: f32, presence: f32) -> Self {
        // SAFETY: plain values; a repeat penalty of 1.0 leaves llama.cpp's own penalty off.
        Self::from_raw(unsafe {
            sys::llama_sampler_init_penalties(vocab.n_tokens(), window, 1.0, frequency, presence)
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
            greedy: false,
            _vocab: PhantomData,
        })
    }

    /// Applies `samplers` in order; the last one picks the token.
    #[must_use]
    pub fn chain(samplers: impl IntoIterator<Item = Sampler<'v>>) -> Self {
        // SAFETY: allocates an empty chain from its default parameters.
        let mut chain = Self::from_raw(unsafe {
            sys::llama_sampler_chain_init(sys::llama_sampler_chain_default_params())
        });
        let (mut count, mut greedy) = (0_usize, false);
        for sampler in samplers {
            count += 1;
            greedy = sampler.greedy;
            let sampler = std::mem::ManuallyDrop::new(sampler);
            // SAFETY: the chain takes ownership of the sampler and frees it with itself, so it
            // is not dropped here.
            unsafe { sys::llama_sampler_chain_add(chain.raw.as_ptr(), sampler.raw.as_ptr()) };
        }
        // A chain of one greedy sampler picks what that sampler picks.
        chain.greedy = count == 1 && greedy;
        chain
    }

    /// Applies the sampler to `logits`, the logit of each token by id, as it applies to a
    /// context's logits, and returns the tokens it leaves to pick from, in the order it
    /// leaves them.
    pub fn apply(&mut self, logits: &[f32]) -> Vec<Token> {
        // llama.cpp's samplers require at least one candidate.
        if logits.is_empty() {
            return Vec::new();
        }
        let mut data: Vec<sys::llama_token_data> = logits
            .iter()
            .enumerate()
            .map(|(id, &logit)| sys::llama_token_data {
                id: i32::try_from(id).unwrap_or(i32::MAX),
                logit,
                p: 0.0,
            })
            .collect();
        let mut candidates = sys::llama_token_data_array {
            data: data.as_mut_ptr(),
            size: data.len(),
            selected: -1,
            sorted: false,
        };
        // SAFETY: `candidates` points at `data`, which outlives the call. A sampler may
        // repoint it at a buffer of its own, which lives as long as the sampler.
        unsafe { sys::llama_sampler_apply(self.raw.as_ptr(), &raw mut candidates) };
        if candidates.data.is_null() {
            return Vec::new();
        }
        // SAFETY: after the call `candidates` describes `size` live entries, in `data` or in
        // the sampler's own buffer, and neither changes before they are copied out here.
        unsafe { std::slice::from_raw_parts(candidates.data, candidates.size) }
            .iter()
            .map(|d| Token(d.id))
            .collect()
    }

    /// Tells the sampler at `index` in this chain that `token` was picked, as picking it
    /// through the chain would. Nothing happens when the chain has no sampler there.
    pub fn accept_at(&mut self, index: i32, token: Token) {
        // SAFETY: the chain owns the sampler it returns, which lives as long as the chain.
        let sampler = unsafe { sys::llama_sampler_chain_get(self.raw.as_ptr(), index) };
        if !sampler.is_null() {
            // SAFETY: as above.
            unsafe { sys::llama_sampler_accept(sampler, token.0) };
        }
    }

    /// Whether the sampler at `index` in this chain leaves `token` possible: whether it keeps
    /// a finite logit for `token` as the only candidate. A grammar sampler rules out a token
    /// that does not continue its text, and llama.cpp aborts when one is told it picked such a
    /// token, so a token from elsewhere is checked here before [`Sampler::accept_at`]. False
    /// when the chain has no sampler there.
    #[must_use]
    pub fn allows_at(&mut self, index: i32, token: Token) -> bool {
        // SAFETY: the chain owns the sampler it returns, which lives as long as the chain.
        let sampler = unsafe { sys::llama_sampler_chain_get(self.raw.as_ptr(), index) };
        if sampler.is_null() {
            return false;
        }
        let mut data = sys::llama_token_data {
            id: token.0,
            logit: 0.0,
            p: 0.0,
        };
        let mut candidates = sys::llama_token_data_array {
            data: &raw mut data,
            size: 1,
            selected: -1,
            sorted: false,
        };
        // SAFETY: `candidates` points at `data`, which outlives the call. A sampler may
        // repoint it at a buffer of its own, which lives as long as the sampler.
        unsafe { sys::llama_sampler_apply(sampler, &raw mut candidates) };
        if candidates.data.is_null() {
            return false;
        }
        // SAFETY: after the call `candidates` describes `size` live entries, in `data` or in
        // the sampler's own buffer, and neither changes before they are read here.
        unsafe { std::slice::from_raw_parts(candidates.data, candidates.size) }
            .iter()
            .any(|d| d.id == token.0 && d.logit.is_finite())
    }

    /// Picks the token for output row `index` of the last decoded batch.
    pub fn sample(&mut self, context: &Context<'_>, index: i32) -> Token {
        if self.greedy
            && let Some(token) = context.logits(index).and_then(first_max)
        {
            // SAFETY: the sampler is live. `llama_sampler_sample` tells the sampler of its pick
            // the same way, which leaves a greedy sampler as it was.
            unsafe { sys::llama_sampler_accept(self.raw.as_ptr(), token.0) };
            return token;
        }
        // SAFETY: the context is live and its last decode marked row `index` for output.
        Token(unsafe { sys::llama_sampler_sample(self.raw.as_ptr(), context.as_ptr(), index) })
    }
}

/// The token with the largest logit, the first one on a tie: the token llama.cpp's greedy
/// sampler picks. That sampler walks the row from its first logit and moves to a later one
/// only when it is larger, and no comparison with a NaN holds, so it picks token 0 when the
/// first logit is a NaN, and otherwise the first token whose logit is the largest of those
/// that are not. This finds that token in two vectorizable passes over the row: its largest
/// logit, then where that first appears. A single scan that keeps the running maximum in a
/// register waits on that comparison at every logit. `None` for an empty row.
fn first_max(logits: &[f32]) -> Option<Token> {
    const LANES: usize = 16;
    const BLOCK: usize = 64;
    if logits.first()?.is_nan() {
        return Some(Token(0));
    }
    // `f32::max` returns the other operand of a NaN, so a NaN never becomes the maximum.
    let mut lanes = [f32::NEG_INFINITY; LANES];
    let (chunks, rest) = logits.as_chunks::<LANES>();
    for chunk in chunks {
        for (lane, &logit) in lanes.iter_mut().zip(chunk) {
            *lane = lane.max(logit);
        }
    }
    let max = lanes
        .iter()
        .chain(rest)
        .fold(f32::NEG_INFINITY, |m, &l| m.max(l));
    // The first block that holds `max` is the first whose own maximum reaches it. No logit that
    // is not a NaN exceeds `max`, so in that block `>=` holds exactly for the logits equal to
    // it, either zero for a zero.
    let block = logits.chunks(BLOCK).position(|block| {
        block
            .iter()
            .fold(f32::NEG_INFINITY, |m, &logit| m.max(logit))
            >= max
    })?;
    let within = logits[block * BLOCK..]
        .iter()
        .position(|&logit| logit >= max)?;
    i32::try_from(block * BLOCK + within).ok().map(Token)
}

impl Drop for Sampler<'_> {
    fn drop(&mut self) {
        // SAFETY: the sampler was not handed to a chain, which would own it.
        unsafe { sys::llama_sampler_free(self.raw.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The token llama.cpp's greedy sampler picks from `logits`, through its C API.
    fn greedy_pick(logits: &[f32]) -> Token {
        let greedy = Sampler::greedy();
        let mut data: Vec<sys::llama_token_data> = logits
            .iter()
            .enumerate()
            .map(|(id, &logit)| sys::llama_token_data {
                id: i32::try_from(id).expect("a token id fits i32"),
                logit,
                p: 0.0,
            })
            .collect();
        let mut candidates = sys::llama_token_data_array {
            data: data.as_mut_ptr(),
            size: data.len(),
            selected: -1,
            sorted: false,
        };
        // SAFETY: `candidates` points at `data`, which outlives the call; greedy only sets
        // `selected`.
        unsafe { sys::llama_sampler_apply(greedy.raw.as_ptr(), &raw mut candidates) };
        Token(data[usize::try_from(candidates.selected).expect("greedy selects a candidate")].id)
    }

    /// `first_max` picks what llama.cpp's greedy sampler picks, over seeded random rows drawn
    /// from a handful of values, NaNs and infinities included, so most rows tie for their
    /// largest logit. Every other row holds its largest logit once or twice at random places,
    /// so the first one falls in every part of rows long enough to span several of the blocks
    /// `first_max` works in.
    #[test]
    fn first_max_picks_what_the_greedy_sampler_picks_ties_included() {
        const SEED: u64 = 0x5eed_a26a_a7ed_0001;
        const VALUES: [f32; 8] = [
            f32::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            0.5,
            1.0,
            f32::INFINITY,
            f32::NAN,
        ];
        // SplitMix64, so the rows are the same on every run.
        let mut state = SEED;
        let mut next = move || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            usize::try_from(z ^ (z >> 31)).expect("usize is 64 bits")
        };
        let (mut ties, mut far) = (0, 0);
        for case in 0..20_000 {
            let len = 1 + next() % 300;
            let mut row: Vec<f32> = (0..len).map(|_| VALUES[next() % VALUES.len()]).collect();
            if case % 2 == 1 {
                row.retain(|logit| !logit.is_infinite() || logit.is_sign_negative());
                row.resize(len, -1.0);
                let at = next() % len;
                row[at] = f32::INFINITY;
                if next() % 2 == 0 {
                    row[at + next() % (len - at)] = f32::INFINITY;
                }
            }
            let expected = greedy_pick(&row);
            assert_eq!(
                first_max(&row),
                Some(expected),
                "case {case} of seed {SEED:#x}: {row:?}"
            );
            let index = usize::try_from(expected.0).expect("a token id is an index");
            let picked = row[index];
            if row.iter().filter(|&&logit| logit >= picked).count() > 1 {
                ties += 1;
            }
            if index >= 128 {
                far += 1;
            }
        }
        // Most rows hold their largest logit more than once, where the first and the last of
        // them differ, and many pick a token past the first two blocks.
        assert!(
            ties > 10_000,
            "only {ties} of 20000 rows tie for their largest logit"
        );
        assert!(far > 1_500, "only {far} of 20000 picks past token 127");
        assert_eq!(first_max(&[]), None);
        assert_eq!(first_max(&[1.0, 3.0, 2.0, 3.0]), Some(Token(1)));
    }

    /// Set in the child process that runs the failed check.
    const CHILD: &str = "LLAMA_CPP_FAILED_CHECK_CHILD";

    /// llama.cpp is patched to throw on a failed check only while it loads a model, where its
    /// C API catches the exception. Anywhere else it must abort, as upstream does: these
    /// functions catch nothing, so an exception would unwind out of the C API into Rust.
    #[test]
    fn a_failed_check_outside_a_model_load_aborts_instead_of_throwing() {
        if std::env::var_os(CHILD).is_some() {
            let top_p = Sampler::top_p(0.5, 1);
            let mut empty = sys::llama_token_data_array {
                data: std::ptr::null_mut(),
                size: 0,
                selected: -1,
                sorted: false,
            };
            // SAFETY: an empty candidate array, on which top-p's softmax fails its check.
            unsafe { sys::llama_sampler_apply(top_p.raw.as_ptr(), &raw mut empty) };
            return;
        }
        let child = std::process::Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "sampler::tests::a_failed_check_outside_a_model_load_aborts_instead_of_throwing",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("the child test runs");
        let stderr = String::from_utf8_lossy(&child.stderr);
        assert!(
            !stderr.contains("exception"),
            "the failed check threw instead of aborting: {stderr}"
        );
        assert!(
            stderr.contains("GGML_ASSERT(cur_p->size > 0) failed"),
            "the child did not fail the check: {child:?}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            // SIGABRT is 6 on Linux and macOS.
            assert_eq!(child.status.signal(), Some(6), "{child:?}");
        }
    }
}
