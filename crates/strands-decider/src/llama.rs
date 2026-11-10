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
//! The decoder on llama.cpp: a GGUF base model with the checkpoint's adapter applied at
//! decode time.

use std::num::NonZeroU32;
use std::sync::Arc;

use llama_cpp::{Backend, Batch, Context, ContextParams, LoraAdapter, Model, Token};
use snafu::ResultExt;

use crate::engine::{BoxError, HiddenRows, HiddenStates};
use crate::{LlamaSnafu, Result};

/// Runs each question after the state in one context, one question at a time, and reads the
/// hidden states the head needs.
///
/// Every question decodes the state again: correct for any model, including hybrid ones
/// (Qwen3.5) whose recurrent state cannot be shared between sequences by copying a cache.
#[derive(Debug)]
pub struct LlamaHiddenStates<'m> {
    context: Context<'m>,
    batch: Batch,
    n_batch: usize,
    width: usize,
}

impl<'m> LlamaHiddenStates<'m> {
    /// A context of `n_ctx` tokens, at least the checkpoint's `max_length`, on `model`, with
    /// `adapter` applied at scale 1 (its trained `alpha / rank`). `n_batch` is the most tokens
    /// one decode takes. `n_threads` is the CPU threads a decode uses; llama.cpp's default
    /// when `None`.
    pub fn new(
        backend: &Backend,
        model: &'m Model,
        adapter: Option<Arc<LoraAdapter<'m>>>,
        n_ctx: u32,
        n_batch: u32,
        n_threads: Option<NonZeroU32>,
    ) -> Result<Self> {
        let params = ContextParams::default()
            .with_n_ctx(NonZeroU32::new(n_ctx))
            .with_n_batch(n_batch)
            .with_n_threads(n_threads)
            .with_hidden_states(true);
        let mut context = model.new_context(backend, &params).context(LlamaSnafu)?;
        if let Some(adapter) = adapter {
            context.set_lora_adapters(&[(adapter, 1.0)]);
        }
        let n_batch = usize::try_from(n_batch.max(1)).unwrap_or(usize::MAX);
        Ok(Self {
            context,
            batch: Batch::new(n_batch),
            n_batch,
            width: model.n_embd_out(),
        })
    }

    /// The hidden states of one sequence, `tokens`, at `rows` (positions in it).
    fn sequence(&mut self, tokens: &[Token], rows: &[usize]) -> Result<Vec<f32>, BoxError> {
        self.context.clear();
        let mut found: Vec<Option<Vec<f32>>> = vec![None; rows.len()];
        for (chunk_index, chunk) in tokens.chunks(self.n_batch).enumerate() {
            let start = chunk_index * self.n_batch;
            self.batch.clear();
            for (j, &token) in chunk.iter().enumerate() {
                let pos = i32::try_from(start + j)?;
                self.batch.add(token, pos, rows.contains(&(start + j)))?;
            }
            self.context.decode(&self.batch)?;
            for (slot, &row) in found.iter_mut().zip(rows) {
                if (start..start + chunk.len()).contains(&row) {
                    let index = i32::try_from(row - start)?;
                    let state = self
                        .context
                        .hidden_state(index)
                        .ok_or("llama.cpp returned no hidden state for an output token")?;
                    *slot = Some(state.to_vec());
                }
            }
        }
        let mut data = Vec::with_capacity(rows.len() * self.width);
        for state in found {
            data.extend(state.ok_or("a requested row is past the sequence's end")?);
        }
        Ok(data)
    }
}

impl HiddenStates for LlamaHiddenStates<'_> {
    fn hidden_states(
        &mut self,
        state: &[u32],
        questions: &[Vec<u32>],
        rows: &[Vec<usize>],
    ) -> Result<Vec<HiddenRows>, BoxError> {
        let to_tokens = |ids: &[u32]| -> Result<Vec<Token>, BoxError> {
            ids.iter()
                .map(|&id| Ok(Token(i32::try_from(id)?)))
                .collect()
        };
        let state = to_tokens(state)?;
        questions
            .iter()
            .zip(rows)
            .map(|(question, rows)| {
                let mut tokens = state.clone();
                tokens.extend(to_tokens(question)?);
                let rows: Vec<usize> = rows.iter().map(|r| r + state.len()).collect();
                Ok(HiddenRows {
                    width: self.width,
                    data: self.sequence(&tokens, &rows)?,
                })
            })
            .collect()
    }
}
