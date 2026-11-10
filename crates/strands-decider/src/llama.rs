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

/// Decodes the state once into sequence 0, copies it to one sequence per question, and
/// decodes the questions together, as upstream reuses one prefix cache for every question.
/// Each question sees the state and itself, never another question.
///
/// Copying a sequence shares its attention cache and copies its recurrent state, so this holds
/// for hybrid models (Qwen3.5) too.
#[derive(Debug)]
pub struct LlamaHiddenStates<'m> {
    context: Context<'m>,
    batch: Batch,
    n_batch: usize,
    /// The most questions decoded together; one sequence each, after the state's.
    n_parallel: usize,
    width: usize,
}

impl<'m> LlamaHiddenStates<'m> {
    /// A context on `model` that holds a prompt of up to `max_length` tokens (the checkpoint's)
    /// for each of `n_parallel` questions decoded together, with `adapter` applied at scale 1
    /// (its trained `alpha / rank`). A request with more questions decodes them in groups.
    /// `n_batch` is the most tokens one decode takes. `n_threads` is the CPU threads a decode
    /// uses; llama.cpp's default when `None`.
    pub fn new(
        backend: &Backend,
        model: &'m Model,
        adapter: Option<Arc<LoraAdapter<'m>>>,
        max_length: u32,
        n_parallel: NonZeroU32,
        n_batch: u32,
        n_threads: Option<NonZeroU32>,
    ) -> Result<Self> {
        // The questions share the state's cache cells, so one cache of `n_parallel` windows
        // holds the state once and every question after it.
        let params = ContextParams::default()
            .with_n_ctx(NonZeroU32::new(max_length.saturating_mul(n_parallel.get())))
            .with_n_seq_max(n_parallel.get().saturating_add(1))
            .with_kv_unified(true)
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
            n_parallel: usize::try_from(n_parallel.get()).unwrap_or(usize::MAX),
            width: model.n_embd_out(),
        })
    }

    /// Decodes `tokens` (token, position, sequence, output), `n_batch` at a time, and returns
    /// the hidden state of each output token in order.
    fn decode(&mut self, tokens: &[(Token, usize, i32, bool)]) -> Result<Vec<f32>, BoxError> {
        let mut data = Vec::new();
        for chunk in tokens.chunks(self.n_batch) {
            self.batch.clear();
            for &(token, pos, seq, output) in chunk {
                self.batch
                    .add_seq(token, i32::try_from(pos)?, seq, output)?;
            }
            self.context.decode(&self.batch)?;
            for (index, &(.., output)) in chunk.iter().enumerate() {
                if output {
                    let state = self
                        .context
                        .hidden_state(i32::try_from(index)?)
                        .ok_or("llama.cpp returned no hidden state for an output token")?;
                    data.extend_from_slice(state);
                }
            }
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
        let token = |id: u32| -> Result<Token, BoxError> { Ok(Token(i32::try_from(id)?)) };
        self.context.clear();
        let prefix = state
            .iter()
            .enumerate()
            .map(|(pos, &id)| Ok((token(id)?, pos, 0, false)))
            .collect::<Result<Vec<_>, BoxError>>()?;
        self.decode(&prefix)?;

        let mut out = Vec::with_capacity(questions.len());
        let groups = questions
            .chunks(self.n_parallel)
            .zip(rows.chunks(self.n_parallel));
        for (group, group_rows) in groups {
            let mut tokens = Vec::new();
            // Each question's output positions, ascending: the order llama.cpp returns them in.
            let mut outputs = Vec::with_capacity(group.len());
            for (i, (question, rows)) in group.iter().zip(group_rows).enumerate() {
                let seq = i32::try_from(i + 1)?;
                if !self.context.copy_seq(0, seq) {
                    return Err("the context holds too few sequences".into());
                }
                let mut positions = rows.clone();
                positions.sort_unstable();
                positions.dedup();
                if positions.last().is_some_and(|&row| row >= question.len()) {
                    return Err("a requested row is past the sequence's end".into());
                }
                for (j, &id) in question.iter().enumerate() {
                    let output = positions.binary_search(&j).is_ok();
                    tokens.push((token(id)?, state.len() + j, seq, output));
                }
                outputs.push(positions);
            }
            let data = self.decode(&tokens)?;
            let mut states = data.chunks_exact(self.width);
            for (positions, rows) in outputs.iter().zip(group_rows) {
                let found: Vec<&[f32]> = states.by_ref().take(positions.len()).collect();
                let mut data = Vec::with_capacity(rows.len() * self.width);
                for row in rows {
                    let at = positions
                        .binary_search(row)
                        .map_err(|_| "a row was not decoded")?;
                    let state = found
                        .get(at)
                        .ok_or("llama.cpp returned too few hidden states")?;
                    data.extend_from_slice(state);
                }
                out.push(HiddenRows {
                    width: self.width,
                    data,
                });
            }
            for i in 0..group.len() {
                self.context.truncate_seq(i32::try_from(i + 1)?, 0);
            }
        }
        Ok(out)
    }
}
