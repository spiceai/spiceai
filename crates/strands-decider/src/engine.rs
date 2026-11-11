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
//! Answering a request: render, tokenize, ask a backend for the hidden states the head
//! reads, and read the answers (upstream's `hidden_engine.HiddenStateEngine`).

use std::path::{Path, PathBuf};

use snafu::{ResultExt, ensure};

use crate::config::DeciderConfig;
use crate::head::{PointerHead, softmax};
use crate::prompting::{option_token_index, render_question, render_state, to_answer};
use crate::schema::{Answer, Content, Question};
use crate::tokenizer::{DeciderTokenizer, bos_token_from_config};
use crate::{BackendOutputSnafu, BackendSnafu, ReadFileSnafu, Result};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Hidden states, one row of `width` values per requested position.
#[derive(Debug, Clone, PartialEq)]
pub struct HiddenRows {
    pub width: usize,
    pub data: Vec<f32>,
}

impl HiddenRows {
    #[must_use]
    pub fn n_rows(&self) -> usize {
        self.data.len().checked_div(self.width).unwrap_or(0)
    }

    /// Row `i`; empty past the last.
    #[must_use]
    pub fn row(&self, i: usize) -> &[f32] {
        self.data
            .get(i * self.width..(i + 1) * self.width)
            .unwrap_or(&[])
    }
}

/// Runs the decoder for the readout.
pub trait HiddenStates {
    /// For each question `i`, runs `state` followed by `questions[i]` as one sequence that sees
    /// no other question, and returns the decoder's final hidden states (after its final norm)
    /// at the positions `rows[i]`, counted from the question's first token, in that order.
    fn hidden_states(
        &mut self,
        state: &[u32],
        questions: &[Vec<u32>],
        rows: &[Vec<usize>],
    ) -> std::result::Result<Vec<HiddenRows>, BoxError>;
}

/// A request's answers, in the order its questions were given.
#[derive(Debug, Clone, PartialEq)]
pub struct Evaluation {
    pub answers: Vec<(String, Answer)>,
    /// The tokens run through the decoder: the state once, then every question.
    pub input_tokens: usize,
}

/// A checkpoint's readout: its tokenizer, pointer head and settings.
#[derive(Debug, Clone)]
pub struct Decider {
    tokenizer: DeciderTokenizer,
    head: PointerHead,
    config: DeciderConfig,
}

impl Decider {
    #[must_use]
    pub fn new(tokenizer: DeciderTokenizer, head: PointerHead, config: DeciderConfig) -> Self {
        Self {
            tokenizer,
            head,
            config,
        }
    }

    /// Loads the readout of the checkpoint in `dir`, as Hugging Face publishes it: the config,
    /// `head.safetensors`, `tokenizer.json` and `tokenizer_config.json`. The adapter in `lora/`
    /// is the backend's.
    pub fn from_checkpoint_dir(dir: &Path) -> Result<Self> {
        Self::from_config_file(&crate::config::config_path(dir))
    }

    /// Loads the readout whose config is at `config`, under any of its names, with
    /// `head.safetensors`, `tokenizer.json` and an optional `tokenizer_config.json` beside it.
    pub fn from_config_file(config: &Path) -> Result<Self> {
        let dir = config.parent().unwrap_or_else(|| Path::new("."));
        let config = DeciderConfig::from_file(config)?;
        let head_path = dir.join("head.safetensors");
        let head_bytes = std::fs::read(&head_path).context(ReadFileSnafu { path: head_path })?;
        let head = PointerHead::from_safetensors(&head_bytes, config.pointer_dim)?;
        let tokenizer_config = dir.join("tokenizer_config.json");
        let bos = if tokenizer_config.exists() {
            let text = std::fs::read_to_string(&tokenizer_config).context(ReadFileSnafu {
                path: PathBuf::from(&tokenizer_config),
            })?;
            serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .as_ref()
                .and_then(bos_token_from_config)
        } else {
            None
        };
        let tokenizer = DeciderTokenizer::from_file(
            &dir.join("tokenizer.json"),
            bos.as_deref(),
            config.force_bos,
        )?;
        Ok(Self::new(tokenizer, head, config))
    }

    #[must_use]
    pub fn config(&self) -> &DeciderConfig {
        &self.config
    }

    #[must_use]
    pub fn tokenizer(&self) -> &DeciderTokenizer {
        &self.tokenizer
    }

    /// Answers `questions` about `state`, each scored on its own against the state.
    pub fn evaluate<B: HiddenStates + ?Sized>(
        &self,
        backend: &mut B,
        state: &Content,
        questions: &[(String, Question)],
    ) -> Result<Evaluation> {
        let rendered: Vec<_> = questions.iter().map(|(_, q)| render_question(q)).collect();
        let texts: Vec<&str> = rendered.iter().map(|rq| rq.text.as_str()).collect();
        let fitted = self
            .tokenizer
            .fit(&render_state(state), &texts, self.config.max_length)?;
        // Found before the backend runs, so a question whose options were cut is refused
        // without the work. Each question's last row is its last token, `<answer>`.
        let rows = rendered
            .iter()
            .zip(&fitted.offsets)
            .zip(&fitted.questions)
            .map(|((rq, offsets), ids)| {
                let mut rows = option_token_index(offsets, &rq.option_spans)?;
                rows.push(ids.len().saturating_sub(1));
                Ok(rows)
            })
            .collect::<Result<Vec<_>>>()?;
        let hidden = backend
            .hidden_states(&fitted.state, &fitted.questions, &rows)
            .context(BackendSnafu)?;
        ensure!(
            hidden.len() == questions.len(),
            BackendOutputSnafu {
                message: format!("{} results for {} questions", hidden.len(), questions.len()),
            }
        );
        let mut answers = Vec::with_capacity(questions.len());
        for (((name, _), rq), (h, r)) in questions
            .iter()
            .zip(&rendered)
            .zip(hidden.iter().zip(&rows))
        {
            ensure!(
                h.width == self.head.hidden_size() && h.n_rows() == r.len(),
                BackendOutputSnafu {
                    message: format!(
                        "{} rows of width {} for question '{name}'; the head needs {} of width {}",
                        h.n_rows(),
                        h.width,
                        r.len(),
                        self.head.hidden_size()
                    ),
                }
            );
            let options: Vec<&[f32]> = (0..r.len() - 1).map(|i| h.row(i)).collect();
            let logits = self.head.logits(h.row(r.len() - 1), &options);
            let probs = softmax(&logits, self.config.temperature(rq.kind));
            answers.push((
                name.clone(),
                to_answer(rq, &probs, self.config.ordinal_smoothing),
            ));
        }
        Ok(Evaluation {
            answers,
            input_tokens: fitted.state.len() + fitted.questions.iter().map(Vec::len).sum::<usize>(),
        })
    }
}
