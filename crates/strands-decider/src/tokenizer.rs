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
//! The checkpoint's own Hugging Face tokenizer, so token ids and offsets match training.

use std::path::Path;

use snafu::{OptionExt, ResultExt, ensure};
use tokenizers::Tokenizer;
use tokenizers::processors::template::TemplateProcessing;

use crate::{BosSnafu, NoQuestionsSnafu, PromptTooLongSnafu, Result, TokenizerSnafu};

/// A tokenized state and its questions, fitted to the model's window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fitted {
    /// The state's tokens, with the tokenizer's special tokens (a BOS, where it adds one).
    pub state: Vec<u32>,
    /// Each question's tokens, which continue the state: no special tokens.
    pub questions: Vec<Vec<u32>>,
    /// Each question token's byte range in the question's text.
    pub offsets: Vec<Vec<(usize, usize)>>,
}

#[derive(Debug, Clone)]
pub struct DeciderTokenizer {
    inner: Tokenizer,
}

impl DeciderTokenizer {
    /// Loads `tokenizer.json`. With `force_bos`, makes the tokenizer prepend `bos` (the
    /// checkpoint's `tokenizer_config.json` names it) whenever it adds special tokens, as
    /// upstream's `ensure_bos` does; a tokenizer that already adds it, or a checkpoint with no
    /// BOS token, is left as it is.
    pub fn from_file(path: &Path, bos: Option<&str>, force_bos: bool) -> Result<Self> {
        let mut inner = Tokenizer::from_file(path).context(TokenizerSnafu {
            message: format!("could not load '{}'", path.display()),
        })?;
        // Upstream tokenizes with padding and truncation off; `fit` refuses a long prompt.
        inner.with_padding(None);
        inner.with_truncation(None).context(TokenizerSnafu {
            message: "could not turn truncation off",
        })?;
        let mut tokenizer = Self { inner };
        if force_bos && let Some(bos) = bos {
            tokenizer.ensure_bos(bos)?;
        }
        Ok(tokenizer)
    }

    fn ensure_bos(&mut self, bos: &str) -> Result<()> {
        let Some(bos_id) = self.inner.token_to_id(bos) else {
            return Ok(());
        };
        if self.first_token("x")? == Some(bos_id) {
            return Ok(());
        }
        let template = TemplateProcessing::builder()
            .try_single(format!("{bos} $A"))
            .and_then(|b| b.try_pair(format!("{bos} $A $B")))
            .and_then(|b| {
                b.special_tokens(vec![(bos.to_string(), bos_id)])
                    .build()
                    .map_err(|e| e.to_string())
            })
            .map_err(|message| BosSnafu { message }.build())?;
        self.inner.with_post_processor(Some(template));
        ensure!(
            self.first_token("x")? == Some(bos_id),
            BosSnafu {
                message: "the tokenizer still adds no BOS token",
            }
        );
        Ok(())
    }

    fn first_token(&self, text: &str) -> Result<Option<u32>> {
        let encoding = self.inner.encode(text, true).context(TokenizerSnafu {
            message: "could not tokenize",
        })?;
        Ok(encoding.get_ids().first().copied())
    }

    /// Tokenizes the state (with special tokens) and each question (without), as upstream's
    /// `fit_window`. Refuses a prompt whose state and longest question pass `max_length`.
    pub fn fit(&self, state: &str, questions: &[&str], max_length: usize) -> Result<Fitted> {
        let encodings =
            self.inner
                .encode_batch(questions.to_vec(), false)
                .context(TokenizerSnafu {
                    message: "could not tokenize the questions",
                })?;
        let question_ids: Vec<Vec<u32>> = encodings.iter().map(|e| e.get_ids().to_vec()).collect();
        let offsets = encodings.iter().map(|e| e.get_offsets().to_vec()).collect();
        let longest = question_ids
            .iter()
            .map(Vec::len)
            .max()
            .context(NoQuestionsSnafu)?;
        let state_ids = self
            .inner
            .encode(state, true)
            .context(TokenizerSnafu {
                message: "could not tokenize the state",
            })?
            .get_ids()
            .to_vec();
        ensure!(
            state_ids.len() + longest <= max_length,
            PromptTooLongSnafu {
                state: state_ids.len(),
                question: longest,
                max_length,
            }
        );
        Ok(Fitted {
            state: state_ids,
            questions: question_ids,
            offsets,
        })
    }

    /// The underlying Hugging Face tokenizer.
    #[must_use]
    pub fn inner(&self) -> &Tokenizer {
        &self.inner
    }
}

/// The BOS token `tokenizer_config.json` names, written either as a string or as an added
/// token with its `content`.
#[must_use]
pub fn bos_token_from_config(config: &serde_json::Value) -> Option<String> {
    match config.get("bos_token")? {
        serde_json::Value::String(token) => Some(token.clone()),
        serde_json::Value::Object(token) => token.get("content")?.as_str().map(str::to_string),
        _ => None,
    }
}
