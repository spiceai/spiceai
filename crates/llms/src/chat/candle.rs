/*
Copyright 2024 The Spice.ai OSS Authors
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
#![allow(clippy::needless_pass_by_value)]
use std::path::Path;

use async_trait::async_trait;
use candle_examples::token_output_stream::TokenOutputStream;
use candle_transformers::{generation::LogitsProcessor, models::quantized_llama::ModelWeights};

use snafu::ResultExt;
use tokenizers::Tokenizer;

use super::{Chat, Error as ChatError, FailedToLoadModelSnafu, FailedToLoadTokenizerSnafu, Result};
use candle_core::{quantized::gguf_file, Tensor};

struct InferenceHyperparams {
    pub to_sample: usize,
    pub max_seq_len: usize,
    pub repeat_last_n: usize,
    pub repeat_penalty: f32,
    pub device: candle_core::Device,
    pub seed: u64,
    pub temperature: f64,
    pub split_prompt: bool,
}

impl Default for InferenceHyperparams {
    fn default() -> Self {
        Self {
            to_sample: 300,
            max_seq_len: 4096,
            repeat_last_n: 64,
            repeat_penalty: 1.1,
            device: candle_core::Device::Cpu,
            seed: 299_792_458,
            temperature: 0.8,
            split_prompt: true,
        }
    }
}

#[derive(Clone)]
pub struct CandleLlama {
    tknzr: Tokenizer,
    mdl: ModelWeights,
}

#[async_trait]
impl Chat for CandleLlama {
    async fn run(&self, prompt: String) -> Result<Option<String>> {
        // tknzr.clone() is bad
        Self::perform_inference(
            prompt,
            TokenOutputStream::new(self.tknzr.clone()),
            &mut self.mdl.clone(),
        )
        .context(FailedToLoadModelSnafu)
    }
}

impl CandleLlama {
    pub fn try_new(tokenizer: &Path, model_weights: &Path) -> Result<Self> {
        Ok(Self {
            tknzr: Tokenizer::from_file(tokenizer).context(FailedToLoadTokenizerSnafu)?,
            mdl: Self::load_gguf_model_weights(model_weights.to_string_lossy().to_string())?,
        })
    }

    pub fn perform_inference(
        prompt_str: String,
        mut tos: TokenOutputStream,
        model: &mut ModelWeights,
    ) -> std::result::Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        let hyper = InferenceHyperparams::default();

        let prompt_tokens = match tos.tokenizer().encode(prompt_str, true) {
            Ok(tokens) => {
                let token_ids = [tokens.get_ids()].concat();
                if token_ids.len() + hyper.to_sample > hyper.max_seq_len - 10 {
                    let to_remove = token_ids.len() + hyper.to_sample + 10 - hyper.max_seq_len;
                    token_ids[token_ids.len().saturating_sub(to_remove)..].to_vec()
                } else {
                    token_ids
                }
            }
            Err(err) => {
                return Err(err);
            }
        };

        let mut all_tokens = vec![];
        let mut logits_processor = LogitsProcessor::new(hyper.seed, Some(hyper.temperature), None);

        let mut next_token = if hyper.split_prompt {
            let mut next_token = 0;
            for (pos, token) in prompt_tokens.iter().enumerate() {
                let input = Tensor::new(&[*token], &hyper.device)?.unsqueeze(0)?;
                let logits = model.forward(&input, pos)?;
                let logits = logits.squeeze(0)?;
                next_token = logits_processor.sample(&logits)?;
            }
            next_token
        } else {
            let input = Tensor::new(prompt_tokens.as_slice(), &hyper.device)?.unsqueeze(0)?;
            let logits = model.forward(&input, 0)?;
            let logits = logits.squeeze(0)?;
            logits_processor.sample(&logits)?
        };
        all_tokens.push(next_token);

        let eos_token = match tos.tokenizer().get_vocab(true).get("</s>") {
            Some(token) => *token,
            None => {
                return Err(ChatError::FailedToTokenize {
                    source: "Failed to get eos_token".into(),
                }
                .into());
            }
        };

        for index in 0..hyper.to_sample {
            let input = Tensor::new(&[next_token], &hyper.device)?.unsqueeze(0)?;
            let logits = {
                let logits = model.forward(&input, prompt_tokens.len() + index)?;
                let logits = logits.squeeze(0)?;
                if hyper.repeat_penalty == 0.0 {
                    logits
                } else {
                    let start_at = all_tokens.len().saturating_sub(hyper.repeat_last_n);
                    candle_transformers::utils::apply_repeat_penalty(
                        &logits,
                        hyper.repeat_penalty,
                        &all_tokens[start_at..],
                    )?
                }
            };
            next_token = logits_processor.sample(&logits)?;
            all_tokens.push(next_token);
            tos.next_token(next_token)?;
            if next_token == eos_token {
                break;
            };
        }
        tos.decode_rest().map_err(std::convert::Into::into)
    }

    fn load_gguf_model_weights(model_weights_path: String) -> Result<ModelWeights> {
        let mut file = std::fs::File::open(model_weights_path.clone()).map_err(|_| {
            ChatError::LocalModelNotFound {
                expected_path: model_weights_path.clone(),
            }
        })?;

        let model_content = gguf_file::Content::read(&mut file)
            .boxed()
            .context(FailedToLoadModelSnafu)?;

        ModelWeights::from_gguf(model_content, &mut file, &candle_core::Device::Cpu)
            .boxed()
            .context(FailedToLoadModelSnafu)
    }
}
