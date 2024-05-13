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

use std::fs;

use async_trait::async_trait;
use candle_examples::token_output_stream::TokenOutputStream;
use candle_transformers::{
    generation::LogitsProcessor,
    models::quantized_llama::ModelWeights,
};

use tokenizers::Tokenizer;

use candle_core::{quantized::gguf_file, Tensor};
use super::{Nql, Result, Error as NqlError};

#[derive()]
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
            seed: 299792458,
            temperature: 0.8,
            split_prompt: true,
        }
    }
}

#[derive(Clone)]
pub struct CandleLlama {
    tknzr: Tokenizer,
    mdl: ModelWeights
}

#[async_trait]
impl Nql for CandleLlama {
    async fn run(&mut self, prompt: String) -> Result<Option<String>> {
        // tknzr.clone() is bad
        let stream = TokenOutputStream::new(self.tknzr.clone());
        match Self::perform_inference(prompt, stream, &mut self.mdl.clone()) {
            Ok(opt_output) => Ok(opt_output),
            Err(e) => Err(NqlError::FailedToRunModel { e }),
        }
    }
}

impl CandleLlama {
    pub fn try_new(tokenizer: String, model_weights: String) -> Result<Self>  {
        let tknzr = {
            Tokenizer::from_file(tokenizer)
                .map_err(|e| NqlError::FailedToLoadTokenizer { e })?
        };

        // let content = fs::read_to_string(tokenizer).unwrap();
        // let t = serde_json::from_str::<Tokenizer>(&content).unwrap();
        let mdl = Self::load_gguf_model_weights(model_weights)?;
        Ok(Self { tknzr, mdl })
    }

    pub fn perform_inference(
        prompt_str: String,
        mut tos: TokenOutputStream,
        model: &mut ModelWeights,
    ) -> std::result::Result<Option<String>, Box<dyn std::error::Error>> {
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
            },
            Err(err) => {
                return Err(err);
            }
        };


        let mut all_tokens = vec![];
        let mut logits_processor = LogitsProcessor::new(hyper.seed, Some(hyper.temperature), None);

        let mut next_token = if !hyper.split_prompt {
            let input = Tensor::new(prompt_tokens.as_slice(), &hyper.device)?.unsqueeze(0)?;
            let logits = model.forward(&input, 0)?;
            let logits = logits.squeeze(0)?;
            logits_processor.sample(&logits)?
        } else {
            let mut next_token = 0;
            for (pos, token) in prompt_tokens.iter().enumerate() {
                let input = Tensor::new(&[*token], &hyper.device)?.unsqueeze(0)?;
                let logits = model.forward(&input, pos)?;
                let logits = logits.squeeze(0)?;
                next_token = logits_processor.sample(&logits)?;
            }
            next_token
        };
        all_tokens.push(next_token);


        let eos_token = match tos.tokenizer().get_vocab(true).get("</s>") {
            Some(token) => *token,
            None => {
                return Err(NqlError::FailedToTokenize { e: "Failed to get eos_token".into() }.into());
            }
        };

        for index in 0..hyper.to_sample {
            let input = Tensor::new(&[next_token], &hyper.device)?.unsqueeze(0)?;
            let logits = {
                let logits = model.forward(&input, prompt_tokens.len() + index)?;
                let logits = logits.squeeze(0)?;
                if hyper.repeat_penalty == 1. {
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
            if let Some(t) = tos.next_token(next_token)? {
                print!("{t}");
            }
            if next_token == eos_token {
                break;
            };
        }
        tos.decode_rest().map_err(|e| e.into())
    }

    fn load_gguf_model_weights(model_weights_path: String) -> Result<ModelWeights> {
        let mut file =
            std::fs::File::open(model_weights_path.clone()).map_err(|_| NqlError::LocalModelNotFound {expected_path: model_weights_path.clone()})?;
        let model_content = gguf_file::Content::read(&mut file) //Content::read(&mut file, &candle_core::Device::Cpu)
            .map_err(|e| e.with_path(model_weights_path))
            .map_err(|e| NqlError::FailedToLoadModel { e: Box::new(e) })?;
    
        Ok(
            ModelWeights::from_gguf(model_content, &mut file, &candle_core::Device::Cpu)
                .map_err(|e| NqlError::FailedToLoadModel { e: Box::new(e) })?,
        )
    }
}

mod test {
    use std::fs;

    use tokenizers::{Token, Tokenizer};

    use crate::nql::Nql;

    use super::CandleLlama;

    #[test]
    fn test_candle_llama_local() {
        let content = fs::read_to_string("/Users/jeadie/.spice/llms/llama2.tokenizer.json").unwrap();
        let t = serde_json::from_str::<Tokenizer>(&content).unwrap();

        let prompt = "SELECT * FROM users WHERE id = 1;".to_string();
        // let result = candle.run(prompt)
    }
}