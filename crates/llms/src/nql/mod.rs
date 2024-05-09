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

use candle_examples::token_output_stream::TokenOutputStream;
use candle_transformers::{
    generation::LogitsProcessor,
    models::quantized_llama::ModelWeights,
};

use snafu::Snafu;
use tokenizers::Tokenizer;

use candle_core::{quantized::gguf_file, Tensor};


#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run the NSQL model"))]
    FailedToRunModel { e: Box<dyn std::error::Error> },

    #[snafu(display("Local model not found"))]
    LocalModelNotFound {},

    #[snafu(display("Failed to load model from file {e}"))]
    FailedToLoadModel { e: candle_core::Error },

    #[snafu(display("Failed to load model tokenizer"))]
    FailedToLoadTokenizer { e: Box<dyn std::error::Error> },

    #[snafu(display("Failed to tokenize"))]
    FailedToTokenize { e: Box<dyn std::error::Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


#[derive(Debug, Clone)]
pub struct NsqlConfig {
    pub tokenizer: Option<String>,
    pub model_weights: String,
}

pub trait Nsql {
    fn try_new(cfg: NsqlConfig) -> Result<Self>;
    fn run(&self, prompt: String) -> Result<Option<String>>;
}