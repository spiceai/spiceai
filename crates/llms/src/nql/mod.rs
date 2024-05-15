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
#![allow(clippy::missing_errors_doc)]
use async_trait::async_trait;
use snafu::Snafu;

#[cfg(feature = "candle")]
pub mod candle;

#[cfg(feature = "mistralrs")]
pub mod mistral;

pub enum LlmRuntime {
    Mistral,
    Candle,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run the NSQL model"))]
    FailedToRunModel {
        e: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Local model, expected at {expected_path}, not found"))]
    LocalModelNotFound { expected_path: String },

    #[snafu(display("Local tokenizer, expected at {expected_path}, not found"))]
    LocalTokenizerNotFound { expected_path: String },

    #[snafu(display("Failed to load model from file {e}"))]
    FailedToLoadModel {
        e: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load model tokenizer"))]
    FailedToLoadTokenizer {
        e: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to tokenize"))]
    FailedToTokenize {
        e: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Nql: Sync + Send {
    async fn run(&mut self, prompt: String) -> Result<Option<String>>;
}

/// Loads `https://huggingface.co/motherduckdb/DuckDB-NSQL-7B-v0.1-GGUF` local spice llms cache (i.e. ~/.spice/llms/),
/// based on the configured features, and the preference order: [`mistralrs`, `candle`].
#[allow(unreachable_patterns)]
pub fn try_duckdb_from_spice_local(runtime: &LlmRuntime) -> Result<Box<dyn Nql>> {
    let spice_dir =
        dirs::home_dir()
            .map(|x| x.join(".spice/llms"))
            .ok_or(Error::LocalModelNotFound {
                expected_path: "~/.spice/llms".to_string(),
            })?;

    let model_weights = spice_dir.join("DuckDB-NSQL-7B-v0.1-q8_0.gguf");
    let tokenizer = spice_dir.join("llama2.tokenizer_2.json");

    if !model_weights.exists() {
        return Err(Error::LocalModelNotFound {
            expected_path: model_weights.to_string_lossy().to_string(),
        });
    }

    if !tokenizer.exists() {
        return Err(Error::LocalTokenizerNotFound {
            expected_path: tokenizer.to_string_lossy().to_string(),
        });
    }

    match runtime {
        #[cfg(feature = "candle")]
        LlmRuntime::Candle => candle::CandleLlama::try_new(
            tokenizer.to_string_lossy().to_string(),
            model_weights.to_string_lossy().to_string(),
        )
        .map(|x| Box::new(x) as Box<dyn Nql>),
        #[cfg(feature = "mistralrs")]
        LlmRuntime::Mistral => {
            let template_file = spice_dir.join("template.json");
            if !template_file.exists() {
                return Err(Error::LocalTokenizerNotFound {
                    expected_path: template_file.to_string_lossy().to_string(),
                });
            }
            mistral::MistralLlama::try_new(
                tokenizer.to_string_lossy().to_string(),
                model_weights.to_string_lossy().to_string(),
                template_file.to_string_lossy().as_ref(),
            )
            .map(|x| Box::new(x) as Box<dyn Nql>)
        }
        _ => Err(Error::FailedToRunModel {
            e: "No NQL model feature enabled".into(),
        }),
    }
}
