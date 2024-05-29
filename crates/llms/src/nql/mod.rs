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
use std::path::Path;

use async_openai::config::OpenAIConfig;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use self::openai::GPT3_5_TURBO_INSTRUCT;

#[cfg(feature = "candle")]
pub mod candle;

#[cfg(feature = "mistralrs")]
pub mod mistral;

pub mod openai;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum NSQLRuntime {
    Candle,
    Mistral,
    Openai,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run the NSQL model: {source}"))]
    FailedToRunModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Local model, expected at {expected_path}, not found"))]
    LocalModelNotFound { expected_path: String },

    #[snafu(display("Local tokenizer, expected at {expected_path}, not found"))]
    LocalTokenizerNotFound { expected_path: String },

    #[snafu(display("Failed to load model from file: {source}"))]
    FailedToLoadModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load model tokenizer: {source}"))]
    FailedToLoadTokenizer {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to tokenize: {source}"))]
    FailedToTokenize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported source of model: {source}"))]
    UnknownModelSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Nql: Sync + Send {
    async fn run(&mut self, prompt: String) -> Result<Option<String>>;
}

#[must_use]
pub fn create_openai(
    model: Option<String>,
    api_base: Option<String>,
    api_key: Option<String>,
    org_id: Option<String>,
    project_id: Option<String>,
) -> Box<dyn Nql> {
    let mut cfg = OpenAIConfig::new()
        .with_api_base(api_base.unwrap_or_default())
        .with_org_id(org_id.unwrap_or_default())
        .with_project_id(project_id.unwrap_or_default());

    // If an API key is provided, use it. Otherwise use default from env variables.
    if let Some(api_key) = api_key {
        cfg = cfg.with_api_key(api_key);
    }

    Box::new(openai::Openai::new(
        cfg,
        model.unwrap_or(GPT3_5_TURBO_INSTRUCT.to_string()),
    ))
}

pub fn create_hf_model(
    model_id: &str,
    model_type: Option<String>,
    model_weights: &Option<String>,
    _tokenizer: &Option<String>,
    _tokenizer_config: &Option<String>,
) -> Result<Box<dyn Nql>> {
    if model_type.is_none() && model_weights.is_none() {
        return Err(Error::FailedToLoadModel {
            source: format!("For {model_id} either model type or model weights is required").into(),
        });
    };

    #[cfg(feature = "mistralrs")]
    {
        mistral::MistralLlama::from_hf(
            model_id,
            &model_type.unwrap_or_default(),
            // TODO: Support HF models with non-standard paths.
            // model_weights,
            // tokenizer,
            // tokenizer_config,
        )
        .map(|x| Box::new(x) as Box<dyn Nql>)
    }
    #[cfg(not(feature = "mistralrs"))]
    {
        Err(Error::FailedToRunModel {
            source: "No NQL model feature enabled".into(),
        })
    }
}

#[allow(unused_variables)]
pub fn create_local_model(
    model_weights: &str,
    tokenizer: &str,
    tokenizer_config: &str,
) -> Result<Box<dyn Nql>> {
    let w = Path::new(&model_weights);
    let t = Path::new(&tokenizer);
    let tc = Path::new(&tokenizer_config);

    if !w.exists() {
        return Err(Error::LocalModelNotFound {
            expected_path: w.to_string_lossy().to_string(),
        });
    }

    if !t.exists() {
        return Err(Error::LocalTokenizerNotFound {
            expected_path: t.to_string_lossy().to_string(),
        });
    }

    #[cfg(feature = "mistralrs")]
    {
        mistral::MistralLlama::from(t, w, tc).map(|x| Box::new(x) as Box<dyn Nql>)
    }
    #[cfg(not(feature = "mistralrs"))]
    {
        Err(Error::FailedToRunModel {
            source: "No NQL model feature enabled".into(),
        })
    }
}
