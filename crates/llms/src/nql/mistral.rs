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
#![allow(clippy::module_name_repetitions)]

use super::{Error as NqlError, Nql, Result};

use async_trait::async_trait;
use mistralrs::{
    Constraint, DeviceMapMetadata, GGMLLoaderBuilder, GGMLSpecificConfig, GGUFLoaderBuilder,
    GGUFSpecificConfig, MistralRs, MistralRsBuilder, NormalLoaderBuilder, NormalRequest,
    Request as MistralRsquest, RequestMessage, Response as MistralRsponse, SamplingParams,
    SchedulerMethod, TokenSource,
};
use mistralrs_core::{LocalModelPaths, ModelPaths, Pipeline};
use snafu::ResultExt;

use std::{path::Path, str::FromStr, sync::Arc};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct MistralLlama {
    pipeline: Arc<MistralRs>,
    tx: Sender<MistralRsponse>,
    rx: Receiver<MistralRsponse>,
}

impl MistralLlama {
    pub fn from(tokenizer: &Path, model_weights: &Path, template_filename: &Path) -> Result<Self> {
        match model_weights.extension().unwrap_or_default().to_str() {
            Some("ggml") => Self::from_ggml(tokenizer, model_weights, template_filename),
            Some("gguf") => Self::from_gguf(tokenizer, model_weights, template_filename),
            _ => Err(NqlError::FailedToLoadModel {
                source: "Unknown model type {}".into(),
            }),
        }
    }

    pub fn from_gguf(
        tokenizer: &Path,
        model_weights: &Path,
        template_filename: &Path,
    ) -> Result<Self> {
        let paths: Box<dyn ModelPaths> = Box::new(LocalModelPaths::new(
            tokenizer.into(),
            // Not needed for LLama2 / DuckDB NQL, but needed in `EricLBuehler/mistral.rs`.
            tokenizer.into(),
            template_filename.into(),
            vec![model_weights.into()],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let pipeline = GGUFLoaderBuilder::new(
            GGUFSpecificConfig::default(),
            None,
            Some(tokenizer.to_string_lossy().to_string()),
            Some("None".to_string()),
            String::new(),
            model_weights.to_string_lossy().to_string(),
        )
        .build()
        .load_model_from_path(
            &paths,
            None,
            &candle_core_rs::Device::Cpu,
            false,
            DeviceMapMetadata::dummy(),
            None,
        )
        .map_err(|e| NqlError::FailedToLoadModel { source: e.into() })?;

        Self::from_pipeline(pipeline)
    }

    pub fn from_ggml(
        tokenizer: &Path,
        model_weights: &Path,
        template_filename: &Path,
    ) -> Result<Self> {
        let paths: Box<dyn ModelPaths> = Box::new(LocalModelPaths::new(
            tokenizer.into(),
            // Not needed for LLama2 / DuckDB NQL, but needed in `EricLBuehler/mistral.rs`.
            tokenizer.into(),
            template_filename.into(),
            vec![model_weights.into()],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let pipeline = GGMLLoaderBuilder::new(
            GGMLSpecificConfig::default(),
            None,
            Some(tokenizer.to_string_lossy().to_string()),
            None,
            String::new(),
            model_weights.to_string_lossy().to_string(),
        )
        .build()
        .load_model_from_path(
            &paths,
            None,
            &candle_core_rs::Device::Cpu,
            false,
            DeviceMapMetadata::dummy(),
            None,
        )
        .map_err(|e| NqlError::FailedToLoadModel { source: e.into() })?;

        Self::from_pipeline(pipeline)
    }

    pub fn from_hf(model_id: &str, arch: &str) -> Result<Self> {
        let model_parts = model_id.split(':').collect::<Vec<&str>>();
        if model_parts.len() != 2 {
            return Err(NqlError::FailedToLoadModel {
                source: format!("Invalid model id {model_id}").into(),
            });
        }

        let builder = NormalLoaderBuilder::new(
            mistralrs::NormalSpecificConfig {
                use_flash_attn: false,
                repeat_last_n: 64,
            },
            None,
            None,
            Some(model_parts[0].to_string()),
        );

        let Ok(loader_type) = mistralrs::NormalLoaderType::from_str(arch) else {
            return Err(NqlError::FailedToLoadModel {
                source: format!("Unknown model type {arch}").into(),
            });
        };

        let pipeline = builder
            .build(loader_type)
            .load_model_from_hf(
                model_parts.get(1).map(std::string::ToString::to_string),
                TokenSource::CacheToken,
                None,
                &candle_core_rs::Device::Cpu,
                false,
                DeviceMapMetadata::dummy(),
                None,
            )
            .map_err(|e| NqlError::FailedToLoadModel { source: e.into() })?;

        Self::from_pipeline(pipeline)
    }

    fn from_pipeline(p: Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>) -> Result<Self> {
        let (tx, rx) = channel(10_000);
        Ok(Self {
            pipeline: MistralRsBuilder::new(
                p,
                SchedulerMethod::Fixed(5.try_into().map_err(|_| NqlError::FailedToLoadModel {
                    source: "couldn't create schedule method".into(),
                })?),
            )
            .build(),
            tx,
            rx,
        })
    }

    fn to_request(&self, prompt: String) -> MistralRsquest {
        MistralRsquest::Normal(NormalRequest {
            messages: RequestMessage::Completion {
                text: prompt,
                echo_prompt: false,
                best_of: 1,
            },
            sampling_params: SamplingParams::default(),
            response: self.tx.clone(),
            return_logprobs: false,
            is_streaming: false,
            id: 0,
            constraint: Constraint::None,
            suffix: None,
            adapters: None,
        })
    }
}

#[async_trait]
impl Nql for MistralLlama {
    async fn run(&mut self, prompt: String) -> Result<Option<String>> {
        let r = self.to_request(prompt);
        self.pipeline
            .get_sender()
            .send(r)
            .await
            .boxed()
            .context(super::FailedToRunModelSnafu)?;

        match self.rx.recv().await {
            Some(response) => match response {
                MistralRsponse::CompletionDone(cr) => Ok(Some(cr.choices[0].text.clone())),
                MistralRsponse::CompletionModelError(err_msg, _cr) => {
                    Err(NqlError::FailedToRunModel {
                        source: err_msg.into(),
                    })
                }
                _ => Err(NqlError::FailedToRunModel {
                    source: "Unexpected error occurred".into(),
                }),
            },
            None => Err(NqlError::FailedToRunModel {
                source: "Mistral pipeline unexpectedly closed".into(),
            }),
        }
    }
}
