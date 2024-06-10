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
#![allow(clippy::borrowed_box)]
#![allow(clippy::needless_pass_by_value)]

use super::{Chat, Error as ChatError, FailedToRunModelSnafu, Result};
use async_trait::async_trait;
use mistralrs::{
    Constraint, Device, DeviceMapMetadata, GGMLLoaderBuilder, GGMLSpecificConfig,
    GGUFLoaderBuilder, GGUFSpecificConfig, MistralRs, MistralRsBuilder, NormalLoaderBuilder,
    NormalRequest, Request as MistralRsquest, RequestMessage, Response as MistralRsponse,
    SamplingParams, SchedulerMethod, TokenSource,
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
    pub fn from(
        tokenizer: Option<&Path>,
        model_weights: &Path,
        tokenizer_config: &Path,
    ) -> Result<Self> {
        let extension = model_weights
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default();
        match extension {
            "ggml" => match tokenizer {
                Some(tokenizer) => Self::from_ggml(tokenizer, model_weights, tokenizer_config),
                None => Err(ChatError::FailedToLoadModel {
                    source: "Tokenizer path is required for GGML model".into(),
                }),
            },
            "gguf" => Self::from_gguf(tokenizer, model_weights, tokenizer_config),
            _ => Err(ChatError::FailedToLoadModel {
                source: format!("Unknown model type: {extension}").into(),
            }),
        }
    }

    fn create_paths(
        model_weights: &Path,
        tokenizer: Option<&Path>,
        tokenizer_config: &Path,
    ) -> Box<dyn ModelPaths> {
        Box::new(LocalModelPaths::new(
            tokenizer.map(Into::into).unwrap_or_default(),
            // Not needed for LLama2 / DuckDB Chat, but needed in `EricLBuehler/mistral.rs`.
            tokenizer.map(Into::into).unwrap_or_default(),
            tokenizer_config.into(),
            vec![model_weights.into()],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ))
    }

    fn load_gguf_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        tokenizer: Option<&Path>,
        model_id: &str,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        GGUFLoaderBuilder::new(
            GGUFSpecificConfig::default(),
            None,
            tokenizer.map(|t| t.to_string_lossy().to_string()),
            model_id.to_string(),
            model_id.to_string(),
        )
        .build()
        .load_model_from_path(
            &paths,
            None,
            device,
            false,
            DeviceMapMetadata::dummy(),
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    fn load_ggml_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        tokenizer: &Path,
        model_id: &str,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        GGMLLoaderBuilder::new(
            GGMLSpecificConfig::default(),
            None,
            Some(tokenizer.to_string_lossy().to_string()),
            None,
            String::new(),
            model_id.to_string(),
        )
        .build()
        .load_model_from_path(
            &paths,
            None,
            device,
            false,
            DeviceMapMetadata::dummy(),
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    pub fn from_gguf(
        tokenizer: Option<&Path>,
        model_weights: &Path,
        tokenizer_config: &Path,
    ) -> Result<Self> {
        let paths = Self::create_paths(model_weights, tokenizer, tokenizer_config);
        let model_id = model_weights
            .file_name()
            .and_then(|x| x.to_str())
            .map_or_else(
                || model_weights.to_string_lossy().to_string(),
                std::string::ToString::to_string,
            );

        let device = Self::get_device();

        let pipeline = Self::load_gguf_pipeline(paths, &device, tokenizer, &model_id)?;

        Self::from_pipeline(pipeline)
    }

    pub fn from_ggml(
        tokenizer: &Path,
        model_weights: &Path,
        tokenizer_config: &Path,
    ) -> Result<Self> {
        let paths = Self::create_paths(model_weights, Some(tokenizer), tokenizer_config);
        let model_id = model_weights.to_string_lossy().to_string();
        let device = Self::get_device();

        let pipeline = Self::load_ggml_pipeline(paths, &device, tokenizer, &model_id)?;

        Self::from_pipeline(pipeline)
    }
    fn get_device() -> Device {
        let default_device = {
            #[cfg(feature = "metal")]
            {
                Device::new_metal(0).unwrap_or(Device::Cpu)
            }
            #[cfg(not(feature = "metal"))]
            {
                Device::Cpu
            }
        };

        Device::cuda_if_available(0).unwrap_or(default_device)
    }

    pub fn from_hf(model_id: &str, arch: &str) -> Result<Self> {
        let model_parts: Vec<&str> = model_id.split(':').collect();

        let loader_type = mistralrs::NormalLoaderType::from_str(arch).map_err(|_| {
            ChatError::FailedToLoadModel {
                source: format!("Unknown model type: {arch}").into(),
            }
        })?;

        let builder = NormalLoaderBuilder::new(
            mistralrs::NormalSpecificConfig {
                use_flash_attn: false,
                repeat_last_n: 64,
            },
            None,
            None,
            Some(model_parts[0].to_string()),
        );

        let pipeline = builder
            .build(loader_type)
            .load_model_from_hf(
                model_parts.get(1).map(|&x| x.to_string()),
                TokenSource::CacheToken,
                None,
                &Device::Cpu,
                false,
                DeviceMapMetadata::dummy(),
                None,
            )
            .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })?;

        Self::from_pipeline(pipeline)
    }

    fn from_pipeline(p: Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>) -> Result<Self> {
        let (tx, rx) = channel(10_000);
        Ok(Self {
            pipeline: MistralRsBuilder::new(
                p,
                SchedulerMethod::Fixed(5.try_into().map_err(|_| ChatError::FailedToLoadModel {
                    source: "Couldn't create schedule method".into(),
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
impl Chat for MistralLlama {

    async fn run(&mut self, prompt: String) -> Result<Option<String>> {
        let r = self.to_request(prompt);
        self.pipeline
            .get_sender()
            .send(r)
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;
        match self.rx.recv().await {
            Some(response) => match response {
                MistralRsponse::CompletionDone(cr) => {
                    println!(
                        "total: {}, avg_tok_per_sec: {}, avg_prompt_tok_per_sec: {}, avg_compl_tok_per_sec: {}",
                        cr.usage.total_time_sec,
                        cr.usage.avg_tok_per_sec,
                        cr.usage.avg_prompt_tok_per_sec,
                        cr.usage.avg_compl_tok_per_sec
                    );
                    Ok(Some(cr.choices[0].text.clone()))
                }
                MistralRsponse::CompletionModelError(err_msg, _cr) => {
                    Err(ChatError::FailedToRunModel {
                        source: err_msg.into(),
                    })
                }
                _ => Err(ChatError::FailedToRunModel {
                    source: "Unexpected error occurred".into(),
                }),
            },
            None => Err(ChatError::FailedToRunModel {
                source: "Mistral pipeline unexpectedly closed".into(),
            }),
        }
    }
}
