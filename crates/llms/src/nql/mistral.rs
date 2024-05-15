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
    Constraint, DeviceMapMetadata, GGUFLoaderBuilder, GGUFSpecificConfig, MistralRs,
    MistralRsBuilder, NormalRequest, Request as MistralRsquest, RequestMessage,
    Response as MistralRsponse, SamplingParams, SchedulerMethod,
};
use mistralrs_core::{ModelPaths, SimpleModelPaths};

use std::{path::PathBuf, str::FromStr, sync::Arc};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct MistralLlama {
    pipeline: Arc<MistralRs>,
    tx: Sender<MistralRsponse>,
    rx: Receiver<MistralRsponse>,
}

impl MistralLlama {
    pub fn try_new(
        tokenizer: String,
        model_weights: String,
        template_filename: &str,
    ) -> Result<Self> {
        let paths: Box<dyn ModelPaths> = Box::new(SimpleModelPaths::new(
            PathBuf::from_str(&tokenizer).map_err(|_| super::Error::LocalTokenizerNotFound {
                expected_path: tokenizer.clone(),
            })?,
            PathBuf::from_str(&tokenizer).map_err(|_| super::Error::LocalTokenizerNotFound {
                expected_path: tokenizer.clone(),
            })?,
            PathBuf::from_str(template_filename).map_err(|_| {
                super::Error::LocalTokenizerNotFound {
                    expected_path: tokenizer.clone(),
                }
            })?,
            vec![PathBuf::from_str(&model_weights).map_err(|_| {
                super::Error::LocalModelNotFound {
                    expected_path: model_weights.clone(),
                }
            })?],
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
            Some(tokenizer),
            Some("motherduckdb/DuckDB-NSQL-7B-v0.1-GGUF".to_string()),
            "quantized_model_id".to_string(),
            model_weights,
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
        .map_err(|e| NqlError::FailedToLoadModel { e: e.into() })?;

        let (tx, rx) = channel(10_000);
        Ok(Self {
            pipeline: MistralRsBuilder::new(
                pipeline,
                SchedulerMethod::Fixed(5.try_into().map_err(|_| NqlError::FailedToLoadModel {
                    e: "couldn't create schedule method".into(),
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
            .map_err(|e| NqlError::FailedToRunModel { e: e.into() })?;

        match self.rx.recv().await {
            Some(response) => match response {
                MistralRsponse::CompletionDone(cr) => Ok(Some(cr.choices[0].text.clone())),
                MistralRsponse::CompletionModelError(err_msg, _cr) => {
                    Err(NqlError::FailedToRunModel { e: err_msg.into() })
                }
                _ => Err(NqlError::FailedToRunModel {
                    e: "Unexpected error occurred".into(),
                }),
            },
            None => Err(NqlError::FailedToRunModel {
                e: "Mistral pipeline unexpectedly closed".into(),
            }),
        }
    }
}
