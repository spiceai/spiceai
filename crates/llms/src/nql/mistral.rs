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


use super::{Nql, Result, Error as NqlError};

use async_trait::async_trait;
use mistralrs::{Constraint, DeviceMapMetadata, GGMLLoaderBuilder, GGMLSpecificConfig, MistralRs, MistralRsBuilder, Request as MistralRsquest, RequestMessage, Response as MistralRsponse, SamplingParams, SchedulerMethod};

use tokio::sync::mpsc::{channel, Sender, Receiver};
use std::sync::Arc;


pub struct MistralLlama {
    pipeline: Arc<MistralRs>,
    tx: Sender<MistralRsponse>,
    rx: Receiver<MistralRsponse>,
}

impl MistralLlama {
    pub fn try_new(tokenizer: String, model_weights: String) -> Result<Self>  {
        let pipeline = GGMLLoaderBuilder::new(
            GGMLSpecificConfig::default(),
        None,
    Some(tokenizer), 
    Some("fake_model_id".to_string()), 
    "s".to_string(), 
    model_weights,
        ).build().load_model(None, mistralrs::TokenSource::CacheToken, None, &candle_core_rs::Device::Cpu, false, DeviceMapMetadata::dummy(), None).map_err(
            |e| NqlError::FailedToLoadModel { e: e.into() }
        )?;

        let (tx, rx) = channel(10_000);
        Ok(Self { 
            pipeline: MistralRsBuilder::new(pipeline, SchedulerMethod::Fixed(5.try_into().unwrap())).build(),
            tx, 
            rx
        })
    }

    fn to_request(&self, prompt: String) -> MistralRsquest {
        MistralRsquest{
            messages: RequestMessage::Completion { text: prompt, echo_prompt: false, best_of: 1 },
            sampling_params: SamplingParams::default(),
            response: self.tx.clone(),
            return_logprobs: false,
            is_streaming: false,
            id: 0,
            constraint: Constraint::None,
            suffix: None,
        }
    }
}

#[async_trait]
impl Nql for MistralLlama{
    async fn run(&mut self, prompt: String) -> Result<Option<String>> {
        let r = self.to_request(prompt);

        self.pipeline.get_sender().blocking_send(r).map_err(|e| NqlError::FailedToRunModel { e: e.into() })?;

        match self.rx.recv().await.unwrap() {
            MistralRsponse::CompletionDone(cr) => {
                Ok(Some(cr.choices[0].text.clone()))
            },
            MistralRsponse::CompletionModelError(err_msg, _cr ) => {
                return Err(NqlError::FailedToRunModel { e: err_msg.into() });
            }
            _ => Err(NqlError::FailedToRunModel { e: "Unexpected error occurred".into() }),
        }
    }
}

mod test {
    use crate::nql::{try_duckdb_from_spice_local, LlmRuntime};

    #[test]
    fn test_candle_llama_local() {
        let mut llama = try_duckdb_from_spice_local(LlmRuntime::Mistral).unwrap();
        let res = llama.run("SELECT * FROM users WHERE id = 1".to_string());
    }
}