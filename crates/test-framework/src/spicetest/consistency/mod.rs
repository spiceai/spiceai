/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::time::{Duration, SystemTime};

use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateEmbeddingRequest,
        EncodingFormat,
    },
    Client as OpenAIClient,
};
use reqwest::Client;
use worker::{ConsistencyWorker, ConsistencyWorkerResult, WorkerHandle};

use crate::{
    metrics::{NoExtendedMetrics, QueryMetric},
    spiced::SpicedInstance,
};

pub trait TestConfig {}
mod worker;

pub struct ConsistencyConfig {
    pub duration: Duration,
    pub buckets: usize,
    pub concurrency: usize,
    pub component: ConsistencyComponent,
}

#[derive(Clone)]
pub enum ConsistencyComponent {
    Model { model: String, api_base: String },
    Embedding { embedding: String, api_base: String },
}

impl ConsistencyComponent {
    fn api_base(&self) -> String {
        match self {
            ConsistencyComponent::Model { api_base, .. } => api_base.clone(),
            ConsistencyComponent::Embedding { api_base, .. } => api_base.clone(),
        }
    }
    pub async fn send_request(&self, client: &Client, payload: &str) -> anyhow::Result<Duration> {
        let c = OpenAIClient::with_config(OpenAIConfig::default().with_api_base(self.api_base()))
            .with_http_client(client.clone())
            .clone();

        let start_time = SystemTime::now();
        match self {
            ConsistencyComponent::Model { model, .. } => {
                let req = CreateChatCompletionRequestArgs::default()
                    .model(model.clone())
                    .messages(vec![ChatCompletionRequestMessage::User(
                        ChatCompletionRequestUserMessageArgs::default()
                            .content(payload.to_string())
                            .build()
                            .expect("failed to build user message"),
                    )])
                    .build()
                    .expect("failed to build model request");
                let _ = c.chat().create(req).await?;
            }
            ConsistencyComponent::Embedding { embedding, .. } => {
                let _ = c
                    .embeddings()
                    .create(CreateEmbeddingRequest {
                        model: embedding.clone(),
                        input: async_openai::types::EmbeddingInput::String(payload.to_string()),
                        encoding_format: Some(EncodingFormat::Float),
                        user: None,
                        dimensions: None,
                    })
                    .await?;
            }
        }
        Ok(start_time.elapsed()?)
    }
}

pub enum ConsistencyState {
    NotStarted,
    Running { worker_handles: Vec<WorkerHandle> },
    Completed { result: ConsistencyWorkerResult },
}

pub struct ConsistencySpiceTest {
    start_time: Option<SystemTime>,
    spiced_instance: SpicedInstance,
    config: ConsistencyConfig,
    state: ConsistencyState,
}

impl ConsistencySpiceTest {
    pub fn new(spiced_instance: SpicedInstance, config: ConsistencyConfig) -> Self {
        Self {
            start_time: None,
            spiced_instance,
            config,
            state: ConsistencyState::NotStarted,
        }
    }

    pub async fn start(self) -> anyhow::Result<ConsistencySpiceTest> {
        if !matches!(self.state, ConsistencyState::NotStarted) {
            return Err(anyhow::anyhow!("Test already started"));
        };

        let client = self.spiced_instance.http_client()?;

        let start_time = SystemTime::now();
        let worker_handles = (0..self.config.concurrency)
            .map(|id| {
                let worker = ConsistencyWorker::new(
                    id,
                    self.config.duration.clone(),
                    self.config.buckets.clone(),
                    client.clone(),
                    self.config.component.clone(),
                );
                worker.start()
            })
            .collect();

        Ok(ConsistencySpiceTest {
            start_time: Some(start_time),
            spiced_instance: self.spiced_instance,
            config: self.config,
            state: ConsistencyState::Running { worker_handles },
        })
    }

    pub async fn wait(self) -> anyhow::Result<ConsistencySpiceTest> {
        match self.state {
            ConsistencyState::Running { worker_handles } => {
                let mut error_count = 0;

                let mut durations: Vec<Vec<Duration>> = vec![vec![]; self.config.buckets];

                for worker_handle in worker_handles {
                    match worker_handle.await {
                        Ok(worker_result) => {
                            for (i, minute) in worker_result.durations.iter().enumerate() {
                                durations[i].extend(minute);
                            }
                            error_count += worker_result.error_count;
                        }
                        Err(_) => {
                            return Err(anyhow::anyhow!("Worker failed"));
                        }
                    }
                }

                Ok(ConsistencySpiceTest {
                    start_time: self.start_time,
                    spiced_instance: self.spiced_instance,
                    config: self.config,
                    state: ConsistencyState::Completed {
                        result: ConsistencyWorkerResult {
                            durations,
                            error_count,
                        },
                    },
                })
            }
            ConsistencyState::NotStarted => Err(anyhow::anyhow!("Test not started")),
            ConsistencyState::Completed { .. } => Err(anyhow::anyhow!("Test already completed")),
        }
    }

    pub fn get_result(&self) -> anyhow::Result<Vec<QueryMetric<NoExtendedMetrics>>> {
        match self.state {
            ConsistencyState::Completed { ref result, .. } => result
                .durations
                .iter()
                .enumerate()
                .map(|(i, durations)| {
                    QueryMetric::new_from_durations(format!("Minute {i}").as_str(), durations)
                })
                .collect(),
            ConsistencyState::NotStarted => Err(anyhow::anyhow!("Test not started")),
            ConsistencyState::Running { .. } => Err(anyhow::anyhow!("Test still running")),
        }
    }
}
