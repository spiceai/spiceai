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

use std::sync::Arc;

use async_openai::types::{ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs};
use scheduler::Result;
use scheduler::task::ScheduledTask;
use spicepod::component::worker::Worker;
use tonic::async_trait;
use tracing_futures::Instrument;

use crate::Runtime;
use crate::component::dataset::Dataset;

pub struct DatasetRefreshTask(Arc<Dataset>);

impl From<Arc<Dataset>> for DatasetRefreshTask {
    fn from(dataset: Arc<Dataset>) -> Self {
        Self(dataset)
    }
}

#[async_trait]
impl ScheduledTask for DatasetRefreshTask {
    async fn execute(&self) -> Result<()> {
        let dataset = Arc::clone(&self.0);
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "scheduler::refresh", input = %dataset.name.to_string());
        async {
            match dataset
                .runtime()
                .datafusion()
                .refresh_table(&dataset.name, None)
                .await
            {
                Ok(notifier) => {
                    if let Some(notifier) = notifier {
                        notifier.notified().await;
                    }
                    Ok(())
                }
                Err(e) => Err(scheduler::Error::RefreshTaskFailure { source: e.into() }),
            }
        }
        .instrument(span)
        .await
    }
}

pub struct WorkerPromptTask {
    runtime: Arc<Runtime>,
    worker: Arc<Worker>,
    prompt: Arc<str>,
}

impl WorkerPromptTask {
    pub fn new(runtime: Arc<Runtime>, worker: Arc<Worker>, prompt: Arc<str>) -> Self {
        Self {
            runtime,
            worker,
            prompt,
        }
    }
}

#[async_trait]
impl ScheduledTask for WorkerPromptTask {
    async fn execute(&self) -> Result<()> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "scheduler::worker", input = %self.prompt, worker = %self.worker.name, prompt = %self.prompt);

        async {
            let worker = Arc::clone(&self.worker);
            let prompt = Arc::clone(&self.prompt);
            let runtime = Arc::clone(&self.runtime);

            let workers_lock = Arc::clone(&runtime.workers);
            let workers = workers_lock.read().await;
            let Some(worker) = workers.get(&worker.name) else {
                tracing::debug!("Worker not found for ScheduledTask: {}", worker.name);
                return Ok(());
            };

            tracing::debug!(
                "Executing worker prompt task for worker: {}, prompt: {}",
                worker.name(),
                prompt
            );

            let Some(model) = Arc::clone(worker).as_model() else {
                tracing::debug!(
                    "Worker is not a model worker, skipping prompt execution: {}",
                    worker.name()
                );
                return Ok(());
            };

            let Ok(message_args) = ChatCompletionRequestUserMessageArgs::default()
                .content(prompt.to_string())
                .build()
            else {
                tracing::error!(
                    "Failed to build chat completion request message for worker '{}'",
                    worker.name()
                );
                return Ok(());
            };

            let Ok(chat_request) = CreateChatCompletionRequestArgs::default()
                .messages(vec![message_args.into()])
                .build()
            else {
                tracing::error!(
                    "Failed to build chat completion request for worker '{}'",
                    worker.name()
                );

                return Ok(());
            };

            if let Err(e) = model.chat_request(chat_request).await {
                tracing::error!(
                    "Failed to execute worker prompt task for worker '{}': {e}",
                    worker.name(),
                );
            }

            Ok(())
        }
        .instrument(span)
        .await
    }
}
