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

use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs,
};
use scheduler::Result;
use scheduler::task::ScheduledTask;
use tonic::async_trait;
use tracing_futures::Instrument;

use crate::Runtime;
use crate::http::v1::run_sql;

pub struct WorkerPromptTask {
    runtime: Arc<Runtime>,
    worker_name: Arc<str>,
    prompt: Arc<str>,
}

impl WorkerPromptTask {
    pub fn new(runtime: Arc<Runtime>, worker_name: Arc<str>, prompt: Arc<str>) -> Self {
        Self {
            runtime,
            worker_name,
            prompt,
        }
    }
}

#[async_trait]
impl ScheduledTask for WorkerPromptTask {
    async fn execute(&self) -> Result<()> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "scheduled_worker", input = %self.prompt, worker = %self.worker_name, prompt = %self.prompt);

        async {
            let worker_name = Arc::clone(&self.worker_name);
            let prompt = Arc::clone(&self.prompt);
            let runtime = Arc::clone(&self.runtime);

            let workers_lock = Arc::clone(&runtime.workers);
            let workers = workers_lock.read().await;
            let Some(worker) = workers.get(&worker_name.to_string()) else {
                tracing::debug!("Worker not found for ScheduledTask: {worker_name}");
                return Ok(());
            };

            tracing::debug!(
                "Executing worker prompt task for worker: {worker_name}, prompt: {prompt}",
            );

            let Some(model) = Arc::clone(worker).as_model() else {
                tracing::debug!(
                    "Worker is not a model worker, skipping prompt execution: {worker_name}",
                );
                return Ok(());
            };

            let Ok(message_args) = ChatCompletionRequestUserMessageArgs::default()
                .content(prompt.to_string())
                .build()
            else {
                tracing::error!(
                    "Failed to build chat completion request message for worker '{worker_name}'",
                );
                return Ok(());
            };

            let Ok(chat_request) = CreateChatCompletionRequestArgs::default()
                .messages(vec![message_args.into()])
                .build()
            else {
                tracing::error!(
                    "Failed to build chat completion request for worker '{worker_name}'",
                );

                return Ok(());
            };

            if let Err(e) = model.chat_request(chat_request).await {
                tracing::error!(
                    "Failed to execute worker prompt task for worker '{worker_name}': {e}",
                );
            }

            Ok(())
        }
        .instrument(span)
        .await
    }
}

pub struct WorkerSqlTask {
    runtime: Arc<Runtime>,
    worker_name: Arc<str>,
    sql: Arc<str>,
}

impl WorkerSqlTask {
    pub fn new(runtime: Arc<Runtime>, worker_name: Arc<str>, sql: Arc<str>) -> Self {
        Self {
            runtime,
            worker_name,
            sql,
        }
    }
}

#[async_trait]
impl ScheduledTask for WorkerSqlTask {
    async fn execute(&self) -> Result<()> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "scheduled_worker", input = %self.sql, worker = %self.worker_name);

        async {
            let worker_name = Arc::clone(&self.worker_name);
            let sql = Arc::clone(&self.sql);
            let runtime = Arc::clone(&self.runtime);

            if !runtime.status.is_ready() {
                tracing::debug!("Runtime is not ready, skipping worker SQL task execution for worker: {worker_name}");
                return Ok(());
            }

            let workers_lock = Arc::clone(&runtime.workers);
            let workers = workers_lock.read().await;
            // we don't actually need the worker, but validate that it still exists
            if !workers.contains_key(&worker_name.to_string()) {
                tracing::debug!("Worker not found for ScheduledTask: {worker_name}");
                return Ok(());
            }

            tracing::debug!("Executing worker SQL task for worker: {worker_name}, SQL: {sql}");

            let df = runtime.datafusion();
            if let Err(e) = run_sql(df, &sql, None).await {
                tracing::error!(
                    "Failed to execute worker SQL task for worker '{worker_name}': {e}",
                );
            }

            Ok(())
        }
        .instrument(span)
        .await
    }
}
