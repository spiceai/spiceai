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
use std::sync::atomic::{AtomicBool, Ordering};

use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs,
};
use scheduler::Result;
use scheduler::task::ScheduledTask;
use tonic::async_trait;
use tracing_futures::Instrument;

use runtime_auth::{AuthPrincipalRef, AuthRequestContext, SystemPrincipal};
use runtime_request_context::{Protocol, RequestContext};

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

            let Some(worker) = runtime
                .workers
                .read()
                .await
                .get(worker_name.as_ref())
                .cloned()
            else {
                tracing::debug!("Worker not found for ScheduledTask: {worker_name}");
                return Ok(());
            };

            tracing::debug!(
                "Executing worker prompt task for worker: {worker_name}, prompt: {prompt}",
            );

            let Some(model) = worker.as_model() else {
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

            // The prompt is the operator's, configured in the Spicepod, so it runs as the
            // runtime acting for the operator: with write access, so its model may call the
            // read-write tools (an MCP server's, `store_memory`) the operator gave it. With no
            // principal it could only read.
            let outcome: std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> =
                async {
                    let context = Arc::new(RequestContext::builder(Protocol::Internal).build());
                    let principal: AuthPrincipalRef = Arc::new(SystemPrincipal);
                    context.set_auth_principal(principal)?;
                    context.scope(model.chat_request(chat_request)).await?;
                    Ok(())
                }
                .await;
            if let Err(e) = outcome {
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
    /// Whether the run before this one was skipped, so a run of skips warns once.
    skipped: AtomicBool,
}

impl WorkerSqlTask {
    pub fn new(runtime: Arc<Runtime>, worker_name: Arc<str>, sql: Arc<str>) -> Self {
        Self {
            runtime,
            worker_name,
            sql,
            skipped: AtomicBool::new(false),
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

            // A component in `Error` must not hold the worker back: it may never load.
            if !runtime.status.is_ready_allowing_unhealthy() {
                if self.skipped.swap(true, Ordering::Relaxed) {
                    tracing::debug!("Worker '{worker_name}' skipped a scheduled run: the runtime is not ready.");
                } else {
                    tracing::warn!("Worker '{worker_name}' skipped a scheduled run: the runtime is not ready. It runs at the first scheduled time after the runtime is ready.");
                }
                return Ok(());
            }
            self.skipped.store(false, Ordering::Relaxed);

            if !runtime.workers.read().await.contains_key(worker_name.as_ref()) {
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
