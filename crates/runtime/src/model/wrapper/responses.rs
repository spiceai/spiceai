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

#![allow(clippy::implicit_hasher)]
use async_openai::{
    error::OpenAIError,
    types::responses::{
        CreateResponse, Response, ResponseCompleted, ResponseEvent, ResponseMetadata,
        ResponseStream,
    },
};
use async_trait::async_trait;
use futures::Stream;
use llms::{
    chat::nsql::SqlGeneration,
    responses::{Responses, Result as ResponsesResult},
};
use opentelemetry::KeyValue;
use std::pin::Pin;
use tokio::time::Instant;
use tracing_futures::Instrument;

use crate::model::metrics::{handle_metrics, request_labels_responses};

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Wraps [`Responses`] models with additional handling specifically for the spice runtime (e.g. telemetry, injecting system prompts).
pub struct ResponsesWrapper {
    pub public_name: String,
    pub responses: Arc<dyn Responses>,
    pub system_prompt: Option<String>,
}

impl ResponsesWrapper {
    pub fn new(
        responses: Arc<dyn Responses>,
        public_name: &str,
        system_prompt: Option<&str>,
    ) -> Self {
        Self {
            public_name: public_name.to_string(),
            responses,
            system_prompt: system_prompt.map(ToString::to_string),
        }
    }

    fn prepare_req(&self, req: CreateResponse) -> CreateResponse {
        self.with_system_prompt(req)
    }

    /// Injects a system prompt as the instructions field in the request, if it exists.
    fn with_system_prompt(&self, mut req: CreateResponse) -> CreateResponse {
        if let Some(prompt) = &self.system_prompt {
            req.instructions = Some(prompt.clone());
        }
        req
    }
}

#[async_trait]
impl Responses for ResponsesWrapper {
    /// Expect `captured_output` to be instrumented by the underlying responses model (to not reopen/parse streams). i.e.
    /// ```rust
    /// tracing::info!(target: "task_history", captured_output = %response_output)
    /// ```
    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError> {
        let start = Instant::now();
        let req = self.prepare_req(req);
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "responses", stream=true, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default());

        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", metadata = ?metadata);
        }

        let labels = request_labels_responses(&req);
        match self
            .responses
            .responses_stream(req)
            .instrument(span.clone())
            .await
        {
            Ok(resp) => {
                let logged_stream = resp;

                // Wrap the stream with our custom aggregator that logs when dropped.
                Ok(Box::pin(TracedResponseStream::new(
                    logged_stream,
                    span.clone(),
                    self.public_name.clone(),
                    labels,
                )))
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run responses model: {}", e);
                handle_metrics(start.elapsed(), true, &labels);
                Err(e)
            }
        }
    }

    async fn health(&self) -> ResponsesResult<()> {
        self.responses.health().await
    }

    /// Unlike [`ResponsesWrapper::responses_stream`], this method will instrument the `captured_output` for the model output.
    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
        let start = Instant::now();

        let req = self.prepare_req(req);
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "responses", stream=false, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default());

        let labels = request_labels_responses(&req);
        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", parent: &span, metadata = ?metadata, "labels");
        }

        let result = match self
            .responses
            .responses_request(req)
            .instrument(span.clone())
            .await
        {
            Ok(mut resp) => {
                let captured_output = &resp;

                if let Some(usage) = resp.usage.clone() {
                    tracing::info!(target: "task_history", parent: &span, completion_tokens = %usage.output_tokens, total_tokens = %usage.total_tokens, prompt_tokens = %usage.input_tokens, id=resp.id, "labels");
                }

                match serde_json::to_string(&captured_output) {
                    Ok(output) => {
                        tracing::info!(target: "task_history", parent: &span, captured_output = %output);
                    }
                    Err(e) => tracing::error!("Failed to serialize response output: {e}"),
                }
                resp.model.clone_from(&self.public_name);
                Ok(resp)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run responses model: {}", e);
                Err(e)
            }
        };
        handle_metrics(start.elapsed(), result.is_err(), &labels);
        result
    }

    async fn run(&self, prompt: String) -> ResponsesResult<Option<String>> {
        self.responses.run(prompt).await
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        self.responses.as_sql()
    }
}

/// [`TracedResponseStream`] wraps a [`ResponseStream`]-like stream and provides metrics and `task_history` tracing.
struct TracedResponseStream<S> {
    inner: S,
    accumulated_response: Arc<Mutex<Option<ResponseMetadata>>>,
    span: tracing::Span,
    model_public_name: String,
    started: Instant,
    labels: Vec<KeyValue>,
}

impl<S> TracedResponseStream<S>
where
    S: Stream<Item = Result<ResponseEvent, OpenAIError>> + Unpin,
{
    pub fn new(
        inner: S,
        span: tracing::Span,
        model_public_name: String,
        labels: Vec<KeyValue>,
    ) -> Self {
        Self {
            inner,
            accumulated_response: Arc::new(Mutex::new(None)),
            span,
            model_public_name,
            started: Instant::now(),
            labels,
        }
    }
}

impl<S> Stream for TracedResponseStream<S>
where
    S: Stream<Item = Result<ResponseEvent, OpenAIError>> + Unpin,
{
    type Item = Result<ResponseEvent, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(mut item))) => {
                match &mut item {
                    ResponseEvent::ResponseCompleted(ResponseCompleted { response, .. }) => {
                        if let Ok(mut guard) = self.accumulated_response.lock() {
                            *guard = Some(response.clone());
                        }

                        // Response completed, add latency and usage metrics here
                        handle_metrics(self.started.elapsed(), false, &self.labels);
                        tracing::info!(
                            target: "task_history",
                            "Response completed"
                        );

                        if let Some(usage) = response.usage.clone() {
                            tracing::info!(
                                target: "task_history",
                                completion_tokens = %usage.output_tokens,
                                total_tokens = %usage.total_tokens,
                                prompt_tokens = %usage.input_tokens,
                                "Usage info"
                            );
                        }

                        response.model = Some(self.model_public_name.clone());
                    }
                    ResponseEvent::ResponseFailed(_) => {
                        handle_metrics(self.started.elapsed(), true, &self.labels);
                        tracing::error!(
                            target: "task_history",
                            "Response failed"
                        );
                    }
                    _ => {}
                }
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                handle_metrics(self.started.elapsed(), true, &self.labels);
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

impl<S> Drop for TracedResponseStream<S> {
    fn drop(&mut self) {
        if let Ok(output) = self.accumulated_response.lock() {
            let _guard = self.span.enter();
            if let Some(response) = &*output
                && let Ok(resp_str) = serde_json::to_string(response)
            {
                tracing::info!(target: "task_history", captured_output = %resp_str);
            }
        } else {
            tracing::warn!(
                "Failed to write output of ai_response for '{}' model",
                self.model_public_name
            );
        }
    }
}
