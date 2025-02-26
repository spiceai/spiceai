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
use std::pin::Pin;

use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionResponseStream, ChatCompletionStreamOptions, CreateChatCompletionRequest,
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse,
    },
};
use async_trait::async_trait;
use futures::Stream;
use futures::TryStreamExt;
use llms::{
    accumulate::{empty_completion_response, fold_completion_stream},
    chat::{nsql::SqlGeneration, Chat, Result as ChatResult},
};
use opentelemetry::KeyValue;
use tokio::time::Instant;
use tracing_futures::Instrument;

use crate::model::metrics::handle_metrics;

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use super::metrics::request_labels;

/// Wraps [`Chat`] models with additional handling specifically for the spice runtime (e.g. telemetry, injecting system prompts).
pub struct ChatWrapper {
    pub public_name: String,
    pub chat: Box<dyn Chat>,
    pub system_prompt: Option<String>,
    pub defaults: Vec<(String, serde_json::Value)>,
}

/// Sets a certain field in a [`CreateChatCompletionRequest`] to a given value.
/// Emit a warning if the value cannot be parsed, from a string to the respective field's type.
macro_rules! set_default_w_warning {
    ($req:expr, $field:ident, $value:expr, $model:expr) => {
        $req.$field = $req
            .$field
            .or_else(|| match serde_json::from_value($value.clone()) {
                Ok(val) => Some(val),
                Err(_) => {
                    tracing::warn!(
                        "Failed to parse `openai_{}` override for model='{}'. Ensure {:?} is of the correct format.",
                        stringify!($field),
                        $model,
                        $value
                    );
                    None
                }
            })
    };
}

impl ChatWrapper {
    pub fn new(
        chat: Box<dyn Chat>,
        public_name: &str,
        system_prompt: Option<&str>,
        defaults: Vec<(String, serde_json::Value)>,
    ) -> Self {
        let s = Self {
            public_name: public_name.to_string(),
            chat,
            system_prompt: system_prompt.map(ToString::to_string),
            defaults,
        };

        // Check defaults provided are valid at startup.
        // `with_model_defaults` will emit appropriate warnings to user.
        s.with_model_defaults(CreateChatCompletionRequest::default());

        s
    }

    fn prepare_req(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let mut prepared_req = self.with_system_prompt(req)?;

        prepared_req = self.with_model_defaults(prepared_req);
        prepared_req = Self::with_stream_usage(prepared_req);
        Ok(prepared_req)
    }

    /// Injects a system prompt as the first message in the request, if it exists.
    fn with_system_prompt(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        if let Some(prompt) = self.system_prompt.clone() {
            let system_message = ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()?;
            req.messages
                .insert(0, ChatCompletionRequestMessage::System(system_message));
        }
        Ok(req)
    }

    /// Ensure that streaming requests have `stream_options: {"include_usage": true}` internally.
    fn with_stream_usage(mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        if req.stream.is_some_and(|s| s) {
            req.stream_options = match req.stream_options {
                Some(mut opts) => {
                    opts.include_usage = true;
                    Some(opts)
                }
                None => Some(ChatCompletionStreamOptions {
                    include_usage: true,
                }),
            };
        }
        req
    }

    /// For [`None`] valued fields in a [`CreateChatCompletionRequest`], if the chat model has non-`None` defaults, use those instead.
    fn with_model_defaults(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> CreateChatCompletionRequest {
        for (key, v) in &self.defaults {
            let value = v.clone();
            match key.as_str() {
                "frequency_penalty" => {
                    set_default_w_warning!(req, frequency_penalty, value, self.public_name);
                }
                "logit_bias" => set_default_w_warning!(req, logit_bias, value, self.public_name),
                "logprobs" => set_default_w_warning!(req, logprobs, value, self.public_name),
                "top_logprobs" => {
                    set_default_w_warning!(req, top_logprobs, value, self.public_name);
                }
                "max_completion_tokens" => {
                    set_default_w_warning!(req, max_completion_tokens, value, self.public_name);
                }
                "reasoning_effort" => {
                    set_default_w_warning!(req, reasoning_effort, value, self.public_name);
                }
                "store" => set_default_w_warning!(req, store, value, self.public_name),
                "metadata" => set_default_w_warning!(req, metadata, value, self.public_name),
                "n" => set_default_w_warning!(req, n, value, self.public_name),
                "presence_penalty" => {
                    set_default_w_warning!(req, presence_penalty, value, self.public_name);
                }
                "response_format" => {
                    set_default_w_warning!(req, response_format, value, self.public_name);
                }
                "seed" => set_default_w_warning!(req, seed, value, self.public_name),
                "stop" => set_default_w_warning!(req, stop, value, self.public_name),
                "stream" => set_default_w_warning!(req, stream, value, self.public_name),
                "stream_options" => {
                    set_default_w_warning!(req, stream_options, value, self.public_name);
                }
                "temperature" => set_default_w_warning!(req, temperature, value, self.public_name),
                "top_p" => set_default_w_warning!(req, top_p, value, self.public_name),
                "tools" => set_default_w_warning!(req, tools, value, self.public_name),
                "tool_choice" => set_default_w_warning!(req, tool_choice, value, self.public_name),
                "parallel_tool_calls" => {
                    set_default_w_warning!(req, parallel_tool_calls, value, self.public_name);
                }
                "user" => set_default_w_warning!(req, user, value, self.public_name),
                _ => {
                    tracing::debug!("Ignoring unknown default key: {}", key);
                }
            }
        }
        req
    }
}

#[async_trait]
impl Chat for ChatWrapper {
    /// Expect `captured_output` to be instrumented by the underlying chat model (to not reopen/parse streams). i.e.
    /// ```rust
    /// tracing::info!(target: "task_history", captured_output = %chat_output)
    /// ```
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let start = Instant::now();
        let req = self.prepare_req(req)?;
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=true, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default());

        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", metadata = %metadata);
        }

        let labels = request_labels(&req);
        match self.chat.chat_stream(req).instrument(span.clone()).await {
            Ok(resp) => {
                let public_name = self.public_name.clone();
                let logged_stream = resp.map_ok(move |mut r| {
                    r.model.clone_from(&public_name);
                    r
                });

                // Wrap the stream with our custom aggregator that logs when dropped.
                Ok(Box::pin(TracedChatCompletionStream::new(
                    logged_stream,
                    span.clone(),
                    self.public_name.clone(),
                    labels,
                )))
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                handle_metrics(start.elapsed(), true, &labels);
                Err(e)
            }
        }
    }

    async fn health(&self) -> ChatResult<()> {
        self.chat.health().await
    }

    /// Unlike [`ChatWrapper::chat_stream`], this method will instrument the `captured_output` for the model output.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let start = Instant::now();

        let req = self.prepare_req(req)?;
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=false, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default());

        let labels = request_labels(&req);
        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", parent: &span, metadata = %metadata, "labels");
        }

        let result = match self.chat.chat_request(req).instrument(span.clone()).await {
            Ok(mut resp) => {
                if let Some(usage) = resp.usage.clone() {
                    tracing::info!(target: "task_history", parent: &span, completion_tokens = %usage.completion_tokens, total_tokens = %usage.total_tokens, prompt_tokens = %usage.prompt_tokens, "labels");
                };
                let captured_output: Vec<_> = resp.choices.iter().map(|c| &c.message).collect();
                match serde_json::to_string(&captured_output) {
                    Ok(output) => {
                        tracing::info!(target: "task_history", parent: &span, captured_output = %output);
                    }
                    Err(e) => tracing::error!("Failed to serialize truncated output: {e}"),
                }
                resp.model.clone_from(&self.public_name);
                Ok(resp)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                Err(e)
            }
        };
        handle_metrics(start.elapsed(), result.is_err(), &labels);
        result
    }

    async fn run(&self, prompt: String) -> ChatResult<Option<String>> {
        self.chat.run(prompt).await
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        self.chat.stream(prompt).await
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        self.chat.as_sql()
    }
}

/// [`TracedChatCompletionStream`] wraps a [`ChatCompletionResponseStream`]-like stream and provides metrics and `task_history` tracing. Importantly, when aggregrating the output, it does not need to block until the full stream is consumed.
struct TracedChatCompletionStream<S> {
    inner: S,
    accumulated_response: Arc<Mutex<CreateChatCompletionResponse>>,
    span: tracing::Span,
    model_public_name: String,
    started: Instant,
    labels: Vec<KeyValue>,
}

impl<S> TracedChatCompletionStream<S>
where
    S: Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Unpin,
{
    pub fn new(
        inner: S,
        span: tracing::Span,
        model_public_name: String,
        labels: Vec<KeyValue>,
    ) -> Self {
        Self {
            inner,
            accumulated_response: Arc::new(Mutex::new(empty_completion_response())),
            span,
            model_public_name,
            started: Instant::now(),
            labels,
        }
    }
}

impl<S> Stream for TracedChatCompletionStream<S>
where
    S: Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Unpin,
{
    type Item = Result<CreateChatCompletionStreamResponse, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                // Aggregate the response.
                if let Ok(mut acc) = self.accumulated_response.lock() {
                    fold_completion_stream(&mut acc, &item);
                }

                // Log usage info if available.
                if let Some(usage) = item.usage.clone() {
                    tracing::info!(
                        target: "task_history",
                        completion_tokens = %usage.completion_tokens,
                        total_tokens = %usage.total_tokens,
                        prompt_tokens = %usage.prompt_tokens,
                        "Usage info"
                    );

                    // Usage should be on last message, so we can add latency metrics here.
                    handle_metrics(self.started.elapsed(), false, &self.labels);
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

impl<S> Drop for TracedChatCompletionStream<S> {
    fn drop(&mut self) {
        if let Ok(output) = self.accumulated_response.lock() {
            let _guard = self.span.enter();
            if let Ok(resp_str) = serde_json::to_string(&*output) {
                tracing::info!(target: "task_history", captured_output = %*resp_str);
            }
        } else {
            tracing::warn!(
                "Failed to write output of ai_completion for '{}' model",
                self.model_public_name
            );
        }
    }
}
