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
        CreateResponse, Response, ResponseCompletedEvent, ResponseStream, ResponseStreamEvent,
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

use crate::model::metrics::{handle_metrics, handle_token_metrics, request_labels_responses};

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Wraps [`Responses`] models with additional handling specifically for the spice runtime (e.g. telemetry, injecting system prompts).
pub struct ResponsesWrapper {
    pub public_name: String,
    pub responses: Arc<dyn Responses>,
    pub system_prompt: Option<String>,
    pub defaults: Vec<(String, serde_json::Value)>,
}

macro_rules! set_default_w_warning {
    ($req:expr, $field:ident, $value:expr, $model:expr) => {
        $req.$field = $req
            .$field
            .or_else(|| match serde_json::from_value($value.clone()) {
                Ok(val) => Some(val),
                Err(_) => {
                    tracing::warn!(
                        "Failed to parse Responses API `{}` override for model='{}'. Ensure {:?} is of the correct format.",
                        stringify!($field),
                        $model,
                        $value
                    );
                    None
                }
            })
    };
}

impl ResponsesWrapper {
    pub fn new(
        responses: Arc<dyn Responses>,
        public_name: &str,
        system_prompt: Option<&str>,
        defaults: Vec<(String, serde_json::Value)>,
    ) -> Self {
        Self {
            public_name: public_name.to_string(),
            responses,
            system_prompt: system_prompt.map(ToString::to_string),
            defaults,
        }
    }

    fn prepare_req(&self, req: CreateResponse) -> CreateResponse {
        self.with_model_defaults(self.with_system_prompt(req))
    }

    /// Injects a system prompt into the instructions field in the request, if it exists.
    /// If the client also provided instructions, the spicepod system prompt is prepended.
    fn with_system_prompt(&self, mut req: CreateResponse) -> CreateResponse {
        if let Some(prompt) = &self.system_prompt {
            req.instructions = Some(match req.instructions {
                Some(existing) => format!("{prompt}\n\n{existing}"),
                None => prompt.clone(),
            });
        }
        req
    }

    fn with_model_defaults(&self, mut req: CreateResponse) -> CreateResponse {
        for (key, value) in &self.defaults {
            match key.as_str() {
                "prompt_cache_key" => {
                    set_default_w_warning!(req, prompt_cache_key, value, self.public_name);
                }
                "prompt_cache_retention" => {
                    set_default_w_warning!(req, prompt_cache_retention, value, self.public_name);
                }
                _ => tracing::debug!("Ignoring unknown Responses API default key: {key}"),
            }
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
        let Some(ref model_id) = req.model else {
            return Err(OpenAIError::InvalidArgument(
                "Model ID must be specified in the request".into(),
            ));
        };
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "responses", stream=true, model = %model_id, input = %serde_json::to_string(&req).unwrap_or_default());

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

        let Some(model_id) = req.model.clone() else {
            return Err(OpenAIError::InvalidArgument(
                "Model ID must be specified in the request".into(),
            ));
        };

        let req = self.prepare_req(req);
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "responses", stream=false, model = %model_id, input = %serde_json::to_string(&req).unwrap_or_default());

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
                    handle_token_metrics(usage.input_tokens, usage.output_tokens, &labels);
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
    accumulated_response: Arc<Mutex<Option<Response>>>,
    span: tracing::Span,
    model_public_name: String,
    started: Instant,
    labels: Vec<KeyValue>,
}

impl<S> TracedResponseStream<S>
where
    S: Stream<Item = Result<ResponseStreamEvent, OpenAIError>> + Unpin,
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
    S: Stream<Item = Result<ResponseStreamEvent, OpenAIError>> + Unpin,
{
    type Item = Result<ResponseStreamEvent, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(mut item))) => {
                match &mut item {
                    ResponseStreamEvent::ResponseCompleted(ResponseCompletedEvent {
                        response,
                        ..
                    }) => {
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
                            handle_token_metrics(
                                usage.input_tokens,
                                usage.output_tokens,
                                &self.labels,
                            );
                        }

                        response.model.clone_from(&self.model_public_name);
                    }
                    ResponseStreamEvent::ResponseFailed(_) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::responses::{CreateResponse, PromptCacheRetention};

    /// Helper to create a [`ResponsesWrapper`] with the given system prompt (no underlying model needed for `with_system_prompt` tests).
    fn wrapper_with_prompt(prompt: Option<&str>) -> ResponsesWrapper {
        ResponsesWrapper {
            public_name: "test-model".to_string(),
            responses: Arc::new(NoopResponses),
            system_prompt: prompt.map(ToString::to_string),
            defaults: Vec::new(),
        }
    }

    /// Minimal no-op implementation of [`Responses`] for unit testing the wrapper logic.
    struct NoopResponses;

    #[async_trait]
    impl Responses for NoopResponses {
        async fn responses_stream(
            &self,
            _req: CreateResponse,
        ) -> Result<ResponseStream, OpenAIError> {
            Err(OpenAIError::InvalidArgument("noop".into()))
        }

        async fn health(&self) -> ResponsesResult<()> {
            Ok(())
        }

        async fn responses_request(
            &self,
            _req: CreateResponse,
        ) -> Result<async_openai::types::responses::Response, OpenAIError> {
            Err(OpenAIError::InvalidArgument("noop".into()))
        }

        async fn run(&self, _prompt: String) -> ResponsesResult<Option<String>> {
            Ok(None)
        }

        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }
    }

    #[test]
    fn test_no_system_prompt_preserves_instructions() {
        let wrapper = wrapper_with_prompt(None);
        let req = CreateResponse {
            instructions: Some("client instructions".to_string()),
            ..CreateResponse::default()
        };
        let result = wrapper.with_system_prompt(req);
        assert_eq!(
            result.instructions.as_deref(),
            Some("client instructions"),
            "Client instructions should be preserved when no system prompt is configured"
        );
    }

    #[test]
    fn test_system_prompt_no_client_instructions() {
        let wrapper = wrapper_with_prompt(Some("spicepod prompt"));
        let req = CreateResponse {
            instructions: None,
            ..CreateResponse::default()
        };
        let result = wrapper.with_system_prompt(req);
        assert_eq!(
            result.instructions.as_deref(),
            Some("spicepod prompt"),
            "System prompt should become instructions when client provides none"
        );
    }

    #[test]
    fn test_system_prompt_combined_with_client_instructions() {
        let wrapper = wrapper_with_prompt(Some("spicepod prompt"));
        let req = CreateResponse {
            instructions: Some("client instructions".to_string()),
            ..CreateResponse::default()
        };
        let result = wrapper.with_system_prompt(req);
        assert_eq!(
            result.instructions.as_deref(),
            Some("spicepod prompt\n\nclient instructions"),
            "Spicepod prompt should be prepended to client instructions"
        );
    }

    #[test]
    fn test_no_system_prompt_no_client_instructions() {
        let wrapper = wrapper_with_prompt(None);
        let req = CreateResponse::default();
        let result = wrapper.with_system_prompt(req);
        assert_eq!(
            result.instructions, None,
            "Instructions should remain None when neither is set"
        );
    }

    #[test]
    fn test_prompt_cache_defaults_preserve_request_values() {
        let wrapper = ResponsesWrapper {
            public_name: "test-model".to_string(),
            responses: Arc::new(NoopResponses),
            system_prompt: None,
            defaults: vec![
                (
                    "prompt_cache_key".to_string(),
                    serde_json::Value::String("default-key".to_string()),
                ),
                (
                    "prompt_cache_retention".to_string(),
                    serde_json::Value::String("24h".to_string()),
                ),
            ],
        };

        let req = CreateResponse {
            prompt_cache_key: Some("request-key".to_string()),
            ..CreateResponse::default()
        };
        let result = wrapper.with_model_defaults(req);

        assert_eq!(result.prompt_cache_key.as_deref(), Some("request-key"));
        assert_eq!(
            result.prompt_cache_retention,
            Some(PromptCacheRetention::Hours24)
        );
    }
}
