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

//! xAI (Grok) LLM provider.

#![allow(clippy::missing_errors_doc)]

mod error;
mod list_models;
mod responses;

pub use list_models::XaiModelLister;

use std::sync::Arc;

use async_openai::{
    Client,
    config::OpenAIConfig,
    error::OpenAIError,
    traits::RequestOptionsBuilder,
    types::chat::{
        ChatCompletionMessageToolCalls, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestMessage,
        ChatCompletionResponseStream, ChatCompletionTools, CompletionUsage,
        CreateChatCompletionRequest, CreateChatCompletionResponse,
    },
};
use async_trait::async_trait;
use futures::StreamExt;
use runtime_rate_control::RateController;
use serde_json::json;

use crate::chat::{
    Chat, Error,
    nsql::{SqlGeneration, json::JsonSchemaSqlGeneration},
};
use crate::openai::default_rate_controller;

static DEFAULT_ENDPOINT: &str = "https://api.x.ai/v1";
static DEFAULT_MODEL: &str = "grok-4.3";

/// [`Xai`] is a chat model for xAI models. xAI is nearly `OpenAI` compatible.
pub struct Xai {
    pub model: String, // Xai model
    pub client: Client<OpenAIConfig>,
    pub rate_controller: Arc<RateController>,
}

impl Xai {
    #[must_use]
    pub fn new(model: Option<&str>, api_key: &str) -> Self {
        Self::with_endpoint(model, api_key, DEFAULT_ENDPOINT)
    }

    /// An xAI model served from `endpoint` rather than `https://api.x.ai/v1`.
    #[must_use]
    pub fn with_endpoint(model: Option<&str>, api_key: &str, endpoint: &str) -> Self {
        let cfg = OpenAIConfig::default()
            .with_api_base(endpoint)
            .with_api_key(api_key);

        // No retries, as `crate::provider::client_without_retry`. The service also reads xAI's
        // error bodies, which `async-openai` cannot.
        let http_client = reqwest::Client::new();
        let client = Client::build(http_client.clone(), cfg)
            .with_http_service(error::XaiErrorService::new(http_client));

        Self {
            model: model.unwrap_or(DEFAULT_MODEL).to_string(),
            client,
            rate_controller: default_rate_controller(),
        }
    }

    /// Changes to `req` to accommodate xAi not being `OpenAI` compatible.
    fn alter_request(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> (CreateChatCompletionRequest, Option<String>) {
        let prompt_cache_key = req.prompt_cache_key.take();

        // Use name of xAI model, not spicepod model.
        req.model.clone_from(&self.model);

        req.messages.iter_mut().for_each(|m| {
            if let ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessage {
                    content,
                    tool_calls: Some(tool_calls),
                    ..
                },
            ) = m
            {
                // xAI requires content to be set, even if tool calls are present.
                if content.is_none() {
                    content.replace(ChatCompletionRequestAssistantMessageContent::Text(
                        String::new(),
                    ));
                }

                // xAI requires tool calls with empty parameters used to be `{}` not ``.
                for t in tool_calls.iter_mut() {
                    if let ChatCompletionMessageToolCalls::Function(func_call) = t
                        && func_call.function.arguments.is_empty()
                    {
                        func_call.function.arguments = "{}".to_string();
                    }
                }
            }
        });

        // xAI should set Option::None parameters to a schema with no inputs, but xAI doesn't.
        // Must be done explicitly.
        if let Some(ref mut tools) = req.tools {
            for t in tools.iter_mut() {
                if let ChatCompletionTools::Function(func_tool) = t
                    && func_tool.function.parameters.is_none()
                {
                    func_tool.function.parameters.replace(json!(
                        {
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "properties": {},
                            "required": [],
                            "title": "",
                            "type": "object"
                        }
                    ));
                }
            }
        }

        (req, prompt_cache_key)
    }
}

/// Counts xAI's reasoning tokens as completion tokens, as `OpenAI` does.
///
/// xAI's Chat Completions API reports `reasoning_tokens` outside `completion_tokens` but
/// inside `total_tokens`, and bills them as output. Spice reads `completion_tokens` as every
/// token the model generated, in `/v1/chat/completions`, `/v1/decisions` and the
/// `llm_completion_tokens_total` metric. Usage changes only when xAI's own total shows the
/// reasoning outside the completion, so usage already in the `OpenAI` shape is not counted
/// twice.
fn count_reasoning_as_completion(usage: &mut CompletionUsage) {
    let Some(reasoning) = usage
        .completion_tokens_details
        .as_ref()
        .and_then(|details| details.reasoning_tokens)
        .filter(|reasoning| *reasoning > 0)
    else {
        return;
    };
    let reasoning_outside_completion = usage
        .prompt_tokens
        .checked_add(usage.completion_tokens)
        .and_then(|tokens| tokens.checked_add(reasoning))
        == Some(usage.total_tokens);
    if reasoning_outside_completion {
        usage.completion_tokens += reasoning;
    }
}

#[async_trait]
impl Chat for Xai {
    async fn health(&self) -> Result<(), Error> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");
        match self.client.models().retrieve(&self.model).await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(Error::ModelNotFound {
                    model: self.model.clone(),
                    model_source: "xai".to_string(),
                })
            }
        }
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        Some(&JsonSchemaSqlGeneration {})
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let (req, prompt_cache_key) = self.alter_request(req);
        let mut chat = self.client.chat();
        if let Some(prompt_cache_key) = prompt_cache_key {
            chat = chat.header("x-grok-conv-id", prompt_cache_key)?;
        }

        let stream = chat.create_stream(req).await?;

        drop(permit);
        Ok(Box::pin(stream.map(|chunk| {
            chunk.map(|mut chunk| {
                if let Some(usage) = chunk.usage.as_mut() {
                    count_reasoning_as_completion(usage);
                }
                chunk
            })
        })))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let (req, prompt_cache_key) = self.alter_request(req);
        let mut chat = self.client.chat();
        if let Some(prompt_cache_key) = prompt_cache_key {
            chat = chat.header("x-grok-conv-id", prompt_cache_key)?;
        }

        let mut resp = chat.create(req).await?;

        drop(permit);
        if let Some(usage) = resp.usage.as_mut() {
            count_reasoning_as_completion(usage);
        }
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_cache_key_is_moved_to_chat_header_value() {
        let xai = Xai::new(Some("grok-4.3"), "test-key");
        let req = CreateChatCompletionRequest {
            prompt_cache_key: Some("conversation-123".to_string()),
            ..CreateChatCompletionRequest::default()
        };

        let (req, prompt_cache_key) = xai.alter_request(req);

        assert_eq!(prompt_cache_key.as_deref(), Some("conversation-123"));
        assert!(req.prompt_cache_key.is_none());
    }

    /// Usage after [`count_reasoning_as_completion`], for the `usage` block a provider sent.
    fn counted(reported: serde_json::Value) -> serde_json::Value {
        let mut usage: CompletionUsage =
            serde_json::from_value(reported).expect("usage block as CompletionUsage");
        count_reasoning_as_completion(&mut usage);
        serde_json::to_value(usage).expect("usage as JSON")
    }

    #[test]
    fn reasoning_tokens_count_as_completion_tokens() {
        // The `usage` block xAI's Chat Completions API returned for one grok-4.7 call.
        let xai = json!({
            "prompt_tokens": 1891,
            "completion_tokens": 4,
            "total_tokens": 4636,
            "prompt_tokens_details": {"text_tokens": 1891, "audio_tokens": 0, "image_tokens": 0, "cached_tokens": 1792},
            "completion_tokens_details": {"reasoning_tokens": 2741, "audio_tokens": 0, "accepted_prediction_tokens": 0, "rejected_prediction_tokens": 0},
            "num_sources_used": 0,
            "cost_in_usd_ticks": 175_640_000
        });
        assert_eq!(
            counted(xai),
            json!({
                "prompt_tokens": 1891,
                "completion_tokens": 2745,
                "total_tokens": 4636,
                "prompt_tokens_details": {"audio_tokens": 0, "cached_tokens": 1792},
                "completion_tokens_details": {"reasoning_tokens": 2741, "audio_tokens": 0, "accepted_prediction_tokens": 0, "rejected_prediction_tokens": 0}
            })
        );

        // Usage in the `OpenAI` shape, as gpt-5-mini returned it: its reasoning is already
        // part of `completion_tokens`.
        let openai_shape = json!({
            "prompt_tokens": 618,
            "completion_tokens": 77,
            "total_tokens": 695,
            "prompt_tokens_details": {"audio_tokens": 0, "cached_tokens": 0},
            "completion_tokens_details": {"reasoning_tokens": 64, "audio_tokens": 0, "accepted_prediction_tokens": 0, "rejected_prediction_tokens": 0}
        });
        assert_eq!(counted(openai_shape.clone()), openai_shape);

        // No reasoning reported: nothing to count.
        let no_reasoning = json!({"prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15});
        assert_eq!(counted(no_reasoning.clone()), no_reasoning);
    }
}
