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

use async_openai::{
    config::OpenAIConfig,
    error::OpenAIError,
    types::{
        ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
        ChatCompletionRequestMessage, ChatCompletionResponseStream, CreateChatCompletionRequest,
        CreateChatCompletionResponse,
    },
    Client,
};
use async_trait::async_trait;
use serde_json::json;

use crate::chat::{nsql::SqlGeneration, Chat};

static DEFAULT_ENDPOINT: &str = "https://api.x.ai/v1";
static DEFAULT_MODEL: &str = "grok-beta";

/// [`Xai`] is a chat model for xAI models. xAI is nearly `OpenAI` compatible.
pub struct Xai {
    pub model: String, // Xai model
    pub client: Client<OpenAIConfig>,
}

impl Xai {
    #[must_use]
    pub fn new(api_base: Option<&str>, api_key: Option<&str>) -> Self {
        let mut cfg = OpenAIConfig::default().with_api_base(api_base.unwrap_or(DEFAULT_ENDPOINT));

        if let Some(api_key) = api_key {
            cfg = cfg.with_api_key(api_key);
        }

        Self {
            model: DEFAULT_MODEL.to_string(),
            client: Client::with_config(cfg),
        }
    }

    /// Changes to `req` to accomodate xAi not being `OpenAI` compatible.
    fn alter_request(&self, mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        // Use name of xAI model, not spicepod model.
        req.model.clone_from(&self.model);

        // xAI requires content to be set, even if tool calls are present.
        req.messages.iter_mut().for_each(|m| {
            if let ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessage {
                    content,
                    tool_calls: Some(ref _tool_calls),
                    ..
                },
            ) = m
            {
                if content.is_none() {
                    content.replace(ChatCompletionRequestAssistantMessageContent::Text(
                        String::new(),
                    ));
                };
            }
        });

        // xAI should set Option::None parameters to a schema with no inputs, but xAI doesn't.
        // Must be done explicitly.
        if let Some(ref mut tools) = req.tools {
            for t in tools.iter_mut() {
                if t.function.parameters.is_none() {
                    t.function.parameters.replace(json!(
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

        req
    }
}

#[async_trait]
impl Chat for Xai {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let stream = self
            .client
            .chat()
            .create_stream(self.alter_request(req))
            .await?;

        Ok(Box::pin(stream))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        self.client.chat().create(self.alter_request(req)).await
    }
}
