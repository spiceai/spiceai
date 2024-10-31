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
    pub fn new(api_base: Option<String>, api_key: Option<String>) -> Self {
        let mut cfg =
            OpenAIConfig::default().with_api_base(api_base.unwrap_or(DEFAULT_ENDPOINT.to_string()));

        if let Some(api_key) = api_key {
            cfg = cfg.with_api_key(api_key);
        }

        Self {
            model: DEFAULT_MODEL.to_string(),
            client: Client::with_config(cfg),
        }
    }

    fn alter_request(&self, mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        req.model.clone_from(&self.model);

        // Changes to `req` to accomodate xAi not being OpenAI compatible.
        req.messages.iter_mut().for_each(|m| {
            if let ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessage {
                    content,
                    tool_calls,
                    ..
                },
            ) = m
            {
                if tool_calls.is_some() && content.is_none() {
                    content.replace(ChatCompletionRequestAssistantMessageContent::Text(
                        String::new(),
                    ));
                };
            }
        });

        if let Some(ref mut tools) = req.tools {
            tools.iter_mut().for_each(|t| {
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
            });
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
