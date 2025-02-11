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
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::chat::{nsql::SqlGeneration, Chat, Error};

static DEFAULT_ENDPOINT: &str = "https://api.x.ai/v1";
static DEFAULT_MODEL: &str = "grok-beta";

/// [`Xai`] is a chat model for xAI models. xAI is nearly `OpenAI` compatible.
pub struct Xai {
    pub model: String, // Xai model
    pub client: Client<OpenAIConfig>,
}

impl Xai {
    #[must_use]
    pub fn new(model: Option<&str>, api_key: &str) -> Self {
        let cfg = OpenAIConfig::default()
            .with_api_base(DEFAULT_ENDPOINT)
            .with_api_key(api_key);

        Self {
            model: model.unwrap_or(DEFAULT_MODEL).to_string(),
            client: Client::with_config(cfg),
        }
    }

    /// Changes to `req` to accomodate xAi not being `OpenAI` compatible.
    fn alter_request(&self, mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        // Use name of xAI model, not spicepod model.
        req.model.clone_from(&self.model);

        req.messages.iter_mut().for_each(|m| {
            if let ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessage {
                    content,
                    tool_calls: Some(ref mut tool_calls),
                    ..
                },
            ) = m
            {
                // xAI requires content to be set, even if tool calls are present.
                if content.is_none() {
                    content.replace(ChatCompletionRequestAssistantMessageContent::Text(
                        String::new(),
                    ));
                };

                // xAI requires tool calls with empty parameters used to be `{}` not ``.
                for t in tool_calls.iter_mut() {
                    if t.function.arguments.is_empty() {
                        t.function.arguments = "{}".to_string();
                    }
                }
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

#[derive(Serialize, Deserialize, Debug)]
struct Model {
    id: String,
    created: u64,
    object: String,
    owned_by: String,
}

#[async_trait]
impl Chat for Xai {
    async fn health(&self) -> Result<(), Error> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");
        match self
            .client
            .get::<Model>(format!("/models/{}", self.model).as_str())
            .await
        {
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
