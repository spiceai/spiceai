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
use super::{FailedToLoadModelSnafu, FailedToLoadTokenizerSnafu, Nql, Result};
use crate::nql::FailedToRunModelSnafu;

use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionResponseFormat, ChatCompletionResponseFormatType,
        CreateChatCompletionRequestArgs,
    },
    Client,
};
use async_trait::async_trait;
use serde_json::Value;
use snafu::ResultExt;

const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.
pub(crate) const GPT3_5_TURBO_INSTRUCT: &str = "gpt-3.5-turbo";

pub struct Openai {
    client: Client<OpenAIConfig>,
    model: String,
}

impl Default for Openai {
    fn default() -> Self {
        Self::new(OpenAIConfig::default(), GPT3_5_TURBO_INSTRUCT.to_string())
    }
}

impl Openai {
    #[must_use]
    pub fn new(client_config: OpenAIConfig, model: String) -> Self {
        Self {
            client: Client::with_config(client_config),
            model,
        }
    }

    #[must_use]
    pub fn using_model(model: String) -> Self {
        Self::new(OpenAIConfig::default(), model)
    }

    /// Convert the Json object returned when using a `{ "type": "json_object" } ` response format.
    /// Expected format is `"content": "{\"arbitrary_key\": \"arbitrary_value\"}"`
    pub fn convert_json_object_to_sql(raw_json: &str) -> Result<Option<String>> {
        let result: Value = serde_json::from_str(raw_json)
            .boxed()
            .context(FailedToRunModelSnafu)?;
        Ok(result["sql"].as_str().map(std::string::ToString::to_string))
    }
}

#[async_trait]
impl Nql for Openai {
    async fn run(&mut self, prompt: String) -> Result<Option<String>> {
        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessageArgs::default()
                .content("Return JSON, with the requested SQL under 'sql'.")
                .build()
                .boxed()
                .context(FailedToLoadTokenizerSnafu)?
                .into(),
            ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()
                .boxed()
                .context(FailedToLoadTokenizerSnafu)?
                .into(),
        ];
        let request = CreateChatCompletionRequestArgs::default()
            .model(self.model.clone())
            .response_format(ChatCompletionResponseFormat {
                r#type: ChatCompletionResponseFormatType::JsonObject,
            })
            .messages(messages)
            .max_tokens(MAX_COMPLETION_TOKENS)
            .build()
            .boxed()
            .context(FailedToLoadModelSnafu)?;

        let response = self
            .client
            .chat()
            .create(request)
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;

        if let Some(usage) = response.usage {
            if usage.completion_tokens >= u32::from(MAX_COMPLETION_TOKENS) {
                tracing::warn!(
                    "Completion response may have been cut off after {} tokens",
                    MAX_COMPLETION_TOKENS
                );
            }
        }

        match response
            .choices
            .iter()
            .find_map(|c| c.message.content.clone())
        {
            Some(json_resp) => Self::convert_json_object_to_sql(&json_resp),
            None => Ok(None),
        }
    }
}
