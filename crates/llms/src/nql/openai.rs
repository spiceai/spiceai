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
use super::{FailedToLoadModelSnafu, Nql, Result};
use crate::nql::FailedToRunModelSnafu;

use async_openai::{config::OpenAIConfig, types::CreateCompletionRequestArgs, Client};
use async_trait::async_trait;
use snafu::ResultExt;

const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.
const GPT3_5_TURBO_INSTRUCT: &str = "gpt-3.5-turbo-instruct";

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
}

#[async_trait]
impl Nql for Openai {
    async fn run(&mut self, prompt: String) -> Result<Option<String>> {
        let request = CreateCompletionRequestArgs::default()
            .model(self.model.clone())
            .prompt(prompt.as_str())
            .max_tokens(MAX_COMPLETION_TOKENS)
            .build()
            .boxed()
            .context(FailedToLoadModelSnafu)?;

        let response = self
            .client
            .completions()
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

        Ok(response.choices.first().map(|c| c.text.clone()))
    }
}
