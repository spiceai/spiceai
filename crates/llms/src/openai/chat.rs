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
#![allow(clippy::missing_errors_doc)]

use crate::chat::nsql::structured_output::StructuredOutputSqlGeneration;
use crate::chat::nsql::{json::JsonSchemaSqlGeneration, SqlGeneration};
use crate::chat::Chat;
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_trait::async_trait;
use futures::TryStreamExt;

use super::Openai;

pub const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.

pub(crate) const GPT3_5_TURBO_INSTRUCT: &str = "gpt-3.5-turbo";

pub const DEFAULT_LLM_MODEL: &str = GPT3_5_TURBO_INSTRUCT;

#[async_trait]
impl Chat for Openai {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        // Only use structured output schema for OpenAI, not openai compatible.
        if self.supports_structured_output() {
            Some(&StructuredOutputSqlGeneration {})
        } else {
            Some(&JsonSchemaSqlGeneration {})
        }
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let outer_model = req.model.clone();
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        let stream = self.client.chat().create_stream(inner_req).await?;

        Ok(Box::pin(stream.map_ok(move |mut s| {
            s.model.clone_from(&outer_model);
            s
        })))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let outer_model = req.model.clone();
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        let mut resp = self.client.chat().create(inner_req).await?;

        resp.model = outer_model;
        Ok(resp)
    }
}
