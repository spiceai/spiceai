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

use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    },
};
use async_trait::async_trait;
use futures::TryStreamExt;

use crate::chat::{nsql::SqlGeneration, Chat};

use super::{types::PerplexityRequest, PerplexitySonar};

#[async_trait]
impl Chat for PerplexitySonar {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let resp = self.search_stream(PerplexityRequest::from(req)).await;

        Ok(Box::pin(resp.map_ok(|c| c.response)))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let resp = self.search_request(PerplexityRequest::from(req)).await?;

        for (i, c) in resp.citations.iter().enumerate() {
            tracing::debug!("{i}th citation for id={}. {}", resp.response.id, c);
        }

        Ok(resp.response)
    }
}
