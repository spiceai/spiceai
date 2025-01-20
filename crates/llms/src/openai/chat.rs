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
#![allow(clippy::missing_errors_doc)]

use crate::chat::nsql::structured_output::StructuredOutputSqlGeneration;
use crate::chat::nsql::{json::JsonSchemaSqlGeneration, SqlGeneration};
use crate::chat::Chat;
use async_openai::config::Config;
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ChatCompletionResponseStream,
    CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_trait::async_trait;
use futures::TryStreamExt;
use tracing_futures::Instrument;

use super::Openai;

#[async_trait]
impl<C: Config + Send + Sync> Chat for Openai<C> {
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

    // Custom healthcheck for OpenAI because Azure dosn't support `max_completion_tokens`.
    #[allow(deprecated)]
    async fn health(&self) -> Result<(), crate::chat::Error> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");

        let mut req = CreateChatCompletionRequest {
            messages: vec![ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessage {
                    name: None,
                    content: ChatCompletionRequestUserMessageContent::Text("ping.".to_string()),
                },
            )],
            ..Default::default()
        };
        if self.supports_max_completion_tokens() {
            req.max_completion_tokens = Some(100);
        } else {
            req.max_tokens = Some(100);
        }

        if let Err(e) = self.chat_request(req).instrument(span.clone()).await {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(crate::chat::Error::HealthCheckError {
                source: Box::new(e),
            });
        }
        Ok(())
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
