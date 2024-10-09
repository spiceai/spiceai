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
use std::pin::Pin;

use crate::chat::nsql::structured_output::StructuredOutputSqlGeneration;
use crate::chat::nsql::{json::JsonSchemaSqlGeneration, SqlGeneration};
use crate::chat::{Chat, Error as ChatError, Result as ChatResult};
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
};

use async_openai::types::{
    ChatCompletionRequestSystemMessageArgs, CreateChatCompletionRequestArgs
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use snafu::ResultExt;
use tracing_futures::Instrument;

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

    async fn run(&self, prompt: String) -> ChatResult<Option<String>> {
        let span = tracing::Span::current();

        async move {
            let req = CreateChatCompletionRequestArgs::default()
                .model(self.model.clone())
                .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                    .content(prompt)
                    .build()
                    .boxed()
                    .map_err(|source| ChatError::FailedToLoadTokenizer { source })?
                    .into()])
                .build()
                .boxed()
                .map_err(|source| ChatError::FailedToLoadModel { source })?;

            let resp = self
                .chat_request(req)
                .await
                .boxed()
                .map_err(|source| ChatError::FailedToRunModel { source })?;

            Ok(resp
                .choices
                .into_iter()
                .next()
                .and_then(|c| c.message.content))
        }
        .instrument(span)
        .await
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        let req = CreateChatCompletionRequestArgs::default()
            .model(self.model.clone())
            .stream(true)
            .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()
                .boxed()
                .map_err(|source| ChatError::FailedToLoadTokenizer { source })?
                .into()])
            .build()
            .boxed()
            .map_err(|source| ChatError::FailedToLoadModel { source })?;
        let mut chat_stream = self
            .chat_stream(req)
            .await
            .boxed()
            .map_err(|source| ChatError::FailedToRunModel { source })?;
        Ok(Box::pin(stream! {
            while let Some(msg) = chat_stream.next().await {
                match msg {
                    Ok(resp) => {
                        yield Ok(resp.choices.into_iter().next().and_then(|c| c.delta.content));
                    }
                    Err(e) => {
                        yield Err(ChatError::FailedToRunModel { source: Box::new(e) });
                    }
                }
            }
        }))
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        let stream = self.client.chat().create_stream(inner_req).await?;

        Ok(Box::pin(stream))
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        self.client.chat().create(inner_req).await
    }
}