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

use llms::chat::{Chat, Error as ChatError, Result as ChatResult};

use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionResponseStream, ChatCompletionTool, ChatCompletionToolChoiceOption,
    CreateChatCompletionRequest, CreateChatCompletionResponse,
};

use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestSystemMessageArgs, CreateChatCompletionRequestArgs, EmbeddingInput,
    },
    Client,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use snafu::ResultExt;

pub struct ToolUsingChat {
    inner_chat: Box<dyn Chat>,
}

impl ToolUsingChat {
    pub fn new(inner_chat: Box<dyn Chat>) -> Self {
        Self { inner_chat }
    }

    pub fn runtime_tools(&self) -> Vec<ChatCompletionTool> {
        vec![]
    }
}

#[async_trait]
impl Chat for ToolUsingChat {
    async fn run(&mut self, prompt: String) -> ChatResult<Option<String>> {
        self.inner_chat.run(prompt).await
    }

    async fn stream<'a>(
        &mut self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        self.inner_chat.stream(prompt).await
    }

    async fn chat_stream(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use use tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
        {
            return self.inner_chat.chat_stream(req).await;
        };

        // Append spiced runtime tools to the request.
        let mut inner_req = req.clone();
        let mut runtime_tools = self.runtime_tools();
        if !runtime_tools.is_empty() {
            runtime_tools.extend(req.tools.unwrap_or_default());
            inner_req.tools = Some(runtime_tools);
        };

        self.inner_chat.chat_stream(inner_req).await
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    ///
    /// TODO: If response messages has ChatCompletionMessageToolCall that are spiced runtime tools, call locally, and pass back.
    async fn chat_request(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use use tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
        {
            return self.inner_chat.chat_request(req).await;
        };

        // Append spiced runtime tools to the request.
        let mut inner_req = req.clone();
        let mut runtime_tools = self.runtime_tools();
        if !runtime_tools.is_empty() {
            runtime_tools.extend(req.tools.unwrap_or_default());
            inner_req.tools = Some(runtime_tools);
        };

        self.inner_chat.chat_request(inner_req).await
    }
}
