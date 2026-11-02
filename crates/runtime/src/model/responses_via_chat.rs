/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The Responses API of a model whose provider has none of its own.

use std::sync::Arc;

use async_openai::error::OpenAIError;
use async_openai::types::responses::{CreateResponse, Response, ResponseStream};
use async_trait::async_trait;
use llms::chat::Chat;
use llms::responses::Responses;
use responses_chat::ChatResponses;

/// Serves `/v1/responses` with a chat model: each request is translated into a chat
/// completion, and the completion, or its stream of chunks, back into the Responses API.
///
/// Built per request rather than when the model loads, as the Messages API translation is:
/// it holds nothing but the chat model, which the chat model store already owns.
pub struct ResponsesViaChat {
    responses: ChatResponses,
}

impl ResponsesViaChat {
    #[must_use]
    pub fn new(chat: Arc<dyn Chat>) -> Self {
        Self {
            responses: ChatResponses::new(chat),
        }
    }
}

#[async_trait]
impl Responses for ResponsesViaChat {
    /// Nothing to check of its own: the chat model passed its health check when it loaded.
    async fn health(&self) -> llms::responses::Result<()> {
        Ok(())
    }

    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError> {
        self.responses.create_stream(req).await
    }

    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
        self.responses.create(req).await
    }
}
