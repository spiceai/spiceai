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

//! The `OpenAI` Responses API, served by any chat model.
//!
//! [`ChatResponses`] translates a Responses API request into a chat completion request
//! ([`request::chat_request`]), runs it through the model's chat completions, and translates
//! the completion back: a [`Response`] ([`response::response`]), or its stream of events
//! ([`stream::response_stream`]).

use std::sync::Arc;

use async_openai::error::OpenAIError;
use async_openai::types::responses::{CreateResponse, Response, ResponseStream};
use chat_api::Chat;

pub mod request;
pub mod response;
pub mod stream;

/// Serves the Responses API with a chat model.
pub struct ChatResponses {
    chat: Arc<dyn Chat>,
}

impl ChatResponses {
    #[must_use]
    pub fn new(chat: Arc<dyn Chat>) -> Self {
        Self { chat }
    }

    /// Answers `request` with one Response.
    ///
    /// # Errors
    ///
    /// Returns an `invalid_request_error` naming the part of `request` a chat completion cannot
    /// carry, the chat model's own error when the completion fails, and a `server_error` when
    /// the completion cannot be a Response.
    pub async fn create(&self, mut request: CreateResponse) -> Result<Response, OpenAIError> {
        request.stream = None;
        let translated = request::chat_request(request)?;
        let completion = self.chat.chat_request(translated.chat).await?;
        response::response(completion, &translated.context)
    }

    /// Answers `request` with a Response's stream of events. Once the stream has started, a
    /// failure of the chat model ends it with `response.failed`.
    ///
    /// # Errors
    ///
    /// Returns an `invalid_request_error` naming the part of `request` a chat completion cannot
    /// carry, and the chat model's own error when it does not start streaming.
    pub async fn create_stream(
        &self,
        mut request: CreateResponse,
    ) -> Result<ResponseStream, OpenAIError> {
        request.stream = Some(true);
        let translated = request::chat_request(request)?;
        let chunks = self.chat.chat_stream(translated.chat).await?;
        Ok(stream::response_stream(chunks, translated.context))
    }
}

#[cfg(test)]
mod tests;
