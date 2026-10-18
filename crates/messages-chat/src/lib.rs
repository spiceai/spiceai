/*
Copyright 2026 The Spice.ai OSS Authors

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

//! The Anthropic Messages API, served by any chat model.
//!
//! [`ChatMessages`] translates a Messages API request into a chat completion request
//! ([`request::chat_request`]), runs it through the model's chat completions, and translates
//! the completion back: a [`Message`](messages_api::types::Message) ([`response::message`]),
//! or its stream of events ([`stream::message_stream`]).

use std::sync::Arc;

use async_trait::async_trait;
use chat_api::Chat;
use messages_api::types::CreateMessageRequest;
use messages_api::{MessageStream, Messages, MessagesError, MessagesRequest};
use serde_json::Value;

pub mod request;
pub mod response;
pub mod stream;

/// Serves the Messages API with a chat model.
pub struct ChatMessages {
    chat: Arc<dyn Chat>,
}

impl ChatMessages {
    #[must_use]
    pub fn new(chat: Arc<dyn Chat>) -> Self {
        Self { chat }
    }
}

/// The request's body, typed.
///
/// # Errors
///
/// Returns an `invalid_request_error` when the body is not a Messages API request.
pub fn parse(request: MessagesRequest) -> Result<CreateMessageRequest, MessagesError> {
    serde_json::from_value(Value::Object(request.body))
        .map_err(|e| MessagesError::invalid_request(e.to_string()))
}

#[async_trait]
impl Messages for ChatMessages {
    async fn create(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        let mut request = parse(request)?;
        request.stream = None;
        let show_thinking = request
            .thinking
            .as_ref()
            .is_some_and(messages_api::types::ThinkingConfig::shows_thinking);
        let completion = self
            .chat
            .chat_request(request::chat_request(request)?)
            .await
            .map_err(response::messages_error)?;
        let message = response::message(completion, show_thinking)?;
        serde_json::to_value(message)
            .map_err(|e| MessagesError::api(format!("Failed to serialize the message: {e}")))
    }

    async fn create_stream(&self, request: MessagesRequest) -> Result<MessageStream, MessagesError> {
        let mut request = parse(request)?;
        request.stream = Some(true);
        let model = request.model.clone();
        let chunks = self
            .chat
            .chat_stream(request::chat_request(request)?)
            .await
            .map_err(response::messages_error)?;
        Ok(stream::message_stream(chunks, model))
    }

    async fn count_tokens(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        let model = request.model().unwrap_or_default();
        Err(MessagesError::invalid_request(format!(
            "Model '{model}' serves the Messages API by translating requests into chat completions, and only a model with `from: anthropic` can count tokens: counts are specific to the model's tokenizer. See: {}",
            request::DOCS
        )))
    }
}

#[cfg(test)]
mod tests;
