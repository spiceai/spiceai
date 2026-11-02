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

//! Serves the Gemini API (`models/{model}:generateContent` and `:streamGenerateContent`) with any
//! chat model.
//!
//! [`GeminiChat`] translates a Gemini API request into a chat completion request
//! ([`request::chat_request`]), runs it through the model's chat completions, and translates
//! the completion back: a [`GenerateContentResponse`] ([`response::generate_content_response`]),
//! or a stream of them ([`stream::response_stream`]).

use std::sync::Arc;

use chat_api::Chat;
use gemini_api::{
    CountTokensRequest, CountTokensResponse, GeminiError, GenerateContentRequest,
    GenerateContentResponse, GenerateContentStream, Model,
};

pub mod request;
pub mod response;
pub mod stream;

/// The method a model listed by [`model_listing`] serves. Google lists `generateContent` alone
/// for a model that also streams it.
const GENERATE_CONTENT: &str = "generateContent";

/// Serves the Gemini API with a chat model.
pub struct GeminiChat {
    chat: Arc<dyn Chat>,
}

impl GeminiChat {
    #[must_use]
    pub fn new(chat: Arc<dyn Chat>) -> Self {
        Self { chat }
    }

    /// Answers `request`, a `models/{model}:generateContent` request, with `model` the model the
    /// URL names.
    ///
    /// # Errors
    ///
    /// Returns `INVALID_ARGUMENT` when the request asks for something a chat completion cannot
    /// carry, and the chat model's error, of the same kind, when it fails.
    pub async fn generate(
        &self,
        model: &str,
        request: GenerateContentRequest,
    ) -> Result<GenerateContentResponse, GeminiError> {
        let completion = self
            .chat
            .chat_request(request::chat_request(model, request, false)?)
            .await
            .map_err(response::gemini_error)?;
        response::generate_content_response(model, completion)
    }

    /// Answers `request`, a `models/{model}:streamGenerateContent` request, with a stream of
    /// responses.
    ///
    /// # Errors
    ///
    /// Returns `INVALID_ARGUMENT` when the request asks for something a chat completion cannot
    /// carry, and the chat model's error, of the same kind, when it fails to start the stream.
    /// An error after that ends the stream.
    pub async fn generate_stream(
        &self,
        model: &str,
        request: GenerateContentRequest,
    ) -> Result<GenerateContentStream, GeminiError> {
        let chunks = self
            .chat
            .chat_stream(request::chat_request(model, request, true)?)
            .await
            .map_err(response::gemini_error)?;
        Ok(stream::response_stream(chunks, model.to_string()))
    }

    /// Counts the tokens of `request`, a `models/{model}:countTokens` request.
    ///
    /// # Errors
    ///
    /// Always returns `INVALID_ARGUMENT`: counts are specific to a model's tokenizer, which a
    /// chat completion does not expose.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "`count_tokens` is async like the other methods of the API, so that a model that can count tokens can be asked without changing its callers"
    )]
    pub async fn count_tokens(
        &self,
        model: &str,
        _request: CountTokensRequest,
    ) -> Result<CountTokensResponse, GeminiError> {
        Err(GeminiError::invalid_argument(format!(
            "Model '{model}' serves the Gemini API by translating requests into chat completions, which cannot count tokens: counts are specific to the model's tokenizer. The `usageMetadata` of a `generateContent` response counts its prompt. See: {}",
            request::DOCS
        )))
    }
}

/// The listing of the model `id` in `GET /v1beta/models`, with its token limits: the
/// Gemini API's `inputTokenLimit` is what a prompt may hold, which is the window less the
/// answer's cap when the model has one, and `outputTokenLimit` that cap.
#[must_use]
pub fn model_listing(id: &str, limits: chat_api::TokenLimits) -> Model {
    let input = limits.context_window.map(|window| {
        limits
            .max_output_tokens
            .filter(|output| *output < window)
            .map_or(window, |output| window - output)
    });
    Model {
        name: format!("models/{id}"),
        display_name: Some(id.to_string()),
        input_token_limit: input.and_then(|limit| u32::try_from(limit).ok()),
        output_token_limit: limits
            .max_output_tokens
            .and_then(|limit| u32::try_from(limit).ok()),
        supported_generation_methods: vec![GENERATE_CONTENT.to_string()],
        ..Model::default()
    }
}

#[cfg(test)]
mod tests;
