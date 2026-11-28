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

//! The Anthropic Messages API, as Spice serves it on `POST /v1/messages`.
//!
//! A model serves it in one of two ways: natively, by forwarding the request to a provider that
//! speaks the Messages API, or by translation through the model's chat completions. Both take
//! the request as the client sent it ([`MessagesRequest`]) and answer in the Messages API's own
//! shapes, so a client written for the Messages API works unchanged against either.
//!
//! A native implementation forwards the body as JSON, so a field this crate does not name still
//! reaches the provider. A translating implementation reads the body through [`types`], which
//! names every field it can carry over and keeps the rest, so that it can refuse what it cannot
//! carry over rather than drop it.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use serde_json::{Map, Value};

pub mod error;
pub mod stream;
pub mod types;

pub use error::{ErrorType, MessagesError};

/// The `anthropic-version` a request is answered under when it names none. It is the only
/// version the Messages API has.
pub const DEFAULT_VERSION: &str = "2023-06-01";

/// A request to `POST /v1/messages` or `POST /v1/messages/count_tokens`, as the client sent it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MessagesRequest {
    /// The request body.
    pub body: Map<String, Value>,
    /// The beta features the client asked for in `anthropic-beta`, in order, each once.
    pub betas: Vec<String>,
    /// The `anthropic-version` the client asked for.
    pub version: Option<String>,
}

impl MessagesRequest {
    #[must_use]
    pub fn new(body: Map<String, Value>) -> Self {
        Self {
            body,
            betas: Vec::new(),
            version: None,
        }
    }

    /// The model the request names.
    #[must_use]
    pub fn model(&self) -> Option<&str> {
        self.body.get("model").and_then(Value::as_str)
    }

    /// Whether the request asks for its answer as a stream of events.
    #[must_use]
    pub fn is_stream(&self) -> bool {
        self.body
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    /// Sets the beta features from the values of the `anthropic-beta` headers, each a
    /// comma-separated list.
    #[must_use]
    pub fn with_beta_headers<'a>(mut self, values: impl IntoIterator<Item = &'a str>) -> Self {
        for beta in values
            .into_iter()
            .flat_map(|value| value.split(','))
            .map(str::trim)
            .filter(|beta| !beta.is_empty())
        {
            if !self.betas.iter().any(|known| known == beta) {
                self.betas.push(beta.to_string());
            }
        }
        self
    }

    #[must_use]
    pub fn with_version(mut self, version: Option<String>) -> Self {
        self.version = version;
        self
    }
}

/// The events of a streamed answer, each one a Messages API event whose `type` names it (see
/// [`stream::event_type`]). An error ends the stream; it is sent to the client as an `error`
/// event.
pub type MessageStream = Pin<Box<dyn Stream<Item = Result<Value, MessagesError>> + Send>>;

/// A model that answers Messages API requests.
///
/// The methods have no default implementations, so a wrapper has to forward each one.
#[async_trait]
pub trait Messages: Send + Sync {
    /// Answers `request` with a Message, the JSON object `POST /v1/messages` returns.
    async fn create(&self, request: MessagesRequest) -> Result<Value, MessagesError>;

    /// Answers `request` with a stream of events.
    async fn create_stream(&self, request: MessagesRequest)
    -> Result<MessageStream, MessagesError>;

    /// Counts the input tokens of `request`, the JSON object
    /// `POST /v1/messages/count_tokens` returns.
    async fn count_tokens(&self, request: MessagesRequest) -> Result<Value, MessagesError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn beta_headers_are_split_trimmed_and_kept_once_in_order() {
        let request = MessagesRequest::default().with_beta_headers([
            "interleaved-thinking-2025-05-14, context-management-2025-06-27",
            "context-management-2025-06-27,,files-api-2025-04-14 ",
        ]);
        assert_eq!(
            request.betas,
            [
                "interleaved-thinking-2025-05-14",
                "context-management-2025-06-27",
                "files-api-2025-04-14",
            ]
        );
    }

    #[test]
    fn model_and_stream_are_read_from_the_body() {
        let body = json!({"model": "sonnet", "stream": true})
            .as_object()
            .cloned()
            .expect("an object");
        let request = MessagesRequest::new(body);
        assert_eq!(
            (request.model(), request.is_stream()),
            (Some("sonnet"), true)
        );
        assert_eq!(
            (
                MessagesRequest::default().model(),
                MessagesRequest::default().is_stream()
            ),
            (None, false)
        );
    }
}
