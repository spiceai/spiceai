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

//! The Messages API served natively: a request goes to Anthropic as the client sent it, with
//! only the model replaced by the one the Spicepod names, and the answer comes back as Anthropic
//! sent it. Fields, content blocks, tools and events this crate does not know of pass through.

use std::collections::VecDeque;

use async_openai::config::Config;
use async_trait::async_trait;
use bytes::Bytes;
use messages_api::stream::event_type;
use messages_api::{MessageStream, Messages, MessagesError, MessagesRequest};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};

use super::{Anthropic, model_not_found_message};
use crate::config::HostedModelConfig;

const ACTION: &str = "answer a Messages API request";

/// Serves the Messages API by forwarding each request to Anthropic.
pub struct AnthropicMessages {
    http: reqwest::Client,
    config: HostedModelConfig,
    model: String,
    model_from_default: bool,
    endpoint_from_default: bool,
}

impl Anthropic {
    /// The Messages API, served natively by this model.
    ///
    /// The requests carry the client's own `anthropic-beta` features and no others: the
    /// features chat completions enable for themselves are not the client's to receive.
    #[must_use]
    pub fn messages(&self) -> AnthropicMessages {
        let mut config = self.client.config().clone();
        config.default_headers.remove("anthropic-beta");
        AnthropicMessages {
            http: reqwest::Client::new(),
            config,
            model: self.model.clone(),
            model_from_default: self.model_from_default,
            endpoint_from_default: self.endpoint_from_default,
        }
    }
}

impl AnthropicMessages {
    /// The model id Anthropic serves the requests with.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    fn headers(&self, request: &MessagesRequest) -> HeaderMap {
        let mut headers = self.config.headers();
        if let Some(version) = request
            .version
            .as_deref()
            .and_then(|version| HeaderValue::from_str(version).ok())
        {
            headers.insert(HeaderName::from_static("anthropic-version"), version);
        }
        if !request.betas.is_empty()
            && let Ok(betas) = HeaderValue::from_str(&request.betas.join(","))
        {
            headers.insert(HeaderName::from_static("anthropic-beta"), betas);
        }
        headers
    }

    fn body(&self, request: MessagesRequest) -> Map<String, Value> {
        let mut body = request.body;
        body.insert("model".to_string(), Value::String(self.model.clone()));
        if let Some(Value::Array(messages)) = body.get_mut("messages") {
            drop_unsigned_thinking(messages);
        }
        body
    }

    /// Sends `body` to `path` and returns the response, or the error Anthropic answered with.
    async fn send(
        &self,
        path: &str,
        request: MessagesRequest,
    ) -> Result<reqwest::Response, MessagesError> {
        let headers = self.headers(&request);
        let url = self.config.url(path);
        let response = self
            .http
            .post(&url)
            .headers(headers)
            .json(&self.body(request))
            .send()
            .await
            .map_err(|e| {
                MessagesError::api(format!(
                    "Failed to {ACTION} with Anthropic model '{}': could not reach {url}: {e}",
                    self.model
                ))
                .with_status(502)
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.bytes().await.unwrap_or_default();
        Err(self.explain(MessagesError::from_response(status.as_u16(), &body)))
    }

    /// `error` with the message for a model Anthropic does not serve, which names the model and
    /// the parameter to change; Anthropic's own says only `model: <id>`.
    fn explain(&self, mut error: MessagesError) -> MessagesError {
        // Anthropic answers a model it does not serve with `model: <id>`; a `not_found_error`
        // about anything else, such as a file the request names, is left as it is.
        if error.error_type == messages_api::ErrorType::NotFound
            && error.message.starts_with("model:")
        {
            error.message = model_not_found_message(
                ACTION,
                &self.model,
                self.model_from_default,
                self.endpoint_from_default,
            );
        }
        error
    }

    async fn json(&self, path: &str, request: MessagesRequest) -> Result<Value, MessagesError> {
        let response = self.send(path, request).await?;
        let body = response.bytes().await.map_err(|e| {
            MessagesError::api(format!(
                "Failed to read Anthropic's answer for model '{}': {e}",
                self.model
            ))
            .with_status(502)
        })?;
        serde_json::from_slice(&body).map_err(|e| {
            MessagesError::api(format!(
                "Anthropic answered model '{}' with a body that is not JSON: {e}",
                self.model
            ))
            .with_status(502)
        })
    }
}

/// Drops each `thinking` block without a signature. Anthropic signs every thinking block it
/// sends and rejects a request carrying an unsigned one, so an unsigned block was written by a
/// model the Messages API was served by translation, earlier in the conversation. An assistant
/// message left with nothing is dropped with it.
fn drop_unsigned_thinking(messages: &mut Vec<Value>) {
    let unsigned = |block: &Value| {
        block.get("type").and_then(Value::as_str) == Some("thinking")
            && block
                .get("signature")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
    };
    let mut emptied = false;
    for message in messages.iter_mut() {
        if let Some(Value::Array(content)) = message.get_mut("content")
            && content.iter().any(unsigned)
        {
            content.retain(|block| !unsigned(block));
            emptied |= content.is_empty();
        }
    }
    if emptied {
        messages.retain(|message| {
            message
                .get("content")
                .and_then(Value::as_array)
                .is_none_or(|content| !content.is_empty())
        });
    }
}

#[async_trait]
impl Messages for AnthropicMessages {
    async fn create(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        self.json("/messages", request).await
    }

    async fn create_stream(
        &self,
        request: MessagesRequest,
    ) -> Result<MessageStream, MessagesError> {
        let response = self.send("/messages", request).await?;
        Ok(event_stream(response, self.model.clone()))
    }

    async fn count_tokens(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        self.json("/messages/count_tokens", request).await
    }
}

/// One server-sent event: its `data`, which for the Messages API is a JSON object whose `type`
/// is the event's name.
#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    /// Adds `chunk` and returns the data of each event it completes.
    fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        // A Messages API event's data is JSON, in which a carriage return is always escaped, so
        // one in the stream only ever ends a line.
        self.buffer
            .extend(chunk.iter().copied().filter(|byte| *byte != b'\r'));
        let mut events = Vec::new();
        while let Some(end) = self.buffer.windows(2).position(|pair| pair == b"\n\n") {
            let block: Vec<u8> = self.buffer.drain(..end + 2).collect();
            let block = String::from_utf8_lossy(&block[..end]);
            let data: Vec<&str> = block
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|data| data.strip_prefix(' ').unwrap_or(data))
                .collect();
            if !data.is_empty() {
                events.push(data.join("\n"));
            }
        }
        events
    }
}

struct StreamState {
    response: reqwest::Response,
    decoder: SseDecoder,
    queue: VecDeque<Result<Value, MessagesError>>,
    model: String,
    done: bool,
}

impl StreamState {
    fn decode(&mut self, chunk: &Bytes) {
        for data in self.decoder.push(chunk) {
            if self.done {
                return;
            }
            match serde_json::from_str::<Value>(&data) {
                Ok(event) => match event_type(&event) {
                    Some("error") => {
                        self.done = true;
                        self.queue
                            .push_back(Err(MessagesError::from_envelope(&event)
                                .unwrap_or_else(|| MessagesError::api(data.clone()))));
                    }
                    Some("message_stop") => {
                        self.done = true;
                        self.queue.push_back(Ok(event));
                    }
                    _ => self.queue.push_back(Ok(event)),
                },
                Err(e) => {
                    self.done = true;
                    self.queue.push_back(Err(MessagesError::api(format!(
                        "Anthropic streamed an event for model '{}' that is not JSON: {e}",
                        self.model
                    ))));
                }
            }
        }
    }
}

/// The events of `response`, a stream Anthropic opened. It ends after `message_stop`, or after
/// an `error` event, which becomes the stream's error.
fn event_stream(response: reqwest::Response, model: String) -> MessageStream {
    let state = StreamState {
        response,
        decoder: SseDecoder::default(),
        queue: VecDeque::new(),
        model,
        done: false,
    };
    Box::pin(futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(item) = state.queue.pop_front() {
                return Some((item, state));
            }
            if state.done {
                return None;
            }
            match state.response.chunk().await {
                Ok(Some(chunk)) => state.decode(&chunk),
                Ok(None) => {
                    state.done = true;
                    state.queue.push_back(Err(MessagesError::api(format!(
                        "Anthropic ended the stream for model '{}' before `message_stop`, so the message is incomplete.",
                        state.model
                    ))));
                }
                Err(e) => {
                    state.done = true;
                    state.queue.push_back(Err(MessagesError::api(format!(
                        "Failed to read Anthropic's stream for model '{}': {e}",
                        state.model
                    ))));
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn events_are_decoded_across_chunk_boundaries() {
        let mut decoder = SseDecoder::default();
        let stream = "event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: ping\r\ndata: {\"type\": \"ping\"}\r\n\r\n: comment\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"h\u{e9}\"}}\n\n";
        let bytes = stream.as_bytes();
        let mut events = Vec::new();
        // Split inside a multi-byte character, a line ending, and the event separator.
        for chunk in bytes.chunks(7) {
            events.extend(decoder.push(chunk));
        }
        assert_eq!(
            events,
            [
                "{\"type\":\"message_start\"}",
                "{\"type\": \"ping\"}",
                "{\"type\":\"content_block_delta\",\"delta\":{\"text\":\"h\u{e9}\"}}",
            ]
        );
        assert!(decoder.buffer.is_empty());
    }

    #[test]
    fn unsigned_thinking_is_dropped_and_signed_thinking_kept() {
        let mut messages = vec![
            json!({"role": "user", "content": "q1"}),
            json!({"role": "assistant", "content": [
                {"type": "thinking", "thinking": "from a translated model", "signature": ""},
                {"type": "text", "text": "a1"},
            ]}),
            json!({"role": "user", "content": "q2"}),
            json!({"role": "assistant", "content": [{"type": "thinking", "thinking": "only thinking"}]}),
            json!({"role": "user", "content": "q3"}),
            json!({"role": "assistant", "content": [
                {"type": "thinking", "thinking": "", "signature": "EqQBCkgIAR"},
                {"type": "text", "text": "a3"},
            ]}),
        ];
        drop_unsigned_thinking(&mut messages);
        assert_eq!(
            messages,
            vec![
                json!({"role": "user", "content": "q1"}),
                json!({"role": "assistant", "content": [{"type": "text", "text": "a1"}]}),
                json!({"role": "user", "content": "q2"}),
                json!({"role": "user", "content": "q3"}),
                json!({"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "", "signature": "EqQBCkgIAR"},
                    {"type": "text", "text": "a3"},
                ]}),
            ]
        );
    }
}
