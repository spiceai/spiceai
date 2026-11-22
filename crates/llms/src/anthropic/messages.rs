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
use http_body_limit::Limit;
use messages_api::stream::event_type;
use messages_api::{MessageStream, Messages, MessagesError, MessagesRequest};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value};

use super::{Anthropic, model_not_found_message};
use crate::config::HostedModelConfig;
use crate::model_http::Limits;

const ACTION: &str = "answer a Messages API request";

/// Serves the Messages API by forwarding each request to Anthropic.
pub struct AnthropicMessages {
    http: reqwest::Client,
    /// The most of an answer held in memory: a whole body once decoded, or one event of a
    /// stream.
    max_response_bytes: usize,
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
        let limits = Limits::default();
        AnthropicMessages {
            http: limits.http_client(),
            max_response_bytes: limits.max_response_bytes,
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
        let body = http_body_limit::read_prefix(response, self.max_response_bytes).await;
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
        let limit = Limit::Size(self.max_response_bytes);
        let body = http_body_limit::read_bytes(response, limit)
            .await
            .map_err(|e| {
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
        Ok(event_stream(
            response,
            self.model.clone(),
            self.max_response_bytes,
        ))
    }

    async fn count_tokens(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        self.json("/messages/count_tokens", request).await
    }
}

/// One server-sent event: its `data`, which for the Messages API is a JSON object whose `type`
/// is the event's name.
struct SseDecoder {
    /// The stream so far, its line endings written as LF.
    buffer: Vec<u8>,
    /// Whether the last byte was a CR, whose CRLF a LF at the start of the next chunk completes.
    after_cr: bool,
    /// Where in `buffer` a blank line can start: the bytes before it were searched already.
    searched: usize,
    /// The most bytes one event may hold, its blank line included and each line ending counted
    /// once.
    max_event_bytes: usize,
    /// Whether an event passed `max_event_bytes`, which ends the stream.
    refused: bool,
}

impl SseDecoder {
    fn new(max_event_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            after_cr: false,
            searched: 0,
            max_event_bytes,
            refused: false,
        }
    }

    /// Adds `chunk` and returns the data of each event it completes, up to the first that passes
    /// the cap, which sets `refused`.
    fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        // A line ends with LF, CRLF or CR. Each is written as LF as soon as it arrives, so an
        // event is dispatched at its blank line rather than when the next chunk shows whether a
        // CR began a CRLF; the LF of a CRLF is then dropped, in whichever chunk it comes.
        for &byte in chunk {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                continue;
            }
            if byte == b'\r' {
                self.buffer.push(b'\n');
                self.after_cr = true;
            } else {
                self.buffer.push(byte);
            }
        }
        let mut events = Vec::new();
        let mut start = 0;
        while let Some(at) = self.buffer[self.searched..]
            .windows(2)
            .position(|pair| pair == b"\n\n")
        {
            let end = self.searched + at;
            if end + 2 - start > self.max_event_bytes {
                self.refused = true;
                break;
            }
            let block = String::from_utf8_lossy(&self.buffer[start..end]);
            let data: Vec<&str> = block
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|data| data.strip_prefix(' ').unwrap_or(data))
                .collect();
            if !data.is_empty() {
                events.push(data.join("\n"));
            }
            start = end + 2;
            self.searched = start;
        }
        self.refused |= self.buffer.len() - start > self.max_event_bytes;
        self.buffer.drain(..start);
        self.searched = self.buffer.len().saturating_sub(1);
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
        if !self.done && self.decoder.refused {
            self.done = true;
            self.queue.push_back(Err(MessagesError::api(format!(
                "Anthropic streamed an event for model '{}' of more than {} bytes, so the stream \
                 was ended",
                self.model, self.decoder.max_event_bytes
            ))));
        }
    }
}

/// The events of `response`, a stream Anthropic opened. It ends after `message_stop`, or after
/// an `error` event, which becomes the stream's error.
fn event_stream(
    response: reqwest::Response,
    model: String,
    max_event_bytes: usize,
) -> MessageStream {
    let state = StreamState {
        response,
        decoder: SseDecoder::new(max_event_bytes),
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
    use futures::StreamExt;
    use rstest::rstest;
    use serde_json::json;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn an_answer_is_read_up_to_the_cap_and_refused_past_it() {
        let answer = r#"{"type":"message","content":[]}"#;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(answer, "application/json"))
            .mount(&server)
            .await;
        let messages = |max_response_bytes| AnthropicMessages {
            http: reqwest::Client::new(),
            max_response_bytes,
            config: HostedModelConfig::from_url(&server.uri()),
            model: "model".to_string(),
            model_from_default: false,
            endpoint_from_default: false,
        };

        messages(answer.len())
            .create(MessagesRequest::new(Map::new()))
            .await
            .expect("an answer of exactly the cap is read");
        let error = messages(answer.len() - 1)
            .create(MessagesRequest::new(Map::new()))
            .await
            .expect_err("an answer one byte past the cap is refused");
        assert_eq!(
            error.message,
            format!(
                "Failed to read Anthropic's answer for model 'model': the response body is larger \
                 than the {} bytes that is read from one HTTP response",
                answer.len() - 1
            )
        );
    }

    const SMALL_EVENT: &str = "event: ping\ndata: {\"type\":\"ping\"}\n\n";

    /// An event past the cap ends the stream however its bytes are split into chunks: in one
    /// chunk, in small pieces, or with only the last chunk taking it past the cap.
    #[rstest]
    #[case::one_chunk(vec![])]
    #[case::small_chunks((1..20).map(|i| i * 16).collect())]
    #[case::the_last_chunk_passes_the_cap(vec![SMALL_EVENT.len(), 2 * SMALL_EVENT.len()])]
    #[tokio::test]
    async fn a_streamed_event_past_the_cap_ends_the_stream(#[case] cuts: Vec<usize>) {
        let large = format!(
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"text\":\"{}\"}}\n\n",
            "b".repeat(100)
        );
        let body = format!("{SMALL_EVENT}{large}");
        let mut chunks = Vec::new();
        let mut start = 0;
        for end in cuts
            .into_iter()
            .filter(|&end| end < body.len())
            .chain([body.len()])
        {
            chunks.push(Ok::<_, std::io::Error>(Bytes::copy_from_slice(
                &body.as_bytes()[start..end],
            )));
            start = end;
        }
        let response = reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(
            futures::stream::iter(chunks),
        )));

        // The first event is exactly the cap.
        let events: Vec<_> = event_stream(response, "model".to_string(), SMALL_EVENT.len())
            .collect()
            .await;
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].as_ref().expect("the event at the cap is read"),
            &json!({"type": "ping"})
        );
        let error = events[1]
            .as_ref()
            .expect_err("the event past the cap is refused");
        assert_eq!(
            error.message,
            format!(
                "Anthropic streamed an event for model 'model' of more than {} bytes, so the \
                 stream was ended",
                SMALL_EVENT.len()
            )
        );
    }

    #[test]
    fn events_are_decoded_across_chunk_boundaries() {
        let mut decoder = SseDecoder::new(usize::MAX);
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
        assert_eq!(decoder.buffer, [] as [u8; 0]);
    }

    /// An event ends at a blank line, whichever of LF, CRLF or CR ends its lines, even when a
    /// CRLF is split across chunks.
    #[test]
    fn events_end_at_a_blank_line_with_any_line_ending() {
        let event = "event: ping\ndata: {\"type\":\"ping\"}\n\n";
        for ending in ["\n", "\r\n", "\r"] {
            let stream = event.replace('\n', ending);
            for size in [1, 2, 3, stream.len()] {
                let mut decoder = SseDecoder::new(usize::MAX);
                let mut events = Vec::new();
                for chunk in stream.as_bytes().chunks(size) {
                    events.extend(decoder.push(chunk));
                }
                assert_eq!(
                    events,
                    ["{\"type\":\"ping\"}"],
                    "{ending:?} in chunks of {size}"
                );
            }
        }
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
