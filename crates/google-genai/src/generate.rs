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

use crate::client::Client;
use crate::error::{HttpSnafu, JsonSnafu, Result, StreamSnafu, handle_unsuccessful_response};
use crate::types::{
    CachedContent, Candidate, Content, GenerationConfig, SafetySetting, Tool, ToolConfig,
    UsageMetadata,
};
use futures::Stream;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::pin::Pin;
use tokio_stream::StreamExt;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateContentRequest {
    pub contents: Vec<Content>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_config: Option<ToolConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_settings: Option<Vec<SafetySetting>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_config: Option<GenerationConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_instruction: Option<Content>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_content: Option<CachedContent>,
}

impl GenerateContentRequest {
    #[must_use]
    pub fn new(contents: Vec<Content>) -> Self {
        Self {
            contents,
            tools: None,
            tool_config: None,
            safety_settings: None,
            generation_config: None,
            system_instruction: None,
            cached_content: None,
        }
    }

    #[must_use]
    pub fn with_generation_config(mut self, config: GenerationConfig) -> Self {
        self.generation_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_tools(mut self, tools: Vec<Tool>) -> Self {
        self.tools = Some(tools);
        self
    }

    #[must_use]
    pub fn with_tool_config(mut self, config: ToolConfig) -> Self {
        self.tool_config = Some(config);
        self
    }

    #[must_use]
    pub fn with_safety_settings(mut self, settings: Vec<SafetySetting>) -> Self {
        self.safety_settings = Some(settings);
        self
    }

    #[must_use]
    pub fn with_system_instruction(mut self, instruction: Content) -> Self {
        self.system_instruction = Some(instruction);
        self
    }

    #[must_use]
    pub fn with_cached_content(mut self, cached: CachedContent) -> Self {
        self.cached_content = Some(cached);
        self
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateContentResponse {
    pub candidates: Vec<Candidate>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage_metadata: Option<UsageMetadata>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_feedback: Option<PromptFeedback>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptFeedback {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_ratings: Option<Vec<crate::types::SafetyRating>>,
}

impl Client {
    pub async fn generate_content(
        &self,
        model: &str,
        request: GenerateContentRequest,
    ) -> Result<GenerateContentResponse> {
        let url = self.build_url(&format!("/models/{model}:generateContent"));

        let headers = self.add_api_key_header(HeaderMap::new());

        let response = self
            .http_client()
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .context(HttpSnafu)?;

        if !response.status().is_success() {
            return Err(handle_unsuccessful_response(response).await);
        }

        response
            .json::<GenerateContentResponse>()
            .await
            .context(HttpSnafu)
    }

    pub async fn stream_generate_content(
        &self,
        model: &str,
        request: GenerateContentRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<GenerateContentResponse>> + Send>>> {
        let url = format!(
            "{}?alt=sse",
            self.build_url(&format!("/models/{model}:streamGenerateContent"))
        );

        let headers = self.add_api_key_header(HeaderMap::new());

        let response = self
            .http_client()
            .post(&url)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .context(HttpSnafu)?;

        if !response.status().is_success() {
            return Err(handle_unsuccessful_response(response).await);
        }

        let stream = response.bytes_stream();

        let parsed_stream = parse_sse_stream(Box::pin(stream));

        Ok(Box::pin(parsed_stream))
    }
}

/// Length of the line terminator beginning at `buf[i]`, or `None` if one does not begin there.
///
/// A `\r` that is the last byte of `buf` reports `None`: it may be the first half of a `\r\n`
/// whose `\n` has not been read yet, and only more bytes can say which.
fn terminator_len(buf: &[u8], i: usize) -> Option<usize> {
    match buf.get(i)? {
        b'\n' => Some(1),
        b'\r' if buf.get(i + 1) == Some(&b'\n') => Some(2),
        b'\r' if i + 1 < buf.len() => Some(1),
        _ => None,
    }
}

/// Byte offsets `(event_end, next_event_start)` of the blank line that terminates the first event
/// in `buf`, or `None` while more bytes could still complete one.
///
/// A line ends with `\n`, `\r\n` or `\r`, so the blank line is any two consecutive terminators.
/// Searching the bytes - rather than decoding each network read as text and searching that - is
/// what makes the decode below land on a character boundary: a multi-byte UTF-8 sequence never
/// contains `\n` or `\r`, so an event never ends inside one.
fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        let Some(first) = terminator_len(buf, i) else {
            i += 1;
            continue;
        };

        let after = i + first;
        if let Some(second) = terminator_len(buf, after) {
            return Some((i, after + second));
        }

        // Either the next line carries data, or the buffer stops before a blank line can be told
        // apart from a terminator that has arrived only halfway.
        if after >= buf.len() {
            return None;
        }
        i = after;
    }

    None
}

/// The payload of one SSE event: the values of its `data:` fields joined by `\n`, or `None` for an
/// event that carries no `data:` field at all, such as a comment or a bare `event:` line.
fn event_data(event: &str) -> Option<String> {
    let mut data: Option<String> = None;

    // Split on either terminator so an event framed with `\r`, `\n` or `\r\n` reads the same way;
    // the empty segment a `\r\n` leaves behind carries no field and is skipped.
    for line in event.split(['\r', '\n']) {
        let Some(value) = line.strip_prefix("data:") else {
            continue;
        };

        // One space after the colon is part of the framing, not of the value.
        let value = value.strip_prefix(' ').unwrap_or(value);

        match &mut data {
            Some(data) => {
                data.push('\n');
                data.push_str(value);
            }
            None => data = Some(value.to_string()),
        }
    }

    data
}

fn parse_sse_stream(
    stream: Pin<Box<dyn Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Send>>,
) -> impl Stream<Item = Result<GenerateContentResponse>> + Send {
    futures::stream::unfold(
        (stream, Vec::<u8>::new()),
        |(mut stream, mut buffer)| async move {
            loop {
                // Take every event the buffer already holds before reading again: one read can
                // carry a comment or keep-alive ahead of a data event, and awaiting another read
                // would report the end of the stream instead of the event already in hand.
                while let Some((event_end, next_event_start)) = find_event_boundary(&buffer) {
                    let event = std::str::from_utf8(&buffer[..event_end]).map(str::to_string);

                    // Consume the event either way, so a stream carrying one undecodable event
                    // reports it once and then continues rather than repeating it forever.
                    buffer.drain(..next_event_start);

                    let event = match event {
                        Ok(event) => event,
                        Err(e) => {
                            return Some((
                                StreamSnafu {
                                    message: format!("Invalid UTF-8 in SSE event: {e}"),
                                }
                                .fail(),
                                (stream, buffer),
                            ));
                        }
                    };

                    let Some(data) = event_data(&event) else {
                        continue;
                    };

                    if data == "[DONE]" {
                        return None;
                    }

                    if data.trim().is_empty() {
                        continue;
                    }

                    let result = serde_json::from_str(&data).context(JsonSnafu);

                    return Some((result, (stream, buffer)));
                }

                match stream.next().await {
                    Some(Ok(bytes)) => buffer.extend_from_slice(&bytes),
                    Some(Err(e)) => {
                        return Some((
                            StreamSnafu {
                                message: e.to_string(),
                            }
                            .fail(),
                            (stream, buffer),
                        ));
                    }
                    None => {
                        if buffer.is_empty() {
                            return None;
                        }

                        // Report the truncated tail once and end there. Consumers forward each
                        // item rather than stopping at the first error, so leaving the tail in the
                        // buffer would yield this same error on every later poll: the stream would
                        // never end.
                        buffer.clear();

                        return Some((
                            StreamSnafu {
                                message: "Unexpected end of stream while parsing SSE event"
                                    .to_string(),
                            }
                            .fail(),
                            (stream, buffer),
                        ));
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_content_request() {
        let request = GenerateContentRequest::new(vec![Content::user("Hello")])
            .with_generation_config(GenerationConfig {
                temperature: Some(0.7),
                max_output_tokens: Some(1024),
                ..Default::default()
            });

        assert_eq!(request.contents.len(), 1);
        assert!(request.generation_config.is_some());
    }

    #[tokio::test]
    async fn test_parse_sse_stream() {
        let body = "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hello\"}]}}]}\r\n\r\n";

        let stream =
            futures::stream::once(async move { Ok::<_, reqwest::Error>(bytes::Bytes::from(body)) });

        let mut parsed_stream = Box::pin(parse_sse_stream(Box::pin(stream)));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("should parse successfully");

        assert_eq!(first.candidates.len(), 1);
    }

    #[tokio::test]
    async fn test_parse_sse_stream_partial_chunk() {
        let body = "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\"";

        let stream =
            futures::stream::once(async move { Ok::<_, reqwest::Error>(bytes::Bytes::from(body)) });

        let mut parsed_stream = Box::pin(parse_sse_stream(Box::pin(stream)));

        let chunk = parsed_stream
            .next()
            .await
            .expect("stream should yield one item");

        chunk.expect_err("partial chunk should be an error");
    }

    #[tokio::test]
    async fn test_parse_sse_stream_invalid_chunk() {
        let body = "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\"\r\n\r\n";

        let stream =
            futures::stream::once(async move { Ok::<_, reqwest::Error>(bytes::Bytes::from(body)) });

        let mut parsed_stream = Box::pin(parse_sse_stream(Box::pin(stream)));

        let chunk = parsed_stream
            .next()
            .await
            .expect("stream should yield one item");

        chunk.expect_err("chunk should be an error");
    }

    /// A stream that hands out exactly the byte groups it is given, so a test can put a boundary
    /// wherever it likes - including inside a multi-byte character or a `\r\n` pair.
    fn dribble(
        reads: Vec<&[u8]>,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Send>> {
        let reads: Vec<_> = reads
            .into_iter()
            .map(|read| Ok(bytes::Bytes::copy_from_slice(read)))
            .collect();

        Box::pin(futures::stream::iter(reads))
    }

    fn text_of(response: &GenerateContentResponse) -> String {
        response
            .candidates
            .iter()
            .flat_map(|candidate| &candidate.content.parts)
            .map(|part| match part {
                crate::types::Part::Text { text } => text.clone(),
                other => panic!("expected a text part, got {other:?}"),
            })
            .collect()
    }

    fn text_event(text: &str) -> String {
        format!(
            "data: {{\"candidates\":[{{\"content\":{{\"role\":\"model\",\"parts\":[{{\"text\":\"{text}\"}}]}}}}]}}\r\n\r\n"
        )
    }

    #[tokio::test]
    async fn a_character_split_across_two_reads_survives() {
        // "é" is two bytes; the read boundary falls between them.
        let event = text_event("caf\u{e9}");
        let bytes = event.as_bytes();
        let split = event
            .find('\u{e9}')
            .expect("the event should carry the character")
            + 1;

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![
            &bytes[..split],
            &bytes[split..],
        ])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("a character split across two reads should still parse");

        assert_eq!(text_of(&first), "caf\u{e9}");
    }

    #[tokio::test]
    async fn a_four_byte_character_split_across_three_reads_survives() {
        let event = text_event("hi \u{1f600}");
        let bytes = event.as_bytes();
        let start = event
            .find('\u{1f600}')
            .expect("the event should carry the character");

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![
            &bytes[..=start],
            &bytes[start + 1..start + 3],
            &bytes[start + 3..],
        ])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("a character split across three reads should still parse");

        assert_eq!(text_of(&first), "hi \u{1f600}");
    }

    #[tokio::test]
    async fn a_crlf_split_across_two_reads_still_terminates_the_event() {
        let event = text_event("Hello");
        let bytes = event.as_bytes();
        // Split inside the final `\r\n`, so neither read holds a complete terminator pair.
        let split = bytes.len() - 1;

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![
            &bytes[..split],
            &bytes[split..],
        ])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("an event whose terminator spans two reads should parse");

        assert_eq!(text_of(&first), "Hello");
    }

    #[tokio::test]
    async fn a_bare_lf_terminated_event_parses() {
        let event = text_event("Hello").replace("\r\n", "\n");

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![event.as_bytes()])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("an `\\n\\n`-separated event should parse");

        assert_eq!(text_of(&first), "Hello");
    }

    #[tokio::test]
    async fn a_keep_alive_ahead_of_the_last_event_does_not_hide_it() {
        let body = format!(": keep-alive\r\n\r\n{}", text_event("Hello"));

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![body.as_bytes()])));

        let first = parsed_stream
            .next()
            .await
            .expect("the data event after the keep-alive should still be yielded")
            .expect("should parse successfully");

        assert_eq!(text_of(&first), "Hello");
        assert!(
            parsed_stream.next().await.is_none(),
            "the stream should end once the buffer is drained"
        );
    }

    #[tokio::test]
    async fn an_events_data_fields_are_joined() {
        let body = "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\r\ndata: \"parts\":[{\"text\":\"Hello\"}]}}]}\r\n\r\n";

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![body.as_bytes()])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("an event split over two data fields should parse as one payload");

        assert_eq!(text_of(&first), "Hello");
    }

    #[tokio::test]
    async fn a_data_field_without_a_space_after_the_colon_parses() {
        let event = text_event("Hello").replace("data: ", "data:");

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![event.as_bytes()])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("the space after `data:` is optional");

        assert_eq!(text_of(&first), "Hello");
    }

    #[tokio::test]
    async fn a_done_sentinel_ends_the_stream() {
        let body = format!("{}data: [DONE]\r\n\r\n", text_event("Hello"));

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![body.as_bytes()])));

        parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("should parse successfully");

        assert!(
            parsed_stream.next().await.is_none(),
            "`[DONE]` should end the stream"
        );
    }

    #[tokio::test]
    async fn an_undecodable_event_is_reported_once_and_the_next_event_still_arrives() {
        // A lone continuation byte is invalid wherever it lands, so this is a genuinely broken
        // event rather than a character waiting for its other half.
        let mut body = b"data: \x80\r\n\r\n".to_vec();
        body.extend_from_slice(text_event("Hello").as_bytes());

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![&body])));

        let error = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect_err("an undecodable event should be an error");
        assert!(
            error.to_string().contains("Invalid UTF-8 in SSE event"),
            "unexpected error: {error}"
        );

        let second = parsed_stream
            .next()
            .await
            .expect("the following event should still be yielded")
            .expect("should parse successfully");
        assert_eq!(text_of(&second), "Hello");
    }

    #[tokio::test]
    async fn a_truncated_tail_is_reported_once_and_then_the_stream_ends() {
        let body = "data: {\"candidates\":[{\"content\":{\"role\":\"model\"";

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![body.as_bytes()])));

        parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect_err("a truncated tail should be an error");

        assert!(
            parsed_stream.next().await.is_none(),
            "the stream must end after reporting the tail, not repeat the error forever"
        );
    }

    #[test]
    fn a_trailing_cr_waits_for_the_byte_that_classifies_it() {
        // `\r` last: it may still grow into the `\r\n` that completes a blank line.
        assert_eq!(find_event_boundary(b"data: a\r\n\r"), None);
        assert_eq!(find_event_boundary(b"data: a\r\n\r\n"), Some((7, 11)));
        // `\r\r` is a blank line in its own right once a following byte proves the second `\r`
        // is not the start of a `\r\n`.
        assert_eq!(find_event_boundary(b"data: a\r\rx"), Some((7, 9)));
    }
}
