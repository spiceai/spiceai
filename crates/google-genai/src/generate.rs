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

        let headers = self.auth_headers(HeaderMap::new());

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

        let headers = self.auth_headers(HeaderMap::new());

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

// The reader below is written here rather than taken from a crate. `eventsource-stream` and the
// `reqwest-eventsource` fork `crates/llms` pins both decode SSE correctly, but both end a stream
// silently when the body stops mid-event, and reporting that rather than passing off a partial
// answer as a whole one is the behavior this module exists to keep. Sharing one decoder across the
// workspace's SSE readers is worth doing - see #12597 - but it belongs in its own change.

/// The most bytes one unterminated event may occupy before the reader refuses to keep buffering.
///
/// A server that never terminates an event would otherwise grow the buffer until the process runs
/// out of memory, and this reader is reached from `crates/llms`, so that growth happens inside
/// `spiced` rather than only in a CLI.
///
/// A Gemini event is normally a few hundred bytes of JSON. The realistic upper end is a candidate
/// part carrying `inlineData`, which is base64 and so a third larger than the bytes it encodes,
/// putting this cap above roughly 6 MiB of inline payload - well past what a response returns
/// inline. Sizing it under that upper end would turn a valid long response into a failure, which is
/// the same class of defect as the growth it bounds.
///
/// What this bounds is one unterminated event - the quantity an endpoint can grow without limit.
/// The buffer as a whole may sit above it in passing, since it also holds whole events waiting to
/// be drained and its allocation carries spare capacity beyond the bytes in use, so peak memory for
/// one stream is somewhat above this figure rather than equal to it.
const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

/// What begins at `buf[i]`.
enum Terminator {
    /// A `\n`, or a `\r\n` whose `\n` is present: the line ends here and the next one starts a
    /// known number of bytes later.
    Complete(usize),
    /// A `\r` last in `buf`. The line ends here either way - a `\r` alone terminates it, and so
    /// does a `\r\n` - but whether the next line starts one byte or two later is not known until
    /// the byte after it arrives.
    TrailingCr,
    /// Not a line terminator.
    None,
}

fn terminator_at(buf: &[u8], i: usize) -> Terminator {
    match buf.get(i) {
        Some(b'\n') => Terminator::Complete(1),
        Some(b'\r') => match buf.get(i + 1) {
            Some(b'\n') => Terminator::Complete(2),
            Some(_) => Terminator::Complete(1),
            None => Terminator::TrailingCr,
        },
        _ => Terminator::None,
    }
}

/// Where the first event in a buffer ends.
#[derive(Debug, PartialEq, Eq)]
struct EventBoundary {
    /// Offset just past the event's last field line - the start of the blank line that ends it.
    end: usize,
    /// Offset the next event starts at.
    next_start: usize,
    /// The blank line ended with a `\r` last in the buffer, so a `\n` read next belongs to that
    /// terminator rather than starting a line. The event itself is complete regardless.
    may_skip_lf: bool,
}

/// The blank line that terminates the first event in `buf`, or `None` if the buffer does not hold a
/// complete event yet.
///
/// A line ends with `\n`, `\r\n` or `\r`, so the blank line is any two consecutive terminators.
/// The answer never depends on whether more bytes may arrive: an event that is complete is reported
/// immediately, so a live connection can never withhold one. Searching the bytes - rather than
/// decoding each network read as text and searching that - is what makes the decode below land on a
/// character boundary: a multi-byte UTF-8 sequence never contains `\n` or `\r`, so an event never
/// ends inside one.
///
/// Scanning starts at `from`, which callers use to skip a prefix an earlier scan already rejected.
/// Only `SseReader::next_boundary` is in a position to know such a prefix exists; everything else
/// passes 0 and reads the whole buffer.
fn find_event_boundary(buf: &[u8], from: usize) -> Option<EventBoundary> {
    let mut i = from;
    while i < buf.len() {
        // A trailing `\r` has nothing after it, so no blank line starts here yet.
        let Terminator::Complete(first) = terminator_at(buf, i) else {
            i += 1;
            continue;
        };

        let after = i + first;
        match terminator_at(buf, after) {
            Terminator::Complete(second) => {
                return Some(EventBoundary {
                    end: i,
                    next_start: after + second,
                    may_skip_lf: false,
                });
            }
            Terminator::TrailingCr => {
                return Some(EventBoundary {
                    end: i,
                    next_start: after + 1,
                    may_skip_lf: true,
                });
            }
            // Either the next line carries data, or the buffer stops before there is a next line
            // to look at.
            Terminator::None => {
                if after >= buf.len() {
                    return None;
                }
                i = after;
            }
        }
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

/// What `parse_sse_stream` carries between polls.
#[derive(Default)]
struct SseReader {
    /// Bytes read but not yet framed into an event.
    bytes: Vec<u8>,
    /// How many leading bytes of `bytes` are known to begin no event boundary, so a later scan can
    /// start there instead of at zero. Maintained by `next_boundary` and `consume`.
    scanned: usize,
    /// The last event's blank line ended with a `\r` that was then the final byte, so a `\n` read
    /// next completes that terminator instead of starting a line.
    skip_leading_lf: bool,
    /// The body has ended. Held so the inner stream is not polled again after it has finished.
    ended: bool,
}

impl SseReader {
    /// The event the buffer holds, if it holds a complete one.
    ///
    /// An unsuccessful scan records where it got to, so the bytes it rejected are not read again
    /// when the next read arrives. Without that, a peer dribbling one unterminated event in small
    /// pieces costs a full rescan per read - work quadratic in the bytes it sends, off a modest
    /// amount of traffic, inside `spiced`.
    fn next_boundary(&mut self) -> Option<EventBoundary> {
        let boundary = find_event_boundary(&self.bytes, self.scanned);

        if boundary.is_none() {
            // Two bytes back, because `terminator_at` reads the byte after the one it classifies:
            // the last two positions are the only ones a later read can reclassify - a `\r` last in
            // the buffer, and a terminator whose blank-line successor has not arrived yet.
            self.scanned = self.bytes.len().saturating_sub(2);
        }

        boundary
    }

    /// Drop the first `n` bytes, keeping `scanned` on the byte it was already on.
    fn consume(&mut self, n: usize) {
        self.bytes.drain(..n);
        self.scanned = self.scanned.saturating_sub(n);
    }

    /// Drop every buffered byte, releasing the capacity behind them now rather than holding it
    /// until the stream is dropped.
    fn discard(&mut self) {
        self.bytes = Vec::new();
        self.scanned = 0;
    }
}

fn parse_sse_stream(
    stream: Pin<Box<dyn Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Send>>,
) -> impl Stream<Item = Result<GenerateContentResponse>> + Send {
    futures::stream::unfold(
        (stream, SseReader::default()),
        |(mut stream, mut reader)| async move {
            loop {
                // A boundary that set this leaves the buffer empty, so the byte in question is
                // always the first one here - and once there is one, or there will never be one,
                // the terminator it belongs to is settled either way.
                if reader.skip_leading_lf && (!reader.bytes.is_empty() || reader.ended) {
                    if reader.bytes.first() == Some(&b'\n') {
                        reader.consume(1);
                    }
                    reader.skip_leading_lf = false;
                }

                // Take every event the buffer already holds before reading again: one read can
                // carry a comment or keep-alive ahead of a data event, and awaiting another read
                // would hold that event back until the server happened to send more - or, at the
                // end of the body, report the stream as truncated instead.
                while let Some(boundary) = reader.next_boundary() {
                    // Read the event's fields while its bytes are still in the buffer, so nothing
                    // has to be copied out of it first.
                    let data = std::str::from_utf8(&reader.bytes[..boundary.end]).map(event_data);

                    // Consume the event either way, so a stream carrying one undecodable event
                    // reports it once and then continues rather than repeating it forever.
                    reader.consume(boundary.next_start);
                    reader.skip_leading_lf = boundary.may_skip_lf;

                    let data = match data {
                        Ok(data) => data,
                        Err(e) => {
                            return Some((
                                StreamSnafu {
                                    message: format!("Invalid UTF-8 in SSE event: {e}"),
                                }
                                .fail(),
                                (stream, reader),
                            ));
                        }
                    };

                    let Some(data) = data else {
                        continue;
                    };

                    if data == "[DONE]" {
                        return None;
                    }

                    if data.trim().is_empty() {
                        continue;
                    }

                    let result = serde_json::from_str(&data).context(JsonSnafu);

                    return Some((result, (stream, reader)));
                }

                // Asked after draining, so this measures one event the server never terminated
                // rather than however many whole events a single read happened to carry.
                // Refusing ends the stream: those bytes cannot become an event, and reading on
                // would resume the growth this bounds.
                if reader.bytes.len() > MAX_EVENT_BYTES {
                    let buffered = reader.bytes.len();

                    reader.discard();
                    reader.ended = true;

                    return Some((
                        StreamSnafu {
                            message: format!(
                                "SSE event grew to {buffered} bytes without being terminated"
                            ),
                        }
                        .fail(),
                        (stream, reader),
                    ));
                }

                // Every event the body held has been delivered by now, so whatever is left is a
                // tail the server never terminated. Report it once and end there: consumers
                // forward each item rather than stopping at the first error, so keeping the tail
                // would yield this same error on every later poll and the stream would never end.
                if reader.ended {
                    if reader.bytes.is_empty() {
                        return None;
                    }

                    reader.discard();

                    return Some((
                        StreamSnafu {
                            message: "Unexpected end of stream while parsing SSE event".to_string(),
                        }
                        .fail(),
                        (stream, reader),
                    ));
                }

                match stream.next().await {
                    Some(Ok(bytes)) => reader.bytes.extend_from_slice(&bytes),
                    Some(Err(e)) => {
                        return Some((
                            StreamSnafu {
                                message: e.to_string(),
                            }
                            .fail(),
                            (stream, reader),
                        ));
                    }
                    None => reader.ended = true,
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

    type ByteStream =
        Pin<Box<dyn Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Send>>;

    fn reads_of(reads: Vec<&[u8]>) -> Vec<std::result::Result<bytes::Bytes, reqwest::Error>> {
        reads
            .into_iter()
            .map(|read| Ok(bytes::Bytes::copy_from_slice(read)))
            .collect()
    }

    /// A stream that hands out exactly the byte groups it is given, so a test can put a boundary
    /// wherever it likes - including inside a multi-byte character or a `\r\n` pair.
    fn dribble(reads: Vec<&[u8]>) -> ByteStream {
        Box::pin(futures::stream::iter(reads_of(reads)))
    }

    /// The same, but the stream never ends: after the given reads it stays pending, the way a
    /// server that has sent a keep-alive and nothing since does.
    fn dribble_then_pending(reads: Vec<&[u8]>) -> ByteStream {
        // Both `StreamExt` traits in scope carry `chain`, so name the one being used.
        Box::pin(futures::StreamExt::chain(
            futures::stream::iter(reads_of(reads)),
            futures::stream::pending(),
        ))
    }

    /// A stream that panics if it is polled after it has finished - which the `Stream` contract
    /// permits an implementation to do. It is what makes "a body that has ended is never polled
    /// again" a tested property rather than a hope about how `reqwest` happens to behave.
    struct PanicsAfterEnd {
        reads: std::vec::IntoIter<bytes::Bytes>,
        finished: bool,
    }

    impl Stream for PanicsAfterEnd {
        type Item = std::result::Result<bytes::Bytes, reqwest::Error>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            assert!(!self.finished, "the stream was polled after it ended");

            if let Some(read) = self.reads.next() {
                std::task::Poll::Ready(Some(Ok(read)))
            } else {
                self.finished = true;
                std::task::Poll::Ready(None)
            }
        }
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
    async fn a_keep_alive_does_not_hold_back_an_event_already_in_the_buffer() {
        let body = format!(": keep-alive\r\n\r\n{}", text_event("Hello"));

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble_then_pending(vec![
            body.as_bytes(),
        ])));

        let first = tokio::time::timeout(std::time::Duration::from_secs(5), parsed_stream.next())
            .await
            .expect("the buffered event must be delivered without awaiting another read")
            .expect("stream should yield one item")
            .expect("should parse successfully");

        assert_eq!(text_of(&first), "Hello");
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

    #[tokio::test]
    async fn an_unterminated_event_past_the_cap_ends_the_stream() {
        // A live connection that keeps sending without ever ending an event. Nothing here is
        // deliverable, so the only question is whether the reader stops buffering it.
        let mut body = Vec::with_capacity(MAX_EVENT_BYTES + 1);
        body.extend_from_slice(b"data: {\"candidates\":[");
        body.resize(MAX_EVENT_BYTES + 1, b'x');

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble_then_pending(vec![&body])));

        let error = tokio::time::timeout(std::time::Duration::from_secs(30), parsed_stream.next())
            .await
            .expect("the cap must fire on the bytes already held, not await another read")
            .expect("stream should yield one item")
            .expect_err("an event past the cap should be an error");

        assert!(
            error.to_string().contains("without being terminated"),
            "the error should say the event was never terminated, got: {error}"
        );

        // Timed too: the body never ends, so a reader that kept reading it would hang here rather
        // than fail, and take the whole run's budget with it.
        let end = tokio::time::timeout(std::time::Duration::from_secs(30), parsed_stream.next())
            .await
            .expect("the stream must not read on after refusing");

        assert!(
            end.is_none(),
            "the stream must end after refusing, not keep reading the body it refused"
        );
    }

    /// The cap bounds one unterminated event, not what a read happens to carry. A read holding
    /// whole events plus a modest remainder can exceed it in total while no single event does,
    /// and rejecting that would fail a stream in which every event was properly terminated.
    #[tokio::test]
    async fn whole_events_plus_a_small_remainder_are_not_measured_against_the_cap() {
        // Sized so the event clears the cap by a wide enough margin that the framing `text_event`
        // adds around the payload cannot silently push it over.
        let payload = MAX_EVENT_BYTES - (64 * 1024);
        let big_event = text_event(&"x".repeat(payload));
        assert!(
            big_event.len() < MAX_EVENT_BYTES,
            "the event itself is under the cap"
        );

        let mut body = big_event.into_bytes();
        body.extend_from_slice(b"data: {\"candidates\":[");
        body.resize(body.len() + 2 * 1024 * 1024, b'y');

        assert!(
            body.len() > MAX_EVENT_BYTES,
            "the read must exceed the cap in total for this to test anything"
        );

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![&body])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("a terminated event under the cap should parse");
        assert_eq!(text_of(&first).len(), payload);

        let error = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect_err("the unterminated remainder should be an error");
        assert!(
            error.to_string().contains("Unexpected end of stream"),
            "a remainder under the cap ends as a truncated tail, not a refusal, got: {error}"
        );
    }

    #[tokio::test]
    async fn a_cr_terminated_event_is_not_held_back_by_a_live_connection() {
        // SSE allows a bare `\r` line ending, so `\r\r` is a blank line and the event before it is
        // complete. Whether that second `\r` later turns out to be the start of a `\r\n` decides
        // only where the *next* event begins, so nothing may wait on it.
        let event = text_event("Hello").replace("\r\n", "\r");

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble_then_pending(vec![
            event.as_bytes(),
        ])));

        let first = tokio::time::timeout(std::time::Duration::from_secs(5), parsed_stream.next())
            .await
            .expect("a complete CR-framed event must not wait for another read")
            .expect("stream should yield one item")
            .expect("should parse successfully");

        assert_eq!(text_of(&first), "Hello");
    }

    #[tokio::test]
    async fn a_final_lf_completing_a_split_terminator_is_not_a_truncated_tail() {
        // The body ends `\r\n\r\n`, split so its last byte arrives alone. The event is delivered on
        // the first read - the `\r` ends the blank line - which leaves that `\n` to be recognized
        // as the rest of a terminator already acted on, not as an unterminated event.
        let event = text_event("Hello");
        let bytes = event.as_bytes();
        let split = bytes.len() - 1;

        let mut parsed_stream = Box::pin(parse_sse_stream(dribble(vec![
            &bytes[..split],
            &bytes[split..],
        ])));

        let first = parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect("should parse successfully");
        assert_eq!(text_of(&first), "Hello");

        assert!(
            parsed_stream.next().await.is_none(),
            "the trailing `\\n` completes a terminator and is not a truncated event"
        );
    }

    #[tokio::test]
    async fn a_body_that_has_ended_is_not_read_again() {
        let stream = PanicsAfterEnd {
            reads: vec![bytes::Bytes::from_static(b"data: {\"candidates\":[")].into_iter(),
            finished: false,
        };

        let mut parsed_stream = Box::pin(parse_sse_stream(Box::pin(stream)));

        parsed_stream
            .next()
            .await
            .expect("stream should yield one item")
            .expect_err("a truncated tail should be an error");

        assert!(
            parsed_stream.next().await.is_none(),
            "the stream should end"
        );
    }

    #[test]
    fn an_event_is_framed_without_regard_for_whether_more_bytes_may_arrive() {
        // A `\r\n` blank line: both terminators are complete, so the next event starts after them.
        assert_eq!(
            find_event_boundary(b"data: a\r\n\r\n", 0),
            Some(EventBoundary {
                end: 7,
                next_start: 11,
                may_skip_lf: false,
            })
        );
        // The blank line's second terminator is a `\r` last in the buffer. The event is complete;
        // only whether a `\n` follows it is unknown, so the next event provisionally starts right
        // after it and a `\n` read next is recognized as part of that terminator.
        assert_eq!(
            find_event_boundary(b"data: a\r\n\r", 0),
            Some(EventBoundary {
                end: 7,
                next_start: 10,
                may_skip_lf: true,
            })
        );
        assert_eq!(
            find_event_boundary(b"data: a\r\r", 0),
            Some(EventBoundary {
                end: 7,
                next_start: 9,
                may_skip_lf: true,
            })
        );
        // A following byte settles it: that second `\r` was bare.
        assert_eq!(
            find_event_boundary(b"data: a\r\rx", 0),
            Some(EventBoundary {
                end: 7,
                next_start: 9,
                may_skip_lf: false,
            })
        );
        // One terminator is not a blank line, and neither is none.
        assert_eq!(find_event_boundary(b"data: a\r", 0), None);
        assert_eq!(find_event_boundary(b"data: a\r\nb", 0), None);
        assert_eq!(find_event_boundary(b"data: a", 0), None);
    }

    /// Resuming is only sound if it never changes what a scan finds. Growing each body one byte at
    /// a time puts the resumption point at every offset in turn - including inside a `\r\n` pair
    /// and between the two terminators of a blank line, the positions a later read reclassifies.
    #[test]
    fn a_resumed_scan_frames_the_same_events_as_one_from_the_start() {
        let bodies: [&[u8]; 6] = [
            b"data: a\r\n\r\ndata: b\r\n\r\n",
            b"data: a\n\ndata: b\n\n",
            b"data: a\r\rdata: b\r\r",
            b"data: a\r\n\rdata: b\r\n\r\n",
            b": keep-alive\n\ndata: a\r\n\r\n",
            b"\r\n\r\n\r\n",
        ];

        for body in bodies {
            let mut reader = SseReader::default();

            for byte in body {
                reader.bytes.push(*byte);

                let resumed = reader.next_boundary();
                let from_the_start = find_event_boundary(&reader.bytes, 0);

                assert_eq!(
                    resumed,
                    from_the_start,
                    "resuming at {} disagreed with a full scan of {:?}",
                    reader.scanned,
                    String::from_utf8_lossy(&reader.bytes)
                );

                // Take the event off the front the way the stream loop does, so the next byte is
                // scanned against a buffer that has had one consumed.
                if let Some(boundary) = resumed {
                    reader.consume(boundary.next_start);
                }
            }
        }
    }

    /// The scan used to restart at zero on every read, so a peer dribbling one unterminated event
    /// paid a full rescan per read - work quadratic in the bytes it sent, off a modest amount of
    /// traffic. An unsuccessful scan now leaves behind where it stopped.
    #[test]
    fn an_unterminated_event_is_not_rescanned_from_the_start() {
        let mut reader = SseReader::default();

        for _ in 0..16 {
            reader.bytes.extend_from_slice(b"0123456789");

            assert_eq!(reader.next_boundary(), None);

            // Only the two positions a later read can reclassify are left to re-examine, so what
            // one more read costs does not grow with the bytes already buffered.
            assert_eq!(reader.scanned, reader.bytes.len().saturating_sub(2));
        }
    }

    async fn text_of_each(stream: ByteStream) -> Vec<String> {
        let mut parsed_stream = Box::pin(parse_sse_stream(stream));
        let mut texts = Vec::new();

        while let Some(response) = parsed_stream.next().await {
            texts.push(text_of(&response.expect("the event should parse")));
        }

        texts
    }

    /// The scan offset is state carried across reads, and the reader also drops bytes off the front
    /// - an event it framed, and the `\n` of a `\r\n` split across two reads. Delivering a body one
    /// byte at a time puts a read boundary at every one of those points at once.
    #[tokio::test]
    async fn a_body_delivered_one_byte_at_a_time_reads_the_same_as_one_delivered_whole() {
        // The comment ahead of the data events makes the reader frame an event, yield nothing for
        // it and scan on - the path a stale offset would survive into the next read on.
        let body = format!(
            ": keep-alive\r\n\r\n{}{}",
            text_event("first"),
            text_event("second")
        );

        let whole = text_of_each(dribble(vec![body.as_bytes()])).await;
        let one_byte_at_a_time = text_of_each(dribble(body.as_bytes().chunks(1).collect())).await;

        assert_eq!(whole, ["first", "second"]);
        assert_eq!(one_byte_at_a_time, whole);
    }
}
