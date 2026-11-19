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

//! The HTTP client that hosted model endpoints are reached with, and the bounds it holds an
//! endpoint to.
//!
//! An endpoint has [`Limits::connect_timeout`] to accept a connection and
//! [`Limits::read_timeout`] to start its answer and then to send each next part of it, so a
//! stream runs for as long as data keeps arriving. A response is read whole, up to
//! [`Limits::max_response_bytes`] once decoded, unless it is the successful answer to a request
//! that asked for a stream; then each of its server-sent events is held to that size instead.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_openai::Client;
use async_openai::config::Config;
use async_openai::error::OpenAIError;
use async_openai::middleware::HttpRequestFactory;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use http_body_limit::Limit;
use reqwest::{Response, ResponseBuilderExt};
use serde::Deserialize;
use snafu::prelude::*;

/// How long a model endpoint may take to accept a connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a model endpoint may take to start its answer, and then to send each next part of
/// it.
const READ_TIMEOUT: Duration = Duration::from_mins(10);

/// The most of a model response held in memory: a non-streamed body once decoded, or one
/// server-sent event of a stream.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The timeouts and the size bound a model endpoint is held to.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
    pub(crate) max_response_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connect_timeout: CONNECT_TIMEOUT,
            read_timeout: READ_TIMEOUT,
            max_response_bytes: MAX_RESPONSE_BYTES,
        }
    }
}

impl Limits {
    /// A `reqwest` client with these timeouts, under the egress policy of
    /// [`crate::provider::guarded_client_builder`].
    #[expect(
        clippy::expect_used,
        reason = "the egress policy and the timeouts add no failure: the build fails only where `reqwest::Client::new()` panics"
    )]
    pub(crate) fn http_client(&self) -> reqwest::Client {
        crate::provider::guarded_client_builder()
            .connect_timeout(self.connect_timeout)
            .read_timeout(self.read_timeout)
            .build()
            .expect("build an HTTP client for a model endpoint")
    }
}

/// An `async-openai` client that sends each request once, through a [`BoundedService`].
pub(crate) fn client<C: Config>(config: C, limits: Limits) -> Client<C> {
    let service = BoundedService::new(limits);
    Client::build(service.http_client().clone(), config).with_http_service(service)
}

/// Sends each request once, as `async-openai`'s `ReqwestService` does, and holds its response to
/// [`Limits::max_response_bytes`].
#[derive(Clone, Debug)]
pub(crate) struct BoundedService {
    client: reqwest::Client,
    max_response_bytes: usize,
}

impl BoundedService {
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            client: limits.http_client(),
            max_response_bytes: limits.max_response_bytes,
        }
    }

    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.client
    }
}

impl tower::Service<HttpRequestFactory> for BoundedService {
    type Response = Response;
    type Error = OpenAIError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, OpenAIError>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequestFactory) -> Self::Future {
        let client = self.client.clone();
        let max_bytes = self.max_response_bytes;
        Box::pin(async move {
            let request = request.build().await?;
            let streamed = asks_for_stream(&request);
            let response = client
                .execute(request)
                .await
                .map_err(OpenAIError::Reqwest)?;
            if streamed && response.status().is_success() {
                Ok(limit_events(response, max_bytes))
            } else {
                read_whole(response, max_bytes).await
            }
        })
    }
}

/// Whether `request` asks for a streamed answer, which a Chat Completions, Responses or Messages
/// request does with `"stream": true` in its JSON body. The server chooses the content type it
/// answers with, so only the request decides whether a response is read as a stream.
fn asks_for_stream(request: &reqwest::Request) -> bool {
    #[derive(Deserialize)]
    struct Body {
        stream: Option<bool>,
    }
    request
        .body()
        .and_then(reqwest::Body::as_bytes)
        .and_then(|body| serde_json::from_slice::<Body>(body).ok())
        .and_then(|body| body.stream)
        == Some(true)
}

/// `response` with its body read whole, up to `max_bytes` once decoded.
async fn read_whole(response: Response, max_bytes: usize) -> Result<Response, OpenAIError> {
    http_body_limit::buffer(response, Limit::CompressionRatioAndSize(max_bytes))
        .await
        .map_err(|error| match error {
            http_body_limit::Error::Read { source } => OpenAIError::Reqwest(source),
            error => OpenAIError::Boxed(Box::new(error)),
        })
}

/// `response` with its body passed on a line at a time, and each server-sent event in it held to
/// `max_event_bytes`.
///
/// `eventsource-stream` parses its whole buffer again for each chunk it receives, so a line that
/// arrives in many chunks would cost time quadratic in its length. Passed whole lines, it parses
/// each line once.
fn limit_events(response: Response, max_event_bytes: usize) -> Response {
    // The builder carries only the URL, which cannot fail to set.
    let (mut parts, ()) = http::Response::builder()
        .url(response.url().clone())
        .body(())
        .unwrap_or_default()
        .into_parts();
    parts.status = response.status();
    parts.version = response.version();
    parts.headers = response.headers().clone();
    let mut endpoint = response.url().clone();
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    let body = event_lines(
        Box::pin(response.bytes_stream()),
        EventLines::new(max_event_bytes),
        endpoint.to_string(),
    );
    Response::from(http::Response::from_parts(
        parts,
        reqwest::Body::wrap_stream(body),
    ))
}

/// `chunks` split by `lines`, ending with an error at the first event past its cap.
fn event_lines<S>(
    chunks: Pin<Box<S>>,
    lines: EventLines,
    endpoint: String,
) -> impl Stream<Item = Result<Bytes, BoxError>> + Send + 'static
where
    S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
{
    struct State<S> {
        chunks: Pin<Box<S>>,
        /// `None` once the stream has ended.
        lines: Option<EventLines>,
        /// An error to return after the lines returned with it.
        refused: Option<EventTooLarge>,
        endpoint: String,
    }

    impl<S> State<S> {
        fn refuse(mut self, error: EventTooLarge) -> (Result<Bytes, BoxError>, Self) {
            // The reader of the body reports only that it failed, so say why here.
            tracing::warn!(
                "Failed to read a streamed answer from the model endpoint '{}', so the answer was \
                 cut short. Cause: {error}",
                self.endpoint
            );
            self.lines = None;
            (Err(error.into()), self)
        }
    }

    let state = State {
        chunks,
        lines: Some(lines),
        refused: None,
        endpoint,
    };
    futures::stream::unfold(state, |mut state| async move {
        if let Some(error) = state.refused.take() {
            return Some(state.refuse(error));
        }
        loop {
            let lines = state.lines.as_mut()?;
            match state.chunks.next().await {
                Some(Ok(chunk)) => {
                    let (complete, refused) = lines.push(&chunk);
                    state.refused = refused;
                    if let Some(complete) = complete {
                        return Some((Ok(complete), state));
                    }
                    if let Some(error) = state.refused.take() {
                        return Some(state.refuse(error));
                    }
                }
                Some(Err(error)) => {
                    state.lines = None;
                    return Some((Err(error.into()), state));
                }
                None => {
                    let rest = state.lines.take()?.finish()?;
                    return Some((Ok(rest), state));
                }
            }
        }
    })
}

#[derive(Debug, Snafu)]
#[snafu(display("one server-sent event is larger than {limit} bytes"))]
struct EventTooLarge {
    limit: usize,
}

/// Splits a server-sent event stream after its last complete line, and counts the bytes of the
/// event being received. A line ends at LF, CR or CRLF, and an empty line ends an event, as
/// `eventsource-stream` reads them.
struct EventLines {
    max_event_bytes: usize,
    /// The start of a line whose end has not arrived.
    partial: BytesMut,
    /// The bytes of the event being received, each CRLF counted once.
    event_bytes: usize,
    /// Whether the last byte was a CR, which an LF next to it joins as one line ending.
    after_cr: bool,
    /// Whether nothing has arrived since the last line ending.
    line_empty: bool,
}

impl EventLines {
    fn new(max_event_bytes: usize) -> Self {
        Self {
            max_event_bytes,
            partial: BytesMut::new(),
            event_bytes: 0,
            after_cr: false,
            line_empty: true,
        }
    }

    /// Adds `chunk`, and returns the lines it completes, if it completes any, and the error for
    /// an event that grows past the cap. The lines returned with that error end before the
    /// byte that passed the cap.
    fn push(&mut self, chunk: &Bytes) -> (Option<Bytes>, Option<EventTooLarge>) {
        let mut complete = 0;
        let mut refused = None;
        for (i, &byte) in chunk.iter().enumerate() {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                complete = i + 1;
                continue;
            }
            self.event_bytes += 1;
            if self.event_bytes > self.max_event_bytes {
                refused = Some(
                    EventTooLargeSnafu {
                        limit: self.max_event_bytes,
                    }
                    .build(),
                );
                break;
            }
            if matches!(byte, b'\r' | b'\n') {
                if self.line_empty {
                    self.event_bytes = 0;
                }
                self.line_empty = true;
                self.after_cr = byte == b'\r';
                complete = i + 1;
            } else {
                self.line_empty = false;
            }
        }
        let lines = if complete == 0 {
            None
        } else if self.partial.is_empty() {
            Some(chunk.slice(..complete))
        } else {
            let mut lines = std::mem::take(&mut self.partial);
            lines.extend_from_slice(&chunk[..complete]);
            Some(lines.freeze())
        };
        if refused.is_none() {
            self.partial.extend_from_slice(&chunk[complete..]);
        }
        (lines, refused)
    }

    /// The start of a line that the stream ended inside.
    fn finish(self) -> Option<Bytes> {
        (!self.partial.is_empty()).then(|| self.partial.freeze())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use async_openai::config::OpenAIConfig;
    use async_openai::types::chat::{
        ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs,
    };
    use rstest::rstest;
    use serde_json::json;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn limits(max_response_bytes: usize) -> Limits {
        Limits {
            max_response_bytes,
            ..Limits::default()
        }
    }

    fn chat_client(api_base: String, limits: Limits) -> Client<OpenAIConfig> {
        client(
            OpenAIConfig::new()
                .with_api_base(api_base)
                .with_api_key("key"),
            limits,
        )
    }

    /// A chat request, which asks for a stream as Spice's callers do: `async-openai` is built
    /// with `byot`, so `create_stream` sends the request's own `stream`.
    fn chat_request(stream: bool) -> CreateChatCompletionRequest {
        CreateChatCompletionRequestArgs::default()
            .model("model")
            .stream(stream)
            .messages([ChatCompletionRequestUserMessageArgs::default()
                .content("hi")
                .build()
                .expect("a user message")
                .into()])
            .build()
            .expect("a chat request")
    }

    fn reply(text: &str) -> String {
        json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 1,
            "model": "model",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop"
            }]
        })
        .to_string()
    }

    fn chunk(text: &str) -> serde_json::Value {
        json!({
            "id": "chatcmpl-1",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "model",
            "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": null}]
        })
    }

    /// `data` as one server-sent event, a `data:` field for each of its lines.
    fn event(data: &str, ending: &str) -> String {
        let mut event = String::new();
        for line in data.lines() {
            event.push_str("data: ");
            event.push_str(line);
            event.push_str(ending);
        }
        event.push_str(ending);
        event
    }

    fn gzip(body: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(body).expect("compress the body");
        encoder.finish().expect("finish the gzip stream")
    }

    async fn serve(response: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(response)
            .mount(&server)
            .await;
        server
    }

    #[rstest]
    #[case::plain(false)]
    #[case::gzip(true)]
    #[tokio::test]
    async fn a_reply_is_read_up_to_the_cap_and_refused_past_it(#[case] compressed: bool) {
        let reply = reply("hello");
        let response = if compressed {
            ResponseTemplate::new(200)
                .insert_header("content-encoding", "gzip")
                .set_body_raw(gzip(reply.as_bytes()), "application/json")
        } else {
            ResponseTemplate::new(200).set_body_raw(reply.clone(), "application/json")
        };
        let server = serve(response).await;

        let answer = chat_client(server.uri(), limits(reply.len()))
            .chat()
            .create(chat_request(false))
            .await
            .expect("a reply of exactly the cap is read");
        assert_eq!(answer.choices[0].message.content.as_deref(), Some("hello"));

        let error = chat_client(server.uri(), limits(reply.len() - 1))
            .chat()
            .create(chat_request(false))
            .await
            .expect_err("a reply one byte past the cap is refused");
        assert_eq!(
            error.to_string(),
            format!(
                "the response body is larger than the {} bytes that is read from one HTTP response",
                reply.len() - 1
            )
        );
    }

    #[tokio::test]
    async fn a_non_streamed_request_answered_with_events_is_read_whole_under_the_cap() {
        let events: String = (0..20)
            .map(|_| event(&chunk("a").to_string(), "\n"))
            .collect();
        let cap = events.len() / 2;
        let server =
            serve(ResponseTemplate::new(200).set_body_raw(events, "text/event-stream")).await;

        let error = chat_client(server.uri(), limits(cap))
            .chat()
            .create(chat_request(false))
            .await
            .expect_err("a body past the cap is refused, whatever its content type");
        assert_eq!(
            error.to_string(),
            format!(
                "the response body is larger than the {cap} bytes that is read from one HTTP response"
            )
        );
    }

    #[tokio::test]
    async fn an_endpoint_that_never_answers_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port");
        let address = listener.local_addr().expect("read the bound address");
        let server = tokio::spawn(async move {
            let (_connection, _) = listener.accept().await.expect("accept the request");
            std::future::pending::<()>().await;
        });
        let limits = Limits {
            read_timeout: Duration::from_millis(200),
            ..Limits::default()
        };

        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            chat_client(format!("http://{address}"), limits)
                .chat()
                .create(chat_request(false)),
        )
        .await
        .expect("the read timeout ends the request");
        server.abort();
        assert!(
            matches!(&outcome, Err(OpenAIError::Reqwest(error)) if error.is_timeout()),
            "{outcome:?}"
        );
    }

    #[rstest]
    #[case::one_line(chunk(&"b".repeat(400)).to_string(), false)]
    #[case::many_lines(
        serde_json::to_string_pretty(&chunk(&"b".repeat(100))).expect("JSON"),
        true
    )]
    #[tokio::test]
    async fn a_streamed_event_past_the_cap_ends_the_stream(
        #[case] large: String,
        #[case] each_line_under_the_cap: bool,
    ) {
        let cap = 250;
        let small = event(&chunk("a").to_string(), "\n");
        let large = event(&large, "\n");
        assert!(small.len() <= cap && large.len() > cap);
        assert_eq!(
            large.lines().all(|line| line.len() < cap),
            each_line_under_the_cap
        );
        let server = serve(ResponseTemplate::new(200).set_body_raw(
            format!("{small}{large}data: [DONE]\n\n"),
            "text/event-stream",
        ))
        .await;

        let mut stream = chat_client(server.uri(), limits(cap))
            .chat()
            .create_stream(chat_request(true))
            .await
            .expect("the stream opens");
        let first = stream
            .next()
            .await
            .expect("an event")
            .expect("the event under the cap is read");
        assert_eq!(first.choices[0].delta.content.as_deref(), Some("a"));
        let error = stream
            .next()
            .await
            .expect("an item")
            .expect_err("the event past the cap is refused");
        // The stream reader keeps only the transport's own description of a body that failed.
        assert_eq!(
            error.to_string(),
            "stream failed: EventStream error: Transport error: error decoding response body"
        );
        assert!(
            stream.next().await.is_none(),
            "the stream ends at the refusal"
        );
    }

    #[tokio::test]
    async fn a_stream_of_events_under_the_cap_is_read_however_long() {
        let texts = ["a", "b", "c", "d", "e", "f"];
        let events: Vec<String> = texts
            .iter()
            .zip(["\n", "\r\n", "\r"].iter().cycle())
            .map(|(text, ending)| event(&chunk(text).to_string(), ending))
            .collect();
        let cap = events.iter().map(String::len).max().expect("events");
        let body = events.concat() + "data: [DONE]\n\n";
        assert!(body.len() > cap);
        let server =
            serve(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream")).await;

        let chunks: Vec<_> = chat_client(server.uri(), limits(cap))
            .chat()
            .create_stream(chat_request(true))
            .await
            .expect("the stream opens")
            .collect()
            .await;
        let read: Vec<String> = chunks
            .into_iter()
            .map(|item| {
                item.expect("an event under the cap").choices[0]
                    .delta
                    .content
                    .clone()
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(read, texts);
    }

    /// An event of exactly the cap passes and one byte more is refused, whichever line ending
    /// it uses and however it is split into chunks.
    #[test]
    fn an_event_is_held_to_the_cap() {
        for ending in ["\n", "\r\n", "\r"] {
            let one = event("x", ending);
            // Each of the event's two line endings counts once, a CRLF included.
            let counted = one.len() - 2 * (ending.len() - 1);
            let stream = one.repeat(2);
            for size in [1, 2, 3, stream.len()] {
                let read = |cap| {
                    let mut lines = EventLines::new(cap);
                    stream
                        .as_bytes()
                        .chunks(size)
                        .find_map(|chunk| lines.push(&Bytes::copy_from_slice(chunk)).1)
                        .map_or(Ok(()), Err)
                };
                read(counted).unwrap_or_else(|error| {
                    panic!("{ending:?} in chunks of {size}: events of the cap pass: {error}")
                });
                let error =
                    read(counted - 1).expect_err("an event one byte past the cap is refused");
                assert_eq!(error.limit, counted - 1);
            }
        }
    }

    #[test]
    fn only_whole_lines_are_passed_on() {
        let mut lines = EventLines::new(1024);
        let mut push = |chunk: &'static str| {
            let (complete, refused) = lines.push(&Bytes::from_static(chunk.as_bytes()));
            assert!(refused.is_none(), "under the cap");
            complete
        };
        assert_eq!(push("data: ab"), None);
        assert_eq!(push("c\r"), Some(Bytes::from_static(b"data: abc\r")));
        assert_eq!(push("\n\ndata"), Some(Bytes::from_static(b"\n\n")));
        assert_eq!(lines.finish(), Some(Bytes::from_static(b"data")));
    }
}
