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

#![expect(
    clippy::expect_used,
    reason = "a failed set-up in a test should name itself and stop"
)]

//! What a caller of `chat_stream` is told when Anthropic refuses the request.
//!
//! These drive the whole path a real failure takes — HTTP status, error body, the SSE client's
//! own error mapping, and the adapter's classification — against a local server standing in for
//! Anthropic, so no credentials are involved and the classification is exercised on the error
//! shape the client actually builds rather than one written by hand.

use std::io::{Read, Write};
use std::net::TcpListener;

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, ChatCompletionResponseStream,
    CreateChatCompletionRequestArgs,
};
use chat_api::Chat;
use futures::StreamExt;
use llms::anthropic::Anthropic;
use llms::config::GenericAuthMechanism;

/// Serves one request with `status` and `body`, then closes. Returns the base URL to configure the
/// adapter with.
fn serve_one_error(status: &'static str, body: &'static str) -> String {
    serve_one(status, "application/json", body)
}

fn serve_one(status: &'static str, content_type: &'static str, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let port = listener
        .local_addr()
        .expect("read the bound address")
        .port();

    std::thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };

        // Read just enough to know the request headers ended; the body is not inspected.
        let mut seen = Vec::new();
        let mut byte = [0u8; 1];
        while stream.read(&mut byte).unwrap_or(0) == 1 {
            seen.push(byte[0]);
            if seen.ends_with(b"\r\n\r\n") {
                break;
            }
        }

        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    });

    format!("http://127.0.0.1:{port}")
}

/// Opens one streaming chat against `base`.
async fn chat_stream_against(base: &str) -> ChatCompletionResponseStream {
    let model = Anthropic::new(
        GenericAuthMechanism::from_api_key("not-a-real-key"),
        Some("claude-sonnet-4-6"),
        Some(base),
        None,
    )
    .expect("the adapter is configured");

    let request = CreateChatCompletionRequestArgs::default()
        .model("claude-sonnet-4-6")
        .messages(vec![
            ChatCompletionRequestUserMessageArgs::default()
                .content("hello")
                .build()
                .expect("build the user message")
                .into(),
        ])
        .build()
        .expect("build the request");

    model
        .chat_stream(request)
        .await
        .expect("a refusal is delivered as a stream item, not as a failure to open the stream")
}

/// Runs one streaming chat against a server that answers `status`/`body`, and returns the error the
/// caller is given.
async fn stream_error(status: &'static str, body: &'static str) -> OpenAIError {
    let base = serve_one_error(status, body);
    let mut stream = chat_stream_against(&base).await;

    match stream.next().await {
        Some(Err(e)) => e,
        Some(Ok(item)) => panic!("expected an error, got a completion chunk: {item:?}"),
        None => panic!("the stream ended without delivering the error"),
    }
}

fn api_error_message(error: &OpenAIError) -> String {
    match error {
        OpenAIError::ApiError(api) => api.message.clone(),
        other => panic!("expected an ApiError, got {other:?}"),
    }
}

fn api_error_type(error: &OpenAIError) -> Option<String> {
    match error {
        OpenAIError::ApiError(api) => api.r#type.clone(),
        other => panic!("expected an ApiError, got {other:?}"),
    }
}

/// Anthropic echoes request detail back in an `invalid_request_error`, so its message carries
/// whatever the caller sent. A tool name containing `429` must not turn a malformed request into a
/// rate-limit report — and the caller has to be told what was actually wrong with the request.
#[tokio::test]
async fn an_invalid_request_naming_429_is_not_reported_as_a_rate_limit() {
    let error = stream_error(
        "400 Bad Request",
        r#"{"type":"error","error":{"type":"invalid_request_error","message":"tools.0.name: `lookup_429` does not match ^[a-zA-Z0-9_-]{1,64}$"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        !message.contains("rate limit"),
        "a malformed request is not a rate limit: {message}"
    );
    assert!(
        message.contains("lookup_429"),
        "the cause must survive so the caller can see which request was refused: {message}"
    );
    assert_eq!(
        api_error_type(&error).as_deref(),
        Some("AnthropicStreamError")
    );
}

/// The same shape on the authentication arm: `403` in the echoed request detail must not send the
/// caller to rotate a key that is working.
#[tokio::test]
async fn an_invalid_request_naming_403_is_not_reported_as_an_auth_failure() {
    let error = stream_error(
        "400 Bad Request",
        r#"{"type":"error","error":{"type":"invalid_request_error","message":"messages.0.content.0.text: expected a string, got 403"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        !message.contains("authentication failed"),
        "a malformed request is not a credential problem: {message}"
    );
    assert!(
        message.contains("expected a string"),
        "the cause must survive: {message}"
    );
}

/// The other direction: a real rate limit whose message says neither `429` nor `too many requests`
/// still has to be reported as one, with its cause attached.
#[tokio::test]
async fn a_rate_limit_error_is_reported_as_a_rate_limit_whatever_its_message_says() {
    let error = stream_error(
        "429 Too Many Requests",
        r#"{"type":"error","error":{"type":"rate_limit_error","message":"Number of input tokens has exceeded your per-minute limit"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        message.contains("rate limit exceeded"),
        "a rate_limit_error must be reported as one: {message}"
    );
    assert!(
        message.contains("per-minute limit"),
        "the cause must survive: {message}"
    );
    assert!(
        message.contains("https://console.anthropic.com/settings/limits"),
        "the remediation link must survive: {message}"
    );
    assert_eq!(
        api_error_type(&error).as_deref(),
        Some("AnthropicRateLimitError")
    );
}

/// A real credential failure, likewise: reported as one, with Anthropic's own words kept as the
/// cause.
#[tokio::test]
async fn an_authentication_error_is_reported_as_one_and_keeps_its_cause() {
    let error = stream_error(
        "401 Unauthorized",
        r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        message.contains("authentication failed"),
        "an authentication_error must be reported as one: {message}"
    );
    assert!(
        message.contains("invalid x-api-key"),
        "the cause must survive: {message}"
    );
    assert_eq!(
        api_error_type(&error).as_deref(),
        Some("AnthropicAuthenticationError")
    );
}

/// Anthropic's 403 is `permission_error`, and it is answered the same way as a bad key.
#[tokio::test]
async fn a_permission_error_is_reported_as_an_authentication_failure() {
    let error = stream_error(
        "403 Forbidden",
        r#"{"type":"error","error":{"type":"permission_error","message":"Your API key does not have permission to use the specified resource"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        message.contains("authentication failed"),
        "a permission_error is answered by checking the key and its workspace: {message}"
    );
    assert!(
        message.contains("does not have permission"),
        "the cause must survive: {message}"
    );
}

/// A proxy in front of Anthropic may answer without a `type`. That is the one case the message
/// tests still serve, so it has to keep working.
#[tokio::test]
async fn an_untyped_error_is_still_classified_from_its_message() {
    let error = stream_error(
        "429 Too Many Requests",
        r#"{"type":"error","error":{"message":"429 Too Many Requests from the gateway"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        message.contains("rate limit exceeded"),
        "an untyped error is all the message tests have to go on: {message}"
    );
}

/// A model the endpoint does not serve keeps the explanation `explain_model_not_found` built for
/// it, rather than being re-typed by the classifier.
#[tokio::test]
async fn a_model_not_found_keeps_its_explanation() {
    let error = stream_error(
        "404 Not Found",
        r#"{"type":"error","error":{"type":"not_found_error","message":"model: claude-sonnet-4-6"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        message.contains("claude-sonnet-4-6"),
        "the model must be named: {message}"
    );
    assert_eq!(api_error_type(&error).as_deref(), Some("not_found_error"));
}

/// Anthropic's overload answer is a 529, and the SSE client hands a server-error body over
/// unparsed — so it arrives with no `type` and its whole JSON body as the message. That is the
/// production shape the message tests still have to serve, and the caller must at least be told
/// what came back.
#[tokio::test]
async fn an_overload_arrives_untyped_and_keeps_its_body_as_the_cause() {
    let error = stream_error(
        "529 ",
        r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
    )
    .await;

    let message = api_error_message(&error);
    assert!(
        message.contains("overloaded_error"),
        "the body must survive as the cause: {message}"
    );
}

/// Anthropic sheds load partway through a generation by sending an `error` event over an HTTP 200
/// stream, which is the only shape a mid-stream failure has. The caller must be told what
/// Anthropic reported — not that the adapter could not parse a packet.
#[tokio::test]
async fn a_mid_stream_error_event_is_reported_as_the_failure_anthropic_sent() {
    let base = serve_one(
        "200 OK",
        "text/event-stream",
        concat!(
            "event: message_start\n",
            r#"data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-6","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1},"content":[],"stop_reason":null}}"#,
            "\n\nevent: error\n",
            r#"data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
            "\n\n",
        ),
    );

    let mut stream = chat_stream_against(&base).await;

    let first = stream.next().await.expect("the message_start chunk");
    assert!(first.is_ok(), "the stream starts normally: {first:?}");

    let error = stream
        .next()
        .await
        .expect("the error event reaches the caller")
        .expect_err("an error event is an error");

    let message = api_error_message(&error);
    assert!(
        message.contains("Overloaded"),
        "the caller must be told what Anthropic reported: {message}"
    );
    assert!(
        !message.contains("unknown variant"),
        "a failure Anthropic reported is not a parse failure: {message}"
    );
}

/// A mid-stream rate limit is classified from its type like any other, so the caller gets the
/// remediation rather than a bare packet.
#[tokio::test]
async fn a_mid_stream_rate_limit_is_classified_and_keeps_its_cause() {
    let base = serve_one(
        "200 OK",
        "text/event-stream",
        concat!(
            "event: error\n",
            r#"data: {"type":"error","error":{"type":"rate_limit_error","message":"Number of output tokens has exceeded your per-minute limit"}}"#,
            "\n\n",
        ),
    );

    let error = chat_stream_against(&base)
        .await
        .next()
        .await
        .expect("the error event reaches the caller")
        .expect_err("an error event is an error");

    let message = api_error_message(&error);
    assert!(
        message.contains("rate limit exceeded"),
        "a mid-stream rate_limit_error must be reported as one: {message}"
    );
    assert!(
        message.contains("per-minute limit"),
        "the cause must survive: {message}"
    );
    assert_eq!(
        api_error_type(&error).as_deref(),
        Some("AnthropicRateLimitError")
    );
}
