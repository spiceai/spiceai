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

#![expect(
    clippy::expect_used,
    reason = "a failed set-up in a test should name itself and stop"
)]

//! What a caller of the xAI model gets when xAI refuses a request (issue #15167).
//!
//! xAI's error body is `{"code": "<grpc status>", "error": "<message>"}`, not the `OpenAI`
//! envelope. These tests serve that body from a local server standing in for `api.x.ai`, and
//! check that chat completions and responses, with and without streaming, return an
//! [`ApiError`] with xAI's message and an error kind that keeps xAI's HTTP status.

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequest,
    CreateChatCompletionRequestArgs,
};
use async_openai::types::responses::{CreateResponse, CreateResponseArgs};
use chat_api::{ApiErrorKind, Chat};
use futures::StreamExt;
use llms::responses::Responses;
use llms::xai::Xai;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TEMPERATURE_MESSAGE: &str = "Temperature must be less than 2 but temperature = 9";

/// A local xAI that answers every request on `route` with `status` and `body`.
async fn xai_answering(route: &str, status: u16, body: &str) -> (MockServer, Xai) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(route))
        .respond_with(
            ResponseTemplate::new(status).set_body_raw(body.to_string(), "application/json"),
        )
        .mount(&server)
        .await;
    let xai = Xai::with_endpoint(Some("grok-3-mini"), "test-key", &server.uri());
    (server, xai)
}

fn xai_error(code: &str, message: &str) -> String {
    serde_json::json!({ "code": code, "error": message }).to_string()
}

fn chat_request(stream: bool) -> CreateChatCompletionRequest {
    CreateChatCompletionRequestArgs::default()
        .model("grok")
        .stream(stream)
        .messages([ChatCompletionRequestUserMessageArgs::default()
            .content("hi")
            .build()
            .expect("user message")
            .into()])
        .build()
        .expect("chat request")
}

fn responses_request() -> CreateResponse {
    CreateResponseArgs::default()
        .model("grok")
        .input("hi")
        .build()
        .expect("responses request")
}

fn api_error(error: OpenAIError) -> ApiError {
    match error {
        OpenAIError::ApiError(api) => api,
        other => panic!("expected an ApiError, got {other:?}"),
    }
}

fn assert_temperature_refusal(error: OpenAIError) {
    let api = api_error(error);
    assert_eq!(api.message, TEMPERATURE_MESSAGE);
    assert_eq!(api.r#type.as_deref(), Some("invalid_request_error"));
    assert_eq!(api.code.as_deref(), Some("invalid_argument"));
    assert_eq!(ApiErrorKind::of(&api), Some(ApiErrorKind::InvalidRequest));
}

#[tokio::test]
async fn chat_request_reads_xai_error() {
    let body = xai_error("invalid-argument", TEMPERATURE_MESSAGE);
    let (_server, xai) = xai_answering("/chat/completions", 400, &body).await;
    let error = xai
        .chat_request(chat_request(false))
        .await
        .expect_err("xAI refused the request");
    assert_temperature_refusal(error);
}

#[tokio::test]
async fn chat_stream_reads_xai_error() {
    let body = xai_error("invalid-argument", TEMPERATURE_MESSAGE);
    let (_server, xai) = xai_answering("/chat/completions", 400, &body).await;
    let Err(error) = xai.chat_stream(chat_request(true)).await else {
        panic!("xAI refused the request, but a stream was returned");
    };
    assert_temperature_refusal(error);
}

#[tokio::test]
async fn responses_request_reads_xai_error() {
    let body = xai_error("invalid-argument", TEMPERATURE_MESSAGE);
    let (_server, xai) = xai_answering("/responses", 400, &body).await;
    let error = xai
        .responses_request(responses_request())
        .await
        .expect_err("xAI refused the request");
    assert_temperature_refusal(error);
}

#[tokio::test]
async fn responses_stream_reads_xai_error() {
    let body = xai_error("invalid-argument", TEMPERATURE_MESSAGE);
    let (_server, xai) = xai_answering("/responses", 400, &body).await;
    let Err(error) = xai.responses_stream(responses_request()).await else {
        panic!("xAI refused the request, but a stream was returned");
    };
    assert_temperature_refusal(error);
}

/// The status and body xAI returned for each refusal in issue #15167, and the kind that the
/// HTTP layer maps back to the same status.
#[tokio::test]
async fn error_kind_keeps_xai_status() {
    for (status, code, message, kind) in [
        (
            400,
            "invalid-argument",
            "Incorrect API key provided. You can obtain an API key from https://console.x.ai.",
            Some(ApiErrorKind::InvalidRequest),
        ),
        (
            401,
            "unauthenticated",
            "Missing API key.",
            Some(ApiErrorKind::Authentication),
        ),
        (
            403,
            "permission-denied",
            "I'm sorry, I can't help with that request.",
            Some(ApiErrorKind::PermissionDenied),
        ),
        (
            404,
            "not-found",
            "Response with id=nope not found",
            Some(ApiErrorKind::NotFound),
        ),
        (
            429,
            "resource-exhausted",
            "Too many requests.",
            Some(ApiErrorKind::RateLimited),
        ),
        (500, "internal", "Internal error.", None),
        (503, "unavailable", "Service unavailable.", None),
    ] {
        let (_server, xai) =
            xai_answering("/chat/completions", status, &xai_error(code, message)).await;
        let api = api_error(
            xai.chat_request(chat_request(false))
                .await
                .expect_err("xAI refused the request"),
        );
        assert_eq!(api.message, message, "{status}");
        assert_eq!(ApiErrorKind::of(&api), kind, "{status}");
    }
}

/// A local xAI that answers the model lookup the health check makes with `status` and `body`.
async fn xai_looking_up_model(status: u16, body: &str) -> (MockServer, Xai) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/models/grok-3-mini"))
        .respond_with(
            ResponseTemplate::new(status).set_body_raw(body.to_string(), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let xai = Xai::with_endpoint(Some("grok-3-mini"), "test-key", &server.uri());
    (server, xai)
}

/// A model that xAI refuses to serve fails its health check with xAI's reason, not as an
/// unknown model.
#[tokio::test]
async fn health_names_xai_refusal() {
    const CREDITS: &str = "Your team has either used all available credits or reached its monthly spending limit. To continue making API requests, please purchase more credits or raise your spending limit.";
    let (_server, xai) = xai_looking_up_model(403, &xai_error("permission-denied", CREDITS)).await;
    let error = Chat::health(&xai).await.expect_err("xAI refused the model");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to check the status of the model. An error occurred: permission_error: {CREDITS} (code: permission_denied) Verify the model configuration."
        )
    );
}

#[tokio::test]
async fn health_reports_a_model_xai_does_not_have() {
    let (_server, xai) = xai_looking_up_model(
        404,
        &xai_error("not-found", "The model grok-3-mini does not exist."),
    )
    .await;
    let error = Chat::health(&xai).await.expect_err("xAI has no such model");
    assert_eq!(
        error.to_string(),
        "The specified model identifier 'grok-3-mini' is not valid for the source 'xai'. Verify the model exists, and try again."
    );
}

/// A body in the `OpenAI` shape is still read by `async-openai`, as before.
#[tokio::test]
async fn openai_error_body_is_unchanged() {
    let body = r#"{"error":{"message":"Bad temperature","type":"invalid_request_error","param":"temperature","code":"decimal_above_max_value"}}"#;
    let (_server, xai) = xai_answering("/chat/completions", 400, body).await;
    let api = api_error(
        xai.chat_request(chat_request(false))
            .await
            .expect_err("refused"),
    );
    assert_eq!(api.message, "Bad temperature");
    assert_eq!(api.param.as_deref(), Some("temperature"));
    assert_eq!(api.code.as_deref(), Some("decimal_above_max_value"));
}

#[tokio::test]
async fn successful_responses_are_unchanged() {
    let completion = r#"{"id":"c1","object":"chat.completion","created":0,"model":"grok-3-mini","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}"#;
    let (_server, xai) = xai_answering("/chat/completions", 200, completion).await;
    let response = xai
        .chat_request(chat_request(false))
        .await
        .expect("a completion");
    assert_eq!(
        response.choices[0].message.content.as_deref(),
        Some("hello")
    );

    let chunk = r#"{"id":"c1","object":"chat.completion.chunk","created":0,"model":"grok-3-mini","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}"#;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!("data: {chunk}\n\ndata: [DONE]\n\n"),
            "text/event-stream",
        ))
        .mount(&server)
        .await;
    let xai = Xai::with_endpoint(Some("grok-3-mini"), "test-key", &server.uri());
    let chunks: Vec<_> = xai
        .chat_stream(chat_request(true))
        .await
        .expect("a stream")
        .collect()
        .await;
    assert_eq!(chunks.len(), 1);
    let chunk = chunks
        .into_iter()
        .next()
        .expect("one chunk")
        .expect("a parsed chunk");
    assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("hello"));
}
