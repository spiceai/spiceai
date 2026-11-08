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

//! Darkbloom model provider: models served by the Darkbloom inference network
//! (<https://darkbloom.ai>), which routes each request to an Apple Silicon machine that serves
//! the model.
//!
//! Darkbloom serves an `OpenAI`-compatible API — chat completions, the Responses API and a model
//! listing — so models are driven through the shared [`crate::openai::Openai`] client. What
//! differs is handled here: Darkbloom authenticates with `Authorization: Bearer`, names a missing
//! model with the error code `model_not_found`, and ends a stream that fails after its first
//! chunk with an error event rather than an HTTP status.

#![allow(clippy::missing_errors_doc)]

mod list_models;

pub use list_models::DarkbloomModelLister;

use async_openai::error::{ApiError, OpenAIError, WrappedError};
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_openai::types::responses::{CreateResponse, Response, ResponseStream};
use async_trait::async_trait;
use futures::TryStreamExt;
use runtime_rate_control::RateController;
use std::sync::Arc;

use crate::chat::nsql::SqlGeneration;
use crate::chat::{ApiErrorKind, Chat, Error, TokenLimits};
use crate::config::HostedModelConfig;
use crate::openai::{Openai, new_openai_client_with_config};
use crate::responses::Responses;

/// The base URL of Darkbloom's `OpenAI`-compatible API.
pub const DEFAULT_ENDPOINT: &str = "https://api.darkbloom.dev/v1";

/// The name of this provider in errors and in `from: darkbloom:<model>`.
const MODEL_SOURCE: &str = "darkbloom";

/// The base URL of the `OpenAI`-compatible API that `endpoint` names, or
/// [`DEFAULT_ENDPOINT`] when it names none. A trailing `/` is dropped, so the client's
/// `{base}/chat/completions` never holds `//`.
#[must_use]
pub fn api_base(endpoint: Option<&str>) -> String {
    endpoint
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .unwrap_or(DEFAULT_ENDPOINT)
        .trim_end_matches('/')
        .to_string()
}

/// A model served by Darkbloom, for chat completions and the Responses API.
pub struct Darkbloom {
    model: String,
    chat: Openai<HostedModelConfig>,
}

impl Darkbloom {
    /// A client for Darkbloom model `model` that authenticates with `api_key`, at `endpoint`, or
    /// at [`DEFAULT_ENDPOINT`] when it is `None`.
    #[must_use]
    pub fn new(model: impl Into<String>, api_key: &str, endpoint: Option<&str>) -> Self {
        let model = model.into();
        let config =
            HostedModelConfig::from_url(&api_base(endpoint)).with_bearer_token(Some(api_key));
        Self {
            chat: new_openai_client_with_config(model.clone(), config),
            model,
        }
    }

    /// Takes a permit from `rate_controller` for each request, in place of the client's
    /// default, so the model's `max_concurrency` and `requests_per_minute_limit` govern every
    /// request it sends.
    #[must_use]
    pub fn with_rate_controller(mut self, rate_controller: Arc<RateController>) -> Self {
        self.chat = self.chat.with_rate_controller(rate_controller);
        self
    }
}

/// Whether Darkbloom's refusal says the model does not exist. Darkbloom names that case with
/// the code `model_not_found`; the generic `not_found_error` kind is read too.
fn is_model_not_found(error: &ApiError) -> bool {
    error.code.as_deref() == Some("model_not_found")
        || error.r#type.as_deref() == Some("model_not_found")
        || ApiErrorKind::of(error) == Some(ApiErrorKind::NotFound)
}

/// Darkbloom's error from a stream event that carries one.
///
/// Darkbloom answers a streamed request with HTTP 200 once the model produces its first chunk,
/// so a failure after that point arrives as a final `data: {"error": {...}}` event, with no
/// `[DONE]`. `async-openai` reads every event as a chunk, and reports that one as a chunk that
/// failed to deserialize, which hides what Darkbloom said behind a parse error. This recovers
/// Darkbloom's error from it; any other error is returned unchanged.
fn darkbloom_stream_error(error: OpenAIError) -> OpenAIError {
    if let OpenAIError::JSONDeserialize(_, body) = &error
        && let Ok(wrapped) = serde_json::from_str::<WrappedError>(body)
    {
        return OpenAIError::ApiError(wrapped.error);
    }
    error
}

#[async_trait]
impl Chat for Darkbloom {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        Chat::as_sql(&self.chat)
    }

    async fn run(&self, prompt: String) -> crate::chat::Result<Option<String>> {
        Chat::run(&self.chat, prompt).await
    }

    async fn health(&self) -> crate::chat::Result<()> {
        match Chat::health(&self.chat).await {
            Err(Error::HealthCheckError { source })
                if source.downcast_ref::<OpenAIError>().is_some_and(
                    |e| matches!(e, OpenAIError::ApiError(api) if is_model_not_found(api)),
                ) =>
            {
                Err(Error::ModelNotFound {
                    model: self.model.clone(),
                    model_source: MODEL_SOURCE.to_string(),
                })
            }
            result => result,
        }
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> crate::chat::Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = crate::chat::Result<Option<String>>> + Send>>,
    > {
        Chat::stream(&self.chat, prompt).await
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let stream = self.chat.chat_stream(req).await?;
        Ok(Box::pin(stream.map_err(darkbloom_stream_error)))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        self.chat.chat_request(req).await
    }

    async fn token_limits(&self) -> TokenLimits {
        self.chat.token_limits().await
    }
}

/// Darkbloom's own Responses API, `POST /v1/responses`.
#[async_trait]
impl Responses for Darkbloom {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        Responses::as_sql(&self.chat)
    }

    async fn run(&self, prompt: String) -> crate::responses::Result<Option<String>> {
        Responses::run(&self.chat, prompt).await
    }

    async fn health(&self) -> crate::responses::Result<()> {
        Responses::health(&self.chat).await
    }

    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError> {
        let stream = Responses::responses_stream(&self.chat, req).await?;
        Ok(Box::pin(stream.map_err(darkbloom_stream_error)))
    }

    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
        Responses::responses_request(&self.chat, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent,
    };
    use async_openai::types::responses::ResponseStreamEvent;
    use futures::StreamExt;
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const KEY: &str = "eigeninference-test";

    fn request(model: &str, stream: bool) -> CreateChatCompletionRequest {
        CreateChatCompletionRequest {
            model: model.to_string(),
            messages: vec![ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessage {
                    name: None,
                    content: ChatCompletionRequestUserMessageContent::Text("ping".to_string()),
                },
            )],
            stream: Some(stream),
            ..Default::default()
        }
    }

    /// A chat completion as Darkbloom's API reference documents it: the `OpenAI` shape, plus
    /// the provider attestation fields Darkbloom adds to the body.
    fn completion() -> serde_json::Value {
        json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 1_778_000_000,
            "model": "gpt-oss-20b",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "pong", "reasoning_content": "The user said ping."},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 9,
                "completion_tokens": 12,
                "total_tokens": 21,
                "prompt_tokens_details": {"cached_tokens": 0},
                "completion_tokens_details": {"reasoning_tokens": 11}
            },
            "se_signature": "MEUCIQ",
            "response_hash": "9f86d081",
            "metadata": {"provider_chip": "M4 Max"}
        })
    }

    #[test]
    fn api_base_defaults_to_darkbloom() {
        assert_eq!(api_base(None), "https://api.darkbloom.dev/v1");
        assert_eq!(api_base(Some("")), "https://api.darkbloom.dev/v1");
        assert_eq!(api_base(Some("  ")), "https://api.darkbloom.dev/v1");
        assert_eq!(
            api_base(Some(" http://localhost:8080/v1/ ")),
            "http://localhost:8080/v1"
        );
    }

    /// The request reaches Darkbloom's chat completions with the key as a bearer token, and
    /// names the Darkbloom model rather than the model's name in the Spicepod.
    #[tokio::test]
    async fn a_chat_request_sends_the_key_as_a_bearer_token_and_the_darkbloom_model_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("authorization", format!("Bearer {KEY}").as_str()))
            .and(body_partial_json(json!({"model": "gpt-oss-20b"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(completion()))
            .expect(1)
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let model = Darkbloom::new("gpt-oss-20b", KEY, Some(&endpoint));
        let response = model
            .chat_request(request("my_spicepod_model", false))
            .await
            .expect("the mock answers with a completion");

        let message = &response.choices[0].message;
        assert_eq!(message.content.as_deref(), Some("pong"));
        assert_eq!(
            message.reasoning_content.as_deref(),
            Some("The user said ping.")
        );
        // The response names the model the client asked for, as for every other provider.
        assert_eq!(response.model, "my_spicepod_model");
        let usage = response.usage.expect("usage is reported");
        assert_eq!(
            (
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.total_tokens
            ),
            (9, 12, 21)
        );
    }

    /// A stream that fails after its first chunk ends with Darkbloom's error event: the
    /// consumer gets the chunk, then Darkbloom's error, not a deserialization error.
    #[tokio::test]
    async fn a_stream_error_event_is_reported_as_darkbloom_said_it() {
        let server = MockServer::start().await;
        let body = concat!(
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1778000000,\"model\":\"gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"po\"},\"finish_reason\":null}]}\n\n",
            "data: {\"error\":{\"message\":\"provider disconnected mid-stream\",\"type\":\"server_error\",\"code\":\"provider_error\"}}\n\n",
        );
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let model = Darkbloom::new("gpt-oss-20b", KEY, Some(&endpoint));
        let items: Vec<_> = model
            .chat_stream(request("m", true))
            .await
            .expect("the stream opens")
            .collect()
            .await;

        assert_eq!(items.len(), 2, "one chunk, then the error: {items:?}");
        let chunk = items[0].as_ref().expect("the first event is a chunk");
        assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("po"));
        let Err(OpenAIError::ApiError(error)) = &items[1] else {
            panic!(
                "the error event must surface as Darkbloom's error, got {:?}",
                items[1]
            );
        };
        assert_eq!(error.message, "provider disconnected mid-stream");
        assert_eq!(error.r#type.as_deref(), Some("server_error"));
        assert_eq!(error.code.as_deref(), Some("provider_error"));
    }

    /// Each request takes a permit from the controller the model was given: while the test
    /// holds that controller's only permit, a request waits and never reaches Darkbloom.
    #[tokio::test]
    async fn requests_take_a_permit_from_the_rate_controller_the_model_was_given() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(completion()))
            .mount(&server)
            .await;
        let controller = RateController::builder()
            .with_max_concurrent_requests(1)
            .build();
        let endpoint = format!("{}/v1", server.uri());
        let model = Darkbloom::new("gpt-oss-20b", KEY, Some(&endpoint))
            .with_rate_controller(Arc::clone(&controller));

        let held = controller
            .acquire()
            .await
            .expect("the controller's only permit");
        let waited = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            model.chat_request(request("m", false)),
        )
        .await;
        waited.expect_err("the request must wait for the permit the test holds");
        let received = server.received_requests().await.unwrap_or_default();
        assert!(
            received.is_empty(),
            "no request may reach Darkbloom: {received:?}"
        );

        drop(held);
        let response = model
            .chat_request(request("m", false))
            .await
            .expect("the permit is free again");
        assert_eq!(response.choices[0].message.content.as_deref(), Some("pong"));
    }

    /// A Responses API answer as the `OpenAI` Responses API reference shapes it.
    fn response(text: &str) -> serde_json::Value {
        json!({
            "id": "resp_1",
            "object": "response",
            "created_at": 1_778_000_000,
            "model": "gpt-oss-20b",
            "status": "completed",
            "output": [{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "annotations": [], "text": text}]
            }],
            "usage": {
                "input_tokens": 9,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 12,
                "output_tokens_details": {"reasoning_tokens": 11},
                "total_tokens": 21
            }
        })
    }

    fn create_response(model: &str) -> CreateResponse {
        async_openai::types::responses::CreateResponseArgs::default()
            .model(model)
            .input("ping")
            .build()
            .expect("a Responses API request")
    }

    /// A Responses API request reaches Darkbloom's own `/v1/responses`, with the key as a bearer
    /// token and the Darkbloom model id.
    #[tokio::test]
    async fn a_responses_request_reaches_darkbloom_responses_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/responses"))
            .and(header("authorization", format!("Bearer {KEY}").as_str()))
            .and(body_partial_json(
                json!({"model": "gpt-oss-20b", "input": "ping"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(response("pong")))
            .expect(1)
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let model = Darkbloom::new("gpt-oss-20b", KEY, Some(&endpoint));
        let answer = Responses::responses_request(&model, create_response("my_spicepod_model"))
            .await
            .expect("the mock answers with a response");
        assert_eq!(answer.output_text().as_deref(), Some("pong"));
        let usage = answer.usage.expect("usage is reported");
        assert_eq!(usage.output_tokens_details.reasoning_tokens, 11);
    }

    /// A Responses API stream that fails after it starts ends with Darkbloom's error, as for chat.
    #[tokio::test]
    async fn a_responses_stream_error_event_is_reported_as_darkbloom_said_it() {
        let server = MockServer::start().await;
        let created =
            json!({"type": "response.created", "sequence_number": 0, "response": response("")});
        let body = format!(
            "data: {created}\n\ndata: {{\"error\":{{\"message\":\"provider disconnected mid-stream\",\"type\":\"server_error\",\"code\":\"provider_error\"}}}}\n\n"
        );
        Mock::given(method("POST"))
            .and(path("/v1/responses"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let model = Darkbloom::new("gpt-oss-20b", KEY, Some(&endpoint));
        let items: Vec<_> = Responses::responses_stream(&model, create_response("m"))
            .await
            .expect("the stream opens")
            .collect()
            .await;

        assert_eq!(items.len(), 2, "one event, then the error: {items:?}");
        let first = items[0]
            .as_ref()
            .expect("the first event is response.created");
        assert!(
            matches!(first, ResponseStreamEvent::ResponseCreated(_)),
            "the first event is response.created: {first:?}"
        );
        let Err(OpenAIError::ApiError(error)) = &items[1] else {
            panic!(
                "the error event must surface as Darkbloom's error, got {:?}",
                items[1]
            );
        };
        assert_eq!(error.message, "provider disconnected mid-stream");
        assert_eq!(error.code.as_deref(), Some("provider_error"));
    }

    #[test]
    fn a_malformed_chunk_that_is_not_an_error_event_is_left_as_it_was() {
        let body = r#"{"id": 1}"#.to_string();
        let parse_error =
            serde_json::from_str::<CreateChatCompletionResponse>(&body).expect_err("not a chunk");
        let error = darkbloom_stream_error(OpenAIError::JSONDeserialize(parse_error, body));
        assert!(
            matches!(&error, OpenAIError::JSONDeserialize(_, b) if b == r#"{"id": 1}"#),
            "{error:?}"
        );
    }

    /// A model Darkbloom does not serve fails the health check as a model that is not found,
    /// which is what makes the runtime list the models Darkbloom does serve.
    #[tokio::test]
    async fn the_health_check_reports_a_model_darkbloom_does_not_serve_as_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "error": {"message": "model 'no-such-model' not available", "type": "invalid_request_error", "code": "model_not_found", "param": "model"}
            })))
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let error = Chat::health(&Darkbloom::new("no-such-model", KEY, Some(&endpoint)))
            .await
            .expect_err("the model does not exist");
        assert_eq!(
            error.to_string(),
            "The specified model identifier 'no-such-model' is not valid for the source 'darkbloom'. Verify the model exists, and try again."
        );
    }

    /// Any other refusal, such as a key Darkbloom does not accept, is reported as Darkbloom
    /// gave it.
    #[tokio::test]
    async fn the_health_check_reports_any_other_refusal_as_darkbloom_gave_it() {
        let server = MockServer::start().await;
        // The body Darkbloom returned for an invalid key on 2026-11-08.
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": {"code": "authentication_error", "message": "invalid API key", "type": "authentication_error"}
            })))
            .mount(&server)
            .await;

        let endpoint = format!("{}/v1", server.uri());
        let error = Chat::health(&Darkbloom::new("gpt-oss-20b", KEY, Some(&endpoint)))
            .await
            .expect_err("the key is refused");
        let Error::HealthCheckError { source } = &error else {
            panic!("expected a health check error, got {error:?}");
        };
        assert!(
            source.to_string().contains("invalid API key"),
            "Darkbloom's reason must reach the user: {error}"
        );
    }
}
