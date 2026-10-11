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

use crate::chat::Chat;
use crate::chat::nsql::structured_output::StructuredOutputSqlGeneration;
use crate::chat::nsql::{SqlGeneration, json::JsonSchemaSqlGeneration};
use async_openai::config::Config;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ChatCompletionResponseStream,
    CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_trait::async_trait;
use futures::TryStreamExt;
use tracing_futures::Instrument;

use super::{ChatBackend, Openai, responses_adapter};

#[async_trait]
impl<C: Config + Send + Sync + Clone> Chat for Openai<C> {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        // Only use structured output schema for OpenAI, not openai compatible.
        if self.supports_structured_output() {
            Some(&StructuredOutputSqlGeneration {})
        } else {
            Some(&JsonSchemaSqlGeneration {})
        }
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        if self.chat_backend == ChatBackend::Responses {
            return self.chat_completions_stream_using_responses_api(req).await;
        }

        let outer_model = req.model.clone();
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);

        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let stream = self.client.chat().create_stream(inner_req).await?;

        drop(permit); // drop the permit after acquiring the stream, instead of after receiving the response
        // semaphore permits aren't `Copy`, so we can't move it into the closure in `.map_ok`

        Ok(Box::pin(stream.map_ok(move |mut s| {
            s.model.clone_from(&outer_model);
            s
        })))
    }

    // Custom healthcheck for OpenAI because Azure dosn't support `max_completion_tokens`.
    #[expect(deprecated)]
    async fn health(&self) -> Result<(), crate::chat::Error> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");

        let mut req = CreateChatCompletionRequest {
            messages: vec![ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessage {
                    name: None,
                    content: ChatCompletionRequestUserMessageContent::Text(
                        "Respond with 'ok'".to_string(),
                    ),
                },
            )],
            ..Default::default()
        };

        if self.supports_reasoning_effort() {
            req.reasoning_effort = Some(async_openai::types::chat::ReasoningEffort::Low);
        }

        if self.supports_max_completion_tokens() {
            req.max_completion_tokens = Some(300);
        } else {
            req.max_tokens = Some(300);
        }

        let result = self.chat_request(req).instrument(span.clone()).await;
        tracing::debug!("{} model health check response: {:?}", self.model, result);
        if let Err(e) = result {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(crate::chat::Error::HealthCheckError {
                source: Box::new(e),
            });
        }
        Ok(())
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        if self.chat_backend == ChatBackend::Responses {
            return self.chat_completions_request_using_responses_api(req).await;
        }

        let outer_model = req.model.clone();
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);

        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let mut resp = self.client.chat().create(inner_req).await?;

        drop(permit);

        resp.model = outer_model;
        Ok(resp)
    }
}

impl<C: Config + Send + Sync + Clone> Openai<C> {
    async fn chat_completions_stream_using_responses_api(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let outer_model = req.model.clone();
        let inner_req =
            responses_adapter::responses_request_from_chat_completion_request(req, &self.model)?;

        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let stream = self.client.responses().create_stream(inner_req).await?;

        drop(permit);

        Ok(responses_adapter::chat_completion_stream_from_response_stream(stream, outer_model))
    }

    async fn chat_completions_request_using_responses_api(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let outer_model = req.model.clone();
        let inner_req =
            responses_adapter::responses_request_from_chat_completion_request(req, &self.model)?;

        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

        let response = self.client.responses().create(inner_req).await?;

        drop(permit);

        responses_adapter::chat_completion_response_from_response(response, outer_model)
    }
}

#[cfg(test)]
mod service_tier_tests {
    //! `OpenAI` reports the tier that served a request in `service_tier`, and the set of tiers
    //! grows on its side, so a reply naming a tier the client has never seen has to load and serve
    //! like any other. These drive the health check a model load runs, a request, and a stream
    //! through each backend against a local endpoint that answers on such a tier, and pin the
    //! request side to the tiers the published API lists.

    use std::fmt::Write as _;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    use async_openai::config::OpenAIConfig;
    use async_openai::types::chat::CreateChatCompletionRequest;
    use async_openai::types::responses::CreateResponse;
    use futures::TryStreamExt as _;
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::super::{ChatBackend, Openai, new_openai_client_with_chat_backend};
    use crate::chat::Chat as _;

    /// A tier the pinned types do not name.
    const UNNAMED_TIER: &str = "fast";

    const TIMEOUT: Duration = Duration::from_secs(20);

    struct ReceivedRequest {
        request_line: String,
        body: Value,
    }

    /// Answer one request with `reply`, sent as `content_type`, then close. Returns the base URL
    /// and a channel carrying the request that arrived.
    fn serve_one(
        content_type: &'static str,
        reply: String,
    ) -> (String, mpsc::Receiver<ReceivedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let port = listener
            .local_addr()
            .expect("read the bound address")
            .port();
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
                if head.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let head = String::from_utf8_lossy(&head).into_owned();
            let content_length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let mut body = vec![0_u8; content_length];
            if stream.read_exact(&mut body).is_err() {
                return;
            }
            let _ = tx.send(ReceivedRequest {
                request_line: head.lines().next().unwrap_or_default().to_string(),
                body: serde_json::from_slice(&body).unwrap_or(Value::Null),
            });

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                reply.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        });

        (format!("http://127.0.0.1:{port}/v1"), rx)
    }

    fn client(backend: ChatBackend, api_base: &str) -> Openai<OpenAIConfig> {
        new_openai_client_with_chat_backend(
            "gpt-4o-mini".to_string(),
            Some(api_base),
            Some("mock-key"),
            None,
            None,
            None,
            backend,
        )
    }

    fn received(rx: &mpsc::Receiver<ReceivedRequest>) -> ReceivedRequest {
        rx.recv_timeout(TIMEOUT)
            .expect("the endpoint received a request")
    }

    /// The path each backend posts to, so a test can show which one it drove.
    fn path(backend: ChatBackend) -> &'static str {
        match backend {
            ChatBackend::ChatCompletions => "POST /v1/chat/completions ",
            ChatBackend::Responses => "POST /v1/responses ",
        }
    }

    /// A completed reply of `text`, served on `tier`, in the backend's wire format.
    fn reply(backend: ChatBackend, tier: &str, text: &str) -> String {
        let reply = match backend {
            ChatBackend::ChatCompletions => json!({
                "id": "chatcmpl-1",
                "object": "chat.completion",
                "created": 1_755_639_134,
                "model": "gpt-4o-mini",
                "service_tier": tier,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                    "logprobs": null
                }],
                "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4}
            }),
            ChatBackend::Responses => response_json(tier, "completed", text),
        };
        reply.to_string()
    }

    fn response_json(tier: &str, status: &str, text: &str) -> Value {
        let output = if text.is_empty() {
            json!([])
        } else {
            json!([{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "annotations": [], "text": text}]
            }])
        };
        json!({
            "id": "resp_1",
            "object": "response",
            "created_at": 1_755_639_134,
            "model": "gpt-4o-mini",
            "service_tier": tier,
            "status": status,
            "output": output,
            "usage": {
                "input_tokens": 3,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 1,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 4
            }
        })
    }

    /// A stream that delivers `text` on `tier`, in the backend's server-sent-events format.
    fn stream_reply(backend: ChatBackend, tier: &str, text: &str) -> String {
        let events = match backend {
            ChatBackend::ChatCompletions => {
                let chunk = |delta: Value, finish_reason: Value| {
                    json!({
                        "id": "chatcmpl-1",
                        "object": "chat.completion.chunk",
                        "created": 1_755_639_134,
                        "model": "gpt-4o-mini",
                        "service_tier": tier,
                        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]
                    })
                };
                vec![
                    chunk(json!({"role": "assistant", "content": text}), Value::Null),
                    chunk(json!({}), json!("stop")),
                ]
            }
            ChatBackend::Responses => vec![
                json!({
                    "type": "response.created",
                    "sequence_number": 0,
                    "response": response_json(tier, "in_progress", "")
                }),
                json!({
                    "type": "response.output_text.delta",
                    "sequence_number": 1,
                    "item_id": "msg_1",
                    "output_index": 0,
                    "content_index": 0,
                    "delta": text
                }),
                json!({
                    "type": "response.completed",
                    "sequence_number": 2,
                    "response": response_json(tier, "completed", text)
                }),
            ],
        };
        let mut body = String::new();
        for event in events {
            write!(body, "data: {event}\n\n").expect("write to a String");
        }
        if backend == ChatBackend::ChatCompletions {
            body.push_str("data: [DONE]\n\n");
        }
        body
    }

    fn request_json(service_tier: Option<&str>) -> Value {
        let mut request = json!({
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "hello"}]
        });
        if let Some(tier) = service_tier {
            request["service_tier"] = json!(tier);
        }
        request
    }

    fn request(service_tier: Option<&str>) -> CreateChatCompletionRequest {
        serde_json::from_value(request_json(service_tier)).expect("the request deserializes")
    }

    /// The failure reported in #14916: the model never loads because its health check, which
    /// names no tier, is answered on one the client does not know.
    #[rstest]
    #[case::chat_completions(ChatBackend::ChatCompletions)]
    #[case::responses(ChatBackend::Responses)]
    #[tokio::test]
    async fn a_reply_on_an_unnamed_tier_passes_the_health_check(#[case] backend: ChatBackend) {
        let (api_base, rx) = serve_one("application/json", reply(backend, UNNAMED_TIER, "ok"));

        let health = tokio::time::timeout(TIMEOUT, client(backend, &api_base).health())
            .await
            .expect("the health check finished");

        let sent = received(&rx);
        assert!(
            sent.request_line.starts_with(path(backend)),
            "{backend:?} sent {}",
            sent.request_line
        );
        assert_eq!(
            sent.body.get("service_tier"),
            None,
            "the health check names no tier, so the one in the reply is the server's choice"
        );
        health.unwrap_or_else(|e| {
            panic!(
                "a reply on tier '{UNNAMED_TIER}' failed the {backend:?} health check, so the \
                 model would not load: {e}"
            )
        });
    }

    #[rstest]
    #[case::chat_completions(ChatBackend::ChatCompletions)]
    #[case::responses(ChatBackend::Responses)]
    #[tokio::test]
    async fn a_reply_on_an_unnamed_tier_is_reported_back(#[case] backend: ChatBackend) {
        let (api_base, rx) = serve_one("application/json", reply(backend, UNNAMED_TIER, "ok"));

        let response = tokio::time::timeout(
            TIMEOUT,
            client(backend, &api_base).chat_request(request(Some("priority"))),
        )
        .await
        .expect("the request finished")
        .unwrap_or_else(|e| {
            panic!("a reply on tier '{UNNAMED_TIER}' failed through {backend:?}: {e}")
        });

        let sent = received(&rx);
        assert!(
            sent.request_line.starts_with(path(backend)),
            "{backend:?} sent {}",
            sent.request_line
        );
        assert_eq!(
            sent.body["service_tier"],
            json!("priority"),
            "the client's tier reaches the server"
        );

        assert_eq!(
            serde_json::to_value(&response.service_tier).expect("serialize the tier"),
            json!(UNNAMED_TIER),
            "the tier the server reports reaches the client"
        );
        assert_eq!(response.choices[0].message.content.as_deref(), Some("ok"));
    }

    /// Only replies are open. A request still names one of the tiers the published API lists,
    /// because `/v1/chat/completions` and `/v1/responses` deserialize into these request types and
    /// their accepted values are user-facing. This fails if a fork re-cut opens the request side.
    #[test]
    fn a_request_naming_an_unnamed_tier_is_refused() {
        let error =
            serde_json::from_value::<CreateChatCompletionRequest>(request_json(Some(UNNAMED_TIER)))
                .expect_err("a chat request naming an unnamed tier has to be refused")
                .to_string();
        assert!(
            error.contains("unknown variant `fast`"),
            "refused for another reason: {error}"
        );

        let responses_request =
            |tier: &str| json!({"model": "gpt-4o-mini", "input": "hello", "service_tier": tier});
        let error = serde_json::from_value::<CreateResponse>(responses_request(UNNAMED_TIER))
            .expect_err("a responses request naming an unnamed tier has to be refused")
            .to_string();
        assert!(
            error.contains("unknown variant `fast`"),
            "refused for another reason: {error}"
        );

        // The control: the same requests on a named tier are accepted, so the refusals above are
        // about the tier rather than the shape of the request.
        assert_eq!(
            request(Some("priority")).service_tier,
            Some(async_openai::types::chat::ServiceTier::Priority)
        );
        assert_eq!(
            serde_json::from_value::<CreateResponse>(responses_request("priority"))
                .expect("a responses request on a named tier deserializes")
                .service_tier,
            Some(async_openai::types::responses::ServiceTier::Priority)
        );
    }

    #[rstest]
    #[case::chat_completions(ChatBackend::ChatCompletions)]
    #[case::responses(ChatBackend::Responses)]
    #[tokio::test]
    async fn a_stream_on_an_unnamed_tier_delivers_its_content(#[case] backend: ChatBackend) {
        let (api_base, rx) = serve_one(
            "text/event-stream",
            stream_reply(backend, UNNAMED_TIER, "ok"),
        );

        let chunks: Vec<_> = tokio::time::timeout(TIMEOUT, async {
            client(backend, &api_base)
                .chat_stream(request(None))
                .await
                .expect("the stream opened")
                .try_collect()
                .await
        })
        .await
        .expect("the stream finished")
        .unwrap_or_else(|e| {
            panic!("a stream on tier '{UNNAMED_TIER}' failed through {backend:?}: {e}")
        });

        let sent = received(&rx);
        assert!(
            sent.request_line.starts_with(path(backend)),
            "{backend:?} sent {}",
            sent.request_line
        );
        let content: String = chunks
            .iter()
            .flat_map(|chunk| &chunk.choices)
            .filter_map(|choice| choice.delta.content.as_deref())
            .collect();
        assert_eq!(content, "ok");
        if backend == ChatBackend::ChatCompletions {
            assert_eq!(
                serde_json::to_value(&chunks[0].service_tier).expect("serialize the tier"),
                json!(UNNAMED_TIER)
            );
        }
    }
}
