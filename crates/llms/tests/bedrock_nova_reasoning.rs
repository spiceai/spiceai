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

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequest, ReasoningEffort,
    StopConfiguration,
};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use chat_api::{ApiErrorKind, Chat};
use llms::bedrock::{BedrockClient, chat::BedrockConverse};
use serde_json::{Value, json};
use std::sync::Arc;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn request() -> CreateChatCompletionRequest {
    request_with_effort(ReasoningEffort::High)
}

fn request_with_effort(reasoning_effort: ReasoningEffort) -> CreateChatCompletionRequest {
    CreateChatCompletionRequest {
        model: "nova".to_string(),
        messages: vec![
            ChatCompletionRequestUserMessageArgs::default()
                .content("What is 2+2?")
                .build()
                .expect("user message should build")
                .into(),
        ],
        reasoning_effort: Some(reasoning_effort),
        stop: Some(StopConfiguration::String("done".to_string())),
        ..Default::default()
    }
}

#[expect(deprecated, reason = "the legacy token-limit field remains supported")]
fn conflicting_requests(reasoning_effort: ReasoningEffort) -> [CreateChatCompletionRequest; 4] {
    let mut max_completion_tokens = request_with_effort(reasoning_effort.clone());
    max_completion_tokens.max_completion_tokens = Some(1024);

    let mut max_tokens = request_with_effort(reasoning_effort.clone());
    max_tokens.max_tokens = Some(1024);

    let mut temperature = request_with_effort(reasoning_effort.clone());
    temperature.temperature = Some(0.5);

    let mut top_p = request_with_effort(reasoning_effort);
    top_p.top_p = Some(0.9);

    [max_completion_tokens, max_tokens, temperature, top_p]
}

fn bedrock_converse(server: &MockServer) -> BedrockConverse {
    let config = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::from_static("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "test-access-key",
            "test-secret-key",
            None,
            None,
            "bedrock-nova-reasoning-test",
        )))
        .endpoint_url(server.uri())
        .build();

    BedrockConverse::new(
        Arc::new(BedrockClient::from(&config)),
        "us.amazon.nova-2-lite-v1:0".to_string(),
    )
}

async fn mount_bedrock_response(server: &MockServer) {
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "4"}]
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 5, "outputTokens": 1, "totalTokens": 6}
        })))
        .mount(server)
        .await;
}

fn assert_invalid_request(error: OpenAIError) {
    let OpenAIError::ApiError(error) = error else {
        panic!("expected a client API error, got {error:?}");
    };

    assert_eq!(
        error.message,
        "Model 'us.amazon.nova-2-lite-v1:0' does not support `max_completion_tokens`, `max_tokens`, `temperature` or `top_p` when `reasoning_effort` is `high` or `xhigh`. Remove those parameters or set `reasoning_effort` to `low` or `medium`. See: https://spiceai.org/docs/components/models/aws-bedrock"
    );
    assert_eq!(error.r#type.as_deref(), Some("invalid_request_error"));
    assert_eq!(error.code.as_deref(), Some("invalid_argument"));
    assert_eq!(ApiErrorKind::of(&error), Some(ApiErrorKind::InvalidRequest));
}

#[tokio::test]
async fn nova_high_reasoning_sends_only_supported_converse_parameters() {
    let server = MockServer::start().await;
    mount_bedrock_response(&server).await;
    let chat = bedrock_converse(&server);

    let response = chat
        .chat_request(request())
        .await
        .expect("local Bedrock response should convert");
    assert_eq!(response.choices[0].message.content.as_deref(), Some("4"));

    let received = server
        .received_requests()
        .await
        .expect("mock server should record the request");
    assert_eq!(received.len(), 1);
    let body: Value =
        serde_json::from_slice(&received[0].body).expect("request body should be JSON");
    assert_eq!(
        body["additionalModelRequestFields"]["reasoningConfig"],
        json!({"type": "enabled", "maxReasoningEffort": "high"})
    );
    assert_eq!(body["inferenceConfig"]["stopSequences"], json!(["done"]));
    assert!(body["inferenceConfig"].get("maxTokens").is_none());
    assert!(body["inferenceConfig"].get("temperature").is_none());
    assert!(body["inferenceConfig"].get("topP").is_none());
}

#[tokio::test]
async fn nova_high_and_xhigh_reasoning_return_conflict_error_before_converse() {
    let server = MockServer::start().await;
    mount_bedrock_response(&server).await;
    let chat = bedrock_converse(&server);

    for reasoning_effort in [ReasoningEffort::High, ReasoningEffort::Xhigh] {
        for conflicting_request in conflicting_requests(reasoning_effort) {
            let Err(error) = chat.chat_request(conflicting_request).await else {
                panic!(
                    "Nova high or xhigh reasoning should reject conflicting inference parameters"
                );
            };
            assert_invalid_request(error);
        }
    }

    let received = server
        .received_requests()
        .await
        .expect("mock server should record the request");
    assert_eq!(
        received.len(),
        0,
        "invalid inference parameters reached Bedrock"
    );
}

#[tokio::test]
async fn nova_high_and_xhigh_reasoning_return_conflict_error_before_converse_stream() {
    let server = MockServer::start().await;
    mount_bedrock_response(&server).await;
    let chat = bedrock_converse(&server);

    for reasoning_effort in [ReasoningEffort::High, ReasoningEffort::Xhigh] {
        for conflicting_request in conflicting_requests(reasoning_effort) {
            let Err(error) = chat.chat_stream(conflicting_request).await else {
                panic!(
                    "Nova high or xhigh reasoning should reject conflicting inference parameters"
                );
            };
            assert_invalid_request(error);
        }
    }

    let received = server
        .received_requests()
        .await
        .expect("mock server should record the request");
    assert_eq!(
        received.len(),
        0,
        "invalid inference parameters reached Bedrock"
    );
}
