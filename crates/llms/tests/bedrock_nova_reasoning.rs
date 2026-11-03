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

use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequest, ReasoningEffort,
    StopConfiguration,
};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use chat_api::Chat;
use llms::bedrock::{BedrockClient, chat::BedrockConverse};
use serde_json::{Value, json};
use std::sync::Arc;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn request() -> CreateChatCompletionRequest {
    let mut request = CreateChatCompletionRequest::default();
    request.model = "nova".to_string();
    request.messages = vec![
        ChatCompletionRequestUserMessageArgs::default()
            .content("What is 2+2?")
            .build()
            .expect("user message should build")
            .into(),
    ];
    request.reasoning_effort = Some(ReasoningEffort::High);
    request.max_completion_tokens = Some(1024);
    request.temperature = Some(0.5);
    request.top_p = Some(0.9);
    request.stop = Some(StopConfiguration::String("done".to_string()));
    request
}

#[tokio::test]
async fn nova_high_reasoning_sends_only_supported_converse_parameters() {
    let server = MockServer::start().await;
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
        .mount(&server)
        .await;

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
    let chat = BedrockConverse::new(
        Arc::new(BedrockClient::from(&config)),
        "us.amazon.nova-2-lite-v1:0".to_string(),
    );

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
