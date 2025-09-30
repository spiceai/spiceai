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

//! Integration tests for streaming chat completions

use async_openai::types::*;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use llms::{chat::Chat, streaming_utils};

/// Mock streaming Chat implementation for testing
struct StreamingMockChat {
    name: String,
    chunks: Vec<String>,
}

impl StreamingMockChat {
    pub fn new(name: String, chunks: Vec<String>) -> Self {
        Self { name, chunks }
    }
}

#[async_trait]
impl Chat for StreamingMockChat {
    fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
        None
    }

    async fn run(&self, prompt: String) -> llms::chat::Result<Option<String>> {
        Ok(Some(format!("Response from {}: {}", self.name, prompt)))
    }

    async fn chat_stream(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError> {
        let completion_tokens: u32 = self
            .chunks
            .iter()
            .map(|c| u32::try_from(c.len()).unwrap_or(0))
            .sum();
        let usage = Some(CompletionUsage {
            prompt_tokens: 10,
            completion_tokens,
            total_tokens: 10 + completion_tokens,
            prompt_tokens_details: None,
            completion_tokens_details: None,
        });

        Ok(streaming_utils::create_mock_streaming_response(
            self.name.clone(),
            self.chunks.clone(),
            usage,
        ))
    }

    async fn chat_request(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError> {
        unimplemented!("Use chat_stream instead")
    }
}

/// Error Chat implementation for testing error scenarios
struct ErrorMockChat;

#[async_trait]
impl Chat for ErrorMockChat {
    fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
        None
    }

    async fn run(&self, _prompt: String) -> llms::chat::Result<Option<String>> {
        Err(llms::chat::Error::FailedToRunModel {
            source: "Test error".into(),
        })
    }

    async fn chat_stream(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError> {
        Err(async_openai::error::OpenAIError::ApiError(
            async_openai::error::ApiError {
                message: "Test streaming error".to_string(),
                r#type: None,
                param: None,
                code: None,
            },
        ))
    }

    async fn chat_request(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError> {
        unimplemented!("Use chat_stream instead")
    }
}

#[tokio::test]
async fn test_streaming_chat_completion() {
    let chunks = vec![
        "Hello ".to_string(),
        "streaming ".to_string(),
        "works!".to_string(),
    ];

    let model = StreamingMockChat::new("test-streaming".to_string(), chunks.clone());

    // Create a test request
    let system_message = ChatCompletionRequestSystemMessageArgs::default()
        .content("Hello, test streaming!".to_string())
        .build()
        .expect("Failed to build system message");
    let request = CreateChatCompletionRequestArgs::default()
        .model("test-streaming".to_string())
        .messages(vec![system_message.into()])
        .stream(true)
        .build()
        .expect("Failed to build chat completion request");

    // Get the stream
    let mut stream = model
        .chat_stream(request)
        .await
        .expect("Failed to create chat stream");

    let mut content_parts = Vec::new();
    let mut usage = None;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(response) => {
                if let Some(choice) = response.choices.first()
                    && let Some(content) = &choice.delta.content
                    && !content.is_empty()
                {
                    content_parts.push(content.clone());
                }
                if response.usage.is_some() {
                    usage = response.usage;
                }
            }
            Err(e) => {
                panic!("Stream error: {e:?}");
            }
        }
    }

    let final_content = content_parts.join("");
    assert_eq!(final_content, "Hello streaming works!");
    let usage = usage.expect("Usage should be present in final chunk");
    assert_eq!(usage.prompt_tokens, 10);
    assert!(usage.completion_tokens > 0);
    assert_eq!(
        usage.total_tokens,
        usage.prompt_tokens + usage.completion_tokens
    );
}

#[tokio::test]
async fn test_streaming_error_handling() {
    let model = ErrorMockChat;

    let system_message = ChatCompletionRequestSystemMessageArgs::default()
        .content("This should fail".to_string())
        .build()
        .expect("Failed to build system message");
    let request = CreateChatCompletionRequestArgs::default()
        .model("error-model".to_string())
        .messages(vec![system_message.into()])
        .stream(true)
        .build()
        .expect("Failed to build chat completion request");

    let result = model.chat_stream(request).await;
    assert!(result.is_err());

    // We can't use unwrap_err() directly because the stream doesn't implement Debug
    // Instead, we'll check that we get an error which is what we expect
}

#[tokio::test]
async fn test_empty_streaming_response() {
    let model = StreamingMockChat::new("test-empty".to_string(), vec![]);

    let system_message = ChatCompletionRequestSystemMessageArgs::default()
        .content("Empty response test".to_string())
        .build()
        .expect("Failed to build system message");
    let request = CreateChatCompletionRequestArgs::default()
        .model("test-empty".to_string())
        .messages(vec![system_message.into()])
        .stream(true)
        .build()
        .expect("Failed to build chat completion request");

    let mut stream = model
        .chat_stream(request)
        .await
        .expect("Failed to create chat stream");

    let mut chunk_count = 0;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(_response) => {
                chunk_count += 1;
            }
            Err(e) => {
                panic!("Stream error: {e:?}");
            }
        }
    }

    // Should have at least one chunk (the final chunk with finish reason)
    assert!(chunk_count >= 1);
}

#[tokio::test]
async fn test_model_store_integration() {
    // Create a mock model store
    let mut store = HashMap::new();
    store.insert(
        "test-streaming".to_string(),
        Arc::new(StreamingMockChat::new(
            "test-streaming".to_string(),
            vec![
                "Hello ".to_string(),
                "from ".to_string(),
                "store!".to_string(),
            ],
        )) as Arc<dyn Chat>,
    );

    let model_store = Arc::new(RwLock::new(store));
    let model_store_guard = model_store.read().await;
    let model = model_store_guard
        .get("test-streaming")
        .expect("Model should exist in store");

    let system_message = ChatCompletionRequestSystemMessageArgs::default()
        .content("Hello from model store!".to_string())
        .build()
        .expect("Failed to build system message");
    let request = CreateChatCompletionRequestArgs::default()
        .model("test-streaming".to_string())
        .messages(vec![system_message.into()])
        .stream(true)
        .build()
        .expect("Failed to build chat completion request");

    let mut stream = model
        .chat_stream(request)
        .await
        .expect("Failed to create chat stream");

    let mut content_parts = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(response) => {
                if let Some(choice) = response.choices.first()
                    && let Some(content) = &choice.delta.content
                    && !content.is_empty()
                {
                    content_parts.push(content.clone());
                }
            }
            Err(e) => {
                panic!("Stream error: {e:?}");
            }
        }
    }

    let final_content = content_parts.join("");
    assert_eq!(final_content, "Hello from store!");
}

#[tokio::test]
async fn test_streaming_response_structure() {
    let chunks = vec!["Test".to_string(), " response".to_string()];
    let model = StreamingMockChat::new("test-structure".to_string(), chunks);

    let system_message = ChatCompletionRequestSystemMessageArgs::default()
        .content("Structure test".to_string())
        .build()
        .expect("Failed to build system message");
    let request = CreateChatCompletionRequestArgs::default()
        .model("test-structure".to_string())
        .messages(vec![system_message.into()])
        .stream(true)
        .build()
        .expect("Failed to build chat completion request");

    let mut stream = model
        .chat_stream(request)
        .await
        .expect("Failed to create chat stream");

    let mut responses = Vec::new();
    while let Some(chunk) = stream.next().await {
        responses.push(chunk.expect("Stream chunk should be valid"));
    }

    // Check that all responses have consistent structure
    for response in &responses {
        assert!(!response.id.is_empty());
        assert_eq!(response.model, "test-structure");
        assert_eq!(response.object, "chat.completion.chunk");
        assert!(response.created > 0);
    }

    // Check that the final response has usage information
    let final_response = responses.last().expect("Should have at least one response");
    assert!(final_response.usage.is_some());

    // Check that the final response has a finish reason
    if let Some(choice) = final_response.choices.first() {
        assert!(choice.finish_reason.is_some());
    }
}
