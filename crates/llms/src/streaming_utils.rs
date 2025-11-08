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

//! Shared utilities for streaming chat completions

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoiceStream, ChatCompletionStreamResponseDelta, CompletionUsage,
        CreateChatCompletionStreamResponse, FinishReason, Role,
    },
};
use async_stream::stream;
use futures::{Stream, StreamExt};
use rand::distr::Alphanumeric;
use rand::{Rng, rng};
use std::{pin::Pin, time::SystemTime};

use crate::chat::Result;

/// Creates a standardized `CreateChatCompletionStreamResponse` with consistent formatting
///
/// # Errors
///
/// Returns an error if the system time cannot be determined or if the timestamp
/// conversion fails.
#[allow(clippy::cast_possible_truncation)]
pub fn create_stream_response(
    id: &str,
    model: &str,
    choices: Vec<ChatChoiceStream>,
    usage: Option<CompletionUsage>,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let created = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
        .as_secs() as u32;

    Ok(CreateChatCompletionStreamResponse {
        id: id.to_string(),
        created,
        model: model.to_string(),
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage,
        choices,
    })
}

/// Creates a standardized `CreateChatCompletionStreamResponse` with custom timestamp
///
/// # Errors
///
/// This function is currently infallible and wrapped in `Ok()`, but returns a `Result`
/// for API consistency with other streaming utility functions.
#[allow(clippy::cast_possible_truncation)]
pub fn create_stream_response_with_timestamp(
    id: &str,
    model: &str,
    choices: Vec<ChatChoiceStream>,
    usage: Option<CompletionUsage>,
    created: u32,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    Ok(CreateChatCompletionStreamResponse {
        id: id.to_string(),
        created,
        model: model.to_string(),
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage,
        choices,
    })
}

/// Creates a chat choice for streaming with optional content
#[must_use]
pub fn create_stream_choice(
    index: u32,
    content: Option<String>,
    role: Option<Role>,
    finish_reason: Option<FinishReason>,
) -> ChatChoiceStream {
    ChatChoiceStream {
        index,
        delta: ChatCompletionStreamResponseDelta {
            content,
            role,
            #[allow(deprecated)]
            function_call: None,
            tool_calls: None,
            refusal: None,
        },
        finish_reason,
        logprobs: None,
    }
}

/// Generates a unique stream ID with the format `{model_id}-{random_suffix}`
pub fn generate_stream_id(model_id: &str) -> String {
    let random_suffix: String = rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    format!("{model_id}-{random_suffix}")
}

/// Converts a basic string stream to an `OpenAI` compatible chat completion stream
#[must_use]
pub fn string_stream_to_chat_stream(
    model_id: String,
    mut stream: Pin<Box<dyn Stream<Item = Result<Option<String>>> + Send>>,
) -> Pin<Box<dyn Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Send>> {
    let stream_id = generate_stream_id(&model_id);

    Box::pin(stream! {
        let mut index = 0;
        while let Some(msg) = stream.next().await {
            let choice = match msg {
                Ok(Some(content)) => {
                    create_stream_choice(index, Some(content), Some(Role::Assistant), None)
                }
                Ok(None) => {
                    create_stream_choice(index, None, Some(Role::Assistant), Some(FinishReason::Stop))
                }
                Err(e) => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: e.to_string(),
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                    break;
                }
            };

            match create_stream_response(
                &format!("{stream_id}-{index}"),
                &model_id,
                vec![choice],
                None,
            ) {
                Ok(response) => yield Ok(response),
                Err(e) => yield Err(e),
            }

            index += 1;
        }
    })
}

/// Creates a stream that yields multiple content chunks for testing streaming behavior
#[must_use]
pub fn create_mock_streaming_response(
    model_name: String,
    content_chunks: Vec<String>,
    final_usage: Option<CompletionUsage>,
) -> Pin<Box<dyn Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Send>> {
    let stream_id = generate_stream_id(&model_name);

    Box::pin(stream! {
        let num_chunks = content_chunks.len();
        for (index, chunk) in content_chunks.into_iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let choice = create_stream_choice(
                index as u32,
                Some(chunk),
                if index == 0 { Some(Role::Assistant) } else { None },
                None,
            );

            match create_stream_response(
                &stream_id,
                &model_name,
                vec![choice],
                None,
            ) {
                Ok(response) => yield Ok(response),
                Err(e) => yield Err(e),
            }
        }

        // Final chunk with finish reason and optional usage
        #[allow(clippy::cast_possible_truncation)]
        let final_choice = create_stream_choice(
            num_chunks as u32,
            Some(String::new()),
            None,
            Some(FinishReason::Stop),
        );

        match create_stream_response(
            &stream_id,
            &model_name,
            vec![final_choice],
            final_usage,
        ) {
            Ok(response) => yield Ok(response),
            Err(e) => yield Err(e),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::{CompletionUsage, Role};
    use futures::StreamExt;

    #[test]
    fn test_generate_stream_id() {
        let model_id = "test-model";
        let id1 = generate_stream_id(model_id);
        let id2 = generate_stream_id(model_id);

        assert!(id1.starts_with(model_id));
        assert!(id2.starts_with(model_id));
        assert_ne!(id1, id2); // Should be unique
        assert!(id1.len() > model_id.len());
    }

    #[test]
    fn test_create_stream_choice() {
        let choice =
            create_stream_choice(0, Some("Hello".to_string()), Some(Role::Assistant), None);

        assert_eq!(choice.index, 0);
        assert_eq!(choice.delta.content, Some("Hello".to_string()));
        assert_eq!(choice.delta.role, Some(Role::Assistant));
        assert_eq!(choice.finish_reason, None);
    }

    #[test]
    fn test_create_stream_response() {
        let choice = create_stream_choice(0, Some("test".to_string()), Some(Role::Assistant), None);
        let response = create_stream_response("test-id", "test-model", vec![choice], None)
            .expect("Failed to create stream response");

        assert_eq!(response.id, "test-id");
        assert_eq!(response.model, "test-model");
        assert_eq!(response.object, "chat.completion.chunk");
        assert_eq!(response.choices.len(), 1);
        assert_eq!(response.choices[0].delta.content, Some("test".to_string()));
    }

    #[tokio::test]
    async fn test_create_mock_streaming_response() {
        let chunks = vec!["Hello ".to_string(), "world".to_string(), "!".to_string()];
        let usage = Some(CompletionUsage {
            prompt_tokens: 10,
            completion_tokens: 15,
            total_tokens: 25,
            prompt_tokens_details: None,
            completion_tokens_details: None,
        });

        let mut stream = create_mock_streaming_response("test-model".to_string(), chunks, usage);

        let mut collected = Vec::new();
        while let Some(item) = stream.next().await {
            collected.push(item.expect("Stream item should be Ok"));
        }

        // Should have 3 content chunks + 1 final chunk
        assert_eq!(collected.len(), 4);

        // Check content chunks
        assert_eq!(
            collected[0].choices[0].delta.content,
            Some("Hello ".to_string())
        );
        assert_eq!(
            collected[1].choices[0].delta.content,
            Some("world".to_string())
        );
        assert_eq!(collected[2].choices[0].delta.content, Some("!".to_string()));

        // Check final chunk
        assert_eq!(
            collected[3].choices[0].finish_reason,
            Some(FinishReason::Stop)
        );
        assert!(collected[3].usage.is_some());
    }
}
