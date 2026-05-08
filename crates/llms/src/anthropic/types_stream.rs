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
#![allow(deprecated)] // `function_call` argument is deprecated but no builder pattern alternative is available.
use super::types::{MessageRole, StopReason, Usage};
use async_openai::{
    error::{ApiError, OpenAIError},
    types::chat::{
        ChatChoiceStream, ChatCompletionMessageToolCallChunk, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CompletionTokensDetails, CompletionUsage,
        CreateChatCompletionStreamResponse, FinishReason, FunctionCallStream, FunctionType,
        PromptTokensDetails, Role,
    },
};
use futures::{Stream, StreamExt};
use reqwest_eventsource::Error as SseError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, pin::Pin, sync::Arc};

use tokio::sync::Mutex;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum MessageCreateStreamResponse {
    #[serde(rename = "message_start")]
    MessageStart { message: MessageStartMessage },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: u32,
        content_block: ContentBlock,
    },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: u32, delta: Delta },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: u32 },

    #[serde(rename = "message_delta")]
    MessageDelta { delta: MessageDelta, usage: Usage },
    #[serde(rename = "message_stop")]
    MessageStop,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageStartMessage {
    pub id: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub role: String,
    pub model: String,
    pub stop_sequence: Option<String>,
    pub usage: Usage,
    pub content: Vec<String>,
    pub stop_reason: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse(ContentBlockToolUse),
    #[serde(rename = "thinking")]
    Thinking { thinking: String, signature: String },
    #[serde(rename = "redacted_thinking")]
    RedactedThinking { data: String },
}

impl ContentBlock {
    pub fn into_completion(self) -> ChatCompletionStreamResponseDelta {
        match self {
            ContentBlock::Text { text } => ChatCompletionStreamResponseDelta {
                content: Some(text),
                function_call: None,
                tool_calls: None,
                refusal: None,
                role: None,
            },
            ContentBlock::ToolUse(ContentBlockToolUse { id, name, .. }) => {
                ChatCompletionStreamResponseDelta {
                    content: None,
                    function_call: None,
                    tool_calls: Some(vec![ChatCompletionMessageToolCallChunk {
                        index: 0,
                        id: Some(id),
                        r#type: Some(FunctionType::Function),
                        function: Some(FunctionCallStream {
                            name: Some(name),
                            arguments: None,
                        }),
                    }]),
                    refusal: None,
                    role: None,
                }
            }
            ContentBlock::Thinking { .. } | ContentBlock::RedactedThinking { .. } => {
                ChatCompletionStreamResponseDelta {
                    content: None,
                    function_call: None,
                    tool_calls: None,
                    refusal: None,
                    role: None,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContentBlockToolUse {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub(crate) enum Delta {
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "input_json_delta")]
    InputJsonDelta { partial_json: String },
}

impl Delta {
    pub fn into_completion(
        self,
        role: Option<&MessageRole>,
        tool_content: Option<&ContentBlockToolUse>,
    ) -> ChatCompletionStreamResponseDelta {
        match (self, tool_content) {
            (Delta::TextDelta { text }, _) => ChatCompletionStreamResponseDelta {
                content: Some(text),
                function_call: None,
                tool_calls: None,
                refusal: None,
                role: match role {
                    Some(MessageRole::Assistant) => Some(Role::Assistant),
                    Some(MessageRole::User) => Some(Role::User),
                    None => None,
                },
            },
            (
                Delta::InputJsonDelta { partial_json },
                Some(ContentBlockToolUse {
                    id, name: _name, ..
                }),
            ) => ChatCompletionStreamResponseDelta {
                content: None,
                function_call: None,
                tool_calls: Some(vec![ChatCompletionMessageToolCallChunk {
                    index: 0,
                    id: Some(id.clone()),
                    r#type: Some(FunctionType::Function),
                    function: Some(FunctionCallStream {
                        name: None, // Intentially leave empty to match OpenAI's format.
                        arguments: Some(partial_json),
                    }),
                }]),
                refusal: None,
                role: match role {
                    Some(MessageRole::Assistant) => Some(Role::Assistant),
                    Some(MessageRole::User) => Some(Role::User),
                    None => None,
                },
            },

            // This should never happen, but we need to handle it as an 'empty' response.
            (Delta::InputJsonDelta { partial_json: _ }, None) => {
                ChatCompletionStreamResponseDelta {
                    content: None,
                    function_call: None,
                    tool_calls: None,
                    refusal: None,
                    role: match role {
                        Some(MessageRole::Assistant) => Some(Role::Assistant),
                        Some(MessageRole::User) => Some(Role::User),
                        None => None,
                    },
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageDelta {
    pub stop_reason: Option<StopReason>,
    pub stop_sequence: Option<String>,
}

/// Convert the stream of Anthropic [`MessageCreateStreamResponse`] into a stream of `OpenAI` compatible [`async_openai::types::CreateChatCompletionStreamResponse`].
///
/// Except for differences in the stream packet formats, the core difference are:
///
///  +---------------------------------------------------------+---------------------------------------------------------+
///  | Anthropic                                               | `OpenAI`                                                  |
///  +---------------------------------------------------------+---------------------------------------------------------+
///  | Only first packet for a specific tool has tool metadata | All packets for a tool have tool metadata               |
///  |                                                         |                                                         |
///  | Initial message has initial usage details. Last message | Last message has usage details.                         |
///  | has additional usage details.                           |                                                         |
///  |                                                         |                                                         |
///  | Tool packets have no out of order protection            | Provides numbering for out of order tool packets        |
///  +---------------------------------------------------------+---------------------------------------------------------+
///
pub fn transform_stream(
    stream: Pin<Box<dyn Stream<Item = Result<MessageCreateStreamResponse, OpenAIError>> + Send>>,
) -> ChatCompletionResponseStream {
    // As mentioned above, only first tool packet has tool metadata.
    // Format:
    //  First Message: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01T1x1fJ34qAmk2tNTrN7Up6","name":"get_weather","input":{}}}
    //  Subsequent Messages: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"o,"}}
    //
    // We need to keep track of the `.content_block` and the index of the tool delta to associate the tool call with the correct content block.
    // Map `.index` to `.content_block`
    #[derive(Clone, Default)]
    struct StreamState {
        id: Option<String>,
        model: Option<String>,
        role: Option<MessageRole>,
        usage: Option<CompletionUsage>,
        tool_id_to_content_block: HashMap<u32, ContentBlockToolUse>,
        tool_id_to_tool_delta_idx: HashMap<u32, i32>,
    }

    let state = Arc::new(Mutex::new(StreamState::default()));

    let transformed_stream = stream
        .filter_map(move |item| {
            let inner_state = Arc::clone(&state);
            async move {
                let mut state = inner_state.lock().await;
                match item {
                    Ok(MessageCreateStreamResponse::MessageStart {
                        message:
                            MessageStartMessage {
                                id: inner_id,
                                role: inner_role,
                                usage: inner_usage,
                                model,
                                ..
                            },
                    }) => {
                        state.role = MessageRole::from_opt(&inner_role);
                        state.id = Some(inner_id);
                        state.usage = Some(inner_usage.into());
                        state.model = Some(model);
                        Some(create_anthropic_stream_response(
                            &state.id.clone().unwrap_or_default(),
                            &state.model.clone().unwrap_or_default(),
                            None,
                            None,
                        ))
                    }
                    Ok(MessageCreateStreamResponse::ContentBlockStart {
                        index,
                        content_block,
                    }) => {
                        if let ContentBlock::ToolUse(t) = &content_block {
                            state.tool_id_to_content_block.insert(index, t.clone());
                            state.tool_id_to_tool_delta_idx.insert(index, 0);
                        }
                        Some(create_anthropic_stream_response(
                            &state.id.clone().unwrap_or_default(),
                            &state.model.clone().unwrap_or_default(),
                            None,
                            Some(ChatChoiceStream {
                                index: 0,
                                delta: content_block.into_completion(),
                                finish_reason: None,
                                logprobs: None,
                            }),
                        ))
                    }
                    Ok(MessageCreateStreamResponse::ContentBlockDelta { index, delta }) => {
                        let tool_idx = *state.tool_id_to_tool_delta_idx.get(&index).unwrap_or(&0);
                        state.tool_id_to_tool_delta_idx.insert(index, tool_idx + 1);

                        Some(create_anthropic_stream_response(
                            &state.id.clone().unwrap_or_default(),
                            &state.model.clone().unwrap_or_default(),
                            None,
                            Some(ChatChoiceStream {
                                index: 0,
                                logprobs: None,
                                finish_reason: None,
                                delta: delta.into_completion(
                                    state.role.as_ref(),
                                    state.tool_id_to_content_block.get(&index),
                                ),
                            }),
                        ))
                    }
                    Ok(MessageCreateStreamResponse::MessageDelta {
                        delta: MessageDelta { stop_reason, .. },
                        usage: inner_usage,
                    }) => {
                        // Update usage
                        if let Some(ref mut u) = state.usage {
                            add_usage_delta(u, inner_usage);
                        }
                        Some(create_anthropic_stream_response(
                            &state.id.clone().unwrap_or_default(),
                            &state.model.clone().unwrap_or_default(),
                            state.usage.clone(),
                            Some(ChatChoiceStream {
                                index: 0,
                                logprobs: None,
                                finish_reason: match stop_reason {
                                    Some(
                                        StopReason::EndTurn
                                        | StopReason::StopSequence
                                        | StopReason::PauseTurn,
                                    ) => Some(FinishReason::Stop),
                                    Some(
                                        StopReason::MaxTokens
                                        | StopReason::ModelContextWindowExceeded,
                                    ) => Some(FinishReason::Length),
                                    Some(StopReason::ToolUse) => Some(FinishReason::ToolCalls),
                                    Some(StopReason::Refusal) => Some(FinishReason::ContentFilter),
                                    None => None,
                                },
                                delta: ChatCompletionStreamResponseDelta {
                                    content: None,
                                    function_call: None,
                                    tool_calls: None,
                                    role: None,
                                    refusal: None,
                                },
                            }),
                        ))
                    }
                    Ok(
                        MessageCreateStreamResponse::Ping
                        | MessageCreateStreamResponse::ContentBlockStop { .. }
                        | MessageCreateStreamResponse::MessageStop,
                    ) => None,
                    Err(e) => {
                        let formatted_error = format_anthropic_stream_error(e);
                        tracing::debug!(
                            "Received an anthropic error stream packet: {:?}",
                            formatted_error
                        );
                        Some(Err(formatted_error))
                    }
                }
            }
        })
        // Because we don't early exit on [`MessageCreateStreamResponse::MessageStop`], we need to handle stream end explicitly, otherwise we will infinite loop on the stream.
        .take_while(|item| {
            let keep_going = !matches!(item, Err(OpenAIError::ApiError(ApiError { message, .. })) if SseError::StreamEnded{}.to_string().eq(message));
            futures::future::ready(keep_going)
        });

    Box::pin(transformed_stream)
}

fn add_usage_delta(usage: &mut CompletionUsage, delta: Usage) {
    let delta = CompletionUsage::from(delta);

    usage.prompt_tokens = usage.prompt_tokens.saturating_add(delta.prompt_tokens);
    usage.completion_tokens = usage
        .completion_tokens
        .saturating_add(delta.completion_tokens);
    usage.total_tokens = usage.total_tokens.saturating_add(delta.total_tokens);
    usage.prompt_tokens_details = combine_prompt_token_details(
        usage.prompt_tokens_details.take(),
        delta.prompt_tokens_details,
    );
    usage.completion_tokens_details = combine_completion_token_details(
        usage.completion_tokens_details.take(),
        delta.completion_tokens_details,
    );
}

fn combine_prompt_token_details(
    current: Option<PromptTokensDetails>,
    delta: Option<PromptTokensDetails>,
) -> Option<PromptTokensDetails> {
    match (current, delta) {
        (Some(current), Some(delta)) => Some(PromptTokensDetails {
            audio_tokens: combine_opt_u32(current.audio_tokens, delta.audio_tokens),
            cached_tokens: combine_opt_u32(current.cached_tokens, delta.cached_tokens),
        }),
        (Some(current), None) => Some(current),
        (None, Some(delta)) => Some(delta),
        (None, None) => None,
    }
}

fn combine_completion_token_details(
    current: Option<CompletionTokensDetails>,
    delta: Option<CompletionTokensDetails>,
) -> Option<CompletionTokensDetails> {
    match (current, delta) {
        (Some(current), Some(delta)) => Some(CompletionTokensDetails {
            accepted_prediction_tokens: combine_opt_u32(
                current.accepted_prediction_tokens,
                delta.accepted_prediction_tokens,
            ),
            audio_tokens: combine_opt_u32(current.audio_tokens, delta.audio_tokens),
            reasoning_tokens: combine_opt_u32(current.reasoning_tokens, delta.reasoning_tokens),
            rejected_prediction_tokens: combine_opt_u32(
                current.rejected_prediction_tokens,
                delta.rejected_prediction_tokens,
            ),
        }),
        (Some(current), None) => Some(current),
        (None, Some(delta)) => Some(delta),
        (None, None) => None,
    }
}

fn combine_opt_u32(current: Option<u32>, delta: Option<u32>) -> Option<u32> {
    match (current, delta) {
        (Some(current), Some(delta)) => Some(current.saturating_add(delta)),
        (Some(current), None) => Some(current),
        (None, Some(delta)) => Some(delta),
        (None, None) => None,
    }
}

fn format_anthropic_stream_error(error: OpenAIError) -> OpenAIError {
    let OpenAIError::ApiError(api_error) = error else {
        return error;
    };

    let lowered = api_error.message.to_lowercase();

    if lowered.contains("too many requests") || lowered.contains("429") {
        return OpenAIError::ApiError(ApiError {
            message: "Anthropic API rate limit exceeded. Check your limits at https://console.anthropic.com/settings/limits and retry shortly.".to_string(),
            r#type: Some("AnthropicRateLimitError".to_string()),
            param: api_error.param,
            code: api_error.code,
        });
    }

    if lowered.contains("401")
        || lowered.contains("403")
        || lowered.contains("authentication")
        || lowered.contains("unauthorized")
        || lowered.contains("forbidden")
    {
        return OpenAIError::ApiError(ApiError {
            message: "Anthropic authentication failed. Verify your Anthropic API key and workspace permissions.".to_string(),
            r#type: Some("AnthropicAuthenticationError".to_string()),
            param: api_error.param,
            code: api_error.code,
        });
    }

    OpenAIError::ApiError(ApiError {
        message: format!("Anthropic streaming error: {}", api_error.message),
        r#type: Some("AnthropicStreamError".to_string()),
        param: api_error.param,
        code: api_error.code,
    })
}

/// Easy way to create stream. Reduce boiler plate. [`CreateChatCompletionStreamResponse`] has no builder pattern.
fn create_anthropic_stream_response(
    id: &str,
    model: &str,
    usage: Option<CompletionUsage>,
    choice: Option<ChatChoiceStream>,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let choices = match choice {
        Some(c) => vec![c],
        None => vec![],
    };

    crate::streaming_utils::create_stream_response(id, model, choices, usage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_delta_accumulates_cache_tokens() {
        let mut usage = Usage {
            input_tokens: 10,
            output_tokens: 1,
            cache_read_input_tokens: Some(3),
            ..Usage::default()
        }
        .into();

        add_usage_delta(
            &mut usage,
            Usage {
                input_tokens: 2,
                output_tokens: 4,
                cache_creation_input_tokens: Some(5),
                cache_read_input_tokens: Some(7),
                ..Usage::default()
            },
        );

        assert_eq!(usage.prompt_tokens, 27);
        assert_eq!(usage.completion_tokens, 5);
        assert_eq!(usage.total_tokens, 32);
        assert_eq!(
            usage
                .prompt_tokens_details
                .as_ref()
                .and_then(|details| details.cached_tokens),
            Some(10)
        );
    }

    #[test]
    fn usage_delta_saturates_token_counts() {
        let mut usage = CompletionUsage {
            prompt_tokens: u32::MAX - 1,
            completion_tokens: u32::MAX - 1,
            total_tokens: u32::MAX - 1,
            prompt_tokens_details: Some(PromptTokensDetails {
                cached_tokens: Some(u32::MAX - 1),
                audio_tokens: Some(u32::MAX - 1),
            }),
            completion_tokens_details: None,
        };

        add_usage_delta(
            &mut usage,
            Usage {
                input_tokens: 2,
                output_tokens: 2,
                cache_read_input_tokens: Some(2),
                ..Usage::default()
            },
        );

        assert_eq!(usage.prompt_tokens, u32::MAX);
        assert_eq!(usage.completion_tokens, u32::MAX);
        assert_eq!(usage.total_tokens, u32::MAX);
        assert_eq!(
            usage
                .prompt_tokens_details
                .as_ref()
                .and_then(|details| details.cached_tokens),
            Some(u32::MAX)
        );
        assert_eq!(
            usage
                .prompt_tokens_details
                .as_ref()
                .and_then(|details| details.audio_tokens),
            Some(u32::MAX - 1)
        );
    }
}
