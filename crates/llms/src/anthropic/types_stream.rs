/*
Copyright 2024 The Spice.ai OSS Authors

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
use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoiceStream, ChatCompletionMessageToolCallChunk, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, ChatCompletionToolType, CompletionUsage,
        CreateChatCompletionStreamResponse, FinishReason, FunctionCallStream, Role,
    },
};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fmt, pin::Pin, time::SystemTime};

use super::types::{MessageRole, StopReason, Usage};

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
            ContentBlock::ToolUse(tool_use) => ChatCompletionStreamResponseDelta {
                content: None,
                function_call: None,
                tool_calls: Some(vec![ChatCompletionMessageToolCallChunk {
                    index: 0,
                    id: Some(tool_use.id),
                    r#type: Some(ChatCompletionToolType::Function),
                    function: Some(FunctionCallStream {
                        name: Some(tool_use.name),
                        arguments: None,
                    }),
                }]),
                refusal: None,
                role: None,
            },
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
        role: &Option<MessageRole>,
        tool_content: Option<(i32, ContentBlockToolUse)>,
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
                Some((index, ContentBlockToolUse { id, name, .. })),
            ) => ChatCompletionStreamResponseDelta {
                content: None,
                function_call: None,
                tool_calls: Some(vec![ChatCompletionMessageToolCallChunk {
                    index,
                    id: Some(id),
                    r#type: Some(ChatCompletionToolType::Function),
                    function: Some(FunctionCallStream {
                        name: Some(name),
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
pub struct AnthropicStreamError {
    #[serde(rename = "type")]
    pub event_type: String,
    pub error: ErrorPayload,
}

impl fmt::Display for AnthropicStreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AnthropicStreamError: {:?}", self.error)
    }
}

impl From<reqwest_eventsource::Error> for AnthropicStreamError {
    fn from(e: reqwest_eventsource::Error) -> Self {
        AnthropicStreamError {
            event_type: "error".to_string(),
            error: ErrorPayload {
                error_type: "reqwest_eventsource_error".to_string(),
                message: e.to_string(),
            },
        }
    }
}

impl From<serde_json::Error> for AnthropicStreamError {
    fn from(e: serde_json::Error) -> Self {
        AnthropicStreamError {
            event_type: "error".to_string(),
            error: ErrorPayload {
                error_type: "serde_json_error".to_string(),
                message: e.to_string(),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorPayload {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
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
#[allow(clippy::too_many_lines)]

pub fn transform_stream(
    stream: Pin<
        Box<dyn Stream<Item = Result<MessageCreateStreamResponse, AnthropicStreamError>> + Send>,
    >,
    model: String,
) -> ChatCompletionResponseStream {
    // As mentioned above, only first tool packet has tool metadata.
    // Format:
    //  First Message: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01T1x1fJ34qAmk2tNTrN7Up6","name":"get_weather","input":{}}}
    //  Subsequent Messages: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"o,"}}
    //
    // We need to keep track of the `.content_block` and the index of the tool delta to associate the tool call with the correct content block.
    // Map `.index` to `.content_block`
    #[derive(Clone)]
    struct StreamState {
        id: Option<String>,
        role: Option<MessageRole>,
        usage: Option<CompletionUsage>,
        tool_id_to_content_block: HashMap<u32, ContentBlockToolUse>,
        tool_id_to_tool_delta_idx: HashMap<u32, i32>,
    }

    let initial_state = StreamState {
        id: None,
        role: None,
        usage: None,
        tool_id_to_content_block: HashMap::new(),
        tool_id_to_tool_delta_idx: HashMap::new(),
    };

    let transformed_stream = stream.scan(initial_state, move |state, item| {
        let model = model.clone();
        let mut state = state.clone();
        async move {
            match item {
                Ok(MessageCreateStreamResponse::MessageStart {
                    message:
                        MessageStartMessage {
                            id: inner_id,
                            role: inner_role,
                            usage: inner_usage,
                            ..
                        },
                }) => {
                    state.role = MessageRole::from_opt(&inner_role);
                    state.id = Some(inner_id);
                    state.usage = Some(CompletionUsage {
                        prompt_tokens: inner_usage.input_tokens,
                        completion_tokens: inner_usage.output_tokens,
                        total_tokens: inner_usage.input_tokens + inner_usage.output_tokens,
                    });
                    let response = create_stream_response(
                        &state.id.clone().unwrap_or_default(),
                        &model,
                        None,
                        None,
                    );
                    Some(response)
                }
                Ok(MessageCreateStreamResponse::ContentBlockStart {
                    index,
                    content_block,
                }) => {
                    if let ContentBlock::ToolUse(t) = &content_block {
                        state.tool_id_to_content_block.insert(index, t.clone());
                        state.tool_id_to_tool_delta_idx.insert(index, 0);
                    };
                    let response = create_stream_response(
                        &state.id.clone().unwrap_or_default(),
                        &model,
                        None,
                        Some(ChatChoiceStream {
                            index: 0,
                            delta: content_block.into_completion(),
                            finish_reason: None,
                            logprobs: None,
                        }),
                    );
                    Some(response)
                }
                Ok(MessageCreateStreamResponse::ContentBlockDelta { index, delta }) => {
                    let tool_idx = *state.tool_id_to_tool_delta_idx.get(&index).unwrap_or(&0);
                    state.tool_id_to_tool_delta_idx.insert(index, tool_idx + 1);

                    let response = create_stream_response(
                        &state.id.clone().unwrap_or_default(),
                        &model,
                        None,
                        Some(ChatChoiceStream {
                            index: 0,
                            logprobs: None,
                            finish_reason: None,
                            delta: delta.into_completion(
                                &state.role,
                                state
                                    .tool_id_to_content_block
                                    .get(&index)
                                    .map(|b| (tool_idx, b.clone())),
                            ),
                        }),
                    );
                    Some(response)
                }
                Ok(MessageCreateStreamResponse::MessageDelta {
                    delta: MessageDelta { stop_reason, .. },
                    usage: inner_usage,
                }) => {
                    // Update usage
                    if let Some(ref mut u) = state.usage {
                        u.prompt_tokens += inner_usage.input_tokens;
                        u.completion_tokens += inner_usage.output_tokens;
                        u.total_tokens += inner_usage.input_tokens + inner_usage.output_tokens;
                    }
                    let response = create_stream_response(
                        &state.id.clone().unwrap_or_default(),
                        &model,
                        state.usage.clone(),
                        Some(ChatChoiceStream {
                            index: 0,
                            logprobs: None,
                            finish_reason: match stop_reason {
                                Some(StopReason::EndTurn | StopReason::StopSequence) => {
                                    Some(FinishReason::Stop)
                                }
                                Some(StopReason::MaxTokens) => Some(FinishReason::Length),
                                Some(StopReason::ToolUse) => Some(FinishReason::ToolCalls),
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
                    );
                    Some(response)
                }
                Ok(MessageCreateStreamResponse::Ping) => {
                    tracing::trace!("Received a ping stream packet");
                    // Skip emitting an item
                    None
                }
                Ok(MessageCreateStreamResponse::ContentBlockStop { .. }) => {
                    tracing::trace!("Received a content block stop packet");
                    // Skip emitting an item
                    None
                }
                Ok(MessageCreateStreamResponse::MessageStop) => {
                    tracing::trace!("Received a stop stream packet");
                    // Skip emitting an item
                    None
                }
                Err(e) => {
                    tracing::debug!("Received an anthropic error stream packet: {:?}", e);
                    Some(Err(OpenAIError::ApiError(ApiError {
                        message: e.to_string(),
                        r#type: Some("AnthropicStreamError".to_string()),
                        param: None,
                        code: None,
                    })))
                }
            }
        }
    });

    Box::pin(transformed_stream)
}

/// Easy way to create stream. Reduce boiler plate. [`CreateChatCompletionStreamResponse`] has no builder pattern.
#[allow(clippy::cast_possible_truncation)]
fn create_stream_response(
    id: &str,
    model: &str,
    usage: Option<CompletionUsage>,
    choice: Option<ChatChoiceStream>,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let choices = match choice {
        Some(c) => vec![c],
        None => vec![],
    };
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
