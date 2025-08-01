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

use crate::bedrock::BedrockClient;
use crate::chat::Chat;
use crate::chat::nsql::SqlGeneration;
use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::{
    ChatChoice, ChatChoiceStream, ChatCompletionMessageToolCall,
    ChatCompletionMessageToolCallChunk, ChatCompletionRequestAssistantMessage,
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestAssistantMessageContentPart,
    ChatCompletionRequestDeveloperMessage, ChatCompletionRequestDeveloperMessageContent,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestSystemMessageContentPart, ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestToolMessageContentPart,
    ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
    ChatCompletionRequestUserMessageContentPart, ChatCompletionResponseMessage,
    ChatCompletionResponseStream, ChatCompletionStreamResponseDelta, ChatCompletionToolType,
    CompletionUsage, CreateChatCompletionRequest, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse, FinishReason, FunctionCall, FunctionCallStream,
    PromptTokensDetails, Role, Stop,
};
use async_stream::stream;
use async_trait::async_trait;
use aws_sdk_bedrockruntime::error::{BuildError, SdkError};
use aws_sdk_bedrockruntime::operation::converse::ConverseOutput;
use aws_sdk_bedrockruntime::operation::converse::builders::ConverseFluentBuilder;
use aws_sdk_bedrockruntime::operation::converse_stream::builders::ConverseStreamFluentBuilder;
use aws_sdk_bedrockruntime::primitives::event_stream::EventReceiver;
use aws_sdk_bedrockruntime::types::builders::{
    MessageBuilder, ToolResultBlockBuilder, ToolUseBlockBuilder,
};
use aws_sdk_bedrockruntime::types::error::ConverseStreamOutputError;
use aws_sdk_bedrockruntime::types::{
    ContentBlock, ContentBlockDelta as ContentBlockDeltaType, ContentBlockDeltaEvent,
    ContentBlockStart as ContentBlockStartInner, ContentBlockStartEvent, ConversationRole,
    ConverseStreamMetadataEvent, ConverseStreamOutput as ConverseStreamOutputPacket,
    GuardrailConverseContentBlock, GuardrailConverseTextBlock, InferenceConfiguration, Message,
    MessageStartEvent, MessageStopEvent, ReasoningContentBlock, ReasoningTextBlock, StopReason,
    SystemContentBlock, TokenUsage, ToolResultBlock, ToolResultContentBlock, ToolResultStatus,
    ToolUseBlock, ToolUseBlockDelta, ToolUseBlockStart,
};
use aws_smithy_types::Document;
use itertools::Itertools;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::Span;

/// [`BedrockConverse`] provides an OpenAI compatible interface (i.e. `impl Chat` ), for models on AWS bedrock that are compatible with the [Converse API](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html).
pub struct BedrockConverse {
    client: Arc<BedrockClient>,
    model_id: String,
}

impl BedrockConverse {
    pub fn new(client: Arc<BedrockClient>, model_id: String) -> Self {
        Self { client, model_id }
    }

    fn convert_non_system_messages(
        msgs: Vec<ChatCompletionRequestMessage>,
    ) -> Result<Vec<Message>, BuildError> {
        msgs.into_iter()
            .map(|m| match m {
                ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
                    content,
                    ..
                }) => MessageBuilder::default()
                    .set_content(Some(vec![ContentBlock::Text(match content {
                        ChatCompletionRequestUserMessageContent::Text(s) => s.clone(),
                        ChatCompletionRequestUserMessageContent::Array(arr) => arr
                            .into_iter()
                            .filter_map(|p| match p {
                                ChatCompletionRequestUserMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) => Some(text.clone()),
                                _ => None,
                            })
                            .join(""),
                    })]))
                    .set_role(Some(ConversationRole::User))
                    .build(),
                ChatCompletionRequestMessage::Assistant(
                    ChatCompletionRequestAssistantMessage {
                        content,
                        tool_calls,
                        ..
                    },
                ) => {
                    let mut message_content = vec![];
                    let text_content: Option<String> = match content {
                        Some(ChatCompletionRequestAssistantMessageContent::Text(s)) => {
                            Some(s.clone())
                        }
                        Some(ChatCompletionRequestAssistantMessageContent::Array(arr)) => arr
                            .into_iter()
                            .filter_map(|p| match p {
                                ChatCompletionRequestAssistantMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) => Some(text.clone()),
                                _ => None,
                            })
                            .join("")
                            .into(),
                        None => None,
                    };

                    let mut tool_content = tool_calls.as_ref().map(|tools| {
                        tools
                            .into_iter()
                            .filter_map(|t| {
                                let ChatCompletionMessageToolCall {
                                    id,
                                    function: FunctionCall { name, arguments },
                                    ..
                                } = t;

                                Some(ContentBlock::ToolUse(
                                    ToolUseBlockBuilder::default()
                                        .set_tool_use_id(Some(id.clone()))
                                        .set_name(Some(name.clone()))
                                        .set_input(Some(Document::String(arguments.clone())))
                                        .build()
                                        .ok()?,
                                ))
                            })
                            .collect::<Vec<_>>()
                    });
                    if let Some(mut messages) = tool_content.as_mut() {
                        message_content.append(&mut messages);
                    };

                    if let Some(text) = text_content {
                        message_content.push(ContentBlock::Text(text));
                    };

                    MessageBuilder::default()
                        .set_content(Some(message_content))
                        .set_role(Some(ConversationRole::Assistant))
                        .build()
                }
                ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                    content,
                    tool_call_id,
                }) => {
                    let block_content = match content {
                        ChatCompletionRequestToolMessageContent::Text(t) => {
                            vec![ToolResultContentBlock::Text(t.clone())]
                        }
                        ChatCompletionRequestToolMessageContent::Array(arr) => arr
                            .into_iter()
                            .map(|s| {
                                let ChatCompletionRequestToolMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) = s;
                                ToolResultContentBlock::Text(text.clone())
                            })
                            .collect(),
                    };
                    MessageBuilder::default()
                        .set_content(
                            ToolResultBlockBuilder::default()
                                .set_content(Some(block_content))
                                .set_tool_use_id(Some(tool_call_id.clone()))
                                .set_status(Some(ToolResultStatus::Success))
                                .build()
                                .ok()
                                .map(|b| vec![ContentBlock::ToolResult(b)]),
                        )
                        .set_role(Some(ConversationRole::User))
                        .build()
                }
                _ => Err(BuildError::invalid_field(
                    "role",
                    // Unreachable, but return understandable error.
                    "unreachable error: cannot reprocess system prompt as messages",
                )),
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn convert_system_messages(msgs: Vec<ChatCompletionRequestMessage>) -> Vec<SystemContentBlock> {
        msgs.into_iter()
            .flat_map(|m| match m {
                ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                    content: ChatCompletionRequestSystemMessageContent::Text(s),
                    name: _,
                }) => vec![SystemContentBlock::Text(s.to_string())],
                ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                    content: ChatCompletionRequestSystemMessageContent::Array(arr),
                    name: _,
                }) => arr
                    .into_iter()
                    .map(|s| match s {
                        ChatCompletionRequestSystemMessageContentPart::Text(
                            ChatCompletionRequestMessageContentPartText { text },
                        ) => SystemContentBlock::Text(text),
                    })
                    .collect(),
                ChatCompletionRequestMessage::Developer(
                    ChatCompletionRequestDeveloperMessage {
                        content: ChatCompletionRequestDeveloperMessageContent::Text(s),
                        name: _,
                    },
                ) => vec![SystemContentBlock::Text(s.to_string())],
                ChatCompletionRequestMessage::Developer(
                    ChatCompletionRequestDeveloperMessage {
                        content: ChatCompletionRequestDeveloperMessageContent::Array(arr),
                        name: _,
                    },
                ) => arr
                    .into_iter()
                    .map(|s| {
                        let ChatCompletionRequestMessageContentPartText { text } = s;
                        SystemContentBlock::Text(text)
                    })
                    .collect(),
                _ => vec![],
            })
            .collect()
    }

    fn inference_cfg(req: &CreateChatCompletionRequest) -> InferenceConfiguration {
        InferenceConfiguration::builder()
            .set_max_tokens(
                req.max_completion_tokens
                    .or(req.max_tokens)
                    .map(|u| u as i32),
            )
            .set_stop_sequences(req.stop.as_ref().map(|stop| match stop {
                Stop::String(s) => vec![s.clone()],
                Stop::StringArray(arr) => arr.clone(),
            }))
            .set_temperature(req.temperature)
            .set_top_p(req.top_p)
            .build()
    }

    #[allow(clippy::deprecated)]
    fn to_converse_stream(
        &self,
        client: Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseStreamFluentBuilder, OpenAIError> {
        let inf_cfg = Self::inference_cfg(&req);
        let CreateChatCompletionRequest {
            messages, metadata, ..
        } = req;

        let (system, messages): (
            Vec<ChatCompletionRequestMessage>,
            Vec<ChatCompletionRequestMessage>,
        ) = messages.into_iter().partition(|m| {
            matches!(
                m,
                ChatCompletionRequestMessage::System(_)
                    | ChatCompletionRequestMessage::Developer(_)
            )
        });

        let system = Self::convert_system_messages(system);
        let messages =
            Self::convert_non_system_messages(messages).map_err(|e| to_api_error(e.to_string()))?;

        let mut bldr = client
            .client
            .converse_stream()
            .model_id(self.model_id.clone())
            .set_messages(Some(messages))
            .inference_config(inf_cfg)
            .set_system(Some(system));

        if let Some(Value::Object(m)) = metadata {
            bldr = bldr.set_request_metadata(Some(
                m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
            ));
        };

        // pub tools: Option<Vec<ChatCompletionTool>>,
        // pub tool_choice: Option<ChatCompletionToolChoiceOption>,
        Ok(bldr)
    }

    #[allow(clippy::deprecated)]
    fn to_converse(
        &self,
        client: Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseFluentBuilder, OpenAIError> {
        let inf_cfg = Self::inference_cfg(&req);
        let CreateChatCompletionRequest {
            messages, metadata, ..
        } = req;

        let (system, messages): (
            Vec<ChatCompletionRequestMessage>,
            Vec<ChatCompletionRequestMessage>,
        ) = messages.into_iter().partition(|m| {
            matches!(
                m,
                ChatCompletionRequestMessage::System(_)
                    | ChatCompletionRequestMessage::Developer(_)
            )
        });

        let system = Self::convert_system_messages(system);
        let messages =
            Self::convert_non_system_messages(messages).map_err(|e| to_api_error(e.to_string()))?;

        let mut bldr = client
            .client
            .converse()
            .model_id(self.model_id.clone())
            .set_messages(Some(messages))
            .inference_config(inf_cfg)
            .set_system(Some(system));

        if let Some(Value::Object(m)) = metadata {
            bldr = bldr.set_request_metadata(Some(
                m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
            ));
        };

        // pub tools: Option<Vec<ChatCompletionTool>>,
        // pub tool_choice: Option<ChatCompletionToolChoiceOption>,
        Ok(bldr)
    }

    fn from_converse_output(
        &self,
        output: ConverseOutput,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let usage = output.usage().map(|u| CompletionUsage {
            completion_tokens: u.output_tokens as u32,
            prompt_tokens: u.input_tokens as u32,
            total_tokens: u.total_tokens as u32,
            prompt_tokens_details: Some(PromptTokensDetails {
                cached_tokens: u.cache_read_input_tokens.map(|i| i as u32),
                audio_tokens: None,
            }),
            completion_tokens_details: None,
        });

        let Some(choices) = output
            .output
            .map(|o| {
                let Message { role, content, .. } =
                    o.as_message().map_err(|e| to_api_error(format!("{e:?}")))?;

                let data: Vec<(
                    (Option<String>, Option<String>),
                    Option<ChatCompletionMessageToolCall>,
                )> = content
                    .iter()
                    .map(extract_from_content_block)
                    .collect::<Result<Vec<_>, _>>()?;
                let (content_and_refusal, tool_calls): (Vec<_>, Vec<_>) = data.into_iter().unzip();
                let (content, refusals): (Vec<_>, Vec<_>) = content_and_refusal.into_iter().unzip();

                Ok::<_, OpenAIError>(ChatChoice {
                    index: 0,
                    message: ChatCompletionResponseMessage {
                        content: Some(content.into_iter().flatten().join("\n")),
                        refusal: Some(refusals.into_iter().flatten().join("\n")),
                        tool_calls: Some(tool_calls.into_iter().flatten().collect()),
                        role: try_convert_role(role)?,
                        function_call: None,
                        audio: None,
                    },
                    logprobs: None,
                    finish_reason: Some(try_convert_finish_reason(&output.stop_reason)?),
                })
            })
            .transpose()?
        else {
            return Err(to_api_error(
                "No outputs received from AWS bedrock converse API",
            ));
        };

        Ok(CreateChatCompletionResponse {
            usage,
            id: Span::current()
                .id()
                .map(|id| id.into_u64().to_string())
                .unwrap_or_default(),
            choices: vec![choices],
            created: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
                .as_secs() as u32,
            model: self.model_id.clone(),
            service_tier: None,
            system_fingerprint: None,
            object: "chat.completion".to_string(),
        })
    }

    async fn process_stream(
        model: String,
        mut input_stream: EventReceiver<ConverseStreamOutputPacket, ConverseStreamOutputError>,
    ) -> ChatCompletionResponseStream {
        #[derive(Clone, Default)]
        struct StreamState {
            id: String,
            role: Option<Role>,
            content_block_index_to_tool_details: HashMap<i32, ToolUseBlockStart>,
            content_block_index_to_delta_idx: HashMap<i32, u32>,
        }

        let s = stream! {
            let mut state = StreamState {
                id: Span::current()
                    .id()
                    .map(|id| id.into_u64().to_string())
                    .unwrap_or_default(),
                ..StreamState::default()
            };
            while let result = input_stream.recv().await {
                match result {
                    Err(SdkError::ServiceError(e)) => {
                        match &e.err() {
                            &ConverseStreamOutputError::InternalServerException(e) => {
                                yield Err(to_api_error(e.to_string()));
                                break;
                            },
                            ee => {
                                // TODO specialise
                                yield Err(to_api_error(ee.to_string()));
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        // TODO specialise
                        yield Err(to_api_error(e.to_string()));
                        break;
                    }
                    Ok(None) => {
                        break;
                    }
                    Ok(Some(pkt)) => {
                        match pkt {
                            ConverseStreamOutputPacket::MessageStart(MessageStartEvent{role,..}) => {
                                state.role = Some(try_convert_role(&role)?);
                            },
                            ConverseStreamOutputPacket::ContentBlockStart(ContentBlockStartEvent{
                                start: Some(ContentBlockStartInner::ToolUse(ToolUseBlockStart{tool_use_id, name,..})), content_block_index,.. }) => {
                                // add tools to incremenetal
                                // emit incremental content.

                            },
                            ConverseStreamOutputPacket::ContentBlockDelta(ContentBlockDeltaEvent{ delta: Some(ContentBlockDeltaType::Text(text)), content_block_index, ..}) => {
                                match chat_completion_stream(
                                    model.clone(),
                                    vec![chat_choice_stream(Some(text), None, state.role.clone(), None, None)],
                                    None,
                                ) {
                                    Ok(s) => yield Ok(s),
                                    Err(e) => {yield Err(e); break}
                                };
                            },
                            ConverseStreamOutputPacket::ContentBlockDelta(ContentBlockDeltaEvent{ delta: Some(ContentBlockDeltaType::ToolUse(ToolUseBlockDelta{input,..})), content_block_index, ..}) => {
                                let tool_delta_idx = state.content_block_index_to_delta_idx.get(&content_block_index).unwrap_or(&0);

                                if let Some(ToolUseBlockStart{tool_use_id, name: _,..}) = state.content_block_index_to_tool_details.get(&content_block_index) {

                                    match chat_completion_stream(
                                        model.clone(),
                                        vec![chat_choice_stream(None, Some(vec![ChatCompletionMessageToolCallChunk{
                                            index: *tool_delta_idx,
                                            id: Some(tool_use_id.clone()),
                                            r#type: Some(ChatCompletionToolType::Function),
                                            function: None
                                        }]), state.role.clone(), None, None)],
                                        None,
                                    ) {
                                        Ok(s) => yield Ok(s),
                                        Err(e) => {yield Err(e); break}
                                    };

                                } else {
                                    yield Err(to_api_error(format!("Invalid stream from Bedrock Converse API. Tool use delta received before starting packet")));
                                    break;
                                };
                                state.content_block_index_to_delta_idx.insert(content_block_index, tool_delta_idx + 1);
                            },
                            ConverseStreamOutputPacket::MessageStop(MessageStopEvent{ stop_reason,.. }) => {
                                let finish_reason = match try_convert_finish_reason(&stop_reason) {
                                    Ok(r) => r,
                                    Err(e) => {
                                        yield Err(e);
                                        break;
                                    }
                                };
                                match chat_completion_stream(
                                    model.clone(),
                                    vec![chat_choice_stream(None, None, state.role.clone(), None, Some(finish_reason))],
                                    None,
                                ) {
                                    Ok(s) => yield Ok(s),
                                    Err(e) => {yield Err(e); break}
                                };
                            },
                            ConverseStreamOutputPacket::Metadata(ConverseStreamMetadataEvent{usage: Some(usage), ..}) => {
                                match chat_completion_stream(
                                    model.clone(), vec![], Some(convert_usage(&usage))
                                ) {
                                    Ok(s) => yield Ok(s),
                                    Err(e) => {yield Err(e); break}
                                };
                            },
                            ConverseStreamOutputPacket::ContentBlockStop(_) => {
                                // No action needed for content block stop
                            },
                            unknown => {
                                yield Err(to_api_error(format!("Unknown event from Bedrock stream: {unknown:?}")));
                            }
                        }
                    }
                }
            }
        };
        Box::pin(s)
    }
}

#[async_trait]
impl Chat for BedrockConverse {
    #[allow(deprecated)]
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let input = self.to_converse_stream(self.client.clone(), req)?;
        let output = self
            .client
            .do_converse_stream(input)
            .await
            .map_err(|e| to_api_error(e.to_string()))?;

        Ok(Self::process_stream(self.model_id.clone(), output.stream).await)
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let input = self.to_converse(self.client.clone(), req)?;
        let output = self
            .client
            .do_converse(input)
            .await
            .map_err(|e| to_api_error(e.to_string()))?;

        self.from_converse_output(output)
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}

fn try_convert_role(role: &ConversationRole) -> Result<Role, OpenAIError> {
    match role {
        ConversationRole::Assistant => Ok(Role::Assistant),
        ConversationRole::User => Ok(Role::User),
        unknown_role => {
            return Err(to_api_error(format!(
                "Unknown role returned from AWS bedrock: {unknown_role:?}"
            )));
        }
    }
}

fn try_convert_finish_reason(stop_reason: &StopReason) -> Result<FinishReason, OpenAIError> {
    let finish_reason = match stop_reason {
        StopReason::MaxTokens => FinishReason::Length,
        StopReason::ContentFiltered | StopReason::GuardrailIntervened => {
            FinishReason::ContentFilter
        }
        StopReason::EndTurn | StopReason::StopSequence => FinishReason::Stop,
        StopReason::ToolUse => FinishReason::ToolCalls,
        reason => {
            return Err(to_api_error(format!(
                "Unknown finish reason returned from AWS bedrock: '{reason}'."
            )));
        }
    };
    Ok(finish_reason)
}

fn to_api_error(err: impl Into<String>) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message: err.into(),
        r#type: None,
        param: None,
        code: None,
    })
}

/// Extract the content, refusal and tool calls from a ContentBlock.
fn extract_from_content_block(
    blck: &ContentBlock,
) -> Result<
    (
        (Option<String>, Option<String>),
        Option<ChatCompletionMessageToolCall>,
    ),
    OpenAIError,
> {
    match blck {
        ContentBlock::GuardContent(GuardrailConverseContentBlock::Text(
            GuardrailConverseTextBlock { text, .. },
        ))
        | ContentBlock::ReasoningContent(ReasoningContentBlock::ReasoningText(
            ReasoningTextBlock { text, .. },
        ))
        | ContentBlock::Text(text) => Ok(((Some(text.clone()), None), None)),

        ContentBlock::ToolResult(ToolResultBlock {
            tool_use_id,
            content,
            status,
            ..
        }) => Ok(((None, None), None)),
        ContentBlock::ToolUse(ToolUseBlock {
            tool_use_id,
            name,
            input,
            ..
        }) => {
            let input: &Document = input;

            Ok((
                (None, None),
                Some(ChatCompletionMessageToolCall {
                    id: tool_use_id.clone(),
                    r#type: ChatCompletionToolType::Function,
                    function: FunctionCall {
                        name: name.clone(),
                        arguments: serde_json::to_string(input).unwrap_or_default(),
                    },
                }),
            ))
        }
        unsupported_block => Err(to_api_error(format!("{unsupported_block:?}"))),
    }
}

fn chat_completion_stream(
    model: String,
    choices: Vec<ChatChoiceStream>,
    usage: Option<CompletionUsage>,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    Ok(CreateChatCompletionStreamResponse {
        choices,
        id: Span::current()
            .id()
            .map(|id| id.into_u64().to_string())
            .unwrap_or_default(),
        created: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .as_secs() as u32,
        model,
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage,
    })
}

fn chat_choice_stream(
    content: Option<String>,
    tool_calls: Option<Vec<ChatCompletionMessageToolCallChunk>>,
    role: Option<Role>,
    refusal: Option<String>,
    finish_reason: Option<FinishReason>,
) -> ChatChoiceStream {
    ChatChoiceStream {
        index: 0,
        delta: ChatCompletionStreamResponseDelta {
            content,
            function_call: None,
            tool_calls,
            role,
            refusal,
        },
        finish_reason,
        logprobs: None,
    }
}

fn convert_usage(usage: &TokenUsage) -> CompletionUsage {
    let TokenUsage {
        input_tokens,
        output_tokens,
        total_tokens,
        cache_read_input_tokens,
        ..
    } = usage;
    CompletionUsage {
        prompt_tokens: *input_tokens as u32,
        completion_tokens: *output_tokens as u32,
        total_tokens: *total_tokens as u32,
        prompt_tokens_details: cache_read_input_tokens.map(|t| PromptTokensDetails {
            cached_tokens: Some(t as u32),
            audio_tokens: None,
        }),
        completion_tokens_details: None,
    }
}
