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
pub mod guardrail;
mod reasoning;
pub(super) mod util;

use crate::bedrock::BedrockClient;
use crate::bedrock::api_error;
use crate::bedrock::chat::guardrail::GuardRail;
use crate::bedrock::chat::reasoning::{ReasoningControl, ReasoningRequest};
use crate::bedrock::chat::util::{
    ResponseParts, chat_choice_stream, convert_usage, into_fallible_stream, to_api_error,
    tool_config, try_convert_finish_reason, try_convert_role, value_to_document,
};
use crate::bedrock::model_id::ModelId;
use crate::chat::Chat;
use crate::chat::nsql::SqlGeneration;
use crate::streaming_utils::{create_stream_response, generate_stream_id};
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatChoice, ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionMessageToolCalls, ChatCompletionRequestAssistantMessage,
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestAssistantMessageContentPart,
    ChatCompletionRequestDeveloperMessage, ChatCompletionRequestDeveloperMessageContent,
    ChatCompletionRequestDeveloperMessageContentPart, ChatCompletionRequestMessage,
    ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestSystemMessageContentPart,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionRequestToolMessageContentPart, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
    ChatCompletionResponseMessage, ChatCompletionResponseStream, ChatCompletionTools,
    CreateChatCompletionRequest, CreateChatCompletionResponse, CreateChatCompletionStreamResponse,
    FunctionCall, FunctionCallStream, FunctionType, ResponseFormat, ResponseFormatJsonSchema, Role,
    StopConfiguration,
};
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
    CachePointBlock, CachePointType, ContentBlock, ContentBlockDelta as ContentBlockDeltaType,
    ContentBlockDeltaEvent, ContentBlockStart as ContentBlockStartInner, ContentBlockStartEvent,
    ConversationRole, ConverseStreamMetadataEvent,
    ConverseStreamOutput as ConverseStreamOutputPacket, GuardrailConfiguration,
    GuardrailStreamConfiguration, InferenceConfiguration, JsonSchemaDefinition, Message,
    MessageStartEvent, MessageStopEvent, OutputConfig, OutputFormat, OutputFormatStructure,
    OutputFormatType, SystemContentBlock, ToolResultContentBlock, ToolResultStatus,
    ToolUseBlockDelta, ToolUseBlockStart,
};
use aws_smithy_types::Document;
use futures::stream::StreamExt;
use itertools::Itertools;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// [`BedrockConverse`] provides an `OpenAI` compatible interface (i.e. `impl Chat` ), for models on AWS bedrock that are compatible with the [Converse API](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html).
pub struct BedrockConverse {
    client: Arc<BedrockClient>,
    model_id: ModelId,
    guardrail: Option<GuardRail>,
}

impl BedrockConverse {
    #[must_use]
    pub fn new(client: Arc<BedrockClient>, model_id: String) -> Self {
        Self {
            client,
            model_id: ModelId::from(model_id),
            guardrail: None,
        }
    }

    #[must_use]
    pub fn with_guardrail(mut self, g: GuardRail) -> Self {
        self.guardrail = Some(g);
        self
    }

    /// Alter the  [`CreateChatCompletionRequest`] in a way that Bedrock understands.
    /// Instead of performing these changes in the underlying conversion (i.e. within Converse
    /// APIs), we do it here for consistency with other model providers.
    fn alter_request(&self, mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        self.model_id.as_str().clone_into(&mut req.model);
        // Bedrock should set Option::None parameters to a schema with no inputs, but doesn't.
        // Must be done explicitly.
        if let Some(ref mut tools) = req.tools {
            for t in tools.iter_mut() {
                if let ChatCompletionTools::Function(tool) = t
                    && tool.function.parameters.is_none()
                {
                    tool.function.parameters.replace(json!(
                        {
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "properties": {},
                            "required": [],
                            "title": "",
                            "type": "object"
                        }
                    ));
                }
            }
        }
        req
    }

    /// Convert [`ChatCompletionRequestMessage`] that are neither [`ChatCompletionRequestMessage::System`] or [`ChatCompletionRequestMessage::Developer`] into the Bedrock equivalent [`Message`] format.
    ///
    /// Other enum variants will be ignored.
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
                        ChatCompletionRequestUserMessageContent::Text(s) => s,
                        ChatCompletionRequestUserMessageContent::Array(arr) => arr
                            .into_iter()
                            .filter_map(|p| match p {
                                ChatCompletionRequestUserMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) => Some(text),
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
                        Some(ChatCompletionRequestAssistantMessageContent::Text(s)) => Some(s),
                        Some(ChatCompletionRequestAssistantMessageContent::Array(arr)) => arr
                            .into_iter()
                            .filter_map(|p| match p {
                                ChatCompletionRequestAssistantMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) => Some(text),
                                ChatCompletionRequestAssistantMessageContentPart::Refusal(_) => {
                                    None
                                }
                            })
                            .join("")
                            .into(),
                        None => None,
                    };

                    let mut tool_content = tool_calls.as_ref().map(|tools| {
                        tools
                            .iter()
                            .filter_map(|t| {
                                let ChatCompletionMessageToolCalls::Function(
                                    ChatCompletionMessageToolCall {
                                        id,
                                        function: FunctionCall { name, arguments },
                                        ..
                                    },
                                ) = t
                                else {
                                    return None;
                                };

                                let tool_input = serde_json::from_str(arguments).ok().map_or(
                                    Document::Object(HashMap::default()),
                                    value_to_document,
                                );
                                Some(ContentBlock::ToolUse(
                                    ToolUseBlockBuilder::default()
                                        .set_tool_use_id(Some(id.clone()))
                                        .set_name(Some(name.clone()))
                                        .set_input(Some(tool_input))
                                        .build()
                                        .ok()?,
                                ))
                            })
                            .collect::<Vec<_>>()
                    });
                    if let Some(messages) = tool_content.as_mut() {
                        message_content.append(messages);
                    }

                    if let Some(text) = text_content {
                        message_content.push(ContentBlock::Text(text));
                    }

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
                            vec![ToolResultContentBlock::Text(t)]
                        }
                        ChatCompletionRequestToolMessageContent::Array(arr) => arr
                            .into_iter()
                            .map(|s| {
                                let ChatCompletionRequestToolMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) = s;
                                ToolResultContentBlock::Text(text)
                            })
                            .collect(),
                    };
                    MessageBuilder::default()
                        .set_content(
                            ToolResultBlockBuilder::default()
                                .set_content(Some(block_content))
                                .set_tool_use_id(Some(tool_call_id))
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
            .map(Self::merge_consecutive_tool_result_messages)?
    }

    /// Bedrock requires all `ToolResult` blocks for a multi-tool-use assistant turn to be
    /// in a single `User` message. `OpenAI` sends each tool result as a separate `Tool` message,
    /// which we convert into separate `User` messages. This function merges consecutive `User`
    /// messages that contain only `ToolResult` content blocks into a single `User` message.
    fn merge_consecutive_tool_result_messages(
        messages: Vec<Message>,
    ) -> Result<Vec<Message>, BuildError> {
        let mut merged: Vec<Message> = Vec::with_capacity(messages.len());

        for msg in messages {
            let is_tool_result_user_msg = msg.role == ConversationRole::User
                && msg
                    .content
                    .iter()
                    .all(|c| matches!(c, ContentBlock::ToolResult(_)));

            if is_tool_result_user_msg {
                // Check if the previous message is also a tool-result-only User message
                let should_merge = merged.last().is_some_and(|prev| {
                    prev.role == ConversationRole::User
                        && prev
                            .content
                            .iter()
                            .all(|c| matches!(c, ContentBlock::ToolResult(_)))
                });

                if should_merge {
                    // Merge into the previous message
                    if let Some(prev) = merged.last_mut() {
                        let mut combined = prev.content.clone();
                        combined.extend(msg.content);
                        *prev = MessageBuilder::default()
                            .set_content(Some(combined))
                            .set_role(Some(ConversationRole::User))
                            .build()?;
                    }
                } else {
                    merged.push(msg);
                }
            } else {
                merged.push(msg);
            }
        }

        Ok(merged)
    }

    /// Convert [`ChatCompletionRequestMessage`] that are [`ChatCompletionRequestMessage::System`] or [`ChatCompletionRequestMessage::Developer`] into the Bedrock equivalent [`SystemContentBlock`] format.
    ///
    /// Other enum variants will be ignored.
    fn convert_system_messages(msgs: Vec<ChatCompletionRequestMessage>) -> Vec<SystemContentBlock> {
        msgs.into_iter()
            .flat_map(|m| match m {
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
                )
                | ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                    content: ChatCompletionRequestSystemMessageContent::Text(s),
                    name: _,
                }) => vec![SystemContentBlock::Text(s)],
                ChatCompletionRequestMessage::Developer(
                    ChatCompletionRequestDeveloperMessage {
                        content: ChatCompletionRequestDeveloperMessageContent::Array(arr),
                        name: _,
                    },
                ) => arr
                    .into_iter()
                    .map(|s| {
                        let ChatCompletionRequestDeveloperMessageContentPart::Text(
                            ChatCompletionRequestMessageContentPartText { text },
                        ) = s;
                        SystemContentBlock::Text(text)
                    })
                    .collect(),
                _ => vec![],
            })
            .collect()
    }

    fn prompt_cache_point() -> Result<CachePointBlock, BuildError> {
        CachePointBlock::builder()
            .r#type(CachePointType::Default)
            .build()
    }

    fn add_prompt_cache_point(
        system: &mut Vec<SystemContentBlock>,
        messages: &mut [Message],
    ) -> Result<(), BuildError> {
        let cache_point = Self::prompt_cache_point()?;
        if let Some(last_message) = messages.last_mut() {
            last_message
                .content
                .push(ContentBlock::CachePoint(cache_point));
        } else {
            system.push(SystemContentBlock::CachePoint(cache_point));
        }
        Ok(())
    }

    fn output_config(
        response_format: Option<ResponseFormat>,
    ) -> Result<Option<OutputConfig>, OpenAIError> {
        match response_format {
            Some(ResponseFormat::JsonObject) => Err(to_api_error(
                "Bedrock does not support 'response_format.type: json_object', only 'json_schema'.",
            )),
            Some(ResponseFormat::JsonSchema {
                json_schema:
                    ResponseFormatJsonSchema {
                        name,
                        schema,
                        description,
                        strict: _,
                    },
            }) => Ok(Some(
                OutputConfig::builder()
                    .text_format(
                        OutputFormat::builder()
                            .r#type(OutputFormatType::JsonSchema)
                            .structure(OutputFormatStructure::JsonSchema(
                                JsonSchemaDefinition::builder()
                                    .set_schema(Some(schema.to_string()))
                                    .name(name)
                                    .set_description(description)
                                    .build()
                                    .map_err(|e| to_api_error(e.to_string()))?,
                            ))
                            .build()
                            .map_err(|e| to_api_error(e.to_string()))?,
                    )
                    .build(),
            )),
            _ => Ok(None),
        }
    }

    #[expect(deprecated)]
    fn max_tokens(req: &CreateChatCompletionRequest) -> Option<u32> {
        req.max_completion_tokens.or(req.max_tokens)
    }

    /// The reasoning settings for `req`'s `reasoning_effort`, in the shape this model takes.
    fn reasoning_request(
        &self,
        req: &CreateChatCompletionRequest,
    ) -> Result<Option<ReasoningRequest>, OpenAIError> {
        ReasoningControl::for_model(&self.model_id).request(
            &self.model_id,
            req.reasoning_effort.as_ref(),
            Self::max_tokens(req),
        )
    }

    /// `reasoning` replaces the request's `max_tokens` when its thinking budget needs room.
    #[expect(clippy::cast_possible_wrap)]
    fn inference_cfg(
        req: &CreateChatCompletionRequest,
        reasoning: Option<&ReasoningRequest>,
    ) -> InferenceConfiguration {
        let max_tokens = match reasoning {
            Some(r) => r.max_tokens,
            None => Self::max_tokens(req),
        };
        InferenceConfiguration::builder()
            .set_max_tokens(max_tokens.map(|u| u as i32))
            .set_stop_sequences(req.stop.as_ref().map(|stop| match stop {
                StopConfiguration::String(s) => vec![s.clone()],
                StopConfiguration::StringArray(arr) => arr.clone(),
            }))
            .set_temperature(req.temperature)
            .set_top_p(req.top_p)
            .build()
    }

    fn to_converse_stream(
        &self,
        client: &Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseStreamFluentBuilder, OpenAIError> {
        let reasoning = self.reasoning_request(&req)?;
        let inf_cfg = Self::inference_cfg(&req, reasoning.as_ref());
        let CreateChatCompletionRequest {
            messages,
            metadata,
            tool_choice,
            tools,
            response_format,
            prompt_cache_key,
            ..
        } = req;

        // Split system and regular messages. They are separate properties in Bedrock.
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

        let mut system = Self::convert_system_messages(system);
        let mut messages =
            Self::convert_non_system_messages(messages).map_err(|e| to_api_error(e.to_string()))?;
        if prompt_cache_key.is_some() {
            Self::add_prompt_cache_point(&mut system, &mut messages)
                .map_err(|e| to_api_error(e.to_string()))?;
        }

        let guardrails: Option<GuardrailStreamConfiguration> = self
            .guardrail
            .as_ref()
            .map(std::convert::TryInto::try_into)
            .transpose()
            .map_err(|e: BuildError| to_api_error(e.to_string()))?;

        let mut bldr = client
            .client
            .converse_stream()
            .model_id(self.model_id.as_str())
            .set_messages(Some(messages))
            .inference_config(inf_cfg)
            .set_system(Some(system))
            .set_guardrail_config(guardrails)
            .set_output_config(Self::output_config(response_format)?)
            .set_tool_config(tool_config(tools, tool_choice))
            .set_additional_model_request_fields(
                reasoning.map(|r| r.additional_model_request_fields),
            );

        if let Some(metadata) = metadata {
            // Metadata is a newtype around serde_json::Value - convert and extract object
            if let Ok(Value::Object(m)) = serde_json::to_value(&metadata) {
                bldr = bldr.set_request_metadata(Some(
                    m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
                ));
            }
        }

        Ok(bldr)
    }

    fn to_converse(
        &self,
        client: &Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseFluentBuilder, OpenAIError> {
        let reasoning = self.reasoning_request(&req)?;
        let inf_cfg = Self::inference_cfg(&req, reasoning.as_ref());
        let CreateChatCompletionRequest {
            messages,
            metadata,
            tools,
            tool_choice,
            response_format,
            prompt_cache_key,
            ..
        } = req;

        // Split system and regular messages. They are separate properties in Bedrock.
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

        let mut system = Self::convert_system_messages(system);
        let mut messages =
            Self::convert_non_system_messages(messages).map_err(|e| to_api_error(e.to_string()))?;
        if prompt_cache_key.is_some() {
            Self::add_prompt_cache_point(&mut system, &mut messages)
                .map_err(|e| to_api_error(e.to_string()))?;
        }

        let guardrails: Option<GuardrailConfiguration> = self
            .guardrail
            .as_ref()
            .map(std::convert::TryInto::try_into)
            .transpose()
            .map_err(|e: BuildError| to_api_error(e.to_string()))?;

        let mut bldr = client
            .client
            .converse()
            .model_id(self.model_id.as_str())
            .set_messages(Some(messages))
            .inference_config(inf_cfg)
            .set_system(Some(system))
            .set_guardrail_config(guardrails)
            .set_output_config(Self::output_config(response_format)?)
            .set_tool_config(tool_config(tools, tool_choice))
            .set_additional_model_request_fields(
                reasoning.map(|r| r.additional_model_request_fields),
            );

        if let Some(metadata) = metadata {
            // Metadata is a newtype around serde_json::Value - convert and extract object
            if let Ok(Value::Object(m)) = serde_json::to_value(&metadata) {
                bldr = bldr.set_request_metadata(Some(
                    m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
                ));
            }
        }

        Ok(bldr)
    }

    #[expect(clippy::cast_possible_truncation, deprecated)]
    fn convert_converse_output(
        &self,
        output: ConverseOutput,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let usage = output.usage().map(convert_usage);

        let Some(choices) = output
            .output
            .map(|o| {
                let Message {
                    role,
                    content: blocks,
                    ..
                } = o.as_message().map_err(|e| to_api_error(format!("{e:?}")))?;

                let ResponseParts {
                    content,
                    reasoning,
                    tool_calls,
                } = ResponseParts::try_from_blocks(blocks)?;

                let tool_calls_enum: Vec<ChatCompletionMessageToolCalls> = tool_calls
                    .into_iter()
                    .map(ChatCompletionMessageToolCalls::Function)
                    .collect();

                Ok::<_, OpenAIError>(ChatChoice {
                    index: 0,
                    message: ChatCompletionResponseMessage {
                        reasoning_content: (!reasoning.is_empty()).then(|| reasoning.join("\n")),
                        content: Some(content.join("\n")),
                        // Converse has no refusal: a guardrail or content filter that stops the
                        // reply is reported through `finish_reason`.
                        refusal: None,
                        tool_calls: if tool_calls_enum.is_empty() {
                            None
                        } else {
                            Some(tool_calls_enum)
                        },
                        annotations: None,
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
            id: generate_stream_id(self.model_id.as_str()),
            choices: vec![choices],
            created: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
                .as_secs() as u32,
            model: self.model_id.to_string(),
            service_tier: None,
            system_fingerprint: None,
            object: "chat.completion".to_string(),
        })
    }

    fn process_stream(
        model: &str,
        input_stream: EventReceiver<ConverseStreamOutputPacket, ConverseStreamOutputError>,
    ) -> ChatCompletionResponseStream {
        let state = Arc::new(RwLock::new(StreamState {
            id: generate_stream_id(model),
            model: model.to_string(),

            ..StreamState::default()
        }));

        Box::pin(into_fallible_stream(input_stream).filter_map(move |packet| {
            let state = Arc::clone(&state);

            async move {
               let mut state_ = state.write().await;
               let zz: Option<Result<CreateChatCompletionStreamResponse, OpenAIError>> = match packet {
                    Err(SdkError::ServiceError(e)) => {
                        match e.into_err() {
                            ConverseStreamOutputError::InternalServerException(e) => {
                                Some(Err(api_error::with_code(format!("Bedrock internal server error: {e}"), Some("InternalServerException"))))
                            }
                            ConverseStreamOutputError::ThrottlingException(e) => {
                                Some(Err(api_error::with_code(format!("Bedrock rate limit exceeded: {e}"), Some("ThrottlingException"))))
                            }
                            ConverseStreamOutputError::ValidationException(e) => {
                                Some(Err(api_error::with_code(format!("Bedrock validation error: {e}"), Some("ValidationException"))))
                            }
                            ConverseStreamOutputError::ModelStreamErrorException(e) => {
                                Some(Err(api_error::with_code(format!("Bedrock model stream error: {e}"), Some("ModelStreamErrorException"))))
                            }
                            ee => {
                                Some(Err(api_error::with_code(format!("Bedrock error: {ee}"), aws_sdk_bedrockruntime::error::ProvideErrorMetadata::code(&ee))))
                            }
                        }
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        let msg = if err_str.contains("UnrecognizedClientException")
                            || err_str.contains("InvalidSignatureException")
                            || err_str.contains("AccessDeniedException")
                            || err_str.contains("ExpiredTokenException")
                            || err_str.contains("IncompleteSignature")
                        {
                            format!("Bedrock authentication failed. Check that your AWS credentials are valid and have permission to access Bedrock. Details: {err_str}")
                        } else {
                            format!("Bedrock error: {err_str}")
                        };
                        Some(Err(to_api_error(msg)))
                    }
                    Ok(None) => None, // Natural end-of-stream.
                    Ok(Some(pkt)) => state_.convert_packet(pkt),
                };
               zz
            }
        }))
    }
}

/// Individual packets in the stream may need information from previous packets to be converted correctly to a [`CreateChatCompletionStreamResponse`]. This struct tracks all the necessary information.
#[derive(Default)]
struct StreamState {
    id: String,
    model: String,
    role: Option<Role>,
    content_block_index_to_tool_details: HashMap<i32, ToolUseBlockStart>,
    content_block_index_to_delta_idx: HashMap<i32, u32>,
}

impl StreamState {
    /// Converts one Converse stream packet into an `OpenAI` chunk, or `None` when the packet yields no chunk.
    fn convert_packet(
        &mut self,
        pkt: ConverseStreamOutputPacket,
    ) -> Option<Result<CreateChatCompletionStreamResponse, OpenAIError>> {
        match pkt {
            ConverseStreamOutputPacket::MessageStart(MessageStartEvent { role, .. }) => {
                match try_convert_role(&role) {
                    Ok(r) => {
                        self.role = Some(r);
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            ConverseStreamOutputPacket::ContentBlockStart(ContentBlockStartEvent {
                start: Some(ContentBlockStartInner::ToolUse(tool_use)),
                content_block_index,
                ..
            }) => {
                self.content_block_index_to_delta_idx
                    .insert(content_block_index, 0);
                self.content_block_index_to_tool_details
                    .insert(content_block_index, tool_use);
                None
            }
            ConverseStreamOutputPacket::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: Some(ContentBlockDeltaType::Text(text)),
                ..
            }) => Some(create_stream_response(
                self.id.as_str(),
                &self.model,
                vec![chat_choice_stream(Some(text), None, self.role, None, None)],
                None,
            )),
            ConverseStreamOutputPacket::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: Some(ContentBlockDeltaType::ToolUse(ToolUseBlockDelta { input, .. })),
                content_block_index,
                ..
            }) => {
                let tool_delta_idx = *self
                    .content_block_index_to_delta_idx
                    .get(&content_block_index)
                    .unwrap_or(&0);

                if let Some(ToolUseBlockStart {
                    tool_use_id, name, ..
                }) = self
                    .content_block_index_to_tool_details
                    .get(&content_block_index)
                {
                    let z = create_stream_response(
                        self.id.as_str(),
                        &self.model,
                        vec![chat_choice_stream(
                            None,
                            Some(vec![ChatCompletionMessageToolCallChunk {
                                index: tool_delta_idx,
                                id: Some(tool_use_id.clone()),
                                r#type: Some(FunctionType::Function),
                                function: Some(FunctionCallStream {
                                    name: Some(name.clone()),
                                    arguments: Some(input),
                                }),
                            }]),
                            self.role,
                            None,
                            None,
                        )],
                        None,
                    );
                    self.content_block_index_to_delta_idx
                        .insert(content_block_index, tool_delta_idx + 1);
                    Some(z)
                } else {
                    Some(Err(to_api_error("Invalid stream from Bedrock Converse API. Tool use delta received before starting packet".to_string())))
                }
            }
            ConverseStreamOutputPacket::MessageStop(MessageStopEvent { stop_reason, .. }) => {
                match try_convert_finish_reason(&stop_reason) {
                    Ok(finish_reason) => Some(create_stream_response(
                        self.id.as_str(),
                        &self.model,
                        vec![chat_choice_stream(
                            None,
                            None,
                            self.role,
                            None,
                            Some(finish_reason),
                        )],
                        None,
                    )),
                    Err(e) => Some(Err(e)),
                }
            }
            ConverseStreamOutputPacket::Metadata(ConverseStreamMetadataEvent {
                usage: Some(usage),
                ..
            }) => Some(create_stream_response(
                self.id.as_str(),
                &self.model,
                vec![],
                Some(convert_usage(&usage)),
            )),
            // An `OpenAI` stream delta has no reasoning field, so reasoning text, signatures and
            // redacted reasoning yield no chunk. The Anthropic provider drops thinking deltas the
            // same way.
            ConverseStreamOutputPacket::ContentBlockDelta(ContentBlockDeltaEvent {
                delta: Some(ContentBlockDeltaType::ReasoningContent(_)),
                ..
            })
            | ConverseStreamOutputPacket::ContentBlockStop(_) => None,
            unknown => Some(Err(to_api_error(format!(
                "Unknown event from Bedrock stream: {unknown:?}"
            )))),
        }
    }
}

#[async_trait]
impl Chat for BedrockConverse {
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let input = self.to_converse_stream(&self.client, self.alter_request(req))?;
        let output = self
            .client
            .do_converse_stream(input)
            .await
            .map_err(|e| api_error::from_client_error(e.as_ref()))?;
        Ok(Self::process_stream(self.model_id.as_str(), output.stream))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let input = self.to_converse(&self.client, self.alter_request(req))?;
        let output = self
            .client
            .do_converse(input)
            .await
            .map_err(|e| api_error::from_client_error(e.as_ref()))?;
        self.convert_converse_output(output)
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::ChatCompletionRequestUserMessageArgs;
    use aws_config::{BehaviorVersion, Region, SdkConfig};
    use aws_sdk_bedrockruntime::types::{ReasoningContentBlock, ReasoningTextBlock};

    #[test]
    fn prompt_cache_point_is_added_to_last_message() {
        let messages = vec![
            ChatCompletionRequestUserMessageArgs::default()
                .content("Reusable context")
                .build()
                .expect("user message should build")
                .into(),
        ];
        let mut system = vec![];
        let mut messages = BedrockConverse::convert_non_system_messages(messages)
            .expect("bedrock messages should convert");

        BedrockConverse::add_prompt_cache_point(&mut system, &mut messages)
            .expect("cache point should build");

        assert!(matches!(
            messages
                .last()
                .and_then(|message| message.content.last()),
            Some(ContentBlock::CachePoint(cache_point))
                if cache_point.r#type == CachePointType::Default
        ));
    }

    fn converse_reply(blocks: Vec<ContentBlock>) -> ConverseOutput {
        let message = Message::builder()
            .role(ConversationRole::Assistant)
            .set_content(Some(blocks))
            .build()
            .expect("message should build");
        ConverseOutput::builder()
            .output(aws_sdk_bedrockruntime::types::ConverseOutput::Message(
                message,
            ))
            .stop_reason(aws_sdk_bedrockruntime::types::StopReason::EndTurn)
            .build()
            .expect("converse output should build")
    }

    fn reasoning_block(text: &str) -> ContentBlock {
        ContentBlock::ReasoningContent(ReasoningContentBlock::ReasoningText(
            ReasoningTextBlock::builder()
                .text(text)
                .build()
                .expect("reasoning text should build"),
        ))
    }

    /// A converse model with a client that is never sent a request.
    fn converse(model_id: &str) -> BedrockConverse {
        let config = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::from_static("us-east-1"))
            .build();
        BedrockConverse::new(Arc::new(BedrockClient::from(&config)), model_id.to_string())
    }

    fn converted_message(blocks: Vec<ContentBlock>) -> ChatCompletionResponseMessage {
        converse("test-model")
            .convert_converse_output(converse_reply(blocks))
            .expect("converse output should convert")
            .choices
            .into_iter()
            .next()
            .expect("one choice")
            .message
    }

    // Regression test for #15406: reasoning text must not go into `content`.
    #[test]
    fn reasoning_text_goes_to_reasoning_content_not_content() {
        let message = converted_message(vec![
            ContentBlock::Text("\n\n4".to_string()),
            reasoning_block("Okay, the user asks for 2+2."),
        ]);

        assert_eq!(message.content.as_deref(), Some("\n\n4"));
        assert_eq!(
            message.reasoning_content.as_deref(),
            Some("Okay, the user asks for 2+2.")
        );
    }

    // Regression test for #15406: a reasoning draft of a JSON answer must not reach the answer
    // text, or `ai_*` decisions see two JSON objects.
    #[test]
    fn json_answer_is_the_only_content_when_reasoning_holds_a_draft() {
        let answer = r#"{"answers":{"q000":1.0}}"#;
        let message = converted_message(vec![
            ContentBlock::Text(answer.to_string()),
            reasoning_block(r#"A first draft is {"answers":{"q000":0.5}}. Let me check."#),
        ]);

        let content = message.content.expect("content should be set");
        assert_eq!(content, answer);
        serde_json::from_str::<Value>(&content).expect("content should be exactly one JSON value");
        assert!(
            message
                .reasoning_content
                .is_some_and(|reasoning| reasoning.contains("first draft"))
        );
    }

    #[test]
    fn redacted_reasoning_is_dropped_without_error() {
        let message = converted_message(vec![
            ContentBlock::ReasoningContent(ReasoningContentBlock::RedactedContent(
                aws_smithy_types::Blob::new(b"encrypted".to_vec()),
            )),
            ContentBlock::Text("answer".to_string()),
        ]);

        assert_eq!(message.content.as_deref(), Some("answer"));
        assert_eq!(message.reasoning_content, None);
    }

    #[test]
    fn reply_without_reasoning_has_no_reasoning_content() {
        let message = converted_message(vec![
            ContentBlock::Text("first".to_string()),
            ContentBlock::Text("second".to_string()),
        ]);

        assert_eq!(message.content.as_deref(), Some("first\nsecond"));
        assert_eq!(message.reasoning_content, None);
    }

    #[test]
    fn prompt_cache_point_is_added_to_system_when_messages_are_empty() {
        let mut system = vec![];
        let mut messages = vec![];

        BedrockConverse::add_prompt_cache_point(&mut system, &mut messages)
            .expect("cache point should build");

        assert!(matches!(
            system.last(),
            Some(SystemContentBlock::CachePoint(cache_point))
                if cache_point.r#type == CachePointType::Default
        ));
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use async_openai::types::chat::{ChatChoiceStream, FinishReason};
    use aws_sdk_bedrockruntime::types::{
        ContentBlockStopEvent, ReasoningContentBlockDelta, StopReason,
    };
    use aws_smithy_types::Blob;

    fn delta_packet(index: i32, delta: ContentBlockDeltaType) -> ConverseStreamOutputPacket {
        ConverseStreamOutputPacket::ContentBlockDelta(
            ContentBlockDeltaEvent::builder()
                .content_block_index(index)
                .delta(delta)
                .build()
                .expect("delta event should build"),
        )
    }

    fn reasoning_packet(
        index: i32,
        delta: ReasoningContentBlockDelta,
    ) -> ConverseStreamOutputPacket {
        delta_packet(index, ContentBlockDeltaType::ReasoningContent(delta))
    }

    fn stop_block_packet(index: i32) -> ConverseStreamOutputPacket {
        ConverseStreamOutputPacket::ContentBlockStop(
            ContentBlockStopEvent::builder()
                .content_block_index(index)
                .build()
                .expect("block stop event should build"),
        )
    }

    #[test]
    fn reasoning_deltas_yield_no_chunk() {
        let mut state = StreamState::default();
        for delta in [
            ReasoningContentBlockDelta::Text("The user asks for a sum.".to_string()),
            ReasoningContentBlockDelta::Signature("c2lnbmF0dXJl".to_string()),
            ReasoningContentBlockDelta::RedactedContent(Blob::new(b"opaque".to_vec())),
        ] {
            let packet = reasoning_packet(0, delta);
            let debug = format!("{packet:?}");
            let out = state.convert_packet(packet);
            assert!(out.is_none(), "{debug} must yield no chunk: {out:?}");
        }
    }

    /// Regression test for #15410: a reasoning delta stopped the stream with an error.
    #[test]
    fn reasoning_stream_yields_only_answer_text_and_finish_reason() {
        let packets = vec![
            ConverseStreamOutputPacket::MessageStart(
                MessageStartEvent::builder()
                    .role(ConversationRole::Assistant)
                    .build()
                    .expect("message start event should build"),
            ),
            reasoning_packet(0, ReasoningContentBlockDelta::Text("2+2 is 4.".to_string())),
            reasoning_packet(
                0,
                ReasoningContentBlockDelta::Signature("c2lnbmF0dXJl".to_string()),
            ),
            stop_block_packet(0),
            delta_packet(1, ContentBlockDeltaType::Text("4".to_string())),
            stop_block_packet(1),
            ConverseStreamOutputPacket::MessageStop(
                MessageStopEvent::builder()
                    .stop_reason(StopReason::EndTurn)
                    .build()
                    .expect("message stop event should build"),
            ),
        ];

        let mut state = StreamState::default();
        let chunks: Vec<CreateChatCompletionStreamResponse> = packets
            .into_iter()
            .filter_map(|packet| state.convert_packet(packet))
            .collect::<Result<_, _>>()
            .expect("a reasoning stream should convert without errors");

        let choices: Vec<&ChatChoiceStream> =
            chunks.iter().flat_map(|chunk| &chunk.choices).collect();
        let content: String = choices
            .iter()
            .filter_map(|choice| choice.delta.content.as_deref())
            .collect();
        assert_eq!(content, "4");
        assert!(
            choices
                .iter()
                .all(|choice| choice.delta.role == Some(Role::Assistant))
        );
        assert_eq!(
            choices.last().and_then(|choice| choice.finish_reason),
            Some(FinishReason::Stop)
        );
    }
}
