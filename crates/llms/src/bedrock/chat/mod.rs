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
pub(super) mod util;

use crate::bedrock::BedrockClient;
use crate::bedrock::chat::guardrail::GuardRail;
use crate::bedrock::chat::util::{
    chat_choice_stream, chat_completion_stream, convert_usage, extract_from_content_block,
    into_fallible_stream, to_api_error, tool_config, try_convert_finish_reason, try_convert_role,
    value_to_document,
};
use crate::chat::Chat;
use crate::chat::nsql::SqlGeneration;
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatChoice, ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestAssistantMessageContentPart, ChatCompletionRequestDeveloperMessage,
    ChatCompletionRequestDeveloperMessageContent, ChatCompletionRequestMessage,
    ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestSystemMessageContentPart,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionRequestToolMessageContentPart, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
    ChatCompletionResponseMessage, ChatCompletionResponseStream, ChatCompletionToolType,
    CreateChatCompletionRequest, CreateChatCompletionResponse, CreateChatCompletionStreamResponse,
    FunctionCall, FunctionCallStream, Role, Stop,
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
    ContentBlock, ContentBlockDelta as ContentBlockDeltaType, ContentBlockDeltaEvent,
    ContentBlockStart as ContentBlockStartInner, ContentBlockStartEvent, ConversationRole,
    ConverseStreamMetadataEvent, ConverseStreamOutput as ConverseStreamOutputPacket,
    GuardrailConfiguration, GuardrailStreamConfiguration, InferenceConfiguration, Message,
    MessageStartEvent, MessageStopEvent, SystemContentBlock, ToolResultContentBlock,
    ToolResultStatus, ToolUseBlockDelta, ToolUseBlockStart,
};
use futures::stream::StreamExt;
use itertools::Itertools;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::Span;

/// [`BedrockConverse`] provides an `OpenAI` compatible interface (i.e. `impl Chat` ), for models on AWS bedrock that are compatible with the [Converse API](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html).
pub struct BedrockConverse {
    client: Arc<BedrockClient>,
    model_id: String,
    guardrail: Option<GuardRail>,
}

impl BedrockConverse {
    #[must_use]
    pub fn new(client: Arc<BedrockClient>, model_id: String) -> Self {
        Self {
            client,
            model_id,
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
        req.model.clone_from(&self.model_id);
        // Bedrock should set Option::None parameters to a schema with no inputs, but doesn't.
        // Must be done explicitly.
        if let Some(ref mut tools) = req.tools {
            for t in tools.iter_mut() {
                if t.function.parameters.is_none() {
                    t.function.parameters.replace(json!(
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
    #[allow(clippy::too_many_lines, clippy::cast_possible_wrap)]
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
                                let ChatCompletionMessageToolCall {
                                    id,
                                    function: FunctionCall { name, arguments },
                                    ..
                                } = t;

                                let tool_input =
                                    serde_json::from_str(arguments).ok().map(value_to_document);
                                Some(ContentBlock::ToolUse(
                                    ToolUseBlockBuilder::default()
                                        .set_tool_use_id(Some(id.clone()))
                                        .set_name(Some(name.clone()))
                                        .set_input(tool_input)
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

    /// Convert [`ChatCompletionRequestMessage`] that are [`ChatCompletionRequestMessage::System`] or [`ChatCompletionRequestMessage::Developer`] into the Bedrock equivalent [`SystemContentBlock`] format.
    ///
    /// Other enum variants will be ignored.
    #[allow(clippy::cast_possible_wrap)]
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
                }) => vec![SystemContentBlock::Text(s.to_string())],
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

    #[allow(clippy::cast_possible_wrap, deprecated)]
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

    fn to_converse_stream(
        &self,
        client: &Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseStreamFluentBuilder, OpenAIError> {
        let inf_cfg = Self::inference_cfg(&req);
        let CreateChatCompletionRequest {
            messages,
            metadata,
            tool_choice,
            tools,
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

        let system = Self::convert_system_messages(system);
        let messages =
            Self::convert_non_system_messages(messages).map_err(|e| to_api_error(e.to_string()))?;

        let guardrails: Option<GuardrailStreamConfiguration> = self
            .guardrail
            .as_ref()
            .map(std::convert::TryInto::try_into)
            .transpose()
            .map_err(|e: BuildError| to_api_error(e.to_string()))?;

        let mut bldr = client
            .client
            .converse_stream()
            .model_id(self.model_id.clone())
            .set_messages(Some(messages))
            .inference_config(inf_cfg)
            .set_system(Some(system))
            .set_guardrail_config(guardrails)
            .set_tool_config(tool_config(tools, tool_choice));

        if let Some(Value::Object(m)) = metadata {
            bldr = bldr.set_request_metadata(Some(
                m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
            ));
        }

        Ok(bldr)
    }

    #[allow(deprecated)]
    fn to_converse(
        &self,
        client: &Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseFluentBuilder, OpenAIError> {
        let inf_cfg = Self::inference_cfg(&req);
        let CreateChatCompletionRequest {
            messages,
            metadata,
            tools,
            tool_choice,
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

        let system = Self::convert_system_messages(system);
        let messages =
            Self::convert_non_system_messages(messages).map_err(|e| to_api_error(e.to_string()))?;

        let guardrails: Option<GuardrailConfiguration> = self
            .guardrail
            .as_ref()
            .map(std::convert::TryInto::try_into)
            .transpose()
            .map_err(|e: BuildError| to_api_error(e.to_string()))?;

        let mut bldr = client
            .client
            .converse()
            .model_id(self.model_id.clone())
            .set_messages(Some(messages))
            .inference_config(inf_cfg)
            .set_system(Some(system))
            .set_guardrail_config(guardrails)
            .set_tool_config(tool_config(tools, tool_choice));

        if let Some(Value::Object(m)) = metadata {
            bldr = bldr.set_request_metadata(Some(
                m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
            ));
        }

        Ok(bldr)
    }

    #[allow(clippy::cast_possible_truncation, deprecated, clippy::type_complexity)]
    fn convert_converse_output(
        &self,
        output: ConverseOutput,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let usage = output.usage().map(convert_usage);

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

    #[allow(clippy::too_many_lines)]
    fn process_stream(
        model: &str,
        input_stream: EventReceiver<ConverseStreamOutputPacket, ConverseStreamOutputError>,
    ) -> ChatCompletionResponseStream {
        // Individual packets in the stream may need information from previous packets to be converted correctly to a  [`CreateChatCompletionStreamResponse`]. This struct track all the necessary information.
        #[derive(Default)]
        struct StreamState {
            id: String,
            model: String,
            role: Option<Role>,
            content_block_index_to_tool_details: HashMap<i32, ToolUseBlockStart>,
            content_block_index_to_delta_idx: HashMap<i32, u32>,
        }

        let state = Arc::new(RwLock::new(StreamState {
            id: Span::current()
                .id()
                .map(|id| id.into_u64().to_string())
                .unwrap_or_default(),
            // input_stream,
            model: model.to_string(),

            ..StreamState::default()
        }));

        Box::pin(into_fallible_stream(input_stream).filter_map(move |packet| {
            let state = Arc::clone(&state);

            async move {
               let mut state_ = state.write().await;
               let zz: Option<Result<CreateChatCompletionStreamResponse, OpenAIError>> = match packet {
                    Err(SdkError::ServiceError(e)) => {
                        match &e.err() {
                            &ConverseStreamOutputError::InternalServerException(e) => {
                                Some(Err(to_api_error(e.to_string())))
                            }
                            ee => {
                                // TODO specialise
                                Some(Err(to_api_error(ee.to_string())))
                            }
                        }
                    }
                    Err(e) => {
                        Some(Err(to_api_error(e.to_string())))
                    }
                    Ok(None) => None, // Natural end-of-stream.
                    Ok(Some(pkt)) => {
                        let value: Option<Result<CreateChatCompletionStreamResponse, OpenAIError>> =
                            match pkt {
                                ConverseStreamOutputPacket::MessageStart(MessageStartEvent {
                                    role,
                                    ..
                                }) => match try_convert_role(&role) {
                                    Ok(r) => {
                                        state_.role = Some(r);
                                        None
                                    }
                                    Err(e) => Some(Err(e)),
                                },
                                ConverseStreamOutputPacket::ContentBlockStart(
                                    ContentBlockStartEvent {
                                        start: Some(ContentBlockStartInner::ToolUse(tool_use)),
                                        content_block_index,
                                        ..
                                    },
                                ) => {
                                    state_
                                        .content_block_index_to_delta_idx
                                        .insert(content_block_index, 0);
                                    state_
                                        .content_block_index_to_tool_details
                                        .insert(content_block_index, tool_use);
                                    None
                                }
                                ConverseStreamOutputPacket::ContentBlockDelta(
                                    ContentBlockDeltaEvent {
                                        delta: Some(ContentBlockDeltaType::Text(text)),
                                        ..
                                    },
                                ) => Some(chat_completion_stream(
                                    state_.id.as_str(),
                                    state_.model.clone(),
                                    vec![chat_choice_stream(
                                        Some(text),
                                        None,
                                        state_.role,
                                        None,
                                        None,
                                    )],
                                    None,
                                )),
                                ConverseStreamOutputPacket::ContentBlockDelta(
                                    ContentBlockDeltaEvent {
                                        delta:
                                            Some(ContentBlockDeltaType::ToolUse(ToolUseBlockDelta {
                                                input,
                                                ..
                                            })),
                                        content_block_index,
                                        ..
                                    },
                                ) => {
                                    let tool_delta_idx = *state_
                                        .content_block_index_to_delta_idx
                                        .get(&content_block_index)
                                        .unwrap_or(&0);

                                    if let Some(ToolUseBlockStart {
                                        tool_use_id, name, ..
                                    }) = state_
                                        .content_block_index_to_tool_details
                                        .get(&content_block_index)
                                    {
                                        let z = chat_completion_stream(
                                            state_.id.as_str(),
                                            state_.model.clone(),
                                            vec![chat_choice_stream(
                                                None,
                                                Some(vec![ChatCompletionMessageToolCallChunk {
                                                    index: tool_delta_idx,
                                                    id: Some(tool_use_id.clone()),
                                                    r#type: Some(ChatCompletionToolType::Function),
                                                    function: Some(FunctionCallStream {
                                                        name: Some(name.clone()),
                                                        arguments: Some(input),
                                                    }),
                                                }]),
                                                state_.role,
                                                None,
                                                None,
                                            )],
                                            None,
                                        );
                                        state_
                                            .content_block_index_to_delta_idx
                                            .insert(content_block_index, tool_delta_idx + 1);
                                        Some(z)
                                    } else {
                                        Some(Err(to_api_error("Invalid stream from Bedrock Converse API. Tool use delta received before starting packet".to_string())))
                                    }
                                }
                                ConverseStreamOutputPacket::MessageStop(MessageStopEvent {
                                    stop_reason,
                                    ..
                                }) => match try_convert_finish_reason(&stop_reason) {
                                    Ok(finish_reason) => Some(chat_completion_stream(
                                        state_.id.as_str(),
                                        state_.model.clone(),
                                        vec![chat_choice_stream(
                                            None,
                                            None,
                                            state_.role,
                                            None,
                                            Some(finish_reason),
                                        )],
                                        None,
                                    )),
                                    Err(e) => Some(Err(e)),
                                },
                                ConverseStreamOutputPacket::Metadata(
                                    ConverseStreamMetadataEvent {
                                        usage: Some(usage), ..
                                    },
                                ) => Some(chat_completion_stream(
                                    state_.id.as_str(),
                                    state_.model.clone(),
                                    vec![],
                                    Some(convert_usage(&usage)),
                                )),
                                ConverseStreamOutputPacket::ContentBlockStop(_) => None,
                                unknown => Some(Err(to_api_error(format!(
                                    "Unknown event from Bedrock stream: {unknown:?}"
                                )))),
                            };
                        // Need to drop [`RwLockWriteGuard`] before returning `state.
                        value
                    }
                };
               zz
            }
        }))
    }
}

#[async_trait]
impl Chat for BedrockConverse {
    #[allow(deprecated)]
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let input = self.to_converse_stream(&self.client, self.alter_request(req))?;
        let output = self
            .client
            .do_converse_stream(input)
            .await
            .map_err(|e| to_api_error(e.to_string()))?;
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
            .map_err(|e| to_api_error(e.to_string()))?;
        self.convert_converse_output(output)
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}
