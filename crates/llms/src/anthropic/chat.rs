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
#![allow(clippy::missing_errors_doc)]
use std::pin::Pin;
use std::str::FromStr;
use std::time::SystemTime;

use crate::chat::nsql::SqlGeneration;
use crate::chat::{Chat, Error as ChatError, Result as ChatResult};
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatChoice, ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessage,
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestAssistantMessageContentPart,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestSystemMessageContentPart, ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestToolMessageContentPart,
    ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
    ChatCompletionRequestUserMessageContentPart, ChatCompletionResponseMessage,
    ChatCompletionResponseStream, ChatCompletionToolType, CompletionUsage,
    CreateChatCompletionRequest, CreateChatCompletionResponse, FinishReason, FunctionCall, Role,
    Stop,
};

use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use mistralrs::ChatCompletionResponse;
use snafu::ResultExt;
use tracing_futures::Instrument;

use super::types::{
    ContentBlock, ContentParam, MessageCreateParams, MessageCreateResponse, MessageParam,
    MessageRole, MetadataParam, AnthropicModelVariant, ResponseContentBlock, StopReason, TextBlockParam,
    ToolResultBlockParam, ToolUseBlockParam,
};
use super::Anthropic;

#[async_trait]
impl Chat for Anthropic {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn run(&self, prompt: String) -> ChatResult<Option<String>> {
        Err(ChatError::UnsupportedModalityType {
            modality: "run".to_string(),
        })
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        Err(ChatError::UnsupportedModalityType {
            modality: "stream".to_string(),
        })
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        Err(OpenAIError::InvalidArgument("()".to_string()))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model = self.model.to_string();
                
        let anth_req = MessageCreateParams::try_from(inner_req)?;
        println!("inner_req: {:?}", anth_req);
        
        let inner_resp: MessageCreateResponse = self.client.post("/messages", anth_req).await?;

        let mut resp = CreateChatCompletionResponse::try_from(inner_resp)?;

        resp.model = self.name.to_string();
        Ok(resp)
    }
}

impl TryFrom<MessageCreateResponse> for CreateChatCompletionResponse {
    type Error = OpenAIError;

    fn try_from(value: MessageCreateResponse) -> Result<Self, Self::Error> {
        Ok(CreateChatCompletionResponse {
            id: value.id,
            model: value.model.to_string(),
            usage: Some(CompletionUsage {
                prompt_tokens: value.usage.input_tokens,
                completion_tokens: value.usage.output_tokens,
                total_tokens: value.usage.input_tokens + value.usage.output_tokens,
            }),
            created: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u32,
            service_tier: None,
            system_fingerprint: None,
            object: "chat.completion".to_string(),
            choices: vec![ChatChoice {
                index: 0,
                logprobs: None,
                finish_reason: match value.stop_reason {
                    Some(StopReason::EndTurn) => Some(FinishReason::Stop),
                    Some(StopReason::MaxTokens) => Some(FinishReason::Length),
                    Some(StopReason::StopSequence) => Some(FinishReason::Stop),
                    Some(StopReason::ToolUse) => Some(FinishReason::ToolCalls),
                    None => None,
                },
                message: create_completion_message(value.content, &value.role)?,
            }],
        })
    }
}

fn create_completion_message(
    blocks: Vec<ResponseContentBlock>,
    role: &MessageRole,
) -> Result<ChatCompletionResponseMessage, OpenAIError> {
    let mut content = String::new();

    // Convert tool calls and add message text to `content`
    let tool_calls: Vec<ChatCompletionMessageToolCall> = blocks
        .iter()
        .filter_map(|b| match b {
            ResponseContentBlock::ToolUse(t) => {
                let arguments = match serde_json::to_string(&t.input)
                    .map_err(|e| OpenAIError::JSONDeserialize(e))
                {
                    Ok(a) => a,
                    Err(e) => return Some(Err(e)),
                };
                Some(Ok(ChatCompletionMessageToolCall {
                    id: t.id.clone(),
                    r#type: ChatCompletionToolType::Function,
                    function: FunctionCall {
                        name: t.name.clone(),
                        arguments,
                    },
                }))
            }
            ResponseContentBlock::Text(TextBlockParam { text, .. }) => {
                content.push_str(text);
                None
            }
        })
        .collect::<Result<Vec<_>, OpenAIError>>()?;

    Ok(ChatCompletionResponseMessage {
        tool_calls: Some(tool_calls),
        refusal: None,
        function_call: None,
        role: match role {
            MessageRole::User => Role::User,
            MessageRole::Assistant => Role::Assistant,
        },
        content: Some(content),
    })
}

impl TryFrom<ChatCompletionRequestMessage> for MessageParam {
    type Error = OpenAIError;

    fn try_from(value: ChatCompletionRequestMessage) -> Result<Self, Self::Error> {
        match value {
            ChatCompletionRequestMessage::System(_) => Err(OpenAIError::InvalidArgument(
                "System message not supported".to_string(),
            )),
            ChatCompletionRequestMessage::Function(_) => Err(OpenAIError::InvalidArgument(
                "Function message not supported".to_string(),
            )),
            ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                content: ChatCompletionRequestToolMessageContent::Text(text),
                tool_call_id,
            }) => Ok(MessageParam::User(vec![ContentBlock::ToolResult(
                ToolResultBlockParam::new(tool_call_id, super::types::ContentParam::String(text)),
            )])),
            ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                content: ChatCompletionRequestToolMessageContent::Array(parts),
                tool_call_id,
            }) => Ok(MessageParam::User(vec![ContentBlock::ToolResult(
                ToolResultBlockParam::new(
                    tool_call_id,
                    ContentParam::Blocks(
                        parts
                            .iter()
                            .map(|p| match p {
                                ChatCompletionRequestToolMessageContentPart::Text(
                                    ChatCompletionRequestMessageContentPartText { text },
                                ) => ContentBlock::Text(TextBlockParam::new(text.clone())),
                            })
                            .collect::<Vec<_>>(),
                    ),
                ),
            )])),
            ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
                content: ChatCompletionRequestUserMessageContent::Text(t),
                ..
            }) => Ok(MessageParam::User(vec![ContentBlock::Text(
                TextBlockParam::new(t),
            )])),
            ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
                content: ChatCompletionRequestUserMessageContent::Array(parts),
                ..
            }) => {
                let blocks: Vec<ContentBlock> = parts
                    .iter()
                    .map(|p| match p {
                        ChatCompletionRequestUserMessageContentPart::Text(
                            ChatCompletionRequestMessageContentPartText { text },
                        ) => Ok(ContentBlock::Text(TextBlockParam::new(text.clone()))),
                        ChatCompletionRequestUserMessageContentPart::ImageUrl(_) => Err(
                            OpenAIError::InvalidArgument("Image URL not supported".to_string()),
                        ),
                    })
                    .collect::<Result<Vec<_>, OpenAIError>>()?;

                Ok(MessageParam::User(blocks))
            }
            ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
                content,
                tool_calls,
                ..
            }) => {
                let mut content_blocks: Vec<ContentBlock> = match content {
                    Some(ChatCompletionRequestAssistantMessageContent::Text(text)) => {
                        vec![ContentBlock::Text(TextBlockParam::new(text))]
                    }
                    Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => parts
                        .iter()
                        .map(|p| match p {
                            ChatCompletionRequestAssistantMessageContentPart::Text(
                                ChatCompletionRequestMessageContentPartText { text },
                            ) => Ok(ContentBlock::Text(TextBlockParam::new(text.clone()))),
                            ChatCompletionRequestAssistantMessageContentPart::Refusal(_) => Err(
                                OpenAIError::InvalidArgument("Refusal not supported".to_string()),
                            ),
                        })
                        .collect::<Result<Vec<_>, OpenAIError>>()?,
                    None => vec![],
                };

                let tool_blocks = match tool_calls {
                    Some(calls) => calls
                        .iter()
                        .map(|call| {
                            ContentBlock::ToolUse(ToolUseBlockParam::new(
                                call.id.clone(),
                                serde_json::from_str(&call.function.arguments).unwrap(),
                                call.function.name.clone(),
                            ))
                        })
                        .collect(),
                    None => vec![],
                };

                content_blocks.extend(tool_blocks);
                Ok(MessageParam::Assistant(content_blocks))
            }
        }
    }
}

impl TryFrom<CreateChatCompletionRequest> for MessageCreateParams {
    type Error = OpenAIError;
    fn try_from(value: CreateChatCompletionRequest) -> Result<Self, Self::Error> {
        let model = AnthropicModelVariant::from_str(value.model.as_str())?;

        let messages = value
            .messages
            .iter()
            .filter(|m| !matches!(m, ChatCompletionRequestMessage::System(_)))
            .map(|m| MessageParam::try_from(m.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(MessageCreateParams {
            top_k: value.top_logprobs.map(u32::from),
            top_p: value.top_p,
            temperature: value.temperature,
            max_tokens: value
                .max_completion_tokens
                .unwrap_or(model.default_max_tokens()),
            stream: value.stream,
            metadata: value
                .metadata
                .and_then(|m| m.get("user_id").cloned())
                .and_then(|id| {
                    Some(MetadataParam {
                        user_id: id.as_str().map(String::from),
                    })
                }),
            model: model,
            stop_sequences: value.stop.map(|s| match s {
                Stop::String(s) => vec![s],
                Stop::StringArray(a) => a,
            }),
            system: system_message_from_messages(&value.messages),
            messages,

            // TODO: Implement these
            tool_choice: None,
            tools: None,
        })
    }
}

fn system_message_from_messages(messages: &[ChatCompletionRequestMessage]) -> Option<String> {
    let system_messages: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                content,
                ..
            }) => match content {
                ChatCompletionRequestSystemMessageContent::Text(text) => Some(text.clone()),
                ChatCompletionRequestSystemMessageContent::Array(a) => {
                    let elements: Vec<_> = a
                        .iter()
                        .filter_map(|part| match part {
                            ChatCompletionRequestSystemMessageContentPart::Text(
                                ChatCompletionRequestMessageContentPartText { text },
                            ) => Some(text),
                            _ => None,
                        })
                        .cloned()
                        .collect();
                    Some(elements.as_slice().join("\n"))
                }
            },
            _ => None,
        })
        .collect();

    if system_messages.len() > 1 {
        tracing::warn!("More than one ({count}) system message found in messages. Concatenating into a single String.", count = system_messages.len());
    }
    if system_messages.is_empty() {
        None
    } else {
        Some(system_messages.join("\n"))
    }
}
