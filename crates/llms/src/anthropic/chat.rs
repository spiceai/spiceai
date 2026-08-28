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
#![allow(clippy::missing_errors_doc)]
use std::time::SystemTime;

use crate::anthropic::types::{EffortLevel, OutputConfig, OutputFormat};
use crate::chat::Chat;
use crate::chat::nsql::SqlGeneration;
use crate::chat::nsql::structured_output::StructuredOutputSqlGeneration;
use async_openai::error::{ApiError, OpenAIError};
use async_openai::traits::RequestOptionsBuilder;
use async_openai::types::chat::{
    ChatChoice, ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
    ChatCompletionNamedToolChoice, ChatCompletionRequestAssistantMessage,
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestAssistantMessageContentPart,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestSystemMessageContentPart, ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestToolMessageContentPart,
    ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
    ChatCompletionRequestUserMessageContentPart, ChatCompletionResponseMessage,
    ChatCompletionResponseStream, ChatCompletionToolChoiceOption, CreateChatCompletionRequest,
    CreateChatCompletionResponse, FinishReason, FunctionCall, FunctionName, ReasoningEffort,
    ResponseFormat, ResponseFormatJsonSchema, Role, StopConfiguration, ToolChoiceOptions,
};
use serde_json::json;

use super::Anthropic;
use super::types::{
    AnthropicModelVariant, CacheControlEphemeral, ContentBlock, ContentParam, MessageCreateParams,
    MessageCreateResponse, MessageParam, MessageRole, MetadataParam, ResponseContentBlock,
    ResponseTextBlock, StopReason, TextBlockParam, ToolChoiceParam, ToolResultBlockParam,
    ToolUseBlockParam, default_max_tokens, tool_from_completion_tools,
};
use super::types_stream::transform_stream;
use async_trait::async_trait;

#[async_trait]
impl Chat for Anthropic {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        Some(&StructuredOutputSqlGeneration {})
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let mut anth_req = MessageCreateParams::try_from((self.model.clone(), req))?;
        anth_req.stream = Some(true);

        let stream = self
            .client
            .chat()
            .path("/messages")?
            .create_stream_byot(anth_req)
            .await?;

        Ok(transform_stream(stream))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let anth_req = MessageCreateParams::try_from((self.model.clone(), req))?;

        let inner_resp: MessageCreateResponse = self
            .client
            .chat()
            .path("/messages")
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .create_byot(anth_req)
            .await?;

        CreateChatCompletionResponse::try_from(inner_resp)
    }
}

impl TryFrom<MessageCreateResponse> for CreateChatCompletionResponse {
    type Error = OpenAIError;

    #[expect(clippy::cast_possible_truncation)]
    fn try_from(value: MessageCreateResponse) -> Result<Self, Self::Error> {
        Ok(CreateChatCompletionResponse {
            id: value.id,
            model: value.model.clone(),
            usage: Some(value.usage.into()),
            created: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
                .as_secs() as u32,
            service_tier: None,
            system_fingerprint: None,
            object: "chat.completion".to_string(),
            choices: vec![ChatChoice {
                index: 0,
                logprobs: None,
                finish_reason: match value.stop_reason {
                    Some(
                        StopReason::StopSequence | StopReason::EndTurn | StopReason::PauseTurn,
                    ) => Some(FinishReason::Stop),
                    Some(StopReason::MaxTokens | StopReason::ModelContextWindowExceeded) => {
                        Some(FinishReason::Length)
                    }
                    Some(StopReason::ToolUse) => Some(FinishReason::ToolCalls),
                    Some(StopReason::Refusal) => Some(FinishReason::ContentFilter),
                    None => None,
                },
                message: create_completion_message(&value.content, &value.role).map_err(|e| {
                    OpenAIError::ApiError(ApiError {
                        message: e.to_string(),
                        r#type: Some("AnthropicConversionError".to_string()),
                        param: None,
                        code: None,
                    })
                })?,
            }],
        })
    }
}

fn create_completion_message(
    blocks: &[ResponseContentBlock],
    role: &MessageRole,
) -> Result<ChatCompletionResponseMessage, Box<dyn std::error::Error + Send + Sync>> {
    let mut content = String::new();

    // Convert tool calls and add message text to `content`
    let tool_calls: Vec<ChatCompletionMessageToolCalls> = blocks
        .iter()
        .filter_map(|b| match b {
            ResponseContentBlock::ToolUse(t) => {
                let arguments = match serde_json::to_string(&t.input) {
                    Ok(a) => a,
                    Err(e) => {
                        return Some(Err(format!(
                            "Failed to serialize tool use argument {}. Error: {e}",
                            t.input
                        )
                        .into()));
                    }
                };
                Some(Ok(ChatCompletionMessageToolCalls::Function(
                    ChatCompletionMessageToolCall {
                        id: t.id.clone(),
                        function: FunctionCall {
                            name: t.name.clone(),
                            arguments,
                        },
                    },
                )))
            }
            ResponseContentBlock::Text(ResponseTextBlock { text, .. }) => {
                content.push_str(text);
                None
            }
            ResponseContentBlock::Thinking(_) => {
                // Internal thinking is not exposed to the user
                None
            }
            ResponseContentBlock::RedactedThinking(_) | ResponseContentBlock::ServerToolUse(_) => {
                None
            }
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?;

    Ok(ChatCompletionResponseMessage {
        reasoning_content: None,
        tool_calls: Some(tool_calls),
        refusal: None,
        annotations: None,
        function_call: None,
        audio: None,
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
            ChatCompletionRequestMessage::Developer(_) => Err(OpenAIError::InvalidArgument(
                "Developer message not supported".to_string(),
            )),
            ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                content: ChatCompletionRequestToolMessageContent::Text(text),
                tool_call_id,
            }) => Ok(MessageParam::user(vec![ContentBlock::ToolResult(
                ToolResultBlockParam::new(tool_call_id, super::types::ContentParam::String(text)),
            )])),
            ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                content: ChatCompletionRequestToolMessageContent::Array(parts),
                tool_call_id,
            }) => Ok(MessageParam::user(vec![ContentBlock::ToolResult(
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
            }) => Ok(MessageParam::user(vec![ContentBlock::Text(
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
                        ChatCompletionRequestUserMessageContentPart::InputAudio(_) => Err(
                            OpenAIError::InvalidArgument("Input Audio not supported".to_string()),
                        ),
                        ChatCompletionRequestUserMessageContentPart::File(_) => Err(
                            OpenAIError::InvalidArgument("File content not supported".to_string()),
                        ),
                    })
                    .collect::<Result<Vec<_>, OpenAIError>>()?;

                Ok(MessageParam::user(blocks))
            }
            ChatCompletionRequestMessage::Assistant(msg) => {
                assistant_messages_to_content_blocks(msg)
            }
        }
    }
}

fn assistant_messages_to_content_blocks(
    msg: ChatCompletionRequestAssistantMessage,
) -> Result<MessageParam, OpenAIError> {
    let ChatCompletionRequestAssistantMessage {
        content,
        tool_calls,
        ..
    } = msg;

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
            .filter_map(|tool_call_enum| {
                // Extract the function call from the enum wrapper
                match tool_call_enum {
                    ChatCompletionMessageToolCalls::Function(call) => {
                        let input = if call.function.arguments.is_empty() {
                            Ok(json!(
                                {
                                    "$schema": "http://json-schema.org/draft-07/schema#",
                                    "properties": {},
                                    "required": [],
                                    "title": "",
                                    "type": "object"
                                }
                            ))
                        } else {
                            serde_json::from_str(&call.function.arguments)
                        };
                        Some(
                            input
                                .map(|i| {
                                    ContentBlock::ToolUse(ToolUseBlockParam::new(
                                        call.id.clone(),
                                        i,
                                        call.function.name.clone(),
                                    ))
                                })
                                .map_err(|e| {
                                    OpenAIError::ApiError(ApiError {
                                        message: e.to_string(),
                                        r#type: Some("AnthropicConversionError".to_string()),
                                        param: None,
                                        code: None,
                                    })
                                }),
                        )
                    }
                    ChatCompletionMessageToolCalls::Custom(_) => {
                        // Custom tool calls are not supported for Anthropic
                        None
                    }
                }
            })
            .collect::<Result<_, OpenAIError>>()?,
        None => vec![],
    };

    content_blocks.extend(tool_blocks);
    Ok(MessageParam::assistant(content_blocks))
}

/// Refuses a request that asks for per-token log probabilities.
///
/// Anthropic's Messages API has no `logprobs` equivalent: it returns no per-token probabilities,
/// and this adapter's response conversion consequently leaves `ChatChoice::logprobs` empty. A
/// request for them therefore cannot be served, and answering it with a completion that silently
/// omits them hides that from the caller, so the parameters are named and refused instead.
fn refuse_unsupported_logprobs(
    model: &AnthropicModelVariant,
    value: &CreateChatCompletionRequest,
) -> Result<(), OpenAIError> {
    // `logprobs: Some(false)` asks for nothing and is satisfiable, so only a positive request is
    // refused. `top_logprobs` is refused on its own: OpenAI documents it as requiring
    // `logprobs: true`, but a caller setting only `top_logprobs` is still asking for log
    // probabilities, so leaving that spelling accepted would serve the same request silently.
    //
    // `param` reports the more specific field, while `remedy` names every field that has to go:
    // dropping just `top_logprobs` from a request that also set `logprobs: true` would otherwise
    // earn a second refusal.
    let (param, remedy) = match (value.top_logprobs.is_some(), value.logprobs == Some(true)) {
        (true, true) => ("top_logprobs", "`logprobs` and `top_logprobs`"),
        (true, false) => ("top_logprobs", "`top_logprobs`"),
        (false, true) => ("logprobs", "`logprobs`"),
        (false, false) => return Ok(()),
    };

    Err(OpenAIError::ApiError(ApiError {
        // Either field can arrive from the request or from the model's configured defaults, so
        // the remedy names both origins.
        message: format!(
            "Failed to run model '{model}' (anthropic): the `{param}` parameter is not supported. \
             Anthropic's Messages API returns no per-token log probabilities. \
             Remove {remedy} from the request and from the model's parameters, \
             or use a model provider that reports them. \
             See: https://spiceai.org/docs/components/models/anthropic"
        ),
        // The caller sent something this provider cannot serve, so it is their request that is
        // invalid. `openai_error_to_response` reads `code` to pick the status, and anything it
        // does not recognize becomes a 500 — which would report a client error as a server fault.
        r#type: Some("invalid_request_error".to_string()),
        param: Some(param.to_string()),
        code: Some("invalid_request_error".to_string()),
    }))
}

impl TryFrom<(AnthropicModelVariant, CreateChatCompletionRequest)> for MessageCreateParams {
    type Error = OpenAIError;
    fn try_from(
        pair: (AnthropicModelVariant, CreateChatCompletionRequest),
    ) -> Result<Self, Self::Error> {
        let (model, value) = pair;
        refuse_unsupported_logprobs(&model, &value)?;
        let cache_control = value
            .prompt_cache_key
            .as_ref()
            .map(|_| CacheControlEphemeral::ephemeral());

        let messages = value
            .messages
            .iter()
            .filter(|m| !matches!(m, ChatCompletionRequestMessage::System(_)))
            .map(|m| MessageParam::try_from(m.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(MessageCreateParams {
            // No OpenAI request field corresponds to Anthropic's `top_k`, which restricts which
            // tokens may be sampled. `top_logprobs` asks how many alternatives to report and
            // leaves sampling untouched, so carrying one into the other would answer a request to
            // observe the distribution by narrowing it instead.
            top_k: None,
            top_p: value.top_p,
            temperature: value.temperature,
            max_tokens: value
                .max_completion_tokens
                .unwrap_or(default_max_tokens(&model)),
            stream: value.stream,
            metadata: value
                .metadata
                .and_then(|m| {
                    // Metadata is a newtype around serde_json::Value - access it as a JSON object
                    serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(
                        serde_json::to_value(&m).ok()?,
                    )
                    .ok()
                    .and_then(|obj| obj.get("user_id").cloned())
                })
                .map(|id| MetadataParam {
                    user_id: id.as_str().map(String::from),
                }),
            model,
            stop_sequences: value.stop.map(|s| match s {
                StopConfiguration::String(s) => vec![s],
                StopConfiguration::StringArray(a) => a,
            }),
            system: system_message_from_messages(&value.messages),
            cache_control,
            messages,

            tool_choice: match value.tool_choice {
                Some(ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto)) => Some(
                    ToolChoiceParam::auto(!value.parallel_tool_calls.unwrap_or_default()),
                ),
                Some(ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Required)) => Some(
                    ToolChoiceParam::any(!value.parallel_tool_calls.unwrap_or_default()),
                ),
                Some(ChatCompletionToolChoiceOption::Function(ChatCompletionNamedToolChoice {
                    function: FunctionName { name },
                    ..
                })) => Some(ToolChoiceParam::tool(
                    name,
                    !value.parallel_tool_calls.unwrap_or_default(),
                )),
                // AllowedTools or Custom not supported, None and ToolChoiceOptions::None both map to None
                _ => None,
            },
            tools: value
                .tools
                .map(|t| t.iter().filter_map(tool_from_completion_tools).collect()),
            thinking: None,
            service_tier: None,
            container: None,
            context_management: None,
            mcp_servers: None,
            output_config: match value.reasoning_effort {
                None | Some(ReasoningEffort::None) => None,
                Some(ReasoningEffort::Minimal | ReasoningEffort::Low) => {
                    Some(OutputConfig {
                        effort: Some(EffortLevel::Low),
                    })
                }
                Some(ReasoningEffort::Medium) => Some(OutputConfig {
                    effort: Some(EffortLevel::Medium),
                }),
                Some(ReasoningEffort::High | ReasoningEffort::Xhigh) => Some(OutputConfig {
                    effort: Some(EffortLevel::High),
                }),
            },
            output_format: value.response_format.and_then(|rf| match rf {
                ResponseFormat::JsonObject => {
                    tracing::warn!("Anthropic does not support arbitrary JSON object response format. Only `type: \"json_schema\"` or `type: \"text\"`.");
                    None
                }
                ResponseFormat::JsonSchema {
                    json_schema:
                        ResponseFormatJsonSchema {
                            schema: Some(schema_v),
                            ..
                        },
                } => Some(OutputFormat {
                    format_type: "json_schema".to_string(),
                    schema: schema_v,
                }),
                _ => None,
            }),
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
                        .map(|part| match part {
                            ChatCompletionRequestSystemMessageContentPart::Text(
                                ChatCompletionRequestMessageContentPartText { text },
                            ) => text,
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
        tracing::warn!(
            "More than one ({count}) system message found in messages. Concatenating into a single String.",
            count = system_messages.len()
        );
    }
    if system_messages.is_empty() {
        None
    } else {
        Some(system_messages.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::{
        ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequest,
    };

    #[test]
    fn prompt_cache_key_enables_automatic_cache_control() {
        let req = CreateChatCompletionRequest {
            messages: vec![
                ChatCompletionRequestUserMessageArgs::default()
                    .content("Use the cached context.")
                    .build()
                    .expect("user message should build")
                    .into(),
            ],
            prompt_cache_key: Some("schema-context".to_string()),
            ..CreateChatCompletionRequest::default()
        };

        let params = MessageCreateParams::try_from(("claude-sonnet-4-6".to_string(), req))
            .expect("anthropic request should convert");

        assert_eq!(
            params.cache_control,
            Some(CacheControlEphemeral::ephemeral())
        );
    }

    fn request_with(
        mutate: impl FnOnce(&mut CreateChatCompletionRequest),
    ) -> CreateChatCompletionRequest {
        let mut req = CreateChatCompletionRequest {
            messages: vec![
                ChatCompletionRequestUserMessageArgs::default()
                    .content("Rank the alternatives.")
                    .build()
                    .expect("user message should build")
                    .into(),
            ],
            ..CreateChatCompletionRequest::default()
        };
        mutate(&mut req);
        req
    }

    fn convert(req: CreateChatCompletionRequest) -> Result<MessageCreateParams, OpenAIError> {
        MessageCreateParams::try_from(("claude-sonnet-4-6".to_string(), req))
    }

    /// Returns the refusal's own fields.
    ///
    /// Asserting on `OpenAIError`'s `Display` instead would be vacuous: it appends
    /// `(param: ...)` and `(code: ...)` to the message, so a check that the rendered string
    /// mentions a parameter passes even when the human-readable message never names it.
    fn refusal(err: &OpenAIError) -> &ApiError {
        match err {
            OpenAIError::ApiError(api) => api,
            other => panic!("expected an ApiError, got {other:?}"),
        }
    }

    #[test]
    fn a_request_for_log_probabilities_is_refused_rather_than_narrowing_sampling() {
        for (param, remedy, req) in [
            (
                "top_logprobs",
                "`top_logprobs`",
                request_with(|r| r.top_logprobs = Some(5)),
            ),
            (
                "logprobs",
                "`logprobs`",
                request_with(|r| r.logprobs = Some(true)),
            ),
            (
                // OpenAI's documented pairing: both set together. The more specific field is
                // reported, but both have to be named as the remedy — removing only the reported
                // one would earn a second refusal.
                "top_logprobs",
                "`logprobs` and `top_logprobs`",
                request_with(|r| {
                    r.logprobs = Some(true);
                    r.top_logprobs = Some(3);
                }),
            ),
            (
                // Zero alternatives is still a request for log probabilities, and it is the value
                // an `Option`-vs-truthiness check is most likely to let through.
                "top_logprobs",
                "`top_logprobs`",
                request_with(|r| r.top_logprobs = Some(0)),
            ),
        ] {
            let err = convert(req).expect_err("a request for log probabilities should be refused");
            let refusal = refusal(&err);

            assert_eq!(refusal.param.as_deref(), Some(param));
            // `openai_error_to_response` picks the HTTP status from `code`, and maps anything it
            // does not recognize to 500. This is a client error, so it has to be the code that
            // maps to 400.
            assert_eq!(refusal.code.as_deref(), Some("invalid_request_error"));
            assert_eq!(refusal.r#type.as_deref(), Some("invalid_request_error"));

            // Asserted against the message itself rather than the rendered error, so the
            // `Display`-appended fields cannot satisfy these on their own.
            for expected in [
                "claude-sonnet-4-6",
                "(anthropic)",
                remedy,
                "https://spiceai.org/docs/components/models/anthropic",
            ] {
                assert!(
                    refusal.message.contains(expected),
                    "refusal should name {expected}, got: {}",
                    refusal.message
                );
            }
        }
    }

    #[test]
    fn a_request_that_asks_for_no_log_probabilities_still_converts() {
        // `logprobs: Some(false)` is satisfiable — Anthropic returns none and none were wanted —
        // so refusing it would reject a request the adapter can serve exactly as asked.
        for req in [
            request_with(|_| {}),
            request_with(|r| r.logprobs = Some(false)),
        ] {
            let params = convert(req).expect("a request asking for no log probabilities converts");
            assert_eq!(
                params.top_k, None,
                "no OpenAI field maps to Anthropic's top_k, so it must be left unset"
            );
        }
    }

    #[test]
    fn a_sampling_control_the_caller_did_set_is_still_forwarded() {
        // Guards the refusal against over-reaching: the unsupported parameters are refused, and
        // the controls Anthropic does accept keep reaching it.
        //
        // One control per request: #13579 reports that setting `temperature` and `top_p` together
        // is refused by the models this adapter reaches, so pairing them here would pin a request
        // that cannot be served as though it were the supported case. Each request is also
        // asserted to carry only the control it set, so neither can be cross-populated.
        let with_temperature = convert(request_with(|r| r.temperature = Some(0.25)))
            .expect("a supported sampling control should convert");
        assert_eq!(with_temperature.temperature, Some(0.25));
        assert_eq!(with_temperature.top_p, None);
        assert_eq!(with_temperature.top_k, None);

        let with_top_p = convert(request_with(|r| r.top_p = Some(0.9)))
            .expect("a supported sampling control should convert");
        assert_eq!(with_top_p.top_p, Some(0.9));
        assert_eq!(with_top_p.temperature, None);
        assert_eq!(with_top_p.top_k, None);
    }
}
