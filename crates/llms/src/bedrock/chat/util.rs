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

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionNamedToolChoice, ChatCompletionStreamResponseDelta,
    ChatCompletionToolChoiceOption, ChatCompletionTools, CompletionUsage, FinishReason,
    FunctionCall, FunctionName, FunctionObject, PromptTokensDetails, Role, ToolChoiceOptions,
};
use aws_sdk_bedrockruntime::error::SdkError;
use aws_sdk_bedrockruntime::primitives::event_stream::EventReceiver;
use aws_sdk_bedrockruntime::types::builders::{AnyToolChoiceBuilder, AutoToolChoiceBuilder};
use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, GuardrailConverseContentBlock, GuardrailConverseTextBlock,
    ReasoningContentBlock, ReasoningTextBlock, SpecificToolChoice, StopReason, TokenUsage, Tool,
    ToolChoice, ToolConfiguration, ToolInputSchema, ToolResultBlock, ToolSpecification,
    ToolUseBlock,
};
use aws_smithy_types::event_stream::RawMessage;
use aws_smithy_types::{Document, Number};
use futures::Stream;
use futures::stream::unfold;
use std::collections::HashMap;

pub(super) fn try_convert_role(role: &ConversationRole) -> Result<Role, OpenAIError> {
    match role {
        ConversationRole::Assistant => Ok(Role::Assistant),
        ConversationRole::User => Ok(Role::User),
        unknown_role => Err(to_api_error(format!(
            "Unknown role returned from AWS bedrock: {unknown_role:?}"
        ))),
    }
}

pub(super) fn try_convert_finish_reason(
    stop_reason: &StopReason,
) -> Result<FinishReason, OpenAIError> {
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

pub(super) fn to_api_error(err: impl Into<String>) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message: err.into(),
        r#type: None,
        param: None,
        code: None,
    })
}

/// The part of an `OpenAI` chat message that one Converse response [`ContentBlock`] fills.
#[derive(Debug, PartialEq)]
enum ResponseBlock {
    /// Answer text, for `message.content`.
    Text(String),
    /// Reasoning text of a reasoning model, for `message.reasoning_content`. It must not go into
    /// `content`: callers that parse the answer (e.g. JSON for `ai_*` functions) would also read
    /// the reasoning, which often holds drafts of the answer.
    Reasoning(String),
    ToolCall(ChatCompletionMessageToolCall),
    /// A block with nothing to show, such as a tool result or encrypted (redacted) reasoning.
    Empty,
}

/// Sort a Converse response `ContentBlock` into the message part it fills.
fn extract_from_content_block(blck: &ContentBlock) -> Result<ResponseBlock, OpenAIError> {
    match blck {
        ContentBlock::GuardContent(GuardrailConverseContentBlock::Text(
            GuardrailConverseTextBlock { text, .. },
        ))
        | ContentBlock::Text(text) => Ok(ResponseBlock::Text(text.clone())),
        ContentBlock::ReasoningContent(ReasoningContentBlock::ReasoningText(
            ReasoningTextBlock { text, .. },
        )) => Ok(ResponseBlock::Reasoning(text.clone())),
        ContentBlock::ReasoningContent(ReasoningContentBlock::RedactedContent(_))
        | ContentBlock::ToolResult(ToolResultBlock { .. }) => Ok(ResponseBlock::Empty),
        ContentBlock::ToolUse(ToolUseBlock {
            tool_use_id,
            name,
            input,
            ..
        }) => {
            let input: &Document = input;

            Ok(ResponseBlock::ToolCall(ChatCompletionMessageToolCall {
                id: tool_use_id.clone(),
                function: FunctionCall {
                    name: name.clone(),
                    arguments: serde_json::to_string(&document_to_value(input.clone()))
                        .unwrap_or_default(),
                },
            }))
        }
        unsupported_block => Err(to_api_error(format!("{unsupported_block:?}"))),
    }
}

/// The parts of an `OpenAI` chat message built from all content blocks of one Converse reply.
#[derive(Debug, Default, PartialEq)]
pub(super) struct ResponseParts {
    pub content: Vec<String>,
    pub reasoning: Vec<String>,
    pub tool_calls: Vec<ChatCompletionMessageToolCall>,
}

impl ResponseParts {
    pub(super) fn try_from_blocks(blocks: &[ContentBlock]) -> Result<Self, OpenAIError> {
        let mut parts = Self::default();
        for block in blocks {
            match extract_from_content_block(block)? {
                ResponseBlock::Text(text) => parts.content.push(text),
                ResponseBlock::Reasoning(text) => parts.reasoning.push(text),
                ResponseBlock::ToolCall(call) => parts.tool_calls.push(call),
                ResponseBlock::Empty => {}
            }
        }
        Ok(parts)
    }
}

pub(super) fn tool_config(
    tools: Option<Vec<ChatCompletionTools>>,
    tool_choice: Option<ChatCompletionToolChoiceOption>,
) -> Option<ToolConfiguration> {
    let tool_choice = match tool_choice {
        Some(ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto)) => {
            Some(ToolChoice::Auto(AutoToolChoiceBuilder::default().build()))
        }
        Some(ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Required)) => {
            Some(ToolChoice::Any(AnyToolChoiceBuilder::default().build()))
        }
        Some(ChatCompletionToolChoiceOption::Function(ChatCompletionNamedToolChoice {
            function: FunctionName { name },
            ..
        })) => SpecificToolChoice::builder()
            .name(name)
            .build()
            .ok()
            .map(ToolChoice::Tool),
        _ => None, // None, Mode(None), AllowedTools, Custom, or Unknown.
    };
    let tools = tools?
        .into_iter()
        .filter_map(|t| {
            let ChatCompletionTools::Function(tool) = t else {
                return None;
            };
            let FunctionObject {
                name,
                description,
                parameters,
                ..
            } = tool.function;
            Some(Tool::ToolSpec(
                ToolSpecification::builder()
                    .name(name)
                    .set_description(description)
                    .set_input_schema(
                        parameters.map(|p| ToolInputSchema::Json(value_to_document(p))),
                    )
                    .build()
                    .ok()?,
            ))
        })
        .collect::<Vec<_>>();

    ToolConfiguration::builder()
        .set_tool_choice(tool_choice)
        .set_tools(Some(tools))
        .build()
        .ok()
}

#[expect(deprecated)]
pub(super) fn chat_choice_stream(
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

#[expect(clippy::cast_sign_loss)]
pub(super) fn convert_usage(usage: &TokenUsage) -> CompletionUsage {
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

// TODO: Standardise logic with `s3_vectors_metadata_filter` crate: `https://github.com/spiceai/spiceai/issues/6676`.
pub(super) fn value_to_document(value: serde_json::Value) -> Document {
    match value {
        serde_json::Value::Object(map) => {
            let converted: HashMap<_, _> = map
                .into_iter()
                .map(|(k, v)| (k, value_to_document(v)))
                .collect();
            Document::Object(converted)
        }
        serde_json::Value::Array(arr) => {
            Document::Array(arr.into_iter().map(value_to_document).collect())
        }
        serde_json::Value::Number(num) => {
            if let Some(u) = num.as_u64() {
                Document::Number(Number::PosInt(u))
            } else if let Some(i) = num.as_i64() {
                Document::Number(Number::NegInt(i))
            } else if let Some(f) = num.as_f64() {
                Document::Number(Number::Float(f))
            } else {
                unreachable!("Invalid number in serde_json::Value")
            }
        }
        serde_json::Value::String(s) => Document::String(s),
        serde_json::Value::Bool(b) => Document::Bool(b),
        serde_json::Value::Null => Document::Null,
    }
}

pub(super) fn document_to_value(doc: Document) -> serde_json::Value {
    match doc {
        Document::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, document_to_value(v)))
                .collect(),
        ),
        Document::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(document_to_value).collect())
        }
        Document::Number(num) => match num {
            Number::PosInt(u) => serde_json::Value::Number(u.into()),
            Number::NegInt(i) => serde_json::Value::Number(i.into()),
            Number::Float(f) => serde_json::Number::from_f64(f)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
        },
        Document::String(s) => serde_json::Value::String(s),
        Document::Bool(b) => serde_json::Value::Bool(b),
        Document::Null => serde_json::Value::Null,
    }
}

/// Make a [`EventReceiver`] a proper [`Stream`].
pub(super) fn into_fallible_stream<T, E>(
    receiver: EventReceiver<T, E>,
) -> impl Stream<Item = Result<Option<T>, SdkError<E, RawMessage>>> {
    unfold(receiver, |mut recv| async move {
        match recv.recv().await {
            Ok(None) => None,
            otherwise => Some((otherwise, recv)),
        }
    })
}
