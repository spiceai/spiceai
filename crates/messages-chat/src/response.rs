/*
Copyright 2026 The Spice.ai OSS Authors

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

//! A chat completion as a Message, and a chat completion error as a Messages API error.

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionMessageToolCalls, CompletionUsage, CreateChatCompletionResponse, FinishReason,
};
use chat_api::ApiErrorKind;
use messages_api::types::{ContentBlock, Message, StopReason, Usage};
use messages_api::{ErrorType, MessagesError};
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use serde_json::{Map, Value};

/// The ID of the Message that answers the chat completion `chat_id`: `msg_` and the
/// completion's own ID, so that each can be found from the other; a random one when the
/// completion has none.
pub(crate) fn message_id(chat_id: &str) -> String {
    let chat_id = chat_id.strip_prefix("chatcmpl-").unwrap_or(chat_id);
    if chat_id.is_empty() {
        format!("msg_{}", random_suffix())
    } else {
        format!("msg_{chat_id}")
    }
}

/// The ID of a tool call that came without one.
pub(crate) fn tool_use_id() -> String {
    format!("toolu_{}", random_suffix())
}

fn random_suffix() -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect()
}

/// The stop reason a chat completion's finish reason means. A completion that sent none
/// stopped on its own, or to call the tools it called.
pub(crate) fn stop_reason(finish: Option<FinishReason>, called_tools: bool) -> StopReason {
    match finish {
        Some(FinishReason::Length) => StopReason::MaxTokens,
        Some(FinishReason::ToolCalls | FinishReason::FunctionCall) => StopReason::ToolUse,
        Some(FinishReason::ContentFilter) => StopReason::Refusal,
        Some(FinishReason::Stop) => StopReason::EndTurn,
        None if called_tools => StopReason::ToolUse,
        None => StopReason::EndTurn,
    }
}

/// The input tokens of `usage` that were not read from the provider's cache, and those that
/// were. The Messages API counts them apart; a chat completion counts the cached ones in its
/// prompt tokens.
pub(crate) fn input_tokens(usage: &CompletionUsage) -> (u64, Option<u64>) {
    let cached = usage
        .prompt_tokens_details
        .as_ref()
        .and_then(|details| details.cached_tokens);
    let prompt = u64::from(usage.prompt_tokens);
    match cached {
        Some(cached) => (
            prompt.saturating_sub(u64::from(cached)),
            Some(u64::from(cached)),
        ),
        None => (prompt, None),
    }
}

pub(crate) fn usage(usage: Option<&CompletionUsage>) -> Usage {
    let Some(usage) = usage else {
        return Usage::default();
    };
    let (input_tokens, cache_read_input_tokens) = input_tokens(usage);
    Usage {
        input_tokens,
        output_tokens: u64::from(usage.completion_tokens),
        cache_creation_input_tokens: None,
        cache_read_input_tokens,
    }
}

/// The input of a tool call whose arguments are `arguments`: the JSON object they hold, or an
/// empty one when they are empty.
///
/// # Errors
///
/// Returns an `api_error` when the arguments are not a JSON object: the Messages API has no way
/// to return them as they are.
pub(crate) fn tool_input(model: &str, tool: &str, arguments: &str) -> Result<Value, MessagesError> {
    if arguments.trim().is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    match serde_json::from_str::<Value>(arguments) {
        Ok(input @ Value::Object(_)) => Ok(input),
        Ok(_) | Err(_) => Err(MessagesError::api(format!(
            "Model '{model}' called the tool '{tool}' with arguments that are not a JSON object, which the Messages API cannot return: {arguments}"
        ))),
    }
}

/// `response` as a Message. Its reasoning becomes a thinking block when `show_thinking`.
///
/// # Errors
///
/// Returns an `api_error` when the completion has no choice, or calls a tool with arguments that
/// are not a JSON object.
pub fn message(
    response: CreateChatCompletionResponse,
    show_thinking: bool,
) -> Result<Message, MessagesError> {
    let CreateChatCompletionResponse {
        id,
        model,
        choices,
        usage: chat_usage,
        ..
    } = response;
    let Some(choice) = choices.into_iter().next() else {
        return Err(MessagesError::api(format!(
            "Model '{model}' returned a chat completion without a choice."
        )));
    };

    let mut content = Vec::new();
    let response_message = choice.message;
    if show_thinking
        && let Some(reasoning) = response_message.reasoning_content
        && !reasoning.is_empty()
    {
        content.push(ContentBlock::Thinking {
            thinking: reasoning,
            signature: String::new(),
        });
    }
    if let Some(text) = response_message.content.filter(|text| !text.is_empty()) {
        content.push(ContentBlock::Text { text });
    }
    let refused = match response_message.refusal.filter(|text| !text.is_empty()) {
        Some(refusal) => {
            content.push(ContentBlock::Text { text: refusal });
            true
        }
        None => false,
    };
    let mut called_tools = false;
    for call in response_message.tool_calls.into_iter().flatten() {
        let ChatCompletionMessageToolCalls::Function(call) = call else {
            return Err(MessagesError::api(format!(
                "Model '{model}' called a custom tool, which the Messages API has no block for."
            )));
        };
        called_tools = true;
        let input = tool_input(&model, &call.function.name, &call.function.arguments)?;
        content.push(ContentBlock::ToolUse {
            id: if call.id.is_empty() {
                tool_use_id()
            } else {
                call.id
            },
            name: call.function.name,
            input,
        });
    }

    let stop_reason = if refused {
        StopReason::Refusal
    } else {
        stop_reason(choice.finish_reason, called_tools)
    };
    Ok(Message {
        content,
        stop_reason: Some(stop_reason),
        usage: usage(chat_usage.as_ref()),
        ..Message::started(message_id(&id), model)
    })
}

/// `error`, a chat completion's error, as a Messages API error of the same kind.
#[must_use]
pub fn messages_error(error: OpenAIError) -> MessagesError {
    match error {
        OpenAIError::InvalidArgument(message) => MessagesError::invalid_request(message),
        OpenAIError::ApiError(api_error) => {
            let error_type = match ApiErrorKind::of(&api_error) {
                Some(ApiErrorKind::InvalidRequest) => ErrorType::InvalidRequest,
                Some(ApiErrorKind::Authentication) => ErrorType::Authentication,
                Some(ApiErrorKind::PermissionDenied) => ErrorType::Permission,
                Some(ApiErrorKind::NotFound) => ErrorType::NotFound,
                Some(ApiErrorKind::RateLimited) => ErrorType::RateLimit,
                Some(ApiErrorKind::InsufficientQuota) => ErrorType::Billing,
                None => ErrorType::Api,
            };
            MessagesError::new(error_type, api_error.message)
        }
        other => MessagesError::api(other.to_string()),
    }
}
