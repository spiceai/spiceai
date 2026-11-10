/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A chat completion as a Response.
//!
//! The Response repeats what its request asked for, as the Responses API does: `instructions`,
//! `tools`, `tool_choice` (`auto` when unset), `parallel_tool_calls` (`true` when unset),
//! `temperature`, `top_p` and `max_output_tokens`. A sampling parameter the request left unset
//! is left out rather than given the Responses API's default, since the chat model's own
//! default, which is what it sampled with, is unknown here. The `model` is the one the request
//! named, so that each event of a stream, the first of which comes before the completion does,
//! names the same model.

use std::time::{SystemTime, UNIX_EPOCH};

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatCompletionMessageToolCalls, CompletionUsage, CreateChatCompletionResponse, FinishReason,
};
use async_openai::types::responses::{
    AssistantRole, CustomToolCall, ErrorObject, FunctionToolCall, IncompleteDetails,
    InputTokenDetails, Instructions, OutputItem, OutputMessage, OutputMessageContent, OutputStatus,
    OutputTextContent, OutputTokenDetails, RefusalContent, Response, ResponseUsage, Status, Tool,
    ToolChoiceParam,
};
use chat_api::ApiErrorKind;
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use serde_json::{Value, json};

use crate::request::{CUSTOM_INPUT, ToolNames, ToolTarget};

/// What a Response repeats of the request it answers, and the Responses API tools behind the
/// functions the chat model was offered. [`chat_request`](crate::request::chat_request) makes
/// it.
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseContext {
    pub(crate) model: String,
    pub(crate) instructions: Option<String>,
    pub(crate) tools: Vec<Tool>,
    pub(crate) tool_choice: ToolChoiceParam,
    pub(crate) parallel_tool_calls: bool,
    pub(crate) temperature: Option<f32>,
    pub(crate) top_p: Option<f32>,
    pub(crate) max_output_tokens: Option<u32>,
    pub(crate) tool_names: ToolNames,
}

impl ResponseContext {
    /// The model the request named.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }
}

fn random_suffix() -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect()
}

pub(crate) fn response_id() -> String {
    format!("resp_{}", random_suffix())
}

pub(crate) fn message_id() -> String {
    format!("msg_{}", random_suffix())
}

/// The ID of a tool call that came without one.
pub(crate) fn call_id() -> String {
    format!("call_{}", random_suffix())
}

/// The ID of the output item for a call of `target`.
pub(crate) fn tool_call_item_id(target: &ToolTarget) -> String {
    if target.custom {
        format!("ctc_{}", random_suffix())
    } else {
        format!("fc_{}", random_suffix())
    }
}

/// Now, in seconds since the Unix epoch.
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

pub(crate) fn server_error(message: String) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message,
        r#type: Some("server_error".to_string()),
        param: None,
        code: None,
    })
}

/// A Response to the request of `context`, in progress, with no output yet.
pub(crate) fn in_progress(context: &ResponseContext, id: String, created_at: u64) -> Response {
    Response {
        background: None,
        billing: None,
        conversation: None,
        created_at,
        completed_at: None,
        error: None,
        id,
        incomplete_details: None,
        instructions: context.instructions.clone().map(Instructions::Text),
        max_output_tokens: context.max_output_tokens,
        metadata: None,
        model: context.model.clone(),
        object: "response".to_string(),
        output: Vec::new(),
        parallel_tool_calls: Some(context.parallel_tool_calls),
        previous_response_id: None,
        prompt: None,
        prompt_cache_key: None,
        prompt_cache_retention: None,
        reasoning: None,
        safety_identifier: None,
        service_tier: None,
        status: Status::InProgress,
        temperature: context.temperature,
        text: None,
        tool_choice: Some(context.tool_choice.clone()),
        tools: Some(context.tools.clone()),
        top_logprobs: None,
        top_p: context.top_p,
        truncation: None,
        usage: None,
    }
}

/// Ends `response` the way the chat completion's `finish_reason` says it ended: cut off by the
/// token limit or a content filter is `incomplete`, anything else `completed`.
pub(crate) fn finish(
    response: &mut Response,
    finish_reason: Option<FinishReason>,
    usage: Option<&CompletionUsage>,
    now: u64,
) {
    let incomplete = match finish_reason {
        Some(FinishReason::Length) => Some("max_output_tokens"),
        Some(FinishReason::ContentFilter) => Some("content_filter"),
        Some(FinishReason::Stop | FinishReason::ToolCalls | FinishReason::FunctionCall) | None => {
            None
        }
    };
    if let Some(reason) = incomplete {
        response.status = Status::Incomplete;
        response.incomplete_details = Some(IncompleteDetails {
            reason: reason.to_string(),
        });
    } else {
        response.status = Status::Completed;
        response.completed_at = Some(now);
    }
    response.usage = usage.map(response_usage);
}

/// `usage` as the Responses API counts it. Both count the cached input tokens in the input
/// tokens. Every count of a [`ResponseUsage`] is required, so a breakdown the chat completion
/// did not report, cached input or reasoning tokens, is 0.
pub(crate) fn response_usage(usage: &CompletionUsage) -> ResponseUsage {
    ResponseUsage {
        input_tokens: usage.prompt_tokens,
        input_tokens_details: InputTokenDetails {
            cached_tokens: usage
                .prompt_tokens_details
                .as_ref()
                .and_then(|details| details.cached_tokens)
                .unwrap_or(0),
        },
        output_tokens: usage.completion_tokens,
        output_tokens_details: OutputTokenDetails {
            reasoning_tokens: usage
                .completion_tokens_details
                .as_ref()
                .and_then(|details| details.reasoning_tokens)
                .unwrap_or(0),
        },
        total_tokens: usage.total_tokens,
    }
}

/// Whether `arguments` are a whole JSON value, so a call's arguments are done.
pub(crate) fn is_whole(arguments: &str) -> bool {
    serde_json::from_str::<serde::de::IgnoredAny>(arguments).is_ok()
}

/// A function call's arguments: as the model wrote them, or `{}` when it wrote none, as the
/// Responses API never gives a call no arguments.
pub(crate) fn whole_arguments(arguments: String) -> String {
    if arguments.is_empty() {
        "{}".to_string()
    } else {
        arguments
    }
}

/// The input of a custom tool call the model made as a call of the function with `arguments`:
/// the string it passed as `input`. Arguments that are not that, such as ones a token limit
/// cut off, are the input as the model wrote it, for the client to refuse as it would any
/// input that does not match the tool's format.
pub(crate) fn custom_input(arguments: &str) -> String {
    match serde_json::from_str::<Value>(arguments) {
        Ok(Value::Object(fields)) => match fields.get(CUSTOM_INPUT) {
            Some(Value::String(input)) => input.clone(),
            _ => arguments.to_string(),
        },
        _ => arguments.to_string(),
    }
}

pub(crate) fn output_text(text: String) -> OutputTextContent {
    OutputTextContent {
        annotations: Vec::new(),
        logprobs: Some(Vec::new()),
        text,
    }
}

pub(crate) fn message_item(
    id: String,
    content: Vec<OutputMessageContent>,
    status: OutputStatus,
) -> OutputItem {
    OutputItem::Message(OutputMessage {
        content,
        id,
        role: AssistantRole::Assistant,
        phase: None,
        status,
    })
}

/// A custom tool call item. [`CustomToolCall`] cannot be built field by field outside its
/// crate, so it is read from its wire form.
fn custom_tool_call(
    id: &str,
    call_id: &str,
    target: &ToolTarget,
    input: &str,
) -> Result<CustomToolCall, OpenAIError> {
    let mut item = json!({"id": id, "call_id": call_id, "name": target.name, "input": input});
    if let Some(namespace) = &target.namespace {
        item["namespace"] = json!(namespace);
    }
    serde_json::from_value(item)
        .map_err(|e| server_error(format!("Failed to build a custom tool call item: {e}")))
}

/// The item of a call of `target`, given what it carries: a function's arguments, or a custom
/// tool's input. A function call keeps the tool's namespace apart from its name.
///
/// # Errors
///
/// Returns a `server_error` when the custom tool call item cannot be built.
pub(crate) fn call_item(
    target: &ToolTarget,
    id: String,
    call_id: String,
    payload: String,
    status: OutputStatus,
) -> Result<OutputItem, OpenAIError> {
    if target.custom {
        return custom_tool_call(&id, &call_id, target, &payload).map(OutputItem::CustomToolCall);
    }
    Ok(OutputItem::FunctionCall(FunctionToolCall {
        arguments: payload,
        call_id,
        namespace: target.namespace.clone(),
        name: target.name.clone(),
        id: Some(id),
        status: Some(status),
    }))
}

/// What the item of a whole call of `target` carries, of the `arguments` the model called its
/// function with.
pub(crate) fn call_payload(target: &ToolTarget, arguments: String) -> String {
    if target.custom {
        custom_input(&arguments)
    } else {
        whole_arguments(arguments)
    }
}

/// `completion` as the Response to the request of `context`.
///
/// A completion that ran out of tokens is `incomplete`, and so is the item it was writing when
/// it did: its tool call whose arguments are not whole, or its text when it called no tools,
/// since a chat completion writes its text before its tool calls.
///
/// # Errors
///
/// Returns a `server_error` when the completion has no choice, or calls a tool that was not
/// offered as a function.
pub fn response(
    completion: CreateChatCompletionResponse,
    context: &ResponseContext,
) -> Result<Response, OpenAIError> {
    let CreateChatCompletionResponse { choices, usage, .. } = completion;
    let Some(choice) = choices.into_iter().next() else {
        return Err(server_error(format!(
            "Model '{}' returned a chat completion without a choice.",
            context.model
        )));
    };
    let cut_off = choice.finish_reason == Some(FinishReason::Length);
    let message = choice.message;
    let calls = message.tool_calls.unwrap_or_default();

    let mut output = Vec::with_capacity(calls.len() + 1);
    let mut parts = Vec::new();
    if let Some(text) = message.content.filter(|text| !text.is_empty()) {
        parts.push(OutputMessageContent::OutputText(output_text(text)));
    }
    if let Some(refusal) = message.refusal.filter(|text| !text.is_empty()) {
        parts.push(OutputMessageContent::Refusal(RefusalContent { refusal }));
    }
    if !parts.is_empty() {
        let status = if cut_off && calls.is_empty() {
            OutputStatus::Incomplete
        } else {
            OutputStatus::Completed
        };
        output.push(message_item(message_id(), parts, status));
    }
    for call in calls {
        let ChatCompletionMessageToolCalls::Function(call) = call else {
            return Err(server_error(format!(
                "Model '{}' called a custom tool of the chat completion, which it was not offered: it is offered each tool as a function.",
                context.model
            )));
        };
        let target = context.tool_names.target(&call.function.name);
        let status = if cut_off && !is_whole(&call.function.arguments) {
            OutputStatus::Incomplete
        } else {
            OutputStatus::Completed
        };
        output.push(call_item(
            &target,
            tool_call_item_id(&target),
            if call.id.is_empty() {
                call_id()
            } else {
                call.id
            },
            call_payload(&target, call.function.arguments),
            status,
        )?);
    }

    let now = now();
    let mut response = in_progress(context, response_id(), now);
    response.output = output;
    finish(&mut response, choice.finish_reason, usage.as_ref(), now);
    Ok(response)
}

/// `error`, a chat completion's error, as the error of a failed Response. Its `code` is the
/// provider's own code when it gave one, and otherwise the Responses API code for its kind.
#[must_use]
pub fn error_object(error: &OpenAIError) -> ErrorObject {
    match error {
        OpenAIError::ApiError(api_error) => ErrorObject {
            code: api_error
                .code
                .clone()
                .filter(|code| !code.is_empty())
                .unwrap_or_else(|| {
                    match ApiErrorKind::of(api_error) {
                        Some(ApiErrorKind::RateLimited) => "rate_limit_exceeded",
                        Some(ApiErrorKind::InsufficientQuota) => "insufficient_quota",
                        Some(ApiErrorKind::InvalidRequest) => "invalid_prompt",
                        Some(
                            ApiErrorKind::Authentication
                            | ApiErrorKind::PermissionDenied
                            | ApiErrorKind::NotFound,
                        )
                        | None => "server_error",
                    }
                    .to_string()
                }),
            message: api_error.message.clone(),
        },
        other => ErrorObject {
            code: "server_error".to_string(),
            message: other.to_string(),
        },
    }
}
