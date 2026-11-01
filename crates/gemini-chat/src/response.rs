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

//! A chat completion as a Gemini response, and a chat completion error as a Gemini API error.

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionMessageToolCalls, CompletionUsage, CreateChatCompletionResponse,
    FinishReason as ChatFinishReason,
};
use chat_api::ApiErrorKind;
use gemini_api::{
    Candidate, Content, FinishReason, FunctionCall, GeminiError, GeminiStatus,
    GenerateContentResponse, Part, UsageMetadata,
};
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use serde_json::{Map, Value};

/// A random ID for an answer, the same in every response of a stream.
pub(crate) fn response_id() -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect()
}

/// The finish reason a chat completion's finish reason means. A completion that stopped to have
/// its function calls run stopped naturally, as Gemini reports it; one without a finish reason is
/// whole, as an unstreamed completion is.
pub(crate) fn finish_reason(finish: Option<ChatFinishReason>) -> FinishReason {
    match finish {
        Some(ChatFinishReason::Length) => FinishReason::MaxTokens,
        Some(ChatFinishReason::ContentFilter) => FinishReason::Safety,
        Some(
            ChatFinishReason::Stop | ChatFinishReason::ToolCalls | ChatFinishReason::FunctionCall,
        )
        | None => FinishReason::Stop,
    }
}

/// The usage a chat completion reported, in Gemini's terms; `None` when it reported none.
///
/// Gemini counts the answer's reasoning apart from its text, and a chat completion counts it in
/// its completion tokens (`completion_tokens_details.reasoning_tokens`), so the reasoning is taken
/// out of `candidatesTokenCount`. A provider that reports more reasoning tokens than completion
/// tokens counted them apart already. Counts the completion did not report, the cached and the
/// reasoning tokens, are left out rather than reported as `0`.
pub(crate) fn usage_metadata(usage: Option<&CompletionUsage>) -> Option<UsageMetadata> {
    let usage = usage?;
    let cached = usage
        .prompt_tokens_details
        .as_ref()
        .and_then(|details| details.cached_tokens);
    let reasoning = usage
        .completion_tokens_details
        .as_ref()
        .and_then(|details| details.reasoning_tokens);
    let candidates = reasoning
        .and_then(|reasoning| usage.completion_tokens.checked_sub(reasoning))
        .unwrap_or(usage.completion_tokens);
    Some(UsageMetadata {
        prompt_token_count: Some(usage.prompt_tokens),
        cached_content_token_count: cached,
        candidates_token_count: Some(candidates),
        tool_use_prompt_token_count: None,
        thoughts_token_count: reasoning,
        total_token_count: Some(usage.total_tokens),
    })
}

/// The `functionCall` part for a call to `name` with `arguments`: their JSON object, or an empty
/// one when they are empty.
///
/// # Errors
///
/// Returns `INTERNAL` naming the function when the arguments are not a JSON object: the Gemini
/// API has no way to return them as they are.
pub(crate) fn function_call(
    model: &str,
    id: String,
    name: String,
    arguments: &str,
) -> Result<Part, GeminiError> {
    let args = if arguments.trim().is_empty() {
        Value::Object(Map::new())
    } else {
        match serde_json::from_str::<Value>(arguments) {
            Ok(args @ Value::Object(_)) => args,
            Ok(_) | Err(_) => {
                return Err(GeminiError::internal(format!(
                    "Model '{model}' called the function '{name}' with arguments that are not a JSON object, which the Gemini API cannot return: {arguments}"
                )));
            }
        }
    };
    Ok(Part::function_call(FunctionCall {
        id: (!id.is_empty()).then_some(id),
        name,
        args: Some(args),
    }))
}

/// A response of the answer `response_id`, with one candidate made of `parts`.
pub(crate) fn response(
    model: &str,
    response_id: &str,
    parts: Vec<Part>,
    finish_reason: Option<FinishReason>,
    usage_metadata: Option<UsageMetadata>,
) -> GenerateContentResponse {
    GenerateContentResponse {
        candidates: vec![Candidate {
            content: Some(Content::model(parts)),
            finish_reason,
            index: Some(0),
            ..Candidate::default()
        }],
        prompt_feedback: None,
        usage_metadata,
        model_version: Some(model.to_string()),
        response_id: Some(response_id.to_string()),
    }
}

/// `completion` as a Gemini response with one candidate: its text first, then a `functionCall`
/// part for each function call.
///
/// # Errors
///
/// Returns `INTERNAL` when the completion has no choice, calls a custom tool, or calls a function
/// with arguments that are not a JSON object.
pub fn generate_content_response(
    model: &str,
    completion: CreateChatCompletionResponse,
) -> Result<GenerateContentResponse, GeminiError> {
    let CreateChatCompletionResponse {
        choices,
        usage: chat_usage,
        ..
    } = completion;
    let Some(choice) = choices.into_iter().next() else {
        return Err(GeminiError::internal(format!(
            "Model '{model}' returned a chat completion without a choice."
        )));
    };
    let message = choice.message;
    let text: String = [message.content, message.refusal]
        .into_iter()
        .flatten()
        .collect();
    let mut parts = Vec::new();
    if !text.is_empty() {
        parts.push(Part::text(text));
    }
    let cut_off = choice.finish_reason == Some(ChatFinishReason::Length);
    for call in message.tool_calls.into_iter().flatten() {
        let ChatCompletionMessageToolCalls::Function(call) = call else {
            return Err(GeminiError::internal(format!(
                "Model '{model}' called a custom tool, which the Gemini API has no part for."
            )));
        };
        match function_call(model, call.id, call.function.name, &call.function.arguments) {
            Ok(part) => parts.push(part),
            // The completion ran out of tokens in the middle of the call's arguments: the answer
            // ends with MAX_TOKENS without the call, which was never made whole.
            Err(_) if cut_off => {}
            Err(error) => return Err(error),
        }
    }
    Ok(response(
        model,
        &response_id(),
        parts,
        Some(finish_reason(choice.finish_reason)),
        usage_metadata(chat_usage.as_ref()),
    ))
}

/// `error`, a chat completion's error, as a Gemini API error of the same kind. A failure to reach
/// the provider or to read its stream is `UNAVAILABLE`, which a client may retry; any other
/// failure of the provider is `INTERNAL`.
#[must_use]
pub fn gemini_error(error: OpenAIError) -> GeminiError {
    match error {
        OpenAIError::InvalidArgument(message) => GeminiError::invalid_argument(message),
        OpenAIError::ApiError(api_error) => {
            let status = match ApiErrorKind::of(&api_error) {
                Some(ApiErrorKind::InvalidRequest) => GeminiStatus::InvalidArgument,
                Some(ApiErrorKind::Authentication) => GeminiStatus::Unauthenticated,
                Some(ApiErrorKind::PermissionDenied) => GeminiStatus::PermissionDenied,
                Some(ApiErrorKind::NotFound) => GeminiStatus::NotFound,
                Some(ApiErrorKind::RateLimited | ApiErrorKind::InsufficientQuota) => {
                    GeminiStatus::ResourceExhausted
                }
                None => GeminiStatus::Internal,
            };
            GeminiError::new(status, api_error.message)
        }
        error @ (OpenAIError::Reqwest(_) | OpenAIError::StreamError(_)) => {
            GeminiError::unavailable(error.to_string())
        }
        other => GeminiError::internal(other.to_string()),
    }
}
