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

//! The responses of `generateContent`, `streamGenerateContent` and `countTokens`
//! (<https://ai.google.dev/api/generate-content#generatecontentresponse>).

use serde::{Deserialize, Serialize};

use crate::content::Content;

/// An answer, or, in a stream, one piece of it: each streamed response carries the parts
/// generated since the one before, and the last carries the `finishReason` and the usage.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenerateContentResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<Candidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_feedback: Option<PromptFeedback>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_metadata: Option<UsageMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    /// The ID of the answer, the same in every response of a stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
}

/// One answer the model generated.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Candidate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Content>,
    /// Why the model stopped; unset while it is still generating.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<FinishReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub safety_ratings: Vec<SafetyRating>,
    /// Why the model stopped, in words.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_message: Option<String>,
}

/// Why the model stopped generating.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinishReason {
    FinishReasonUnspecified,
    /// A natural stop, or a stop sequence.
    Stop,
    MaxTokens,
    Safety,
    Recitation,
    Language,
    Other,
    Blocklist,
    ProhibitedContent,
    Spii,
    MalformedFunctionCall,
    ImageSafety,
    ImageProhibitedContent,
    ImageOther,
    NoImage,
    ImageRecitation,
    UnexpectedToolCall,
    TooManyToolCalls,
    MissingThoughtSignature,
    MalformedResponse,
    Escalation,
    PupLimitedDisabled,
    Continuation,
}

/// Why the prompt was blocked, if it was.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PromptFeedback {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<BlockReason>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub safety_ratings: Vec<SafetyRating>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BlockReason {
    BlockReasonUnspecified,
    Safety,
    Other,
    Blocklist,
    ProhibitedContent,
    ImageSafety,
}

/// The probability that content is harmful in one category.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SafetyRating {
    /// A `HarmCategory`, such as `HARM_CATEGORY_HARASSMENT`.
    pub category: String,
    /// A `HarmProbability`: `NEGLIGIBLE`, `LOW`, `MEDIUM` or `HIGH`.
    pub probability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<bool>,
}

/// The tokens an answer used. A count the model did not report is left out.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageMetadata {
    /// The tokens of the prompt, those read from a cache included.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_token_count: Option<u32>,
    /// The tokens of the prompt read from a cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_content_token_count: Option<u32>,
    /// The tokens of the answer, its reasoning left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidates_token_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_prompt_token_count: Option<u32>,
    /// The tokens of the model's reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thoughts_token_count: Option<u32>,
    /// The tokens of the prompt, the reasoning and the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_token_count: Option<u32>,
}

/// A `countTokens` response.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CountTokensResponse {
    pub total_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_content_token_count: Option<u32>,
}
