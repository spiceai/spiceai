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

//! The requests of `models/{model}:generateContent`, `:streamGenerateContent` and
//! `:countTokens` (<https://ai.google.dev/api/generate-content#request-body>).
//!
//! A request is read as leniently as Google's parser reads it: fields in camelCase or in the
//! snake case of their proto names, a single value for an array, enum values in any case, and
//! fields this crate does not name, which are kept in [`GenerateContentRequest::other`] rather
//! than refused. Requests from Google's own clients carry such fields (`labels`, `sessionId`).

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::content::Content;
use crate::serde_util::one_or_many;

/// A `generateContent` or `streamGenerateContent` request body. The model is named by the URL,
/// `models/{model}:generateContent`, not by the body.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenerateContentRequest {
    /// The conversation, oldest turn first.
    #[serde(default, deserialize_with = "one_or_many")]
    pub contents: Vec<Content>,
    #[serde(
        default,
        alias = "system_instruction",
        skip_serializing_if = "Option::is_none"
    )]
    pub system_instruction: Option<Content>,
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub tools: Vec<Tool>,
    #[serde(
        default,
        alias = "tool_config",
        skip_serializing_if = "Option::is_none"
    )]
    pub tool_config: Option<ToolConfig>,
    #[serde(
        default,
        alias = "generation_config",
        skip_serializing_if = "Option::is_none"
    )]
    pub generation_config: Option<GenerationConfig>,
    #[serde(
        default,
        alias = "safety_settings",
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub safety_settings: Vec<SafetySetting>,
    /// The name of cached content to answer from, `cachedContents/{id}`.
    #[serde(
        default,
        alias = "cached_content",
        skip_serializing_if = "Option::is_none"
    )]
    pub cached_content: Option<String>,
    /// Every other field, by name: `labels`, `serviceTier`, `store`, and fields that are not in
    /// the public API, such as the `sessionId` the Antigravity CLI sends.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// A tool the model may use: functions the client declares and runs, or a tool Google runs, such
/// as `googleSearch`, `codeExecution` or `urlContext`, kept by name in `other`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    #[serde(
        default,
        alias = "function_declarations",
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub function_declarations: Vec<FunctionDeclaration>,
    /// The tools Google runs, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// A function the model may call.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FunctionDeclaration {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The parameters, an OpenAPI-subset Schema (<https://ai.google.dev/api/caching#Schema>)
    /// whose types are named in upper case (`OBJECT`, `STRING`). It is kept as JSON so that
    /// every keyword reaches a translation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// The parameters as JSON Schema, in place of `parameters`.
    #[serde(
        default,
        alias = "parameters_json_schema",
        skip_serializing_if = "Option::is_none"
    )]
    pub parameters_json_schema: Option<Value>,
    /// The result, an OpenAPI-subset Schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
    /// The result as JSON Schema.
    #[serde(
        default,
        alias = "response_json_schema",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_json_schema: Option<Value>,
    /// `BLOCKING` or `NON_BLOCKING`, which only the Live API reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolConfig {
    #[serde(
        default,
        alias = "function_calling_config",
        skip_serializing_if = "Option::is_none"
    )]
    pub function_calling_config: Option<FunctionCallingConfig>,
}

/// How the model may call the declared functions.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FunctionCallingConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<FunctionCallingMode>,
    /// The functions the model may call, of those declared. Unset or empty is all of them.
    #[serde(
        default,
        alias = "allowed_function_names",
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub allowed_function_names: Vec<String>,
}

/// The `mode` of a [`FunctionCallingConfig`]. It is read in any case, as Google's own examples
/// write it (`"mode": "auto"`), and written in upper case.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FunctionCallingMode {
    ModeUnspecified,
    /// The model answers with text or function calls, as it decides.
    Auto,
    /// The model answers with function calls only.
    Any,
    /// The model answers with text only.
    None,
    /// The model answers with text or function calls, and its calls adhere to the functions'
    /// schemas.
    Validated,
}

impl<'de> Deserialize<'de> for FunctionCallingMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mode = String::deserialize(deserializer)?;
        match mode.to_ascii_uppercase().as_str() {
            "MODE_UNSPECIFIED" => Ok(Self::ModeUnspecified),
            "AUTO" => Ok(Self::Auto),
            "ANY" => Ok(Self::Any),
            "NONE" => Ok(Self::None),
            "VALIDATED" => Ok(Self::Validated),
            _ => Err(de::Error::unknown_variant(
                &mode,
                &["MODE_UNSPECIFIED", "AUTO", "ANY", "NONE", "VALIDATED"],
            )),
        }
    }
}

/// How the model generates its answer (<https://ai.google.dev/api/generate-content#generationconfig>).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenerationConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, alias = "top_p", skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(default, alias = "top_k", skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,
    /// The number of answers to generate.
    #[serde(
        default,
        alias = "candidate_count",
        skip_serializing_if = "Option::is_none"
    )]
    pub candidate_count: Option<u32>,
    #[serde(
        default,
        alias = "max_output_tokens",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<u32>,
    #[serde(
        default,
        alias = "stop_sequences",
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub stop_sequences: Vec<String>,
    #[serde(
        default,
        alias = "presence_penalty",
        skip_serializing_if = "Option::is_none"
    )]
    pub presence_penalty: Option<f32>,
    #[serde(
        default,
        alias = "frequency_penalty",
        skip_serializing_if = "Option::is_none"
    )]
    pub frequency_penalty: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<i32>,
    /// `text/plain`, `application/json`, or `text/x.enum`.
    #[serde(
        default,
        alias = "response_mime_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_mime_type: Option<String>,
    /// The answer's schema, an OpenAPI-subset Schema, kept as JSON.
    #[serde(
        default,
        alias = "response_schema",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_schema: Option<Value>,
    /// The answer's schema as JSON Schema, in place of `responseSchema`.
    #[serde(
        default,
        alias = "response_json_schema",
        alias = "_responseJsonSchema",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_json_schema: Option<Value>,
    /// The modalities of the answer: `TEXT`, `IMAGE`, `AUDIO`.
    #[serde(
        default,
        alias = "response_modalities",
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub response_modalities: Vec<String>,
    #[serde(
        default,
        alias = "thinking_config",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_config: Option<ThinkingConfig>,
}

/// How much the model reasons before it answers.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThinkingConfig {
    /// Whether the answer carries the model's reasoning, as parts marked `thought`.
    #[serde(
        default,
        alias = "include_thoughts",
        skip_serializing_if = "Option::is_none"
    )]
    pub include_thoughts: Option<bool>,
    /// The tokens the model may reason with: `0` turns reasoning off, `-1` lets the model
    /// decide.
    #[serde(
        default,
        alias = "thinking_budget",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_budget: Option<i32>,
    /// `MINIMAL`, `LOW`, `MEDIUM` or `HIGH`.
    #[serde(
        default,
        alias = "thinking_level",
        skip_serializing_if = "Option::is_none"
    )]
    pub thinking_level: Option<String>,
}

/// The threshold at which content of a harm category is blocked.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SafetySetting {
    /// A `HarmCategory`, such as `HARM_CATEGORY_HARASSMENT`.
    pub category: String,
    /// A `HarmBlockThreshold`, such as `BLOCK_ONLY_HIGH`.
    pub threshold: String,
}

/// A `countTokens` request: the contents to count, or a whole `generateContent` request.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CountTokensRequest {
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub contents: Vec<Content>,
    /// A whole request, counted in place of `contents`.
    #[serde(
        default,
        alias = "generate_content_request",
        skip_serializing_if = "Option::is_none"
    )]
    pub generate_content_request: Option<GenerateContentRequest>,
}
