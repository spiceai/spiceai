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

//! [`Content`] and its [`Part`]s, which a request and a response share
//! (<https://ai.google.dev/api/caching#Content>).
//!
//! Fields serialize in camelCase, and are read in camelCase or in the snake case of their proto
//! names, which Google's parser accepts too and its own examples use (`inline_data`,
//! `mime_type`).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::serde_util::one_or_many;

/// The role of a user's turn.
pub const USER_ROLE: &str = "user";
/// The role of a model's turn.
pub const MODEL_ROLE: &str = "model";

/// One turn of a conversation, or a system instruction.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Content {
    /// `user` or `model`. Unset is the user's turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, deserialize_with = "one_or_many")]
    pub parts: Vec<Part>,
}

impl Content {
    /// A model's turn made of `parts`.
    #[must_use]
    pub fn model(parts: Vec<Part>) -> Self {
        Self {
            role: Some(MODEL_ROLE.to_string()),
            parts,
        }
    }
}

/// A part of a turn. It carries one kind of data: `text`, `inlineData`, `fileData`,
/// `functionCall`, `functionResponse`, `executableCode` or `codeExecutionResult`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Part {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Whether the part is the model's reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thought: Option<bool>,
    /// The provider's opaque record of the reasoning behind the part.
    #[serde(
        default,
        alias = "thought_signature",
        skip_serializing_if = "Option::is_none"
    )]
    pub thought_signature: Option<String>,
    #[serde(
        default,
        alias = "inline_data",
        skip_serializing_if = "Option::is_none"
    )]
    pub inline_data: Option<Blob>,
    #[serde(default, alias = "file_data", skip_serializing_if = "Option::is_none")]
    pub file_data: Option<FileData>,
    #[serde(
        default,
        alias = "function_call",
        skip_serializing_if = "Option::is_none"
    )]
    pub function_call: Option<FunctionCall>,
    #[serde(
        default,
        alias = "function_response",
        skip_serializing_if = "Option::is_none"
    )]
    pub function_response: Option<FunctionResponse>,
    #[serde(
        default,
        alias = "executable_code",
        skip_serializing_if = "Option::is_none"
    )]
    pub executable_code: Option<ExecutableCode>,
    #[serde(
        default,
        alias = "code_execution_result",
        skip_serializing_if = "Option::is_none"
    )]
    pub code_execution_result: Option<CodeExecutionResult>,
    /// Every other field, by name: metadata such as `videoMetadata` or `partMetadata`, and kinds
    /// of data not named above, such as a server-side `toolCall`.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

impl Part {
    /// A text part.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            ..Self::default()
        }
    }

    /// A function call part.
    #[must_use]
    pub fn function_call(call: FunctionCall) -> Self {
        Self {
            function_call: Some(call),
            ..Self::default()
        }
    }
}

/// Inline media bytes.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Blob {
    /// The IANA media type of the data, such as `image/png`.
    #[serde(alias = "mime_type")]
    pub mime_type: String,
    /// The data, base64-encoded.
    pub data: String,
    #[serde(
        default,
        alias = "display_name",
        skip_serializing_if = "Option::is_none"
    )]
    pub display_name: Option<String>,
}

/// Data by URI, such as a file of the Gemini Files API.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileData {
    #[serde(default, alias = "mime_type", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(alias = "file_uri")]
    pub file_uri: String,
    #[serde(
        default,
        alias = "display_name",
        skip_serializing_if = "Option::is_none"
    )]
    pub display_name: Option<String>,
}

/// A call the model made to a declared function.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FunctionCall {
    /// The call's ID, which the [`FunctionResponse`] that answers it repeats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    /// The arguments, a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,
}

/// The result of a [`FunctionCall`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FunctionResponse {
    /// The ID of the call this answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    /// The result, a JSON object.
    pub response: Value,
    /// Media the function returned beside `response`.
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub parts: Vec<FunctionResponsePart>,
}

/// Media a function returned.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FunctionResponsePart {
    #[serde(
        default,
        alias = "inline_data",
        skip_serializing_if = "Option::is_none"
    )]
    pub inline_data: Option<Blob>,
    #[serde(default, alias = "file_data", skip_serializing_if = "Option::is_none")]
    pub file_data: Option<FileData>,
}

/// Code the model wrote for the `codeExecution` tool to run.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutableCode {
    /// `PYTHON`.
    pub language: String,
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// The result of running an [`ExecutableCode`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodeExecutionResult {
    /// `OUTCOME_OK`, `OUTCOME_FAILED` or `OUTCOME_DEADLINE_EXCEEDED`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}
