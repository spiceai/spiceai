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

//! The Gemini API request and response types (`models/{model}:generateContent`,
//! `:streamGenerateContent`, `:countTokens`) and the errors it reports, independent of any model
//! provider.
//!
//! The types follow the API reference (<https://ai.google.dev/api/generate-content>): fields
//! are written in camelCase, and read in camelCase or snake case, as Google's parser reads them.
//! A request field this crate does not name is kept, not refused: Google's own clients send
//! fields outside the public API.

use std::pin::Pin;

use futures::Stream;

pub mod content;
pub mod error;
pub mod model;
pub mod request;
pub mod response;
mod serde_util;

pub use content::{
    Blob, CodeExecutionResult, Content, ExecutableCode, FileData, FunctionCall, FunctionResponse,
    FunctionResponsePart, MODEL_ROLE, Part, USER_ROLE,
};
pub use error::{GeminiError, GeminiStatus};
pub use model::{ListModelsResponse, Model};
pub use request::{
    CountTokensRequest, FunctionCallingConfig, FunctionCallingMode, FunctionDeclaration,
    GenerateContentRequest, GenerationConfig, SafetySetting, ThinkingConfig, Tool, ToolConfig,
};
pub use response::{
    BlockReason, Candidate, CountTokensResponse, FinishReason, GenerateContentResponse,
    PromptFeedback, SafetyRating, UsageMetadata,
};

/// The responses of `streamGenerateContent`, each sent as one server-sent event. An error ends
/// the stream; it is sent as the error body ([`GeminiError::body`]).
pub type GenerateContentStream =
    Pin<Box<dyn Stream<Item = Result<GenerateContentResponse, GeminiError>> + Send>>;

#[cfg(test)]
mod tests;
