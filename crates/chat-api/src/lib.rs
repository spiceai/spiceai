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

//! The chat-completion contract.
//!
//! A chat model answers a completion request, optionally streaming. Implemented by
//! provider crates; called by the runtime's `/v1/chat` endpoint, NSQL, the tool loop and
//! search — none of which name a provider.

use std::pin::Pin;

use async_openai::{
    error::{ApiError, OpenAIError},
    types::chat::{
        ChatChoice, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestDeveloperMessage,
        ChatCompletionRequestDeveloperMessageContent,
        ChatCompletionRequestDeveloperMessageContentPart, ChatCompletionRequestFunctionMessage,
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestToolMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageArgs, ChatCompletionRequestUserMessageContent,
        ChatCompletionResponseMessage, ChatCompletionResponseStream, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs, CreateChatCompletionResponse, Role,
    },
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use snafu::Snafu;
use spicepod::component::model::ModelSource;
use tracing_futures::Instrument;

use crate::streaming_utils::generate_stream_id;

pub mod streaming_utils;

#[derive(Debug, Snafu)]
// Selectors are `pub`: provider crates construct e.g. `FailedToRunModelSnafu` across the
// crate boundary, which the in-crate definition never had to allow.
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Failed to check the status of the model. An error occurred: {source} Verify the model configuration."
    ))]
    HealthCheckError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to run the model. An error occurred: {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToRunModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to find the Local model at '{expected_path}'. Verify the model exists, and try again."
    ))]
    LocalModelNotFound { expected_path: String },

    #[snafu(display(
        "Failed to find the Local model config at '{expected_path}'. Verify the model config exists, and try again."
    ))]
    LocalModelConfigNotFound { expected_path: String },

    #[snafu(display(
        "Failed to find the Local tokenizer at '{expected_path}'. Verify the tokenizer exists, and try again."
    ))]
    LocalTokenizerNotFound { expected_path: String },

    #[snafu(display(
        "Failed to load the model. An error occurred: {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToLoadModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unsupported value for `model_type` parameter. {source}  Verify the `model_type` parameter, and try again"
    ))]
    UnsupportedModelType {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The specified model identifier '{model}' is not valid for the source '{model_source}'. Verify the model exists, and try again."
    ))]
    ModelNotFound { model: String, model_source: String },

    #[snafu(display(
        "A model identifier must be provided for source '{model_source}' via `from: {model_source}:<model_id>`"
    ))]
    ModelNotProvided { model_source: String },

    #[snafu(display(
        "Failed to load model tokenizer. An error occurred: {source} Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToLoadTokenizer {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "An unsupported model source was specified in the 'from' parameter: '{from}'. Specify a valid source, like 'openai', and try again. For details, visit: https://spiceai.org/docs/components/models"
    ))]
    UnknownModelSource { from: String },

    #[snafu(display(
        "The specified model, '{from}', does not support executing the task '{task}'. Select a different model or task, and try again."
    ))]
    UnsupportedTaskForModel { from: String, task: String },

    #[snafu(display("Invalid value for parameter {param}. {message}"))]
    InvalidParamValueError { param: String, message: String },

    #[snafu(display("Expected `param.{param_key}`, but it was not provided"))]
    MissingParamError { param_key: &'static str },

    #[snafu(display(
        "Failed to find weights for the model. Expected tensors with a file extension of: {extensions}. Verify the model is correctly configured, and try again."
    ))]
    ModelMissingWeights { extensions: String },

    #[snafu(display(
        "Failed to load a file specified for the model. Could not find the file: {file_url}. Verify the `files` parameters for the model, and try again."
    ))]
    ModelFileMissing { file_url: String },

    #[snafu(display(
        "Refusing to load model weight file '{path}': '.{extension}' is a Python pickle format \
         that executes arbitrary code on load (CVE-class: untrusted pickle deserialization → RCE). \
         Convert the model to `.safetensors` or `.gguf`, or set `params.trust_pickle: true` if the \
         file source is fully trusted."
    ))]
    UnsafePickleWeight { path: String, extension: String },

    #[snafu(display(
        "Invalid parameters for model '{model}': {source} Verify the model parameters, and try again."
    ))]
    ModelParameterFailed {
        model: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Model '{from}' does not support the OpenAI Responses API. Change the model provider to 'openai' to use the Responses API or use the Chat Completions API."
    ))]
    ResponsesNotSupported { from: ModelSource },

    #[snafu(display(
        "The tool '{tool}' was not found. Verify the Spicepod configuration, and view the tools documentation at https://spiceai.org/docs/components/tools"
    ))]
    ToolNotFound { tool: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Default)]
pub struct QueryGenerationContext {
    pub failed_attempts: Vec<FailedAttempt>,
}

pub struct FailedAttempt {
    pub attempted_query: String,
    pub error_message: String,
}

impl FailedAttempt {
    #[must_use]
    pub fn new(attempted_query: String, error_message: String) -> Self {
        Self {
            attempted_query,
            error_message,
        }
    }
}

/// Additional methods (beyond [`Chat`]), whereby a model can provide improved results for SQL code generation.
pub trait SqlGeneration: Sync + Send {
    /// Builds the completion request that asks the model for SQL.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAIError`] when the request cannot be constructed.
    fn create_request_for_query(
        &self,
        model_id: &str,
        query: &str,
        context: &QueryGenerationContext,
    ) -> Result<CreateChatCompletionRequest, OpenAIError>;

    /// Extracts the generated SQL from the model's response, or `None` when it produced
    /// nothing usable.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAIError`] when the response cannot be read.
    fn parse_response(
        &self,
        resp: CreateChatCompletionResponse,
    ) -> Result<Option<String>, OpenAIError>;
}

/// Convert a structured [`ChatCompletionRequestMessage`] to a basic string. Useful for basic
/// [`Chat::run`] but reduces optional configuration provided by callers.
#[must_use]
pub fn message_to_content(message: &ChatCompletionRequestMessage) -> String {
    match message {
        ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) => match content {
            ChatCompletionRequestUserMessageContent::Text(text) => text.clone(),
            ChatCompletionRequestUserMessageContent::Array(array) => {
                let x: Vec<_> = array
                    .iter()
                    .map(|p| match p {
                        async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::Text(t) => {
                            t.text.clone()
                        }
                        async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::ImageUrl(
                            i,
                        ) => i.image_url.url.clone(),
                        async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::InputAudio(
                            a
                        ) => a.input_audio.data.clone(),
                        async_openai::types::chat::ChatCompletionRequestUserMessageContentPart::File(
                            f
                        ) => serde_json::to_string(&f.file).unwrap_or_default(),
                    })
                    .collect();
                x.join("\n")
            }
        },
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content,
            ..
        }) => match content {
            async_openai::types::chat::ChatCompletionRequestSystemMessageContent::Text(t) => {
                t.clone()
            }
            async_openai::types::chat::ChatCompletionRequestSystemMessageContent::Array(parts) => {
                let x: Vec<_> = parts
                    .iter()
                    .map(|p| match p {
                        async_openai::types::chat::ChatCompletionRequestSystemMessageContentPart::Text(t) => {
                            t.text.clone()
                        }
                    })
                    .collect();
                x.join("\n")
            }
        },
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content, ..
        }) => match content {
            async_openai::types::chat::ChatCompletionRequestToolMessageContent::Text(t) => {
                t.clone()
            }
            async_openai::types::chat::ChatCompletionRequestToolMessageContent::Array(parts) => {
                let x: Vec<_> = parts
                    .iter()
                    .map(|p| match p {
                        async_openai::types::chat::ChatCompletionRequestToolMessageContentPart::Text(
                            t,
                        ) => t.text.clone(),
                    })
                    .collect();
                x.join("\n")
            }
        },
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content,
            ..
        }) => match content {
            Some(ChatCompletionRequestAssistantMessageContent::Text(s)) => s.clone(),
            Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => {
                let x: Vec<_> = parts
                        .iter()
                        .map(|p| match p {
                            async_openai::types::chat::ChatCompletionRequestAssistantMessageContentPart::Text(t) => {
                                t.text.clone()
                            }
                            async_openai::types::chat::ChatCompletionRequestAssistantMessageContentPart::Refusal(i) => {
                                i.refusal.clone()
                            }
                        })
                        .collect();
                x.join("\n")
            }
            // A tool-call-only assistant message carries no text. This converter is
            // lossy by contract — it already discards tool metadata — so the textual
            // content of such a message is genuinely empty, matching the `Function`
            // arm below.
            None => String::new(),
        },
        ChatCompletionRequestMessage::Function(ChatCompletionRequestFunctionMessage {
            content,
            ..
        }) => content.clone().unwrap_or_default(),
        ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
            content,
            ..
        }) => match content {
            ChatCompletionRequestDeveloperMessageContent::Text(t) => t.clone(),
            ChatCompletionRequestDeveloperMessageContent::Array(parts) => {
                let x: Vec<_> = parts
                    .iter()
                    .map(|p| {
                        let ChatCompletionRequestDeveloperMessageContentPart::Text(t) = p;
                        t.text.clone()
                    })
                    .collect();
                x.join("\n")
            }
        },
    }
}

#[async_trait]
pub trait Chat: Sync + Send {
    fn as_sql(&self) -> Option<&dyn SqlGeneration>;
    async fn run(&self, prompt: String) -> Result<Option<String>> {
        // Deliberately not instrumented: the call inherits the caller's span, and adding
        // an explicit `.instrument(Span::current())` here nests a second span that breaks
        // propagation to the parent rather than preserving it.
        self.chat_request(
            CreateChatCompletionRequestArgs::default()
                .messages(vec![
                    ChatCompletionRequestUserMessageArgs::default()
                        .content(prompt)
                        .build()
                        .map_err(|e| Error::FailedToRunModel {
                            source: Box::new(e),
                        })?
                        .into(),
                ])
                .build()
                .map_err(|e| Error::FailedToRunModel {
                    source: Box::new(e),
                })?,
        )
        .await
        .map_err(|e| Error::FailedToRunModel {
            source: Box::new(e),
        })
        .map(|resp| {
            resp.choices
                .into_iter()
                .next()
                .and_then(|c| c.message.content)
        })
    }

    /// A basic health check to ensure the model can process future [`Self::run`]
    /// requests. Default implementation is a basic call to [`Self::run`].
    async fn health(&self) -> Result<()> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");

        if let Err(e) = self
            .chat_request(CreateChatCompletionRequest {
                // Cannot be set too low. Some providers will error if it cannot complete in < `max_completion_tokens`.
                max_completion_tokens: Some(100),
                messages: vec![ChatCompletionRequestMessage::User(
                    ChatCompletionRequestUserMessage {
                        name: None,
                        content: ChatCompletionRequestUserMessageContent::Text("ping.".to_string()),
                    },
                )],
                ..Default::default()
            })
            .instrument(span.clone())
            .await
        {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(Error::HealthCheckError {
                source: Box::new(e),
            });
        }
        Ok(())
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<String>>> + Send>>> {
        let resp = self.run(prompt).await;
        Ok(Box::pin(stream! { yield resp }))
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let model_id = req.model.clone();
        let prompt = req
            .messages
            .iter()
            .map(message_to_content)
            .collect::<Vec<String>>()
            .join("\n");

        // `stream()` inherits the caller's span; instrumenting it here would interfere
        // with propagation to the parent rather than adding context.
        let stream = self.stream(prompt).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        Ok(crate::streaming_utils::string_stream_to_chat_stream(
            model_id, stream,
        ))
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    #[expect(deprecated)]
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let model_id = req.model.clone();
        let prompt = req
            .messages
            .iter()
            .map(message_to_content)
            .collect::<Vec<String>>()
            .join("\n");

        // `run()` inherits the caller's span; instrumenting it here would interfere with
        // propagation to the parent rather than adding context.
        let choices: Vec<ChatChoice> = match self.run(prompt).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })? {
            Some(resp) => vec![ChatChoice {
                message: ChatCompletionResponseMessage {
                    reasoning_content: None,
                    content: Some(resp),
                    tool_calls: None,
                    role: Role::System,
                    audio: None,
                    function_call: None,
                    refusal: None,
                    annotations: None,
                },
                index: 0,
                finish_reason: None,
                logprobs: None,
            }],
            None => vec![],
        };

        Ok(CreateChatCompletionResponse {
            id: generate_stream_id(&model_id),
            choices,
            model: model_id,
            created: 0,
            system_fingerprint: None,
            object: "list".to_string(),
            usage: None,
            service_tier: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::{
        ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
        ChatCompletionRequestAssistantMessage, FunctionCall,
    };

    /// An assistant message replaying a tool call carries no text content. The
    /// flattener must yield an empty string rather than failing, since clients send
    /// this shape whenever they include tool-call history in a request.
    #[test]
    #[expect(deprecated)]
    fn tool_call_only_assistant_message_flattens_to_empty_text() {
        let message =
            ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
                content: None,
                refusal: None,
                name: None,
                audio: None,
                tool_calls: Some(vec![ChatCompletionMessageToolCalls::Function(
                    ChatCompletionMessageToolCall {
                        id: "call_123".to_string(),
                        function: FunctionCall {
                            name: "lookup".to_string(),
                            arguments: "{\"q\":\"spice\"}".to_string(),
                        },
                    },
                )]),
                function_call: None,
            });

        assert_eq!(message_to_content(&message), "");
    }
}
