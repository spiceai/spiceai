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
use async_openai::types::{ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use nsql::SqlGeneration;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use spicepod::component::model::ModelSource;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::{path::Path, pin::Pin};
use tracing_futures::Instrument;

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoice, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestDeveloperMessage,
        ChatCompletionRequestDeveloperMessageContent, ChatCompletionRequestFunctionMessage,
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestToolMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent, ChatCompletionResponseMessage,
        ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
        Role,
    },
};

pub mod mistral;
pub mod nsql;
use crate::streaming_utils::generate_stream_id;
use indexmap::IndexMap;
use mistralrs::MessageContent;

static WEIGHTS_EXTENSIONS: [&str; 7] = [
    ".safetensors",
    ".pth",
    ".pt",
    ".bin",
    ".onyx",
    ".gguf",
    ".ggml",
];

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum LlmRuntime {
    Candle,
    Mistral,
    Openai,
}

#[derive(Debug, Snafu)]
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

/// Attempts to string match a model error to a known error type.
/// Returns None if no match is found.
#[must_use]
pub fn try_map_boxed_error(e: &(dyn std::error::Error + Send + Sync)) -> Option<Error> {
    let err_string = e.to_string().to_ascii_lowercase();
    if err_string.contains("expected file with extension")
        && WEIGHTS_EXTENSIONS
            .iter()
            .any(|ext| err_string.contains(ext))
    {
        Some(Error::ModelMissingWeights {
            extensions: WEIGHTS_EXTENSIONS.join(", "),
        })
    } else if err_string.contains("hf api error") && err_string.contains("status: 404") {
        let file_url = err_string
            .split("url: ")
            .last()
            .map(|url| {
                url.split(' ')
                    .next()
                    .unwrap_or_default()
                    .replace([']', ')'], "")
            })
            .unwrap_or_default();

        if file_url.is_empty() {
            None
        } else {
            Some(Error::ModelFileMissing { file_url })
        }
    } else {
        None
    }
}

/// Re-writes a boxed error to a known error type, if possible.
/// Always returns a boxed error. Returns the original error if no match is found.
#[must_use]
pub fn try_map_boxed_error_to_box(
    e: Box<dyn std::error::Error + Send + Sync>,
) -> Box<dyn std::error::Error + Send + Sync> {
    try_map_boxed_error(&*e).map_or_else(|| e, std::convert::Into::into)
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
                        async_openai::types::ChatCompletionRequestUserMessageContentPart::Text(t) => {
                            t.text.clone()
                        }
                        async_openai::types::ChatCompletionRequestUserMessageContentPart::ImageUrl(
                            i,
                        ) => i.image_url.url.clone(),
                        async_openai::types::ChatCompletionRequestUserMessageContentPart::InputAudio(
                            a
                        ) => a.input_audio.data.clone(),
                    })
                    .collect();
                x.join("\n")
            }
        },
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content,
            ..
        }) => match content {
            async_openai::types::ChatCompletionRequestSystemMessageContent::Text(t) => t.clone(),
            async_openai::types::ChatCompletionRequestSystemMessageContent::Array(parts) => {
                let x: Vec<_> = parts
                    .iter()
                    .map(|p| match p {
                        async_openai::types::ChatCompletionRequestSystemMessageContentPart::Text(t) => {
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
            async_openai::types::ChatCompletionRequestToolMessageContent::Text(t) => t.clone(),
            async_openai::types::ChatCompletionRequestToolMessageContent::Array(parts) => {
                let x: Vec<_> = parts
                    .iter()
                    .map(|p| match p {
                        async_openai::types::ChatCompletionRequestToolMessageContentPart::Text(
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
                            async_openai::types::ChatCompletionRequestAssistantMessageContentPart::Text(t) => {
                                t.text.clone()
                            }
                            async_openai::types::ChatCompletionRequestAssistantMessageContentPart::Refusal(i) => {
                                i.refusal.clone()
                            }
                        })
                        .collect();
                x.join("\n")
            }
            None => unimplemented!("Assistant message with no content is not supported"),
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
                let x: Vec<_> = parts.iter().map(|p| p.text.clone()).collect();
                x.join("\n")
            }
        },
    }
}

/// Convert a structured [`ChatCompletionRequestMessage`] to the mistral.rs compatible [`RequestMessage`] type.
#[must_use]
pub fn message_to_mistral(
    message: &ChatCompletionRequestMessage,
) -> IndexMap<String, MessageContent> {
    use async_openai::types::{
        ChatCompletionRequestSystemMessageContent, ChatCompletionRequestToolMessageContent,
    };
    use either::Either;
    use serde_json::{Value, json};

    match message {
        ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) => {
            let body: MessageContent = match content {
                ChatCompletionRequestUserMessageContent::Text(text) => {
                    either::Either::Left(text.clone())
                }
                ChatCompletionRequestUserMessageContent::Array(array) => {
                    let index_map = array.iter().map(|p| {
                        match p {
                            async_openai::types::ChatCompletionRequestUserMessageContentPart::Text(t) => {
                                ("content".to_string(), Value::String(t.text.clone()))
                            }
                            async_openai::types::ChatCompletionRequestUserMessageContentPart::ImageUrl(i) => {
                                ("image_url".to_string(), Value::String(i.image_url.url.clone()))
                            }
                            async_openai::types::ChatCompletionRequestUserMessageContentPart::InputAudio(a) => {
                                ("input_audio".to_string(), Value::String(a.input_audio.data.clone()))
                            }
                        }

                    }).collect();
                    either::Either::Right(vec![index_map])
                }
            };
            IndexMap::from([
                (String::from("role"), Either::Left(String::from("user"))),
                (String::from("content"), body),
            ])
        }
        ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
            content: ChatCompletionRequestDeveloperMessageContent::Text(text),
            ..
        }) => IndexMap::from([
            (
                String::from("role"),
                Either::Left(String::from("developer")),
            ),
            (String::from("content"), Either::Left(text.clone())),
        ]),
        ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
            content: ChatCompletionRequestDeveloperMessageContent::Array(parts),
            ..
        }) => {
            // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
            let content_json = parts.iter().map(|p| p.text.clone()).collect::<Vec<_>>();
            IndexMap::from([
                (
                    String::from("role"),
                    Either::Left(String::from("developer")),
                ),
                (
                    String::from("content"),
                    Either::Left(json!(content_json).to_string()),
                ),
            ])
        }
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content: ChatCompletionRequestSystemMessageContent::Text(text),
            ..
        }) => IndexMap::from([
            (String::from("role"), Either::Left(String::from("system"))),
            (String::from("content"), Either::Left(text.clone())),
        ]),
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content: ChatCompletionRequestSystemMessageContent::Array(parts),
            ..
        }) => {
            // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
            let content_json = parts
                .iter()
                .map(|p| match p {
                    async_openai::types::ChatCompletionRequestSystemMessageContentPart::Text(t) => {
                        ("text".to_string(), t.text.clone())
                    }
                })
                .collect::<Vec<_>>();
            IndexMap::from([
                (String::from("role"), Either::Left(String::from("system"))),
                (
                    String::from("content"),
                    Either::Left(json!(content_json).to_string()),
                ),
            ])
        }
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content: ChatCompletionRequestToolMessageContent::Text(text),
            tool_call_id,
        }) => IndexMap::from([
            (String::from("role"), Either::Left(String::from("tool"))),
            (String::from("content"), Either::Left(text.clone())),
            (
                String::from("tool_call_id"),
                Either::Left(tool_call_id.clone()),
            ),
        ]),
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content: ChatCompletionRequestToolMessageContent::Array(parts),
            tool_call_id,
        }) => {
            // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
            let content_json = parts
                .iter()
                .map(|p| match p {
                    async_openai::types::ChatCompletionRequestToolMessageContentPart::Text(t) => {
                        ("text".to_string(), t.text.clone())
                    }
                })
                .collect::<Vec<_>>();

            IndexMap::from([
                (String::from("role"), Either::Left(String::from("tool"))),
                (
                    String::from("content"),
                    Either::Left(json!(content_json).to_string()),
                ),
                (
                    String::from("tool_call_id"),
                    Either::Left(tool_call_id.clone()),
                ),
            ])
        }
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content,
            name,
            tool_calls,
            ..
        }) => {
            let mut map: IndexMap<String, MessageContent> = IndexMap::from([(
                String::from("role"),
                Either::Left(String::from("assistant")),
            )]);
            match content {
                Some(ChatCompletionRequestAssistantMessageContent::Text(s)) => {
                    map.insert("content".to_string(), Either::Left(s.clone()));
                }
                Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => {
                    // TODO: This will cause issue for some chat_templates. Tracking: https://github.com/EricLBuehler/mistral.rs/issues/793
                    let content_json= parts.iter().map(|p| match p {
                        async_openai::types::ChatCompletionRequestAssistantMessageContentPart::Text(t) => {
                            ("text".to_string(), t.text.clone())
                        }
                        async_openai::types::ChatCompletionRequestAssistantMessageContentPart::Refusal(i) => {
                            ("refusal".to_string(), i.refusal.clone())
                        }
                    }).collect::<Vec<_>>();
                    map.insert(
                        String::from("content"),
                        Either::Left(json!(content_json).to_string()),
                    );
                }
                None => {
                    // Use Some(""), not None as it is more compatible with many open source `chat_template`s.
                    map.insert("content".to_string(), Either::Left(String::new()));
                }
            }
            if let Some(name) = name {
                map.insert("name".to_string(), Either::Left(name.clone()));
            }
            if let Some(tool_calls) = tool_calls {
                let tool_call_results: Vec<IndexMap<String, Value>> = tool_calls
                    .iter()
                    .filter_map(|t| {
                        let Ok(function) = serde_json::to_value(&t.function) else {
                            tracing::warn!("Invalid function call: {:#?}", t.function);
                            return None;
                        };

                        let mut map = IndexMap::new();
                        map.insert("id".to_string(), Value::String(t.id.to_string()));
                        map.insert("function".to_string(), function);
                        map.insert("type".to_string(), Value::String("function".to_string()));

                        Some(map)
                    })
                    .collect();

                map.insert("tool_calls".to_string(), Either::Right(tool_call_results));
            }
            map
        }
        ChatCompletionRequestMessage::Function(ChatCompletionRequestFunctionMessage {
            content,
            name,
        }) => IndexMap::from([
            (String::from("role"), Either::Left(String::from("function"))),
            (
                "content".to_string(),
                Either::Left(content.clone().unwrap_or_default()),
            ),
            ("name".to_string(), Either::Left(name.clone())),
        ]),
    }
}

#[async_trait]
pub trait Chat: Sync + Send {
    fn as_sql(&self) -> Option<&dyn SqlGeneration>;
    async fn run(&self, prompt: String) -> Result<Option<String>> {
        // BUG FIX: Remove double .instrument(Span::current()) calls that break span propagation
        // The outer .instrument is redundant and interferes with parent span context
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

        // BUG FIX: The stream() call should inherit the current span context automatically
        // No need to explicitly instrument here as it interferes with parent span propagation
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

        // BUG FIX: The run() call should inherit the current span context automatically
        // No need to explicitly instrument here as it interferes with parent span propagation
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
                    content: Some(resp),
                    tool_calls: None,
                    role: Role::System,
                    audio: None,
                    function_call: None,
                    refusal: None,
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

/// Create a model to run locally, via files from Huggingface.
///
/// `model_id` uniquely refers to a Huggingface model.
/// `model_type` is the type of model, if needed to be explicit. Often this can
///    be inferred from the `.model_type` key in a HF's `config.json`, or from the GGUF metadata.
/// `from_gguf` is a path to a GGUF file within the huggingface model repo. If provided, the model will be loaded from this GGUF. This is useful for loading quantized models.
/// `hf_token_literal` is a literal string of the Huggingface API token. If not provided, the token will be read from the HF token cache (i.e. `~/.cache/huggingface/token` or set via `HF_TOKEN_PATH`).
pub async fn create_hf_model(
    model_id: &str,
    model_type: Option<&str>,
    from_gguf: Option<PathBuf>,
    hf_token_literal: Option<&SecretString>,
) -> Result<Arc<dyn Chat>> {
    mistral::MistralLlama::from_hf(model_id, model_type, hf_token_literal, from_gguf)
        .await
        .map(|x| Arc::new(x) as Arc<dyn Chat>)
}

pub async fn create_local_model(
    model_weights: &[String],
    config: Option<&str>,
    tokenizer: Option<&str>,
    tokenizer_config: Option<&str>,
    generation_config: Option<&str>,
    chat_template_literal: Option<&str>,
) -> Result<Arc<dyn Chat>> {
    mistral::MistralLlama::from(
        model_weights
            .iter()
            .map(|p| PathBuf::from_str(p))
            .collect::<Result<Vec<_>, _>>()
            .boxed()
            .map_err(|e| Error::FailedToLoadModel { source: e })?
            .as_slice(),
        config.map(Path::new),
        tokenizer.map(Path::new),
        tokenizer_config.map(Path::new),
        generation_config.map(Path::new),
        chat_template_literal,
    )
    .await
    .map(|x| Arc::new(x) as Arc<dyn Chat>)
}
