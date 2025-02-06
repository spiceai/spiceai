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
use async_openai::types::{
    ChatCompletionRequestAssistantMessageContent, ChatCompletionRequestSystemMessageArgs,
    CreateChatCompletionRequestArgs,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use nsql::SqlGeneration;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use secrecy::Secret;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::path::PathBuf;
use std::str::FromStr;
use std::{path::Path, pin::Pin};
use tracing_futures::Instrument;

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoice, ChatChoiceStream, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestDeveloperMessage, ChatCompletionRequestDeveloperMessageContent,
        ChatCompletionRequestFunctionMessage, ChatCompletionRequestMessage,
        ChatCompletionRequestSystemMessage, ChatCompletionRequestToolMessage,
        ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
        ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CreateChatCompletionRequest,
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse, Role,
    },
};

pub mod mistral;
pub mod nsql;
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
    #[snafu(display("Failed to check the status of the model.\nAn error occurred: {source}\nVerify the model configuration."))]
    HealthCheckError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to run the model.\nAn error occurred: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    FailedToRunModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to find the Local model at '{expected_path}'.\nVerify the model exists, and try again."))]
    LocalModelNotFound { expected_path: String },

    #[snafu(display("Failed to find the Local model config at '{expected_path}'.\nVerify the model config exists, and try again."))]
    LocalModelConfigNotFound { expected_path: String },

    #[snafu(display("Failed to find the Local tokenizer at '{expected_path}'.\nVerify the tokenizer exists, and try again."))]
    LocalTokenizerNotFound { expected_path: String },

    #[snafu(display("Failed to load the model.\nAn error occurred: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    FailedToLoadModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported value for `model_type` parameter.\n{source}\n Verify the `model_type` parameter, and try again"))]
    UnsupportedModelType {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("The specified model identifier '{model}' is not valid for the source '{model_source}'.\nVerify the model exists, and try again."))]
    ModelNotFound { model: String, model_source: String },

    #[snafu(display("Failed to load model tokenizer.\nAn error occurred: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    FailedToLoadTokenizer {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("An unsupported model source was specified in the 'from' parameter: '{from}'.\nSpecify a valid source, like 'openai', and try again.\nFor details, visit: https://spiceai.org/docs/components/models"))]
    UnknownModelSource { from: String },

    #[snafu(display("The specified model, '{from}', does not support executing the task '{task}'.\nSelect a different model or task, and try again."))]
    UnsupportedTaskForModel { from: String, task: String },

    #[snafu(display("Invalid value for parameter {param}. {message}"))]
    InvalidParamError { param: String, message: String },

    #[snafu(display("Failed to find weights for the model.\nExpected tensors with a file extension of: {extensions}.\nVerify the model is correctly configured, and try again."))]
    ModelMissingWeights { extensions: String },

    #[snafu(display("Failed to load a file specified for the model.\nCould not find the file: {file_url}.\nVerify the `files` parameters for the model, and try again."))]
    ModelFileMissing { file_url: String },
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
        }
        .clone(),
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
            None => todo!(),
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
#[allow(clippy::too_many_lines)]
pub fn message_to_mistral(
    message: &ChatCompletionRequestMessage,
) -> IndexMap<String, MessageContent> {
    use async_openai::types::{
        ChatCompletionRequestSystemMessageContent, ChatCompletionRequestToolMessageContent,
    };
    use either::Either;
    use serde_json::{json, Value};

    match message {
        ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) => {
            let body: MessageContent = match content {
                ChatCompletionRequestUserMessageContent::Text(text) => {
                    either::Either::Left(text.clone())
                }
                ChatCompletionRequestUserMessageContent::Array(array) => {
                    let v = array.iter().map(|p| {
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

                    }).collect::<Vec<_>>();
                    let index_map: IndexMap<String, Value> = v.into_iter().collect();
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
            };
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
                Either::Left(content.clone().unwrap_or_default().clone()),
            ),
            ("name".to_string(), Either::Left(name.clone())),
        ]),
    }
}

#[async_trait]
pub trait Chat: Sync + Send {
    fn as_sql(&self) -> Option<&dyn SqlGeneration>;
    async fn run(&self, prompt: String) -> Result<Option<String>> {
        let span = tracing::Span::current();

        async move {
            let req = CreateChatCompletionRequestArgs::default()
                .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                    .content(prompt)
                    .build()
                    .boxed()
                    .context(FailedToLoadTokenizerSnafu)?
                    .into()])
                .build()
                .boxed()
                .context(FailedToLoadModelSnafu)?;

            let resp = self
                .chat_request(req)
                .await
                .boxed()
                .context(FailedToRunModelSnafu)?;

            Ok(resp
                .choices
                .into_iter()
                .next()
                .and_then(|c| c.message.content))
        }
        .instrument(span)
        .await
    }

    /// A basic health check to ensure the model can process future [`Self::run`] requests.
    /// Default implementation is a basic call to [`Self::run`].
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

    #[allow(deprecated)]
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

        let mut stream = self.stream(prompt).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        let strm_id: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let strm = stream! {
            let mut i  = 0;
            while let Some(msg) = stream.next().await {
                let choice = ChatChoiceStream {
                    delta: ChatCompletionStreamResponseDelta {
                        content: Some(msg?.unwrap_or_default()),
                        tool_calls: None,
                        role: Some(Role::System),
                        function_call: None,
                        refusal: None,
                    },
                    index: i,
                    finish_reason: None,
                    logprobs: None,
                };

            yield Ok(CreateChatCompletionStreamResponse {
                id: format!("{}-{}-{i}", model_id.clone(), strm_id),
                choices: vec![choice],
                model: model_id.clone(),
                created: 0,
                system_fingerprint: None,
                object: "list".to_string(),
                usage: None,
                service_tier: None,
            });
            i+=1;
        }};

        Ok(Box::pin(strm.map_err(|e: Error| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })))
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    #[allow(deprecated)]
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
            id: format!(
                "{}-{}",
                model_id.clone(),
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(10)
                    .map(char::from)
                    .collect::<String>()
            ),
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
pub fn create_hf_model(
    model_id: &str,
    model_type: Option<&str>,
    from_gguf: Option<PathBuf>,
    hf_token_literal: Option<&Secret<String>>,
) -> Result<Box<dyn Chat>> {
    mistral::MistralLlama::from_hf(model_id, model_type, hf_token_literal, from_gguf)
        .map(|x| Box::new(x) as Box<dyn Chat>)
}

#[allow(unused_variables)]
pub fn create_local_model(
    model_weights: &[String],
    config: Option<&str>,
    tokenizer: Option<&str>,
    tokenizer_config: Option<&str>,
    generation_config: Option<&str>,
    chat_template_literal: Option<&str>,
) -> Result<Box<dyn Chat>> {
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
    .map(|x| Box::new(x) as Box<dyn Chat>)
}
