/*
Copyright 2024 The Spice.ai OSS Authors
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
use std::{path::Path, pin::Pin};
use tracing_futures::Instrument;

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoice, ChatChoiceStream, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestFunctionMessage, ChatCompletionRequestMessage,
        ChatCompletionRequestSystemMessage, ChatCompletionRequestToolMessage,
        ChatCompletionRequestUserMessage, ChatCompletionRequestUserMessageContent,
        ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CreateChatCompletionRequest,
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse, Role,
    },
};

#[cfg(feature = "mistralrs")]
pub mod mistral;
pub mod nsql;
use indexmap::IndexMap;
use mistralrs::MessageContent;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum LlmRuntime {
    Candle,
    Mistral,
    Openai,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run LLM health check: {source}"))]
    HealthCheckError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to run the LLM chat model: {source}"))]
    FailedToRunModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Local model, expected at {expected_path}, not found"))]
    LocalModelNotFound { expected_path: String },

    #[snafu(display("Local tokenizer, expected at {expected_path}, not found"))]
    LocalTokenizerNotFound { expected_path: String },

    #[snafu(display("Failed to load model: {source}"))]
    FailedToLoadModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load model tokenizer: {source}"))]
    FailedToLoadTokenizer {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to tokenize: {source}"))]
    FailedToTokenize {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported source of model: {source}"))]
    UnknownModelSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("No model from {from} currently supports {task}"))]
    UnsupportedTaskForModel { from: String, task: String },

    #[snafu(display("Invalid value for 'params.spice_tools'"))]
    UnsupportedSpiceToolUseParameterError {},

    #[snafu(display("Runtime does not currently support the {modality} modality"))]
    UnsupportedModalityType { modality: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    }
}

/// Convert a structured [`ChatCompletionRequestMessage`] to the mistral.rs compatible [`RequesstMessage`] type.
#[cfg(feature = "mistralrs")]
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
            let mut map: IndexMap<String, MessageContent> = IndexMap::new();

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
                None => {}
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
                max_completion_tokens: None,
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

pub fn create_hf_model(
    model_id: &str,
    model_type: &Option<String>,
    hf_token_literal: Option<&Secret<String>>,
) -> Result<Box<dyn Chat>> {
    mistral::MistralLlama::from_hf(model_id, model_type.as_deref(), hf_token_literal)
        .map(|x| Box::new(x) as Box<dyn Chat>)
}

#[allow(unused_variables)]
pub fn create_local_model(
    model_weights: &str,
    tokenizer: Option<&str>,
    tokenizer_config: &str,
) -> Result<Box<dyn Chat>> {
    let w = Path::new(&model_weights);
    let t = tokenizer.map(Path::new);
    let tc = Path::new(tokenizer_config);

    if !w.exists() {
        return Err(Error::LocalModelNotFound {
            expected_path: w.to_string_lossy().to_string(),
        });
    }

    if let Some(tokenizer_path) = t {
        if !tokenizer_path.exists() {
            return Err(Error::LocalTokenizerNotFound {
                expected_path: tokenizer_path.to_string_lossy().to_string(),
            });
        }
    }

    if !tc.exists() {
        return Err(Error::LocalTokenizerNotFound {
            expected_path: tc.to_string_lossy().to_string(),
        });
    }

    #[cfg(feature = "mistralrs")]
    {
        mistral::MistralLlama::from(t, w, tc).map(|x| Box::new(x) as Box<dyn Chat>)
    }
    #[cfg(not(feature = "mistralrs"))]
    {
        Err(Error::FailedToRunModel {
            source: "No Chat model feature enabled".into(),
        })
    }
}
