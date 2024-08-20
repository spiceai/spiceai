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
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
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

#[cfg(feature = "candle")]
pub mod candle;

#[cfg(feature = "mistralrs")]
pub mod mistral;
use mistralrs::RequestMessage;

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
                        async_openai::types::ChatCompletionRequestMessageContentPart::Text(t) => {
                            t.text.clone()
                        }
                        async_openai::types::ChatCompletionRequestMessageContentPart::ImageUrl(
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
        })
        | ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content, ..
        }) => content.clone(),
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content,
            ..
        })
        | ChatCompletionRequestMessage::Function(ChatCompletionRequestFunctionMessage {
            content,
            ..
        }) => content.clone().unwrap_or_default(),
    }
}

/// Convert a structured [`ChatCompletionRequestMessage`] to the mistral.rs compatible [`RequesstMessage`] type.
#[cfg(feature = "mistralrs")]
#[must_use]
pub fn messages_to_mistral(messages: &[ChatCompletionRequestMessage]) -> RequestMessage {
    use either::Either;
    use indexmap::IndexMap;
    use mistralrs::MessageContent;

    let output = messages.iter().map(|message| {
        match message {
            ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
                content, ..
            }) => {
                let body: MessageContent = match content {
                    ChatCompletionRequestUserMessageContent::Text(text) => either::Either::Left(text.clone()),
                    ChatCompletionRequestUserMessageContent::Array(array) => {

                        let v = array.iter().map(|p| {
                            match p {
                                async_openai::types::ChatCompletionRequestMessageContentPart::Text(t) => {
                                    ("text".to_string(), t.text.clone())
                                }
                                async_openai::types::ChatCompletionRequestMessageContentPart::ImageUrl(i) => {
                                    ("image_url".to_string(), i.image_url.url.clone())
                                }
                            }

                        }).collect::<Vec<_>>();
                        let index_map: IndexMap<String, String> = v.into_iter().collect();
                        either::Either::Right(vec![index_map])
                    }
                };
                (String::from("user"), body)
            },
            ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                content,
                ..
            }) => ("system".to_string(), Either::Left(content.clone())),
            | ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                content, tool_call_id
            }) => ("tool".to_string(), Either::Right(vec![IndexMap::from([
                ("content".to_string(), content.clone()),
                ("tool_call_id".to_string(), tool_call_id.clone())
            ])])),
            ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
                content,
                name,
                tool_calls,
                ..
            }) => ("assistant".to_string(), Either::Right(vec![IndexMap::from([
                ("content".to_string(), content.clone().unwrap_or_default()),
                ("name".to_string(), name.clone().unwrap_or_default()),
                ("tool_calls".to_string(), tool_calls.clone().map(|m| serde_json::to_string(&m).unwrap_or_default()).unwrap_or_default())
            ])])),
            | ChatCompletionRequestMessage::Function(ChatCompletionRequestFunctionMessage {
                content,
                name
            }) => ("function".to_string(), Either::Right(vec![IndexMap::from([
                ("content".to_string(), content.clone().unwrap_or_default().clone()),
                ("name".to_string(), name.clone())
            ])])),
        }
    }).collect::<IndexMap<_, _>>();

    RequestMessage::Chat(vec![output])
}

#[async_trait]
pub trait Chat: Sync + Send {
    async fn run(&self, prompt: String) -> Result<Option<String>>;

    /// A basic health check to ensure the model can process future [`Self::run`] requests.
    /// Default implementation is a basic call to [`Self::run`].
    async fn health(&self) -> Result<()> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");
        self.run("health".to_string())
            .instrument(span)
            .await
            .boxed()
            .context(HealthCheckSnafu)?;
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
        })
    }
}

pub fn create_hf_model(
    model_id: &str,
    model_type: Option<String>,
    model_weights: &Option<String>,
    _tokenizer: &Option<String>,
    _tokenizer_config: &Option<String>,
) -> Result<Box<dyn Chat>> {
    if model_type.is_none() && model_weights.is_none() {
        return Err(Error::FailedToLoadModel {
            source: format!("For {model_id} either model type or model weights is required").into(),
        });
    };

    #[cfg(feature = "mistralrs")]
    {
        mistral::MistralLlama::from_hf(
            model_id,
            &model_type.unwrap_or_default(),
            // TODO: Support HF models with non-standard paths.
            // model_weights,
            // tokenizer,
            // tokenizer_config,
        )
        .map(|x| Box::new(x) as Box<dyn Chat>)
    }
    #[cfg(not(feature = "mistralrs"))]
    {
        Err(Error::FailedToRunModel {
            source: "No Chat model feature enabled".into(),
        })
    }
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
