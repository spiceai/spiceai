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
use std::path::Path;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoice, ChatCompletionRequestAssistantMessage, ChatCompletionRequestFunctionMessage,
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
        ChatCompletionRequestToolMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent, ChatCompletionResponseMessage,
        CreateChatCompletionRequest, CreateChatCompletionResponse, Role,
    },
};

#[cfg(feature = "candle")]
pub mod candle;

#[cfg(feature = "mistralrs")]
pub mod mistral;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum LlmRuntime {
    Candle,
    Mistral,
    Openai,
}

#[derive(Debug, Snafu)]
pub enum Error {
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn chat_complete_message_to_content(message: &ChatCompletionRequestMessage) -> String {
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
                        async_openai::types::ChatCompletionRequestMessageContentPart::Image(i) => {
                            i.image_url.url.clone()
                        }
                    })
                    .collect();
                x.join("\n")
            }
        },
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content,
            ..
        }) => content.clone(),
        ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            content, ..
        }) => content.clone(),
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content,
            ..
        }) => content.clone().unwrap_or_default(),
        ChatCompletionRequestMessage::Function(ChatCompletionRequestFunctionMessage {
            content,
            ..
        }) => content.clone().unwrap_or_default(),
    }
}

#[async_trait]
pub trait Chat: Sync + Send {
    async fn run(&mut self, prompt: String) -> Result<Option<String>>;

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    async fn chat_request(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let model_id = req.model.clone();
        let prompt = req
            .messages
            .iter()
            .map(chat_complete_message_to_content)
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
            id: "42".to_string(),
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
