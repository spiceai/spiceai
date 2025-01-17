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

use anyhow::{anyhow, Result};
use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateEmbeddingRequest,
        EncodingFormat,
    },
    Client as OpenAIClient,
};
use reqwest::Client;
use std::time::{Duration, Instant};

/// A component within the Spiced instance to test for consistency.
///
/// This component must be accessible over HTTP.
#[derive(Clone)]
pub enum HttpComponent {
    Model {
        model: String,
        api_base: String,
    },
    Embedding {
        embedding: String,
        api_base: String,
    },
    Generic {
        // For a generic component, must know full URL.
        http_url: String,
        component_name: String,
    },
}

impl HttpComponent {
    fn api_base(&self) -> String {
        match self {
            HttpComponent::Generic {
                http_url: api_base, ..
            }
            | HttpComponent::Model { api_base, .. }
            | HttpComponent::Embedding { api_base, .. } => api_base.clone(),
        }
    }

    #[must_use]
    pub fn component_name(&self) -> String {
        match self {
            HttpComponent::Generic {
                component_name: model,
                ..
            }
            | HttpComponent::Model { model, .. } => model.clone(),
            HttpComponent::Embedding { embedding, .. } => embedding.clone(),
        }
    }

    #[must_use]
    pub fn with_api_base(self, api_base: String) -> Self {
        match self {
            HttpComponent::Model { model, .. } => HttpComponent::Model { model, api_base },
            HttpComponent::Embedding { embedding, .. } => HttpComponent::Embedding {
                embedding,
                api_base,
            },
            HttpComponent::Generic { component_name, .. } => HttpComponent::Generic {
                http_url: api_base,
                component_name,
            },
        }
    }

    #[must_use]
    pub fn with_component_name(self, component_name: String) -> Self {
        match self {
            HttpComponent::Model { api_base, .. } => HttpComponent::Model {
                model: component_name,
                api_base,
            },
            HttpComponent::Embedding { api_base, .. } => HttpComponent::Embedding {
                embedding: component_name,
                api_base,
            },
            HttpComponent::Generic {
                http_url: http_base,
                ..
            } => HttpComponent::Generic {
                http_url: http_base,
                component_name,
            },
        }
    }

    /// Sends a request to the component and returns the duration of the request.
    /// Payload may be the entire HTTP request body, or a portion of it (dependent of the component).
    pub async fn send_request(&self, client: &Client, payload: &str) -> Result<Duration> {
        let start_time = Instant::now();
        match self {
            HttpComponent::Generic {
                http_url: http_base,
                ..
            } => {
                let req = client.post(http_base.clone())
                    .body(payload.to_string()).send()
                    .await
                    .map_err(|e| anyhow!("Error received from generic HTTP POST request to {http_base}. Error: {e:?}"))?;

                req.error_for_status()
                    .map_err(|e| anyhow!("Received error status from {http_base}. Error: {e:?}"))?;
            }
            HttpComponent::Model { model, .. } => {
                let c = OpenAIClient::with_config(
                    OpenAIConfig::default().with_api_base(self.api_base()),
                )
                .with_http_client(client.clone())
                .clone();

                let req: CreateChatCompletionRequest =
                    match serde_json::from_str::<CreateChatCompletionRequest>(payload) {
                        Ok(mut req) => {
                            // Ensure the model is overriden.
                            req.model.clone_from(model);
                            req
                        }
                        Err(_) => CreateChatCompletionRequestArgs::default()
                            .model(model.clone())
                            .messages(vec![ChatCompletionRequestMessage::User(
                                ChatCompletionRequestUserMessageArgs::default()
                                    .content(payload.to_string())
                                    .build()
                                    .map_err(|e| {
                                        anyhow!("failed to build user message. Error: {e:?}")
                                    })?,
                            )])
                            .build()
                            .map_err(|e| anyhow!("Failed to build model request. Error: {e:?}"))?,
                    };
                let _ = c.chat().create(req).await?;
            }
            HttpComponent::Embedding { embedding, .. } => {
                let c = OpenAIClient::with_config(
                    OpenAIConfig::default().with_api_base(self.api_base()),
                )
                .with_http_client(client.clone())
                .clone();
                let req: CreateEmbeddingRequest =
                    match serde_json::from_str::<CreateEmbeddingRequest>(payload) {
                        Ok(mut req) => {
                            // Ensure the model is overriden.
                            req.model.clone_from(embedding);
                            req
                        }
                        Err(_) => CreateEmbeddingRequest {
                            model: embedding.clone(),
                            input: async_openai::types::EmbeddingInput::String(payload.to_string()),
                            encoding_format: Some(EncodingFormat::Float),
                            user: None,
                            dimensions: None,
                        },
                    };
                c.embeddings().create(req).await?;
            }
        }
        Ok(start_time.elapsed())
    }
}
