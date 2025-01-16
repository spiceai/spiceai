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
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct HttpConfig {
    /// The total duration of the test.
    pub duration: Duration,

    /// The number of buckets to divide the test duration into.
    pub buckets: usize,

    /// The number of individial HTTP clients to make requests in parallel.
    pub concurrency: usize,

    /// The payloads to send to the component, specifically to be used in [`HttpComponent::send_request`].
    pub payloads: Vec<Arc<str>>,

    /// The HTTP component, within the Spiced instance, to test.
    pub component: HttpComponent,
}

/// A component within the Spiced instance to test for consistency.
///
/// This component must be accessible over HTTP.
#[derive(Clone)]
pub enum HttpComponent {
    Model { model: String, api_base: String },
    Embedding { embedding: String, api_base: String },
}

impl HttpComponent {
    fn api_base(&self) -> String {
        match self {
            HttpComponent::Model { api_base, .. } | HttpComponent::Embedding { api_base, .. } => {
                api_base.clone()
            }
        }
    }

    /// Sends a request to the component and returns the duration of the request.
    /// Payload may be the entire HTTP request body, or a portion of it (dependent of the component).
    pub async fn send_request(&self, client: &Client, payload: &str) -> Result<Duration> {
        let c = OpenAIClient::with_config(OpenAIConfig::default().with_api_base(self.api_base()))
            .with_http_client(client.clone())
            .clone();

        let start_time = Instant::now();
        match self {
            HttpComponent::Model { model, .. } => {
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
