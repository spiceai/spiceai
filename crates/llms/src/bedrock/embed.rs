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

use crate::bedrock::{BedrockClient, CohereEmbedRequest, CohereEmbedResponse, TitanEmbedRequest, TitanEmbedResponse};
use crate::embeddings::{Embed, Error as EmbedError, Result as EmbedResult};
use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingResponse, Embedding, EmbeddingInput, EmbeddingUsage,
    EmbeddingVector, EncodingFormat,
};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::primitives::Blob;
use snafu::ResultExt;
use std::fmt::Debug;
use tracing::{debug, warn};

const TITAN_TEXT_EMBED_V1: &str = "amazon.titan-embed-text-v1";
const TITAN_TEXT_EMBED_V2: &str = "amazon.titan-embed-text-v2:0";
const COHERE_EMBED_ENGLISH_V3: &str = "cohere.embed-english-v3";
const COHERE_EMBED_MULTILINGUAL_V3: &str = "cohere.embed-multilingual-v3";

const MAX_TITAN_INPUT_LENGTH: usize = 8192; // tokens
const MAX_COHERE_INPUT_LENGTH: usize = 2048; // characters
const MAX_COHERE_TEXTS_PER_REQUEST: usize = 96;

#[derive(Debug, Clone)]
pub struct BedrockEmbed {
    client: BedrockClient,
    model_id: String,
    dimensions: Option<u32>,
    normalize: bool,
    truncate: Option<String>,
    input_type: String,
    total_tokens: std::sync::Arc<std::sync::atomic::AtomicU32>,
}

impl BedrockEmbed {
    pub fn new(
        client: BedrockClient,
        model_id: String,
        dimensions: Option<u32>,
        normalize: bool,
        truncate: Option<String>,
        input_type: Option<String>,
    ) -> Self {
        let default_input_type = if model_id.starts_with("cohere") {
            "search_document".to_string()
        } else {
            "text".to_string()
        };

        Self {
            client,
            model_id,
            dimensions,
            normalize,
            truncate,
            input_type: input_type.unwrap_or(default_input_type),
            total_tokens: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }

    fn is_titan_model(&self) -> bool {
        self.model_id.starts_with("amazon.titan-embed")
    }

    fn is_cohere_model(&self) -> bool {
        self.model_id.starts_with("cohere.embed")
    }

    async fn embed_titan(&self, texts: Vec<String>) -> EmbedResult<Vec<Vec<f32>>> {
        let mut results = Vec::new();
        
        for text in texts {
            // For Titan models, we need to be more careful about token limits
            // This is still an approximation as we don't have access to the actual tokenizer
            let estimated_tokens = text.split_whitespace().count();
            let truncated_text = if estimated_tokens > MAX_TITAN_INPUT_LENGTH {
                warn!("Truncating input text from estimated {} to {} tokens for Titan model", estimated_tokens, MAX_TITAN_INPUT_LENGTH);
                text.split_whitespace()
                    .take(MAX_TITAN_INPUT_LENGTH)
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                text
            };

            let request = TitanEmbedRequest {
                input_text: truncated_text,
                normalize: Some(self.normalize),
                dimensions: self.dimensions,
                embedding_types: Some(vec!["float".to_string()]),
            };

            let body = serde_json::to_string(&request)
                .boxed()
                .map_err(|e| EmbedError::FailedToPrepareInput { source: e })?;

            debug!("Invoking Titan model {} with request: {}", self.model_id, body);

            let response = self
                .client
                .client
                .invoke_model()
                .model_id(&self.model_id)
                .body(Blob::new(body.as_bytes()))
                .content_type("application/json")
                .send()
                .await
                .boxed()
                .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

            let response_body = response.body().as_ref();
            let titan_response: TitanEmbedResponse = serde_json::from_slice(response_body)
                .boxed()
                .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

            // Track actual token count from the response
            self.total_tokens.fetch_add(
                titan_response.input_text_token_count,
                std::sync::atomic::Ordering::Relaxed,
            );

            results.push(titan_response.embedding);
        }

        Ok(results)
    }

    async fn embed_cohere(&self, texts: Vec<String>) -> EmbedResult<Vec<Vec<f32>>> {
        let mut all_results = Vec::new();
        
        // Process texts in batches to respect Cohere's limits
        for batch in texts.chunks(MAX_COHERE_TEXTS_PER_REQUEST) {
            let truncated_texts: Vec<String> = batch
                .iter()
                .map(|text| {
                    if text.len() > MAX_COHERE_INPUT_LENGTH {
                        warn!("Truncating input text from {} to {} characters for Cohere model", text.len(), MAX_COHERE_INPUT_LENGTH);
                        text.chars().take(MAX_COHERE_INPUT_LENGTH).collect::<String>()
                    } else {
                        text.clone()
                    }
                })
                .collect();

            let request = CohereEmbedRequest {
                texts: truncated_texts.clone(),
                input_type: self.input_type.clone(),
                truncate: self.truncate.clone(),
                embedding_types: Some(vec!["float".to_string()]),
            };

            let body = serde_json::to_string(&request)
                .boxed()
                .map_err(|e| EmbedError::FailedToPrepareInput { source: e })?;

            debug!("Invoking Cohere model {} with request: {}", self.model_id, body);

            let response = self
                .client
                .client
                .invoke_model()
                .model_id(&self.model_id)
                .body(Blob::new(body.as_bytes()))
                .content_type("application/json")
                .send()
                .await
                .boxed()
                .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

            let response_body = response.body().as_ref();
            let cohere_response: CohereEmbedResponse = serde_json::from_slice(response_body)
                .boxed()
                .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

            // Estimate token count for Cohere models (approximate)
            let estimated_tokens: u32 = truncated_texts
                .iter()
                .map(|text| (text.split_whitespace().count() as f32 * 1.3) as u32) // Rough estimate
                .sum();
            self.total_tokens.fetch_add(
                estimated_tokens,
                std::sync::atomic::Ordering::Relaxed,
            );

            all_results.extend(cohere_response.embeddings);
        }

        Ok(all_results)
    }

    fn convert_input_to_texts(&self, input: &EmbeddingInput) -> Vec<String> {
        match input {
            EmbeddingInput::String(text) => vec![text.clone()],
            EmbeddingInput::StringArray(texts) => texts.clone(),
            EmbeddingInput::ArrayOfIntegerArray(arrays) => {
                // Convert token arrays to string representation
                warn!("Converting token arrays to text representation for Bedrock models. This may not accurately represent the original text.");
                arrays
                    .iter()
                    .map(|tokens| {
                        tokens
                            .iter()
                            .map(|token| token.to_string())
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect()
            }
            EmbeddingInput::IntegerArray(tokens) => {
                // Convert single token array to string representation
                warn!("Converting token array to text representation for Bedrock models. This may not accurately represent the original text.");
                vec![tokens
                    .iter()
                    .map(|token| token.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")]
            }
        }
    }
}

#[async_trait]
impl Embed for BedrockEmbed {
    async fn embed(&self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        let texts = self.convert_input_to_texts(&input);
        
        if texts.is_empty() {
            return Ok(vec![]);
        }

        // Reset token counter for this request
        self.total_tokens.store(0, std::sync::atomic::Ordering::Relaxed);

        if self.is_titan_model() {
            self.embed_titan(texts).await
        } else if self.is_cohere_model() {
            self.embed_cohere(texts).await
        } else {
            Err(EmbedError::UnsupportedTaskForModel {
                from: self.model_id.clone(),
                task: "embedding".to_string(),
            })
        }
    }

    async fn health(&self) -> EmbedResult<()> {
        self.embed(EmbeddingInput::String("health check".to_string()))
            .await
            .map(|_| ())
    }

    fn size(&self) -> i32 {
        match self.model_id.as_str() {
            TITAN_TEXT_EMBED_V1 => match self.dimensions {
                Some(256) => 256,
                Some(512) => 512,
                Some(1024) | None => 1024,
                _ => 1024,
            },
            TITAN_TEXT_EMBED_V2 => match self.dimensions {
                Some(256) => 256,
                Some(512) => 512,
                Some(1024) | None => 1024,
                _ => 1024,
            },
            COHERE_EMBED_ENGLISH_V3 | COHERE_EMBED_MULTILINGUAL_V3 => 1024,
            _ => -1, // Unknown model, size will be inferred
        }
    }

    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let format = req.encoding_format.unwrap_or_default();
        let result = self.embed(req.input).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        // Get actual token usage from the embedding process
        let total_tokens = self.total_tokens.load(std::sync::atomic::Ordering::Relaxed);

        Ok(CreateEmbeddingResponse {
            object: "list".to_string(),
            model: req.model.clone(),
            data: result
                .into_iter()
                .enumerate()
                .map(|(i, emb)| Embedding {
                    index: i as u32,
                    object: "embedding".to_string(),
                    embedding: match format {
                        EncodingFormat::Float => EmbeddingVector::Float(emb),
                        EncodingFormat::Base64 => {
                            let base64_str = EmbeddingVector::Float(emb).into();
                            EmbeddingVector::Base64(base64_str)
                        }
                    },
                })
                .collect(),
            usage: EmbeddingUsage {
                prompt_tokens: total_tokens,
                total_tokens,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::SdkConfig;

    fn create_mock_bedrock_client() -> BedrockClient {
        let config = SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        BedrockClient::new(&config)
    }

    #[test]
    fn test_bedrock_embed_new() {
        let client = create_mock_bedrock_client();
        let embed = BedrockEmbed::new(
            client,
            TITAN_TEXT_EMBED_V1.to_string(),
            Some(1024),
            true,
            None,
            None,
        );

        assert_eq!(embed.model_id, TITAN_TEXT_EMBED_V1);
        assert_eq!(embed.dimensions, Some(1024));
        assert!(embed.normalize);
        assert_eq!(embed.input_type, "text");
        assert_eq!(embed.total_tokens.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn test_is_titan_model() {
        let client = create_mock_bedrock_client();
        let embed = BedrockEmbed::new(
            client,
            TITAN_TEXT_EMBED_V1.to_string(),
            None,
            true,
            None,
            None,
        );

        assert!(embed.is_titan_model());
        assert!(!embed.is_cohere_model());
    }

    #[test]
    fn test_is_cohere_model() {
        let client = create_mock_bedrock_client();
        let embed = BedrockEmbed::new(
            client,
            COHERE_EMBED_ENGLISH_V3.to_string(),
            None,
            true,
            None,
            Some("search_document".to_string()),
        );

        assert!(!embed.is_titan_model());
        assert!(embed.is_cohere_model());
    }

    #[test]
    fn test_embedding_vector_sizes() {
        let client = create_mock_bedrock_client();
        
        // Test Titan V1 with different dimensions
        let embed_v1_1024 = BedrockEmbed::new(
            client.clone(),
            TITAN_TEXT_EMBED_V1.to_string(),
            Some(1024),
            true,
            None,
            None,
        );
        assert_eq!(embed_v1_1024.size(), 1024);

        let embed_v1_512 = BedrockEmbed::new(
            client.clone(),
            TITAN_TEXT_EMBED_V1.to_string(),
            Some(512),
            true,
            None,
            None,
        );
        assert_eq!(embed_v1_512.size(), 512);

        // Test Cohere
        let embed_cohere = BedrockEmbed::new(
            client,
            COHERE_EMBED_ENGLISH_V3.to_string(),
            None,
            true,
            None,
            Some("search_document".to_string()),
        );
        assert_eq!(embed_cohere.size(), 1024);
    }

    #[test]
    fn test_convert_input_to_texts() {
        let client = create_mock_bedrock_client();
        let embed = BedrockEmbed::new(
            client,
            TITAN_TEXT_EMBED_V1.to_string(),
            None,
            true,
            None,
            None,
        );

        // Test single string
        let input = EmbeddingInput::String("hello world".to_string());
        let texts = embed.convert_input_to_texts(&input);
        assert_eq!(texts, vec!["hello world".to_string()]);

        // Test string array
        let input = EmbeddingInput::StringArray(vec![
            "hello".to_string(),
            "world".to_string(),
        ]);
        let texts = embed.convert_input_to_texts(&input);
        assert_eq!(texts, vec!["hello".to_string(), "world".to_string()]);

        // Test integer array
        let input = EmbeddingInput::ArrayOfIntegerArray(vec![vec![1, 2, 3], vec![4, 5, 6]]);
        let texts = embed.convert_input_to_texts(&input);
        assert_eq!(texts, vec!["1 2 3".to_string(), "4 5 6".to_string()]);

        // Test single integer array
        let input = EmbeddingInput::IntegerArray(vec![1, 2, 3]);
        let texts = embed.convert_input_to_texts(&input);
        assert_eq!(texts, vec!["1 2 3".to_string()]);
    }
}
