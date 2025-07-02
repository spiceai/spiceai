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

use crate::bedrock::{
    BedrockClient, CohereEmbedRequest, CohereEmbedResponse, CohereEmbeddingInputType,
    CohereEmbeddingTruncate, CohereEmbeddingType, TitanEmbedRequest, TitanEmbedResponse,
};
use crate::embeddings::{Embed, Error as EmbedError, Result as EmbedResult};
use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingResponse, Embedding, EmbeddingInput, EmbeddingUsage,
    EmbeddingVector,
};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::primitives::Blob;
use snafu::ResultExt;
use std::fmt::Debug;
use std::str::FromStr;
use tracing::{debug, warn};

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
}

impl BedrockEmbed {
    #[must_use]
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
        }
    }

    fn is_titan_model(&self) -> bool {
        self.model_id.starts_with("amazon.titan-embed")
    }

    fn is_cohere_model(&self) -> bool {
        self.model_id.starts_with("cohere.embed")
    }

    async fn embed_titan(&self, texts: Vec<String>) -> EmbedResult<(Vec<Vec<f32>>, u32)> {
        let mut results = Vec::new();
        let mut num_tokens = 0;
        for text in texts {
            // For Titan models, we need to be more careful about token limits
            // This is still an approximation as we don't have access to the actual tokenizer
            let estimated_tokens = text.split_whitespace().count();
            let truncated_text = if estimated_tokens > MAX_TITAN_INPUT_LENGTH {
                warn!(
                    "Truncating input text from estimated {} to {} tokens for Titan model",
                    estimated_tokens, MAX_TITAN_INPUT_LENGTH
                );
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

            debug!(
                "Invoking Titan model {} with request: {}",
                self.model_id, body
            );

            let response = self
                .client
                .client
                .invoke_model()
                .model_id(&self.model_id)
                .body(Blob::new(body.as_bytes()))
                .content_type("application/json")
                .send()
                .await
                .map_err(|e| EmbedError::FailedToCreateEmbedding {
                    source: match e.into_source() {
                        Ok(s_err) => s_err,
                        Err(e) => Box::new(e),
                    },
                })?;

            let response_body = response.body().as_ref();
            let titan_response: TitanEmbedResponse = serde_json::from_slice(response_body)
                .boxed()
                .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

            num_tokens += titan_response.input_text_token_count;

            results.push(titan_response.embedding);
        }

        Ok((results, num_tokens))
    }

    async fn embed_cohere(&self, texts: Vec<String>) -> EmbedResult<(Vec<Vec<f32>>, u32)> {
        let mut all_results = Vec::new();
        let mut num_tokens = 0;

        // Process texts in batches to respect Cohere's limits
        for batch in texts.chunks(MAX_COHERE_TEXTS_PER_REQUEST) {
            let truncated_texts: Vec<String> = batch
                .iter()
                .map(|text| {
                    if text.len() > MAX_COHERE_INPUT_LENGTH {
                        warn!(
                            "Truncating input text from {} to {} characters for Cohere model",
                            text.len(),
                            MAX_COHERE_INPUT_LENGTH
                        );
                        text.chars()
                            .take(MAX_COHERE_INPUT_LENGTH)
                            .collect::<String>()
                    } else {
                        text.clone()
                    }
                })
                .collect();

            let request = CohereEmbedRequest {
                texts: truncated_texts.clone(),
                input_type: CohereEmbeddingInputType::from_str(self.input_type.as_str())?,
                truncate: self
                    .truncate
                    .as_deref()
                    .map(CohereEmbeddingTruncate::from_str)
                    .transpose()?,
                embedding_types: Some(vec![CohereEmbeddingType::Float]),
            };

            let body = serde_json::to_string(&request)
                .boxed()
                .map_err(|e| EmbedError::FailedToPrepareInput { source: e })?;

            debug!(
                "Invoking Cohere model {} with request: {}",
                self.model_id, body
            );

            let response = self
                .client
                .client
                .invoke_model()
                .model_id(&self.model_id)
                .body(Blob::new(body.as_bytes()))
                .content_type("application/json")
                .send()
                .await
                .map_err(|e| EmbedError::FailedToCreateEmbedding {
                    source: match e.into_source() {
                        Ok(s_err) => s_err,
                        Err(e) => Box::new(e),
                    },
                })?;

            let response_body = response.body().as_ref();
            let mut cohere_response: CohereEmbedResponse = serde_json::from_slice(response_body)
                .boxed()
                .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

            // Estimate token count for Cohere models (approximate)
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_precision_loss,
                clippy::cast_sign_loss
            )]
            let estimated_tokens: u32 = truncated_texts
                .iter()
                .map(|text| (text.split_whitespace().count() as f32 * 1.3) as u32) // Rough estimate
                .sum();

            num_tokens += estimated_tokens;

            if let Some(float_embedding) = cohere_response
                .embeddings
                .remove(&CohereEmbeddingType::Float)
            {
                all_results.extend(float_embedding);
            }
        }

        Ok((all_results, num_tokens))
    }

    fn convert_input_to_texts(input: &EmbeddingInput) -> Vec<String> {
        match input {
            EmbeddingInput::String(text) => vec![text.clone()],
            EmbeddingInput::StringArray(texts) => texts.clone(),
            EmbeddingInput::ArrayOfIntegerArray(arrays) => {
                // Convert token arrays to string representation
                warn!(
                    "Converting token arrays to text representation for Bedrock models. This may not accurately represent the original text."
                );
                arrays
                    .iter()
                    .map(|tokens| {
                        tokens
                            .iter()
                            .map(std::string::ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect()
            }
            EmbeddingInput::IntegerArray(tokens) => {
                // Convert single token array to string representation
                warn!(
                    "Converting token array to text representation for Bedrock models. This may not accurately represent the original text."
                );
                vec![
                    tokens
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(" "),
                ]
            }
        }
    }
}

#[async_trait]
impl Embed for BedrockEmbed {
    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let texts = Self::convert_input_to_texts(&req.input);

        let (vectors, num_tokens) = if self.is_titan_model() {
            self.embed_titan(texts).await.map_err(|e| {
                OpenAIError::ApiError(ApiError {
                    message: e.to_string(),
                    r#type: None,
                    param: None,
                    code: None,
                })
            })?
        } else if self.is_cohere_model() {
            self.embed_cohere(texts).await.map_err(|e| {
                OpenAIError::ApiError(ApiError {
                    message: e.to_string(),
                    r#type: None,
                    param: None,
                    code: None,
                })
            })?
        } else {
            return Err(OpenAIError::InvalidArgument(format!(
                "Invalid model: {}",
                req.model
            )));
        };

        Ok(CreateEmbeddingResponse {
            object: "list".to_string(),
            model: req.model.clone(),
            data: vectors
                .into_iter()
                .enumerate()
                .map(|(i, emb)| Embedding {
                    #[allow(clippy::cast_possible_truncation)]
                    index: i as u32,
                    object: "embedding".to_string(),
                    embedding: EmbeddingVector::Float(emb),
                })
                .collect(),
            usage: EmbeddingUsage {
                prompt_tokens: num_tokens,
                total_tokens: num_tokens,
            },
        })
    }

    async fn embed(&self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        let texts = Self::convert_input_to_texts(&input);

        if texts.is_empty() {
            return Ok(vec![]);
        }

        let (vectors, _num_tokens) = if self.is_titan_model() {
            self.embed_titan(texts).await?
        } else if self.is_cohere_model() {
            self.embed_cohere(texts).await?
        } else {
            return Err(EmbedError::UnsupportedTaskForModel {
                from: self.model_id.clone(),
                task: "embedding".to_string(),
            });
        };

        Ok(vectors)
    }

    fn size(&self) -> i32 {
        match self.model_id.as_str() {
            TITAN_TEXT_EMBED_V2 => match self.dimensions {
                Some(256) => 256,
                Some(512) => 512,
                _ => 1024,
            },
            COHERE_EMBED_ENGLISH_V3 | COHERE_EMBED_MULTILINGUAL_V3 => 1024,
            _ => -1, // Unknown model, size will be inferred
        }
    }
}
