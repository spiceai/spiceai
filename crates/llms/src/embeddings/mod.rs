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

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        CreateEmbeddingRequest, CreateEmbeddingResponse, Embedding, EmbeddingInput, EmbeddingUsage,
        EmbeddingVector, EncodingFormat,
    },
};
use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

use crate::chunking::{Chunker, ChunkingConfig, RecursiveSplittingChunker};

pub mod candle;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run Embedding health check: {source}"))]
    HealthCheckError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to prepare input for embedding: {source}"))]
    FailedToPrepareInput {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create embedding: {source}"))]
    FailedToCreateEmbedding {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create chunker: {source}"))]
    FailedToCreateChunker {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to instantiate embedding model: {source}"))]
    FailedToInstantiateEmbeddingModel {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported source of model: {source}"))]
    UnknownModelSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Model '{model_name}' does not exist"))]
    ModelDoesNotExist { model_name: String },

    #[snafu(display("No model from {from} currently supports {task}"))]
    UnsupportedTaskForModel { from: String, task: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Convert the float-vector representation to the desired format.
fn encode_embedding(format: &EncodingFormat, array: Vec<f32>) -> EmbeddingVector {
    match format {
        EncodingFormat::Float => EmbeddingVector::Float(array),
        EncodingFormat::Base64 => {
            let base64_str = EmbeddingVector::Float(array).into();
            EmbeddingVector::Base64(base64_str)
        }
    }
}

#[async_trait]
pub trait Embed: Sync + Send {
    async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>>;

    /// A basic health check to ensure the model can process future [`Self::embed`] requests.
    /// Default implementation is a basic call to [`embed()`].
    async fn health(&self) -> Result<()> {
        self.embed(EmbeddingInput::String("health".to_string()))
            .await
            .boxed()
            .context(HealthCheckSnafu)?;
        Ok(())
    }

    fn chunker(&self, cfg: &ChunkingConfig) -> Result<Arc<dyn Chunker>> {
        Ok(Arc::new(
            RecursiveSplittingChunker::with_character_sizer(cfg)
                .boxed()
                .context(FailedToCreateChunkerSnafu)?,
        ))
    }

    /// Returns the size of the embedding vector returned by the model. Return -1 if the size should be inferred from [`Embed::embed`] method.
    fn size(&self) -> i32;

    /// An OpenAI-compatible interface for the embedding trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`embed`] method.
    #[allow(clippy::cast_possible_truncation)]
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

        Ok(CreateEmbeddingResponse {
            object: "list".to_string(),
            model: req.model.clone(),
            data: result
                .into_iter()
                .enumerate()
                .map(|(i, emb)| Embedding {
                    index: i as u32,
                    object: "embedding".to_string(),
                    embedding: encode_embedding(&format, emb),
                })
                .collect(),
            usage: EmbeddingUsage {
                prompt_tokens: 0,
                total_tokens: 0,
            },
        })
    }
}
