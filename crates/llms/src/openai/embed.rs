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
use async_openai::config::Config;
use bytes::Bytes;
use std::sync::Arc;

use crate::chunking::{
    ArcSizer, Chunker, ChunkingConfig, RecursiveSplittingChunker, TokenizerWrapper,
};

use crate::embeddings::{Embed, Error as EmbedError, Result as EmbedResult};
use async_openai::error::OpenAIError;
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingRequestArgs, CreateEmbeddingResponse, EmbeddingInput,
};

use async_trait::async_trait;
use futures::future::try_join_all;
use snafu::ResultExt;
use text_splitter::ChunkSizer;
use tokenizers::Tokenizer;

use super::Openai;

pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

/// Embedding implementation for `OpenAI` compatible embedding models.
///
/// For non-OpenAI models, a [`Tokenizer`] can be provided to correctly size
/// chunks (instead of the default `OpenAI` BPE tokenizer).
pub struct OpenaiEmbed<C: Config> {
    pub inner: Openai<C>,
    pub chunk_sizer: Option<Arc<dyn ChunkSizer + Send + Sync>>,
}

impl<C: Config> OpenaiEmbed<C> {
    #[must_use]
    pub fn new(inner: Openai<C>) -> Self {
        Self {
            inner,
            chunk_sizer: None,
        }
    }

    #[must_use]
    fn with_tokenizer(mut self, tokenizer: Arc<Tokenizer>) -> Self {
        self.chunk_sizer = Some(Arc::new(Into::<TokenizerWrapper>::into(tokenizer)));
        self
    }

    pub fn try_with_tokenizer_bytes(mut self, bytz: &Bytes) -> Result<Self, EmbedError> {
        let tokenizer = Tokenizer::from_bytes(bytz)
            .map_err(|e| EmbedError::FailedToCreateTokenizer { source: e })?;

        self = self.with_tokenizer(Arc::new(tokenizer));
        Ok(self)
    }
}

#[async_trait]
impl<C: Config + Sync + Send> Embed for OpenaiEmbed<C> {
    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let outer_model = req.model.clone();
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.inner.model);
        let mut resp = self.inner.client.embeddings().create(inner_req).await?;

        resp.model = outer_model;
        Ok(resp)
    }

    async fn embed(&self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        // Batch requests to OpenAI endpoint because "any array must be 2048 dimensions or less".
        // https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-input
        let embed_batches = match input {
            EmbeddingInput::StringArray(ref batch) => batch
                .chunks(2048)
                .map(|chunk| EmbeddingInput::StringArray(chunk.to_vec()))
                .collect(),
            EmbeddingInput::ArrayOfIntegerArray(ref batch) => batch
                .chunks(2048)
                .map(|chunk| EmbeddingInput::ArrayOfIntegerArray(chunk.to_vec()))
                .collect(),
            _ => vec![input],
        };

        let request_batches_result: EmbedResult<Vec<CreateEmbeddingRequest>> = embed_batches
            .into_iter()
            .map(|batch| {
                CreateEmbeddingRequestArgs::default()
                    .model(self.inner.model.clone())
                    .input(batch)
                    .build()
                    .boxed()
                    .map_err(|source| EmbedError::FailedToPrepareInput { source })
            })
            .collect();

        let embed_futures: Vec<_> = request_batches_result?
            .into_iter()
            .map(|req| {
                let local_client = self.inner.client.clone();
                async move {
                    let embedding: Vec<Vec<f32>> = local_client
                        .embeddings()
                        .create_float(req)
                        .await
                        .boxed()
                        .map_err(|source| EmbedError::FailedToCreateEmbedding { source })?
                        .data
                        .into_iter()
                        .map(|d| d.embedding.into())
                        .collect();
                    Ok::<Vec<Vec<f32>>, EmbedError>(embedding)
                }
            })
            .collect();

        let combined_results: Vec<Vec<f32>> = try_join_all(embed_futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(combined_results)
    }

    fn size(&self) -> i32 {
        match self.inner.model.as_str() {
            "text-embedding-3-large" => 3_072,
            "text-embedding-3-small" | "text-embedding-ada-002" => 1_536,
            _ => -1, // unreachable. If not a valid model, it won't create embeddings.
        }
    }

    fn chunker(&self, cfg: &ChunkingConfig<'_>) -> EmbedResult<Arc<dyn Chunker>> {
        match self.chunk_sizer {
            Some(ref sizer) => Ok(Arc::new(
                RecursiveSplittingChunker::try_new(cfg, Into::<ArcSizer>::into(Arc::clone(sizer)))
                    .boxed()
                    .map_err(|e| EmbedError::FailedToCreateChunker { source: e })?,
            )),
            None => Ok(Arc::new(
                RecursiveSplittingChunker::for_openai_model(&self.inner.model, cfg)
                    .map_err(|e| EmbedError::FailedToCreateChunker { source: e })?,
            )),
        }
    }
}
