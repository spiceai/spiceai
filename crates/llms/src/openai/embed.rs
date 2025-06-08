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
use std::fmt::Debug;
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

impl<C: Config + Debug> std::fmt::Debug for OpenaiEmbed<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenaiEmbed")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
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
impl<C: Config + Sync + Send + Debug> Embed for OpenaiEmbed<C> {
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
        // Batch requests to match OpenAI API limits: max_tokens_per_request and max array size.
        let embed_batches: Vec<EmbeddingInput> = chunk_embedding_input(&input);

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

// `OpenAPI` estimator counts utf-8 bytes as 0.25 tokens so allowed string size is 1,200,000 bytes.
const MAX_BATCH_STR_BYTES: usize = 300_000 * 4;
const MAX_BATCH_SIZE: usize = 2048;

/// Chunks embedding input to batches to be `OpenAI` API compliant: `<https://platform.openai.com/docs/api-reference/embeddings/create>`
///  - "any array must be 2048 dimensions or less"
///  - "maximum of 300,000 tokens summed across all inputs in a single request"
fn chunk_embedding_input(input: &EmbeddingInput) -> Vec<EmbeddingInput> {
    match input {
        EmbeddingInput::StringArray(items) => {
            let mut batches = Vec::new();
            let mut curr_batch = Vec::new();
            let mut curr_str_bytes = 0;

            for str in items {
                let str_bytes = str.len(); // `len` returns the length in bytes
                if (!curr_batch.is_empty())
                    && (curr_batch.len() >= MAX_BATCH_SIZE
                        || curr_str_bytes + str_bytes > MAX_BATCH_STR_BYTES)
                {
                    batches.push(EmbeddingInput::StringArray(curr_batch));
                    curr_batch = Vec::new();
                    curr_str_bytes = 0;
                }
                curr_batch.push(str.clone());
                curr_str_bytes += str_bytes;
            }

            if !curr_batch.is_empty() {
                batches.push(EmbeddingInput::StringArray(curr_batch));
            }

            batches
        }
        EmbeddingInput::ArrayOfIntegerArray(arr) => arr
            .chunks(MAX_BATCH_SIZE)
            .map(|chunk| EmbeddingInput::ArrayOfIntegerArray(chunk.to_vec()))
            .collect(),
        _ => vec![input.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_embedding_input_single_batch() {
        let input = EmbeddingInput::StringArray(vec!["short".to_string(); 10]);
        let batches = chunk_embedding_input(&input);

        assert_eq!(batches.len(), 1);
        if let EmbeddingInput::StringArray(strings) = &batches[0] {
            assert_eq!(strings.len(), 10);
        } else {
            panic!("Expected StringArray");
        }
    }

    #[test]
    fn test_chunk_embedding_input_breaks_max_batch_size() {
        let input = EmbeddingInput::StringArray(vec!["test".to_string(); 3000]);
        let batches = chunk_embedding_input(&input);

        // Should break into multiple batches due to MAX_BATCH_SIZE (2048)
        assert_eq!(batches.len(), 2);

        let total_items: usize = batches
            .iter()
            .map(|batch| {
                if let EmbeddingInput::StringArray(strings) = batch {
                    strings.len()
                } else {
                    0
                }
            })
            .sum();

        assert_eq!(total_items, 3000);
    }

    #[test]
    fn test_chunk_embedding_input_breaks_300k_tokens_fits_estimator() {
        // 1001 chunks each 300 characters = 300300 characters
        // OpenAI estimator counts utf-8 bytes as 0.25 tokens
        // ASCII characters are 1 byte each, so 300300 bytes = 75075 tokens (under 300k)
        let input = EmbeddingInput::StringArray(vec!["a".repeat(300); 1001]);
        let batches = chunk_embedding_input(&input);

        // Should fit in one batch since estimated tokens < 300k
        assert_eq!(batches.len(), 1);
        if let EmbeddingInput::StringArray(strings) = &batches[0] {
            assert_eq!(strings.len(), 1001);
        } else {
            panic!("Expected StringArray");
        }
    }

    #[test]
    fn test_chunk_embedding_input_breaks_300k_tokens() {
        // 500 chunks each 3000 ASCII characters = 1,500,000 bytes
        // 1500,000 bytes / 4 = 375000 tokens (over 300k, should split)
        let input = EmbeddingInput::StringArray(vec!["a".repeat(3000); 500]);
        let batches = chunk_embedding_input(&input);

        // Should break into 2 batches due to exceeding MAX_BATCH_STR_BYTES
        assert_eq!(batches.len(), 2);

        let total_items: usize = batches
            .iter()
            .map(|batch| {
                if let EmbeddingInput::StringArray(strings) = batch {
                    strings.len()
                } else {
                    0
                }
            })
            .sum();

        assert_eq!(total_items, 500);
    }

    #[test]
    fn test_chunk_embedding_input_breaks_300k_tokens_unicode() {
        // 500 chunks each 1000 characters using multi-byte UTF-8 character (中)
        // 中 is 3 bytes = 0.75 tokens * 1000 * 500 = 375000 tokens (over 300k, should split)
        let input = EmbeddingInput::StringArray(vec!["中".repeat(1000); 500]);
        let batches = chunk_embedding_input(&input);

        // Should break into 2 batches due to exceeding MAX_BATCH_STR_BYTES
        assert_eq!(batches.len(), 2);

        let total_items: usize = batches
            .iter()
            .map(|batch| {
                if let EmbeddingInput::StringArray(strings) = batch {
                    strings.len()
                } else {
                    0
                }
            })
            .sum();

        assert_eq!(total_items, 500);
    }

    #[test]
    fn test_chunk_embedding_input_integer_array() {
        let large_array = vec![vec![1, 2, 3]; 3000];
        let input = EmbeddingInput::ArrayOfIntegerArray(large_array);
        let batches = chunk_embedding_input(&input);

        // Should break into chunks of MAX_BATCH_SIZE (2048)
        assert!(batches.len() > 1);

        let total_items: usize = batches
            .iter()
            .map(|batch| {
                if let EmbeddingInput::ArrayOfIntegerArray(arrays) = batch {
                    arrays.len()
                } else {
                    0
                }
            })
            .sum();

        assert_eq!(total_items, 3000);
    }

    #[test]
    fn test_chunk_embedding_input_single_string() {
        let input = EmbeddingInput::String("test".to_string());
        let batches = chunk_embedding_input(&input);

        // Single string should remain as-is
        assert_eq!(batches.len(), 1);
        assert!(matches!(batches[0], EmbeddingInput::String(_)));
    }
}
