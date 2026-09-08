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
use async_openai::error::OpenAIError;
use bytes::Bytes;
use cache::CacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use reqwest::StatusCode;
use runtime_rate_control::RateController;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use util::fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder};
use util::{RetryError, retry};

use chunking::{
    ArcSizer, ChunkSizer, Chunker, ChunkingConfig, RecursiveSplittingChunker, TokenizerWrapper,
};

use crate::embeddings::{
    Embed, Error as EmbedError, FailedToAcquireRateControllerPermitSnafu,
    FailedToCreateEmbeddingSnafu, Result as EmbedResult,
};
use async_openai::types::embeddings::{
    CreateEmbeddingRequest, CreateEmbeddingRequestArgs, CreateEmbeddingResponse, EmbeddingInput,
};

use async_trait::async_trait;
use futures::future::try_join_all;
use snafu::ResultExt;
use tokenizers::Tokenizer;

use super::{Openai, default_rate_controller};

pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

fn default_retry_strategy() -> FibonacciBackoff {
    FibonacciBackoffBuilder::new().max_retries(Some(10)).build()
}

/// Embedding implementation for `OpenAI` compatible embedding models.
///
/// For non-OpenAI models, a [`Tokenizer`] can be provided to correctly size
/// chunks (instead of the default `OpenAI` BPE tokenizer).
pub struct OpenaiEmbed<C: Config + Clone> {
    pub inner: Openai<C>,
    pub chunk_sizer: Option<Arc<dyn ChunkSizer + Send + Sync>>,
    // Retry strategy for transient or throttling errors
    retry_strategy: FibonacciBackoff,

    // Rate limiter for requests
    rate_controller: Arc<RateController>,

    // Shared embeddings cache
    cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
}

impl<C: Config + Debug + Clone> std::fmt::Debug for OpenaiEmbed<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenaiEmbed")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<C: Config + Clone> OpenaiEmbed<C> {
    #[must_use]
    pub fn new(inner: Openai<C>, rate_controller: Option<Arc<RateController>>) -> Self {
        Self {
            inner,
            chunk_sizer: None,
            retry_strategy: default_retry_strategy(),
            rate_controller: rate_controller.unwrap_or_else(default_rate_controller),
            cache: None,
        }
    }

    #[must_use]
    fn with_tokenizer(mut self, tokenizer: Arc<Tokenizer>) -> Self {
        self.chunk_sizer = Some(Arc::new(Into::<TokenizerWrapper>::into(tokenizer)));
        self
    }

    #[must_use]
    pub fn set_cache(
        mut self,
        cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
    ) -> Self {
        self.cache = cache;
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
impl<C: Config + Sync + Send + Debug + Clone> Embed for OpenaiEmbed<C> {
    fn cache(&self) -> Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>> {
        self.cache.as_ref().map(Arc::clone)
    }

    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> EmbedResult<CreateEmbeddingResponse> {
        if let Some(CachedEmbeddingResult::Response(cached)) =
            self.get_cached_embed((&req).into()).await
        {
            return Ok(cached);
        }

        let outer_model = req.model.clone();
        let mut inner_req = req.clone();

        inner_req.model.clone_from(&self.inner.model);
        let permit = self
            .rate_controller
            .acquire()
            .await
            .context(FailedToAcquireRateControllerPermitSnafu)?;
        let mut resp = self
            .inner
            .client
            .embeddings()
            .create(inner_req)
            .await
            .boxed()
            .context(FailedToCreateEmbeddingSnafu)?;
        drop(permit);

        resp.model = outer_model;

        self.put_cached_embed((&req).into(), CachedEmbeddingResult::Response(resp.clone()))
            .await;

        Ok(resp)
    }

    async fn embed(&self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        // Batch requests to match OpenAI API limits: max_tokens_per_request and max array size.
        let embed_batches: Vec<EmbeddingInput> = chunk_embedding_input(&input);
        tracing::trace!(
            "OpenAI embedding input split into {} batches",
            embed_batches.len()
        );

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

        let client_ref = Arc::new(self.inner.client.clone());

        let embed_futures: Vec<_> = request_batches_result?
            .into_iter()
            .map(|req| {
                let retry_strategy = self.retry_strategy.clone();
                let client = Arc::clone(&client_ref);
                let rate_controller = Arc::clone(&self.rate_controller);
                async move {
                    retry(retry_strategy, async || {
                        if let Some(CachedEmbeddingResult::Vector(cached)) = self.get_cached_embed((&req).into()).await {
                            return Ok(cached);
                        }

                        let permit = rate_controller.acquire().await.context(FailedToAcquireRateControllerPermitSnafu)?;
                        let start = Instant::now();

                        let embeddings: Vec<Vec<f32>> = client.embeddings().create_float(req.clone()).await
                            .map(|resp| {
                                let end = Instant::now();
                                drop(permit);
                                tracing::trace!("OpenAI embedding request completed in {:?}", end - start);
                                resp.data.into_iter().map(|d| d.embedding.into()).collect::<Vec<_>>()
                            })
                            .map_err(|err| {
                                if is_retriable_error(&err) {
                                    tracing::debug!(
                                        "OpenAI embedding model encountered a retriable server error: {err}. Backing off and retrying..."
                                    );

                                    if is_throttling_error(&err) {
                                        return RetryError::transient(EmbedError::RateLimited { source: err.into() });
                                    }

                                    return RetryError::transient(EmbedError::FailedToCreateEmbedding { source: err.into() });
                                }
                                tracing::debug!(
                                    "OpenAI embedding model encountered a non-retriable server error: {err}"
                                );
                                RetryError::permanent(EmbedError::FailedToCreateEmbedding { source: err.into() })
                            })?;

                        self.put_cached_embed((&req).into(), CachedEmbeddingResult::Vector(embeddings.clone())).await;

                        Ok(embeddings)
                    })
                    .await
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

fn is_retriable_error(err: &OpenAIError) -> bool {
    match err {
        OpenAIError::ApiError(api_err) => {
            // Supported error codes: https://platform.openai.com/docs/guides/error-codes/api-errors
            matches!(api_err.code.as_deref(), None | Some("429" | "500" | "503"))
        }
        OpenAIError::JSONDeserialize(..) => true,
        OpenAIError::Reqwest(request) => {
            request.is_timeout()
                || request.is_connect()
                || request.is_request()
                || request.is_body()
                || matches!(
                    request.status(),
                    Some(
                        StatusCode::TOO_MANY_REQUESTS
                            | StatusCode::INTERNAL_SERVER_ERROR
                            | StatusCode::BAD_GATEWAY
                            | StatusCode::SERVICE_UNAVAILABLE
                            | StatusCode::GATEWAY_TIMEOUT
                    )
                )
        }
        _ => false,
    }
}

fn is_throttling_error(err: &OpenAIError) -> bool {
    match err {
        OpenAIError::ApiError(api_err) => {
            // Supported error codes: https://platform.openai.com/docs/guides/error-codes/api-errors
            matches!(api_err.code.as_deref(), Some("429"))
        }
        OpenAIError::Reqwest(request) => {
            matches!(request.status(), Some(StatusCode::TOO_MANY_REQUESTS))
        }
        _ => false,
    }
}

// `OpenAPI` estimator counts utf-8 bytes as 0.25 tokens so max allowed string size is 1,200,000 bytes.
const MAX_BATCH_STR_BYTES: usize = 512 * 1024; // 512 KiB
const MAX_BATCH_SIZE: usize = 256; // set from https://github.com/spiceai/spiceai/issues/6743

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
        let input = EmbeddingInput::StringArray(vec!["test".to_string(); 2048]);
        let batches = chunk_embedding_input(&input);

        // Should break into multiple batches due to MAX_BATCH_SIZE (256)
        assert_eq!(batches.len(), 8);

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

        assert_eq!(total_items, 2048);
    }

    #[test]
    fn test_chunk_embedding_input_breaks_300k_tokens_fits_estimator() {
        // 256 chunks each 1900 characters = 486,400 bytes
        // MAX_BATCH_STR_BYTES is 512 KiB, so this should fit in one batch
        let input = EmbeddingInput::StringArray(vec!["a".repeat(1900); 256]);
        let batches = chunk_embedding_input(&input);

        assert_eq!(batches.len(), 1);
        if let EmbeddingInput::StringArray(strings) = &batches[0] {
            assert_eq!(strings.len(), 256);
        } else {
            panic!("Expected StringArray");
        }
    }

    #[test]
    fn test_chunk_embedding_input_breaks_300k_tokens() {
        // 256 chunks each 5859 ASCII characters = 1,499,904 bytes
        // MAX_BATCH_STR_BYTES is 512 KiB, so this should break into multiple batches
        let input = EmbeddingInput::StringArray(vec!["a".repeat(5859); 256]);
        let batches = chunk_embedding_input(&input);

        // Should break into 3 batches due to exceeding MAX_BATCH_STR_BYTES
        assert_eq!(batches.len(), 3);

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

        assert_eq!(total_items, 256);
    }

    #[test]
    fn test_chunk_embedding_input_breaks_300k_tokens_unicode() {
        // 256 chunks each 1500 characters using multi-byte UTF-8 character (中)
        // 中 is 3 bytes * 1500 * 256 = 1,152,000 bytes (over MAX_BATCH_STR_BYTES, should split)
        let input = EmbeddingInput::StringArray(vec!["中".repeat(1500); 256]);
        let batches = chunk_embedding_input(&input);

        // Should break into 3 batches due to exceeding MAX_BATCH_STR_BYTES
        assert_eq!(batches.len(), 3);

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

        assert_eq!(total_items, 256);
    }

    #[test]
    fn test_chunk_embedding_input_integer_array() {
        let large_array = vec![vec![1, 2, 3]; 3000];
        let input = EmbeddingInput::ArrayOfIntegerArray(large_array);
        let batches = chunk_embedding_input(&input);

        // Should break into chunks of MAX_BATCH_SIZE (256)
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
