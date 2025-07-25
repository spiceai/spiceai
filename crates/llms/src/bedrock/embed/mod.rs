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

use crate::bedrock::BedrockClient;
use crate::bedrock::embed::cohere::{
    CohereConfig, CohereEmbedRequest, CohereEmbedResponse, CohereEmbeddingInputType,
    CohereEmbeddingTruncate, CohereEmbeddingType,
};
use crate::bedrock::embed::titan::{TitanConfig, TitanEmbedRequest, TitanEmbedResponse};
use crate::embeddings::{Embed, Error as EmbedError, Result as EmbedResult};
use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingResponse, Embedding, EmbeddingInput, EmbeddingUsage,
    EmbeddingVector,
};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::operation::invoke_model::InvokeModelError;
use aws_sdk_bedrockruntime::{error::SdkError, primitives::Blob};
use futures::{StreamExt, stream};
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::{Quota, RateLimiter};
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::warn;
use util::{
    RetryError,
    fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder},
    retry,
};

pub mod cohere;
pub mod titan;

const TITAN_TEXT_EMBED_V2: &str = "amazon.titan-embed-text-v2:0";
// Maximum number of concurrently running requests.
// The overall request rate is controlled by the rate_limiter.
const DEFAULT_MAX_CONCURRENT_INVOCATIONS: usize = 40;

fn default_retry_strategy() -> FibonacciBackoff {
    FibonacciBackoffBuilder::new().max_retries(Some(10)).build()
}

#[derive(Debug, Clone)]
pub struct BedrockEmbed<Rq, Rsp>
where
    Rq: Serialize + Sized,
    Rsp: DeserializeOwned,
{
    client: BedrockClient,
    config: Arc<dyn BedrockEmbeddingConfig<Rq, Rsp> + 'static>,
    rate_limiter: Arc<RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>>,
    // Control the max number of concurrent requests
    semaphore: Arc<Semaphore>,
    // Retry strategy for transient or throttling errors
    retry_strategy: FibonacciBackoff,
    // Rate limiting configuration for logging and metrics
    rate_config: BedrockRateLimitConfig,
}

#[must_use]
pub fn new_titan_v2(
    client: BedrockClient,
    normalize: bool,
    dimensions: u32,
    rate_config: BedrockRateLimitConfig,
) -> BedrockEmbed<TitanEmbedRequest, TitanEmbedResponse> {
    tracing::debug!(
        "Initializing Titan v2 embedder: normalize={normalize}, dimensions={dimensions}, rate_limit={rate_config:?}"
    );

    let config = Arc::new(TitanConfig {
        model_name: TITAN_TEXT_EMBED_V2.to_string(),
        normalize,
        dimensions,
    }) as Arc<dyn BedrockEmbeddingConfig<TitanEmbedRequest, TitanEmbedResponse>>;

    let rate_limiter = Arc::new(RateLimiter::direct(rate_config.to_quota()));

    BedrockEmbed::<TitanEmbedRequest, TitanEmbedResponse> {
        client,
        config,
        rate_limiter,
        semaphore: Arc::new(Semaphore::new(rate_config.max_concurrent_invocations)),
        retry_strategy: default_retry_strategy(),
        rate_config,
    }
}

#[must_use]
pub fn new_cohere(
    client: BedrockClient,
    model_name: String,
    truncate: CohereEmbeddingTruncate,
    input_type: CohereEmbeddingInputType,
    embedding_type: CohereEmbeddingType,
    rate_config: BedrockRateLimitConfig,
) -> BedrockEmbed<CohereEmbedRequest, CohereEmbedResponse> {
    tracing::debug!(
        "Initializing Cohere embedder: model_name={model_name}, truncate={truncate:?}, input_type={input_type}, embedding_type={embedding_type}, rate_limit={rate_config:?}"
    );

    let config = Arc::new(CohereConfig {
        model_name,
        truncate,
        input_type,
        embedding_type,
    }) as Arc<dyn BedrockEmbeddingConfig<CohereEmbedRequest, CohereEmbedResponse>>;

    let rate_limiter = Arc::new(RateLimiter::direct(rate_config.to_quota()));

    BedrockEmbed::<CohereEmbedRequest, CohereEmbedResponse> {
        client,
        config,
        rate_limiter,
        semaphore: Arc::new(Semaphore::new(rate_config.max_concurrent_invocations)),
        retry_strategy: default_retry_strategy(),
        rate_config,
    }
}

impl<Rq, Rsp> BedrockEmbed<Rq, Rsp>
where
    Rq: Serialize + Sized,
    Rsp: DeserializeOwned,
{
    async fn embed_texts(&self, texts: Vec<String>) -> Result<(Vec<Vec<f32>>, u32), OpenAIError> {
        let request_payloads = self.config.to_request_blobs(texts)?;

        if request_payloads.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut results = Vec::new();
        let mut total_tokens = 0;

        // Run embedding requests with up to 5 requests in parallel
        let mut stream = stream::iter(request_payloads)
            .map(|req| self.process_single_request(req))
            .buffered(self.rate_config.max_concurrent_invocations);

        while let Some(result) = stream.next().await {
            let (mut vectors, tokens) = result?;
            results.append(&mut vectors);
            total_tokens += tokens;
        }

        Ok((results, total_tokens))
    }

    async fn process_single_request(&self, req: Rq) -> Result<(Vec<Vec<f32>>, u32), OpenAIError> {
        let body = serde_json::to_string(&req).boxed().map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        // Control num concurrent requests
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: format!("Unable to acquire rate limiter permit: {e}"),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        let response = retry(self.retry_strategy.clone(), || async {
            // Wait for rate limiter
            self.rate_limiter.until_ready().await;

            match self
                .client
                .client
                .invoke_model()
                .model_id(self.config.model_id())
                .body(Blob::new(body.as_bytes()))
                .content_type("application/json")
                .send()
                .await
            {
                Ok(response) => Ok(response),
                Err(e) => Err(match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        InvokeModelError::ThrottlingException(_) => {
                            tracing::debug!(
                                "Bedrock embedding model throttled, backing off and retrying..."
                            );
                            RetryError::transient(e)
                        }
                        _ => RetryError::permanent(e),
                    },
                    _ => RetryError::permanent(e),
                }),
            }
        })
        .await
        .map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: match e.into_source() {
                    Ok(s_err) => s_err.to_string(),
                    Err(e) => e.to_string(),
                },
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        let response_body = response.body().as_ref();
        let response_obj = serde_json::from_slice(response_body).boxed().map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        self.config.extract_embeddings(response_obj)
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

/// [`BedrockEmbeddingConfig`] handles the model-specific request and response payloads expected by AWS Bedrock.
///
/// AWS Bedrock does not have a standard API interface for its models. For each model, or model family, a different API is exposed.
pub trait BedrockEmbeddingConfig<Rq: Serialize + Sized, Rsp: DeserializeOwned>:
    Debug + Sync + Send
{
    fn model_id(&self) -> &String;
    fn dimensions(&self) -> i32;

    /// For given text to embed, construct a set of request payloads (i.e. [`Blob`]) to provider to Bedrock runtime.
    fn to_request_blobs(&self, input_text: Vec<String>) -> Result<Vec<Rq>, OpenAIError>;

    /// For responses content from AWS Bedrock, extract the embedding vectors and the number of tokens embedded.
    fn extract_embeddings(&self, resp: Rsp) -> Result<(Vec<Vec<f32>>, u32), OpenAIError>;
}

#[async_trait]
impl<Rq, Rsp> Embed for BedrockEmbed<Rq, Rsp>
where
    Rq: Serialize + Sized + Send + Sync + Debug,
    Rsp: DeserializeOwned + Send + Sync + Debug,
{
    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let texts = Self::convert_input_to_texts(&req.input);

        let (vectors, num_tokens) = self.embed_texts(texts).await?;

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

        let num_items = texts.len();
        tracing::trace!(
            "Embedding {} records using model {} (max_concurrent_invocations: {}, requests_per_minute_limit: {})",
            num_items,
            self.config.model_id(),
            self.rate_config.max_concurrent_invocations,
            self.rate_config.requests_per_minute_limit
        );

        let start = std::time::Instant::now();

        if texts.is_empty() {
            return Ok(vec![]);
        }

        let (vectors, _num_tokens) = self
            .embed_texts(texts)
            .await
            .boxed()
            .map_err(|e| EmbedError::FailedToCreateEmbedding { source: e })?;

        let duration = start.elapsed();
        tracing::debug!(
            "Embedding completed in {duration:?} for {num_items} records using model {}",
            self.config.model_id()
        );

        Ok(vectors)
    }

    fn size(&self) -> i32 {
        self.config.dimensions()
    }
}

#[derive(Debug)]
pub struct BedrockRateLimitConfigBuilder {
    requests_per_minute_limit: Option<u32>,
    max_concurrent_invocations: Option<usize>,
}

impl Default for BedrockRateLimitConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BedrockRateLimitConfigBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            requests_per_minute_limit: None,
            max_concurrent_invocations: None,
        }
    }

    #[must_use]
    pub fn requests_per_minute(&mut self, limit: u32) -> &Self {
        self.requests_per_minute_limit = Some(limit);
        self
    }

    #[must_use]
    pub fn max_concurrent_invocations(&mut self, limit: usize) -> &Self {
        self.max_concurrent_invocations = Some(limit);
        self
    }

    #[must_use]
    pub fn build(self) -> BedrockRateLimitConfig {
        BedrockRateLimitConfig {
            requests_per_minute_limit: self.requests_per_minute_limit.unwrap_or(1_500),
            max_concurrent_invocations: self
                .max_concurrent_invocations
                .unwrap_or(DEFAULT_MAX_CONCURRENT_INVOCATIONS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BedrockRateLimitConfig {
    pub requests_per_minute_limit: u32,
    pub max_concurrent_invocations: usize,
}

impl BedrockRateLimitConfig {
    #[must_use]
    pub fn to_quota(&self) -> Quota {
        Quota::per_minute(
            NonZeroU32::new(self.requests_per_minute_limit).unwrap_or_else(|| {
                unreachable!(
                    "requests_per_minute_limit is u32 and should always successfully convert to NonZeroU32"
                )
            }),
        )
    }
}
