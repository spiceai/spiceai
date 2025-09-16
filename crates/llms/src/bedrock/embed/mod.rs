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
use crate::bedrock::embed::titan::{
    TITAN_TEXT_EMBED_V2, TitanConfig, TitanEmbedRequest, TitanEmbedResponse,
};

use crate::embeddings::{
    Embed, Error as EmbedError, FailedToCreateEmbeddingSnafu, FailedToPrepareInputSnafu,
    Result as EmbedResult,
};
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingResponse, Embedding, EmbeddingInput, EmbeddingUsage,
    EmbeddingVector,
};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::types::error::ThrottlingException as BedrockThrottlingException;
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::warn;

pub mod cohere;
pub mod titan;

#[derive(Debug, Clone)]
pub struct BedrockEmbed<Rq, Rsp>
where
    Rq: Serialize + Sized,
    Rsp: DeserializeOwned,
{
    client: BedrockClient,
    config: Arc<dyn BedrockEmbeddingConfig<Rq, Rsp> + 'static>,
}

#[must_use]
pub fn new_titan_v2(
    client: BedrockClient,
    normalize: bool,
    dimensions: u32,
) -> BedrockEmbed<TitanEmbedRequest, TitanEmbedResponse> {
    tracing::debug!(
        "Initializing Titan v2 embedder: normalize={normalize}, dimensions={dimensions}, rate_limit={:?}",
        client.rate_controller
    );

    let config = Arc::new(TitanConfig {
        model_name: TITAN_TEXT_EMBED_V2.to_string(),
        normalize,
        dimensions,
    }) as Arc<dyn BedrockEmbeddingConfig<TitanEmbedRequest, TitanEmbedResponse>>;

    BedrockEmbed::<TitanEmbedRequest, TitanEmbedResponse> { client, config }
}

#[must_use]
pub fn new_cohere(
    client: BedrockClient,
    model_name: String,
    truncate: CohereEmbeddingTruncate,
    input_type: CohereEmbeddingInputType,
    embedding_type: CohereEmbeddingType,
) -> BedrockEmbed<CohereEmbedRequest, CohereEmbedResponse> {
    tracing::debug!(
        "Initializing Cohere embedder: model_name={model_name}, truncate={truncate:?}, input_type={input_type}, embedding_type={embedding_type}, rate_limit={:?}",
        client.rate_controller
    );

    let config = Arc::new(CohereConfig {
        model_name,
        truncate,
        input_type,
        embedding_type,
    }) as Arc<dyn BedrockEmbeddingConfig<CohereEmbedRequest, CohereEmbedResponse>>;

    BedrockEmbed::<CohereEmbedRequest, CohereEmbedResponse> { client, config }
}

impl<Rq, Rsp> BedrockEmbed<Rq, Rsp>
where
    Rq: Serialize + Sized,
    Rsp: DeserializeOwned,
{
    async fn embed_texts(&self, texts: Vec<String>) -> EmbedResult<(Vec<Vec<f32>>, u32)> {
        let request_payloads = self.config.to_request_blobs(texts)?;

        if request_payloads.is_empty() {
            return Ok((Vec::new(), 0));
        }

        // join all requests, as the inner rate limit will manage concurrency
        let results = futures::future::try_join_all(
            request_payloads
                .into_iter()
                .map(|req| self.process_single_request(req)),
        )
        .await?;

        let results = results.into_iter().fold(
            (Vec::new(), 0),
            |(mut acc_vectors, acc_tokens), (vectors, tokens)| {
                acc_vectors.extend(vectors);
                (acc_vectors, acc_tokens + tokens)
            },
        );

        Ok(results)
    }

    async fn process_single_request(&self, req: Rq) -> EmbedResult<(Vec<Vec<f32>>, u32)> {
        let body = serde_json::to_string(&req)
            .boxed()
            .context(FailedToPrepareInputSnafu)?;

        let response = self
            .client
            .do_invoke(self.config.model_id().clone(), body)
            .await
            .map_err(|err| match err.downcast::<BedrockThrottlingException>() {
                Ok(e) => EmbedError::RateLimited { source: e },
                Err(e) => EmbedError::FailedToCreateEmbedding { source: e },
            })?;

        let response_body = response.body().as_ref();
        let response_obj = serde_json::from_slice(response_body)
            .boxed()
            .context(FailedToCreateEmbeddingSnafu)?;

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
    fn to_request_blobs(&self, input_text: Vec<String>) -> EmbedResult<Vec<Rq>>;

    /// For responses content from AWS Bedrock, extract the embedding vectors and the number of tokens embedded.
    fn extract_embeddings(&self, resp: Rsp) -> EmbedResult<(Vec<Vec<f32>>, u32)>;
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
    ) -> EmbedResult<CreateEmbeddingResponse> {
        let texts = Self::convert_input_to_texts(&req.input);

        let (vectors, num_tokens) = self
            .embed_texts(texts)
            .await
            .boxed()
            .context(FailedToCreateEmbeddingSnafu)?;

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

        let start = std::time::Instant::now();

        if texts.is_empty() {
            return Ok(vec![]);
        }

        let (vectors, _num_tokens) = self.embed_texts(texts).await.boxed().map_err(|err| {
            match err.downcast::<EmbedError>() {
                Ok(embed_err) => *embed_err,
                Err(err) => EmbedError::FailedToCreateEmbedding { source: err },
            }
        })?;

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
