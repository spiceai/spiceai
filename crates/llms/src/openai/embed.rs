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
use std::sync::Arc;

use crate::chunking::{Chunker, ChunkingConfig, RecursiveSplittingChunker};
use crate::embeddings::{Embed, Error as EmbedError, Result as EmbedResult};
use async_openai::error::OpenAIError;
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingRequestArgs, CreateEmbeddingResponse, EmbeddingInput,
};

use async_trait::async_trait;
use futures::future::try_join_all;
use snafu::ResultExt;

use super::Openai;

pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

#[async_trait]
impl Embed for Openai {
    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        self.client.embeddings().create(inner_req).await
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
                    .model(self.model.clone())
                    .input(batch)
                    .build()
                    .boxed()
                    .map_err(|source| EmbedError::FailedToPrepareInput { source })
            })
            .collect();

        let embed_futures: Vec<_> = request_batches_result?
            .into_iter()
            .map(|req| {
                let local_client = self.client.clone();
                async move {
                    let embedding: Vec<Vec<f32>> = local_client
                        .embeddings()
                        .create(req)
                        .await
                        .boxed()
                        .map_err(|source| EmbedError::FailedToCreateEmbedding { source })?
                        .data
                        .iter()
                        .map(|d| d.embedding.clone())
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
        match self.model.as_str() {
            "text-embedding-3-large" => 3_072,
            "text-embedding-3-small" | "text-embedding-ada-002" => 1_536,
            _ => 0, // unreachable. If not a valid model, it won't create embeddings.
        }
    }

    fn chunker(&self, cfg: &ChunkingConfig<'_>) -> EmbedResult<Arc<dyn Chunker>> {
        Ok(Arc::new(
            RecursiveSplittingChunker::for_openai_model(&self.model, cfg)
                .map_err(|e| EmbedError::FailedToCreateChunker { source: e })?,
        ))
    }
}
