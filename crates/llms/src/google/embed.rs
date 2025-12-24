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

use std::sync::Arc;

use super::Google;
use crate::embeddings::{Embed, EmbeddingInput, Error, Result};
use async_trait::async_trait;
use cache::{CacheProvider, result::embeddings::CachedEmbeddingResult};
use google_genai::{embeddings::EmbedContentRequest, types::Content};

#[derive(Debug)]
pub struct EmbedGoogle {
    pub(crate) g: Google,
    pub(crate) dimensions: Option<u32>,
    pub(crate) embeddings_cache:
        Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
}

#[async_trait]
impl Embed for EmbedGoogle {
    fn cache(&self) -> Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>> {
        self.embeddings_cache.clone()
    }

    async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>> {
        let texts: Vec<String> = match input {
            EmbeddingInput::String(s) => vec![s],
            EmbeddingInput::StringArray(arr) => arr,
            EmbeddingInput::IntegerArray(_) | EmbeddingInput::ArrayOfIntegerArray(_) => {
                return Err(Error::FailedToPrepareInput {
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Integer array input not supported for Google embeddings",
                    )),
                });
            }
        };

        let requests: Vec<EmbedContentRequest> = texts
            .into_iter()
            .map(|v| EmbedContentRequest {
                model: format!("models/{}", self.g.model),
                content: Content::user(v),
                output_dimensionality: self.dimensions,
                task_type: None,
            })
            .collect();

        let response = self
            .g
            .client
            .batch_embed_content(&self.g.model, requests)
            .await
            .map_err(|e| Error::FailedToCreateEmbedding {
                source: Box::new(std::io::Error::other(format!(
                    "Google embedding error: {e}"
                ))),
            })?;

        let embeddings = response
            .embeddings
            .into_iter()
            .map(|emb| emb.values)
            .collect();

        Ok(embeddings)
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.g.model)
    }

    #[expect(clippy::cast_possible_wrap)]
    fn size(&self) -> i32 {
        match self.dimensions {
            None => -1,
            Some(d) => d as i32,
        }
    }
}
