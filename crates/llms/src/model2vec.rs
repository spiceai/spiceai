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

use crate::embeddings::Embed;
use crate::embeddings::Error::{FailedToInstantiateEmbeddingModel, UnsupportedEmbeddingInput};
use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use cache::CacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use model2vec_rs::model::StaticModel;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A wrapper around the `model2vec` library for generating text embeddings.
///
/// `Model2Vec` is a technique that distills embeddings from
/// transformer models into static word embeddings.
pub struct Model2Vec {
    pub name: String,
    model: StaticModel,

    // Bound on model instantiation
    normalize: Option<bool>,

    // Bound during each embed call
    embed_max_token_length: Option<usize>,
    embed_custom_batch_size: Option<usize>,

    // Spice-specific concurrency limits
    parallelism: Option<usize>,

    // Shared embeddings cache
    cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
}

impl Model2Vec {
    /// Creates a new `Model2Vec` instance from the given parameters.
    ///
    /// # Arguments
    /// * `name` - The name/identifier of the model
    /// * `hf_token` - Optional Hugging Face authentication token
    /// * `normalize` - Whether to normalize embeddings (defaults to model's setting)
    /// * `subfolder` - When using a huggingface model, it may reside in a subfolder of the repo
    /// * `parallelism` - Spice-specific parallelism control (how many threads to embed on)
    /// * `embed_batch_size` - Batch size for embedding operations
    /// * `embed_custom_batch_size` - Custom batch size override
    ///
    /// # Errors
    /// Returns an error if:
    /// - The model cannot be loaded or initialized
    /// - Invalid parameters are provided
    /// - Network issues occur when downloading the model
    /// - Authentication fails with the provided HF token
    pub fn from_params(
        name: &str,
        hf_token: Option<&str>,
        normalize: Option<bool>,
        subfolder: Option<&str>,
        parallelism: Option<usize>,
        embed_max_token_length: Option<usize>,
        embed_custom_batch_size: Option<usize>,
    ) -> Result<Self, super::embeddings::Error> {
        let model = StaticModel::from_pretrained(name, hf_token, normalize, subfolder)
            .map_err(|e| FailedToInstantiateEmbeddingModel { source: e.into() })?;

        let model2vec = Self {
            name: name.to_string(),
            model,
            normalize,
            parallelism,
            embed_max_token_length,
            embed_custom_batch_size,
            cache: None,
        };

        tracing::trace!("Model2Vec::from_params: {model2vec:?}");

        Ok(model2vec)
    }

    #[must_use]
    pub fn set_cache(
        mut self,
        cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
    ) -> Self {
        self.cache = cache;
        self
    }
}

impl Debug for Model2Vec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            name,
            normalize,
            parallelism,
            embed_max_token_length,
            embed_custom_batch_size,
            ..
        } = self;
        write!(
            f,
            "Model2Vec: {name}, normalize: {normalize:?}, parallelism: {parallelism:?}, embed_max_token_length: {embed_max_token_length:?}, embed_custom_batch_size: {embed_custom_batch_size:?}"
        )
    }
}

#[async_trait]
impl Embed for Model2Vec {
    fn cache(&self) -> Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>> {
        self.cache.as_ref().map(Arc::clone)
    }

    async fn embed(
        &self,
        input: EmbeddingInput,
    ) -> Result<Vec<Vec<f32>>, super::embeddings::Error> {
        if let Some(CachedEmbeddingResult::Vector(cached)) =
            self.get_cached_embed((&input).into()).await
        {
            return Ok(cached);
        }

        let vectors = self.embed_sync(input.clone())?;

        self.put_cached_embed(
            (&input).into(),
            CachedEmbeddingResult::Vector(vectors.clone()),
        )
        .await;

        Ok(vectors)
    }

    fn embed_sync(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>, super::embeddings::Error> {
        let embedding_input = match input {
            EmbeddingInput::String(s) => vec![s],
            EmbeddingInput::StringArray(sentences) => sentences,
            _ => {
                return Err(UnsupportedEmbeddingInput {
                    model: self.name.clone(),
                    message: "Model2Vec models only support strings or vectors of strings"
                        .to_string(),
                });
            }
        };

        Ok(self.model.encode_with_args(
            &embedding_input,
            self.embed_max_token_length,
            self.embed_custom_batch_size.unwrap_or(1024),
        ))
    }

    fn supports_sync_embeddings(&self) -> bool {
        true
    }

    fn parallelism(&self) -> Option<usize> {
        self.parallelism
    }

    fn size(&self) -> i32 {
        -1
    }
}

#[cfg(test)]
mod tests {
    use crate::embeddings::Embed;
    use crate::model2vec::Model2Vec;
    use async_openai::types::EmbeddingInput;

    #[tokio::test]
    async fn test_embed() {
        // This embedding is dim 256
        let model = Model2Vec::from_params(
            "minishlab/potion-base-8M",
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("Must instantiate");

        let embed_sentence = model
            .embed(EmbeddingInput::String("hello world".to_string()))
            .await;

        assert!(embed_sentence.is_ok());

        let embed_sentence = embed_sentence.expect("Must embed sentence");
        assert_eq!(embed_sentence.len(), 1);
        assert_eq!(embed_sentence[0].len(), 256);

        insta::assert_debug_snapshot!("model2vec_single_embedding", embed_sentence);

        let embed_sentences = model
            .embed(EmbeddingInput::StringArray(vec![
                "i can eat glass".to_string(),
                "for it does not hurt me".to_string(),
            ]))
            .await;

        assert!(embed_sentences.is_ok());

        let embed_sentences = embed_sentences.expect("Must embed sentences");
        assert_eq!(embed_sentences.len(), 2);
        for embedded_sentence in &embed_sentences {
            assert_eq!(embedded_sentence.len(), 256);
        }

        insta::assert_debug_snapshot!("model2vec_multiple_embeddings", embed_sentences);

        let embed_ints = model.embed(EmbeddingInput::IntegerArray(vec![1])).await;

        assert!(embed_ints.is_err());

        let embed_2d_int = model
            .embed(EmbeddingInput::ArrayOfIntegerArray(vec![vec![1]]))
            .await;

        assert!(embed_2d_int.is_err());
    }
}
