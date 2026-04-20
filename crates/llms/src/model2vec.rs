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
use crate::embeddings::Error::{
    FailedToInstantiateEmbeddingModel, LocalModelPathDoesNotExist, UnsupportedEmbeddingInput,
};
use async_openai::types::embeddings::EmbeddingInput;
use async_trait::async_trait;
use cache::CacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use model2vec_rs::model::StaticModel;
use std::fmt::{Debug, Formatter};
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use util::home_dir::home_dir;

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
        let name = if let Some(local_model_path) = local_model_path(name)? {
            match local_model_path.try_exists() {
                Ok(true) => local_model_path.to_string_lossy().into_owned(),
                Ok(false) => {
                    return Err(LocalModelPathDoesNotExist {
                        path: name.to_string(),
                    });
                }
                Err(source) => {
                    return Err(FailedToInstantiateEmbeddingModel {
                        source: source.into(),
                    });
                }
            }
        } else {
            name.to_string()
        };

        let model = StaticModel::from_pretrained(&name, hf_token, normalize, subfolder)
            .map_err(|e| FailedToInstantiateEmbeddingModel { source: e.into() })?;

        let model2vec = Self {
            name,
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

fn looks_like_local_model_path(name: &str) -> bool {
    let path = Path::new(name);
    path.is_absolute()
        || name.starts_with("./")
        || name.starts_with("../")
        || name.starts_with(".\\")
        || name.starts_with("..\\")
        || name.starts_with("~/")
        || name.starts_with("~\\")
}

fn local_model_path(name: &str) -> Result<Option<PathBuf>, super::embeddings::Error> {
    if let Some(home_relative_path) = name.strip_prefix("~/").or_else(|| name.strip_prefix("~\\")) {
        let Some(home_dir) = home_dir() else {
            return Err(FailedToInstantiateEmbeddingModel {
                source: IoError::new(
                    ErrorKind::NotFound,
                    format!(
                        "Unable to resolve home directory while expanding local model path '{name}'"
                    ),
                )
                .into(),
            });
        };

        // Trim leading path separators so `~//foo` doesn't silently resolve to `/foo`
        // via PathBuf::join's absolute-path override.
        let home_relative_path = home_relative_path.trim_start_matches(['/', '\\']);

        return Ok(Some(home_dir.join(home_relative_path)));
    }

    if looks_like_local_model_path(name) {
        return Ok(Some(Path::new(name).to_path_buf()));
    }

    Ok(None)
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

    fn model_name(&self) -> Option<&str> {
        Some(self.name.as_str())
    }

    async fn embed(
        &self,
        input: EmbeddingInput,
    ) -> Result<Vec<Vec<f32>>, super::embeddings::Error> {
        let cache_key = self.embedding_input_cache_key(&input);

        let cached_response = if let Some(key) = cache_key {
            self.get_cached_embed(key).await
        } else {
            None
        };

        if let Some(CachedEmbeddingResult::Vector(cached)) = cached_response {
            return Ok(cached);
        }

        let vectors = self.embed_sync(input.clone())?;

        if let Some(key) = cache_key {
            self.put_cached_embed(key, CachedEmbeddingResult::Vector(vectors.clone()))
                .await;
        }

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

        if embedding_input.is_empty() {
            tracing::debug!("Embedding input is empty, returning empty vector");
            return Ok(vec![]);
        }

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
    use crate::embeddings::Error;
    use crate::model2vec::Model2Vec;
    use async_openai::types::embeddings::EmbeddingInput;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{home_dir, local_model_path, looks_like_local_model_path};

    #[test]
    fn detects_local_model_paths() {
        assert!(looks_like_local_model_path("/tmp/model"));
        assert!(looks_like_local_model_path("//tmp/model"));
        assert!(looks_like_local_model_path("./model"));
        assert!(looks_like_local_model_path("../model"));
        assert!(looks_like_local_model_path("~/model"));
        assert!(!looks_like_local_model_path("minishlab/potion-base-8M"));
    }

    #[test]
    fn missing_local_model_path_returns_specific_error() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after Unix epoch")
            .as_nanos();
        let missing_path = std::env::temp_dir().join(format!("missing-model2vec-{suffix}"));
        assert!(
            !missing_path
                .try_exists()
                .expect("test path check should not fail"),
            "test path should not exist before model loading"
        );
        let missing_path = missing_path.to_string_lossy().into_owned();

        let err = Model2Vec::from_params(&missing_path, None, None, None, None, None, None)
            .expect_err("missing local model path should fail before Hugging Face lookup");

        assert!(
            matches!(err, Error::LocalModelPathDoesNotExist { ref path } if path == &missing_path),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn expands_home_directory_model_paths() {
        let Some(home_dir_path) = home_dir() else {
            return;
        };

        assert_eq!(
            local_model_path("~/model")
                .expect("home-relative model path should resolve")
                .expect("home-relative model path should be treated as local"),
            home_dir_path.join("model")
        );

        #[cfg(windows)]
        assert_eq!(
            local_model_path("~\\model")
                .expect("home-relative model path should resolve")
                .expect("home-relative model path should be treated as local"),
            home_dir_path.join("model")
        );

        // Leading separators after `~/` must not silently resolve to an absolute path
        // outside the home directory.
        assert_eq!(
            local_model_path("~//tmp/model")
                .expect("home-relative model path should resolve")
                .expect("home-relative model path should be treated as local"),
            home_dir_path.join("tmp/model")
        );
    }

    #[expect(dead_code)]
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

        embed_ints.expect_err("Should fail for integer input");

        let embed_2d_int = model
            .embed(EmbeddingInput::ArrayOfIntegerArray(vec![vec![1]]))
            .await;

        embed_2d_int.expect_err("Should fail for 2D integer input");
    }
}
