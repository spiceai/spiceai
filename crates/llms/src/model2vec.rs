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
    FailedToDownloadModel, FailedToInstantiateEmbeddingModel, LocalModelPathDoesNotExist,
    UnsupportedEmbeddingInput,
};
use crate::hf::HubParams;
use async_openai::types::embeddings::EmbeddingInput;
use async_trait::async_trait;
use cache::CacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use huggingface_hub::{DownloadError, Requester};
use model2vec_rs::model::StaticModel;
use std::fmt::{Debug, Formatter};
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use util::home_dir::home_dir;

const DOCS_URL: &str = "https://spiceai.org/docs/components/embeddings/model2vec";

/// How a `model2vec` model is loaded and run.
#[derive(Debug, Clone, Default)]
pub struct Model2VecOptions {
    /// Whether to normalize embeddings; the model's `config.json` decides when `None`.
    pub normalize: Option<bool>,
    /// The folder of the repository or directory that holds the model.
    pub subfolder: Option<String>,
    /// Spice-specific parallelism control: how many threads to embed on.
    pub parallelism: Option<usize>,
    pub embed_max_token_length: Option<usize>,
    pub embed_custom_batch_size: Option<usize>,
}

/// A wrapper around the `model2vec` library for generating text embeddings.
///
/// `Model2Vec` is a technique that distills embeddings from
/// transformer models into static word embeddings.
pub struct Model2Vec {
    pub name: String,
    // `Arc` so the model can be shared into `spawn_blocking` to run the
    // (CPU-bound, synchronous) forward pass off the async runtime thread.
    model: Arc<StaticModel>,

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
    /// Loads the `model2vec` model `model` for the embedding model `component`: a local
    /// directory, or a Hugging Face repository (`org/name`) downloaded into the Hugging Face
    /// cache first. The model is read on a blocking thread.
    ///
    /// # Errors
    ///
    /// Returns an error if a local directory does not exist, the download fails, or the model
    /// cannot be loaded.
    pub async fn load(
        component: &str,
        model: &str,
        hub: HubParams<'_>,
        options: Model2VecOptions,
    ) -> Result<Self, super::embeddings::Error> {
        let (dir, name) = match local_model_path(model)? {
            Some(path) => {
                match tokio::fs::try_exists(&path).await {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(LocalModelPathDoesNotExist {
                            path: model.to_string(),
                        });
                    }
                    Err(source) => {
                        return Err(FailedToInstantiateEmbeddingModel {
                            source: source.into(),
                        });
                    }
                }
                let name = path.to_string_lossy().into_owned();
                (path, name)
            }
            // `model2vec-rs` reads a relative directory that exists before it asks the Hub.
            None if tokio::fs::try_exists(model).await.unwrap_or(false) => {
                (PathBuf::from(model), model.to_string())
            }
            None => {
                let requester = Requester {
                    kind: "embedding model",
                    name: component.to_string(),
                    docs_url: DOCS_URL,
                    // Spice loads an embedding model once.
                    retried: false,
                };
                let dir = download(requester, model, hub, options.subfolder.as_deref())
                    .await
                    .map_err(|source| FailedToDownloadModel {
                        source: Box::new(source),
                    })?;
                (dir, model.to_string())
            }
        };
        tokio::task::spawn_blocking(move || Self::from_dir(&dir, name, options))
            .await
            .map_err(|source| FailedToInstantiateEmbeddingModel {
                source: Box::new(source),
            })?
    }

    /// Loads the `model2vec` model in `dir`, or in its `options.subfolder`, as `name`. Reads
    /// the files synchronously.
    fn from_dir(
        dir: &Path,
        name: String,
        options: Model2VecOptions,
    ) -> Result<Self, super::embeddings::Error> {
        let Model2VecOptions {
            normalize,
            subfolder,
            parallelism,
            embed_max_token_length,
            embed_custom_batch_size,
        } = options;
        let model = StaticModel::from_pretrained(dir, None, normalize, subfolder.as_deref())
            .map_err(|e| FailedToInstantiateEmbeddingModel { source: e.into() })?;

        let model2vec = Self {
            name,
            model: Arc::new(model),
            normalize,
            parallelism,
            embed_max_token_length,
            embed_custom_batch_size,
            cache: None,
        };

        tracing::trace!("Model2Vec::from_dir: {model2vec:?}");

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

/// The files `model2vec-rs` reads from a model in `subfolder` of a repository: its tokenizer
/// and its weights, and its `config.json` when the repository has one; `has` says whether the
/// repository holds a path. When the repository lacks a file, what it lacks.
fn model2vec_files(
    subfolder: Option<&str>,
    has: impl Fn(&str) -> bool,
) -> Result<Vec<String>, String> {
    let prefix = subfolder
        .map(|folder| folder.trim_matches('/'))
        .filter(|folder| !folder.is_empty())
        .map(|folder| format!("{folder}/"))
        .unwrap_or_default();
    let mut paths = Vec::new();
    for required in ["tokenizer.json", "model.safetensors"] {
        let path = format!("{prefix}{required}");
        if !has(&path) {
            return Err(format!("'{path}'"));
        }
        paths.push(path);
    }
    let config = format!("{prefix}config.json");
    if has(&config) {
        paths.push(config);
    }
    Ok(paths)
}

/// Downloads the files [`model2vec_files`] names from the repository `repo` into the Hugging
/// Face cache, and returns the snapshot directory that holds them.
async fn download(
    requester: Requester,
    repo: &str,
    hub: HubParams<'_>,
    subfolder: Option<&str>,
) -> Result<PathBuf, DownloadError> {
    let snapshot = crate::hf::open_model(requester, repo, None, hub).await?;
    let paths = model2vec_files(subfolder, |path| snapshot.file(path).is_some())
        .map_err(|missing| DownloadError::missing(&snapshot, missing))?;
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    snapshot.download(&paths).await?;
    Ok(snapshot.dir())
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

/// Run the `Model2Vec` forward pass. Synchronous and CPU-bound — callers on the
/// async runtime must invoke this via `spawn_blocking`.
fn encode_with_static_model(
    model: &StaticModel,
    input: EmbeddingInput,
    model_name: &str,
    max_token_length: Option<usize>,
    batch_size: usize,
) -> Result<std::sync::Arc<Vec<Vec<f32>>>, super::embeddings::Error> {
    let embedding_input = match input {
        EmbeddingInput::String(s) => vec![s],
        EmbeddingInput::StringArray(sentences) => sentences,
        _ => {
            return Err(UnsupportedEmbeddingInput {
                model: model_name.to_string(),
                message: "Model2Vec models only support strings or vectors of strings".to_string(),
            });
        }
    };

    if embedding_input.is_empty() {
        tracing::debug!("Embedding input is empty, returning empty vector");
        return Ok(std::sync::Arc::new(vec![]));
    }

    Ok(std::sync::Arc::new(model.encode_with_args(
        &embedding_input,
        max_token_length,
        batch_size,
    )))
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
    ) -> Result<std::sync::Arc<Vec<Vec<f32>>>, super::embeddings::Error> {
        let cache_key = self.embedding_input_cache_key(&input);

        let cached_response = if let Some(key) = cache_key {
            self.get_cached_embed(key).await
        } else {
            None
        };

        if let Some(cached) = cached_response
            && let CachedEmbeddingResult::Vector(vectors) = cached.as_ref()
        {
            return Ok(std::sync::Arc::clone(vectors));
        }

        // The forward pass is CPU-bound and synchronous; run it on the blocking
        // pool so it doesn't stall the async runtime thread (which also serves
        // `/health`, `/v1/embeddings`, etc.).
        let model = Arc::clone(&self.model);
        let model_name = self.name.clone();
        let max_token_length = self.embed_max_token_length;
        let batch_size = self.embed_custom_batch_size.unwrap_or(1024);
        // `cache_key` borrows `input`, so hand the blocking task an owned clone.
        let owned_input = input.clone();
        let vectors = tokio::task::spawn_blocking(move || {
            encode_with_static_model(
                &model,
                owned_input,
                &model_name,
                max_token_length,
                batch_size,
            )
        })
        .await
        .map_err(|e| super::embeddings::Error::FailedToCreateEmbedding {
            source: Box::new(e),
        })??;

        if let Some(key) = cache_key {
            self.put_cached_embed(
                key,
                CachedEmbeddingResult::Vector(std::sync::Arc::clone(&vectors)),
            )
            .await;
        }

        Ok(vectors)
    }

    fn embed_sync(
        &self,
        input: EmbeddingInput,
    ) -> Result<std::sync::Arc<Vec<Vec<f32>>>, super::embeddings::Error> {
        encode_with_static_model(
            &self.model,
            input,
            &self.name,
            self.embed_max_token_length,
            self.embed_custom_batch_size.unwrap_or(1024),
        )
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
    use crate::hf::HubParams;
    use crate::model2vec::{Model2Vec, Model2VecOptions};
    use async_openai::types::embeddings::EmbeddingInput;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{home_dir, local_model_path, looks_like_local_model_path, model2vec_files};

    /// Loads `model`, a local directory in these tests, as the embedding model `test`.
    async fn load(model: &str) -> Result<Model2Vec, Error> {
        Model2Vec::load(
            "test",
            model,
            HubParams::default(),
            Model2VecOptions::default(),
        )
        .await
    }

    /// A static-embedding model directory holding only the two files
    /// `model2vec-rs` actually reads — the tokenizer and the embedding tensor,
    /// named `tensor_key` — and no `config.json`.
    ///
    /// Written by hand rather than downloaded so the guard is offline and needs no
    /// fixture. `safetensors` is a length-prefixed JSON header followed by the raw
    /// tensor bytes, and the loader takes the first of `embeddings`,
    /// `embedding.weight` or `0` that it finds. The key is a parameter because
    /// upstream reads `embeddings` only, while a sentence-transformers export
    /// names it `embedding.weight`.
    fn sentence_transformers_style_model_dir(tensor_key: &str) -> tempfile::TempDir {
        use std::io::Write;
        use tokenizers::Tokenizer;
        use tokenizers::models::wordpiece::WordPiece;

        let dir = tempfile::tempdir().expect("creates a directory for the fixture model");

        // The loader resolves the tokenizer's `unk_token` and requires it to be in
        // the vocabulary, so the fixture needs a real one.
        let vocab_path = dir.path().join("vocab.txt");
        std::fs::write(&vocab_path, "[UNK]\nan\napple\nday\n")
            .expect("writes the fixture vocabulary");
        let model = WordPiece::from_file(
            vocab_path
                .to_str()
                .expect("the fixture vocabulary path is UTF-8"),
        )
        .unk_token("[UNK]".to_string())
        .build()
        .expect("builds the fixture WordPiece model");
        Tokenizer::new(model)
            .save(dir.path().join("tokenizer.json"), false)
            .expect("writes the fixture tokenizer");

        // A 4x2 f32 embedding tensor, one row per vocabulary entry.
        let rows: usize = 4;
        let cols: usize = 2;
        let mut data = Vec::with_capacity(rows * cols * 4);
        for i in 0..rows * cols {
            #[expect(
                clippy::cast_precision_loss,
                reason = "the fixture's values are small and arbitrary"
            )]
            data.extend_from_slice(&(i as f32).to_le_bytes());
        }
        let header = format!(
            r#"{{"{tensor_key}":{{"dtype":"F32","shape":[{rows},{cols}],"data_offsets":[0,{}]}}}}"#,
            data.len()
        );
        let mut safetensors = std::fs::File::create(dir.path().join("model.safetensors"))
            .expect("creates the tensor file");
        safetensors
            .write_all(&(header.len() as u64).to_le_bytes())
            .expect("writes the safetensors header length");
        safetensors
            .write_all(header.as_bytes())
            .expect("writes the safetensors header");
        safetensors
            .write_all(&data)
            .expect("writes the safetensors tensor data");

        dir
    }

    /// A local static-embedding model has to load without a `config.json`.
    ///
    /// `config.json` carries one thing `model2vec-rs` reads — the default for
    /// `normalize` — and a sentence-transformers model does not ship it. Upstream
    /// requires it anyway and refuses the directory outright ("missing tokenizer /
    /// model / config"); a Spice patch to the `spiceai/model2vec-rs` fork makes it
    /// optional and defaults `normalize` to true. Losing the patch is not a wrong
    /// answer, it is a model that will not load at all, and the error names a file
    /// the model was never supposed to have.
    #[tokio::test]
    async fn a_local_model_loads_without_a_config_json() {
        let dir = sentence_transformers_style_model_dir("embeddings");
        assert!(
            !dir.path().join("config.json").exists(),
            "this guard needs a model directory with no config.json"
        );
        // An absolute path, so the name is resolved as a local model rather than a
        // Hub repo id.
        let name = dir
            .path()
            .to_str()
            .expect("the fixture model path is UTF-8");
        assert!(
            looks_like_local_model_path(name),
            "this guard has to reach the local-directory branch, not the Hub"
        );

        load(name).await.unwrap_or_else(|e| {
            panic!(
                "a static-embedding model directory without a config.json must load, since \
                 sentence-transformers models do not ship one: {e}"
            )
        });
    }

    /// The embedding tensor has to be found under the alternative names too, not
    /// only `embeddings`.
    ///
    /// The same fork commit carries this, and it is a separate contract: one line
    /// chain (`.or_else(|_| safet.tensor("embedding.weight"))`, then `"0"`) that
    /// upstream does not have. It is the *same* use case as the row's other half — a
    /// sentence-transformers export ships no `config.json` **and** names its
    /// tensor `embedding.weight` — so a re-cut that carried only the optional
    /// `config.json` would leave exactly the model the row is about still failing
    /// to load, with the guard beside this one green.
    ///
    /// The control is the other test: it loads the same fixture under the
    /// upstream name, so this one failing means the fallback is gone rather than
    /// the fixture being malformed.
    #[tokio::test]
    async fn a_local_model_loads_with_the_sentence_transformers_tensor_names() {
        for tensor_key in ["embedding.weight", "0"] {
            let dir = sentence_transformers_style_model_dir(tensor_key);
            let name = dir
                .path()
                .to_str()
                .expect("the fixture model path is UTF-8");

            load(name).await.unwrap_or_else(|e| {
                panic!(
                    "a static-embedding model whose tensor is named `{tensor_key}` must load: \
                     upstream reads `embeddings` only, so without the fork's fallback chain \
                     every export using this name fails here: {e}"
                )
            });
        }
    }

    /// The files `model2vec-rs` reads from a directory, as paths in the repository.
    #[test]
    fn model2vec_files_are_what_model2vec_rs_reads() {
        let repo = |names: &'static [&'static str]| move |path: &str| names.contains(&path);
        assert_eq!(
            model2vec_files(
                None,
                repo(&[
                    "config.json",
                    "tokenizer.json",
                    "model.safetensors",
                    "README.md"
                ])
            ),
            Ok(vec![
                "tokenizer.json".to_string(),
                "model.safetensors".to_string(),
                "config.json".to_string(),
            ])
        );
        // A sentence-transformers export has no `config.json`.
        assert_eq!(
            model2vec_files(None, repo(&["tokenizer.json", "model.safetensors"])),
            Ok(vec![
                "tokenizer.json".to_string(),
                "model.safetensors".to_string(),
            ])
        );
        // A model in a subfolder, however the folder is written.
        for subfolder in ["0_StaticEmbedding", "/0_StaticEmbedding/"] {
            assert_eq!(
                model2vec_files(
                    Some(subfolder),
                    repo(&[
                        "tokenizer.json",
                        "0_StaticEmbedding/tokenizer.json",
                        "0_StaticEmbedding/model.safetensors",
                    ])
                ),
                Ok(vec![
                    "0_StaticEmbedding/tokenizer.json".to_string(),
                    "0_StaticEmbedding/model.safetensors".to_string(),
                ])
            );
        }
        // What a repository lacks, worded to follow "the repo has no".
        assert_eq!(
            model2vec_files(
                Some("0_StaticEmbedding"),
                repo(&["tokenizer.json", "model.safetensors"])
            ),
            Err("'0_StaticEmbedding/tokenizer.json'".to_string())
        );
        assert_eq!(
            model2vec_files(None, repo(&["tokenizer.json", "pytorch_model.bin"])),
            Err("'model.safetensors'".to_string())
        );
    }

    #[test]
    fn detects_local_model_paths() {
        assert!(looks_like_local_model_path("/tmp/model"));
        assert!(looks_like_local_model_path("//tmp/model"));
        assert!(looks_like_local_model_path("./model"));
        assert!(looks_like_local_model_path("../model"));
        assert!(looks_like_local_model_path("~/model"));
        assert!(!looks_like_local_model_path("minishlab/potion-base-8M"));
    }

    #[tokio::test]
    async fn missing_local_model_path_returns_specific_error() {
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

        let err = load(&missing_path)
            .await
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
        let model = load("minishlab/potion-base-8M")
            .await
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
        for embedded_sentence in embed_sentences.iter() {
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
