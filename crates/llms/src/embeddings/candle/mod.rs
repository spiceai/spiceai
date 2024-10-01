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

use super::{
    Embed, FailedToCreateEmbeddingSnafu, FailedToInstantiateEmbeddingModelSnafu,
    FailedToPrepareInputSnafu, Result,
};
use std::{
    collections::HashMap,
    fs,
    path::{self, Path, PathBuf},
    sync::{Arc, Mutex},
};

#[cfg(target_os = "windows")]
use std::os::windows::fs::symlink_file as symlink;

#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::symlink;

use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use hf_hub::api::sync::ApiBuilder;
use hf_hub::{Repo, RepoType};
use serde::Deserialize;
use snafu::ResultExt;
use tei_backend_core::{Backend, ModelType, Pool};
use tei_candle::{batch, sort_embeddings, CandleBackend};
use tempfile::tempdir;
use tokenizers::{Encoding, Tokenizer};

pub struct CandleEmbedding {
    backend: Arc<Mutex<CandleBackend>>,
    tok: Arc<Tokenizer>,
    model_cfg: ModelConfig,
}

/// Important fields from a model's `config.json`
#[derive(Debug, Deserialize)]
pub struct ModelConfig {
    pub hidden_size: i32,
}

static F32_DTYPE: &str = "float32";

impl CandleEmbedding {
    pub fn from_local(
        model_path: &Path,
        config_path: &Path,
        tokenizer_path: &Path,
    ) -> Result<Self> {
        // `text-embeddings-inference` expects the model artifacts to to be in a single folder with specific filenames.
        let files: HashMap<String, &Path> = vec![
            ("model.safetensors".to_string(), model_path),
            ("config.json".to_string(), config_path),
            ("tokenizer.json".to_string(), tokenizer_path),
        ]
        .into_iter()
        .collect();

        let model_root = link_files_into_tmp_dir(files)?;
        tracing::trace!(
            "Embedding model has files linked at location={:?}",
            model_root
        );
        Self::try_new(&model_root, F32_DTYPE)
    }

    pub fn from_hf(model_id: &str, revision: Option<&str>) -> Result<Self> {
        let model_root = download_hf_artifacts(model_id, revision)?;
        Self::try_new(&model_root, F32_DTYPE)
    }

    /// Attempt to create a new `CandleEmbedding` instance. Requires all model artifacts to be within a single folder.
    pub fn try_new(model_root: &Path, dtype: &str) -> Result<Self> {
        tracing::trace!(
            "Loading tokenizer from {:?}",
            model_root.join("tokenizer.json")
        );
        let tokenizer = Tokenizer::from_file(model_root.join("tokenizer.json"))
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
        tracing::trace!("Tokenizer loaded.");

        Ok(Self {
            backend: Arc::new(Mutex::new(
                CandleBackend::new(
                    model_root.to_path_buf(),
                    dtype.to_string(),
                    ModelType::Embedding(Pool::Cls),
                )
                .boxed()
                .context(FailedToInstantiateEmbeddingModelSnafu)?,
            )),
            tok: Arc::new(tokenizer),
            model_cfg: Self::model_config(model_root)?,
        })
    }

    fn model_config(model_root: &Path) -> Result<ModelConfig> {
        tracing::trace!(
            "Loading model config from {:?}",
            model_root.join("config.json")
        );
        let config_str = fs::read_to_string(model_root.join("config.json"))
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;

        tracing::trace!("Model config loaded.");

        let config: ModelConfig = serde_json::from_str(&config_str)
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;

        tracing::trace!("Model config parsed: {:?}", config);

        Ok(config)
    }
}

#[async_trait]
impl Embed for CandleEmbedding {
    async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>> {
        let add_special_tokens = true;

        let encodings: Vec<Encoding> = match input {
            EmbeddingInput::String(s) => vec![self
                .tok
                .encode::<String>(s, add_special_tokens)
                .context(FailedToPrepareInputSnafu)?],
            EmbeddingInput::StringArray(arr) => arr
                .into_iter()
                .map(|s| {
                    self.tok
                        .encode::<String>(s, add_special_tokens)
                        .context(FailedToPrepareInputSnafu)
                })
                .collect::<Result<Vec<_>>>()?,
            _ => {
                return Err(super::Error::FailedToPrepareInput {
                    source: "Unsupported input type".into(),
                })
            }
        };

        #[allow(clippy::cast_possible_truncation)]
        let pooled_idx = (0..=encodings.len()).map(|i| i as u32).collect::<Vec<_>>();
        let b = batch(encodings, pooled_idx, vec![]);

        let (pooled_embeddings, _raw_embeddings) = match self.backend.lock() {
            Ok(r) => sort_embeddings(r.embed(b).boxed().context(FailedToCreateEmbeddingSnafu)?),
            Err(e) => {
                tracing::error!("Failed to lock backend: {:?}", e);
                return Err(super::Error::FailedToCreateEmbedding {
                    source: "Failed to lock backend".into(),
                });
            }
        };

        Ok(pooled_embeddings)
    }

    fn size(&self) -> i32 {
        self.model_cfg.hidden_size
    }
}

/// For a given `HuggingFace` repo, download the needed files to create a `CandleEmbedding`.
pub fn download_hf_artifacts(model_id: &str, revision: Option<&str>) -> Result<PathBuf> {
    let api = ApiBuilder::new()
        .with_progress(false)
        .build()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    let repo = if let Some(revision) = revision {
        Repo::with_revision(model_id.to_string(), RepoType::Model, revision.to_string())
    } else {
        Repo::new(model_id.to_string(), RepoType::Model)
    };
    let api_repo = api.repo(repo.clone());

    tracing::trace!("Downloading 'config.json' for {}", repo.url());
    api_repo
        .get("config.json")
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    tracing::trace!("Downloading 'tokenizer.json' for {}", repo.url());
    api_repo
        .get("tokenizer.json")
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    tracing::trace!("Downloading 'model.safetensors' for {}", repo.url());
    let model = if let Ok(p) = api_repo.get("model.safetensors") {
        p
    } else {
        let p = api_repo
            .get("pytorch_model.bin")
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
        tracing::warn!("`model.safetensors` not found. Using `pytorch_model.bin` instead. Model loading will be significantly slower.");
        p
    };
    Ok(model
        .parent()
        .ok_or("".into())
        .context(FailedToInstantiateEmbeddingModelSnafu)?
        .to_path_buf())
}

/// Create a temporary directory with the provided files softlinked into the base folder (i.e not nested). The files are linked with to names defined in the hashmap, as keys.
///
/// Example:
///
/// ```rust
/// use std::collections::HashMap;
/// use std::path::Path;
///
/// let files: HashMap<String, &Path> = vec![
///    ("model.safetensors".to_string(), Path::new("path/to/model.safetensors")),
///   ("config.json".to_string(), Path::new("path/to/irrelevant_filename.json")),
/// ].into_iter().collect();
///
/// let temp_dir = link_files_into_tmp_dir(files).unwrap();
///
/// ```
///
fn link_files_into_tmp_dir(files: HashMap<String, &Path>) -> Result<PathBuf> {
    let temp_dir = tempdir()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    for (name, file) in files {
        let Ok(abs_path) = path::absolute(file) else {
            return Err(super::Error::FailedToCreateEmbedding {
                source: format!(
                    "Failed to get absolute path of provided file: {}",
                    file.as_os_str().to_string_lossy()
                )
                .into(),
            });
        };
        symlink(abs_path, temp_dir.path().join(name))
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
    }

    Ok(temp_dir.into_path())
}
