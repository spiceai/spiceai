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
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

use super::{
    Embed, FailedToCreateEmbeddingSnafu, FailedToInstantiateEmbeddingModelSnafu,
    FailedToPrepareInputSnafu, Result,
};
use std::{
    fs,
    os::unix::fs::symlink,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use candle_core::DType;
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
    tok: Tokenizer,
    model_cfg: ModelConfig,
}

/// Important fields from a model's `config.json`
#[derive(Debug, Deserialize)]
pub struct ModelConfig {
    pub hidden_size: i32,
}

impl CandleEmbedding {
    pub fn from_local(
        model_path: &Path,
        config_path: &Path,
        tokenizer_path: &Path,
    ) -> Result<Self> {
        let model_root = link_files_into_tmp_dir(vec![model_path, config_path, tokenizer_path])?;
        Self::try_new(&model_root, DType::F32)
    }

    pub fn from_hf(model_id: &str, revision: Option<&str>) -> Result<Self> {
        println!("Downloading model: {model_id} revision: {revision:?}");
        let model_root = download_hf_artifacts(model_id, revision)?;
        Self::try_new(&model_root, DType::F32)
    }

    /// Attemmpt to create a new `CandleEmbedding` instance. Requires all model artifacts to be within a single folder.
    pub fn try_new(model_root: &Path, dtype: DType) -> Result<Self> {
        Ok(Self {
            backend: Arc::new(Mutex::new(
                CandleBackend::new(
                    model_root.to_path_buf(),
                    Self::convert_dtype(dtype),
                    ModelType::Embedding(Pool::Cls),
                )
                .boxed()
                .context(FailedToInstantiateEmbeddingModelSnafu)?,
            )),
            tok: Tokenizer::from_file(model_root.join("tokenizer.json"))
                .context(FailedToInstantiateEmbeddingModelSnafu)?,
            model_cfg: Self::model_config(model_root)?,
        })
    }

    fn convert_dtype(dtype: DType) -> String {
        let dtype_str = if dtype == DType::F32 {
            "float32"
        } else if dtype == DType::F16 {
            "float16"
        } else {
            dtype.as_str()
        };
        dtype_str.to_string()
    }

    fn model_config(model_root: &Path) -> Result<ModelConfig> {
        let config_path = model_root.join("config.json");
        let config = fs::read_to_string(config_path)
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
        let config: ModelConfig = serde_json::from_str(&config)
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
        Ok(config)
    }
}

#[async_trait]
impl Embed for CandleEmbedding {
    async fn embed(&mut self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>> {
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
    let builder = ApiBuilder::new().with_progress(false);

    let api = builder
        .build()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;
    let api_repo = if let Some(revision) = revision {
        api.repo(Repo::with_revision(
            model_id.to_string(),
            RepoType::Model,
            revision.to_string(),
        ))
    } else {
        api.repo(Repo::new(model_id.to_string(), RepoType::Model))
    };
    println!("Downloading 'config.json'");
    api_repo
        .get("config.json")
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    println!("Downloading 'tokenizer.json'");
    api_repo
        .get("tokenizer.json")
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    println!("Downloading 'model.safetensors'");
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

/// Create a temporary directory with the provided files softlinked into the base folder (i.e not nested).
///
/// TODO: make this a Hashmap to predefine linked file names.
fn link_files_into_tmp_dir(files: Vec<&Path>) -> Result<PathBuf> {
    let temp_dir = tempdir()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    for file in files {
        if let Some(file_name) = file.file_name() {
            let temp_file_path = temp_dir.path().join(file_name);
            symlink(file, &temp_file_path)
                .boxed()
                .context(FailedToInstantiateEmbeddingModelSnafu)?;
        }
    }

    Ok(temp_dir.into_path())
}
