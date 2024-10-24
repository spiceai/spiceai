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

use std::{
    collections::HashMap,
    fs,
    path::{self, Path, PathBuf},
};

use crate::embeddings::{
    candle::ModelConfig, Error, FailedToInstantiateEmbeddingModelSnafu, Result,
};
use async_openai::types::EmbeddingInput;
use hf_hub::{api::sync::ApiBuilder, Repo, RepoType};
use serde::Deserialize;
use snafu::ResultExt;
use tei_backend::Pool;
use tei_core::tokenization::EncodingInput;
use tempfile::tempdir;
use tokenizers::Tokenizer;

pub(crate) fn load_tokenizer(model_root: &Path) -> Result<Tokenizer> {
    let tokenizer = Tokenizer::from_file(model_root.join("tokenizer.json"))
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    Ok(tokenizer)
}

pub(crate) fn load_config(model_root: &Path) -> Result<ModelConfig> {
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

pub(crate) fn position_offset(config: &ModelConfig) -> usize {
    // Position IDs offset. Used for Roberta and camembert.
    if config.model_type == "xlm-roberta"
        || config.model_type == "camembert"
        || config.model_type == "roberta"
    {
        config.pad_token_id + 1
    } else {
        0
    }
}

/// Converts the `OpenAI` format to the TEI format
pub(crate) fn inputs_from_openai(input: &EmbeddingInput) -> Vec<EncodingInput> {
    match input {
        EmbeddingInput::String(s) => vec![EncodingInput::Single(s.to_string())],
        EmbeddingInput::StringArray(ref arr) => arr
            .iter()
            .map(|s| EncodingInput::Single(s.clone()))
            .collect::<Vec<_>>(),
        EmbeddingInput::IntegerArray(i) => vec![EncodingInput::Ids(i.clone())],
        EmbeddingInput::ArrayOfIntegerArray(arr) => arr
            .iter()
            .map(|x| EncodingInput::Ids(x.clone()))
            .collect::<Vec<_>>(),
    }
}

/// For a given `HuggingFace` repo, download the needed files to create a `CandleEmbedding`.
pub(crate) fn download_hf_artifacts(
    model_id: &str,
    revision: Option<&str>,
    hf_token: Option<String>,
) -> Result<PathBuf> {
    let api = ApiBuilder::new()
        .with_progress(false)
        .with_token(hf_token)
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

    tracing::trace!("Downloading '1_Pooling/config.json' for {}", repo.url());
    if let Err(e) = api_repo.get("1_Pooling/config.json") {
        // May not be an issue, will be checked later.
        tracing::trace!(
            "`1_Pooling/config.json` not found for {model_id}@{revision}. Error: {e}",
            revision = revision.unwrap_or_default()
        );
    }

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
pub(crate) fn link_files_into_tmp_dir(files: HashMap<String, &Path>) -> Result<PathBuf> {
    let temp_dir = tempdir()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    for (name, file) in files {
        let Ok(abs_path) = path::absolute(file) else {
            return Err(Error::FailedToCreateEmbedding {
                source: format!(
                    "Failed to get absolute path of provided file: {}",
                    file.as_os_str().to_string_lossy()
                )
                .into(),
            });
        };

        // Hard link so windows can handle it without developer mode.
        std::fs::hard_link(abs_path, temp_dir.path().join(name))
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
    }

    Ok(temp_dir.into_path())
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PoolConfig {
    pooling_mode_cls_token: bool,
    pooling_mode_mean_tokens: bool,
    #[serde(default)]
    pooling_mode_lasttoken: bool,
}

impl From<PoolConfig> for Option<Pool> {
    fn from(value: PoolConfig) -> Self {
        if value.pooling_mode_cls_token {
            return Some(Pool::Cls);
        }
        if value.pooling_mode_mean_tokens {
            return Some(Pool::Mean);
        }
        if value.pooling_mode_lasttoken {
            return Some(Pool::LastToken);
        }
        None
    }
}

pub(crate) fn pool_from_str(p: &str) -> Option<Pool> {
    match p {
        "cls" => Some(Pool::Cls),
        "mean" => Some(Pool::Mean),
        "splade" => Some(Pool::Splade),
        "last_token" => Some(Pool::LastToken),
        _ => None,
    }
}
