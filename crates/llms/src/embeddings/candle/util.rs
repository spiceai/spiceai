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

use crate::embeddings::{
    candle::ModelConfig, Error, FailedToInstantiateEmbeddingModelSnafu, FailedWithHFApiSnafu,
    Result,
};
use async_openai::types::EmbeddingInput;
use hf_hub::{
    api::tokio::{ApiBuilder, ApiRepo},
    Repo, RepoType,
};
use serde::Deserialize;
use snafu::ResultExt;
use std::{
    collections::HashMap,
    fs, io,
    path::{self, Path, PathBuf},
};
use tei_backend::Pool;
use tei_core::{
    download::{download_artifacts, download_pool_config, download_st_config, ST_CONFIG_NAMES},
    tokenization::EncodingInput,
};

use tempfile::tempdir;
use tokenizers::Tokenizer;
use tracing;

pub(crate) fn load_tokenizer(model_root: &Path) -> Result<Tokenizer> {
    tracing::trace!(
        "Loading model tokenizer from {:?}",
        model_root.join("tokenizer.json")
    );
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

fn get_api(model_id: &str, revision: Option<&str>, hf_token: Option<&str>) -> Result<ApiRepo> {
    let api = ApiBuilder::new()
        .with_progress(false)
        .with_token(hf_token.map(ToString::to_string))
        .build()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    let repo = if let Some(revision) = revision {
        Repo::with_revision(model_id.to_string(), RepoType::Model, revision.to_string())
    } else {
        Repo::new(model_id.to_string(), RepoType::Model)
    };
    let api_repo = api.repo(repo.clone());

    Ok(api_repo)
}

pub async fn download_hf_file(
    repo_id: &str,
    revision: Option<&str>,
    repo_type_opt: Option<&str>,
    file: &str,
    hf_token: Option<&str>,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let api = ApiBuilder::new()
        .with_progress(false)
        .with_token(hf_token.map(ToString::to_string))
        .build()
        .boxed()?;

    let repo_type = match repo_type_opt {
        Some("datasets") => RepoType::Dataset,
        Some("spaces") => RepoType::Space,
        _ => RepoType::Model,
    };

    let repo = if let Some(revision) = revision {
        Repo::with_revision(repo_id.to_string(), repo_type, revision.to_string())
    } else {
        Repo::new(repo_id.to_string(), repo_type)
    };
    api.repo(repo).get(file).await.boxed()
}

/// For a given `HuggingFace` repo, download the needed files to create a `CandleEmbedding`.
pub(crate) async fn download_hf_artifacts(
    model_id: &str,
    revision: Option<&str>,
    hf_token: Option<&str>,
) -> Result<PathBuf> {
    let api_repo = get_api(model_id, revision, hf_token)?;
    let repo_url = api_repo.url("");

    tracing::trace!("Downloading artifacts for {repo_url}");
    tracing::debug!("Starting download of model artifacts from {repo_url}");
    let root_dir = download_artifacts(&api_repo)
        .await
        .context(FailedWithHFApiSnafu)?;

    tracing::trace!("Downloading pool config for {repo_url}");
    let _ = download_pool_config(&api_repo)
        .await
        .context(FailedWithHFApiSnafu)?;

    tracing::trace!("Downloading sentence transformer config for {repo_url}");
    let _ = download_st_config(&api_repo)
        .await
        .context(FailedWithHFApiSnafu)?;

    tracing::debug!("Model artifacts downloaded successfully to {:?}", root_dir);
    Ok(root_dir)
}

/// For a local repo of model artifacts, attempt to find a relevant `sentence_transformers` config file, and extract the `max_seq_length` from it.
///
/// If no config file is found, or config files don't containt `max_seq_length`, return `None`.
pub(crate) fn max_seq_length_from_st_config(
    model_root: &Path,
) -> Result<Option<usize>, serde_json::Error> {
    #[derive(Debug, Deserialize)]
    pub struct STConfig {
        max_seq_length: usize,
    }
    for name in ST_CONFIG_NAMES {
        let config_path = model_root.join(name);
        if let Ok(config) = fs::read_to_string(config_path) {
            let st_config: STConfig = serde_json::from_str(config.as_str())?;
            return Ok(Some(st_config.max_seq_length));
        }
    }
    Ok(None)
}

/// Create a temporary directory with the provided files softlinked into the base folder (i.e not nested). The files are linked with to names defined in the hashmap, as keys.
///
/// Example:
///
/// ```rust
/// use std::collections::HashMap;
/// use std::path::PathBuf;
/// use llms::embeddings::candle::link_files_into_tmp_dir;
///
/// let files: HashMap<String, PathBuf> = vec![
///    ("model.safetensors".to_string(), PathBuf::from("path/to/model.safetensors")),
///   ("config.json".to_string(), PathBuf::from("path/to/irrelevant_filename.json")),
/// ].into_iter().collect();
///
/// let temp_dir = link_files_into_tmp_dir(files);
///
/// ```
///
#[allow(clippy::implicit_hasher)]
pub fn link_files_into_tmp_dir(files: HashMap<String, PathBuf>) -> Result<PathBuf> {
    let temp_dir = tempdir()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?
        .into_path();

    for (name, file) in files {
        let Ok(abs_path) = path::absolute(&file) else {
            return Err(Error::FailedToCreateEmbedding {
                source: format!(
                    "Failed to get absolute path of provided file: {}",
                    file.to_string_lossy()
                )
                .into(),
            });
        };

        // Hard link so windows can handle it without developer mode.
        std::fs::hard_link(abs_path, temp_dir.join(name))
            .boxed()
            .context(FailedToInstantiateEmbeddingModelSnafu)?;
    }

    Ok(temp_dir)
}

/// Clean up a model directory by removing all files and the directory itself.
/// This is used during graceful shutdown to ensure no temporary files are left behind.
pub fn cleanup_model_dir(dir_path: &Path) -> io::Result<()> {
    // We need to use tracing in a way that works across crates
    let path_str = dir_path.to_string_lossy().to_string();
    tracing::debug!("Starting model cleanup for: {}", path_str);

    // Use a more robust process to ensure cleanup happens even during forced shutdown
    let result = std::panic::catch_unwind(|| {
        if !dir_path.exists() {
            // Directory doesn't exist, nothing to do
            let path_str = dir_path.to_string_lossy().to_string();
            tracing::debug!(
                "Model directory doesn't exist, no cleanup needed: {}",
                path_str
            );
            return Ok(());
        }

        if dir_path.is_dir() {
            let path_str = dir_path.to_string_lossy().to_string();
            tracing::debug!("Starting cleanup of model directory: {}", path_str);

            // First attempt to remove each file individually (for better error reporting and counting)
            let mut cleaned_files = 0;
            let mut file_list = Vec::new();

            if let Ok(entries) = fs::read_dir(dir_path) {
                for entry_result in entries {
                    if let Ok(entry) = entry_result {
                        let path = entry.path();
                        if path.is_file() {
                            let file_str = path.to_string_lossy().to_string();
                            file_list.push(file_str.clone());
                            if let Err(e) = fs::remove_file(&path) {
                                tracing::debug!("Failed to remove model file {}: {}", file_str, e);
                            } else {
                                cleaned_files += 1;
                            }
                        }
                    }
                }
            }

            if !file_list.is_empty() {
                tracing::debug!("Cleaned up {} model files", cleaned_files);
                // Only log details at trace level to avoid too much output
                tracing::trace!("Removed files: {:?}", file_list);
            }

            // Then remove the directory itself (which should also clean up any remaining files)
            let path_str = dir_path.to_string_lossy().to_string();
            match fs::remove_dir_all(dir_path) {
                Ok(_) => tracing::debug!(
                    "SUCCESS: Successfully removed model directory: {}",
                    path_str
                ),
                Err(e) => tracing::debug!("Failed to remove model directory {}: {}", path_str, e),
            }
            Ok(())
        } else {
            let path_str = dir_path.to_string_lossy().to_string();
            tracing::debug!("Not a model directory: {}", path_str);
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Not a directory",
            ))
        }
    });

    // Handle any panics during cleanup
    let path_str = dir_path.to_string_lossy().to_string();
    match result {
        Ok(io_result) => io_result,
        Err(e) => {
            tracing::debug!(
                "Panic during model directory cleanup of {}: {:?}",
                path_str,
                e
            );
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Panic during cleanup: {:?}", e),
            ))
        }
    }
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
