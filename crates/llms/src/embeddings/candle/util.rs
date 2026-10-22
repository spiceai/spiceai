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
    Error, FailedToInstantiateEmbeddingModelSnafu, Result, candle::ModelConfig,
};
use crate::hf::{SAFETENSORS_INDEX, read_shards};
use async_openai::types::embeddings::EmbeddingInput;
use hf_hub::{Repo, RepoType, api::tokio::ApiBuilder};
use huggingface_hub::{DownloadError, Snapshot};
use serde::Deserialize;
use snafu::ResultExt;
use std::{
    collections::HashMap,
    fs,
    path::{self, Path, PathBuf},
};
use tei_backend::Pool;
use tei_core::{
    download::ST_CONFIG_NAMES,
    tokenization::{EncodingInput, Tokenization},
};

use tempfile::tempdir;
use tokenizers::Tokenizer;

pub(crate) fn load_tokenizer(model_root: &Path) -> Result<Tokenizer> {
    tracing::trace!(
        "Loading model tokenizer from {:?}",
        model_root.join("tokenizer.json")
    );
    let mut tokenizer = Tokenizer::from_file(model_root.join("tokenizer.json"))
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

    // Some Sentence-Transformers tokenizers bake a fixed-length padding and truncation into `tokenizer.json`.
    // Clear both here so TEI is the single source of truth, matching upstream text-embeddings-inference.
    tokenizer.with_padding(None);
    tokenizer
        .with_truncation(None)
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

/// Result of [`load_tokenization`]: the parsed tokenizer, the model config, and
/// the derived [`Tokenization`] settings needed to build a TEI `Infer` pipeline.
pub(crate) struct LoadedTokenization {
    pub tokenizer: Tokenizer,
    pub config: ModelConfig,
    pub tokenization: Tokenization,
}

/// Loads the tokenizer, config, and derives the [`Tokenization`] settings needed to build a TEI
/// `Infer` pipeline from a directory of model artifacts. Shared by
/// [`crate::embeddings::candle::tei::TeiEmbed::from_dir`] and
/// [`crate::rerank::tei::TeiRerank::from_dir`], which both instantiate the same backend and would
/// otherwise duplicate this setup.
///
/// Runs the synchronous load — reading `config.json`, the sentence-transformers
/// config, and parsing `tokenizer.json` — on a blocking thread. For a large
/// tokenizer the parse alone can exceed the runtime's per-task latency budget,
/// so keeping it off the Tokio worker thread prevents it from stalling other
/// tasks (and `/health`) during model registration.
pub(crate) async fn load_tokenization(
    root: &Path,
    max_seq_length_overwrite: Option<usize>,
) -> Result<LoadedTokenization> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let tokenizer = load_tokenizer(&root)?;
        let config = load_config(&root)?;
        let position_offset = position_offset(&config);

        let max_input_length = if let Some(max_seq_length) = max_seq_length_overwrite {
            max_seq_length
        } else {
            // Some models will have `sentence_*_config.json` file defining a specific `max_seq_length`.
            match max_seq_length_from_st_config(&root) {
                Ok(max_seq_length_opt) => {
                    max_seq_length_opt.unwrap_or(config.max_position_embeddings - position_offset)
                }
                Err(e) => {
                    tracing::warn!("Failed to load max_seq_length from ST config: {e}");
                    config.max_position_embeddings - position_offset
                }
            }
        };

        let tokenization = Tokenization::new(
            1,
            tokenizer.clone(),
            max_input_length,
            position_offset,
            None,
            None,
        );

        Ok(LoadedTokenization {
            tokenizer,
            config,
            tokenization,
        })
    })
    .await
    .boxed()
    .context(FailedToInstantiateEmbeddingModelSnafu)?
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
        EmbeddingInput::String(s) => vec![EncodingInput::Single(s.clone())],
        EmbeddingInput::StringArray(arr) => arr
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

/// Builds a `HuggingFace` API client, honouring `HF_HUB_CACHE` and applying `hf_token`
/// only when it actually carries a value.
///
/// `ApiBuilder::with_token` *overwrites* the builder's token, and `ApiBuilder::new`
/// pre-populates it from the Hub's own credential file (`~/.cache/huggingface/token`,
/// written by `huggingface-cli login`). Passing `None` through therefore discards a
/// credential the machine already has, leaving every download anonymous and at the
/// mercy of the Hub's unauthenticated access rules. An empty string is worse than
/// `None`: it is sent as an empty bearer token and rejected with 401. So both are
/// treated as "no token supplied — keep whatever the Hub cache provided".
fn hf_api_builder(hf_token: Option<&str>) -> ApiBuilder {
    let mut builder = ApiBuilder::new().with_progress(false);

    if let Some(token) = hf_token.map(str::trim).filter(|t| !t.is_empty()) {
        builder = builder.with_token(Some(token.to_string()));
    }

    if let Ok(cache_dir) = std::env::var("HF_HUB_CACHE") {
        let cache_path: PathBuf = cache_dir.into();
        if cache_path.exists() {
            tracing::debug!("Using HF_HUB_CACHE directory {:?}", cache_path);
            builder = builder.with_cache_dir(cache_path);
        } else {
            tracing::debug!(
                "HF_HUB_CACHE directory {:?} does not exist, ignoring.",
                cache_path
            );
        }
    }

    builder
}

pub async fn download_hf_file(
    repo_id: &str,
    revision: Option<&str>,
    repo_type_opt: Option<&str>,
    file: &str,
    hf_token: Option<&str>,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let api = hf_api_builder(hf_token).build().boxed()?;

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

/// Downloads the files the TEI backend loads an embedding or reranker model from into the
/// Hugging Face cache, and returns the snapshot directory that holds them. One listing of the
/// repository picks them, as `text-embeddings-inference` would: `config.json` and
/// `tokenizer.json`; the Sentence Transformers configs the repository has
/// (`1_Pooling/config.json`, `config_sentence_transformers.json`, and the first of
/// [`ST_CONFIG_NAMES`]); and the weights: `model.safetensors`, else the shards
/// `model.safetensors.index.json` names, else `pytorch_model.bin`.
///
/// # Errors
///
/// Returns an error if the repository lacks `config.json`, `tokenizer.json` or weights, or a
/// download fails.
pub(crate) async fn download_hf_artifacts(snapshot: &Snapshot) -> Result<PathBuf, DownloadError> {
    let (files, weights) = tei_files(|path| snapshot.file(path).is_some())
        .map_err(|missing| DownloadError::missing(snapshot, missing))?;
    let mut paths: Vec<String> = files.iter().map(ToString::to_string).collect();
    match weights {
        TeiWeights::Safetensors => {}
        TeiWeights::ShardedSafetensors => paths.extend(read_shards(snapshot).await?),
        TeiWeights::Pickle => tracing::warn!(
            "{} has no safetensors weights in Hugging Face repo '{}', so it loads 'pytorch_model.bin', which is significantly slower.",
            snapshot.requester().capitalized(),
            snapshot.repo()
        ),
    }
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    snapshot.download(&paths).await?;
    Ok(snapshot.dir())
}

/// The weights the TEI backend loads from a model's directory.
#[derive(Debug, PartialEq, Eq)]
enum TeiWeights {
    /// `model.safetensors`.
    Safetensors,
    /// `model.safetensors.index.json` and the shards it names.
    ShardedSafetensors,
    /// `pytorch_model.bin`, which loads significantly slower.
    Pickle,
}

/// The files the TEI backend reads that a repository holds, chosen as
/// `text-embeddings-inference` chooses them, with `has` saying whether the repository holds a
/// path; or, when the repository lacks something the backend needs, what it lacks.
fn tei_files(has: impl Fn(&str) -> bool) -> Result<(Vec<&'static str>, TeiWeights), String> {
    let mut paths = Vec::new();
    for required in ["config.json", "tokenizer.json"] {
        if !has(required) {
            return Err(format!("'{required}'"));
        }
        paths.push(required);
    }
    for optional in ["1_Pooling/config.json", "config_sentence_transformers.json"] {
        if has(optional) {
            paths.push(optional);
        }
    }
    if let Some(st_config) = ST_CONFIG_NAMES.iter().find(|name| has(name)) {
        paths.push(*st_config);
    }
    let weights = if has("model.safetensors") {
        paths.push("model.safetensors");
        TeiWeights::Safetensors
    } else if has(SAFETENSORS_INDEX) {
        paths.push(SAFETENSORS_INDEX);
        TeiWeights::ShardedSafetensors
    } else if has("pytorch_model.bin") {
        paths.push("pytorch_model.bin");
        TeiWeights::Pickle
    } else {
        return Err(
            "weights the model can load ('model.safetensors', 'model.safetensors.index.json' or 'pytorch_model.bin')"
                .to_string(),
        );
    };
    Ok((paths, weights))
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
#[expect(clippy::implicit_hasher)]
pub fn link_files_into_tmp_dir(files: HashMap<String, PathBuf>) -> Result<PathBuf> {
    let temp_dir = tempdir()
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?
        .keep();

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

/// Async wrapper around [`link_files_into_tmp_dir`] that runs the synchronous
/// hard-linking filesystem I/O on a blocking thread, so it never stalls a Tokio
/// worker thread (and `/health`) during model registration.
pub async fn link_files_into_tmp_dir_blocking(files: HashMap<String, PathBuf>) -> Result<PathBuf> {
    tokio::task::spawn_blocking(move || link_files_into_tmp_dir(files))
        .await
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?
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

#[cfg(test)]
mod tests {
    use super::{TeiWeights, load_tokenizer, tei_files};
    use tempfile::tempdir;
    use tokenizers::models::bpe::BPE;
    use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

    /// The files `text-embeddings-inference` would read, from what a repository holds.
    #[test]
    fn tei_files_are_chosen_as_text_embeddings_inference_chooses_them() {
        let repo = |names: &'static [&'static str]| move |path: &str| names.contains(&path);
        // A Sentence Transformers repository: its configs and its single safetensors file, not
        // its pickle, ONNX export or other legacy configs.
        assert_eq!(
            tei_files(repo(&[
                "config.json",
                "tokenizer.json",
                "1_Pooling/config.json",
                "config_sentence_transformers.json",
                "sentence_roberta_config.json",
                "sentence_bert_config.json",
                "modules.json",
                "model.safetensors",
                "pytorch_model.bin",
                "onnx/model.onnx",
            ])),
            Ok((
                vec![
                    "config.json",
                    "tokenizer.json",
                    "1_Pooling/config.json",
                    "config_sentence_transformers.json",
                    "sentence_bert_config.json",
                    "model.safetensors",
                ],
                TeiWeights::Safetensors
            ))
        );
        // A legacy config is read when it is the only one; sharded safetensors come before a
        // pickle.
        assert_eq!(
            tei_files(repo(&[
                "config.json",
                "tokenizer.json",
                "sentence_xlnet_config.json",
                "model.safetensors.index.json",
                "pytorch_model.bin",
            ])),
            Ok((
                vec![
                    "config.json",
                    "tokenizer.json",
                    "sentence_xlnet_config.json",
                    "model.safetensors.index.json",
                ],
                TeiWeights::ShardedSafetensors
            ))
        );
        assert_eq!(
            tei_files(repo(&[
                "config.json",
                "tokenizer.json",
                "pytorch_model.bin"
            ])),
            Ok((
                vec!["config.json", "tokenizer.json", "pytorch_model.bin"],
                TeiWeights::Pickle
            ))
        );
        // What a repository lacks, worded to follow "the repo has no".
        assert_eq!(
            tei_files(repo(&["config.json", "model.safetensors"])),
            Err("'tokenizer.json'".to_string())
        );
        assert_eq!(
            tei_files(repo(&["config.json", "tokenizer.json", "onnx/model.onnx"])),
            Err("weights the model can load ('model.safetensors', 'model.safetensors.index.json' or 'pytorch_model.bin')".to_string())
        );
    }

    // Regression test for #13415: a tokenizer.json that bakes in a fixed-length padding and
    // truncation must not keep them. TEI owns padding/masking; a leaked fixed padding pads every
    // input to that width and the padding then corrupts mean-pooled embeddings (near-random
    // retrieval). `load_tokenizer` must strip both so TEI is the single source of truth.
    #[test]
    fn load_tokenizer_clears_baked_in_padding_and_truncation() {
        let mut tokenizer = Tokenizer::new(BPE::default());
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::Fixed(128),
            ..Default::default()
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: 128,
                ..Default::default()
            }))
            .expect("failed to set truncation on fixture tokenizer");

        // Sanity check that the fixture actually carries the settings we intend to strip.
        assert!(tokenizer.get_padding().is_some());
        assert!(tokenizer.get_truncation().is_some());

        let dir = tempdir().expect("failed to create temp dir");
        tokenizer
            .save(dir.path().join("tokenizer.json"), false)
            .expect("failed to save fixture tokenizer");

        let loaded = load_tokenizer(dir.path()).expect("failed to load tokenizer");

        assert!(
            loaded.get_padding().is_none(),
            "load_tokenizer must clear the baked-in padding"
        );
        assert!(
            loaded.get_truncation().is_none(),
            "load_tokenizer must clear the baked-in truncation"
        );
    }
}
