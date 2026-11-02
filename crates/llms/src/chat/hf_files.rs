/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The files of a Hugging Face model repository mistral.rs loads, downloaded by Spice so the
//! engine reads a local directory and never the network.
//!
//! mistral.rs reads a local model directory by its top-level file names: `tokenizer.json` or
//! `tekken.json`, `config.json` or `params.json`, the chat template files, the processor and
//! generation configs, and the weights. The weights are chosen as it chooses them —
//! `model.safetensors.index.json` and the shards it names, else every `model-NNNNN-of-NNNNN`,
//! `model` or `consolidated` safetensors file, else sharded `pytorch_model-…` pickles — and
//! every top-level JSON or Jinja file comes along, so a config the engine reads by name is
//! there whichever it is.
//!
//! A GGUF model loads the files its `files` resolve to ([`gguf_files::resolve`]): exact
//! files with their sibling shards, or a quantization by name with the repository's draft
//! head. A GGUF repository named with no `files` loads the largest complete quantization that
//! fits the memory the engine reports.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use huggingface_hub::{DownloadError, Requester, Snapshot};
use regex::Regex;

use crate::chat::gguf_files::{self, ListedFile, Quantization};
use crate::hf::HubParams;

use crate::hf::SAFETENSORS_INDEX as SAFETENSOR_INDEX;
const DOCS_URL: &str = "https://spiceai.org/docs/components/models/huggingface";

/// What a model loads, once downloaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Downloaded {
    /// The snapshot directory the files are in.
    pub dir: PathBuf,
    /// The GGUF files the model loads, as repository paths in load order: the weights, shard 1
    /// first, then a draft head. Empty for a model mistral.rs loads from safetensors.
    pub gguf: Vec<String>,
}

/// What a model that names no GGUF files chooses its quantization by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Budget {
    /// The memory free for the model, in bytes: the largest quantization whose weights fit it.
    Free(u64),
    /// No engine measures the memory, so only a repository's one quantization is chosen.
    Unmeasured,
    /// The model is split across machines, each of which would choose by its own memory, so
    /// only a repository's one quantization is chosen: every machine must load the same files.
    Split,
}

/// Downloads the files the model `model_id` (`org/name[:revision]`) loads from, for the model
/// named `name`: the GGUF files `files` resolve to, or, when `files` names none, the GGUF
/// quantization `budget` chooses or the files mistral.rs loads from a safetensors repository.
///
/// # Errors
///
/// Returns an error if the repository or revision cannot be read, `files` names something it
/// lacks, no quantization fits, a file the engine needs is missing, or a download fails.
pub async fn download_model(
    model_id: &str,
    files: &[&str],
    budget: Budget,
    hub: HubParams<'_>,
    name: &str,
) -> Result<Downloaded, DownloadError> {
    let snapshot = open(model_id, hub, name).await?;
    let gguf = resolve_gguf(&snapshot, files, budget, name, repo_of(model_id))?;
    let paths: Vec<&str> = gguf.iter().map(String::as_str).collect();
    let dir = download(&snapshot, &paths).await?;
    Ok(Downloaded { dir, gguf })
}

/// What a model loads, resolved from the Hub without downloading the weights.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    /// The GGUF files the model loads, as [`Downloaded::gguf`]: empty for a model mistral.rs
    /// loads from safetensors.
    pub gguf: Vec<String>,
    /// The architecture the first shard's header names; `None` when no header names one.
    pub architecture: Option<String>,
}

/// The GGUF files the model `model_id` (`org/name[:revision]`) loads, resolved from `files` and
/// `budget` as [`download_model`] resolves them, and the architecture their first shard's
/// header names, both read from the Hub without downloading the weights. A node that holds a
/// share of a model split across nodes decides from them which engine serves the model, as the
/// node that downloads the weights does.
///
/// # Errors
///
/// Returns an error if the repository or revision cannot be read, `files` names something it
/// lacks, no quantization fits, or a read fails.
pub async fn model_architecture(
    model_id: &str,
    files: &[&str],
    budget: Budget,
    hub: HubParams<'_>,
    name: &str,
) -> Result<Resolved, DownloadError> {
    let snapshot = open(model_id, hub, name).await?;
    let gguf = resolve_gguf(&snapshot, files, budget, name, repo_of(model_id))?;
    let paths: Vec<&str> = gguf.iter().map(String::as_str).collect();
    let architecture = super::gguf_header::model_architecture(&paths, |file: &&str, len| {
        let file = (*file).to_string();
        let snapshot = &snapshot;
        async move { snapshot.read_prefix(&file, len).await }
    })
    .await?;
    Ok(Resolved { gguf, architecture })
}

/// The repository `model_id` (`org/name[:revision]`) names.
fn repo_of(model_id: &str) -> &str {
    model_id.split_once(':').map_or(model_id, |(repo, _)| repo)
}

/// Opens the model `model_id` (`org/name[:revision]`), named `name` in the Spicepod.
async fn open(model_id: &str, hub: HubParams<'_>, name: &str) -> Result<Snapshot, DownloadError> {
    let requester = Requester {
        kind: "model",
        name: name.to_string(),
        docs_url: DOCS_URL,
        retried: true,
    };
    let (repo, revision) = match model_id.split_once(':') {
        Some((repo, revision)) => (repo, Some(revision).filter(|r| !r.is_empty())),
        None => (model_id, None),
    };
    crate::hf::open_model(requester, repo, revision, hub).await
}

/// The GGUF files a model loads from `snapshot`: those `files` resolve to, or with none named,
/// the largest complete quantization whose weights fit `memory`, and its draft head. Empty for
/// a repository that offers no GGUF quantization or that mistral.rs loads from safetensors.
fn resolve_gguf(
    snapshot: &Snapshot,
    files: &[&str],
    budget: Budget,
    name: &str,
    repo: &str,
) -> Result<Vec<String>, DownloadError> {
    let listing: Vec<ListedFile> = snapshot
        .files()
        .map(|file| ListedFile {
            path: file.path.clone(),
            size: file.size,
        })
        .collect();
    if let Some(resolved) = gguf_files::resolve(&listing, files)
        .map_err(|error| DownloadError::unusable(snapshot, error.to_string()))?
    {
        if files.iter().any(|file| !is_gguf_path(file)) {
            tracing::info!(
                "Model '{name}' loads {} from Hugging Face repo '{repo}'{}.",
                describe(resolved.quantization.as_deref(), resolved.weights.len()),
                resolved
                    .draft
                    .first()
                    .map_or_else(String::new, |draft| format!(", drafting with '{draft}'"))
            );
        }
        return Ok(resolved
            .files()
            .into_iter()
            .map(ToString::to_string)
            .collect());
    }
    let names: Vec<&str> = listing.iter().map(|file| file.path.as_str()).collect();
    let quantizations = gguf_files::quantizations(&listing);
    if quantizations.is_empty() || weights(&names).is_some() {
        return Ok(Vec::new());
    }
    let chosen = choose_quantization(snapshot, &listing, &quantizations, budget, name, repo)?;
    let mut gguf = chosen.files.clone();
    gguf.extend(gguf_files::draft_head(&listing, chosen));
    Ok(gguf)
}

/// The quantization a model with no `files` loads: the only one, or the largest whose weights
/// fit `memory` with room for the context.
fn choose_quantization<'q>(
    snapshot: &Snapshot,
    listing: &[ListedFile],
    quantizations: &'q [Quantization],
    budget: Budget,
    name: &str,
    repo: &str,
) -> Result<&'q Quantization, DownloadError> {
    let names = || {
        quantizations
            .iter()
            .map(|q| format!("{} ({})", q.name, gigabytes(q.bytes)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let memory = match budget {
        Budget::Free(memory) => memory,
        Budget::Unmeasured | Budget::Split => {
            if let [only] = quantizations {
                return Ok(only);
            }
            let why = if budget == Budget::Split {
                "a model split across machines names its quantization, so that every machine loads the same weights"
            } else {
                "this build cannot measure the memory to choose one"
            };
            return Err(DownloadError::unusable(
                snapshot,
                format!(
                    "it offers {} GGUF quantizations ({}) and {why}. Name one in the model's `files`, as `files: [{{path: {}}}]`",
                    quantizations.len(),
                    names(),
                    quantizations.last().map_or("", |q| q.name.as_str())
                ),
            ));
        }
    };
    match gguf_files::choose(quantizations, gguf_files::weights_budget(memory)) {
        Ok(chosen) => {
            let larger = quantizations
                .iter()
                .rev()
                .find(|q| q.bytes > chosen.bytes)
                .map_or_else(String::new, |q| {
                    format!("; {} ({}) does not", q.name, gigabytes(q.bytes))
                });
            tracing::info!(
                "Model '{name}' loads {} ({}) from Hugging Face repo '{repo}', the largest of its {} complete GGUF quantizations whose weights fit the {} of memory free for it with room for the context{larger}. Pin it with `files: [{}]`.",
                describe(Some(&chosen.name), chosen.files.len()),
                gigabytes(chosen.bytes),
                quantizations.len(),
                gibibytes(memory),
                gguf_files::pin(listing, chosen)
                    .iter()
                    .map(|entry| format!("{{path: {entry}}}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            Ok(chosen)
        }
        Err(smallest) => Err(DownloadError::unusable(
            snapshot,
            format!(
                "none of its GGUF quantizations fits the {} of memory free for the model with room for the context{}. Name one in the model's `files` to load it anyway, across machines with `nodes` if one cannot hold it: {}",
                gibibytes(memory),
                smallest.map_or_else(String::new, |q| format!(
                    "; the smallest, {}, is {}",
                    q.name,
                    gigabytes(q.bytes)
                )),
                names()
            ),
        )),
    }
}

/// `quantization UD-Q4_K_XL (6 files)`, or `3 files` when the files are no one quantization.
fn describe(quantization: Option<&str>, files: usize) -> String {
    let files = if files == 1 {
        "1 file".to_string()
    } else {
        format!("{files} files")
    };
    quantization.map_or_else(|| files.clone(), |q| format!("quantization {q} ({files})"))
}

fn is_gguf_path(path: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gguf"))
}

/// Bytes as the Hub shows a file's size, in decimal gigabytes.
#[expect(clippy::cast_precision_loss, reason = "a size shown to one decimal")]
fn gigabytes(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / 1e9)
}

/// Bytes of memory, in binary gibibytes.
#[expect(clippy::cast_precision_loss, reason = "a size shown to one decimal")]
fn gibibytes(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / f64::from(1u32 << 30))
}

/// mistral.rs's weight file patterns (`mistralrs-core/src/pipeline/paths.rs`).
static SAFETENSORS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"model-\d+-of-\d+\.safetensors\b",
        r"model\.safetensors\b",
        r"consolidated\.safetensors\b",
    ]
    .iter()
    .filter_map(|pattern| Regex::new(pattern).ok())
    .collect()
});
static PICKLES: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"pytorch_model-\d{5}-of-\d{5}.((pth)|(pt)|(bin))\b").ok());

/// Downloads what a model loads from: the GGUF files `gguf` names (every shard, and any MTP
/// draft head), or, when it names none, the repo's safetensors (or sharded pickle) weights;
/// and the top-level configs. Returns the snapshot directory.
///
/// # Errors
///
/// Returns an error if the repository lacks a file the engine needs, or a download fails.
pub async fn download(snapshot: &Snapshot, gguf: &[&str]) -> Result<PathBuf, DownloadError> {
    let names: Vec<&str> = snapshot.files().map(|file| file.path.as_str()).collect();
    let mut paths = configs(&names);
    if gguf.is_empty() {
        for alternatives in [
            ["tokenizer.json", "tekken.json"],
            ["config.json", "params.json"],
        ] {
            if !alternatives.iter().any(|name| names.contains(name)) {
                return Err(DownloadError::missing(
                    snapshot,
                    format!("'{}' or '{}'", alternatives[0], alternatives[1]),
                ));
            }
        }
        let weights = match weights(&names) {
            Some(Weights::Index) => crate::hf::read_shards(snapshot).await?,
            Some(Weights::Files(files)) => files,
            None => {
                return Err(DownloadError::missing(
                    snapshot,
                    "weights the model can load ('model.safetensors', 'model-00001-of-0000N.safetensors', 'consolidated.safetensors' or sharded 'pytorch_model-00001-of-0000N.bin')",
                ));
            }
        };
        paths.extend(weights);
    } else {
        paths.extend(gguf.iter().map(ToString::to_string));
    }
    paths.sort();
    paths.dedup();
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    snapshot.download(&paths).await?;
    Ok(snapshot.dir())
}

/// The repository's top-level JSON and Jinja files: every config, tokenizer and chat template
/// the engine reads by name.
fn configs(names: &[&str]) -> Vec<String> {
    names
        .iter()
        .filter(|name| !name.contains('/'))
        .filter(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension == "json" || extension == "jinja")
        })
        .map(ToString::to_string)
        .collect()
}

/// The file mistral.rs reads a downloaded repository's chat template from: its
/// `chat_template.jinja`, else its `tokenizer_config.json`. Every top-level JSON and Jinja file
/// of the repository is downloaded with the model, so a file missing from `dir` is missing from
/// the repository.
pub async fn chat_template_file(dir: &Path) -> Option<PathBuf> {
    for name in ["chat_template.jinja", "tokenizer_config.json"] {
        let path = dir.join(name);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Some(path);
        }
    }
    None
}

/// Where the weights mistral.rs loads are named.
#[derive(Debug, PartialEq, Eq)]
enum Weights {
    /// In `model.safetensors.index.json`, which must be read first.
    Index,
    /// These files.
    Files(Vec<String>),
}

/// The weights mistral.rs would load from a directory holding `names`, as its
/// `get_model_paths` chooses them: the safetensors index, else the safetensors files, else the
/// sharded pickles. `None` when there are none.
fn weights(names: &[&str]) -> Option<Weights> {
    let top_level: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !name.contains('/'))
        .collect();
    if top_level.contains(&SAFETENSOR_INDEX) {
        return Some(Weights::Index);
    }
    let safetensors: Vec<String> = top_level
        .iter()
        .filter(|name| SAFETENSORS.iter().any(|pattern| pattern.is_match(name)))
        .map(ToString::to_string)
        .collect();
    if !safetensors.is_empty() {
        return Some(Weights::Files(safetensors));
    }
    let pickles: Vec<String> = top_level
        .iter()
        .filter(|name| {
            PICKLES
                .as_ref()
                .is_some_and(|pattern| pattern.is_match(name))
                && [".pth", ".pt", ".bin"]
                    .iter()
                    .any(|extension| name.ends_with(extension))
        })
        .map(ToString::to_string)
        .collect();
    (!pickles.is_empty()).then_some(Weights::Files(pickles))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(names: &[&str]) -> Option<Weights> {
        weights(names)
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    /// As mistral.rs does, a repository's `chat_template.jinja` is read before its
    /// `tokenizer_config.json`, and a repository with neither has no template file.
    #[tokio::test]
    async fn the_chat_template_file_is_the_jinja_file_before_tokenizer_config() {
        let dir = tempfile::tempdir().expect("a scratch directory");
        assert_eq!(chat_template_file(dir.path()).await, None);
        let config = dir.path().join("tokenizer_config.json");
        std::fs::write(&config, "{}").expect("tokenizer_config.json writes");
        assert_eq!(chat_template_file(dir.path()).await, Some(config));
        let jinja = dir.path().join("chat_template.jinja");
        std::fs::write(&jinja, "").expect("chat_template.jinja writes");
        assert_eq!(chat_template_file(dir.path()).await, Some(jinja));
    }

    #[test]
    fn weights_are_chosen_as_mistral_rs_chooses_them() {
        // The index wins over everything.
        assert_eq!(
            files(&[
                "model.safetensors.index.json",
                "model-00001-of-00002.safetensors"
            ]),
            Some(Weights::Index)
        );
        // Safetensors, sharded or not, over pickles.
        assert_eq!(
            files(&[
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors",
                "pytorch_model-00001-of-00002.bin",
                "config.json",
            ]),
            Some(Weights::Files(owned(&[
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors"
            ])))
        );
        assert_eq!(
            files(&["model.safetensors", "consolidated.safetensors"]),
            Some(Weights::Files(owned(&[
                "model.safetensors",
                "consolidated.safetensors"
            ])))
        );
        // Only sharded pickles: mistral.rs does not load a lone `pytorch_model.bin`.
        assert_eq!(
            files(&[
                "pytorch_model-00001-of-00002.bin",
                "pytorch_model-00002-of-00002.bin"
            ]),
            Some(Weights::Files(owned(&[
                "pytorch_model-00001-of-00002.bin",
                "pytorch_model-00002-of-00002.bin"
            ])))
        );
        assert_eq!(files(&["pytorch_model.bin", "config.json"]), None);
        // A local directory is read by its top-level names: weights in a folder are not
        // the model's.
        assert_eq!(files(&["onnx/model.safetensors", "config.json"]), None);
    }

    #[test]
    fn configs_are_every_top_level_json_and_jinja_file() {
        assert_eq!(
            configs(&[
                "config.json",
                "tokenizer.json",
                "chat_template.jinja",
                "generation_config.json",
                "README.md",
                "model.safetensors",
                "1_Pooling/config.json",
            ]),
            owned(&[
                "config.json",
                "tokenizer.json",
                "chat_template.jinja",
                "generation_config.json"
            ])
        );
    }
}
