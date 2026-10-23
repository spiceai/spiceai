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

use std::path::PathBuf;
use std::sync::LazyLock;

use huggingface_hub::{DownloadError, Requester, Snapshot};
use regex::Regex;

use crate::hf::HubParams;

use crate::hf::SAFETENSORS_INDEX as SAFETENSOR_INDEX;
const DOCS_URL: &str = "https://spiceai.org/docs/components/models/huggingface";

/// Downloads the files mistral.rs loads the model `model_id` (`org/name[:revision]`) from, for
/// the model named `name`, and returns the directory they are in.
///
/// # Errors
///
/// Returns an error if the repository or revision cannot be read, lacks a file the engine
/// needs, or a download fails.
pub async fn download_model(
    model_id: &str,
    gguf: Option<&str>,
    hub: HubParams<'_>,
    name: &str,
) -> Result<PathBuf, DownloadError> {
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
    let snapshot = crate::hf::open_model(requester, repo, revision, hub).await?;
    download(&snapshot, gguf).await
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

/// Downloads what mistral.rs loads a model from: the weights named by `gguf`, or the repo's
/// safetensors (or sharded pickle) weights, and the top-level configs. Returns the snapshot
/// directory.
///
/// # Errors
///
/// Returns an error if the repository lacks a file the engine needs, or a download fails.
pub async fn download(snapshot: &Snapshot, gguf: Option<&str>) -> Result<PathBuf, DownloadError> {
    let names: Vec<&str> = snapshot.files().map(|file| file.path.as_str()).collect();
    let mut paths = configs(&names);
    if let Some(gguf) = gguf {
        paths.push(gguf.to_string());
    } else {
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
