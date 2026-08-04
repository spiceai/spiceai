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

use std::{collections::HashMap, fmt::Display};

use crate::metric::Metrics;

use super::{
    Nameable, WithDependsOn,
    model::{HUGGINGFACE_PATH_REGEX, ModelFile, ModelFileType},
};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Embeddings {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub files: Vec<ModelFile>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, Value>,

    #[serde(rename = "datasets", default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,
}

impl Nameable for Embeddings {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<Embeddings> for Embeddings {
    fn depends_on(&self, depends_on: &[String]) -> Embeddings {
        Embeddings {
            depends_on: depends_on.to_vec(),
            ..self.clone()
        }
    }
}

impl Embeddings {
    #[must_use]
    pub fn new(from: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            name: name.into(),
            files: Vec::default(),
            params: HashMap::default(),
            datasets: Vec::default(),
            depends_on: Vec::default(),
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_params(self, params: HashMap<String, Value>) -> Self {
        Self { params, ..self }
    }

    #[must_use]
    pub fn get_prefix(&self) -> Option<EmbeddingPrefix> {
        EmbeddingPrefix::try_from(self.from.as_str()).ok()
    }

    #[must_use]
    pub fn get_all_file_paths(&self) -> Vec<String> {
        self.files.iter().map(|f| f.path.clone()).collect()
    }

    /// Finds at most one model file with the given [`ModelFileType`].
    #[must_use]
    pub fn find_any_file_path(&self, file_type: ModelFileType) -> Option<String> {
        self.find_any_file(file_type)
            .map(|model_file| model_file.path)
    }

    /// Finds at most one model file with the given [`ModelFileType`].
    #[must_use]
    pub fn find_any_file(&self, file_type: ModelFileType) -> Option<ModelFile> {
        self.files
            .iter()
            .find(|f| f.file_type() == Some(file_type))
            .cloned()
    }

    /// Get the model id from the `from` field. The model id is the part of the `from` field after the prefix.
    ///
    /// # Example
    /// - `spice.ai/taxi_tech_co/taxi_drives/models/drive_stats:latest`
    ///     - Prefix: `spice.ai`
    ///     - Model Id: `taxi_tech_co/taxi_drives/models/drive_stats:latest`
    /// - `huggingface:huggingface.co/transformers/gpt-2:latest`
    ///    - Prefix: `huggingface:huggingface.co`
    ///    - Model Id: `transformers/gpt-2:latest`
    ///
    /// - `file://absolute/path/to/my/model.gguf`
    ///     - Prefix: `file:`
    ///     - Model Id: `/absolute/path/to/my/model.gguf`
    #[must_use]
    pub fn get_model_id(&self) -> Option<String> {
        match self.get_prefix() {
            Some(EmbeddingPrefix::HuggingFace) => {
                HUGGINGFACE_PATH_REGEX.captures(&self.from).map(|caps| {
                    let model = format!("{}/{}", &caps["org"], &caps["model"]);
                    if let Some(revision) = caps.name("revision") {
                        format!("{}:{}", model, revision.as_str())
                    } else {
                        model
                    }
                })
            }
            Some(EmbeddingPrefix::OpenAi) => {
                let from = &self.from;
                from.strip_prefix("openai:").map(ToString::to_string)
            }
            Some(EmbeddingPrefix::Azure) => {
                let from = &self.from;
                from.strip_prefix("azure:").map(ToString::to_string)
            }
            Some(EmbeddingPrefix::Google) => {
                let from = &self.from;
                from.strip_prefix("google:").map(ToString::to_string)
            }
            Some(EmbeddingPrefix::File) => {
                let from = &self.from;
                from.strip_prefix("file:").map(ToString::to_string)
            }
            Some(EmbeddingPrefix::Databricks) => {
                let from = &self.from;
                from.strip_prefix("databricks:").map(ToString::to_string)
            }
            Some(EmbeddingPrefix::Bedrock) => {
                let from = &self.from;
                from.strip_prefix("bedrock:").map(ToString::to_string)
            }
            Some(EmbeddingPrefix::Model2Vec) => {
                let from = &self.from;
                from.strip_prefix("model2vec:").map(ToString::to_string)
            }
            None => None,
        }
    }
}

/// The revision a model id returned by [`Embeddings::get_model_id`] pins, if any.
///
/// Only an `org/model:rev` shape pins one — this defers to
/// [`HUGGINGFACE_PATH_REGEX`], the same definition the `huggingface:` arm of
/// `get_model_id` uses, so the two cannot disagree about what a revision is.
/// Anything the regex does not match has none, which is what keeps a local
/// filesystem path (including a Windows `C:/models/…`, whose colon is not a
/// revision separator) out of this.
///
/// Callers whose loader cannot pass a revision downstream use this to reject the
/// configuration with that as the stated reason, rather than sending the whole
/// `org/model:rev` string to the Hub as a repository name and surfacing the 401
/// that comes back (#12445).
#[must_use]
pub fn pinned_revision(model_id: &str) -> Option<&str> {
    let caps = HUGGINGFACE_PATH_REGEX.captures(model_id)?;
    caps.name("revision").map(|m| m.as_str())
}

pub enum EmbeddingPrefix {
    OpenAi,
    Azure,
    Google,
    HuggingFace,
    File,
    Databricks,
    Bedrock,
    Model2Vec,
}

impl TryFrom<&str> for EmbeddingPrefix {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.starts_with("huggingface") || value.starts_with("hf") {
            Ok(EmbeddingPrefix::HuggingFace)
        } else if value.starts_with("file") {
            Ok(EmbeddingPrefix::File)
        } else if value.starts_with("openai") {
            Ok(EmbeddingPrefix::OpenAi)
        } else if value.starts_with("azure") {
            Ok(EmbeddingPrefix::Azure)
        } else if value.starts_with("google") {
            Ok(EmbeddingPrefix::Google)
        } else if value.starts_with("databricks") {
            Ok(EmbeddingPrefix::Databricks)
        } else if value.starts_with("bedrock") {
            Ok(EmbeddingPrefix::Bedrock)
        } else if value.starts_with("model2vec") {
            Ok(EmbeddingPrefix::Model2Vec)
        } else {
            Err("Unknown prefix")
        }
    }
}

impl Display for EmbeddingPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbeddingPrefix::OpenAi => write!(f, "openai"),
            EmbeddingPrefix::Azure => write!(f, "azure"),
            EmbeddingPrefix::Google => write!(f, "google"),
            EmbeddingPrefix::HuggingFace => write!(f, "huggingface"),
            EmbeddingPrefix::File => write!(f, "file"),
            EmbeddingPrefix::Databricks => write!(f, "databricks"),
            EmbeddingPrefix::Bedrock => write!(f, "bedrock"),
            EmbeddingPrefix::Model2Vec => write!(f, "model2vec"),
        }
    }
}

/// Aggregation strategy applied when a multi-vector (list-typed) column
/// is queried. Each list element produces its own embedding; at query
/// time the per-element similarities are combined into a single per-row
/// score using this aggregation.
///
/// `Max` is the ColBERT-style `MaxSim` default — a row scores as high as
/// its best-matching element.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum EmbeddingAggregation {
    #[default]
    Max,
    Mean,
    Sum,
}

impl Display for EmbeddingAggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Max => write!(f, "max"),
            Self::Mean => write!(f, "mean"),
            Self::Sum => write!(f, "sum"),
        }
    }
}

/// Hard cap on multi-vector list elements embedded per row. Beyond this
/// limit, excess elements are dropped with a warning. See
/// `ColumnEmbeddingConfig::max_elements_per_row`.
pub const MULTI_VECTOR_MAX_ELEMENTS_HARD_CAP: usize = 1024;

/// Default cap if none specified on a multi-vector column.
pub const MULTI_VECTOR_MAX_ELEMENTS_DEFAULT: usize = 32;

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct EmbeddingChunkConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub target_chunk_size: usize,

    #[serde(default)]
    pub overlap_size: usize,

    #[serde(default)]
    pub trim_whitespace: bool,
}

impl EmbeddingChunkConfig {
    #[must_use]
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn target_chunk_size(mut self, size: usize) -> Self {
        self.target_chunk_size = size;
        self
    }
    #[must_use]
    pub fn trim_whitespace(mut self, trim_whitespace: bool) -> Self {
        self.trim_whitespace = trim_whitespace;
        self
    }
}

/// Configuration for if and how a dataset's column should be embedded.
///
/// Prefer to use [`super::dataset::column::ColumnLevelEmbeddingConfig`] going
/// forward. Support for [`ColumnEmbeddingConfig`] will be removed in future.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct ColumnEmbeddingConfig {
    pub column: String,

    #[serde(rename = "use", default)]
    pub model: String,

    #[serde(rename = "column_pk", skip_serializing_if = "Option::is_none")]
    pub primary_keys: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunking: Option<EmbeddingChunkConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_size: Option<usize>,

    /// Aggregation strategy for multi-vector embeddings. Only meaningful
    /// when the underlying column is list-typed (`List<Utf8>` /
    /// `LargeList<Utf8>`). Defaults to `max` (ColBERT-style `MaxSim`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregation: Option<EmbeddingAggregation>,

    /// Maximum number of list elements embedded per row for multi-vector
    /// columns. Defaults to `32`; hard-capped at `1024`. Excess elements
    /// are dropped with a warning log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_elements_per_row: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::model::split_hf_model_id;

    fn embedding(from: &str) -> Embeddings {
        Embeddings::new(from, "test")
    }

    #[test]
    fn pinned_revision_reads_the_revision_off_a_hub_id() {
        for (id, want) in [
            ("organization/model-name:v1", "v1"),
            ("organization/model-name:latest", "latest"),
            ("organization/model-name:my-branch", "my-branch"),
            ("organization/model-name:v1.2-beta.3", "v1.2-beta.3"),
            ("organization/my-model.v2:v1", "v1"),
            // A commit SHA is just another revision as far as the id is concerned.
            ("minishlab/potion-base-8M:9f1a2b3c4d5e", "9f1a2b3c4d5e"),
        ] {
            assert_eq!(
                pinned_revision(id),
                Some(want),
                "expected {id} to pin revision {want}"
            );
        }
    }

    #[test]
    fn pinned_revision_is_none_when_nothing_is_pinned() {
        for id in [
            "minishlab/potion-base-8M",
            "organization/my-model.v2",
            // Not a hub id at all: a bare name, and the local filesystem paths
            // `StaticModel::from_pretrained` also accepts. The colon in a Windows
            // path is a drive separator, not a revision separator — reading it as
            // one would reject a working local model.
            "potion-base-8M",
            "/absolute/path/to/model",
            "./relative/path/to/model",
            "C:/models/potion-base-8M",
            "C:/models",
            r"C:\models\potion-base-8M",
            "",
        ] {
            assert_eq!(pinned_revision(id), None, "{id} pins nothing");
        }
    }

    /// The shape of #12445: `get_model_id` hands the loader an id with the
    /// revision still glued on, because the `model2vec:` arm is a bare
    /// `strip_prefix`. The loader has no revision parameter to pass it to, so it
    /// has to detect this and say so rather than send the whole string to the Hub
    /// as a repository name and surface the 401 that comes back.
    #[test]
    fn a_pinned_model2vec_id_keeps_its_revision_through_get_model_id() {
        let id = embedding("model2vec:minishlab/potion-base-8M:v1").get_model_id();
        assert_eq!(id.as_deref(), Some("minishlab/potion-base-8M:v1"));
        assert_eq!(id.as_deref().and_then(pinned_revision), Some("v1"));
    }

    #[test]
    fn an_unpinned_model2vec_id_pins_nothing() {
        let id = embedding("model2vec:minishlab/potion-base-8M").get_model_id();
        assert_eq!(id.as_deref(), Some("minishlab/potion-base-8M"));
        assert_eq!(id.as_deref().and_then(pinned_revision), None);
    }

    /// The `huggingface:` arm splits and rejoins the id, so a revision survives
    /// there too — it just has somewhere to go downstream (#12430). Asserted here
    /// so both arms are visibly held to one notion of a revision.
    #[test]
    fn the_huggingface_arm_agrees_about_what_a_revision_is() {
        let from = "huggingface:huggingface.co/sentence-transformers/all-MiniLM-L6-v2:v1";
        let id = embedding(from).get_model_id();
        assert_eq!(
            id.as_deref(),
            Some("sentence-transformers/all-MiniLM-L6-v2:v1")
        );
        assert_eq!(id.as_deref().and_then(pinned_revision), Some("v1"));
    }

    /// A revision-pinned `HuggingFace` embedding must survive the round trip through
    /// `get_model_id`. `get_model_id` re-joins the revision onto the repo id, so a loader has
    /// to split it back out; the embeddings loader did not, and asked the Hub for a repo
    /// literally named `org/model:revision`, which 401s (#12430).
    #[test]
    fn revision_pinned_embedding_model_id_round_trips() {
        let sha = "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a";

        // The exact spicepod reported in #12430.
        assert_splits_to(
            &format!("huggingface:huggingface.co/BAAI/bge-base-en-v1.5:{sha}"),
            "BAAI/bge-base-en-v1.5",
            Some(sha),
        );

        // A branch name pins just as well as a sha.
        assert_splits_to(
            "hf:BAAI/bge-small-en-v1.5:v2-branch",
            "BAAI/bge-small-en-v1.5",
            Some("v2-branch"),
        );

        // Unpinned: every existing fixture takes this branch, which is why the defect
        // stayed invisible.
        assert_splits_to(
            "hf:sentence-transformers/all-MiniLM-L6-v2",
            "sentence-transformers/all-MiniLM-L6-v2",
            None,
        );
    }

    /// Asserts that the id `get_model_id` builds for `from` splits back into the repo and
    /// revision a loader has to pass to the Hub separately.
    fn assert_splits_to(from: &str, expected_repo: &str, expected_revision: Option<&str>) {
        let Some(model_id) = Embeddings::new(from, "test").get_model_id() else {
            panic!("expected a model id for {from}");
        };

        assert_eq!(
            split_hf_model_id(&model_id),
            (expected_repo, expected_revision),
            "round trip lost the revision for {from}"
        );
    }

    /// Guards the reason the bug was silent: the joined id is not itself a usable repo id, so
    /// forwarding it unsplit cannot work. Without the split, the repo segment of the request
    /// URL carries the revision and the revision segment falls back to `main`.
    #[test]
    fn joined_model_id_is_not_a_usable_repo_id() {
        let from = "hf:BAAI/bge-base-en-v1.5:a5beb1e3";
        let Some(model_id) = Embeddings::new(from, "test").get_model_id() else {
            panic!("expected a model id for {from}");
        };

        assert!(
            model_id.contains(':'),
            "expected the joined id to carry the revision separator: {model_id}"
        );

        let (repo_id, revision) = split_hf_model_id(&model_id);
        assert!(
            !repo_id.contains(':'),
            "repo id must not carry the revision: {repo_id}"
        );
        assert_eq!(revision, Some("a5beb1e3"));
    }
}
