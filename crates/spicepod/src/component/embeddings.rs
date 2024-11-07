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

use std::{collections::HashMap, fmt::Display};

use super::{
    model::{ModelFile, ModelFileType},
    Nameable, WithDependsOn,
};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Embeddings {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub files: Vec<ModelFile>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,

    #[serde(rename = "datasets", default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
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
        }
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
        self.files
            .iter()
            .find(|f| f.file_type() == Some(file_type))
            .map(|f| f.path.clone())
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
    /// - `file://absolute/path/to/my/model.onnx`
    ///     - Prefix: `file:`
    ///     - Model Id: `/absolute/path/to/my/model.onnx`
    #[must_use]
    pub fn get_model_id(&self) -> Option<String> {
        match self.get_prefix() {
            Some(EmbeddingPrefix::HuggingFace) => {
                let from = &self.from;
                from.strip_prefix("huggingface:huggingface.co/")
                    .map(std::string::ToString::to_string)
            }
            Some(EmbeddingPrefix::OpenAi) => {
                let from = &self.from;
                from.strip_prefix("openai:")
                    .map(std::string::ToString::to_string)
            }
            Some(EmbeddingPrefix::File) => {
                let from = &self.from;
                from.strip_prefix("file:")
                    .map(std::string::ToString::to_string)
            }
            None => None,
        }
    }
}

pub enum EmbeddingPrefix {
    OpenAi,
    HuggingFace,
    File,
}

impl TryFrom<&str> for EmbeddingPrefix {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.starts_with("huggingface:huggingface.co") {
            Ok(EmbeddingPrefix::HuggingFace)
        } else if value.starts_with("file") {
            Ok(EmbeddingPrefix::File)
        } else if value.starts_with("openai") {
            Ok(EmbeddingPrefix::OpenAi)
        } else {
            Err("Unknown prefix")
        }
    }
}

impl Display for EmbeddingPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbeddingPrefix::OpenAi => write!(f, "openai"),
            EmbeddingPrefix::HuggingFace => write!(f, "huggingface:huggingface.co"),
            EmbeddingPrefix::File => write!(f, "file"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
}
