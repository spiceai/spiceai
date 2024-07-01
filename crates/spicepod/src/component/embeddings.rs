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

use super::WithDependsOn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Embeddings {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl WithDependsOn<Embeddings> for Embeddings {
    fn depends_on(&self, depends_on: &[String]) -> Embeddings {
        Embeddings {
            from: self.from.clone(),
            name: self.name.clone(),
            params: self.params.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

impl Embeddings {
    #[must_use]
    pub fn get_prefix(&self) -> Option<EmbeddingPrefix> {
        EmbeddingPrefix::try_from(self.from.as_str()).ok()
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
            Some(p) => self
                .from
                .strip_prefix(&format!("{p}/"))
                .map(ToString::to_string),
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
        } else if value.starts_with("file:") {
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
            EmbeddingPrefix::File => write!(f, "file:"),
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum EmbeddingParams {
    OpenAiParams {
        api_base: Option<String>,
        api_key: Option<String>,
        org_id: Option<String>,
        project_id: Option<String>,
    },
    HuggingfaceParams {},

    LocalModelParams {
        weights_path: String,
        config_path: String,
        tokenizer_path: String,
    },
    None,
}

/// Configuration for if and how a dataset's column should be embedded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnEmbeddingConfig {
    pub column: String,

    #[serde(rename = "use", default)]
    pub model: String,

    #[serde(rename = "column_pk", skip_serializing_if = "Option::is_none")]
    pub primary_keys: Option<Vec<String>>,
}
