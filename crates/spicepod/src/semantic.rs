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
//! Data types for semantic information about tables (datasets or views).

use std::{collections::HashMap, fmt::Display};

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::Error};
use serde_json::Value;

use crate::component::embeddings::EmbeddingChunkConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Column {
    pub name: String,

    /// Optional semantic details about the column
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub embeddings: Vec<ColumnLevelEmbeddingConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_text_search: Option<FullTextSearchConfig>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

impl Column {
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: None,
            embeddings: Vec::new(),
            full_text_search: None,
            metadata: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: HashMap<String, Value>) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub fn with_embeddings(mut self, embeddings: Vec<ColumnLevelEmbeddingConfig>) -> Self {
        self.embeddings = embeddings;
        self
    }

    #[must_use]
    pub fn with_embedding(mut self, embedding: ColumnLevelEmbeddingConfig) -> Self {
        self.embeddings.push(embedding);
        self
    }

    #[must_use]
    pub fn with_full_text_search(mut self, full_text_search: FullTextSearchConfig) -> Self {
        self.full_text_search = Some(full_text_search);
        self
    }

    /// Return the column-level metadata that should be added to a [`arrow::datatypes::Field`].
    #[must_use]
    pub fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        if let Some(d) = self.description.as_ref() {
            metadata.insert("description".to_string(), d.to_string());
        }
        for (k, v) in &self.metadata {
            metadata.insert(k.to_string(), v.to_string());
        }
        metadata
    }

    #[must_use]
    pub fn as_vector_metadata(&self) -> Option<MetadataType> {
        let value = self.metadata.get("vectors")?.clone();
        // If it doesn't deserialize to `MetadataType`, not an issue, just not a `MetadataType`.
        serde_json::from_value(value).ok()
    }
}

impl From<&str> for Column {
    fn from(value: &str) -> Self {
        Column::new(value)
    }
}

/// Configuration for if and how a dataset's column should be embedded.
/// Different to [`crate::component::embeddings::ColumnEmbeddingConfig`],
/// as [`ColumnLevelEmbeddingConfig`] should be a property of [`Column`],
/// not [`super::Dataset`].
///
/// [`crate::component::embeddings::ColumnEmbeddingConfig`] will be
/// deprecated long term in favour of [`ColumnLevelEmbeddingConfig`].
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct ColumnLevelEmbeddingConfig {
    #[serde(rename = "from", default)]
    pub model: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunking: Option<EmbeddingChunkConfig>,

    #[serde(
        rename = "row_id",
        default,
        deserialize_with = "deserialize_row_ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub row_ids: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_size: Option<usize>,
}

impl ColumnLevelEmbeddingConfig {
    #[must_use]
    pub fn model(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            chunking: None,
            row_ids: None,
            vector_size: None,
        }
    }

    #[must_use]
    pub fn chunking(mut self, chunking: EmbeddingChunkConfig) -> Self {
        self.chunking = Some(chunking);
        self
    }

    #[must_use]
    pub fn with_row_id(self, row_id: &str) -> Self {
        if let Some(mut row_ids) = self.row_ids {
            row_ids.push(row_id.to_string());
            Self {
                row_ids: Some(row_ids),
                ..self
            }
        } else {
            Self {
                row_ids: Some(vec![row_id.to_string()]),
                ..self
            }
        }
    }
}

// Let `row_id` handle single string or arrays. All acceptable
// ```yaml
// row_id: foo
//
// row_id: foo, bar
//
// row_id:
//  - foo
//  - bar
// ```
fn deserialize_row_ids<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match serde_yaml::Value::deserialize(deserializer)? {
            serde_yaml::Value::Null => Ok(None),
            serde_yaml::Value::String(s) => {
                Ok(Some(s.split(',').map(|s| s.trim().to_string()).collect()))
            }
            serde_yaml::Value::Sequence(seq) => {
                seq.iter()
                    .map(|v| {
                        v.as_str().map(ToString::to_string).ok_or_else(|| {
                            D::Error::custom(format!("Invalid format for row_id. Expected a string, or array of strings. Found {v:?}"))
                        })
                    })
                    .collect::<Result<Vec<String>, D::Error>>()
                    .map(Some)
            }
            other => Err(D::Error::custom(format!("Invalid format for row_id. Expected a string, or array of strings. Found {other:?}"))),
        }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct FullTextSearchConfig {
    pub enabled: bool,

    #[serde(
        rename = "row_id",
        default,
        deserialize_with = "deserialize_row_ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub row_ids: Option<Vec<String>>,

    pub index_store: Option<IndexStore>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_directory: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum IndexStore {
    #[default]
    Memory,
    File,
}

impl Display for IndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory => write!(f, "memory"),
            Self::File => write!(f, "file"),
        }
    }
}

impl FullTextSearchConfig {
    #[must_use]
    pub fn disabled() -> Self {
        FullTextSearchConfig {
            enabled: false,
            row_ids: None,
            index_store: Some(IndexStore::default()),
            index_directory: None,
        }
    }

    #[must_use]
    pub fn enabled() -> Self {
        FullTextSearchConfig {
            enabled: true,
            row_ids: None,
            index_store: Some(IndexStore::default()),
            index_directory: None,
        }
    }

    #[must_use]
    pub fn with_row_id(self, row_id: &str) -> Self {
        if let Some(mut row_ids) = self.row_ids {
            row_ids.push(row_id.to_string());
            FullTextSearchConfig {
                row_ids: Some(row_ids),
                ..self
            }
        } else {
            FullTextSearchConfig {
                row_ids: Some(vec![row_id.to_string()]),
                ..self
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum MetadataType {
    #[serde(rename = "non-filterable")]
    NonFilterable,
    #[serde(rename = "filterable")]
    Filterable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml;

    #[test]
    fn test_deserialize_row_ids_single_string() {
        let yaml = r"
                from: foo
                row_id: foo
            ";
        let parsed: ColumnLevelEmbeddingConfig =
            serde_yaml::from_str(yaml).expect("Failed to parse ColumnLevelEmbeddingConfig");
        assert_eq!(parsed.row_ids, Some(vec!["foo".to_string()]));
    }

    #[test]
    fn test_deserialize_row_ids_comma_separated() {
        let yaml = r"
                from: foo
                row_id: foo, bar
            ";
        let parsed: ColumnLevelEmbeddingConfig =
            serde_yaml::from_str(yaml).expect("Failed to parse ColumnLevelEmbeddingConfig");
        assert_eq!(
            parsed.row_ids,
            Some(vec!["foo".to_string(), "bar".to_string()])
        );
    }

    #[test]
    fn test_deserialize_row_ids_list() {
        let yaml = r"
                from: foo
                row_id:
                 - foo
                 - bar
            ";
        let parsed: ColumnLevelEmbeddingConfig =
            serde_yaml::from_str(yaml).expect("Failed to parse ColumnLevelEmbeddingConfig");
        assert_eq!(
            parsed.row_ids,
            Some(vec!["foo".to_string(), "bar".to_string()])
        );
    }

    #[test]
    fn test_deserialize_row_ids_errors() {
        match serde_yaml::from_str::<ColumnLevelEmbeddingConfig>(
            r"
                from: foo
                row_id:
                  - foo: bar
            ",
        ) {
            Ok(v) => panic!("Expected an error, but successfully parsed to {v:?}"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Invalid format for row_id. Expected a string, or array of strings. Found Mapping {\"foo\": String(\"bar\")} at line 2 column 17"
            ),
        }

        match serde_yaml::from_str::<ColumnLevelEmbeddingConfig>(
            r"
                from: foo
                row_id: {foo: bar, extra: value}
            ",
        ) {
            Ok(v) => panic!("Expected an error, but successfully parsed to {v:?}"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Invalid format for row_id. Expected a string, or array of strings. Found Mapping {\"foo\": String(\"bar\"), \"extra\": String(\"value\")} at line 2 column 17"
            ),
        }

        match serde_yaml::from_str::<ColumnLevelEmbeddingConfig>(
            r"
                from: foo
                row_id: [foo, bar
            ",
        ) {
            Ok(v) => panic!("Expected an error, but successfully parsed to {v:?}"),
            Err(e) => assert_eq!(
                e.to_string(),
                "did not find expected ',' or ']' at line 5 column 1, while parsing a flow sequence at line 3 column 25"
            ),
        }
    }

    #[test]
    fn test_deserialize_row_ids_missing() {
        let yaml = "from: model_name";
        let parsed: ColumnLevelEmbeddingConfig =
            serde_yaml::from_str(yaml).expect("Failed to parse ColumnLevelEmbeddingConfig");
        assert_eq!(parsed.row_ids, None);
    }
}
