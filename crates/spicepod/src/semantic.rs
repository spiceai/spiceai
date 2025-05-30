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

use std::collections::HashMap;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::Error};

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
}

impl Column {
    /// Return the column-level metadata that should be added to a [`arrow::datatypes::Field`].
    #[must_use]
    pub fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        if let Some(d) = self.description.as_ref() {
            metadata.insert("description".to_string(), d.to_string());
        }
        metadata
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
