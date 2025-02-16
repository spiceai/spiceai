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

use std::collections::HashMap;

use column::Column;
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    embeddings::ColumnEmbeddingConfig, is_default, params::Params, Nameable, WithDependsOn,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TimeFormat {
    #[default]
    Timestamp,
    Timestamptz,
    UnixSeconds,
    UnixMillis,
    #[serde(rename = "ISO8601")]
    ISO8601,
    Date,
}

impl std::fmt::Display for TimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedTypeAction {
    #[default]
    Error,
    Warn,
    Ignore,
    String,
}

/// Controls when the dataset is marked ready for queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReadyState {
    /// The table is ready once the initial load completes.
    #[default]
    OnLoad,
    /// The table is ready immediately on registration, with fallback to federated table for queries until the initial load completes.
    OnRegistration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
#[serde(try_from = "DatasetDeserializer")]
pub struct Dataset {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,

    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<Column>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub mode: Mode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_metadata_table: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<replication::Replication>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_column: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_format: Option<TimeFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_partition_column: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_partition_format: Option<TimeFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<acceleration::Acceleration>,

    #[serde(rename = "embeddings", default, skip_serializing_if = "Vec::is_empty")]
    pub embeddings: Vec<ColumnEmbeddingConfig>,

    #[serde(rename = "dependsOn", default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported_type_action: Option<UnsupportedTypeAction>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub ready_state: ReadyState,
}

impl Nameable for Dataset {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Dataset {
    #[must_use]
    pub fn new(from: impl Into<String>, name: impl Into<String>) -> Self {
        Dataset {
            from: from.into(),
            name: name.into(),
            description: None,
            metadata: HashMap::default(),
            columns: Vec::default(),
            mode: Mode::default(),
            params: None,
            has_metadata_table: None,
            replication: None,
            time_column: None,
            time_format: None,
            time_partition_column: None,
            time_partition_format: None,
            acceleration: None,
            embeddings: Vec::default(),
            depends_on: Vec::default(),
            unsupported_type_action: None,
            ready_state: ReadyState::default(),
        }
    }

    #[must_use]
    pub fn has_embeddings(&self) -> bool {
        !self.embeddings.is_empty() || self.columns.iter().any(|c| !c.embeddings.is_empty())
    }
}

impl WithDependsOn<Dataset> for Dataset {
    fn depends_on(&self, depends_on: &[String]) -> Dataset {
        Dataset {
            from: self.from.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            metadata: self.metadata.clone(),
            columns: self.columns.clone(),
            mode: self.mode.clone(),
            params: self.params.clone(),
            has_metadata_table: self.has_metadata_table,
            replication: self.replication.clone(),
            time_column: self.time_column.clone(),
            time_format: self.time_format.clone(),
            time_partition_column: self.time_partition_column.clone(),
            time_partition_format: self.time_partition_format.clone(),
            acceleration: self.acceleration.clone(),
            embeddings: self.embeddings.clone(),
            depends_on: depends_on.to_vec(),
            unsupported_type_action: self.unsupported_type_action,
            ready_state: self.ready_state,
        }
    }
}

pub mod acceleration {
    #[cfg(feature = "schemars")]
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, fmt::Display};

    use crate::component::params::Params;

    use super::ReadyState;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(rename_all = "lowercase")]
    pub enum RefreshMode {
        Full,
        Append,
        Changes,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(rename_all = "lowercase")]
    pub enum Mode {
        #[default]
        Memory,
        File,
    }

    impl Display for Mode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Mode::Memory => write!(f, "memory"),
                Mode::File => write!(f, "file"),
            }
        }
    }

    /// Behavior when a query on an accelerated table returns zero results.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(rename_all = "snake_case")]
    pub enum ZeroResultsAction {
        /// Return an empty result set. This is the default.
        #[default]
        ReturnEmpty,
        /// Fallback to querying the source table.
        UseSource,
    }

    impl Display for ZeroResultsAction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ZeroResultsAction::ReturnEmpty => write!(f, "return_empty"),
                ZeroResultsAction::UseSource => write!(f, "use_source"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(rename_all = "lowercase")]
    pub enum IndexType {
        #[default]
        Enabled,
        Unique,
    }

    impl Display for IndexType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                IndexType::Enabled => write!(f, "enabled"),
                IndexType::Unique => write!(f, "unique"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(rename_all = "lowercase")]
    pub enum OnConflictBehavior {
        #[default]
        Drop,
        Upsert,
    }

    impl Display for OnConflictBehavior {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                OnConflictBehavior::Drop => write!(f, "drop"),
                OnConflictBehavior::Upsert => write!(f, "upsert"),
            }
        }
    }

    #[allow(clippy::struct_excessive_bools)]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(deny_unknown_fields)]
    pub struct Acceleration {
        #[serde(default = "default_true")]
        pub enabled: bool,

        #[serde(default)]
        pub mode: Mode,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub engine: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_mode: Option<RefreshMode>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_check_interval: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_sql: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_data_window: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_append_overlap: Option<String>,

        #[serde(default = "default_true")]
        pub refresh_retry_enabled: bool,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_retry_max_attempts: Option<usize>,

        #[serde(default)]
        pub refresh_jitter_enabled: bool,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub refresh_jitter_max: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub params: Option<Params>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retention_period: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub retention_check_interval: Option<String>,

        #[serde(default, skip_serializing_if = "is_false")]
        pub retention_check_enabled: bool,

        #[serde(default)]
        pub on_zero_results: ZeroResultsAction,

        #[serde(default)]
        #[deprecated(since = "1.0.0-rc.1", note = "Use `dataset.ready_state` instead.")]
        pub ready_state: Option<ReadyState>,

        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        pub indexes: HashMap<String, IndexType>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub primary_key: Option<String>,

        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        pub on_conflict: HashMap<String, OnConflictBehavior>,
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn is_false(b: &bool) -> bool {
        !b
    }

    const fn default_true() -> bool {
        true
    }

    impl Default for Acceleration {
        #[allow(deprecated)]
        fn default() -> Self {
            Self {
                enabled: true,
                mode: Mode::Memory,
                engine: None,
                refresh_mode: None,
                refresh_check_interval: None,
                refresh_sql: None,
                refresh_data_window: None,
                refresh_append_overlap: None,
                refresh_retry_enabled: true,
                refresh_retry_max_attempts: None,
                refresh_jitter_enabled: false,
                refresh_jitter_max: None,
                params: None,
                retention_period: None,
                retention_check_interval: None,
                retention_check_enabled: false,
                on_zero_results: ZeroResultsAction::ReturnEmpty,
                ready_state: None,
                indexes: HashMap::default(),
                primary_key: None,
                on_conflict: HashMap::default(),
            }
        }
    }
}

pub mod replication {
    #[cfg(feature = "schemars")]
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    pub struct Replication {
        #[serde(default)]
        pub enabled: bool,
    }
}

pub mod column {
    use std::collections::HashMap;

    #[cfg(feature = "schemars")]
    use schemars::JsonSchema;
    use serde::{de::Error, Deserialize, Serialize};

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
                Err(e) => assert_eq!(e.to_string(), "Invalid format for row_id. Expected a string, or array of strings. Found Mapping {\"foo\": String(\"bar\")} at line 2 column 17"),
            };

            match serde_yaml::from_str::<ColumnLevelEmbeddingConfig>(
                r"
                from: foo
                row_id: {foo: bar, extra: value}
            ",
            ) {
                Ok(v) => panic!("Expected an error, but successfully parsed to {v:?}"),
                Err(e) => assert_eq!(e.to_string(), "Invalid format for row_id. Expected a string, or array of strings. Found Mapping {\"foo\": String(\"bar\"), \"extra\": String(\"value\")} at line 2 column 17"),
            };

            match serde_yaml::from_str::<ColumnLevelEmbeddingConfig>(
                r"
                from: foo
                row_id: [foo, bar
            ",
            ) {
                Ok(v) => panic!("Expected an error, but successfully parsed to {v:?}"),
                Err(e) => assert_eq!(e.to_string(), "did not find expected ',' or ']' at line 5 column 1, while parsing a flow sequence at line 3 column 25"),
            };
        }

        #[test]
        fn test_deserialize_row_ids_missing() {
            let yaml = "from: model_name";
            let parsed: ColumnLevelEmbeddingConfig =
                serde_yaml::from_str(yaml).expect("Failed to parse ColumnLevelEmbeddingConfig");
            assert_eq!(parsed.row_ids, None);
        }
    }
}

/// This is deprecated, use `unsupported_type_action` instead.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum InvalidTypeAction {
    Error,
    Warn,
    Ignore,
}

/// Helper struct for deserializing Dataset with custom logic for handling `InvalidTypeAction` migration
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetDeserializer {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    from: String,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    columns: Vec<Column>,
    #[serde(default, skip_serializing_if = "is_default")]
    mode: Mode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    params: Option<Params>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    has_metadata_table: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replication: Option<replication::Replication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    time_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    time_format: Option<TimeFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    time_partition_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    time_partition_format: Option<TimeFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    acceleration: Option<acceleration::Acceleration>,
    #[serde(rename = "embeddings", default, skip_serializing_if = "Vec::is_empty")]
    embeddings: Vec<ColumnEmbeddingConfig>,
    #[serde(rename = "dependsOn", default, skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deprecated(since = "1.0.3", note = "Use `unsupported_type_action` instead.")]
    invalid_type_action: Option<InvalidTypeAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unsupported_type_action: Option<UnsupportedTypeAction>,
    #[serde(default, skip_serializing_if = "is_default")]
    ready_state: ReadyState,
}

#[allow(deprecated)]
impl TryFrom<DatasetDeserializer> for Dataset {
    type Error = String;

    fn try_from(deserializer: DatasetDeserializer) -> Result<Self, Self::Error> {
        // If unsupported_type_action is set, use it directly
        // If invalid_type_action is set but unsupported_type_action isn't, convert invalid_type_action
        let unsupported_type_action = match (
            deserializer.unsupported_type_action,
            deserializer.invalid_type_action,
        ) {
            (Some(unsupported), _) => Some(unsupported), // Prefer unsupported_type_action if present
            (None, Some(invalid)) => {
                // Convert from InvalidTypeAction to UnsupportedTypeAction
                tracing::warn!(
                    "{}: `dataset.invalid_type_action` is deprecated, use `dataset.unsupported_type_action` instead",
                    deserializer.name
                );
                Some(match invalid {
                    InvalidTypeAction::Error => UnsupportedTypeAction::Error,
                    InvalidTypeAction::Warn => UnsupportedTypeAction::Warn,
                    InvalidTypeAction::Ignore => UnsupportedTypeAction::Ignore,
                })
            }
            (None, None) => None,
        };

        Ok(Dataset {
            from: deserializer.from,
            name: deserializer.name,
            description: deserializer.description,
            metadata: deserializer.metadata,
            columns: deserializer.columns,
            mode: deserializer.mode,
            params: deserializer.params,
            has_metadata_table: deserializer.has_metadata_table,
            replication: deserializer.replication,
            time_column: deserializer.time_column,
            time_format: deserializer.time_format,
            time_partition_column: deserializer.time_partition_column,
            time_partition_format: deserializer.time_partition_format,
            acceleration: deserializer.acceleration,
            embeddings: deserializer.embeddings,
            depends_on: deserializer.depends_on,
            unsupported_type_action,
            ready_state: deserializer.ready_state,
        })
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use serde_yaml;

    #[test]
    fn test_invalid_type_action_migration() {
        // Test when only invalid_type_action is present
        let yaml = r"
            name: test
            from: test
            invalid_type_action: warn
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(
            dataset.unsupported_type_action,
            Some(UnsupportedTypeAction::Warn)
        );

        // Test when only unsupported_type_action is present
        let yaml = r"
            name: test
            from: test
            unsupported_type_action: warn
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(
            dataset.unsupported_type_action,
            Some(UnsupportedTypeAction::Warn)
        );

        // Test when both are present - unsupported_type_action should take precedence
        let yaml = r"
            name: test
            from: test
            invalid_type_action: error
            unsupported_type_action: warn
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(
            dataset.unsupported_type_action,
            Some(UnsupportedTypeAction::Warn)
        );

        // Test when neither is present
        let yaml = r"
            name: test
            from: test
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.unsupported_type_action, None);
    }
}
