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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Nameable, WithDependsOn, embeddings::ColumnEmbeddingConfig, is_default};
use crate::acceleration::Acceleration;
use crate::metric::Metrics;
use crate::param::Params;
use crate::semantic::Column;
use crate::vector::VectorStore;

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

/// Controls whether the federated table periodically has its availability checked.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CheckAvailability {
    /// The dataset is checked for availability if it isn't accelerated.
    #[default]
    Auto,
    /// The dataset is not checked for availability.
    Disabled,
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
    pub acceleration: Option<Acceleration>,

    #[serde(rename = "embeddings", default, skip_serializing_if = "Vec::is_empty")]
    pub embeddings: Vec<ColumnEmbeddingConfig>,

    #[serde(rename = "dependsOn", default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsupported_type_action: Option<UnsupportedTypeAction>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub ready_state: ReadyState,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vectors: Option<VectorStore>,

    /// Configures whether the dataset availability monitor is enabled for this dataset.
    /// When enabled, the runtime will periodically check dataset availability
    /// and report metrics. Dataset availability is only checked if the dataset is not accelerated.
    #[serde(default, skip_serializing_if = "is_default")]
    pub check_availability: CheckAvailability,
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
            metrics: None,
            vectors: None,
            check_availability: CheckAvailability::default(),
        }
    }

    #[must_use]
    pub fn has_embeddings(&self) -> bool {
        !self.embeddings.is_empty() || self.columns.iter().any(|c| !c.embeddings.is_empty())
    }

    /// Find any primary keys explicitly defined in the [`Dataset`]. Order of precedence:
    ///  1. Primary key defined in `.columns[].embeddings[].row_id`
    ///  2. Primary key defined in `.columns[].full_text_search[].row_id`
    ///  3. Primary key defined in `.embeddings[].column_pk` (on the path to deprecation)
    pub fn primary_key_override(&self) -> Option<Vec<String>> {
        let pks_from_embeddings: Option<Vec<String>> =
            self.embeddings.iter().find_map(|e| e.primary_keys.clone());

        let mut pks_from_columns: Option<Vec<String>> = self
            .columns
            .iter()
            .find_map(|c| c.embeddings.iter().find_map(|e| e.row_ids.clone()));

        let pks_from_fts: Option<Vec<String>> = self
            .columns
            .iter()
            .find_map(|c| c.full_text_search.as_ref().and_then(|f| f.row_ids.clone()));

        pks_from_columns = pks_from_columns.or(pks_from_fts);

        let primary_keys = match (pks_from_columns, pks_from_embeddings) {
            (Some(pks), None) | (None, Some(pks)) => pks,
            (Some(pks), Some(_)) => {
                tracing::warn!(
                    "Dataset '{}' provided primary keys in both `.columns[].embeddings[].row_id` and `.embeddings[].primary_keys`. Using the former.",
                    self.name
                );
                pks
            }
            (None, None) => return None,
        };

        Some(primary_keys)
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
            metrics: self.metrics.clone(),
            vectors: self.vectors.clone(),
            check_availability: self.check_availability,
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
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
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
    acceleration: Option<Acceleration>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metrics: Option<Metrics>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    vectors: Option<VectorStore>,
    #[serde(default, skip_serializing_if = "is_default")]
    check_availability: CheckAvailability,
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
            metrics: deserializer.metrics,
            vectors: deserializer.vectors,
            check_availability: deserializer.check_availability,
        })
    }
}

#[cfg(test)]
mod check_availability_tests {
    use super::*;
    use serde_yaml;

    #[test]
    fn test_check_availability_enabled_by_default() {
        let yaml = r"
            name: test
            from: file://test.csv
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.check_availability, CheckAvailability::Auto);
    }

    #[test]
    fn test_check_availability_disabled_via_config() {
        let yaml = r"
            name: test
            from: file://test.csv
            check_availability: disabled
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.check_availability, CheckAvailability::Disabled);
    }

    #[test]
    fn test_check_availability_enabled_via_config() {
        let yaml = r"
            name: test
            from: file://test.csv
            check_availability: auto
        ";
        let dataset: Dataset = serde_yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.check_availability, CheckAvailability::Auto);
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
