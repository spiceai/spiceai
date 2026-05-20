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
use crate::component::access::AccessMode;
use crate::fts::FtsStore;
use crate::metadata::metadata_value_to_string;
use crate::metric::Metrics;
use crate::param::Params;
use crate::semantic::Column;
use crate::vector::VectorStore;

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
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

impl<'de> serde::Deserialize<'de> for TimeFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "timestamp" => Ok(TimeFormat::Timestamp),
            "timestamptz" => Ok(TimeFormat::Timestamptz),
            "unix_seconds" => Ok(TimeFormat::UnixSeconds),
            "unix_millis" => Ok(TimeFormat::UnixMillis),
            "iso8601" => Ok(TimeFormat::ISO8601),
            "date" => Ok(TimeFormat::Date),
            _ => Err(serde::de::Error::unknown_variant(
                &s,
                &[
                    "timestamp",
                    "timestamptz",
                    "unix_seconds",
                    "unix_millis",
                    "ISO8601",
                    "date",
                ],
            )),
        }
    }
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

/// Policy for handling source schema changes after the dataset is registered.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    /// Block schema changes from being applied automatically. The dataset stays healthy and
    /// continues serving queries using the registered schema.
    #[default]
    Block,
    /// Fail when the projected source schema diverges from the registered dataset schema.
    Fail,
    /// Add new source columns to the registered schema; reject removals and incompatible changes.
    AppendNewColumns,
    /// Keep the registered dataset schema synchronized with the projected source schema.
    SyncAllColumns,
}

impl std::fmt::Display for OnSchemaChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnSchemaChange::Block => write!(f, "block"),
            OnSchemaChange::Fail => write!(f, "fail"),
            OnSchemaChange::AppendNewColumns => write!(f, "append_new_columns"),
            OnSchemaChange::SyncAllColumns => write!(f, "sync_all_columns"),
        }
    }
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
    /// The table is ready once the federated source's schema has been resolved (which also implies access
    /// to the source has been verified), without waiting for the initial data refresh to complete. Queries
    /// fall back to the federated source until the initial load completes. Subsequent refresh failures are
    /// still reported via dataset status and metrics.
    OnSchemaResolved,
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

    #[serde(default, skip_serializing_if = "is_default", alias = "mode")]
    pub access: AccessMode,

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

    /// Controls how the runtime handles source schema changes after the dataset is registered.
    ///
    /// Options: `block` / `fail` / `append_new_columns` / `sync_all_columns`.
    ///
    /// `block` (default) keeps the dataset healthy and queryable using the registered schema.
    #[serde(default, skip_serializing_if = "is_default")]
    pub on_schema_change: OnSchemaChange,

    #[serde(default, skip_serializing_if = "is_default")]
    pub ready_state: ReadyState,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vectors: Option<VectorStore>,

    /// Dataset-level full-text search store configuration.
    /// When present, overrides the per-column `index_store` setting and routes
    /// FTS through the specified external engine (e.g. `elasticsearch`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_text_search: Option<FtsStore>,

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
            access: AccessMode::default(),
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
            on_schema_change: OnSchemaChange::default(),
            ready_state: ReadyState::default(),
            metrics: None,
            vectors: None,
            full_text_search: None,
            check_availability: CheckAvailability::default(),
        }
    }

    #[must_use]
    pub fn with_params(self, params: Params) -> Self {
        Self {
            params: Some(params),
            ..self
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

    #[must_use]
    pub fn metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        if let Some(d) = self.description.as_ref() {
            metadata.insert("comment".to_string(), d.clone());
        }
        for (k, v) in &self.metadata {
            metadata.insert(k.clone(), metadata_value_to_string(v));
        }
        metadata
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
            access: self.access.clone(),
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
            on_schema_change: self.on_schema_change,
            ready_state: self.ready_state,
            metrics: self.metrics.clone(),
            vectors: self.vectors.clone(),
            full_text_search: self.full_text_search.clone(),
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
    #[serde(default, skip_serializing_if = "is_default", alias = "mode")]
    access: AccessMode,
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
    on_schema_change: OnSchemaChange,
    #[serde(default, skip_serializing_if = "is_default")]
    ready_state: ReadyState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metrics: Option<Metrics>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    vectors: Option<VectorStore>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    full_text_search: Option<FtsStore>,
    #[serde(default, skip_serializing_if = "is_default")]
    check_availability: CheckAvailability,
}

#[expect(deprecated)]
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
            access: deserializer.access,
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
            on_schema_change: deserializer.on_schema_change,
            ready_state: deserializer.ready_state,
            metrics: deserializer.metrics,
            vectors: deserializer.vectors,
            full_text_search: deserializer.full_text_search,
            check_availability: deserializer.check_availability,
        })
    }
}

#[cfg(test)]
mod check_availability_tests {
    use super::*;
    use yaml;

    #[test]
    fn test_check_availability_enabled_by_default() {
        let yaml = r"
            name: test
            from: file://test.csv
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.check_availability, CheckAvailability::Auto);
    }

    #[test]
    fn test_check_availability_disabled_via_config() {
        let yaml = r"
            name: test
            from: file://test.csv
            check_availability: disabled
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.check_availability, CheckAvailability::Disabled);
    }

    #[test]
    fn test_check_availability_enabled_via_config() {
        let yaml = r"
            name: test
            from: file://test.csv
            check_availability: auto
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.check_availability, CheckAvailability::Auto);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yaml;

    #[test]
    fn test_invalid_type_action_migration() {
        // Test when only invalid_type_action is present
        let yaml = r"
            name: test
            from: test
            invalid_type_action: warn
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
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
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
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
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(
            dataset.unsupported_type_action,
            Some(UnsupportedTypeAction::Warn)
        );

        // Test when neither is present
        let yaml = r"
            name: test
            from: test
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.unsupported_type_action, None);
    }

    #[test]
    fn test_time_format_case_insensitive_iso8601() {
        // Uppercase ISO8601 (original format)
        let yaml = r"
            name: test
            from: test
            time_format: ISO8601
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::ISO8601));

        // Lowercase iso8601
        let yaml = r"
            name: test
            from: test
            time_format: iso8601
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::ISO8601));

        // Mixed case Iso8601
        let yaml = r"
            name: test
            from: test
            time_format: Iso8601
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::ISO8601));
    }

    #[test]
    fn test_deserialize_default_on_schema_change() {
        let yaml = r"
            name: test
            from: test
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.on_schema_change, OnSchemaChange::Block);
    }

    #[test]
    fn test_deserialize_all_on_schema_change_modes() {
        for (yaml_value, expected) in [
            ("block", OnSchemaChange::Block),
            ("fail", OnSchemaChange::Fail),
            ("append_new_columns", OnSchemaChange::AppendNewColumns),
            ("sync_all_columns", OnSchemaChange::SyncAllColumns),
        ] {
            let yaml = format!(
                r"
                    name: test
                    from: test
                    on_schema_change: {yaml_value}
                "
            );
            let dataset: Dataset = yaml::from_str(&yaml)
                .unwrap_or_else(|_| panic!("should parse on_schema_change '{yaml_value}'"));
            assert_eq!(
                dataset.on_schema_change, expected,
                "unexpected parse for '{yaml_value}'"
            );
        }
    }

    #[test]
    fn test_time_format_case_insensitive_other_variants() {
        // Uppercase TIMESTAMP
        let yaml = r"
            name: test
            from: test
            time_format: TIMESTAMP
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::Timestamp));

        // Mixed case Unix_Seconds
        let yaml = r"
            name: test
            from: test
            time_format: Unix_Seconds
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::UnixSeconds));

        // Uppercase UNIX_MILLIS
        let yaml = r"
            name: test
            from: test
            time_format: UNIX_MILLIS
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::UnixMillis));

        // Mixed case Timestamptz
        let yaml = r"
            name: test
            from: test
            time_format: Timestamptz
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::Timestamptz));

        // Uppercase DATE
        let yaml = r"
            name: test
            from: test
            time_format: DATE
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_format, Some(TimeFormat::Date));
    }

    #[test]
    fn test_time_format_invalid_value() {
        let yaml = r"
            name: test
            from: test
            time_format: invalid_format
        ";
        let result: Result<Dataset, _> = yaml::from_str(yaml);
        result.expect_err("invalid time_format should fail to parse");
    }

    #[test]
    fn test_time_partition_format_case_insensitive() {
        // Verify time_partition_format also benefits from case-insensitive parsing
        let yaml = r"
            name: test
            from: test
            time_partition_format: iso8601
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert_eq!(dataset.time_partition_format, Some(TimeFormat::ISO8601));
    }
}
