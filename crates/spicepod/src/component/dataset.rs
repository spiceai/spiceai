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
use crate::metric::Metrics;
use crate::param::Params;
use crate::param::connectors::{
    ClickhouseParams, DuckDbParams, MysqlParams, PostgresParams,
};
use crate::semantic::Column;
use crate::vector::VectorStore;

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

/// Typed, per-connector dataset parameters.
///
/// Known connectors get typed param structs with `SecretParam<T>` fields.
/// Unknown connectors fall back to `Generic(Params)`.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum DatasetParams {
    Postgres(PostgresParams),
    DuckDb(DuckDbParams),
    Clickhouse(ClickhouseParams),
    Mysql(MysqlParams),
    Generic(Params),
}

impl DatasetParams {
    /// Converts typed params into a flat `HashMap<String, String>` for backward
    /// compatibility with the existing runtime parameter pipeline.
    #[must_use]
    pub fn as_string_map(&self) -> HashMap<String, String> {
        match self {
            DatasetParams::Postgres(p) => postgres_to_map(p),
            DatasetParams::DuckDb(p) => duckdb_to_map(p),
            DatasetParams::Clickhouse(p) => clickhouse_to_map(p),
            DatasetParams::Mysql(p) => mysql_to_map(p),
            DatasetParams::Generic(p) => p.as_string_map(),
        }
    }

    /// Constructs a `Generic` variant from a flat string map.
    #[must_use]
    pub fn from_string_map(map: HashMap<String, String>) -> Self {
        DatasetParams::Generic(Params::from_string_map(map))
    }

    /// Looks up a key in the params, returning its string value.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<String> {
        self.as_string_map().remove(key)
    }
}

fn insert_opt<T: std::fmt::Display>(
    map: &mut HashMap<String, String>,
    key: &str,
    val: &Option<crate::param::SecretParam<T>>,
) {
    if let Some(v) = val {
        map.insert(key.to_string(), v.as_string());
    }
}

fn postgres_to_map(p: &PostgresParams) -> HashMap<String, String> {
    let mut m = HashMap::new();
    insert_opt(&mut m, "pg_connection_string", &p.connection_string);
    insert_opt(&mut m, "pg_user", &p.user);
    insert_opt(&mut m, "pg_pass", &p.pass);
    insert_opt(&mut m, "pg_host", &p.host);
    insert_opt(&mut m, "pg_port", &p.port);
    insert_opt(&mut m, "pg_db", &p.db);
    insert_opt(&mut m, "pg_sslmode", &p.sslmode);
    insert_opt(&mut m, "pg_sslrootcert", &p.sslrootcert);
    insert_opt(&mut m, "connection_pool_min_idle", &p.connection_pool_min_idle);
    insert_opt(&mut m, "connection_pool_size", &p.connection_pool_size);
    m
}

fn duckdb_to_map(p: &DuckDbParams) -> HashMap<String, String> {
    let mut m = HashMap::new();
    insert_opt(&mut m, "duckdb_open", &p.open);
    m
}

fn clickhouse_to_map(p: &ClickhouseParams) -> HashMap<String, String> {
    let mut m = HashMap::new();
    insert_opt(&mut m, "clickhouse_connection_string", &p.connection_string);
    insert_opt(&mut m, "clickhouse_pass", &p.pass);
    insert_opt(&mut m, "clickhouse_user", &p.user);
    insert_opt(&mut m, "clickhouse_host", &p.host);
    insert_opt(&mut m, "clickhouse_tcp_port", &p.tcp_port);
    insert_opt(&mut m, "clickhouse_db", &p.db);
    insert_opt(&mut m, "clickhouse_secure", &p.secure);
    insert_opt(&mut m, "clickhouse_connection_timeout", &p.connection_timeout);
    m
}

fn mysql_to_map(p: &MysqlParams) -> HashMap<String, String> {
    let mut m = HashMap::new();
    insert_opt(&mut m, "mysql_connection_string", &p.connection_string);
    insert_opt(&mut m, "mysql_user", &p.user);
    insert_opt(&mut m, "mysql_pass", &p.pass);
    insert_opt(&mut m, "mysql_host", &p.host);
    insert_opt(&mut m, "mysql_tcp_port", &p.tcp_port);
    insert_opt(&mut m, "mysql_db", &p.db);
    insert_opt(&mut m, "mysql_sslmode", &p.sslmode);
    insert_opt(&mut m, "mysql_sslrootcert", &p.sslrootcert);
    insert_opt(&mut m, "mysql_pool_min", &p.pool_min);
    insert_opt(&mut m, "mysql_pool_max", &p.pool_max);
    insert_opt(&mut m, "mysql_time_zone", &p.time_zone);
    m
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
    pub params: Option<DatasetParams>,

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
            ready_state: ReadyState::default(),
            metrics: None,
            vectors: None,
            check_availability: CheckAvailability::default(),
        }
    }

    #[must_use]
    pub fn with_params(self, params: DatasetParams) -> Self {
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
            metadata.insert("description".to_string(), d.clone());
        }
        for (k, v) in &self.metadata {
            metadata.insert(k.clone(), v.to_string());
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

/// Extract the connector prefix from a `from` field value.
///
/// Examples:
/// - `"postgres:my_table"` → `Some("postgres")`
/// - `"mysql://host/db"` → `Some("mysql")`
/// - `"duckdb:path/to/file"` → `Some("duckdb")`
/// - `"file://test.csv"` → `Some("file")`
fn extract_connector_prefix(from: &str) -> Option<&str> {
    // Check for `://` first, then `:` (but not windows drive letters like `C:`)
    if let Some(idx) = from.find("://") {
        return Some(&from[..idx]);
    }
    if let Some(idx) = from.find(':') {
        let prefix = &from[..idx];
        // Must look like a connector name, not a windows drive letter
        if prefix.len() > 1 && prefix.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Some(prefix);
        }
    }
    None
}

/// Returns the key prefix to strip for a given connector prefix.
fn connector_key_prefix(connector: &str) -> Option<&'static str> {
    match connector {
        "postgres" => Some("pg_"),
        "mysql" => Some("mysql_"),
        "clickhouse" => Some("clickhouse_"),
        "duckdb" => Some("duckdb_"),
        _ => None,
    }
}

/// Strips connector-specific prefixes from keys in a YAML map value.
/// For example, given prefix `"pg_"`, the key `"pg_host"` becomes `"host"`.
fn strip_key_prefixes(
    raw: serde_value::Value,
    prefix: &str,
) -> serde_value::Value {
    if let serde_value::Value::Map(map) = raw {
        let new_map = map
            .into_iter()
            .map(|(k, v)| {
                let new_key = if let serde_value::Value::String(ref s) = k {
                    if let Some(stripped) = s.strip_prefix(prefix) {
                        serde_value::Value::String(stripped.to_owned())
                    } else {
                        k
                    }
                } else {
                    k
                };
                (new_key, v)
            })
            .collect();
        serde_value::Value::Map(new_map)
    } else {
        raw
    }
}

/// Helper struct for deserializing Dataset with custom logic for handling
/// `InvalidTypeAction` migration and typed connector params.
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
    params: Option<serde_value::Value>,
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

/// Deserialize a raw `serde_value::Value` into a typed connector params struct,
/// dispatching on the connector prefix extracted from the `from` field.
fn deserialize_typed_params(
    from: &str,
    raw: serde_value::Value,
) -> Result<DatasetParams, String> {
    let connector = extract_connector_prefix(from);

    match connector {
        Some("postgres") => {
            let stripped = strip_key_prefixes(raw, connector_key_prefix("postgres").unwrap_or(""));
            PostgresParams::deserialize(stripped)
                .map(DatasetParams::Postgres)
                .map_err(|e| format!("invalid postgres params: {e}"))
        }
        Some("duckdb") => {
            let stripped = strip_key_prefixes(raw, connector_key_prefix("duckdb").unwrap_or(""));
            DuckDbParams::deserialize(stripped)
                .map(DatasetParams::DuckDb)
                .map_err(|e| format!("invalid duckdb params: {e}"))
        }
        Some("clickhouse") => {
            let stripped =
                strip_key_prefixes(raw, connector_key_prefix("clickhouse").unwrap_or(""));
            ClickhouseParams::deserialize(stripped)
                .map(DatasetParams::Clickhouse)
                .map_err(|e| format!("invalid clickhouse params: {e}"))
        }
        Some("mysql") => {
            let stripped = strip_key_prefixes(raw, connector_key_prefix("mysql").unwrap_or(""));
            MysqlParams::deserialize(stripped)
                .map(DatasetParams::Mysql)
                .map_err(|e| format!("invalid mysql params: {e}"))
        }
        _ => {
            // Fallback to generic Params
            Params::deserialize(raw)
                .map(DatasetParams::Generic)
                .map_err(|e| format!("invalid params: {e}"))
        }
    }
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

        let params = match deserializer.params {
            Some(raw) => Some(deserialize_typed_params(&deserializer.from, raw)?),
            None => None,
        };

        Ok(Dataset {
            from: deserializer.from,
            name: deserializer.name,
            description: deserializer.description,
            metadata: deserializer.metadata,
            columns: deserializer.columns,
            access: deserializer.access,
            params,
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
    fn test_postgres_typed_params() {
        let yaml = r#"
            name: test
            from: postgres:my_table
            params:
                pg_host: localhost
                pg_port: 5432
                pg_db: mydb
        "#;
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        match &dataset.params {
            Some(DatasetParams::Postgres(p)) => {
                assert_eq!(
                    p.host,
                    Some(crate::param::SecretParam::Plain("localhost".to_string()))
                );
                assert_eq!(p.port, Some(crate::param::SecretParam::Plain(5432)));
                assert_eq!(
                    p.db,
                    Some(crate::param::SecretParam::Plain("mydb".to_string()))
                );
            }
            other => panic!("Expected Postgres params, got {other:?}"),
        }
    }

    #[test]
    fn test_postgres_typed_params_with_secret_ref() {
        let yaml = r#"
            name: test
            from: postgres:my_table
            params:
                pg_host: ${env:PG_HOST}
                pg_port: 5432
        "#;
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        match &dataset.params {
            Some(DatasetParams::Postgres(p)) => {
                assert_eq!(
                    p.host,
                    Some(crate::param::SecretParam::Unresolved(
                        "${env:PG_HOST}".to_string()
                    ))
                );
                assert_eq!(p.port, Some(crate::param::SecretParam::Plain(5432)));
            }
            other => panic!("Expected Postgres params, got {other:?}"),
        }
    }

    #[test]
    fn test_generic_params_for_unknown_connector() {
        let yaml = r#"
            name: test
            from: file://test.csv
            params:
                key1: value1
                key2: 42
        "#;
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        match &dataset.params {
            Some(DatasetParams::Generic(p)) => {
                let map = p.as_string_map();
                assert_eq!(map.get("key1"), Some(&"value1".to_string()));
                assert_eq!(map.get("key2"), Some(&"42".to_string()));
            }
            other => panic!("Expected Generic params, got {other:?}"),
        }
    }

    #[test]
    fn test_dataset_params_as_string_map() {
        let yaml = r#"
            name: test
            from: postgres:my_table
            params:
                pg_host: localhost
                pg_port: 5432
        "#;
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        let map = dataset.params.as_ref().map(DatasetParams::as_string_map).unwrap();
        assert_eq!(map.get("pg_host"), Some(&"localhost".to_string()));
        assert_eq!(map.get("pg_port"), Some(&"5432".to_string()));
    }

    #[test]
    fn test_duckdb_typed_params() {
        let yaml = r#"
            name: test
            from: duckdb:my_table
            params:
                duckdb_open: /path/to/file.db
        "#;
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        match &dataset.params {
            Some(DatasetParams::DuckDb(p)) => {
                assert_eq!(
                    p.open,
                    Some(crate::param::SecretParam::Plain(
                        "/path/to/file.db".to_string()
                    ))
                );
            }
            other => panic!("Expected DuckDb params, got {other:?}"),
        }
    }

    #[test]
    fn test_no_params() {
        let yaml = r"
            name: test
            from: test
        ";
        let dataset: Dataset = yaml::from_str(yaml).expect("Failed to parse Dataset");
        assert!(dataset.params.is_none());
    }

    #[test]
    fn test_extract_connector_prefix() {
        assert_eq!(extract_connector_prefix("postgres:my_table"), Some("postgres"));
        assert_eq!(extract_connector_prefix("mysql://host/db"), Some("mysql"));
        assert_eq!(extract_connector_prefix("duckdb:path/to/file"), Some("duckdb"));
        assert_eq!(extract_connector_prefix("file://test.csv"), Some("file"));
        assert_eq!(extract_connector_prefix("clickhouse:table"), Some("clickhouse"));
        assert_eq!(extract_connector_prefix("just_a_name"), None);
    }

    #[test]
    fn test_from_string_map_bridge() {
        let map = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);
        let params = DatasetParams::from_string_map(map.clone());
        let result = params.as_string_map();
        assert_eq!(result, map);
    }
}
