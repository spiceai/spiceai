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

use arrow_schema::SchemaRef;

use super::{find_first_delimiter, validate_identifier};
use crate::{Runtime, component::access::AccessMode, dataaccelerator::AccelerationSource};
use acceleration::{Acceleration, Engine};
use app::App;
use datafusion::sql::{
    TableReference,
    sqlparser::{
        dialect::{Dialect, GenericDialect},
        parser::{Parser, ParserError},
    },
};
use datafusion_table_providers::util::column_reference;
use snafu::prelude::*;
use spicepod::{
    component::{dataset as spicepod_dataset, embeddings::ColumnEmbeddingConfig},
    metric::Metrics,
    semantic::{Column, IndexStore},
    vector::VectorStore,
};
use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc, time::Duration};

pub mod acceleration;
pub mod builder;
pub mod declared_schema;
pub mod declared_type;
pub mod metadata;
pub mod replication;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Column for index '{index}' was not found in the schema. Valid columns: {valid_columns}. Verify configuration and try again. For details, visit https://spiceai.org/docs/features/data-acceleration/indexes"
    ))]
    IndexColumnNotFound {
        index: String,
        valid_columns: String,
    },

    #[snafu(display(
        "Primary key column '{invalid_column}' was not found in the schema. Valid columns: {valid_columns}. Verify configuration and try again. For details, visit https://spiceai.org/docs/features/data-acceleration/constraints"
    ))]
    PrimaryKeyColumnNotFound {
        invalid_column: String,
        valid_columns: String,
    },

    #[snafu(display("Failed to retrieve table constraints for the dataset: {source}"))]
    UnableToGetTableConstraints {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to convert the dataset schema for query planning: {source}"))]
    UnableToConvertSchemaRefToDFSchema {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Only one `on_conflict` target can be specified, or all `on_conflict` targets must be specified and set to `drop`. {extra_detail}"
    ))]
    OnConflictTargetMismatch { extra_detail: String },

    #[snafu(display("Error parsing column reference {column_ref}: {source}"))]
    UnableToParseColumnReference {
        column_ref: String,
        source: column_reference::Error,
    },

    #[snafu(display("Error parsing {field} as duration: {source}"))]
    UnableToParseFieldAsDuration {
        field: String,
        source: fundu::ParseError,
    },

    #[snafu(display("Error parsing 'snapshots_batches` as integer: {source}"))]
    UnableToParseSnapshotsBatches { source: std::num::ParseIntError },

    #[snafu(display("Error parsing `from` path {path} as table reference: {source}"))]
    UnableToParseTableReferenceFromPath { path: String, source: ParserError },

    #[snafu(display(
        "Failed to build dataset '{dataset}': required component '{missing_component}' is missing. An unexpected error occurred. Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToBuildDataset {
        dataset: String,
        missing_component: String,
    },

    #[snafu(display(
        "Both a 'refresh_cron' and 'refresh_check_interval' were specified. Only one of these options can be specified for a given dataset. For details, visit: https://spiceai.org/docs/features/data-acceleration/data-refresh"
    ))]
    MultipleRefreshExpressionSpecified,

    #[snafu(display(
        "Chunking is not supported for vector engines. Disable chunking for the column '{column}', or disable the vector engine, and try again."
    ))]
    ChunkingNotSupportedForVectorEngine { column: String },

    #[snafu(display("Invalid configuration for '{config_key}': {message}"))]
    InvalidConfiguration { config_key: String, message: String },

    #[snafu(display("Invalid column type in dataset '{dataset}': {source}"))]
    InvalidColumnType {
        dataset: String,
        source: declared_schema::DeclaredSchemaError,
    },

    #[snafu(display(
        "'snapshots_batches' is required when setting 'snapshots_trigger: batches'. For details, visit: https://spiceai.org/docs/features/data-acceleration/snapshots"
    ))]
    SnapshotTriggerIntervalRequiresInterval,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum TimeFormat {
    #[default]
    Timestamp,
    Timestamptz,
    UnixSeconds,
    UnixMillis,
    ISO8601,
    Date,
}

impl From<spicepod_dataset::TimeFormat> for TimeFormat {
    fn from(time_format: spicepod_dataset::TimeFormat) -> Self {
        match time_format {
            spicepod_dataset::TimeFormat::UnixSeconds => TimeFormat::UnixSeconds,
            spicepod_dataset::TimeFormat::UnixMillis => TimeFormat::UnixMillis,
            spicepod_dataset::TimeFormat::ISO8601 => TimeFormat::ISO8601,
            spicepod_dataset::TimeFormat::Timestamp => TimeFormat::Timestamp,
            spicepod_dataset::TimeFormat::Timestamptz => TimeFormat::Timestamptz,
            spicepod_dataset::TimeFormat::Date => TimeFormat::Date,
        }
    }
}

impl std::fmt::Display for TimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UnsupportedTypeAction {
    Error,
    Warn,
    Ignore,
    String,
}

impl From<spicepod_dataset::UnsupportedTypeAction> for UnsupportedTypeAction {
    fn from(action: spicepod_dataset::UnsupportedTypeAction) -> Self {
        match action {
            spicepod_dataset::UnsupportedTypeAction::Error => UnsupportedTypeAction::Error,
            spicepod_dataset::UnsupportedTypeAction::Warn => UnsupportedTypeAction::Warn,
            spicepod_dataset::UnsupportedTypeAction::Ignore => UnsupportedTypeAction::Ignore,
            spicepod_dataset::UnsupportedTypeAction::String => UnsupportedTypeAction::String,
        }
    }
}

impl From<UnsupportedTypeAction> for datafusion_table_providers::UnsupportedTypeAction {
    fn from(action: UnsupportedTypeAction) -> Self {
        match action {
            UnsupportedTypeAction::Error => {
                datafusion_table_providers::UnsupportedTypeAction::Error
            }
            UnsupportedTypeAction::Warn => datafusion_table_providers::UnsupportedTypeAction::Warn,
            UnsupportedTypeAction::Ignore => {
                datafusion_table_providers::UnsupportedTypeAction::Ignore
            }
            UnsupportedTypeAction::String => {
                datafusion_table_providers::UnsupportedTypeAction::String
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OnSchemaChange {
    #[default]
    Block,
    Fail,
    AppendNewColumns,
    SyncAllColumns,
}

impl From<spicepod_dataset::OnSchemaChange> for OnSchemaChange {
    fn from(on_schema_change: spicepod_dataset::OnSchemaChange) -> Self {
        match on_schema_change {
            spicepod_dataset::OnSchemaChange::Block => OnSchemaChange::Block,
            spicepod_dataset::OnSchemaChange::Fail => OnSchemaChange::Fail,
            spicepod_dataset::OnSchemaChange::AppendNewColumns => OnSchemaChange::AppendNewColumns,
            spicepod_dataset::OnSchemaChange::SyncAllColumns => OnSchemaChange::SyncAllColumns,
        }
    }
}

impl Display for OnSchemaChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnSchemaChange::Block => write!(f, "block"),
            OnSchemaChange::Fail => write!(f, "fail"),
            OnSchemaChange::AppendNewColumns => write!(f, "append_new_columns"),
            OnSchemaChange::SyncAllColumns => write!(f, "sync_all_columns"),
        }
    }
}

/// Controls when the table is marked ready for queries.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ReadyState {
    /// The table is ready once the initial load completes.
    #[default]
    OnLoad,
    /// The table is ready immediately, with fallback to federated table for queries until the initial load completes.
    OnRegistration,
    /// The table is ready once the federated source's schema has been resolved (which also implies access
    /// to the source has been verified), without waiting for the initial data refresh to complete. Queries
    /// fall back to the federated source until the initial load completes.
    OnSchemaResolved,
}

impl From<spicepod_dataset::ReadyState> for ReadyState {
    fn from(ready_state: spicepod_dataset::ReadyState) -> Self {
        match ready_state {
            spicepod_dataset::ReadyState::OnLoad => ReadyState::OnLoad,
            spicepod_dataset::ReadyState::OnRegistration => ReadyState::OnRegistration,
            spicepod_dataset::ReadyState::OnSchemaResolved => ReadyState::OnSchemaResolved,
        }
    }
}

impl Display for ReadyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadyState::OnLoad => write!(f, "on_load"),
            ReadyState::OnRegistration => write!(f, "on_registration"),
            ReadyState::OnSchemaResolved => write!(f, "on_schema_resolved"),
        }
    }
}

/// Controls whether the federated table periodically has its availability checked.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum CheckAvailability {
    /// The dataset is checked for availability if it isn't accelerated.
    #[default]
    Auto,
    /// The dataset is not checked for availability.
    Disabled,
}

impl From<spicepod_dataset::CheckAvailability> for CheckAvailability {
    fn from(monitor: spicepod_dataset::CheckAvailability) -> Self {
        match monitor {
            spicepod_dataset::CheckAvailability::Auto => CheckAvailability::Auto,
            spicepod_dataset::CheckAvailability::Disabled => CheckAvailability::Disabled,
        }
    }
}

impl Display for CheckAvailability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckAvailability::Auto => write!(f, "auto"),
            CheckAvailability::Disabled => write!(f, "disabled"),
        }
    }
}

#[derive(Clone)]
pub struct Dataset {
    pub from: String,
    pub name: TableReference,
    pub access: AccessMode,
    pub params: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
    /// Arrow schema derived from `columns[].type` declarations. `None` when no
    /// column carries an explicit type. Connectors merge this with their inferred
    /// schema so declared types take precedence.
    pub schema: Option<SchemaRef>,
    pub has_metadata_table: bool,
    pub replication: Option<replication::Replication>,
    pub time_column: Option<String>,
    pub time_format: Option<TimeFormat>,
    pub time_partition_column: Option<String>,
    pub time_partition_format: Option<TimeFormat>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub embeddings: Vec<ColumnEmbeddingConfig>,
    pub app: Arc<App>,
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub on_schema_change: OnSchemaChange,
    pub ready_state: ReadyState,
    pub metrics: Metrics,
    pub runtime: Arc<Runtime>,
    pub vectors: Option<VectorStore>,
    pub full_text_search: Option<spicepod::fts::FtsStore>,
    pub check_availability: CheckAvailability,
}

impl std::fmt::Debug for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dataset")
            .field("from", &self.from)
            .field("name", &self.name)
            .field("access", &self.access)
            .field("params", &self.params)
            .field("metadata", &self.metadata)
            .field("columns", &self.columns)
            .field("schema", &self.schema)
            .field("has_metadata_table", &self.has_metadata_table)
            .field("replication", &self.replication)
            .field("time_column", &self.time_column)
            .field("time_format", &self.time_format)
            .field("time_partition_column", &self.time_partition_column)
            .field("time_partition_format", &self.time_partition_format)
            .field("acceleration", &self.acceleration)
            .field("embeddings", &self.embeddings)
            .field("app", &self.app)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("on_schema_change", &self.on_schema_change)
            .field("ready_state", &self.ready_state)
            .field("metrics", &self.metrics)
            .field("vectors", &self.vectors)
            .field("full_text_search", &self.full_text_search)
            .field("check_availability", &self.check_availability)
            .finish_non_exhaustive()
    }
}

// Implement a custom PartialEq for Dataset to ignore the app field
// This allows the Runtime to compare datasets like-for-like between App reloads,
// because different App instances will cause datasets that are exactly the same to be considered different.
impl PartialEq for Dataset {
    fn eq(&self, other: &Self) -> bool {
        self.from == other.from
            && self.name == other.name
            && self.access == other.access
            && self.params == other.params
            && self.has_metadata_table == other.has_metadata_table
            && self.replication == other.replication
            && self.time_column == other.time_column
            && self.time_format == other.time_format
            && self.time_partition_column == other.time_partition_column
            && self.time_partition_format == other.time_partition_format
            && self.acceleration == other.acceleration
            && self.embeddings == other.embeddings
            && self.columns == other.columns
            && self.metrics == other.metrics
            && self.on_schema_change == other.on_schema_change
            && self.vectors == other.vectors
            && self.full_text_search == other.full_text_search
            && self.check_availability == other.check_availability
    }
}

impl Dataset {
    #[must_use]
    pub fn app(&self) -> Arc<App> {
        Arc::clone(&self.app)
    }

    #[must_use]
    pub fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    #[must_use]
    pub fn with_params(mut self, params: HashMap<String, String>) -> Self {
        self.params = params;
        self
    }

    #[expect(clippy::result_large_err)]
    pub(crate) fn parse_table_reference(
        name: &str,
    ) -> std::result::Result<TableReference, crate::Error> {
        match TableReference::parse_str(name) {
            table_ref @ (TableReference::Bare { .. } | TableReference::Partial { .. }) => {
                Ok(table_ref)
            }
            TableReference::Full { catalog, .. } => crate::DatasetNameIncludesCatalogSnafu {
                catalog,
                name: name.to_string(),
            }
            .fail(),
        }
    }

    /// Returns the dataset source - the first part of the `from` field before the first '://', ':', or '/'
    #[must_use]
    pub fn source(&self) -> &str {
        if self.from == "sink" || self.from.is_empty() {
            return "sink";
        }

        match find_first_delimiter(&self.from) {
            Some((0, _)) => "",
            Some((pos, _)) => &self.from[..pos],
            None => "spice.ai",
        }
    }

    /// Returns the dataset path - the remainder of the `from` field after the first '://', ':', or '/'
    #[must_use]
    pub fn path(&self) -> &str {
        match find_first_delimiter(&self.from) {
            Some((pos, len)) => &self.from[pos + len..],
            None => &self.from,
        }
    }

    /// For [`Dataset`]s where the path in the `from` field is a [`TableReference`], parse and return the [`TableReference`].
    ///
    ///
    pub fn parse_path(
        &self,
        case_sensitive: bool,
        dialect: Option<&dyn Dialect>,
    ) -> Result<TableReference> {
        // Manually parse the table reference to avoid case folding.
        if case_sensitive {
            let path_str = self.path();
            let dialect = dialect.unwrap_or(&GenericDialect {});
            let mut parts = Parser::new(dialect)
                .try_with_sql(path_str)
                .context(UnableToParseTableReferenceFromPathSnafu {
                    path: path_str.to_string(),
                })?
                .parse_multipart_identifier()
                .context(UnableToParseTableReferenceFromPathSnafu {
                    path: path_str.to_string(),
                })?
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .into_iter();

            let tbl = match (parts.next(), parts.next(), parts.next()) {
                (Some(catalog), Some(schema), Some(table)) => TableReference::Full {
                    catalog: catalog.into(),
                    schema: schema.into(),
                    table: table.into(),
                },
                (Some(schema), Some(table), None) => TableReference::Partial {
                    schema: schema.into(),
                    table: table.into(),
                },
                (Some(table), None, None) => TableReference::Bare {
                    table: table.into(),
                },
                _ => TableReference::Bare {
                    table: self.path().into(),
                },
            };
            Ok(tbl)
        } else {
            Ok(self.path().into())
        }
    }

    #[must_use]
    pub fn refresh_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_check_interval;
        }
        None
    }

    #[must_use]
    pub fn refresh_cron(&self) -> Option<Arc<str>> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_cron.clone();
        }
        None
    }

    #[must_use]
    pub fn refresh_max_jitter(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && acceleration.refresh_jitter_enabled
        {
            // If `refresh_jitter_max` is not set, use 10% of `refresh_check_interval`.
            return match acceleration.refresh_jitter_max {
                Some(jitter) => Some(jitter),
                None => self.refresh_check_interval().map(|i| i.mul_f64(0.1)),
            };
        }
        None
    }

    pub fn retention_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && let Some(retention_check_interval) = &acceleration.retention_check_interval
        {
            if let Ok(duration) = fundu::parse_duration(retention_check_interval) {
                return Some(duration);
            }
            tracing::warn!(
                "Unable to parse retention check interval for dataset {}: {}",
                self.name,
                retention_check_interval
            );
        }

        None
    }

    pub fn retention_period(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && let Some(retention_period) = &acceleration.retention_period
        {
            if let Ok(duration) = fundu::parse_duration(retention_period) {
                return Some(duration);
            }
            tracing::warn!(
                "Unable to parse retention period for dataset {}: {}",
                self.name,
                retention_period
            );
        }

        None
    }

    #[must_use]
    pub fn retention_sql(&self) -> Option<String> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.retention_sql.clone();
        }

        None
    }

    #[must_use]
    pub fn refresh_sql(&self) -> Option<String> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_sql.clone();
        }

        None
    }

    #[must_use]
    pub fn refresh_data_window(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && let Some(refresh_data_window) = &acceleration.refresh_data_window
        {
            if let Ok(duration) = fundu::parse_duration(refresh_data_window) {
                return Some(duration);
            }
            tracing::warn!(
                "Unable to parse refresh period for dataset {}: {}",
                self.name,
                refresh_data_window
            );
        }

        None
    }

    #[must_use]
    pub fn refresh_retry_enabled(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_enabled;
        }
        false
    }

    #[must_use]
    pub fn refresh_retry_max_attempts(&self) -> Option<usize> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_max_attempts;
        }
        None
    }

    #[must_use]
    pub fn access(&self) -> AccessMode {
        self.access
    }

    #[must_use]
    pub fn is_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled;
        }

        false
    }

    #[must_use]
    pub fn is_file_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            if acceleration.engine == Engine::PostgreSQL {
                return true;
            }

            return acceleration.enabled
                && matches!(
                    acceleration.mode,
                    acceleration::Mode::File
                        | acceleration::Mode::FileCreate
                        | acceleration::Mode::FileUpdate
                );
        }

        false
    }

    #[must_use]
    pub async fn is_accelerator_initialized(&self) -> bool {
        if let Some(acceleration_settings) = &self.acceleration {
            let Some(accelerator) = self
                .runtime()
                .accelerator_engine_registry()
                .get_accelerator_engine(acceleration_settings.engine)
                .await
            else {
                return false; // if the accelerator engine is not found, it's impossible for it to be initialized
            };

            return accelerator.is_initialized(self);
        }

        false
    }

    /// Get a parameter from the dataset's params, with a default value if the parameter is not set or is not valid.
    ///
    /// Returns `default_value` if the parameter is not set or is not valid.
    ///
    /// If the parameter is set but is not valid, logs a warning and returns `default_value`.
    #[must_use]
    pub fn get_param<T>(&self, param: &str, default_value: T) -> T
    where
        T: Display + FromStr,
    {
        let Some(value) = self.params.get(param) else {
            return default_value;
        };

        if let Ok(parsed_value) = value.parse::<T>() {
            parsed_value
        } else {
            tracing::warn!(
                "Dataset {}: params.{param} is not valid, defaulting to {default_value}",
                self.name
            );
            default_value
        }
    }

    #[must_use]
    pub fn has_embeddings(&self) -> bool {
        !self.embeddings.is_empty() || self.columns.iter().any(|c| !c.embeddings.is_empty())
    }

    #[must_use]
    pub fn has_full_text_column(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.full_text_search.as_ref().is_some_and(|cfg| cfg.enabled))
    }

    /// Returns the dataset-level FTS engine name if configured and enabled.
    /// e.g. `Some("elasticsearch")` when `full_text_search.engine: elasticsearch`.
    #[must_use]
    pub fn fts_engine(&self) -> Option<&str> {
        self.full_text_search
            .as_ref()
            .filter(|fts| fts.enabled)
            .and_then(|fts| fts.engine.as_deref())
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

/// Summarizes all full-text search configuration for a given [`Dataset`] (compared to the column-level [`FullTextSearchConfig`]).
pub struct FullTextSearchDatasetConfig {
    pub index_store: IndexStore,
    pub index_path: Option<String>,
    pub search_fields: Vec<String>,
    pub primary_key: Vec<String>,
}

impl AccelerationSource for Dataset {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(self.clone())
    }

    fn is_file_accelerated(&self) -> bool {
        self.is_file_accelerated()
    }

    fn app(&self) -> Arc<app::App> {
        self.app()
    }

    fn runtime(&self) -> Arc<Runtime> {
        self.runtime()
    }

    fn acceleration(&self) -> Option<&Acceleration> {
        self.acceleration.as_ref()
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn time_column(&self) -> Option<&str> {
        self.time_column.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_table_providers::util::column_reference::ColumnReference;

    use super::acceleration::{Acceleration, IndexType};
    use super::builder::DatasetBuilder;
    use super::*;
    use app::AppBuilder;
    use spicepod::{
        fts::FtsStore,
        param::Params,
        semantic::{ColumnLevelEmbeddingConfig, FullTextSearchConfig},
        vector::VectorStore,
    };

    #[test]
    fn test_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("foo".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::hashmap_to_option_string(&indexes_map);
        assert!(indexes_str == "foo:enabled;bar:unique" || indexes_str == "bar:unique;foo:enabled");
        let roundtrip_indexes_map: HashMap<String, IndexType> =
            datafusion_table_providers::util::hashmap_from_option_string(&indexes_str);

        let roundtrip_indexes_map = roundtrip_indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        let indexes_map = indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        assert_eq!(indexes_map, roundtrip_indexes_map);
    }

    #[test]
    fn test_compound_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("(foo, bar)".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::hashmap_to_option_string(&indexes_map);
        assert!(
            indexes_str == "(foo, bar):enabled;bar:unique"
                || indexes_str == "bar:unique;(foo, bar):enabled"
        );
        let roundtrip_indexes_map: HashMap<String, IndexType> =
            datafusion_table_providers::util::hashmap_from_option_string(&indexes_str);

        let roundtrip_indexes_map = roundtrip_indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        let indexes_map = indexes_map
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<String, String>>();

        assert_eq!(indexes_map, roundtrip_indexes_map);
    }

    #[test]
    fn test_get_index_columns() {
        let column_ref = ColumnReference::try_from("foo").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["foo"]);

        let column_ref = ColumnReference::try_from("(foo, bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let column_ref = ColumnReference::try_from("(foo,bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let err = ColumnReference::try_from("(foo,bar").expect_err("invalid columns");
        assert_eq!(
            err.to_string(),
            "The column reference \"(foo,bar\" is missing a closing parenthensis."
        );
    }

    async fn create_dataset_with_params(params: HashMap<String, String>) -> Dataset {
        let spicepod_dataset =
            spicepod::component::dataset::Dataset::new("test".to_string(), "test".to_string());

        let app = AppBuilder::new("test")
            .with_dataset(spicepod_dataset.clone())
            .build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("valid dataset");

        dataset.params = params;
        dataset
    }

    fn params(entries: &[(&str, &str)]) -> Params {
        Params::from_string_map(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
    }

    async fn build_dataset(
        spicepod_dataset: spicepod::component::dataset::Dataset,
        app: app::App,
    ) -> Result<Dataset> {
        let runtime = crate::Runtime::builder().build().await;

        DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
    }

    #[tokio::test]
    async fn test_dataset_level_search_engine_configuration_is_preserved() {
        let app = AppBuilder::new("test").build();

        let mut spicepod_dataset =
            spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        spicepod_dataset.vectors = Some(VectorStore {
            enabled: true,
            engine: Some("elasticsearch".to_string()),
            partition_by: Vec::new(),
            params: Some(params(&[("endpoint", "http://es:9200"), ("metric", "dot")])),
        });
        spicepod_dataset.full_text_search = Some(FtsStore {
            enabled: true,
            engine: Some("elasticsearch".to_string()),
            params: Some(params(&[
                ("endpoint", "http://es:9200"),
                ("index", "docs_text"),
            ])),
        });

        let dataset = build_dataset(spicepod_dataset, app)
            .await
            .expect("direct search engine configuration should build");

        let vector_store = dataset.vectors.as_ref().expect("vectors should be enabled");
        assert_eq!(vector_store.engine.as_deref(), Some("elasticsearch"));
        let vector_params = vector_store
            .params
            .as_ref()
            .expect("vector params should merge")
            .as_string_map();
        assert_eq!(
            vector_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(vector_params.get("metric").map(String::as_str), Some("dot"));

        let fts_store = dataset
            .full_text_search
            .as_ref()
            .expect("full text search should be enabled");
        assert_eq!(fts_store.engine.as_deref(), Some("elasticsearch"));
        let fts_params = fts_store
            .params
            .as_ref()
            .expect("fts params should merge")
            .as_string_map();
        assert_eq!(
            fts_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(
            fts_params.get("index").map(String::as_str),
            Some("docs_text")
        );
    }

    #[tokio::test]
    async fn test_column_level_search_engine_overrides_enable_stores() {
        let app = AppBuilder::new("test").build();

        let mut column_fts = FullTextSearchConfig::enabled().with_row_id("id");
        column_fts.engine = Some("elasticsearch".to_string());
        column_fts.params = Some(params(&[
            ("endpoint", "http://es:9200"),
            ("index", "body_text"),
        ]));

        let mut spicepod_dataset =
            spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        spicepod_dataset.columns = vec![
            spicepod::semantic::Column::new("body")
                .with_embedding(ColumnLevelEmbeddingConfig {
                    model: "openai_embeddings".to_string(),
                    chunking: None,
                    row_ids: Some(vec!["id".to_string()]),
                    vector_size: None,
                    engine: Some("elasticsearch".to_string()),
                    params: Some(params(&[
                        ("endpoint", "http://es:9200"),
                        ("index", "body_vectors"),
                    ])),
                    aggregation: None,
                    max_elements_per_row: None,
                })
                .with_full_text_search(column_fts),
        ];

        let dataset = build_dataset(spicepod_dataset, app)
            .await
            .expect("column search engine overrides should build");

        let vector_store = dataset.vectors.as_ref().expect("vectors should be enabled");
        assert!(vector_store.enabled);
        assert_eq!(vector_store.engine, None);

        let column_embedding = dataset.columns[0]
            .embeddings
            .first()
            .expect("embedding should remain on column");
        assert_eq!(column_embedding.engine.as_deref(), Some("elasticsearch"));
        let embedding_params = column_embedding
            .params
            .as_ref()
            .expect("embedding params should merge")
            .as_string_map();
        assert_eq!(
            embedding_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(
            embedding_params.get("index").map(String::as_str),
            Some("body_vectors")
        );

        let fts_store = dataset
            .full_text_search
            .as_ref()
            .expect("column text engine should enable dataset fts store");
        assert_eq!(fts_store.engine.as_deref(), Some("elasticsearch"));
        let fts_store_params = fts_store
            .params
            .as_ref()
            .expect("column fts params should be promoted")
            .as_string_map();
        assert_eq!(
            fts_store_params.get("endpoint").map(String::as_str),
            Some("http://es:9200")
        );
        assert_eq!(
            fts_store_params.get("index").map(String::as_str),
            Some("body_text")
        );

        let column_fts = dataset.columns[0]
            .full_text_search
            .as_ref()
            .expect("column fts config should remain");
        assert_eq!(column_fts.engine.as_deref(), Some("elasticsearch"));
    }

    #[tokio::test]
    async fn test_mixed_column_fts_params_error() {
        let app = AppBuilder::new("test").build();

        let mut first_fts = FullTextSearchConfig::enabled().with_row_id("id");
        first_fts.engine = Some("elasticsearch".to_string());
        first_fts.params = Some(params(&[("index", "body_text")]));

        let mut second_fts = FullTextSearchConfig::enabled().with_row_id("id");
        second_fts.engine = Some("elasticsearch".to_string());
        second_fts.params = Some(params(&[("index", "title_text")]));

        let mut spicepod_dataset =
            spicepod::component::dataset::Dataset::new("file:data.csv", "docs");
        spicepod_dataset.columns = vec![
            spicepod::semantic::Column::new("body").with_full_text_search(first_fts),
            spicepod::semantic::Column::new("title").with_full_text_search(second_fts),
        ];

        let err = build_dataset(spicepod_dataset, app)
            .await
            .expect_err("mixed column fts params should fail safely");

        assert!(
            err.to_string().contains("different parameters"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_get_dataset_param() {
        // Test case 1: Parameter is not set
        let dataset = create_dataset_with_params(HashMap::new()).await;
        assert!(dataset.get_param("test_param", true));
        assert!(!dataset.get_param("test_param", false));

        // Test case 2: Parameter is set to "true"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "true".to_string());
        let dataset = create_dataset_with_params(params).await;
        assert!(dataset.get_param("test_param", false));

        // Test case 3: Parameter is set to "false"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "false".to_string());
        let dataset = create_dataset_with_params(params).await;
        assert!(!dataset.get_param("test_param", true));

        // Test case 4: Parameter is set to an invalid boolean value
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "not_a_bool".to_string());
        let dataset = create_dataset_with_params(params).await;
        assert!(dataset.get_param("test_param", true));
        assert!(!dataset.get_param("test_param", false));

        // Test case 5: App is None
        assert!(dataset.get_param("test_param", true));
        assert!(!dataset.get_param("test_param", false));
    }

    #[tokio::test]
    async fn test_source() {
        let test_cases = vec![
            // Basic delimiter cases
            ("foo:bar", "foo"),
            ("foo/bar", "foo"),
            ("foo://bar", "foo"),
            // Empty and sink cases
            ("", "sink"),
            ("sink", "sink"),
            ("sink:", "sink"),
            ("sink/", "sink"),
            ("sink://", "sink"),
            // No delimiter case
            ("foo", "spice.ai"),
            // Multiple delimiters - should use first occurrence
            ("foo:bar:baz", "foo"),
            ("foo/bar/baz", "foo"),
            ("foo://bar://baz", "foo"),
            // Mixed delimiters - should handle "://" first
            ("foo://bar:baz", "foo"),
            ("foo://bar/baz", "foo"),
            ("foo:bar//baz", "foo"),
            ("foo/bar://baz", "foo"),
            // Edge cases with delimiters
            ("://bar", ""),
            (":bar", ""),
            ("/bar", ""),
            ("//bar", ""),
            // Common real-world patterns
            ("mysql://localhost", "mysql"),
            ("http://example.com", "http"),
            ("https://api.example.com", "https"),
            ("postgresql://localhost", "postgresql"),
            ("s3://bucket", "s3"),
            ("file:/path", "file"),
            ("snowflake://account", "snowflake"),
            // Special characters
            ("foo-bar:baz", "foo-bar"),
            ("foo_bar:baz", "foo_bar"),
            ("foo.bar:baz", "foo.bar"),
            // Unicode characters
            ("über:data", "über"),
            ("数据:source", "数据"),
            // Whitespace handling
            ("  foo:bar", "  foo"),
            ("foo  :bar", "foo  "),
            ("\tfoo:bar", "\tfoo"),
        ];

        for (input, expected) in test_cases {
            let app = app::AppBuilder::new("test").build();
            let rt = crate::Runtime::builder().build().await;

            let dataset = DatasetBuilder::try_new(input.to_string(), "test")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");
            assert_eq!(dataset.source(), expected, "Failed for input: {input}");
        }
    }

    #[tokio::test]
    async fn test_path() {
        let test_cases = vec![
            // Basic delimiter cases
            ("foo:bar", "bar"),
            ("foo/bar", "bar"),
            ("foo://bar", "bar"),
            // Empty cases
            ("", ""),
            (":", ""),
            ("/", ""),
            ("://", ""),
            // Multiple delimiters - should use first occurrence
            ("foo:bar:baz", "bar:baz"),
            ("foo/bar/baz", "bar/baz"),
            ("foo://bar://baz", "bar://baz"),
            // Mixed delimiters - should handle "://" first
            ("foo://bar:baz", "bar:baz"),
            ("foo://bar/baz", "bar/baz"),
            ("foo:bar//baz", "bar//baz"),
            ("foo/bar://baz", "bar://baz"),
            // Edge cases with delimiters
            ("://bar", "bar"),
            (":bar", "bar"),
            ("/bar", "bar"),
            ("//bar", "/bar"),
            // Common real-world patterns
            ("mysql://localhost:3306", "localhost:3306"),
            ("http://example.com/path", "example.com/path"),
            ("https://api.example.com/v1", "api.example.com/v1"),
            ("postgresql://localhost:5432/db", "localhost:5432/db"),
            ("s3://bucket/key", "bucket/key"),
            ("file:/path/to/file", "/path/to/file"),
            ("file:///path/to/file", "/path/to/file"),
            ("file://path/to/file", "path/to/file"),
            ("snowflake://account/db/schema", "account/db/schema"),
            // Special characters
            ("foo-bar:baz-qux", "baz-qux"),
            ("foo_bar:baz_qux", "baz_qux"),
            ("foo.bar:baz.qux", "baz.qux"),
            // Unicode characters
            ("source:数据", "数据"),
            ("来源:数据", "数据"),
            // Whitespace handling
            ("foo:  bar", "  bar"),
            ("foo:bar  ", "bar  "),
            ("foo:\tbar", "\tbar"),
            ("foo:\nbar", "\nbar"),
            // Query parameters
            ("mysql://host/db?param=value", "host/db?param=value"),
            ("http://example.com?q=1&r=2", "example.com?q=1&r=2"),
            // Authentication information
            ("mysql://user:pass@host/db", "user:pass@host/db"),
            ("https://token@api.com", "token@api.com"),
        ];

        for (input, expected) in test_cases {
            let app = app::AppBuilder::new("test").build();
            let rt = crate::Runtime::builder().build().await;

            let dataset = DatasetBuilder::try_new(input.to_string(), "test")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");
            assert_eq!(dataset.path(), expected, "Failed for input: {input}");
        }
    }
}
