/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use crate::{access::AccessMode, find_first_delimiter};
use acceleration::Engine;
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
pub mod declared_schema;
pub mod declared_type;
pub mod metadata;
pub mod replication;
pub mod schema_inference;

#[derive(Debug, Snafu)]
// Context selectors are `pub`: the runtime-side `DatasetBuilder` (which stays in
// the `runtime` crate) constructs `InvalidColumnTypeSnafu`/`InvalidConfigurationSnafu`
// across the crate boundary.
#[snafu(visibility(pub))]
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

    #[snafu(display(
        "Cannot configure {constraint} because the dataset schema has no columns. This usually means the source table does not exist or could not be read. Verify the dataset's `from` target exists and is accessible, then try again."
    ))]
    AcceleratedSchemaEmpty { constraint: String },

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

    #[snafu(display("Error parsing `snapshots_batches` as integer: {source}"))]
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

impl From<runtime_acceleration::AccelerationParseError> for Error {
    fn from(e: runtime_acceleration::AccelerationParseError) -> Self {
        use runtime_acceleration::AccelerationParseError;
        match e {
            AccelerationParseError::UnableToParseColumnReference { column_ref, source } => {
                Error::UnableToParseColumnReference { column_ref, source }
            }
            AccelerationParseError::UnableToParseFieldAsDuration { source, field } => {
                Error::UnableToParseFieldAsDuration { source, field }
            }
            AccelerationParseError::MultipleRefreshExpressionSpecified => {
                Error::MultipleRefreshExpressionSpecified
            }
            AccelerationParseError::IndexColumnNotFound {
                index,
                valid_columns,
            } => Error::IndexColumnNotFound {
                index,
                valid_columns,
            },
            AccelerationParseError::PrimaryKeyColumnNotFound {
                invalid_column,
                valid_columns,
            } => Error::PrimaryKeyColumnNotFound {
                invalid_column,
                valid_columns,
            },
            AccelerationParseError::AcceleratedSchemaEmpty { constraint } => {
                Error::AcceleratedSchemaEmpty { constraint }
            }
            AccelerationParseError::UnableToGetTableConstraints { source } => {
                Error::UnableToGetTableConstraints { source }
            }
            AccelerationParseError::OnConflictTargetMismatch { extra_detail } => {
                Error::OnConflictTargetMismatch { extra_detail }
            }
            _ => Error::InvalidConfiguration {
                config_key: "acceleration".into(),
                message: e.to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum TimeFormat {
    #[default]
    Timestamp,
    Timestamptz,
    UnixSeconds,
    UnixMillis,
    UnixNanos,
    ISO8601,
    Date,
}

impl From<spicepod_dataset::TimeFormat> for TimeFormat {
    fn from(time_format: spicepod_dataset::TimeFormat) -> Self {
        match time_format {
            spicepod_dataset::TimeFormat::UnixSeconds => TimeFormat::UnixSeconds,
            spicepod_dataset::TimeFormat::UnixMillis => TimeFormat::UnixMillis,
            spicepod_dataset::TimeFormat::UnixNanos => TimeFormat::UnixNanos,
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

// `on_schema_change` is the accelerator's policy for a source schema change, so it
// lives with the acceleration contract. Re-exported for the
// `runtime_component::dataset::OnSchemaChange` path.
pub use runtime_acceleration::OnSchemaChange;

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

/// Config-only core of a dataset — every declared field of a
/// `runtime::component::dataset::Dataset` except the runtime handles
/// (`app`/`runtime`). The runtime wrapper holds `Self` plus those handles and
/// `Deref`s to it, so callers keep reading `dataset.acceleration`,
/// `dataset.columns`, etc. unchanged.
#[derive(Clone)]
pub struct DatasetSpec {
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
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub on_schema_change: OnSchemaChange,
    pub ready_state: ReadyState,
    pub metrics: Metrics,
    pub vectors: Option<VectorStore>,
    pub full_text_search: Option<spicepod::fts::FtsStore>,
    /// Forwards this dataset's CDC stream to a Drasi source. Requires
    /// `acceleration.refresh_mode: changes`.
    pub drasi: Option<spicepod::drasi::Drasi>,
    pub check_availability: CheckAvailability,
    /// How often the availability monitor probes this (non-accelerated)
    /// dataset's source, parsed from the Spicepod duration string at
    /// construction. Availability monitoring is **opt-in**: `None` means the
    /// dataset is not monitored at all.
    pub check_availability_interval: Option<Duration>,
}

impl std::fmt::Debug for DatasetSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetSpec")
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
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("on_schema_change", &self.on_schema_change)
            .field("ready_state", &self.ready_state)
            .field("metrics", &self.metrics)
            .field("vectors", &self.vectors)
            .field("full_text_search", &self.full_text_search)
            .field("drasi", &self.drasi)
            .field("check_availability", &self.check_availability)
            .field(
                "check_availability_interval",
                &self.check_availability_interval,
            )
            .finish_non_exhaustive()
    }
}

// Two specs are equal when their identity-defining configuration matches, which
// lets the runtime compare datasets like-for-like across App reloads. `schema`
// (derived from `columns`), `metadata`, `unsupported_type_action`, and
// `ready_state` are deliberately excluded from the comparison.
impl PartialEq for DatasetSpec {
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
            && self.check_availability_interval == other.check_availability_interval
            // Compared so a reload that only edits `drasi:` still recreates the
            // dataset: the forwarder is built when the connector is, so an
            // unequal-but-untracked block would leave the old one running.
            && self.drasi == other.drasi
    }
}

impl DatasetSpec {
    /// A spec for the dataset `name` read from `from`, with every other field at
    /// its default. Callers that need more set the fields they care about;
    /// `DatasetBuilder` in the runtime builds the fully-configured spec from a
    /// Spicepod definition.
    #[must_use]
    pub fn new(from: impl Into<String>, name: TableReference) -> Self {
        Self {
            from: from.into(),
            name,
            access: AccessMode::default(),
            params: HashMap::default(),
            metadata: HashMap::default(),
            columns: Vec::default(),
            schema: None,
            has_metadata_table: false,
            replication: None,
            time_column: None,
            time_format: None,
            time_partition_column: None,
            time_partition_format: None,
            acceleration: None,
            embeddings: Vec::default(),
            unsupported_type_action: None,
            on_schema_change: OnSchemaChange::default(),
            ready_state: ReadyState::default(),
            metrics: Metrics::default(),
            vectors: None,
            full_text_search: None,
            drasi: None,
            check_availability: CheckAvailability::default(),
            check_availability_interval: None,
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

    /// For a [`DatasetSpec`] where the path in the `from` field is a [`TableReference`], parse and return the [`TableReference`].
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

    /// Find any primary keys explicitly defined in the [`DatasetSpec`]. Order of precedence:
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

/// Summarizes all full-text search configuration for a given [`DatasetSpec`] (compared to the column-level `FullTextSearchConfig`).
pub struct FullTextSearchDatasetConfig {
    pub index_store: IndexStore,
    pub index_path: Option<String>,
    pub search_fields: Vec<String>,
    pub primary_key: Vec<String>,
}
