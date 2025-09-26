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

use super::{find_first_delimiter, validate_identifier};
use crate::{Runtime, dataaccelerator::AccelerationSource};
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
    semantic::{Column, FullTextSearchConfig, IndexStore},
    vector::VectorStore,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

pub mod acceleration;
pub mod builder;
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

    #[snafu(display("Unable to get table constraints: {source}"))]
    UnableToGetTableConstraints {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to convert a SchemaRef to a DFSchema: {source}"))]
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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Mode {
    #[default]
    Read,
    ReadWrite,
}

impl From<spicepod_dataset::Mode> for Mode {
    fn from(mode: spicepod_dataset::Mode) -> Self {
        match mode {
            spicepod_dataset::Mode::Read => Mode::Read,
            spicepod_dataset::Mode::ReadWrite => Mode::ReadWrite,
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

/// Controls when the table is marked ready for queries.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ReadyState {
    /// The table is ready once the initial load completes.
    #[default]
    OnLoad,
    /// The table is ready immediately, with fallback to federated table for queries until the initial load completes.
    OnRegistration,
}

impl From<spicepod_dataset::ReadyState> for ReadyState {
    fn from(ready_state: spicepod_dataset::ReadyState) -> Self {
        match ready_state {
            spicepod_dataset::ReadyState::OnLoad => ReadyState::OnLoad,
            spicepod_dataset::ReadyState::OnRegistration => ReadyState::OnRegistration,
        }
    }
}

impl Display for ReadyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadyState::OnLoad => write!(f, "on_load"),
            ReadyState::OnRegistration => write!(f, "on_registration"),
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
    pub mode: Mode,
    pub params: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
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
    pub ready_state: ReadyState,
    pub metrics: Metrics,
    pub runtime: Arc<Runtime>,
    pub vectors: Option<VectorStore>,
    pub check_availability: CheckAvailability,
}

impl std::fmt::Debug for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dataset")
            .field("from", &self.from)
            .field("name", &self.name)
            .field("mode", &self.mode)
            .field("params", &self.params)
            .field("metadata", &self.metadata)
            .field("columns", &self.columns)
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
            .field("ready_state", &self.ready_state)
            .field("metrics", &self.metrics)
            .field("vectors", &self.vectors)
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
            && self.mode == other.mode
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
            && self.vectors == other.vectors
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

    #[allow(clippy::result_large_err)]
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
    pub fn mode(&self) -> Mode {
        self.mode
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

            return acceleration.enabled && acceleration.mode == acceleration::Mode::File;
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

    #[allow(clippy::type_complexity)] // From a two-part `.unzip()`.
    #[must_use]
    pub fn full_text_search_config(&self) -> Option<FullTextSearchDatasetConfig> {
        let (search_fields_and_primary_key_overrides, indexes): (
            Vec<(String, Option<Vec<String>>)>,
            Vec<(IndexStore, Option<String>)>,
        ) = self
            .columns
            .iter()
            .filter_map(|c| {
                let Some(FullTextSearchConfig {
                    enabled: true,
                    row_ids,
                    index_store,
                    index_directory,
                }) = &c.full_text_search
                else {
                    return None;
                };

                if index_store.is_some_and(|is| is == IndexStore::Memory) && index_directory.is_some() {
                    tracing::warn!("Dataset '{}' column '{}' has `index_store: memory` but also sets `index_directory`. These options are mutually exclusive. Defaulting to `index_store: memory`.", self.name, c.name);
                }
                Some(((c.name.clone(), row_ids.clone()), (index_store.unwrap_or_default(), index_directory.clone())))
            })
            .unzip();
        let (search_fields, primary_key_overrides): (Vec<String>, Vec<Option<Vec<String>>>) =
            search_fields_and_primary_key_overrides.into_iter().unzip();

        // No columns have full text search fields defined.
        if search_fields.is_empty() {
            return None;
        }

        // For all full text search columns, find the first with a non-null primary key override and
        // if there are multiple, warn if they are different.
        let mut first_pks: Option<Vec<String>> = None;
        let mut first_search_field: Option<String> = None;
        for (search_field, pk_overrides) in search_fields.iter().zip(primary_key_overrides.iter()) {
            let Some(mut pks) = pk_overrides.clone() else {
                continue;
            };
            pks.sort();

            // If this is not the first FTS column that defined row ids, check if they match the previous.
            // Otherwise set to be used for next comparison.
            if let (Some(f), Some(s)) = (&first_pks, &first_search_field) {
                if *pks != *f {
                    tracing::warn!(
                        "Dataset '{}' has different primary keys for different full-text search columns. Using first.\n  Column '{}'. Key: {}.\n  Column '{}'. Key: {}.",
                        self.name(),
                        s,
                        f.join(", "),
                        search_field,
                        pks.join(", "),
                    );
                }
            } else {
                first_pks = Some(pks.clone());
                first_search_field = Some(search_field.clone());
            }
        }

        let index_paths: HashSet<String> = indexes
            .iter()
            .filter_map(|(_, directory)| directory.clone())
            .collect();
        let index_path_len = index_paths.len();
        let index_path: Option<String> = index_paths.into_iter().next();

        if let Some(ref path) = index_path
            && index_path_len > 1
        {
            tracing::warn!(
                "Dataset '{}' has several full text search index directories provided. Using '{path}'.",
                self.name
            );
        }

        let index_store = if indexes.iter().any(|(store, _)| *store == IndexStore::File) {
            IndexStore::File
        } else {
            IndexStore::Memory
        };

        Some(FullTextSearchDatasetConfig {
            index_store,
            index_path,
            search_fields,
            primary_key: first_pks.unwrap_or_default(),
        })
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_table_providers::util::column_reference::ColumnReference;

    use super::acceleration::{Acceleration, IndexType};
    use super::builder::DatasetBuilder;
    use super::*;
    use app::AppBuilder;

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
