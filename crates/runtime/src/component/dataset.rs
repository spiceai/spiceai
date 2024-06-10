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

use datafusion::sql::TableReference;
use snafu::prelude::*;
use spicepod::component::{
    dataset as spicepod_dataset, embeddings::ColumnEmbeddingConfig, params::Params,
};
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Column for index {index} not found in schema. Valid columns: {valid_columns}"
    ))]
    IndexColumnNotFound {
        index: String,
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
    UnixSeconds,
    UnixMillis,
    ISO8601,
}

impl From<spicepod_dataset::TimeFormat> for TimeFormat {
    fn from(time_format: spicepod_dataset::TimeFormat) -> Self {
        match time_format {
            spicepod_dataset::TimeFormat::UnixSeconds => TimeFormat::UnixSeconds,
            spicepod_dataset::TimeFormat::UnixMillis => TimeFormat::UnixMillis,
            spicepod_dataset::TimeFormat::ISO8601 => TimeFormat::ISO8601,
        }
    }
}

impl std::fmt::Display for TimeFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub from: String,
    pub name: TableReference,
    pub mode: Mode,
    pub params: HashMap<String, String>,
    pub has_metadata_table: bool,
    pub replication: Option<replication::Replication>,
    pub time_column: Option<String>,
    pub time_format: Option<TimeFormat>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub embeddings: Vec<ColumnEmbeddingConfig>,
}

impl TryFrom<spicepod_dataset::Dataset> for Dataset {
    type Error = crate::Error;

    fn try_from(dataset: spicepod_dataset::Dataset) -> std::result::Result<Self, Self::Error> {
        let acceleration = dataset
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        let table_reference = Dataset::parse_table_reference(&dataset.name)?;

        Ok(Dataset {
            from: dataset.from,
            name: table_reference,
            mode: Mode::from(dataset.mode),
            params: dataset
                .params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            has_metadata_table: dataset
                .has_metadata_table
                .unwrap_or(Dataset::have_metadata_table_by_default()),
            replication: dataset.replication.map(replication::Replication::from),
            time_column: dataset.time_column,
            time_format: dataset.time_format.map(TimeFormat::from),
            embeddings: dataset.embeddings,
            acceleration,
        })
    }
}

impl Dataset {
    pub fn try_new(from: String, name: &str) -> std::result::Result<Self, crate::Error> {
        Ok(Dataset {
            from,
            name: Self::parse_table_reference(name)?,
            mode: Mode::default(),
            params: HashMap::default(),
            has_metadata_table: Self::have_metadata_table_by_default(),
            replication: None,
            time_column: None,
            time_format: None,
            acceleration: None,
            embeddings: Vec::default(),
        })
    }

    #[must_use]
    /// Returns whether the dataset should enable metadata by default.
    fn have_metadata_table_by_default() -> bool {
        false
    }

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

    /// Returns the dataset source - the first part of the `from` field before the first `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use runtime::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo:bar".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.source(), "foo".to_string());
    /// ```
    ///
    /// ```
    /// use runtime::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.source(), "spiceai");
    /// ```
    #[must_use]
    pub fn source(&self) -> String {
        let parts: Vec<&str> = self.from.splitn(2, ':').collect();
        if parts.len() > 1 {
            parts[0].to_string()
        } else {
            if self.from == "localhost" || self.from.is_empty() {
                return "localhost".to_string();
            }
            "spiceai".to_string()
        }
    }

    /// Returns the dataset path - the remainder of the `from` field after the first `:` or the whole string if no `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo:bar".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.path(), "bar".to_string());
    /// ```
    ///
    /// ```
    /// use crate::component::dataset::Dataset;
    ///
    /// let dataset = Dataset::new("foo".to_string(), "bar".to_string());
    ///
    /// assert_eq!(dataset.path(), "foo".to_string());
    /// ```
    #[must_use]
    pub fn path(&self) -> String {
        match self.from.find(':') {
            Some(index) => self.from[index + 1..].to_string(),
            None => self.from.clone(),
        }
    }

    #[must_use]
    pub fn engine_secret(&self) -> Option<String> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.engine_secret.clone();
        }

        None
    }

    #[must_use]
    pub fn refresh_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(refresh_check_interval) = &acceleration.refresh_check_interval {
                if let Ok(duration) = fundu::parse_duration(refresh_check_interval) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse refresh interval for dataset {}: {}",
                    self.name,
                    refresh_check_interval
                );
            }
        }

        None
    }

    pub fn retention_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(retention_check_interval) = &acceleration.retention_check_interval {
                if let Ok(duration) = fundu::parse_duration(retention_check_interval) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse retention check interval for dataset {}: {}",
                    self.name,
                    retention_check_interval
                );
            }
        }

        None
    }

    pub fn retention_period(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            if let Some(retention_period) = &acceleration.retention_period {
                if let Ok(duration) = fundu::parse_duration(retention_period) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse retention period for dataset {}: {}",
                    self.name,
                    retention_period
                );
            }
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
        if let Some(acceleration) = &self.acceleration {
            if let Some(refresh_data_window) = &acceleration.refresh_data_window {
                if let Ok(duration) = fundu::parse_duration(refresh_data_window) {
                    return Some(duration);
                }
                tracing::warn!(
                    "Unable to parse refresh period for dataset {}: {}",
                    self.name,
                    refresh_data_window
                );
            }
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
            return acceleration.enabled && acceleration.mode == acceleration::Mode::File;
        }

        false
    }
}

pub mod acceleration {
    use super::Result;
    use arrow::datatypes::SchemaRef;
    use data_components::util::indexes::index_columns;
    use datafusion::{
        common::{Constraints, DFSchema},
        sql::sqlparser::ast::TableConstraint,
    };
    use snafu::prelude::*;
    use spicepod::component::{dataset::acceleration as spicepod_acceleration, params::Params};
    use std::{collections::HashMap, fmt::Display, sync::Arc};

    #[derive(Debug, Clone, PartialEq, Default)]
    pub enum RefreshMode {
        #[default]
        Full,
        Append,
    }

    impl From<spicepod_acceleration::RefreshMode> for RefreshMode {
        fn from(refresh_mode: spicepod_acceleration::RefreshMode) -> Self {
            match refresh_mode {
                spicepod_acceleration::RefreshMode::Full => RefreshMode::Full,
                spicepod_acceleration::RefreshMode::Append => RefreshMode::Append,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Default)]
    pub enum Mode {
        #[default]
        Memory,
        File,
    }

    impl From<spicepod_acceleration::Mode> for Mode {
        fn from(mode: spicepod_acceleration::Mode) -> Self {
            match mode {
                spicepod_acceleration::Mode::Memory => Mode::Memory,
                spicepod_acceleration::Mode::File => Mode::File,
            }
        }
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
    #[derive(Debug, Clone, PartialEq, Default)]
    pub enum ZeroResultsAction {
        /// Return an empty result set. This is the default.
        #[default]
        ReturnEmpty,
        /// Fallback to querying the source table.
        UseSource,
    }

    impl From<spicepod_acceleration::ZeroResultsAction> for ZeroResultsAction {
        fn from(zero_results_action: spicepod_acceleration::ZeroResultsAction) -> Self {
            match zero_results_action {
                spicepod_acceleration::ZeroResultsAction::ReturnEmpty => {
                    ZeroResultsAction::ReturnEmpty
                }
                spicepod_acceleration::ZeroResultsAction::UseSource => ZeroResultsAction::UseSource,
            }
        }
    }

    impl Display for ZeroResultsAction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ZeroResultsAction::ReturnEmpty => write!(f, "return_empty"),
                ZeroResultsAction::UseSource => write!(f, "use_source"),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
    pub enum Engine {
        #[default]
        Arrow,
        DuckDB,
        Sqlite,
        PostgreSQL,
    }

    impl Display for Engine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Engine::Arrow => write!(f, "arrow"),
                Engine::DuckDB => write!(f, "duckdb"),
                Engine::Sqlite => write!(f, "sqlite"),
                Engine::PostgreSQL => write!(f, "postgres"),
            }
        }
    }

    impl TryFrom<&str> for Engine {
        type Error = crate::Error;

        fn try_from(engine: &str) -> std::result::Result<Self, Self::Error> {
            match engine.to_lowercase().as_str() {
                "arrow" => Ok(Engine::Arrow),
                "duckdb" => Ok(Engine::DuckDB),
                "sqlite" => Ok(Engine::Sqlite),
                "postgres" | "postgresql" => Ok(Engine::PostgreSQL),
                _ => crate::AcceleratorEngineNotAvailableSnafu {
                    name: engine.to_string(),
                }
                .fail(),
            }
        }
    }

    impl TryFrom<String> for Engine {
        type Error = crate::Error;

        fn try_from(engine: String) -> std::result::Result<Self, Self::Error> {
            Engine::try_from(engine.as_str())
        }
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub enum IndexType {
        #[default]
        Enabled,
        Unique,
    }

    impl From<spicepod_acceleration::IndexType> for IndexType {
        fn from(index_type: spicepod_acceleration::IndexType) -> Self {
            match index_type {
                spicepod_acceleration::IndexType::Enabled => IndexType::Enabled,
                spicepod_acceleration::IndexType::Unique => IndexType::Unique,
            }
        }
    }

    impl From<&str> for IndexType {
        fn from(index_type: &str) -> Self {
            match index_type.to_lowercase().as_str() {
                "unique" => IndexType::Unique,
                _ => IndexType::Enabled,
            }
        }
    }

    impl Display for IndexType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                IndexType::Enabled => write!(f, "enabled"),
                IndexType::Unique => write!(f, "unique"),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Acceleration {
        pub enabled: bool,

        pub mode: Mode,

        pub engine: Engine,

        pub refresh_mode: RefreshMode,

        pub refresh_check_interval: Option<String>,

        pub refresh_sql: Option<String>,

        pub refresh_data_window: Option<String>,

        pub params: HashMap<String, String>,

        pub engine_secret: Option<String>,

        pub retention_period: Option<String>,

        pub retention_check_interval: Option<String>,

        pub retention_check_enabled: bool,

        pub on_zero_results: ZeroResultsAction,

        pub indexes: HashMap<String, IndexType>,
    }

    impl Acceleration {
        #[must_use]
        pub fn indexes_to_option_string(indexes: &HashMap<String, IndexType>) -> String {
            indexes
                .iter()
                .map(|(k, v)| format!("{k}:{v}"))
                .collect::<Vec<String>>()
                .join(";")
        }

        pub fn index_columns(indexes_key: &str) -> Vec<&str> {
            // The key to an index can be either a single column or a compound index
            if indexes_key.starts_with('(') {
                // Compound index
                let end = indexes_key.find(')').unwrap_or(indexes_key.len());
                indexes_key[1..end]
                    .split(',')
                    .map(str::trim)
                    .collect::<Vec<&str>>()
            } else {
                // Single column index
                vec![indexes_key]
            }
        }

        fn valid_columns(schema: &SchemaRef) -> String {
            schema
                .all_fields()
                .into_iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        }

        pub fn validate_indexes(
            indexes: &HashMap<String, IndexType>,
            schema: &SchemaRef,
        ) -> Result<()> {
            for column in indexes.keys() {
                let index_columns = Self::index_columns(column);
                for index_column in index_columns {
                    if schema.field_with_name(index_column).is_err() {
                        return super::IndexColumnNotFoundSnafu {
                            index: index_column.to_string(),
                            valid_columns: Self::valid_columns(schema),
                        }
                        .fail();
                    }
                }
            }

            Ok(())
        }

        pub fn table_constraints(&self, schema: SchemaRef) -> Result<Option<Constraints>> {
            if self.indexes.is_empty() {
                return Ok(None);
            }

            Self::validate_indexes(&self.indexes, &schema)?;

            let mut table_constraints: Vec<TableConstraint> = Vec::new();

            self.indexes.iter().for_each(|(column, index_type)| {
                match index_type {
                    IndexType::Enabled => {}
                    IndexType::Unique => {
                        let tc = TableConstraint::Unique {
                            columns: index_columns(column).into_iter().map(Into::into).collect(),
                            name: None,
                            index_name: None,
                            index_type_display:
                                datafusion::sql::sqlparser::ast::KeyOrIndexDisplay::None,
                            index_options: vec![],
                            characteristics: None,
                            index_type: None,
                        };

                        table_constraints.push(tc);
                    }
                };
            });

            Ok(Some(
                Constraints::new_from_table_constraints(
                    &table_constraints,
                    &Arc::new(
                        DFSchema::try_from(schema)
                            .context(super::UnableToConvertSchemaRefToDFSchemaSnafu)?,
                    ),
                )
                .context(super::UnableToGetTableConstraintsSnafu)?,
            ))
        }
    }

    impl TryFrom<spicepod_acceleration::Acceleration> for Acceleration {
        type Error = crate::Error;

        fn try_from(
            acceleration: spicepod_acceleration::Acceleration,
        ) -> std::result::Result<Self, Self::Error> {
            Ok(Acceleration {
                enabled: acceleration.enabled,
                mode: Mode::from(acceleration.mode),
                engine: Engine::try_from(
                    acceleration.engine.unwrap_or_else(|| "arrow".to_string()),
                )?,
                refresh_mode: RefreshMode::from(acceleration.refresh_mode),
                refresh_check_interval: acceleration.refresh_check_interval,
                refresh_sql: acceleration.refresh_sql,
                refresh_data_window: acceleration.refresh_data_window,
                params: acceleration
                    .params
                    .as_ref()
                    .map(Params::as_string_map)
                    .unwrap_or_default(),
                engine_secret: acceleration.engine_secret,
                retention_period: acceleration.retention_period,
                retention_check_interval: acceleration.retention_check_interval,
                retention_check_enabled: acceleration.retention_check_enabled,
                on_zero_results: ZeroResultsAction::from(acceleration.on_zero_results),
                indexes: acceleration
                    .indexes
                    .into_iter()
                    .map(|(k, v)| (k, IndexType::from(v)))
                    .collect(),
            })
        }
    }

    impl Default for Acceleration {
        fn default() -> Self {
            Self {
                enabled: true,
                mode: Mode::Memory,
                engine: Engine::default(),
                refresh_mode: RefreshMode::Full,
                refresh_check_interval: None,
                refresh_sql: None,
                refresh_data_window: None,
                params: HashMap::default(),
                engine_secret: None,
                retention_period: None,
                retention_check_interval: None,
                retention_check_enabled: false,
                on_zero_results: ZeroResultsAction::ReturnEmpty,
                indexes: HashMap::default(),
            }
        }
    }
}

pub mod replication {
    use spicepod::component::dataset::replication as spicepod_replication;

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Replication {
        pub enabled: bool,
    }

    impl From<spicepod_replication::Replication> for Replication {
        fn from(replication: spicepod_replication::Replication) -> Self {
            Replication {
                enabled: replication.enabled,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::acceleration::{Acceleration, IndexType};

    #[test]
    fn test_indexes_roundtrip() {
        let indexes_map = HashMap::from([
            ("foo".to_string(), IndexType::Enabled),
            ("bar".to_string(), IndexType::Unique),
        ]);

        let indexes_str = Acceleration::indexes_to_option_string(&indexes_map);
        assert!(indexes_str == "foo:enabled;bar:unique" || indexes_str == "bar:unique;foo:enabled");
        let roundtrip_indexes_map =
            data_components::util::indexes::indexes_from_option_string(&indexes_str);

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

        let indexes_str = Acceleration::indexes_to_option_string(&indexes_map);
        assert!(
            indexes_str == "(foo, bar):enabled;bar:unique"
                || indexes_str == "bar:unique;(foo, bar):enabled"
        );
        let roundtrip_indexes_map =
            data_components::util::indexes::indexes_from_option_string(&indexes_str);

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
        let index_columns_vec = Acceleration::index_columns("foo");
        assert_eq!(index_columns_vec, vec!["foo"]);

        let index_columns_vec = Acceleration::index_columns("(foo, bar)");
        assert_eq!(index_columns_vec, vec!["foo", "bar"]);

        let index_columns_vec = Acceleration::index_columns("(foo,bar)");
        assert_eq!(index_columns_vec, vec!["foo", "bar"]);
    }
}
