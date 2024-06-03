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
use std::{collections::HashMap, fs, time::Duration};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load SQL file {file}: {source}"))]
    UnableToLoadSqlFile {
        file: String,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    /// Inline SQL that describes a view.
    sql: Option<String>,
    /// Reference to a SQL file that describes a view.
    sql_ref: Option<String>,
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

    fn try_from(dataset: spicepod_dataset::Dataset) -> Result<Self, Self::Error> {
        let acceleration = dataset
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        let table_reference = Dataset::parse_table_reference(&dataset.name)?;

        Ok(Dataset {
            from: dataset.from,
            name: table_reference,
            mode: Mode::from(dataset.mode),
            sql: dataset.sql,
            sql_ref: dataset.sql_ref,
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
    pub fn try_new(from: String, name: &str) -> Result<Self, crate::Error> {
        Ok(Dataset {
            from,
            name: Self::parse_table_reference(name)?,
            mode: Mode::default(),
            sql: None,
            sql_ref: None,
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

    fn parse_table_reference(name: &str) -> Result<TableReference, crate::Error> {
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
    pub fn is_view(&self) -> bool {
        self.sql.is_some() || self.sql_ref.is_some()
    }

    #[must_use]
    pub fn view_sql(&self) -> Option<Result<String>> {
        if let Some(sql) = &self.sql {
            return Some(Ok(sql.clone()));
        }

        if let Some(sql_ref) = &self.sql_ref {
            return Some(Self::load_sql_ref(sql_ref));
        }

        None
    }

    fn load_sql_ref(sql_ref: &str) -> Result<String> {
        let sql =
            fs::read_to_string(sql_ref).context(UnableToLoadSqlFileSnafu { file: sql_ref })?;
        Ok(sql)
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
    use spicepod::component::{dataset::acceleration as spicepod_acceleration, params::Params};
    use std::{collections::HashMap, fmt::Display};

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

        fn try_from(engine: &str) -> Result<Self, Self::Error> {
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

        fn try_from(engine: String) -> Result<Self, Self::Error> {
            Engine::try_from(engine.as_str())
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
    }

    impl TryFrom<spicepod_acceleration::Acceleration> for Acceleration {
        type Error = crate::Error;

        fn try_from(
            acceleration: spicepod_acceleration::Acceleration,
        ) -> Result<Self, Self::Error> {
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
