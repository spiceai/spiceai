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

use datafusion_table_providers::util::column_reference::ColumnReference;
use serde::{Deserialize, Serialize};
use spicepod::component::{dataset::acceleration as spicepod_acceleration, params::Params};
use std::{collections::HashMap, fmt::Display, time::Duration};

pub mod constraints;
pub mod on_conflict;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RefreshMode {
    Disabled,
    Full,
    Append,
    Changes,
}

impl From<spicepod_acceleration::RefreshMode> for RefreshMode {
    fn from(refresh_mode: spicepod_acceleration::RefreshMode) -> Self {
        match refresh_mode {
            spicepod_acceleration::RefreshMode::Full => RefreshMode::Full,
            spicepod_acceleration::RefreshMode::Append => RefreshMode::Append,
            spicepod_acceleration::RefreshMode::Changes => RefreshMode::Changes,
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
            spicepod_acceleration::ZeroResultsAction::ReturnEmpty => ZeroResultsAction::ReturnEmpty,
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

#[derive(Debug, Clone, Copy, PartialEq, Default)]
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

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OnConflictBehavior {
    #[default]
    Drop,
    Upsert,
}

impl From<spicepod_acceleration::OnConflictBehavior> for OnConflictBehavior {
    fn from(index_type: spicepod_acceleration::OnConflictBehavior) -> Self {
        match index_type {
            spicepod_acceleration::OnConflictBehavior::Drop => OnConflictBehavior::Drop,
            spicepod_acceleration::OnConflictBehavior::Upsert => OnConflictBehavior::Upsert,
        }
    }
}

impl From<&str> for OnConflictBehavior {
    fn from(index_type: &str) -> Self {
        match index_type.to_lowercase().as_str() {
            "upsert" => OnConflictBehavior::Upsert,
            _ => OnConflictBehavior::Drop,
        }
    }
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
#[derive(Debug, Clone, PartialEq)]
pub struct Acceleration {
    pub enabled: bool,

    pub mode: Mode,

    pub engine: Engine,

    pub refresh_mode: Option<RefreshMode>,

    pub refresh_check_interval: Option<Duration>,

    pub refresh_sql: Option<String>,

    pub refresh_data_window: Option<String>,

    pub refresh_append_overlap: Option<Duration>,

    pub refresh_retry_enabled: bool,

    pub refresh_retry_max_attempts: Option<usize>,

    pub refresh_jitter_enabled: bool,

    pub refresh_jitter_max: Option<Duration>,

    pub params: HashMap<String, String>,

    pub retention_period: Option<String>,

    pub retention_check_interval: Option<String>,

    pub retention_check_enabled: bool,

    pub on_zero_results: ZeroResultsAction,

    pub indexes: HashMap<ColumnReference, IndexType>,

    pub primary_key: Option<ColumnReference>,

    pub on_conflict: HashMap<ColumnReference, OnConflictBehavior>,

    pub disable_query_push_down: bool,
}

impl Acceleration {
    #[must_use]
    pub fn with_primary_key(mut self, primary_key: ColumnReference) -> Self {
        self.primary_key = Some(primary_key);
        self
    }
}

impl TryFrom<spicepod_acceleration::Acceleration> for Acceleration {
    type Error = crate::Error;

    fn try_from(
        acceleration: spicepod_acceleration::Acceleration,
    ) -> std::result::Result<Self, Self::Error> {
        let try_parse_column_reference = |column: &str| {
            ColumnReference::try_from(column).map_err(|e| crate::Error::InvalidSpicepodDataset {
                source: super::Error::UnableToParseColumnReference {
                    column_ref: column.to_string(),
                    source: e,
                },
            })
        };

        let try_parse_duration = |field: &str, duration: Option<String>| {
            let Some(duration) = duration else {
                return Ok(None);
            };
            fundu::parse_duration(&duration).map(Some).map_err(|e| {
                crate::Error::InvalidSpicepodDataset {
                    source: super::Error::UnableToParseFieldAsDuration {
                        source: e,
                        field: field.into(),
                    },
                }
            })
        };

        let primary_key = match acceleration.primary_key {
            Some(pk) => Some(try_parse_column_reference(pk.as_str())?),
            None => None,
        };

        let mut indexes = HashMap::new();
        for (k, v) in acceleration.indexes {
            indexes.insert(try_parse_column_reference(k.as_str())?, IndexType::from(v));
        }

        let mut on_conflict = HashMap::new();
        for (k, v) in acceleration.on_conflict {
            on_conflict.insert(
                try_parse_column_reference(k.as_str())?,
                OnConflictBehavior::from(v),
            );
        }

        let engine = Engine::try_from(acceleration.engine.unwrap_or_else(|| "arrow".to_string()))?;

        if engine == Engine::Arrow && !indexes.is_empty() {
            tracing::warn!(
                "Indexes are not supported for Arrow engine acceleration. Ignoring indexes."
            );
        }
        if engine == Engine::Arrow && primary_key.is_some() {
            tracing::warn!(
                "Primary key is not supported for Arrow engine acceleration. Ignoring primary_key."
            );
        }
        if engine == Engine::Arrow && !on_conflict.is_empty() {
            tracing::warn!(
                "Conflict resolution is not supported for Arrow engine acceleration. Ignoring on_conflict."
            );
        }

        let mut params = acceleration.params.clone();

        let disable_query_push_down = match params
            .as_mut()
            .and_then(|x| x.data.remove("disable_query_push_down"))
        {
            Some(spicepod::component::params::ParamValue::Bool(value)) => value,
            _ => false,
        };

        let refresh_check_interval = try_parse_duration(
            "refresh_check_interval",
            acceleration.refresh_check_interval,
        )?;

        let refresh_jitter_max =
            try_parse_duration("refresh_jitter_max", acceleration.refresh_jitter_max)?;

        Ok(Acceleration {
            enabled: acceleration.enabled,
            mode: Mode::from(acceleration.mode),
            engine,
            refresh_mode: acceleration.refresh_mode.map(RefreshMode::from),
            refresh_check_interval,
            refresh_sql: acceleration.refresh_sql,
            refresh_data_window: acceleration.refresh_data_window,
            refresh_append_overlap: try_parse_duration(
                "refresh_append_overlap",
                acceleration.refresh_append_overlap,
            )?,
            refresh_retry_enabled: acceleration.refresh_retry_enabled,
            refresh_retry_max_attempts: acceleration.refresh_retry_max_attempts,
            refresh_jitter_max,
            refresh_jitter_enabled: acceleration.refresh_jitter_enabled,
            params: params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            retention_period: acceleration.retention_period,
            retention_check_interval: acceleration.retention_check_interval,
            retention_check_enabled: acceleration.retention_check_enabled,
            disable_query_push_down,
            on_zero_results: ZeroResultsAction::from(acceleration.on_zero_results),
            indexes,
            primary_key,
            on_conflict,
        })
    }
}

impl Default for Acceleration {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: Mode::Memory,
            engine: Engine::default(),
            refresh_mode: None,
            refresh_check_interval: None,
            refresh_sql: None,
            refresh_data_window: None,
            refresh_append_overlap: None,
            refresh_retry_enabled: true,
            refresh_retry_max_attempts: None,
            refresh_jitter_enabled: false,
            refresh_jitter_max: None,
            params: HashMap::default(),
            retention_period: None,
            retention_check_interval: None,
            retention_check_enabled: false,
            on_zero_results: ZeroResultsAction::ReturnEmpty,
            indexes: HashMap::default(),
            primary_key: None,
            on_conflict: HashMap::default(),
            disable_query_push_down: false,
        }
    }
}
