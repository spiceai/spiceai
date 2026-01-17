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

#[cfg(feature = "duckdb")]
use crate::dataaccelerator::partitioned_duckdb::{DuckDBPartitionMode, get_duckdb_partition_mode};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, constraints::UpsertOptions,
};
use runtime_acceleration::snapshot::SnapshotBehavior;
use serde::{Deserialize, Serialize};
use spicepod::acceleration::{SnapshotsCompaction, SnapshotsTrigger};
use spicepod::{
    acceleration::{self as spicepod_acceleration},
    param::Params,
    partitioning::PartitionedBy,
};
use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};

pub use runtime_acceleration::Engine;

pub mod constraints;
pub mod on_conflict;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum RefreshMode {
    Disabled,
    Full,
    Append,
    Changes,
    Caching,
}

impl From<spicepod_acceleration::RefreshMode> for RefreshMode {
    fn from(refresh_mode: spicepod_acceleration::RefreshMode) -> Self {
        match refresh_mode {
            spicepod_acceleration::RefreshMode::Full => RefreshMode::Full,
            spicepod_acceleration::RefreshMode::Append => RefreshMode::Append,
            spicepod_acceleration::RefreshMode::Changes => RefreshMode::Changes,
            spicepod_acceleration::RefreshMode::Caching => RefreshMode::Caching,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Mode {
    #[default]
    Memory,
    /// Open an existing file if it exists, otherwise create a new one.
    /// This is the default file behavior that preserves data across restarts.
    File,
    /// Always create a new file, truncating/overwriting any existing file on startup.
    /// Use this when you want a fresh acceleration on each startup.
    FileCreate,
}

impl From<spicepod_acceleration::Mode> for Mode {
    fn from(mode: spicepod_acceleration::Mode) -> Self {
        match mode {
            spicepod_acceleration::Mode::Memory => Mode::Memory,
            spicepod_acceleration::Mode::File => Mode::File,
            spicepod_acceleration::Mode::FileCreate => Mode::FileCreate,
        }
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Memory => write!(f, "memory"),
            Mode::File => write!(f, "file"),
            Mode::FileCreate => write!(f, "file_create"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum RefreshOnStartup {
    /// Always start a new refresh when Spice starts.
    Always,
    /// Only start a refresh if an existing acceleration is not available.
    #[default]
    Auto,
}

impl From<spicepod_acceleration::RefreshOnStartup> for RefreshOnStartup {
    fn from(refresh_on_startup: spicepod_acceleration::RefreshOnStartup) -> Self {
        match refresh_on_startup {
            spicepod_acceleration::RefreshOnStartup::Always => RefreshOnStartup::Always,
            spicepod_acceleration::RefreshOnStartup::Auto => RefreshOnStartup::Auto,
        }
    }
}

impl Display for RefreshOnStartup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshOnStartup::Always => write!(f, "always"),
            RefreshOnStartup::Auto => write!(f, "auto"),
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

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OnConflictBehavior {
    #[default]
    Drop,
    Upsert(UpsertOptions),
}

impl From<spicepod_acceleration::OnConflictBehavior> for OnConflictBehavior {
    fn from(index_type: spicepod_acceleration::OnConflictBehavior) -> Self {
        match index_type {
            spicepod_acceleration::OnConflictBehavior::Drop => OnConflictBehavior::Drop,
            spicepod_acceleration::OnConflictBehavior::Upsert => {
                OnConflictBehavior::Upsert(UpsertOptions::default())
            }
            spicepod_acceleration::OnConflictBehavior::UpsertDedup => {
                OnConflictBehavior::Upsert(UpsertOptions::default().with_remove_duplicates(true))
            }
            spicepod_acceleration::OnConflictBehavior::UpsertDedupByRowId => {
                OnConflictBehavior::Upsert(UpsertOptions::default().with_last_write_wins(true))
            }
        }
    }
}

impl From<&str> for OnConflictBehavior {
    fn from(index_type: &str) -> Self {
        match index_type.to_lowercase().as_str() {
            "upsert" => OnConflictBehavior::Upsert(UpsertOptions::default()),
            _ => OnConflictBehavior::Drop,
        }
    }
}

impl Display for OnConflictBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnConflictBehavior::Drop => write!(f, "drop"),
            OnConflictBehavior::Upsert(_options) => write!(f, "upsert"),
        }
    }
}

/// Behavior when a stale-if-error condition occurs in caching mode.
/// When enabled, serves expired cached data if the upstream source returns an error.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum StaleIfError {
    /// Do not serve stale data on error - propagate the error to the client.
    #[default]
    Disabled,
    /// Serve expired data if the upstream source returns an error.
    Enabled,
}

impl StaleIfError {
    #[must_use]
    pub fn is_enabled(self) -> bool {
        matches!(self, StaleIfError::Enabled)
    }
}

impl Display for StaleIfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StaleIfError::Disabled => write!(f, "disabled"),
            StaleIfError::Enabled => write!(f, "enabled"),
        }
    }
}

#[expect(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct Acceleration {
    pub enabled: bool,

    pub mode: Mode,

    pub engine: Engine,

    pub refresh_mode: Option<RefreshMode>,

    pub refresh_on_startup: RefreshOnStartup,

    pub refresh_check_interval: Option<Duration>,

    pub caching_ttl: Option<Duration>,

    pub caching_stale_while_revalidate_ttl: Option<Duration>,

    pub caching_stale_if_error: StaleIfError,

    pub refresh_cron: Option<Arc<str>>,

    pub refresh_sql: Option<String>,

    pub refresh_data_window: Option<String>,

    pub refresh_append_overlap: Option<Duration>,

    pub refresh_retry_enabled: bool,

    pub refresh_retry_max_attempts: Option<usize>,

    pub refresh_jitter_enabled: bool,

    pub refresh_jitter_max: Option<Duration>,

    pub params: HashMap<String, String>,

    pub retention_period: Option<String>,

    pub retention_sql: Option<String>,

    pub retention_check_interval: Option<String>,

    pub retention_check_enabled: bool,

    pub on_zero_results: ZeroResultsAction,

    pub indexes: HashMap<ColumnReference, IndexType>,

    pub primary_key: Option<ColumnReference>,

    pub on_conflict: HashMap<ColumnReference, OnConflictBehavior>,

    pub disable_federation: bool,

    pub partition_by: Vec<PartitionedBy>,

    pub snapshot_behavior: SnapshotBehavior,

    pub snapshots_trigger: Option<SnapshotsTrigger>,

    pub snapshots_trigger_threshold: Option<String>,

    pub snapshots_compaction: SnapshotsCompaction,

    pub snapshots_reset_expiry_on_load_enabled: bool,
}

impl Acceleration {
    #[must_use]
    pub fn with_primary_key(mut self, primary_key: ColumnReference) -> Self {
        self.primary_key = Some(primary_key);
        self
    }

    #[must_use]
    pub fn with_on_conflict(
        mut self,
        on_conflict: HashMap<ColumnReference, OnConflictBehavior>,
    ) -> Self {
        self.on_conflict = on_conflict;
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

        let mut params = acceleration.params.clone();

        let engine_str = acceleration.engine.as_deref().unwrap_or("arrow");
        let engine = match Engine::try_from(engine_str).map_err(|_| {
            crate::Error::AcceleratorEngineNotAvailable {
                name: engine_str.to_string(),
            }
        })? {
            #[cfg(feature = "duckdb")]
            Engine::DuckDB if !acceleration.partition_by.is_empty() => {
                match get_duckdb_partition_mode(&params) {
                    DuckDBPartitionMode::Tables => Engine::TableModePartitionedDuckDB,
                    DuckDBPartitionMode::Files => Engine::PartitionedDuckDB,
                }
            }
            engine => engine,
        };

        if engine == Engine::Arrow && !indexes.is_empty() {
            tracing::warn!(
                "Indexes are not supported for Arrow engine acceleration. Ignoring indexes."
            );
        }
        // Only warn about primary_key if hash_index is not enabled
        let hash_index_enabled = params
            .as_ref()
            .and_then(|p| p.data.get("hash_index"))
            .is_some_and(|v| v.as_string().eq_ignore_ascii_case("enabled"));
        if engine == Engine::Arrow && primary_key.is_some() && !hash_index_enabled {
            tracing::warn!(
                "Primary key specified but hash_index is not enabled for Arrow engine. \
                 Add 'hash_index: enabled' to use primary_key for fast lookups. Note, hash_index is experimental in Arrow acceleration."
            );
        }
        // Warn when hash_index is enabled that it's experimental
        if engine == Engine::Arrow && hash_index_enabled {
            tracing::warn!(
                "hash_index is enabled for Arrow engine acceleration. Note: hash_index is experimental and may have breaking changes in future releases."
            );
        }
        if engine == Engine::Arrow && !on_conflict.is_empty() {
            tracing::warn!(
                "Conflict resolution is not supported for Arrow engine acceleration. Ignoring on_conflict."
            );
        }

        if matches!(
            acceleration.snapshots_reset_expiry_on_load,
            spicepod_acceleration::SnapshotsResetExpiryOnLoad::Enabled
        ) && (engine != Engine::DuckDB
            || !matches!(
                acceleration.refresh_mode,
                Some(spicepod_acceleration::RefreshMode::Caching)
            ))
        {
            tracing::warn!(
                "Resetting expiry on load is only supported for DuckDB engine acceleration with caching refresh mode. Ignoring snapshots_reset_expiry_on_load."
            );
        }

        let disable_federation = parse_is_query_federation_disabled(&mut params)?;

        let caching_ttl = parse_caching_ttl(&mut params)?;
        let caching_stale_while_revalidate_ttl =
            parse_caching_stale_while_revalidate_ttl(&mut params)?;
        let caching_stale_if_error = parse_caching_stale_if_error(&mut params)?;

        let refresh_check_interval = try_parse_duration(
            "refresh_check_interval",
            acceleration.refresh_check_interval,
        )?;

        let refresh_cron = acceleration.refresh_cron.map(Into::into);
        if refresh_cron.is_some() && refresh_check_interval.is_some() {
            return Err(crate::Error::InvalidSpicepodDataset {
                source: super::Error::MultipleRefreshExpressionSpecified,
            });
        }

        let refresh_jitter_max =
            try_parse_duration("refresh_jitter_max", acceleration.refresh_jitter_max)?;

        // TODO: Add validation for other refresh mode params here if needed.

        Ok(Acceleration {
            enabled: acceleration.enabled,
            mode: Mode::from(acceleration.mode),
            engine,
            refresh_mode: acceleration.refresh_mode.map(RefreshMode::from),
            refresh_on_startup: RefreshOnStartup::from(acceleration.refresh_on_startup),
            refresh_check_interval,
            caching_ttl,
            caching_stale_while_revalidate_ttl,
            caching_stale_if_error,
            refresh_cron,
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
            retention_sql: acceleration.retention_sql,
            retention_check_interval: acceleration.retention_check_interval,
            retention_check_enabled: acceleration.retention_check_enabled,
            disable_federation,
            on_zero_results: ZeroResultsAction::from(acceleration.on_zero_results),
            indexes,
            primary_key,
            on_conflict,
            partition_by: acceleration.partition_by,
            snapshot_behavior: SnapshotBehavior::disabled(),
            snapshots_trigger: acceleration.snapshots_trigger,
            snapshots_trigger_threshold: acceleration.snapshots_trigger_threshold,
            snapshots_compaction: acceleration.snapshots_compaction,
            snapshots_reset_expiry_on_load_enabled: matches!(
                acceleration.snapshots_reset_expiry_on_load,
                spicepod_acceleration::SnapshotsResetExpiryOnLoad::Enabled
            ),
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
            caching_ttl: None,
            caching_stale_while_revalidate_ttl: None,
            caching_stale_if_error: StaleIfError::default(),
            refresh_cron: None,
            refresh_sql: None,
            refresh_data_window: None,
            refresh_append_overlap: None,
            refresh_retry_enabled: true,
            refresh_retry_max_attempts: None,
            refresh_jitter_enabled: false,
            refresh_jitter_max: None,
            params: HashMap::default(),
            retention_period: None,
            retention_sql: None,
            retention_check_interval: None,
            retention_check_enabled: false,
            on_zero_results: ZeroResultsAction::ReturnEmpty,
            indexes: HashMap::default(),
            primary_key: None,
            on_conflict: HashMap::default(),
            disable_federation: false,
            refresh_on_startup: RefreshOnStartup::default(),
            partition_by: vec![],
            snapshot_behavior: SnapshotBehavior::Disabled,
            snapshots_trigger: None,
            snapshots_trigger_threshold: None,
            snapshots_compaction: SnapshotsCompaction::Disabled,
            snapshots_reset_expiry_on_load_enabled: false,
        }
    }
}

/// Returns true if the `query_federation` parameter is set to "disabled".
#[expect(clippy::result_large_err)]
fn parse_is_query_federation_disabled(params: &mut Option<Params>) -> Result<bool, crate::Error> {
    if let Some(params) = params
        && let Some(value) = params.data.remove("query_federation")
    {
        match value {
            spicepod::param::ParamValue::String(s) if s == "enabled" => return Ok(false),
            spicepod::param::ParamValue::String(s) if s == "disabled" => return Ok(true),
            _ => {
                return Err(crate::Error::InvalidAccelerationConfiguration {
                        source:
                            format!("Invalid 'query_federation' param value: {value:?}. Expected 'enabled' or 'disabled'.").into(),
                    });
            }
        }
    }
    Ok(false)
}

/// Parse `caching_ttl` duration from params for caching mode.
#[expect(clippy::result_large_err)]
fn parse_caching_ttl(params: &mut Option<Params>) -> Result<Option<Duration>, crate::Error> {
    parse_duration_param(params, "caching_ttl")
}

/// Parse `caching_stale_while_revalidate_ttl` duration from params for caching mode.
#[expect(clippy::result_large_err)]
fn parse_caching_stale_while_revalidate_ttl(
    params: &mut Option<Params>,
) -> Result<Option<Duration>, crate::Error> {
    parse_duration_param(params, "caching_stale_while_revalidate_ttl")
}

/// Parse `caching_stale_if_error` from params for caching mode.
/// Valid values: "enabled", "disabled" (default)
#[expect(clippy::result_large_err)]
fn parse_caching_stale_if_error(params: &mut Option<Params>) -> Result<StaleIfError, crate::Error> {
    let Some(params) = params else {
        return Ok(StaleIfError::default());
    };
    let Some(value) = params.data.remove("caching_stale_if_error") else {
        return Ok(StaleIfError::default());
    };
    match value {
        spicepod::param::ParamValue::String(s) => match s.to_lowercase().as_str() {
            "enabled" => Ok(StaleIfError::Enabled),
            "disabled" => Ok(StaleIfError::Disabled),
            _ => Err(crate::Error::InvalidAccelerationConfiguration {
                source: format!(
                    "Invalid 'caching_stale_if_error' value: '{s}'. Expected 'enabled' or 'disabled'."
                )
                .into(),
            }),
        },
        _ => Err(crate::Error::InvalidAccelerationConfiguration {
            source: format!(
                "Invalid 'caching_stale_if_error' param value: {value:?}. Expected 'enabled' or 'disabled'."
            )
            .into(),
        }),
    }
}

/// Helper to parse a duration parameter from params.
#[expect(clippy::result_large_err)]
fn parse_duration_param(
    params: &mut Option<Params>,
    param_name: &str,
) -> Result<Option<Duration>, crate::Error> {
    let Some(params) = params else {
        return Ok(None);
    };
    let Some(value) = params.data.remove(param_name) else {
        return Ok(None);
    };
    match value {
        spicepod::param::ParamValue::String(s) => {
            fundu::parse_duration(&s)
                .map(Some)
                .map_err(|e| crate::Error::InvalidSpicepodDataset {
                    source: super::Error::UnableToParseFieldAsDuration {
                        source: e,
                        field: param_name.into(),
                    },
                })
        }
        _ => Err(crate::Error::InvalidAccelerationConfiguration {
            source: format!(
                "Invalid '{param_name}' param value: {value:?}. Expected a duration string."
            )
            .into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_federation_disabled_param() {
        let params_enabled = Params::from_string_map(HashMap::from([(
            "query_federation".to_string(),
            "enabled".to_string(),
        )]));
        let is_disabled =
            parse_is_query_federation_disabled(&mut Some(params_enabled)).expect("to parse");
        assert!(!is_disabled);

        let params_disabled = Params::from_string_map(HashMap::from([(
            "query_federation".to_string(),
            "disabled".to_string(),
        )]));
        let is_disabled =
            parse_is_query_federation_disabled(&mut Some(params_disabled)).expect("to parse");
        assert!(is_disabled);

        let params_invalid = Params::from_string_map(HashMap::from([(
            "query_federation".to_string(),
            "invalid".to_string(),
        )]));
        let result_invalid = parse_is_query_federation_disabled(&mut Some(params_invalid));
        result_invalid.expect_err("should error parsing query_federation param");

        let params_missing = Params::from_string_map(HashMap::new());
        let is_disabled =
            parse_is_query_federation_disabled(&mut Some(params_missing)).expect("to parse");
        assert!(!is_disabled);
    }

    #[test]
    fn test_parse_caching_stale_if_error() {
        // Test "enabled"
        let params_enabled = Params::from_string_map(HashMap::from([(
            "caching_stale_if_error".to_string(),
            "enabled".to_string(),
        )]));
        let result = parse_caching_stale_if_error(&mut Some(params_enabled)).expect("to parse");
        assert_eq!(result, StaleIfError::Enabled);

        // Test "disabled"
        let params_disabled = Params::from_string_map(HashMap::from([(
            "caching_stale_if_error".to_string(),
            "disabled".to_string(),
        )]));
        let result = parse_caching_stale_if_error(&mut Some(params_disabled)).expect("to parse");
        assert_eq!(result, StaleIfError::Disabled);

        // Test invalid value
        let params_invalid = Params::from_string_map(HashMap::from([(
            "caching_stale_if_error".to_string(),
            "invalid".to_string(),
        )]));
        parse_caching_stale_if_error(&mut Some(params_invalid)).expect_err("should error");

        // Test missing parameter (default)
        let result = parse_caching_stale_if_error(&mut None).expect("to parse");
        assert_eq!(result, StaleIfError::Disabled);
    }
}
