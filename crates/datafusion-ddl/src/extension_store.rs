/*
Copyright 2026, Spice AI, Inc.

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

//! DDL extension store and option parsing for `CREATE TABLE` statements.
//!
//! Supports:
//! - `WITH (...)` clauses with two option prefixes:
//!   - `acceleration.*` — acceleration engine, mode, refresh settings, etc.
//!   - `dataset.*` — dataset-level settings like `time_column` and `time_format`.
//! - `PARTITION BY` clauses (stored as the raw sqlparser `Expr`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::sqlparser::ast::Expr as SqlParserExpr;
use datafusion::sql::{ResolvedTableReference, TableReference};
use spicepod::acceleration::{self, Acceleration};
use spicepod::component::dataset::TimeFormat as SpicepodTimeFormat;

/// Dataset-level options extracted from `CREATE TABLE ... WITH ("dataset.*")` clauses.
#[derive(Debug, Clone, Default)]
pub struct DatasetOptions {
    /// The column to use as the time column for append-mode refreshes.
    pub time_column: Option<String>,
    /// The format of the time column (e.g. `timestamp`, `ISO8601`, `unix_seconds`).
    pub time_format: Option<SpicepodTimeFormat>,
}

/// Extensions extracted from a `CREATE TABLE` statement.
///
/// Bundles `WITH (...)` options (`acceleration.*` and `dataset.*`) and
/// `PARTITION BY` expressions extracted during SQL pre-processing.
#[derive(Debug, Clone, Default)]
pub struct CreateTableStatementExtension {
    /// Acceleration options, if any `acceleration.*` keys were present in `WITH (...)`.
    pub acceleration: Option<Acceleration>,
    /// Dataset-level options, if any `dataset.*` keys were present in `WITH (...)`.
    pub dataset: DatasetOptions,
    /// Partitioning expression from a `PARTITION BY` clause.
    /// The raw SQL expression as parsed by sqlparser.
    pub partition_by: Option<Box<SqlParserExpr>>,
}

/// Stores DDL extensions extracted from `CREATE TABLE` statements.
///
/// Keyed by the resolved table name. The analyzer rule retrieves (and removes)
/// the extensions when rewriting the DDL plan.
#[derive(Debug, Clone, Default)]
pub struct DdlExtensionStore {
    pub extensions: HashMap<ResolvedTableReference, CreateTableStatementExtension>,
    default_schema: String,
    default_catalog: String,
}

impl DdlExtensionStore {
    #[must_use]
    pub fn new(default_catalog: String, default_schema: String) -> Self {
        Self {
            default_catalog,
            default_schema,
            extensions: HashMap::new(),
        }
    }

    /// Insert DDL extensions for a table.
    pub fn insert(&mut self, table_name: TableReference, extension: CreateTableStatementExtension) {
        self.extensions.insert(
            table_name.resolve(&self.default_catalog, &self.default_schema),
            extension,
        );
    }

    /// Remove and return DDL extensions for a table (consume on use).
    pub fn remove(&mut self, table_name: &TableReference) -> Option<CreateTableStatementExtension> {
        self.extensions.remove(
            &table_name
                .clone()
                .resolve(&self.default_catalog, &self.default_schema),
        )
    }
}

/// Thread-safe, shared DDL extension store.
pub type SharedDdlExtensionStore = Arc<RwLock<DdlExtensionStore>>;

/// Create a new shared store.
#[must_use]
pub fn new_shared_store(
    default_catalog: impl Into<String>,
    default_schema: impl Into<String>,
) -> SharedDdlExtensionStore {
    Arc::new(RwLock::new(DdlExtensionStore::new(
        default_catalog.into(),
        default_schema.into(),
    )))
}

/// Parse `acceleration.*` and `dataset.*` key-value pairs into a [`CreateTableStatementExtension`].
///
/// # Errors
///
/// Returns an error if an unknown key prefix is encountered or a value cannot be parsed.
pub fn parse_ddl_table_options(
    options: &[(String, String)],
) -> DFResult<CreateTableStatementExtension> {
    let mut accel_opts = Vec::new();
    let mut dataset_opts = Vec::new();

    for (key, value) in options {
        if key.starts_with("acceleration.") {
            accel_opts.push((key.clone(), value.clone()));
        } else if key.starts_with("dataset.") {
            dataset_opts.push((key.clone(), value.clone()));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Unknown option prefix in '{key}'. Expected 'acceleration.*' or 'dataset.*'."
            )));
        }
    }

    let acceleration = if accel_opts.is_empty() {
        None
    } else {
        Some(parse_acceleration_options(&accel_opts)?)
    };

    let dataset = parse_dataset_options(&dataset_opts)?;

    Ok(CreateTableStatementExtension {
        acceleration,
        dataset,
        partition_by: None,
    })
}

/// Parse `acceleration.*` key-value pairs into an [`Acceleration`] struct.
///
/// # Errors
///
/// Returns an error if an unknown `acceleration.*` key is encountered or a value
/// cannot be parsed.
pub fn parse_acceleration_options(options: &[(String, String)]) -> DFResult<Acceleration> {
    let mut accel = Acceleration::default();

    for (key, value) in options {
        let field = key.strip_prefix("acceleration.").ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Expected 'acceleration.' prefix on table option, got '{key}'"
            ))
        })?;

        match field {
            "enabled" => {
                accel.enabled = parse_bool(value, "acceleration.enabled")?;
            }
            "engine" => {
                accel.engine = Some(value.clone());
            }
            "mode" => {
                accel.mode = parse_mode(value)?;
            }
            "refresh_mode" => {
                accel.refresh_mode = Some(parse_refresh_mode(value)?);
            }
            "refresh_check_interval" => {
                accel.refresh_check_interval = Some(value.clone());
            }
            "refresh_sql" => {
                accel.refresh_sql = Some(value.clone());
            }
            "refresh_data_window" => {
                accel.refresh_data_window = Some(value.clone());
            }
            "refresh_append_overlap" => {
                accel.refresh_append_overlap = Some(value.clone());
            }
            "retention_period" => {
                accel.retention_period = Some(value.clone());
            }
            "retention_check_interval" => {
                accel.retention_check_interval = Some(value.clone());
            }
            "retention_check_enabled" => {
                accel.retention_check_enabled =
                    parse_bool(value, "acceleration.retention_check_enabled")?;
            }
            unknown => {
                return Err(DataFusionError::Plan(format!(
                    "Unknown acceleration option: 'acceleration.{unknown}'. \
                     Supported options: acceleration.enabled, acceleration.engine, \
                     acceleration.mode, acceleration.refresh_mode, \
                     acceleration.refresh_check_interval, acceleration.refresh_sql, \
                     acceleration.refresh_data_window, acceleration.refresh_append_overlap, \
                     acceleration.retention_period, acceleration.retention_check_interval, \
                     acceleration.retention_check_enabled."
                )));
            }
        }
    }

    Ok(accel)
}

/// Parse `dataset.*` key-value pairs into a [`DatasetOptions`] struct.
///
/// # Errors
///
/// Returns an error if an unknown `dataset.*` key is encountered or a value
/// cannot be parsed.
pub fn parse_dataset_options(options: &[(String, String)]) -> DFResult<DatasetOptions> {
    let mut dataset = DatasetOptions::default();

    for (key, value) in options {
        let field = key.strip_prefix("dataset.").ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Expected 'dataset.' prefix on table option, got '{key}'"
            ))
        })?;

        match field {
            "time_column" => {
                dataset.time_column = Some(value.clone());
            }
            "time_format" => {
                dataset.time_format = Some(parse_time_format(value)?);
            }
            unknown => {
                return Err(DataFusionError::Plan(format!(
                    "Unknown dataset option: 'dataset.{unknown}'. \
                     Supported options: dataset.time_column, dataset.time_format."
                )));
            }
        }
    }

    Ok(dataset)
}

fn parse_time_format(value: &str) -> DFResult<SpicepodTimeFormat> {
    match value.to_lowercase().as_str() {
        "timestamp" => Ok(SpicepodTimeFormat::Timestamp),
        "timestamptz" => Ok(SpicepodTimeFormat::Timestamptz),
        "unix_seconds" | "unixseconds" => Ok(SpicepodTimeFormat::UnixSeconds),
        "unix_millis" | "unixmillis" => Ok(SpicepodTimeFormat::UnixMillis),
        "iso8601" => Ok(SpicepodTimeFormat::ISO8601),
        "date" => Ok(SpicepodTimeFormat::Date),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid value for 'dataset.time_format': '{value}'. \
             Expected 'timestamp', 'timestamptz', 'unix_seconds', 'unix_millis', 'ISO8601', or 'date'."
        ))),
    }
}

fn parse_bool(value: &str, field: &str) -> DFResult<bool> {
    if value.eq_ignore_ascii_case("true") {
        return Ok(true);
    }
    if value.eq_ignore_ascii_case("false") {
        return Ok(false);
    }
    Err(DataFusionError::Plan(format!(
        "Invalid value for '{field}': '{value}'. Expected 'true' or 'false'."
    )))
}

fn parse_mode(value: &str) -> DFResult<acceleration::Mode> {
    match value.to_lowercase().as_str() {
        "memory" => Ok(acceleration::Mode::Memory),
        "file" => Ok(acceleration::Mode::File),
        "file_create" => Ok(acceleration::Mode::FileCreate),
        "file_update" => Ok(acceleration::Mode::FileUpdate),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid value for 'acceleration.mode': '{value}'. Expected 'memory', 'file', 'file_create', or 'file_update'."
        ))),
    }
}

fn parse_refresh_mode(value: &str) -> DFResult<acceleration::RefreshMode> {
    match value.to_lowercase().as_str() {
        "full" => Ok(acceleration::RefreshMode::Full),
        "append" => Ok(acceleration::RefreshMode::Append),
        "changes" => Ok(acceleration::RefreshMode::Changes),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid value for 'acceleration.refresh_mode': '{value}'. Expected 'full', 'append', or 'changes'."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::TableReference;

    use super::*;

    #[test]
    fn test_store_insert_and_remove() {
        let mut store = DdlExtensionStore::default();
        store.insert(
            TableReference::parse_str("my_table"),
            CreateTableStatementExtension::default(),
        );
        assert!(
            store
                .remove(&TableReference::parse_str("my_table"))
                .is_some()
        );
        assert!(
            store
                .remove(&TableReference::parse_str("my_table"))
                .is_none()
        );
    }

    #[test]
    fn test_parse_ddl_table_options_unknown_prefix_errors() {
        let options = vec![("other.key".to_string(), "value".to_string())];
        let err = parse_ddl_table_options(&options)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("Unknown option prefix"));
    }

    #[test]
    fn test_parse_acceleration_unknown_option_errors() {
        let options = vec![(
            "acceleration.unknown_field".to_string(),
            "value".to_string(),
        )];
        let err = parse_acceleration_options(&options)
            .expect_err("should error")
            .to_string();
        assert!(err.contains("Unknown acceleration option"));
    }

    #[test]
    fn test_parse_dataset_time_formats() {
        for (input, expected) in [
            ("timestamp", SpicepodTimeFormat::Timestamp),
            ("timestamptz", SpicepodTimeFormat::Timestamptz),
            ("unix_seconds", SpicepodTimeFormat::UnixSeconds),
            ("iso8601", SpicepodTimeFormat::ISO8601),
        ] {
            let opts = vec![("dataset.time_format".to_string(), input.to_string())];
            let d = parse_dataset_options(&opts).unwrap_or_else(|_| panic!("parse {input}"));
            assert_eq!(d.time_format, Some(expected), "for '{input}'");
        }
    }
}
