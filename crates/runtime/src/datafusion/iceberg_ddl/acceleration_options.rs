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

//! Store and parse DDL table options extracted from `CREATE TABLE ... WITH (...)` statements.
//!
//! Supports two option prefixes:
//! - `acceleration.*` — acceleration engine, mode, refresh settings, etc.
//! - `dataset.*` — dataset-level settings like `time_column` and `time_format`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::error::{DataFusionError, Result as DFResult};
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

/// Combined DDL table options extracted from `CREATE TABLE ... WITH (...)` clauses.
///
/// Bundles both `acceleration.*` and `dataset.*` options.
#[derive(Debug, Clone, Default)]
pub struct DdlTableOptions {
    /// Acceleration options, if any `acceleration.*` keys were present.
    pub acceleration: Option<Acceleration>,
    /// Dataset-level options, if any `dataset.*` keys were present.
    pub dataset: DatasetOptions,
}

/// Stores DDL table options extracted from `CREATE TABLE ... WITH (...)` clauses.
///
/// Keyed by the table name as it appeared in the SQL statement. The analyzer rule
/// retrieves (and removes) the options when rewriting the DDL plan.
#[derive(Debug, Clone, Default)]
pub struct DdlOptionsStore {
    options: HashMap<String, DdlTableOptions>,
}

impl DdlOptionsStore {
    /// Insert DDL table options for a table.
    pub fn insert(&mut self, table_name: String, options: DdlTableOptions) {
        self.options.insert(table_name, options);
    }

    /// Remove and return DDL table options for a table (consume on use).
    pub fn remove(&mut self, table_name: &str) -> Option<DdlTableOptions> {
        self.options.remove(table_name)
    }
}

/// Thread-safe, shared DDL options store.
pub type SharedDdlOptionsStore = Arc<RwLock<DdlOptionsStore>>;

/// Create a new shared store.
#[must_use]
pub fn new_shared_store() -> SharedDdlOptionsStore {
    Arc::new(RwLock::new(DdlOptionsStore::default()))
}

/// Parse `acceleration.*` and `dataset.*` key-value pairs into a [`DdlTableOptions`].
///
/// Keys use dot-prefix format: `acceleration.engine`, `dataset.time_column`, etc.
/// In SQL `WITH (...)` clauses, these must be double-quoted since dots are not
/// valid in bare identifiers:
///
/// ```sql
/// CREATE TABLE t (id INT) WITH (
///     "acceleration.engine" = 'arrow',
///     "dataset.time_column" = 'created_at',
///     "dataset.time_format" = 'timestamp'
/// )
/// ```
///
/// # Errors
///
/// Returns an error if an unknown key is encountered or a value cannot be parsed.
pub fn parse_ddl_table_options(options: &[(String, String)]) -> DFResult<DdlTableOptions> {
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

    Ok(DdlTableOptions {
        acceleration,
        dataset,
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
        _ => Err(DataFusionError::Plan(format!(
            "Invalid value for 'acceleration.mode': '{value}'. Expected 'memory', 'file', or 'file_create'."
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
    use super::*;

    #[test]
    fn test_parse_basic_acceleration_options() {
        let options = vec![
            ("acceleration.engine".to_string(), "arrow".to_string()),
            ("acceleration.mode".to_string(), "memory".to_string()),
            ("acceleration.refresh_mode".to_string(), "full".to_string()),
            (
                "acceleration.refresh_check_interval".to_string(),
                "10s".to_string(),
            ),
        ];

        let accel = parse_acceleration_options(&options).expect("should parse");
        assert!(accel.enabled);
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(accel.mode, acceleration::Mode::Memory);
        assert_eq!(accel.refresh_mode, Some(acceleration::RefreshMode::Full));
        assert_eq!(accel.refresh_check_interval.as_deref(), Some("10s"));
    }

    #[test]
    fn test_parse_disabled_acceleration() {
        let options = vec![("acceleration.enabled".to_string(), "false".to_string())];
        let accel = parse_acceleration_options(&options).expect("should parse");
        assert!(!accel.enabled);
    }

    #[test]
    fn test_parse_uppercase_booleans() {
        let options = vec![
            ("acceleration.enabled".to_string(), "TRUE".to_string()),
            (
                "acceleration.retention_check_enabled".to_string(),
                "FALSE".to_string(),
            ),
        ];

        let accel = parse_acceleration_options(&options).expect("should parse");
        assert!(accel.enabled);
        assert!(!accel.retention_check_enabled);
    }

    #[test]
    fn test_parse_unknown_option_errors() {
        let options = vec![(
            "acceleration.unknown_field".to_string(),
            "value".to_string(),
        )];
        let result = parse_acceleration_options(&options);
        let err = result.expect_err("should return an error").to_string();
        assert!(err.contains("Unknown acceleration option"));
    }

    #[test]
    fn test_parse_invalid_mode_errors() {
        let options = vec![("acceleration.mode".to_string(), "invalid".to_string())];
        let _ = parse_acceleration_options(&options).expect_err("should return an error");
    }

    #[test]
    fn test_parse_invalid_refresh_mode_errors() {
        let options = vec![(
            "acceleration.refresh_mode".to_string(),
            "invalid".to_string(),
        )];
        let _ = parse_acceleration_options(&options).expect_err("should return an error");
    }

    #[test]
    fn test_parse_missing_prefix_errors() {
        let options = vec![("engine".to_string(), "arrow".to_string())];
        let _ = parse_acceleration_options(&options).expect_err("should return an error");
    }

    #[test]
    fn test_store_insert_and_remove() {
        let mut store = DdlOptionsStore::default();
        store.insert("my_table".to_string(), DdlTableOptions::default());

        assert!(store.remove("my_table").is_some());
        assert!(store.remove("my_table").is_none()); // consumed
    }

    #[test]
    fn test_parse_dataset_time_column() {
        let options = vec![("dataset.time_column".to_string(), "created_at".to_string())];
        let dataset = parse_dataset_options(&options).expect("should parse");
        assert_eq!(dataset.time_column.as_deref(), Some("created_at"));
        assert_eq!(dataset.time_format, None);
    }

    #[test]
    fn test_parse_dataset_time_format() {
        let options = vec![
            ("dataset.time_column".to_string(), "updated_at".to_string()),
            ("dataset.time_format".to_string(), "timestamptz".to_string()),
        ];
        let dataset = parse_dataset_options(&options).expect("should parse");
        assert_eq!(dataset.time_column.as_deref(), Some("updated_at"));
        assert_eq!(dataset.time_format, Some(SpicepodTimeFormat::Timestamptz));
    }

    #[test]
    fn test_parse_dataset_all_time_formats() {
        for (input, expected) in [
            ("timestamp", SpicepodTimeFormat::Timestamp),
            ("timestamptz", SpicepodTimeFormat::Timestamptz),
            ("unix_seconds", SpicepodTimeFormat::UnixSeconds),
            ("unixseconds", SpicepodTimeFormat::UnixSeconds),
            ("unix_millis", SpicepodTimeFormat::UnixMillis),
            ("unixmillis", SpicepodTimeFormat::UnixMillis),
            ("ISO8601", SpicepodTimeFormat::ISO8601),
            ("iso8601", SpicepodTimeFormat::ISO8601),
            ("date", SpicepodTimeFormat::Date),
        ] {
            let options = vec![("dataset.time_format".to_string(), input.to_string())];
            let dataset = parse_dataset_options(&options)
                .unwrap_or_else(|_| panic!("should parse time_format '{input}'"));
            assert_eq!(dataset.time_format, Some(expected), "for input '{input}'");
        }
    }

    #[test]
    fn test_parse_dataset_invalid_time_format_errors() {
        let options = vec![("dataset.time_format".to_string(), "invalid".to_string())];
        let err = parse_dataset_options(&options)
            .expect_err("should return an error")
            .to_string();
        assert!(err.contains("Invalid value for 'dataset.time_format'"));
    }

    #[test]
    fn test_parse_dataset_unknown_option_errors() {
        let options = vec![("dataset.unknown".to_string(), "value".to_string())];
        let err = parse_dataset_options(&options)
            .expect_err("should return an error")
            .to_string();
        assert!(err.contains("Unknown dataset option"));
    }

    #[test]
    fn test_parse_ddl_table_options_mixed() {
        let options = vec![
            ("acceleration.engine".to_string(), "arrow".to_string()),
            ("dataset.time_column".to_string(), "created_at".to_string()),
            ("dataset.time_format".to_string(), "timestamptz".to_string()),
        ];
        let ddl_opts = parse_ddl_table_options(&options).expect("should parse");
        assert!(ddl_opts.acceleration.is_some());
        let accel = ddl_opts.acceleration.expect("acceleration should be Some");
        assert_eq!(accel.engine.as_deref(), Some("arrow"));
        assert_eq!(ddl_opts.dataset.time_column.as_deref(), Some("created_at"));
        assert_eq!(
            ddl_opts.dataset.time_format,
            Some(SpicepodTimeFormat::Timestamptz)
        );
    }

    #[test]
    fn test_parse_ddl_table_options_unknown_prefix_errors() {
        let options = vec![("other.key".to_string(), "value".to_string())];
        let err = parse_ddl_table_options(&options)
            .expect_err("should return an error")
            .to_string();
        assert!(err.contains("Unknown option prefix"));
    }

    #[test]
    fn test_parse_ddl_table_options_dataset_only() {
        let options = vec![("dataset.time_column".to_string(), "created_at".to_string())];
        let ddl_opts = parse_ddl_table_options(&options).expect("should parse");
        assert!(ddl_opts.acceleration.is_none());
        assert_eq!(ddl_opts.dataset.time_column.as_deref(), Some("created_at"));
    }
}
