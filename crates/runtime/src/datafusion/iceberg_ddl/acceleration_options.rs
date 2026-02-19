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

//! Store and parse acceleration options extracted from `CREATE TABLE ... WITH (...)` statements.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::error::{DataFusionError, Result as DFResult};
use spicepod::acceleration::{self, Acceleration};

/// Stores acceleration options extracted from `CREATE TABLE ... WITH (acceleration.*)` clauses.
///
/// Keyed by the table name as it appeared in the SQL statement. The analyzer rule
/// retrieves (and removes) the options when rewriting the DDL plan.
#[derive(Debug, Clone, Default)]
pub struct AccelerationOptionsStore {
    options: HashMap<String, Acceleration>,
}

impl AccelerationOptionsStore {
    /// Insert acceleration options for a table.
    pub fn insert(&mut self, table_name: String, acceleration: Acceleration) {
        self.options.insert(table_name, acceleration);
    }

    /// Remove and return acceleration options for a table (consume on use).
    pub fn remove(&mut self, table_name: &str) -> Option<Acceleration> {
        self.options.remove(table_name)
    }
}

/// Thread-safe, shared acceleration options store.
pub type SharedAccelerationOptionsStore = Arc<RwLock<AccelerationOptionsStore>>;

/// Create a new shared store.
#[must_use]
pub fn new_shared_store() -> SharedAccelerationOptionsStore {
    Arc::new(RwLock::new(AccelerationOptionsStore::default()))
}

/// Parse `acceleration.*` key-value pairs into an [`Acceleration`] struct.
///
/// Keys use dot-prefix format: `acceleration.engine`, `acceleration.mode`, etc.
/// In SQL `WITH (...)` clauses, these must be double-quoted since dots are not
/// valid in bare identifiers:
///
/// ```sql
/// CREATE TABLE t (id INT) WITH ("acceleration.engine" = 'arrow')
/// ```
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
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown acceleration option"));
    }

    #[test]
    fn test_parse_invalid_mode_errors() {
        let options = vec![("acceleration.mode".to_string(), "invalid".to_string())];
        let result = parse_acceleration_options(&options);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_refresh_mode_errors() {
        let options = vec![(
            "acceleration.refresh_mode".to_string(),
            "invalid".to_string(),
        )];
        let result = parse_acceleration_options(&options);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_prefix_errors() {
        let options = vec![("engine".to_string(), "arrow".to_string())];
        let result = parse_acceleration_options(&options);
        assert!(result.is_err());
    }

    #[test]
    fn test_store_insert_and_remove() {
        let mut store = AccelerationOptionsStore::default();
        let accel = Acceleration::default();
        store.insert("my_table".to_string(), accel.clone());

        assert!(store.remove("my_table").is_some());
        assert!(store.remove("my_table").is_none()); // consumed
    }
}
