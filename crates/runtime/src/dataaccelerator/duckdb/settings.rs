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

use datafusion_table_providers::duckdb::{DuckDBSetting, DuckDBSettingScope, Error};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OrderByNonIntegerLiteral;

impl DuckDBSetting for OrderByNonIntegerLiteral {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "order_by_non_integer_literal"
    }

    fn get_value(&self, _options: &std::collections::HashMap<String, String>) -> Option<String> {
        Some(String::from("true"))
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Local
    }
}

/// `DuckDB` setting for configuring the percentage of rows that trigger an index scan.
///
/// The index scan percentage sets a threshold for index scans. An index scan is performed
/// instead of a table scan when the number of matching rows is less than the maximum of
/// `index_scan_max_count` and `index_scan_percentage × total_row_count`.
///
/// Type: DOUBLE, Default: 0.001
///
/// See: <https://duckdb.org/docs/stable/guides/performance/indexing#art-index-scans>
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct IndexScanPercentage;

impl DuckDBSetting for IndexScanPercentage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "index_scan_percentage"
    }

    fn get_value(&self, options: &std::collections::HashMap<String, String>) -> Option<String> {
        options.get(self.setting_name()).cloned()
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Global
    }

    fn validate(&self, value: &str) -> Result<(), Error> {
        // Validate that the value is a valid percentage (0.0-1.0)
        let percentage = value.parse::<f64>().map_err(|e| Error::DbConnectionError {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid index_scan_percentage value '{value}'. Must be a number between 0.0 and 1.0. Error: {e}"
                ),
            )),
        })?;

        if !(0.0..=1.0).contains(&percentage) {
            return Err(Error::DbConnectionError {
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Invalid index_scan_percentage value '{percentage}'. Must be between 0.0 and 1.0."
                    ),
                )),
            });
        }

        Ok(())
    }
}

/// `DuckDB` setting for configuring the maximum number of rows that trigger an index scan.
///
/// The maximum index scan count sets a threshold for index scans. An index scan is performed
/// instead of a table scan when the number of matching rows is less than the maximum of
/// `index_scan_max_count` and `index_scan_percentage × total_row_count`.
///
/// Type: UBIGINT, Default: 2048
///
/// See: <https://duckdb.org/docs/stable/guides/performance/indexing#art-index-scans>
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct IndexScanMaxCount;

impl DuckDBSetting for IndexScanMaxCount {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "index_scan_max_count"
    }

    fn get_value(&self, options: &std::collections::HashMap<String, String>) -> Option<String> {
        options.get(self.setting_name()).cloned()
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Global
    }

    fn validate(&self, value: &str) -> Result<(), Error> {
        // Validate that the value is a valid non-negative integer
        value.parse::<u64>().map_err(|e| Error::DbConnectionError {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Invalid index_scan_max_count value '{value}'. Must be a non-negative integer. Error: {e}"
                ),
            )),
        })?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TimeZone;

impl DuckDBSetting for TimeZone {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "TimeZone"
    }

    fn get_value(&self, _options: &std::collections::HashMap<String, String>) -> Option<String> {
        Some(String::from("UTC"))
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Local
    }
}
