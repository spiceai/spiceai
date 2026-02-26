/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Partition buffers configurations.

use std::{collections::HashMap, fmt, path::PathBuf};

use crate::spice_data_base_path;

// Buffering rows allows for much more efficient writes in `DuckDB`
// 122_880 represents DuckDB default size of groups of rows - that are stored together at the storage level.
const ROWS_PER_PARTITION_BUFFER: usize = 122_880;

/// Configuration for partition buffer type selection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PartitionBufferType {
    /// Use in-memory buffers (default behavior)
    #[default]
    Memory,
    /// Use Parquet file-based buffers
    Parquet,
}

impl PartitionBufferType {
    /// Parse buffer type from string configuration.
    ///
    /// Defaults to Memory for backward compatibility.
    pub fn parse_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "parquet" => Self::Parquet,
            "memory" => Self::Memory,
            _ => {
                tracing::warn!(
                    "Unrecognized partition buffer type '{s}', supported options: 'memory', 'parquet'. Defaulting to 'memory'."
                );
                Self::Memory
            }
        }
    }

    /// Get the string representation for configuration.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Parquet => "parquet",
        }
    }
}

impl fmt::Display for PartitionBufferType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Configuration for partition buffer creation.
#[derive(Debug, Clone)]
pub struct PartitionBufferConfig {
    /// Type of buffer to create
    pub buffer_type: PartitionBufferType,
    /// Number of rows per partition before flushing
    pub rows_per_partition_threshold: usize,
    /// Working directory for temporary files
    pub temp_dir: PathBuf,
}

impl Default for PartitionBufferConfig {
    fn default() -> Self {
        Self {
            buffer_type: PartitionBufferType::default(),
            rows_per_partition_threshold: ROWS_PER_PARTITION_BUFFER,
            temp_dir: spice_data_base_path().into(),
        }
    }
}

impl PartitionBufferConfig {
    /// Parse buffer configuration from parameters.
    pub fn from_params(params: Option<&HashMap<String, String>>) -> PartitionBufferConfig {
        let mut config = PartitionBufferConfig::default();

        if let Some(params) = params {
            if let Some(rows_threshold_str) =
                params.get("duckdb_partitioned_write_flush_threshold_rows")
            {
                if let Ok(threshold) = rows_threshold_str.parse::<usize>() {
                    config.rows_per_partition_threshold = threshold;
                } else {
                    tracing::warn!(
                        "Invalid `duckdb_partitioned_write_flush_threshold_rows` parameter '{rows_threshold_str}': must be a positive integer"
                    );
                }
            }

            if let Some(buffer_type_str) = params.get("partitioned_write_buffer") {
                config.buffer_type = PartitionBufferType::parse_str(buffer_type_str);
            }

            if let Some(data_dir) = params.get("duckdb_data_dir") {
                config.temp_dir = PathBuf::from(data_dir);
            } else if let Some(duckdb_file) = params.get("duckdb_file") {
                let file_path = PathBuf::from(duckdb_file);
                if let Some(parent_dir) = file_path.parent() {
                    config.temp_dir = parent_dir.to_path_buf();
                }
            }
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_partition_buffer_type_parse_str() {
        assert_eq!(
            PartitionBufferType::parse_str("memory"),
            PartitionBufferType::Memory
        );
        assert_eq!(
            PartitionBufferType::parse_str("Memory"),
            PartitionBufferType::Memory
        );
        assert_eq!(
            PartitionBufferType::parse_str("MEMORY"),
            PartitionBufferType::Memory
        );

        assert_eq!(
            PartitionBufferType::parse_str("parquet"),
            PartitionBufferType::Parquet
        );
        assert_eq!(
            PartitionBufferType::parse_str("Parquet"),
            PartitionBufferType::Parquet
        );
        assert_eq!(
            PartitionBufferType::parse_str("PARQUET"),
            PartitionBufferType::Parquet
        );

        // Test unknown/invalid values default to Memory
        assert_eq!(
            PartitionBufferType::parse_str("unknown"),
            PartitionBufferType::Memory
        );
        assert_eq!(
            PartitionBufferType::parse_str(""),
            PartitionBufferType::Memory
        );
        assert_eq!(
            PartitionBufferType::parse_str("invalid"),
            PartitionBufferType::Memory
        );
    }

    #[test]
    fn test_partition_buffer_config_default() {
        let config = PartitionBufferConfig::default();
        assert_eq!(config.buffer_type, PartitionBufferType::Memory);
        assert_eq!(
            config.rows_per_partition_threshold,
            ROWS_PER_PARTITION_BUFFER
        );
        assert_eq!(config.temp_dir, PathBuf::from(spice_data_base_path()));
    }

    #[test]
    fn test_config_from_params_empty() {
        let config = PartitionBufferConfig::from_params(None);
        assert_eq!(config.buffer_type, PartitionBufferType::Memory);
        assert_eq!(
            config.rows_per_partition_threshold,
            ROWS_PER_PARTITION_BUFFER
        );
        assert_eq!(config.temp_dir, PathBuf::from(spice_data_base_path()));
    }

    #[test]
    fn test_config_from_params_parquet_buffer() {
        let mut params = HashMap::new();
        params.insert(
            "partitioned_write_buffer".to_string(),
            "parquet".to_string(),
        );

        let config = PartitionBufferConfig::from_params(Some(&params));
        assert_eq!(config.buffer_type, PartitionBufferType::Parquet);
        assert_eq!(
            config.rows_per_partition_threshold,
            ROWS_PER_PARTITION_BUFFER
        );
    }

    #[test]
    fn test_config_from_params_custom_threshold() {
        let mut params = HashMap::new();
        params.insert(
            "duckdb_partitioned_write_flush_threshold_rows".to_string(),
            "50000".to_string(),
        );

        let config = PartitionBufferConfig::from_params(Some(&params));
        assert_eq!(config.rows_per_partition_threshold, 50000);
        assert_eq!(config.buffer_type, PartitionBufferType::Memory);
    }

    #[test]
    fn test_config_from_params_invalid_threshold() {
        let mut params = HashMap::new();
        params.insert(
            "duckdb_partitioned_write_flush_threshold_rows".to_string(),
            "not_a_number".to_string(),
        );

        let config = PartitionBufferConfig::from_params(Some(&params));
        // Should fallback to default when invalid
        assert_eq!(
            config.rows_per_partition_threshold,
            ROWS_PER_PARTITION_BUFFER
        );
    }

    #[test]
    fn test_config_from_params_duckdb_data_dir() {
        let mut params = HashMap::new();
        params.insert(
            "duckdb_data_dir".to_string(),
            "/custom/data/dir".to_string(),
        );

        let config = PartitionBufferConfig::from_params(Some(&params));
        assert_eq!(config.temp_dir, PathBuf::from("/custom/data/dir"));
    }

    #[test]
    fn test_config_from_params_duckdb_file() {
        let mut params = HashMap::new();
        params.insert(
            "duckdb_file".to_string(),
            "/path/to/database/my.duckdb".to_string(),
        );

        let config = PartitionBufferConfig::from_params(Some(&params));
        assert_eq!(config.temp_dir, PathBuf::from("/path/to/database"));
    }

    #[test]
    fn test_config_from_params_duckdb_file_root() {
        let mut params = HashMap::new();
        params.insert("duckdb_file".to_string(), "/my.duckdb".to_string());

        let config = PartitionBufferConfig::from_params(Some(&params));
        assert_eq!(config.temp_dir, PathBuf::from("/"));
    }

    #[test]
    fn test_config_from_params_priority_data_dir_over_file() {
        let mut params = HashMap::new();
        params.insert(
            "duckdb_data_dir".to_string(),
            "/priority/data/dir".to_string(),
        );
        params.insert(
            "duckdb_file".to_string(),
            "/secondary/path/db.duckdb".to_string(),
        );

        let config = PartitionBufferConfig::from_params(Some(&params));
        // duckdb_data_dir should take priority over duckdb_file
        assert_eq!(config.temp_dir, PathBuf::from("/priority/data/dir"));
    }
}
