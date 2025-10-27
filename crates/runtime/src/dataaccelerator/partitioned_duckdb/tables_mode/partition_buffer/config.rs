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

//! Configuration types for partition buffers.

use std::{collections::HashMap, fmt, path::PathBuf};

use crate::spice_data_base_path;

// Buffering rows allows for much more efficient writes in `DuckDB`
// 122_880 represents DuckDB default size of groups of rows - that are stored together at the storage level.
const ROWS_PER_PARTITION_BUFFER: usize = 122_880;

/// Configuration for partition buffer type selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionBufferType {
    /// Use in-memory buffers (existing behavior)
    Memory,
    /// Use Parquet file-based buffers for better memory efficiency
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
                tracing::warn!("Unrecognized partition buffer type '{s}', defaulting to 'memory'");
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

impl Default for PartitionBufferType {
    fn default() -> Self {
        Self::Memory
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
        let mut config = PartitionBufferConfig {
            ..Default::default()
        };

        if let Some(params) = params {
            if let Some(rows_threshold_str) = params.get("duckdb_partitioned_write_flush_threshold")
            {
                if let Ok(threshold) = rows_threshold_str.parse::<usize>() {
                    config.rows_per_partition_threshold = threshold;
                } else {
                    tracing::warn!(
                        "Invalid `duckdb_partitioned_write_flush_threshold` parameter '{rows_threshold_str}': must be a positive integer"
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
