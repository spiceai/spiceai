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

//! Data structures for Cayenne metadata.

use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};

/// Metadata about a table in the catalog.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Unique identifier for this table
    pub table_id: i64,
    /// UUID for this table (for external references)
    pub table_uuid: String,
    /// Name of the table
    pub table_name: String,
    /// Path to the table's data directory
    pub path: String,
    /// Whether the path is relative to the catalog base
    pub path_is_relative: bool,
    /// Arrow schema for this table
    pub schema: SchemaRef,
    /// Primary key columns (for deletion vector support)
    pub primary_key: Vec<String>,
    /// Current snapshot ID (`UUIDv7`, changes on overwrite/delete operations)
    /// All tables are created with an initial snapshot.
    pub current_snapshot_id: String,
    /// Partition column name (if this is a partitioned table)
    pub partition_column: Option<String>,
    /// Vortex encoding configuration for this table
    pub vortex_config: VortexConfig,
}

/// Represents a data file containing table rows.
///
/// In Cayenne, a "file" is actually a virtual file represented by a Vortex `ListingTable`
/// at a unique directory. The `path` field points to the directory containing the
/// `ListingTable`'s Vortex files. All operations (read, append, stats) delegate to the
/// corresponding `ListingTable`.
#[derive(Debug, Clone)]
pub struct DataFile {
    /// Unique identifier for this data file
    pub data_file_id: i64,
    /// Table this file belongs to
    pub table_id: i64,
    /// Partition this file belongs to (None for non-partitioned tables)
    pub partition_id: Option<i64>,
    /// Ordering of this file within the table
    pub file_order: i64,
    /// Path to the directory containing the `ListingTable`'s Vortex files
    /// This is the "virtual file" - a directory managed by a Vortex `ListingTable`
    pub path: String,
    /// Whether the path is relative to the table's base path
    pub path_is_relative: bool,
    /// File format (always "vortex" for Cayenne)
    pub file_format: String,
    /// Number of records in this virtual file (cached from `ListingTable` stats)
    pub record_count: i64,
    /// Total size of all Vortex files in the `ListingTable` directory
    pub file_size_bytes: i64,
    /// Starting row ID for this file (for row ID assignment)
    pub row_id_start: i64,
}

/// Represents a deletion vector file tracking deleted rows.
#[derive(Debug, Clone)]
pub struct DeleteFile {
    /// Unique identifier for this delete file
    pub delete_file_id: i64,
    /// Table this delete file belongs to
    pub table_id: i64,
    /// Path to the delete file (Parquet format)
    pub path: String,
    /// Whether the path is relative
    pub path_is_relative: bool,
    /// Format of the delete file (always "parquet")
    pub format: String,
    /// Number of deleted rows in this file
    pub delete_count: i64,
    /// Size of the file in bytes
    pub file_size_bytes: i64,
}

/// Metadata about a partition in a table.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Unique identifier for this partition
    pub partition_id: i64,
    /// Table this partition belongs to
    pub table_id: i64,
    /// Name of the partition column
    pub partition_column: String,
    /// Partition value (serialized as string for storage)
    pub partition_value: String,
    /// Path to the partition's data directory
    pub path: String,
    /// Whether the path is relative to the table's base path
    pub path_is_relative: bool,
    /// Total number of records in this partition
    pub record_count: i64,
    /// Total size of data files in this partition (bytes)
    pub file_size_bytes: i64,
}

/// Which compression strategy to use for the Vortex layout.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum CompressionStrategy {
    /// Uses the default Vortex Btrblocks compression.
    #[default]
    Btrblocks,
    /// Uses the Vortex `CompactCompressor` with Zstd compression.
    Zstd,
}

/// Configuration for Vortex encodings to optimize compression and performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VortexConfig {
    /// Footer cache size in MB
    pub footer_cache_mb: usize,
    /// Segment cache size in MB
    pub segment_cache_mb: usize,
    /// Target size for individual Vortex files in MB. When writes exceed this size,
    /// a new Vortex file will be created in the same listing directory. This allows
    /// for better parallelism and more granular statistics for query optimization.
    /// Defaults to 128 MB.
    pub target_vortex_file_size_mb: usize,
    /// Columns to sort data by on refresh operations (empty = no sorting)
    pub sort_columns: Vec<String>,
    /// Compression strategy to use for Vortex files
    /// Defaults to Btrblocks
    pub compression_strategy: CompressionStrategy,
}

impl Default for VortexConfig {
    fn default() -> Self {
        Self {
            // Larger caches improve read performance
            footer_cache_mb: 128,
            segment_cache_mb: 256,
            // Smaller files = better parallelism and predicate pushdown
            target_vortex_file_size_mb: 128,
            // No sort columns by default
            sort_columns: Vec::new(),
            compression_strategy: CompressionStrategy::default(),
        }
    }
}

/// Options for creating a new Cayenne table.
#[derive(Debug, Clone)]
pub struct CreateTableOptions {
    /// Name of the table
    pub table_name: String,
    /// Schema for the table
    pub schema: SchemaRef,
    /// Primary key columns (for deletion vector support)
    pub primary_key: Vec<String>,
    /// Base path for storing table data
    pub base_path: String,
    /// Optional partition column name (for partitioned tables)
    pub partition_column: Option<String>,
    /// Vortex encoding configuration
    pub vortex_config: VortexConfig,
}
