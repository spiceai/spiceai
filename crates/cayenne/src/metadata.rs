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
use datafusion_table_providers::util::on_conflict::OnConflict;
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
    /// Configured on-conflict behavior for primary key uniqueness enforcement.
    pub on_conflict: Option<OnConflict>,
    /// Current snapshot ID (`UUIDv7`, changes on overwrite/delete operations)
    /// All tables are created with an initial snapshot.
    pub current_snapshot_id: String,
    /// Partition column name (if this is a partitioned table)
    pub partition_column: Option<String>,
    /// Vortex encoding configuration for this table
    pub vortex_config: VortexConfig,
    /// Current sequence number for ordering operations (Iceberg-style).
    ///
    /// Monotonically increasing counter used to order deletes and inserts.
    /// When data is inserted, it gets the current sequence number.
    /// When a delete is written, it also gets the current sequence number.
    /// A delete only applies to data with `data_sequence < delete_sequence`.
    ///
    /// This enables upsert semantics: if a PK is deleted and then re-inserted,
    /// the new insert has a higher sequence than the delete, so the delete
    /// doesn't apply to the new data.
    pub current_sequence_number: i64,
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
    /// Sequence number when this data file was written.
    /// Used for ordering deletions: a deletion only applies to data files with
    /// `sequence_number` <= the delete file's `sequence_number`.
    pub sequence_number: i64,
}

/// The type of deletion vector: position-based or key-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeletionType {
    /// Position-based deletion using row IDs (for tables without primary key).
    /// Requires consistent ordering between delete and read operations.
    #[default]
    PositionBased,
    /// Key-based deletion using primary key bytes (for tables with primary key).
    /// Position-independent, survives data reorganization.
    KeyBased,
}

/// Represents a deletion vector file tracking deleted rows.
#[derive(Debug, Clone)]
pub struct DeleteFile {
    /// Unique identifier for this delete file
    pub delete_file_id: i64,
    /// Table this delete file belongs to
    pub table_id: i64,
    /// Path of the data file this deletion vector applies to (for position-based deletions).
    /// `None` for key-based deletions which apply to the entire table.
    /// For position-based deletions, row IDs are relative to this specific data file.
    pub source_data_file_path: Option<String>,
    /// Path to the delete file (Arrow IPC format)
    pub path: String,
    /// Whether the path is relative
    pub path_is_relative: bool,
    /// Format of the delete file (always `arrow_ipc`)
    pub format: String,
    /// Number of deleted rows in this file
    pub delete_count: i64,
    /// Size of the file in bytes
    pub file_size_bytes: i64,
    /// The type of deletion vector (position-based or key-based).
    /// Inferred from the file schema when read, or set when writing.
    pub deletion_type: DeletionType,
    /// Sequence number for ordering deletes (Iceberg-style).
    ///
    /// A delete only applies to data files whose `data_sequence_number` is
    /// strictly less than this delete's `sequence_number`. This enables
    /// upsert semantics without anti-deletion tracking:
    /// - New inserts get higher sequence numbers
    /// - Old deletes don't apply to new data with the same PK
    pub sequence_number: i64,
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
    /// Maximum number of concurrent file uploads when writing multiple Vortex files.
    /// Each file uses multipart uploads internally via `object_store`.
    /// Defaults to 4 for balanced I/O throughput vs resource usage.
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
}

const fn default_upload_concurrency() -> usize {
    4
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
            // 4 concurrent uploads balances throughput vs resource usage
            upload_concurrency: 4,
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
    /// Optional on-conflict behavior for enforcing primary key uniqueness.
    pub on_conflict: Option<OnConflict>,
    /// Base path for storing table data (can be local path or S3 URL)
    pub base_path: String,
    /// Optional partition column name (for partitioned tables)
    pub partition_column: Option<String>,
    /// Vortex encoding configuration
    pub vortex_config: VortexConfig,
}

/// Configuration for an external object store (e.g., S3).
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// The object store URL (e.g., `s3://bucket-name/prefix/`)
    pub url: url::Url,
    /// The object store implementation
    pub store: std::sync::Arc<dyn object_store::ObjectStore>,
}
