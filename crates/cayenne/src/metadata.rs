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

/// Default maximum number of rows to inline in the metastore instead of writing a Vortex file.
pub const DEFAULT_INLINE_MAX_ROWS: usize = 1024;
/// Default maximum serialized IPC size in bytes for a single inlined entry.
pub const DEFAULT_INLINE_MAX_BYTES: usize = 1_048_576;
/// Default maximum in-memory byte budget while buffering an inline fast-path stream.
pub const DEFAULT_INLINE_MAX_BUFFER_BYTES: usize = 4 * 1_048_576;
/// Default maximum rows to keep inline before flushing to Vortex.
pub const DEFAULT_INLINE_FLUSH_MAX_ROWS: i64 = 10_000;
/// Default maximum inline entries before flushing to Vortex.
pub const DEFAULT_INLINE_FLUSH_MAX_SEGMENTS: i64 = 64;
/// Default maximum serialized IPC bytes to keep inline before flushing to Vortex.
pub const DEFAULT_INLINE_FLUSH_MAX_BYTES: i64 = 8 * 1_048_576;

/// Metadata about a table in the catalog.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Unique identifier for this table (`UUIDv7`)
    pub table_id: String,
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
    /// Table this file belongs to (`UUIDv7`)
    pub table_id: String,
    /// Partition this file belongs to (None for non-partitioned tables)
    pub partition_id: Option<String>,
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
    /// Unique identifier for this delete file (`UUIDv7`)
    pub delete_file_id: String,
    /// Table this delete file belongs to (`UUIDv7`)
    pub table_id: String,
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
///
/// Supports both single and composite partition keys (e.g., `partition_by: [year, month, day]`).
/// Partition columns and values are stored as ordered lists, where the i-th column name
/// corresponds to the i-th partition value.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Unique identifier for this partition (`UUIDv7`)
    pub partition_id: String,
    /// Table this partition belongs to (`UUIDv7`)
    pub table_id: String,
    /// Names of the partition columns (ordered).
    /// For a single partition column, this is a single-element vector.
    /// For composite partitions like `partition_by: [year, month]`, this contains
    /// all column names in order: `["year", "month"]`.
    pub partition_columns: Vec<String>,
    /// Partition values (serialized as strings, ordered to match `partition_columns`).
    /// For a single partition, this is a single-element vector.
    /// For composite partitions, values are ordered to match columns:
    /// e.g., `["2025", "10"]` for year=2025, month=10.
    pub partition_values: Vec<String>,
    /// Path to the partition's data directory
    pub path: String,
    /// Whether the path is relative to the table's base path
    pub path_is_relative: bool,
    /// Total number of records in this partition
    pub record_count: i64,
    /// Total size of data files in this partition (bytes)
    pub file_size_bytes: i64,
}

impl PartitionMetadata {
    /// Returns a composite key string for this partition.
    ///
    /// For single partitions: returns the single value (e.g., `"us-east-1"`).
    /// For composite partitions: returns a slash-separated path (e.g., `"2025/10/15"`).
    ///
    /// This key uniquely identifies the partition within a table and is used
    /// for `HashMap` lookups and Hive-style directory naming.
    #[must_use]
    pub fn composite_key(&self) -> String {
        self.partition_values.join("/")
    }

    /// Creates a new `PartitionMetadata` for a single partition column (legacy compatibility).
    #[must_use]
    pub fn new_single(
        table_id: String,
        partition_column: String,
        partition_value: String,
        path: String,
        path_is_relative: bool,
    ) -> Self {
        Self {
            partition_id: String::new(),
            table_id,
            partition_columns: vec![partition_column],
            partition_values: vec![partition_value],
            path,
            path_is_relative,
            record_count: 0,
            file_size_bytes: 0,
        }
    }

    /// Creates a new `PartitionMetadata` for composite partition columns.
    #[must_use]
    pub fn new_composite(
        table_id: String,
        partition_columns: Vec<String>,
        partition_values: Vec<String>,
        path: String,
        path_is_relative: bool,
    ) -> Self {
        Self {
            partition_id: String::new(),
            table_id,
            partition_columns,
            partition_values,
            path,
            path_is_relative,
            record_count: 0,
            file_size_bytes: 0,
        }
    }
}

/// Which compression strategy to use for the Vortex layout.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionStrategy {
    /// Uses the default Vortex Btrblocks compression.
    #[default]
    Btrblocks,
    /// Uses the Vortex `CompactCompressor` with Zstd compression.
    Zstd,
}

/// Primary-key conflict detection behavior for Cayenne inserts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PkConflictDetection {
    /// Build a PK keyset and apply configured `on_conflict` behavior.
    #[default]
    Auto,
    /// Append without scanning existing PKs. The source must enforce PK uniqueness,
    /// and the ingestion path must not replay rows across bootstrap/WAL boundaries.
    None,
}

impl PkConflictDetection {
    /// Parse a spicepod parameter value.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "none" => Some(Self::None),
            _ => None,
        }
    }

    /// Return the spicepod/config string for this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
        }
    }
}

/// Configuration for Vortex encodings to optimize compression and performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VortexConfig {
    /// Runtime-global footer metadata cache size in MB, when explicitly configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_mb: Option<usize>,
    /// Segment cache size in MB.
    ///
    /// Currently ignored because the current Vortex `DataFusion` API does not expose
    /// segment cache sizing.
    pub segment_cache_mb: usize,
    /// Target size for individual Vortex files in MB. When writes exceed this size,
    /// a new Vortex file will be created in the same listing directory. This allows
    /// for better parallelism and more granular statistics for query optimization.
    /// Defaults to 256 MB.
    pub target_vortex_file_size_mb: usize,
    /// Columns to sort data by on refresh operations (empty = no sorting)
    pub sort_columns: Vec<String>,
    /// Compression strategy to use for Vortex files
    /// Defaults to Btrblocks
    pub compression_strategy: CompressionStrategy,
    /// Maximum number of concurrent file uploads when writing multiple Vortex files.
    /// Each file uses multipart uploads internally via `object_store`.
    /// Defaults to the available CPU parallelism.
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
    /// Optional override for writer partitions when ingesting unsorted data into a snapshot.
    /// When unset, writes use the current `DataFusion` session target partition count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_concurrency: Option<usize>,
    /// Minimum number of "small" Vortex files that must accumulate in the current
    /// snapshot before tiered compaction is eligible to run. Files are classified
    /// as "small" when their size is below `target_vortex_file_size_mb / 4`. The
    /// compactor also requires that the eligible tier's total size meets the
    /// per-tier target before rewriting the current snapshot (see
    /// [`crate::provider::compaction`]).
    ///
    /// Defaults to 8.
    #[serde(default = "default_compaction_trigger_files")]
    pub compaction_trigger_files: usize,
    /// Number of protected snapshots that can accumulate before snapshot-maintenance
    /// compaction is eligible to run. Kept separate from `compaction_trigger_files`
    /// so small-file compaction tuning does not silently change scan amplification
    /// behavior for protected snapshots.
    ///
    /// Defaults to 8.
    #[serde(default = "default_compaction_trigger_protected_snapshots")]
    pub compaction_trigger_protected_snapshots: usize,
    /// Maximum age in milliseconds of the oldest protected snapshot before
    /// snapshot-maintenance compaction is eligible to run. This bounds how long
    /// low-volume update/delete workloads can keep extra protected snapshots
    /// attached to every scan when they do not reach the count trigger. Set to
    /// 0 to disable the age trigger.
    ///
    /// Defaults to 300,000 ms (5 minutes).
    #[serde(default = "default_compaction_trigger_snapshot_age_ms")]
    pub compaction_trigger_snapshot_age_ms: u64,
    /// Maximum number of consecutive compaction passes that a single trigger can
    /// run. Each pass picks the smallest eligible tier and rewrites a single
    /// snapshot. Capping this avoids unbounded write amplification when the
    /// picker would keep finding work after each promotion.
    ///
    /// Defaults to 3.
    #[serde(default = "default_compaction_max_levels")]
    pub compaction_max_levels: usize,
    /// Maximum number of eligible file paths the picker retains in a single
    /// compaction candidate. The current runner uses the candidate as a trigger
    /// and observability signal, then rewrites the whole current snapshot; this
    /// setting does not bound rewrite IO or memory.
    ///
    /// Defaults to 32.
    #[serde(default = "default_compaction_max_files_per_pick")]
    pub compaction_max_files_per_pick: usize,
    /// Background compaction interval in milliseconds. The accelerator spawns a
    /// per-table background task that calls the compactor every interval. Set to
    /// 0 to disable the background task — inline compaction on writes still runs.
    ///
    /// Defaults to `30_000` ms.
    #[serde(default = "default_compaction_background_interval_ms")]
    pub compaction_background_interval_ms: u64,
    /// Maximum rows in a single write that can be inlined directly into the metastore.
    /// Set to 0 to disable write-entry inlining.
    #[serde(default = "default_inline_max_rows")]
    pub inline_max_rows: usize,
    /// Maximum serialized Arrow IPC bytes in a single inlined metastore entry.
    /// Set to 0 to disable write-entry inlining.
    #[serde(default = "default_inline_max_bytes")]
    pub inline_max_bytes: usize,
    /// Maximum Arrow in-memory bytes to buffer while deciding whether to inline a write.
    /// Set to 0 to force the normal Vortex write path after the first buffered batch.
    #[serde(default = "default_inline_max_buffer_bytes")]
    pub inline_max_buffer_bytes: usize,
    /// Maximum inline rows before checkpointing inline data to Vortex.
    #[serde(
        default = "default_inline_flush_max_rows",
        alias = "inline_memtable_max_rows"
    )]
    pub inline_flush_max_rows: i64,
    /// Maximum inline entries before checkpointing inline data to Vortex.
    #[serde(
        default = "default_inline_flush_max_segments",
        alias = "inline_memtable_max_segments"
    )]
    pub inline_flush_max_segments: i64,
    /// Maximum inline IPC bytes before checkpointing inline data to Vortex.
    #[serde(
        default = "default_inline_flush_max_bytes",
        alias = "inline_memtable_max_bytes"
    )]
    pub inline_flush_max_bytes: i64,
    /// Whether inserts should scan existing data for primary-key conflicts. Set to `none` only
    /// when the source enforces PK uniqueness and ingestion cannot replay existing rows.
    #[serde(default)]
    pub pk_conflict_detection: PkConflictDetection,
}

fn default_concurrency() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

fn default_upload_concurrency() -> usize {
    default_concurrency()
}

fn default_compaction_trigger_files() -> usize {
    8
}

fn default_compaction_trigger_protected_snapshots() -> usize {
    8
}

fn default_compaction_trigger_snapshot_age_ms() -> u64 {
    300_000
}

fn default_compaction_max_levels() -> usize {
    3
}

fn default_compaction_max_files_per_pick() -> usize {
    32
}

fn default_compaction_background_interval_ms() -> u64 {
    30_000
}

fn default_inline_max_rows() -> usize {
    DEFAULT_INLINE_MAX_ROWS
}

fn default_inline_max_bytes() -> usize {
    DEFAULT_INLINE_MAX_BYTES
}

fn default_inline_max_buffer_bytes() -> usize {
    DEFAULT_INLINE_MAX_BUFFER_BYTES
}

fn default_inline_flush_max_rows() -> i64 {
    DEFAULT_INLINE_FLUSH_MAX_ROWS
}

fn default_inline_flush_max_segments() -> i64 {
    DEFAULT_INLINE_FLUSH_MAX_SEGMENTS
}

fn default_inline_flush_max_bytes() -> i64 {
    DEFAULT_INLINE_FLUSH_MAX_BYTES
}

impl Default for VortexConfig {
    fn default() -> Self {
        Self {
            footer_cache_mb: None,
            segment_cache_mb: 256,
            // Balanced file size for scan throughput and write amplification
            target_vortex_file_size_mb: 256,
            // No sort columns by default
            sort_columns: Vec::new(),
            compression_strategy: CompressionStrategy::default(),
            upload_concurrency: default_upload_concurrency(),
            write_concurrency: None,
            compaction_trigger_files: default_compaction_trigger_files(),
            compaction_trigger_protected_snapshots: default_compaction_trigger_protected_snapshots(
            ),
            compaction_trigger_snapshot_age_ms: default_compaction_trigger_snapshot_age_ms(),
            compaction_max_levels: default_compaction_max_levels(),
            compaction_max_files_per_pick: default_compaction_max_files_per_pick(),
            compaction_background_interval_ms: default_compaction_background_interval_ms(),
            inline_max_rows: default_inline_max_rows(),
            inline_max_bytes: default_inline_max_bytes(),
            inline_max_buffer_bytes: default_inline_max_buffer_bytes(),
            inline_flush_max_rows: default_inline_flush_max_rows(),
            inline_flush_max_segments: default_inline_flush_max_segments(),
            inline_flush_max_bytes: default_inline_flush_max_bytes(),
            pk_conflict_detection: PkConflictDetection::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PkConflictDetection, VortexConfig};

    #[test]
    fn test_concurrency_defaults_use_available_parallelism_where_global() {
        let available_parallelism =
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let config = VortexConfig::default();

        assert_eq!(config.upload_concurrency, available_parallelism);
        assert_eq!(config.write_concurrency, None);
        assert_eq!(config.pk_conflict_detection, PkConflictDetection::Auto);
    }

    #[test]
    fn test_vortex_config_deserializes_pk_conflict_detection_default() {
        let config: VortexConfig = serde_json::from_str("{}").expect("valid empty config");

        assert_eq!(config.pk_conflict_detection, PkConflictDetection::Auto);
    }

    #[test]
    fn test_pk_conflict_detection_parse() {
        assert_eq!(
            PkConflictDetection::parse("auto"),
            Some(PkConflictDetection::Auto)
        );
        assert_eq!(
            PkConflictDetection::parse("none"),
            Some(PkConflictDetection::None)
        );
        assert_eq!(PkConflictDetection::parse("invalid"), None);
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

/// Table-level statistics stored as a serialized Vortex [`FileStatistics`] blob.
///
/// Stores per-column statistics (min, max, null count, sum, etc.) captured from
/// the most recent write (the write's `ColumnStatsAccumulator`). The row in
/// `cayenne_table_statistics` is keyed by `table_id` and upserted on every write,
/// so entries represent last-write-wins snapshots rather than aggregates across
/// every file ever produced; a future change will merge new writes into the
/// existing blob.
///
/// Consumers should treat these values as optimization hints only. Uses Vortex's
/// native statistics format for zero-conversion overhead and compatibility with
/// the Vortex file footer statistics.
///
/// [`FileStatistics`]: vortex::file::FileStatistics
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Table this stats entry belongs to (`UUIDv7`)
    pub table_id: String,
    /// Serialized Vortex `FileStatistics` flatbuffer bytes. Today the write
    /// path populates only min, max, and null count per column; other fields
    /// supported by the Vortex format (sum, NaN count, `is_constant`, etc.)
    /// remain `Absent` until future writer work fills them in.
    pub statistics_blob: Vec<u8>,
    /// Row count captured by the most recent write's accumulator (not an
    /// aggregate across every file ever produced — see the struct docs).
    pub num_rows: i64,
}

/// A small batch of insert data inlined directly in the metastore.
///
/// For streaming workloads that produce many tiny writes, storing data as
/// Arrow IPC blobs in the catalog avoids the overhead of creating individual
/// Vortex files. A checkpoint operation flushes accumulated inline data to
/// consolidated Vortex files when the total size exceeds a threshold.
#[derive(Debug, Clone)]
pub struct InlinedData {
    /// Unique identifier for this inlined entry (`UUIDv7`)
    pub inlined_id: String,
    /// Table this data belongs to (`UUIDv7`)
    pub table_id: String,
    /// Partition key (for partitioned tables), `None` for non-partitioned
    pub partition_key: Option<String>,
    /// Arrow IPC serialized `RecordBatch`
    pub data_ipc: Vec<u8>,
    /// Number of rows in this batch
    pub record_count: i64,
    /// Sequence number when this data was inlined
    pub sequence_number: i64,
    /// ISO 8601 timestamp of when this entry was created
    pub created_at: String,
}

/// Aggregate size information for inline data entries in the metastore.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InlinedDataStats {
    /// Total number of visible rows represented by inline entries.
    pub record_count: i64,
    /// Number of inline entries for the table.
    pub entry_count: i64,
    /// Total serialized Arrow IPC bytes stored inline.
    pub ipc_bytes: i64,
}

impl InlinedData {
    /// Build an inline data row whose identity, sequence number, and timestamp
    /// are assigned by `MetadataCatalog::commit_inlined_mutation`.
    pub(crate) fn pending_catalog_insert(
        table_id: String,
        partition_key: Option<String>,
        data_ipc: Vec<u8>,
        record_count: i64,
    ) -> Self {
        Self {
            inlined_id: String::new(),
            table_id,
            partition_key,
            data_ipc,
            record_count,
            sequence_number: 0,
            created_at: String::new(),
        }
    }
}

/// A small batch of delete identifiers inlined in the metastore.
///
/// Mirrors `InlinedData` but for deletions. Stores deletion identifiers
/// (row IDs or primary key bytes) as Arrow IPC blobs.
#[derive(Debug, Clone)]
pub struct InlinedDelete {
    /// Unique identifier for this inlined entry (`UUIDv7`)
    pub inlined_id: String,
    /// Table this delete belongs to (`UUIDv7`)
    pub table_id: String,
    /// Arrow IPC serialized deletion identifiers
    pub delete_ipc: Vec<u8>,
    /// Number of deleted rows in this batch
    pub delete_count: i64,
    /// Sequence number when this delete was inlined
    pub sequence_number: i64,
    /// ISO 8601 timestamp
    pub created_at: String,
}

/// Configuration for an external object store (e.g., S3).
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// The object store URL (e.g., `s3://bucket-name/prefix/`)
    pub url: url::Url,
    /// The object store implementation
    pub store: std::sync::Arc<dyn object_store::ObjectStore>,
}
