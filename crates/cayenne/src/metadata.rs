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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Which compression strategy the table's FULL encoding tier uses — i.e.
/// maintenance writes (compaction outputs, rewrites, overwrites) and delta
/// writes that resolve to a full level (`7..=10`, or `auto` on large /
/// unknown-size writes). Light delta levels (`0..=6`) are a fixed
/// `BtrBlocks`-subset effort ladder and are not affected by this choice.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionStrategy {
    /// The default Vortex `BtrBlocks` cascading scheme search.
    #[default]
    Btrblocks,
    /// The default cascade PLUS the Zstd string schemes
    /// (`StringCode::Zstd` / `ZstdBuffers`) in the search — the encoder picks
    /// them when they beat FSST/dict on a column. Integer/float columns are
    /// unchanged (Vortex has no zstd schemes for them at this revision).
    /// Trades encode CPU and string-decode speed for better ratios on
    /// long/high-entropy strings.
    Zstd,
}

/// Encoding effort for *delta* writes — fresh CDC/append snapshot files that
/// the tiered compactor later folds into properly-encoded files.
///
/// zstd-style level scale (`cayenne_delta_encoding` param):
///
/// - `auto` (default) — size-gated: a write smaller than a quarter of the
///   target file size encodes at a light level (the file is transient by
///   definition — compaction exists to fold it); larger or unknown-size
///   writes use the full default encoding.
/// - `0` — no compression (canonical arrays; cheapest encode).
/// - `1`–`6` — progressively richer scheme sets. The cheap levels skip the
///   per-file encoder-strategy search and FSST symbol-table training.
/// - `7`–`10` — the full default `BtrBlocks` cascade: byte-for-byte the
///   pre-feature write behavior (`7` is the explicit opt-out of `auto`;
///   `8`–`10` are reserved for future heavier-effort search).
///
/// Maintenance writes (compaction outputs, sorted rewrites, overwrites) are
/// NOT affected by this setting — they always use the full default encoding,
/// because their output is the long-lived artifact whose encoding quality
/// pays for scan throughput and storage footprint.
///
/// The exact level → scheme-set mapping lives in
/// `provider::delta_encoding::strategy_builder_for_level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum DeltaEncoding {
    /// Size-gated: light for small deltas, full for large writes.
    Auto,
    /// Fixed encoding level `0..=10` applied to every delta write.
    Level(u8),
}

impl Default for DeltaEncoding {
    /// `Auto` — size-gated light encoding for small deltas. This is also what
    /// pre-feature stored table configs deserialize to via
    /// `#[serde(default)]`, so existing tables pick up the policy on upgrade
    /// (write-time only; existing data files are unaffected and a level
    /// change never forces a table re-create). Set the
    /// `cayenne_delta_encoding` param to `7` to opt out (the full default
    /// cascade, byte-for-byte the pre-feature behavior).
    fn default() -> Self {
        Self::Auto
    }
}

/// Maximum supported [`DeltaEncoding`] level.
pub const DELTA_ENCODING_MAX_LEVEL: u8 = 10;

/// First level that maps to the full default `BtrBlocks` cascade (levels
/// `7..=10` are all "full" today). Shared with
/// `provider::delta_encoding::FULL_LEVEL` so the two can't drift.
pub const DELTA_ENCODING_FULL_LEVEL: u8 = 7;

impl std::fmt::Display for DeltaEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Level(level) => write!(f, "{level}"),
        }
    }
}

impl std::str::FromStr for DeltaEncoding {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("auto") {
            return Ok(Self::Auto);
        }
        let level: u8 = trimmed.parse().map_err(|_| {
            format!(
                "invalid delta encoding '{value}': expected 'auto' or a level 0..={DELTA_ENCODING_MAX_LEVEL}"
            )
        })?;
        if level > DELTA_ENCODING_MAX_LEVEL {
            return Err(format!(
                "invalid delta encoding level {level}: maximum is {DELTA_ENCODING_MAX_LEVEL}"
            ));
        }
        Ok(Self::Level(level))
    }
}

impl TryFrom<String> for DeltaEncoding {
    type Error = String;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<DeltaEncoding> for String {
    fn from(value: DeltaEncoding) -> Self {
        value.to_string()
    }
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

/// How Cayenne records and applies primary-key deletions for upsert/delete on
/// tables that have a primary key (`Int64Pk` / `RowConverterBased` strategies).
///
/// PK-less tables always use position-based deletion regardless of this setting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeletionMode {
    /// Resolve the mode from the table's configuration (the default). `Auto`
    /// resolves to [`Self::Position`] — the merge-on-read path — for every table:
    /// a primary-key table captures positions via the `row_idx()` read-back
    /// (with key-based fallback for not-yet-captured rows), and a PK-less table
    /// uses the long-standing `PositionBased` strategy. The presence of a primary
    /// key selects the *mechanism*; the resolved *mode* is position either way.
    /// Use [`Self::Key`] to explicitly opt out. See [`Self::resolved`].
    #[default]
    Auto,
    /// Record deletions by primary-key bytes + sequence number and apply them
    /// above the Vortex scan via a `RowConverter`/`HashSet` probe per scanned
    /// row. Position-independent and reorganization-proof, but pays an
    /// O(scanned-rows) re-encode on every scan with a non-empty delete set and
    /// is invisible to Vortex page-skipping.
    Key,
    /// Record deletions as per-file row-position `RoaringBitmap`s and push them
    /// into the Vortex scan (`Selection::ExcludeRoaring`), so deleted pages are
    /// skipped at the storage layer with zero per-row CPU. Requires the writer
    /// to know each row's `(file, file-local position)`, captured via a
    /// `row_idx()` read-back after each write. Rows whose position is unknown
    /// (cold-rebuilt keysets, over-budget bloom tables, inlined rows) fall back
    /// to the `Key` path, so a table can mix both within one snapshot.
    Position,
}

impl DeletionMode {
    /// Parse a spicepod parameter value.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "key" => Some(Self::Key),
            "position" => Some(Self::Position),
            _ => None,
        }
    }

    /// Return the spicepod/config string for this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Key => "key",
            Self::Position => "position",
        }
    }

    /// Resolve `Auto` against the table's configuration, returning a concrete
    /// [`Self::Key`] or [`Self::Position`].
    ///
    /// `Auto` resolves to **position** — the merge-on-read path that pushes
    /// per-file position deletes into the Vortex scan. For a table **with** a
    /// primary key, positions are captured by the `row_idx()` read-back after
    /// each write (and any row whose position isn't yet known falls back to a
    /// key-based delete, so this is always correct — under bursty back-to-back
    /// writes the capture may not have run yet and the key path is used); for a
    /// table **without** a primary key it is the long-standing `PositionBased`
    /// strategy. So the *mechanism* is chosen by the presence of a PK, but the
    /// resolved *mode* is position either way.
    ///
    /// `Key` is the explicit opt-out (apply deletes above the scan). It only has
    /// meaning with a PK; a PK-less table can only do position-based deletion, so
    /// `Key` there resolves to `Position`.
    #[must_use]
    pub const fn resolved(self, has_primary_key: bool) -> Self {
        match self {
            // `Key` is only honored when there is a key to record.
            Self::Key if has_primary_key => Self::Key,
            // Everything else is position-based: `Auto` and `Position` always,
            // and `Key` on a PK-less table (there is no key to record).
            Self::Auto | Self::Position | Self::Key => Self::Position,
        }
    }

    /// Whether this mode (already resolved via [`Self::resolved`]) records and
    /// applies deletions as per-file row positions.
    #[must_use]
    pub const fn is_position(self) -> bool {
        matches!(self, Self::Position)
    }
}

/// Which adaptive-tunable knobs the operator pinned with an explicit value. In
/// `adaptive` mode the closed-loop controller must not move a pinned knob — its
/// tuning bounds collapse to a single point so `decide()` naturally skips it and
/// falls through to another lever. (In `auto` mode there is no loop, so an
/// explicit value is already frozen.) This is how the "override per config
/// value" mode composes with `auto`/`adaptive`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "one independent pin flag per adaptive-tunable knob"
)]
pub struct PinnedTuningKnobs {
    /// The inline-memtable flush caps were operator-set (don't adapt them).
    pub inline_flush: bool,
    /// `cayenne_compaction_background_interval_ms` was operator-set.
    pub compaction_interval: bool,
    /// `cayenne_compaction_trigger_files` was operator-set.
    pub compaction_trigger: bool,
    /// `cayenne_write_concurrency` was operator-set.
    pub write_concurrency: bool,
}

/// Configuration for Vortex encodings to optimize compression and performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VortexConfig {
    /// Runtime-global footer metadata cache size in MB, when explicitly configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_mb: Option<usize>,
    /// Shared Vortex segment cache capacity in MB.
    ///
    /// Passed through to `vortex-datafusion` as the per-format segment cache size.
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
    /// Encoding effort for delta writes (fresh CDC/append snapshot files).
    /// `auto` (default) size-gates: small deltas encode light and are folded
    /// into properly-encoded files by compaction; explicit `0..=10` pins the
    /// level (`7` = the full default cascade, the pre-feature behavior).
    /// Maintenance writes (compaction, rewrites) always use the full default
    /// encoding. See [`DeltaEncoding`].
    #[serde(default)]
    pub delta_encoding: DeltaEncoding,
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
    /// Byte budget (set in MB via `cayenne_pk_keyset_cache_mb`) for the in-memory
    /// primary-key index used to detect upsert conflicts. Within budget an exact
    /// keyset is kept. Over budget, `OnConflict::Upsert` tables fall back to a
    /// bounded bloom existence filter (O(batch) maintenance, no per-batch
    /// rebuild); `DoNothing` tables rebuild the keyset from a full-table scan on
    /// the next batch (a bloom's false positives are harmless for upsert but
    /// would wrongly drop rows under `DoNothing`). `None` uses the built-in default
    /// (256 MiB); the accelerator auto-derives a memory-aware default when the
    /// param is unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pk_keyset_cache_mb: Option<usize>,
    /// How primary-key deletions are recorded and applied for PK tables.
    /// Defaults to [`DeletionMode::Auto`], which resolves to `position`
    /// (merge-on-read): deletes are pushed into the Vortex scan as per-file
    /// row-position bitmaps, eliminating the per-row `RowConverter` deletion tax
    /// above the scan. Set `cayenne_deletion_mode: key` to opt out and keep the
    /// above-scan key-based filter.
    #[serde(default)]
    pub deletion_mode: DeletionMode,
    /// Enable the closed-loop dynamic auto-tuner (see `provider::tuning`). Set by
    /// the `cayenne_tuning` mode: `auto` (default) → `false` (static derivation
    /// only); `adaptive` → `true` (static warm-start + the closed loop). When on,
    /// a per-table controller measures the CDC ingest rate *and the runtime's
    /// whole-system response* (apply latency vs offered load, read amplification
    /// that slows queries, cgroup-aware memory pressure) and nudges the safe
    /// per-operation knobs within the environment-derived `[floor, ceiling]`.
    #[serde(default)]
    pub dynamic_tuning: bool,
    /// Adaptive-tunable knobs the operator pinned with an explicit value; the
    /// closed loop leaves these alone (see [`PinnedTuningKnobs`]).
    #[serde(default)]
    pub pinned_tuning_knobs: PinnedTuningKnobs,
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

impl VortexConfig {
    /// Surface parameter values that *parse* but won't behave as a user likely
    /// intends — out-of-range values that get silently clamped at their use site,
    /// and combinations that don't compose with each other. Returns
    /// human-readable warnings; the caller logs each with the dataset context.
    ///
    /// Pure and side-effect-free so the rules stay unit-testable. The actual
    /// clamping still happens at the use sites — this only makes it visible
    /// instead of silent. `available_cores` is the host's logical core count (the
    /// encode-shard ceiling); pass `std::thread::available_parallelism()`.
    #[must_use]
    pub fn config_warnings(&self, available_cores: usize) -> Vec<String> {
        let cores = available_cores.max(1);
        let mut warnings = Vec::new();

        // Vortex encode is CPU-bound, so `write_concurrency` above the core count
        // is capped at encode time (`VortexFormat::build_shard_spec`); the surplus
        // only inflates the per-snapshot file count (read amplification).
        if let Some(write_concurrency) = self.write_concurrency
            && write_concurrency > cores
        {
            warnings.push(format!(
                "cayenne_write_concurrency ({write_concurrency}) exceeds the host core count ({cores}); encode is CPU-bound so it is capped at {cores} — the surplus only inflates the per-snapshot file count without speeding the write. Set it to {cores} or below."
            ));
        }

        // `target_vortex_file_size_mb` feeds both the sink's size-based file
        // rolling and the size-tiered compaction picker (its small/mid tiers derive
        // from it), so 0 disables both.
        if self.target_vortex_file_size_mb == 0 {
            warnings.push(
                "cayenne_target_file_size_mb is 0, which disables size-based file rolling and the size-tiered compaction picker; compaction then relies only on the protected-snapshot count/age triggers. Set a positive size (e.g. 256) unless one file per write is intended.".to_owned(),
            );
        }

        // The picker needs at least two files in a tier to merge, so 1 is clamped
        // up to 2 — surface that rather than silently changing the value.
        if self.compaction_trigger_files == 1 {
            warnings.push(
                "cayenne_compaction_trigger_files is 1, but a single file cannot be compacted; it is clamped to a minimum of 2.".to_owned(),
            );
        }

        // Likewise the protected-snapshot subset merge needs at least two
        // snapshots; 1 is clamped up to 2 at the trigger.
        if self.compaction_trigger_protected_snapshots == 1 {
            warnings.push(
                "cayenne_compaction_trigger_protected_snapshots is 1, but a single protected snapshot cannot be merged; it is clamped to a minimum of 2.".to_owned(),
            );
        }

        // A trigger above the per-pass pick cap still fires, but each pass only
        // consolidates `max_files_per_pick` of the accumulated files, so a backlog
        // drains over several passes instead of in one.
        if self.compaction_trigger_files > self.compaction_max_files_per_pick {
            warnings.push(format!(
                "cayenne_compaction_trigger_files ({}) exceeds cayenne_compaction_max_files_per_pick ({}); each compaction pass consolidates at most {} files, so a backlog drains over several passes. Consider raising cayenne_compaction_max_files_per_pick.",
                self.compaction_trigger_files,
                self.compaction_max_files_per_pick,
                self.compaction_max_files_per_pick
            ));
        }

        warnings
    }
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
            // `auto`: size-gated light encoding for small deltas (re-encoded
            // by compaction). Local micro A/B (2026-06-06) was neutral on the
            // upsert/bulk lanes; the aggregate CPU-per-delta benefit targets
            // production-scale CDC and is to be validated there. Set the
            // param to `7` to opt out (pre-feature behavior).
            delta_encoding: DeltaEncoding::default(),
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
            pk_keyset_cache_mb: None,
            deletion_mode: DeletionMode::default(),
            dynamic_tuning: false,
            pinned_tuning_knobs: PinnedTuningKnobs::default(),
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
    fn config_warnings_clean_default_is_silent() {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        assert!(
            VortexConfig::default().config_warnings(cores).is_empty(),
            "the default config must not produce any warnings"
        );
    }

    #[test]
    fn config_warnings_flags_write_concurrency_over_cores() {
        let config = VortexConfig {
            write_concurrency: Some(64),
            ..VortexConfig::default()
        };
        assert!(
            config
                .config_warnings(8) // 64 > 8 cores
                .iter()
                .any(|w| w.contains("cayenne_write_concurrency") && w.contains("64")),
            "expected a write_concurrency-over-cores warning"
        );
        // At or below the core count is fine.
        let ok = VortexConfig {
            write_concurrency: Some(8),
            ..VortexConfig::default()
        };
        assert!(ok.config_warnings(8).is_empty());
    }

    #[test]
    fn config_warnings_flags_zero_target_file_size() {
        let config = VortexConfig {
            target_vortex_file_size_mb: 0,
            ..VortexConfig::default()
        };
        assert!(
            config
                .config_warnings(16)
                .iter()
                .any(|w| w.contains("cayenne_target_file_size_mb"))
        );
    }

    #[test]
    fn config_warnings_flags_trigger_above_pick_cap() {
        let config = VortexConfig {
            compaction_trigger_files: 64,
            compaction_max_files_per_pick: 32,
            ..VortexConfig::default()
        };
        assert!(
            config
                .config_warnings(16)
                .iter()
                .any(|w| w.contains("exceeds cayenne_compaction_max_files_per_pick"))
        );
    }

    #[test]
    fn config_warnings_flags_single_file_trigger() {
        let config = VortexConfig {
            compaction_trigger_files: 1,
            ..VortexConfig::default()
        };
        assert!(
            config
                .config_warnings(16)
                .iter()
                .any(|w| w.contains("a single file cannot be compacted"))
        );
    }

    #[test]
    fn config_warnings_flags_single_protected_snapshot_trigger() {
        let config = VortexConfig {
            compaction_trigger_protected_snapshots: 1,
            ..VortexConfig::default()
        };
        assert!(
            config
                .config_warnings(16)
                .iter()
                .any(|w| w.contains("a single protected snapshot cannot be merged"))
        );
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
/// Stores per-column statistics (min, max, null count) maintained incrementally
/// on the write path. The row in `cayenne_table_statistics` is keyed by
/// `table_id` and upserted on every write: each write's `ColumnStatsAccumulator`
/// is *merged* into the existing blob (min/max widen, null counts accumulate),
/// so the entry is a running per-table aggregate, not a last-write snapshot.
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
    /// Serialized Vortex `FileStatistics` flatbuffer bytes (per-column min, max,
    /// and null count). Other fields supported by the Vortex format (sum, NaN
    /// count, `is_constant`, etc.) remain `Absent`.
    pub statistics_blob: Vec<u8>,
    /// Live row count, maintained incrementally on commit: inserts add and
    /// supersedes/deletes subtract, so it tracks `SELECT COUNT(*)` rather than
    /// the sum of every insert ever made. Compaction and overwrite reset it to
    /// the authoritative rewritten count.
    pub num_rows: i64,
    /// Serialized per-column NDV (distinct-count) `HyperLogLog` sketches
    /// ([`crate::hll::NdvSketches`]), `None` when no integer column has a sketch.
    /// Merged across writes register-wise; used to size distributed joins on
    /// sparse integer keys. See [`crate::hll`].
    pub ndv_sketches: Option<Vec<u8>>,
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
    /// Durable per-tombstone activation flag.
    ///
    /// A staged inline-conflict upsert writes its tombstone with `published =
    /// false` at a `sequence_number` reserved below the staged snapshot's
    /// `snapshot_sequence`. The read filter (`load_inlined_deletion_maps`)
    /// applies the tombstone ONLY when this is `true`, so a durable-but-inactive
    /// tombstone observed by an inline-cache rebuild (which a concurrent
    /// same-table inline INSERT can trigger) before the owning snapshot
    /// publishes cannot hide the old inline row — eliminating the transient
    /// vanish that a global watermark could not (advance ⇒ HIDE polarity). The
    /// owning snapshot's finalize flips this durably to `true`
    /// (`MetadataCatalog::mark_inlined_delete_published`) before its replacement
    /// rows become discoverable, and only the inline checkpoint clears it.
    pub published: bool,
}

/// Configuration for an external object store (e.g., S3).
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// The object store URL (e.g., `s3://bucket-name/prefix/`)
    pub url: url::Url,
    /// The object store implementation
    pub store: std::sync::Arc<dyn object_store::ObjectStore>,
}
