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
// Re-exported so callers configuring a `VortexConfig` (the runtime's Cayenne
// accelerator) name the mode through this module alongside the other config
// enums, rather than reaching into `vortex-datafusion` for one type.
pub use vortex_datafusion::ScanConcurrency;

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
/// Default maximum age of buffered streaming-append data before the sink cuts
/// the segment and publishes it (bounds ingest-to-queryable latency for
/// long-lived insert streams).
pub const DEFAULT_STREAM_PUBLISH_INTERVAL_MS: u64 = 10_000;

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

impl TableMetadata {
    /// Physical directory segment for the **datalake (cold) tier**:
    /// `{sanitized_table_name}-{table_id}`.
    ///
    /// The datalake tier groups a table's objects under this segment
    /// (`{cayenne_datalake_location}/{segment}/data/{promotion_id}/…`). Prepending
    /// a human-readable slug of the table name makes a shared datalake bucket
    /// navigable, while the trailing `UUIDv7` `table_id` preserves the collision-free
    /// namespacing that lets multiple tables/instances safely share one location.
    ///
    /// Because the `table_id` suffix already guarantees uniqueness, the name slug
    /// may be **lossy**: any character outside `[A-Za-z0-9_-]` becomes `_`, leading
    /// and trailing `_`/`-` are trimmed, and the slug is capped at
    /// [`Self::DATALAKE_SLUG_MAX_LEN`] characters. A name that slugs to nothing
    /// (e.g. all symbols) falls back to the bare `table_id`.
    ///
    /// The segment is a pure function of two immutable fields (`table_name` never
    /// changes for a given `table_id` — a rename is a drop + recreate that mints a
    /// new id), so it is derived on demand and never persisted. The warm tier is
    /// intentionally left keyed by the bare `table_id`.
    #[must_use]
    pub fn datalake_dir_segment(&self) -> String {
        let slug = Self::sanitize_name_slug(&self.table_name);
        if slug.is_empty() {
            self.table_id.clone()
        } else {
            format!("{slug}-{}", self.table_id)
        }
    }

    /// Lossy, path-safe slug of a table name for [`Self::datalake_dir_segment`]:
    /// non-`[A-Za-z0-9_-]` characters become `_`, leading/trailing `_`/`-` are
    /// trimmed, and the result is capped at [`Self::DATALAKE_SLUG_MAX_LEN`]. May
    /// return an empty string (e.g. an all-symbol name); callers fall back to the
    /// bare `table_id`, which alone keeps segments unique.
    fn sanitize_name_slug(name: &str) -> String {
        let mapped: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        mapped
            .trim_matches(['_', '-'])
            .chars()
            .take(Self::DATALAKE_SLUG_MAX_LEN)
            .collect::<String>()
            // Re-trim: truncation can re-expose a trailing separator.
            .trim_end_matches(['_', '-'])
            .to_string()
    }

    /// Maximum length (in characters) of the sanitized table-name slug used in
    /// [`Self::datalake_dir_segment`]. Keeps the full segment well under the
    /// 255-byte path-component limit on common filesystems even with a 36-char
    /// UUID and a separator appended.
    pub const DATALAKE_SLUG_MAX_LEN: usize = 64;
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
    /// Metadata-only publish: the per-commit-constant sequence at which the keys
    /// deleted by THIS file were RE-INSERTED in the same upsert publish (`None`
    /// for a pure delete that re-inserts nothing, and for position-based files
    /// which carry no keys). On load the merge-on-read path assigns this sequence
    /// to every key in this file's deletion vector, reconstructing the per-key
    /// `cayenne_insert_record` map WITHOUT a durable row per key — the keys
    /// deleted by an upsert are exactly the keys it re-inserts, at one shared
    /// sequence. Legacy rows (pre-feature) and pure deletes leave this `None` and
    /// fall back to the `cayenne_insert_record` table.
    pub reinsert_sequence: Option<i64>,
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
    /// Components are length-prefixed so tuple boundaries are unambiguous even
    /// for legacy values containing separators.
    #[must_use]
    pub fn composite_key(&self) -> String {
        let mut composite = String::from("v1:");
        for value in &self.partition_values {
            composite.push_str(&value.len().to_string());
            composite.push(':');
            composite.push_str(value);
        }
        composite
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

/// Provenance of [`VortexConfig::sort_columns`] — whether the operator asked
/// for that sort order or schema inference guessed it.
///
/// This exists because the two carry different authority. An explicit
/// `cayenne_sort_columns` is a statement of intent and wins outright. An
/// inference-derived value is a *fallback guess* — for `PostgreSQL` CDC tables it
/// resolves to the primary key when the source has no `CLUSTER` or natural
/// order, which is close to the worst clustering for range/date predicates. It
/// must therefore rank below the hot filter columns actually observed on scans,
/// or the guess permanently shadows the measurement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortColumnsOrigin {
    /// Explicitly configured by the operator (or absent, in which case
    /// `sort_columns` is empty and the distinction is moot). Authoritative.
    #[default]
    User,
    /// Filled in by schema inference from the source's declared sort order.
    /// A guess — outranked by observed filter columns.
    Inferred,
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
/// - `auto` (default) — every delta write encodes at a light level: a delta
///   is transient by definition (the tiered compactor folds it into a
///   properly-encoded file), so it skips the full cascade regardless of size.
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum DeltaEncoding {
    /// `Auto` — light encoding for every delta write. Deltas are transient
    /// (compaction re-encodes them at the full cascade), so the CDC hot path
    /// skips the per-file encoder-strategy search + FSST symbol-table training
    /// regardless of delta size. The SF1000 CH-benCHmark HTAP sweep validated
    /// this at production scale: shedding checkpoint encode CPU lets the apply
    /// loop keep up (it enables replication convergence where full-encode does
    /// not, and gives the best analytic QPH on top of coalescing).
    ///
    /// This is also what pre-feature stored table configs deserialize to via
    /// `#[serde(default)]`, so existing tables pick up the policy on upgrade
    /// (write-time only; existing data files are unaffected and a level
    /// change never forces a table re-create). Set the
    /// `cayenne_delta_encoding` param to `7` to opt out (the full default
    /// cascade, byte-for-byte the pre-feature behavior).
    #[default]
    Auto,
    /// Fixed encoding level `0..=10` applied to every delta write.
    Level(u8),
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
    /// NOTE: the Spice accelerator's auto-tune layer pre-resolves `Auto` to
    /// [`Self::Key`] for `refresh_mode: changes` (CDC) tables that have a
    /// primary key BEFORE the config reaches this engine-level resolution —
    /// position-delete compaction must serialize with writers and starves
    /// under continuous CDC, while key-delete compaction runs concurrently.
    /// This function therefore only sees `Auto` for non-CDC tables (and PK-less
    /// CDC tables), where position remains the right resolution.
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

/// Durability mode for the inline CDC write path (`refresh_mode: changes`).
///
/// In [`Self::File`] (the explicit conservative opt-out — byte-identical to
/// the pre-mem-tier behavior) every CDC batch persists a durable metastore
/// entry / staged Vortex write before the source slot ack advances. In
/// [`Self::Memory`] (the default) the inline path
/// appends each batch to an in-RAM tier and DEFERS the source slot ack until a
/// periodic/cap-triggered checkpoint flushes the tier to a durable Vortex file —
/// collapsing per-batch durability cost at the price of replaying the
/// un-checkpointed tail from the source slot on crash (the apply is
/// PK-idempotent, so this is exactly-once). Memory mode is bounded by a
/// per-table byte cap AND a process-global byte budget so it can never OOM; on
/// cap breach it spills (checkpoints) and, under sustained overload, falls back
/// to the durable path for the breaching batch.
///
/// Memory mode only applies to the small-write CDC profile; it is forced to
/// [`Self::File`] for full/snapshot/append-without-fast-refresh profiles (no
/// inline write path to invert) and for partitioned tables (their visibility
/// flip cannot be deferred).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CdcDurability {
    /// Per-batch durable metastore/Vortex persist before the slot ack. The
    /// conservative explicit opt-out: byte-identical to the pre-mem-tier
    /// behavior. Also what every NON-eligible table silently uses regardless
    /// of configuration (see [`Self::Memory`]'s eligibility gates).
    File,
    /// In-RAM tier; slot ack deferred to a periodic/cap-triggered checkpoint.
    /// The DEFAULT: A/B-validated faster than `file` end-to-end on the CDC
    /// profile (higher analytical QPH, lower replication lag, and a fraction
    /// of the disk footprint) with identical convergence. Safe as a default
    /// because it is eligibility-gated, not forced: it engages only for the
    /// changes/small-write refresh profile AND a replayable source committer
    /// (the runtime arms it lazily on the first batch whose committer reports
    /// `supports_deferral()`) on a non-partitioned table; every other
    /// profile/source/table silently keeps the durable `File` path. The RAM
    /// tier is bounded on three axes so the deferred slot ack keeps advancing
    /// and the tier never grows unbounded: a per-table byte cap
    /// (`cdc_mem_tier_max_bytes`, memory-scaled 256 MiB–1 GiB when unset)
    /// checked by the write path per burst (the synchronous OOM backstop), an age cap
    /// (`cdc_mem_tier_max_age_ms`, default 10 s) enforced by the periodic
    /// background checkpoint task WITHOUT blocking the writer
    /// (`cdc_mem_tier_checkpoint_interval_ms`, default 1 s), which also
    /// flushes idle / pure-upsert tables that never trip the byte cap.
    /// Correctness is unaffected (a crash discards the un-checkpointed tail;
    /// the source re-streams on restart and the PK-idempotent apply converges
    /// exactly-once).
    #[default]
    Memory,
}

impl CdcDurability {
    /// Parse a spicepod parameter value (`file` | `memory`).
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "file" => Some(Self::File),
            "memory" => Some(Self::Memory),
            _ => None,
        }
    }

    /// Return the spicepod/config string for this mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Memory => "memory",
        }
    }

    /// Whether this is the in-memory deferred-ack mode.
    #[must_use]
    pub const fn is_memory(self) -> bool {
        matches!(self, Self::Memory)
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
    reason = "one independent pin flag per adaptive-tunable actuator"
)]
pub struct PinnedTuningActuators {
    /// The inline-memtable flush caps were operator-set (don't adapt them).
    pub inline_flush: bool,
    /// `cayenne_compaction_background_interval_ms` was operator-set.
    pub compaction_interval: bool,
    /// `cayenne_compaction_trigger_files` was operator-set.
    pub compaction_trigger: bool,
    /// `cayenne_bake_deletion_index_trigger` was operator-set (don't adapt the
    /// seq-prefix bake's deletion-index trigger).
    pub bake_deletion_index_trigger: bool,
    /// `cayenne_write_concurrency` was operator-set.
    pub write_concurrency: bool,
    /// `cayenne_cdc_mem_tier_max_bytes` was operator-set (don't adapt the
    /// in-memory CDC durability tier byte cap).
    pub mem_tier: bool,
    /// `cayenne_target_file_size_mb` was operator-set (don't adapt the target
    /// Vortex file size).
    pub target_file_size: bool,
}

/// Storage medium backing a table's data files or metastore, mapped from the
/// runtime's detected acceleration storage class at registration. Cayenne-local:
/// the runtime's `ResolvedAccelerationStorage` lives in the `runtime` crate (which
/// depends on this one), so it cannot be imported here — the accelerator maps onto
/// this enum. A *detected* fact, not an operator knob: it never (de)serializes with
/// the spicepod config (`#[serde(skip)]` on the carrying fields).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageClass {
    /// Local NVMe/SSD — fast random I/O; the tuner applies no write-amortization bias.
    LocalSsd,
    /// Network-attached storage — EBS / Azure managed block disks, or an NFS/SMB
    /// network filesystem. Higher, variable latency: the slow/networked tier.
    /// (The variant name is historical — EBS was the first case.)
    Ebs,
    /// tmpfs / RAM-backed — fastest; no bias.
    Tmpfs,
    /// Object store (S3) or an undetectable mount. The safe default — treated as the
    /// slow tier so the tuner biases toward fewer, larger files / amortized commits.
    #[default]
    Unknown,
}

impl StorageClass {
    /// Slow/networked tier (EBS, object store, or undetected) — biases the tuner
    /// toward larger inline-flush (fewer, bigger files; fewer metastore commits).
    /// `LocalSsd`/`Tmpfs` are fast and get no bias.
    #[must_use]
    pub fn is_slow_tier(self) -> bool {
        matches!(self, Self::Ebs | Self::Unknown)
    }

    /// Stable numeric code for the telemetry info gauge: `0` `LocalSsd`, `1` `Ebs`,
    /// `2` `Tmpfs`, `3` `Unknown`. Lets dashboards see the storage tier the tuner
    /// detected without emitting a high-cardinality string label.
    #[must_use]
    pub fn metric_code(self) -> u64 {
        match self {
            Self::LocalSsd => 0,
            Self::Ebs => 1,
            Self::Tmpfs => 2,
            Self::Unknown => 3,
        }
    }
}

/// Serializes [`ScanConcurrency`] through its own `Display`/`FromStr` pair.
///
/// The type is owned by `vortex-datafusion`, which carries no serde dependency, so
/// the string form is produced here rather than derived there. Going through the
/// enum's own parser keeps one definition of what `auto`/`off`/`<n>` mean — a
/// second mapping here would be free to drift from the one the scan honors.
mod scan_concurrency_serde {
    use super::ScanConcurrency;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        value: &ScanConcurrency,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ScanConcurrency, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Configuration for Vortex encodings to optimize compression and performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "VortexConfig is a flat aggregate of many independent, unrelated runtime toggles \
              mapped 1:1 from spicepod params; grouping them into sub-structs would obscure that mapping"
)]
pub struct VortexConfig {
    /// Runtime-global footer metadata cache size in MB, when explicitly configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footer_cache_mb: Option<usize>,
    /// Shared Vortex segment cache capacity in MB.
    ///
    /// Passed through to `vortex-datafusion` as the per-format segment cache size.
    pub segment_cache_mb: usize,
    /// How many splits a single Vortex file scan decodes concurrently.
    ///
    /// `auto` (the default) derives it from `DataFusion` target partitions and the
    /// planned file count; `off` forces serial decoding; an explicit count pins it.
    /// Raising it trades resident decode memory for scan throughput — the scan
    /// memory accounting charges the query pool for every concurrent split, so a
    /// wide fan-out over a small pool surfaces as a refused query rather than an
    /// over-committed host.
    #[serde(default, with = "scan_concurrency_serde")]
    pub scan_concurrency: ScanConcurrency,
    /// Target size for individual Vortex files in MB. When writes exceed this size,
    /// a new Vortex file will be created in the same listing directory. This allows
    /// for better parallelism and more granular statistics for query optimization.
    /// Defaults to 256 MB.
    pub target_vortex_file_size_mb: usize,
    /// Columns to sort data by on refresh operations (empty = no sorting)
    pub sort_columns: Vec<String>,
    /// Where [`Self::sort_columns`] came from. `user` (an explicit
    /// `cayenne_sort_columns`) is authoritative and outranks everything.
    /// `inferred` means schema inference filled it from the source's declared
    /// order — for a `PostgreSQL` CDC table that is usually just the primary key,
    /// a *guess* about what queries will filter on. An inferred value therefore
    /// ranks BELOW the hot filter columns actually observed on scans, so the
    /// default-on adaptive layout can override a guess with evidence.
    ///
    /// Deliberately NOT compared by `configuration_matches`: it is provenance
    /// about a value, not a data-affecting field. Comparing it would make every
    /// existing table look config-changed on upgrade and trip the recreate path.
    #[serde(default)]
    pub sort_columns_origin: SortColumnsOrigin,
    /// Columns to hash-cluster rows by during intra-write sharding (the parallel
    /// encode fan-out). Empty = derive from the primary key (PK-hash clustering,
    /// the historical behavior); PK-less tables shard round-robin. Ignored for
    /// sorted tables (`sort_columns` forces a single serial writer).
    #[serde(default)]
    pub shard_key_columns: Vec<String>,
    /// Compression strategy to use for Vortex files
    /// Defaults to Btrblocks
    pub compression_strategy: CompressionStrategy,
    /// Encoding effort for delta writes (fresh CDC/append snapshot files).
    /// `auto` (default) encodes every delta light (deltas are transient and
    /// folded into properly-encoded files by compaction); explicit `0..=10`
    /// pins the level (`7` = the full default cascade, the pre-feature
    /// behavior). Maintenance writes (compaction, rewrites) always use the full
    /// default encoding. See [`DeltaEncoding`].
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
    /// Deletion-index size (count of live PK tombstones) at or above which the
    /// seq-prefix bake (Stage 2 compaction) is triggered. The bake consolidates
    /// the settled older prefix of protected snapshots so their tombstones drop
    /// out of the live merge-on-read deletion index, lowering the per-query probe
    /// cost — at the cost of write amplification. A larger value bakes less often
    /// (bounding write-amp); a smaller value bakes more often (smaller index,
    /// cheaper probe). Key-delete tables only (position-delete tables never bake).
    ///
    /// Defaults to `50_000` (see
    /// [`crate::provider::table::BAKE_DELETION_INDEX_TRIGGER`]).
    #[serde(default = "default_bake_deletion_index_trigger")]
    pub bake_deletion_index_trigger: usize,
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
    /// Maximum age (ms) of buffered data in a streaming append before the sink
    /// cuts the segment and publishes it, bounding ingest-to-queryable latency
    /// for long-lived insert streams (e.g. ADBC bulk ingest). Each segment is a
    /// complete prepare→stage→publish write. Set to 0 to disable and publish
    /// only when the stream ends (pre-feature behavior).
    #[serde(default = "default_stream_publish_interval_ms")]
    pub stream_publish_interval_ms: u64,
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
    /// Whether this table is a pure in-memory (`mode: memory`) accelerator: all
    /// data lives in the RAM mem-tier (the in-memory metastore holds only metadata), no Vortex
    /// data files are ever written (checkpoint + compaction disabled), it is
    /// ephemeral (reload from the source on restart — for CDC `changes` the source
    /// slot is committed immediately after each in-RAM write, since there is no
    /// durable checkpoint to defer behind), and a hard RAM bound returns an error
    /// on breach instead of spilling.
    ///
    /// Not a user param — `VortexConfig` is only serde-deserialized from the metastore
    /// (never from user input; the accelerator builds it field-by-field), so this is
    /// set programmatically by the accelerator from the acceleration `mode: memory`.
    /// It IS serialized with the table config so it survives the create-time
    /// metastore round-trip (`create` re-reads the table via `get_table`). For a
    /// memory table the metastore is itself in-RAM; a file-mode table stores `false`.
    #[serde(default)]
    pub memory_mode: bool,
    /// Durability mode for the inline CDC write path. [`CdcDurability::Memory`]
    /// (default) appends to an in-RAM tier and defers the slot ack to a
    /// periodic/cap-triggered checkpoint — A/B-validated faster than `file`
    /// end-to-end and eligibility-gated (non-CDC profiles, non-replayable
    /// sources, and partitioned tables silently keep the durable path).
    /// [`CdcDurability::File`] persists each batch durably before advancing
    /// the source slot: the explicit conservative opt-out, byte-identical to
    /// the pre-mem-tier behavior.
    #[serde(default)]
    pub cdc_durability: CdcDurability,
    /// Per-table RAM-tier byte cap before a forced spill (checkpoint) + slot
    /// advance, in `cdc_durability: memory` mode only. `0` disables the
    /// per-table cap; the process-global byte budget still bounds aggregate
    /// resident memory. When both are set, whichever is breached first triggers
    /// the spill. The serde default is the 256 MiB floor; when the param is
    /// unset the accelerator auto-derives a memory-scaled value (~1/64 of host
    /// RAM, clamped 256 MiB–1 GiB) so the periodic background checkpoint is
    /// the primary flush path and the write-path spill remains a rare backstop.
    #[serde(default = "default_cdc_mem_tier_max_bytes")]
    pub cdc_mem_tier_max_bytes: i64,
    /// Number of PK-hash shards for the in-mem CDC tier (`cdc_durability: memory`).
    /// Each shard is an independent sub-tier (its own segments + tombstones + publish
    /// lock), so ONE apply fans its rows across shards by `shard_of_pk` and runs the
    /// validate→append per shard in parallel (intra-apply parallelism — the win is
    /// inside a single apply, since `write_lock` already serializes applies). `1` is
    /// the unsharded path, byte-identical to pre-sharding. `>1` is gated behind the
    /// SF-1000 N-sweep; the checkpoint is ALWAYS whole-tier-atomic regardless of N
    /// (a single-shard checkpoint would pin the source watermark → unbounded WAL).
    #[serde(default = "default_cdc_mem_tier_shards")]
    pub cdc_mem_tier_shards: usize,
    /// Max wall-clock milliseconds a RAM-tier epoch may age before a forced
    /// checkpoint, in `cdc_durability: memory` mode only. Bounds the crash-replay
    /// window for cold/low-traffic tables (whose byte cap would otherwise never
    /// trip). `0` disables the age trigger. Defaults to 10 s.
    #[serde(default = "default_cdc_mem_tier_max_age_ms")]
    pub cdc_mem_tier_max_age_ms: u64,
    /// Minimum resident tier bytes before the PERIODIC background tick durably
    /// checkpoints, in `cdc_durability: memory` mode only. Bounds snapshot /
    /// delete-file churn: below this size a tick is a no-op unless the tier's
    /// age has reached `cdc_mem_tier_max_age_ms` (which still bounds the
    /// deferred slot ack and crash-replay window). The write-path cap spill and
    /// explicit checkpoints are NOT gated. `0` disables the gate. The serde
    /// default is the 32 MiB floor; when the param is unset the accelerator
    /// auto-derives 1/8 of the derived byte cap (clamped 32–128 MiB), keeping
    /// the cap:gate ratio constant so a larger tier flushes proportionally
    /// larger files.
    #[serde(default = "default_cdc_mem_tier_min_flush_bytes")]
    pub cdc_mem_tier_min_flush_bytes: i64,
    /// Periodic background mem-tier checkpoint interval in milliseconds, in
    /// `cdc_durability: memory` mode only. The accelerator spawns a per-table
    /// background task that checkpoints the RAM tier every interval (mirroring
    /// the background compactor); this is what advances the deferred source slot
    /// ack on an idle or pure-upsert stream that never trips a delete/truncate
    /// event trigger or a write-path cap. `0` disables the periodic task. Defaults
    /// to 1 s.
    #[serde(default = "default_cdc_mem_tier_checkpoint_interval_ms")]
    pub cdc_mem_tier_checkpoint_interval_ms: u64,
    /// Max wall-clock milliseconds the ACTIVE ingestion piece may age before a
    /// **seal** durably shadows it and advances the source replication slot, in
    /// `cdc_durability: memory` mode only. This is the fresh-durability cadence
    /// that DECOUPLES the slot ack (and thus replication/freshness lag) from the
    /// heavy protected-snapshot checkpoint: a seal writes the un-sealed RAM delta
    /// to the durable-but-unpublished inline corpus (one metastore commit, no
    /// Vortex encode, no listing-fence publish, no read-amp) and fires the slot
    /// advancer, so the slot advances every ~`seal_age_ms` instead of every
    /// `max_age_ms`/`min_flush_bytes` checkpoint. Reads are unaffected — they
    /// already union the RAM tier; the shadow is invisible in-process and is only
    /// replayed on crash recovery. Bounds replication lag WITHOUT the read-amp of a
    /// faster full checkpoint. `0` disables sealing (slot ack reverts to the
    /// checkpoint cadence — the pre-seal behavior). Defaults to 2 s so replication
    /// freshness stays under ~3 s. Should be `<= cdc_mem_tier_max_age_ms` to have
    /// any effect (a seal older than the checkpoint window is superseded by the
    /// checkpoint's own slot advance).
    #[serde(default = "default_cdc_mem_tier_seal_age_ms")]
    pub cdc_mem_tier_seal_age_ms: u64,
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
    /// closed loop leaves these alone (see [`PinnedTuningActuators`]).
    #[serde(default)]
    pub pinned_tuning_actuators: PinnedTuningActuators,
    /// Build the scan's file set from the per-snapshot manifest
    /// (`cayenne_snapshot_file`) instead of by listing the snapshot directory.
    ///
    /// EXPERIMENTAL and **off by default**. This path is not yet
    /// production-hardened: the manifest is maintained as a dual-source supplement
    /// to directory listing and has not been proven complete on every write/commit
    /// path, so opting in trades the authoritative listing for a faster-to-resolve
    /// but less-exercised source. It is **not** required by — and is independent
    /// of — the seq-prefix deletion-index bake: the bake reads the above-`T`
    /// protected snapshots via the existing protected-snapshot scan union
    /// (`protected_snapshots`), never via this manifest.
    ///
    /// Defaults to `false` (directory listing) so the manifest stays a
    /// dual-source supplement until it is proven complete on every path. When
    /// `true`, a scan resolves its data files from the manifest rows for the
    /// snapshot it pinned; if the manifest is empty for that snapshot (e.g. a
    /// snapshot written before population, or a transient post-write rebuild
    /// failure) the scan transparently falls back to directory listing, so this
    /// flag never makes a scan miss a live file. The manifest is populated to be
    /// equal to the directory listing by construction (see
    /// `upsert_snapshot_manifest_from_listing`).
    #[serde(default)]
    pub scan_from_manifest: bool,
    /// Which widening schema differences detected at table open (the requested
    /// schema vs the stored metastore schema) may be committed in place via
    /// `update_table_schema` instead of pinning the stored schema. Set by the
    /// runtime accelerator from the dataset's `on_schema_change` policy
    /// (`append_new_columns` / `sync_all_columns`); the default
    /// [`SchemaEvolutionMode::Disabled`] keeps the legacy pin-stored-schema
    /// behavior verbatim (`on_schema_change: block`/`fail`, or omitted).
    /// Runtime-only — never compared by `configuration_matches`.
    #[serde(default, skip_serializing_if = "SchemaEvolutionMode::is_disabled")]
    pub schema_evolution: SchemaEvolutionMode,
    /// Goal-driven adaptive-tuning setpoints (each `None` = unset → the legacy
    /// signal-driven controller for that metric). When any is set, the closed loop
    /// drives that high-level SLO toward target with small incremental steps,
    /// converging within `goal_convergence_window_secs`. Set from the
    /// `cayenne_goal_*` params. A goal declares a target, not a controller, so it
    /// never turns `dynamic_tuning` on — set without it, the goal is inert (the
    /// accelerator warns). Runtime-only — never compared by
    /// `configuration_matches` (does not affect data layout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_replication_lag_secs: Option<f64>,
    /// Freshness goal target in seconds (age of the newest queryable data).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_freshness_secs: Option<f64>,
    /// Query-latency (p99) goal target in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_query_latency_ms: Option<f64>,
    /// Query-throughput goal target in queries per hour (higher is better).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_qph: Option<f64>,
    /// Convergence window (seconds) for goal-driven tuning. `None` → the default
    /// (`provider::tuning::DEFAULT_GOAL_CONVERGENCE_WINDOW`, 60s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_convergence_window_secs: Option<f64>,
    /// Storage medium of the table's DATA files (Vortex), detected at registration
    /// and mapped from the runtime's acceleration storage class. A slow tier
    /// (EBS / object store / undetected) biases the tuner toward fewer, larger
    /// files. Detected runtime fact — `#[serde(skip)]` (never a spicepod knob,
    /// never compared by `configuration_matches`).
    #[serde(skip)]
    pub data_storage_class: StorageClass,
    /// Storage medium of the METASTORE (where publish commits + inline re-reads
    /// land). A slow tier biases toward larger inline-flush to amortize commits.
    #[serde(skip)]
    pub metastore_storage_class: StorageClass,
    /// Measured sequential write throughput of the DATA volume in MiB/s, from the
    /// runtime's startup calibration probe; `None` when unprobed (remote/object
    /// store) or the probe failed. Refines [`Self::data_storage_class`] into the
    /// tuner's *continuous* slow-tier bias (a fast io2 volume gets less
    /// amortization pressure than a slow gp3). Detected runtime fact — `#[serde(skip)]`.
    #[serde(skip)]
    pub data_storage_write_mbps: Option<f64>,
    /// Measured sequential write throughput of the METASTORE volume in MiB/s
    /// (calibration probe); refines the publish-bias bar. `None` when unprobed.
    #[serde(skip)]
    pub metastore_storage_write_mbps: Option<f64>,
    /// Force the **read/query** scan to emit Arrow *view* types (`Utf8View`/
    /// `BinaryView`) for `Utf8`/`Binary` columns, decoupled from the stored
    /// schema (which keeps the original types for writes/CDC/stats/keyset). Lets
    /// `DataFusion` plan joins/aggregates on view arrays, avoiding the i32 2 GiB
    /// offset overflow in hash-join build-side `concat_batches` (e.g. `CH-benCH`
    /// q21 at SF1000, where `su_name` fans out across a ~100M-row join).
    ///
    /// Runtime-configurable: the accelerator factory sets it (default off; opt in
    /// with `cayenne_force_view_types: true`).
    #[serde(skip)]
    pub force_view_read_schema: bool,

    /// End-to-end integrity checksums for durability surfaces (staging-WAL
    /// records and Vortex data files). When enabled:
    ///
    /// * Each staging-WAL record is written with a checksum envelope, and a
    ///   record that fails its checksum on recovery is *detected and discarded*
    ///   (converging to the last committed snapshot) rather than parsed as
    ///   garbage or replayed with corrupted move instructions.
    /// * A digest is computed for each published Vortex data file and stored in
    ///   the manifest, then verified before the file is first scanned; a
    ///   mismatch fails the read as a *detected fault* instead of returning
    ///   silently-wrong rows.
    ///
    /// Runtime-configurable: the accelerator factory sets it (default off; opt
    /// in with `cayenne_integrity_checksums: true`). Off is byte-identical to
    /// the pre-feature on-disk format and adds no read/write overhead. Reads
    /// always accept both framed and legacy pre-feature WAL records regardless
    /// of this flag, so toggling it (or downgrading) never orphans a WAL.
    #[serde(skip)]
    pub integrity_checksums: bool,

    // ---- Cold object-store tier (storage-cascade bottom tier; cascade model) ----
    /// Absolute object-store URL prefix for the cold tier (e.g.
    /// `s3://bucket/prefix`). `None`/empty (the default) disables the cold tier.
    /// Set from the `cayenne_datalake_location` spicepod param. Persisted so a
    /// reopened table knows where its cold files live; NOT compared by
    /// `configuration_matches`, so toggling it never recreates the table (the
    /// cold tier is a strict superset of behavior over an unchanged warm tier).
    pub cold_tier_location: Option<String>,
    /// Liquid-clustering key columns for cold files (multi-column Z-order).
    /// Empty = fall back to `sort_columns`, then the primary key. Set from
    /// `cayenne_datalake_clustering_columns`.
    pub cold_clustering_columns: Vec<String>,
    /// Target size for cold Vortex files in MB. Larger than the warm
    /// `target_vortex_file_size_mb` because object stores favor fewer, larger
    /// objects and cold scans are range reads. Set from
    /// `cayenne_datalake_target_file_size_mb`. Defaults to 512.
    pub cold_target_file_size_mb: usize,
    /// Max input bytes (in MB) fed to one bounded Z-order sort run during a
    /// warm-to-datalake move. `None` (the default) derives
    /// [`Self::cold_clustering_run_size_bytes`] as `cold_target_file_size_mb *
    /// 16` — 16 target files' worth of input gives enough locality for good
    /// clustering (8 GiB with the default 512 MB target).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_clustering_run_size_mb: Option<usize>,
    /// The warm tier moves to the datalake only once it exceeds this many bytes
    /// (`<= 0` disables the byte trigger). Set from
    /// `cayenne_datalake_warm_max_bytes`.
    pub cold_tier_warm_max_bytes: i64,
    /// The warm tier moves to the datalake only once it exceeds this many files
    /// (`0` disables the file-count trigger). Set from
    /// `cayenne_datalake_warm_max_files`.
    pub cold_tier_warm_max_files: usize,
    /// How often (ms) the background loop checks whether to move warm-tier data
    /// to the datalake. Datalake tiering is not latency-critical, so this is much coarser than the
    /// compaction interval. Set from the user-facing
    /// `cayenne_datalake_tiering_check_interval_ms`. Defaults to 60s.
    pub cold_tier_background_interval_ms: u64,
    /// Physical-GC cadence AND orphan grace (ms) for superseded cold objects:
    /// the sweep runs about this often, and an orphan (on the store, not in the
    /// manifest) is deleted only after being observed orphaned this long — mark
    /// on one sweep, delete on the next, so an in-flight scan gets a full
    /// interval to finish. From `cayenne_datalake_gc_interval_ms`; defaults to
    /// 5min, lowered in tests.
    pub cold_tier_gc_interval_ms: u64,
}

impl VortexConfig {
    /// Whether the cold object-store tier is enabled (a non-empty location set).
    #[must_use]
    pub fn cold_tier_enabled(&self) -> bool {
        self.cold_tier_location
            .as_ref()
            .is_some_and(|s| !s.trim().is_empty())
    }

    /// Effective byte cap for one bounded Z-order sort run during cold
    /// promotion: an explicit [`Self::cold_clustering_run_size_mb`], else
    /// derived as `cold_target_file_size_mb * 16`. The single derivation rule
    /// for standalone and runtime paths — never returns 0.
    #[must_use]
    pub fn cold_clustering_run_size_bytes(&self) -> usize {
        self.cold_clustering_run_size_mb
            .unwrap_or_else(|| self.cold_target_file_size_mb.saturating_mul(16))
            .max(1)
            .saturating_mul(1024 * 1024)
    }
}

/// Evolution set permitted when a widening schema difference is detected at
/// table open. Mirrors the runtime's `on_schema_change` policy semantics:
/// `append_new_columns` evolves added nullable columns only, `sync_all_columns`
/// evolves the full widening set (added columns + lossless type widening +
/// nullability relax).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaEvolutionMode {
    /// Never evolve the stored schema (legacy behavior: pin the stored schema
    /// and warn on any difference).
    #[default]
    Disabled,
    /// Evolve added nullable columns only (`on_schema_change: append_new_columns`).
    AddColumnsOnly,
    /// Evolve the full widening set (`on_schema_change: sync_all_columns`).
    Widen,
}

impl SchemaEvolutionMode {
    /// `true` when this is [`SchemaEvolutionMode::Disabled`] (the serde
    /// skip-serializing predicate, so stored `vortex_config_json` stays
    /// byte-identical for non-evolving tables).
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        matches!(self, SchemaEvolutionMode::Disabled)
    }

    /// Whether `plan` falls within this mode's evolution set.
    #[must_use]
    pub fn allows(&self, plan: &arrow_tools::schema_evolution::WideningPlan) -> bool {
        match self {
            SchemaEvolutionMode::Disabled => false,
            SchemaEvolutionMode::AddColumnsOnly => plan.is_additive_only(),
            SchemaEvolutionMode::Widen => true,
        }
    }
}

fn default_concurrency() -> usize {
    cpu_budget::cpu_budget().cayenne_upload_concurrency()
}

fn default_upload_concurrency() -> usize {
    default_concurrency()
}

fn default_compaction_trigger_files() -> usize {
    8
}

fn default_bake_deletion_index_trigger() -> usize {
    crate::provider::table::BAKE_DELETION_INDEX_TRIGGER
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

fn default_stream_publish_interval_ms() -> u64 {
    DEFAULT_STREAM_PUBLISH_INTERVAL_MS
}

/// Default per-table RAM-tier byte cap for `cdc_durability: memory` (256 MiB —
/// the serde/engine floor; the accelerator's auto-tune derives a memory-scaled
/// value of ~1/64 of host RAM clamped to 256 MiB–1 GiB when the param is unset,
/// so memory-rich hosts get a larger cap and hosts at/under 16 GiB keep this
/// floor).
///
/// This is the SYNCHRONOUS write-path spill threshold (`mem_tier_per_table_cap_breached`),
/// which blocks the refresh-task's next batch while it flushes — so it should be
/// a rare backstop, not the primary flush. The primary flush is the periodic
/// background checkpointer, which (since the two-phase `checkpoint_mem_tier`
/// moved the encode + `BEGIN IMMEDIATE` commit OUTSIDE the listing fence) no
/// longer stalls appends — so a larger tier is cheap. 256 MiB gives the 1 s
/// background tick time to drain the tier before this cap is reached at typical
/// CDC rates, while staying small enough that ~N memory-mode tables sum well
/// under the process-global budget, which remains the RAM-scaling aggregate
/// backstop.
fn default_cdc_mem_tier_max_bytes() -> i64 {
    256 * 1024 * 1024
}

/// One shard by default — the unsharded path, byte-identical to pre-sharding.
/// `>1` (intra-apply parallelism) is opted into per-table via the accelerator and
/// sized from the SF-1000 N-sweep; never auto-scaled (finer N = finer bloom slices
/// + more metastore refills, so the knee is empirical).
fn default_cdc_mem_tier_shards() -> usize {
    1
}

/// Default minimum tier size before the PERIODIC tick durably checkpoints
/// (32 MiB — the serde/engine floor; the accelerator's auto-tune derives 1/8 of
/// the derived byte cap, clamped 32–128 MiB, when the param is unset, keeping
/// the cap:gate ratio constant). Every durable checkpoint costs a new snapshot +
/// delete-vector files + a listing refresh under the fence; at a 1 s tick a
/// high-rate table would otherwise produce ~600 tiny snapshots per 10 minutes
/// (measured at SF-100: 408–676 accumulated snapshot dirs per heavy table), and
/// the accumulated churn degrades scans and the apply path. Gating the tick on
/// min-flush-bytes OR the age cap caps churn at ~`max_bytes/min_flush` files per
/// flush window while leaving freshness untouched (RAM rows are visible to
/// queries immediately; only the deferred source-slot ack waits, bounded by
/// `cdc_mem_tier_max_age_ms`). The write-path cap spill and explicit
/// checkpoints bypass the gate. `0` disables the gate (every tick flushes).
fn default_cdc_mem_tier_min_flush_bytes() -> i64 {
    32 * 1024 * 1024
}

/// Default RAM-tier age cap for `cdc_durability: memory` (10 s).
///
/// Bounds the crash-replay window for a slow-trickle table that never reaches
/// the byte cap. Enforced ONLY by the periodic background tick
/// (`run_mem_tier_checkpoint_tick`) — the write path never blocks on age (the
/// writer-side spill predicate is byte-only; see
/// `mem_tier_per_table_cap_breached`), so the slot-ack/replay bound is
/// `max_age` + one tick interval + the checkpoint duration, with zero apply
/// stall. Raised from 2 s now that the background checkpointer (1 s tick) is
/// the primary, non-fence-blocking flush path: an actively-written table is
/// drained by the background tick long before 10 s, so this age cap only
/// catches genuinely slow tables. Deliberately NOT hardware-derived by the
/// accelerator's auto-tune: it is a time-domain durability-policy bound, and
/// scaling it with host capacity would silently change crash-replay semantics.
fn default_cdc_mem_tier_max_age_ms() -> u64 {
    10_000
}

/// Default periodic mem-tier checkpoint interval for `cdc_durability: memory`
/// (1 s). The accelerator spawns a per-memory-mode-table background task that
/// checkpoints the RAM tier every interval (mirroring the background compactor),
/// which is what advances the deferred source slot ack on an idle/pure-upsert
/// stream. Set to 0 to disable the periodic task (the write-path caps still
/// bound hot tables).
fn default_cdc_mem_tier_checkpoint_interval_ms() -> u64 {
    1_000
}

/// Default seal cadence for `cdc_durability: memory` (2 s). A seal durably
/// shadows the un-sealed RAM delta into the unpublished inline corpus and
/// advances the source slot WITHOUT a full protected-snapshot checkpoint, so
/// replication/freshness lag is bounded by this (not by `max_age_ms` /
/// `min_flush_bytes`). 2 s keeps freshness under ~3 s while amortizing the
/// per-seal metastore commit. Set to 0 to disable sealing (slot ack reverts to
/// the checkpoint cadence). Like the age cap, this is a time-domain durability
/// policy bound and is deliberately NOT hardware-derived.
fn default_cdc_mem_tier_seal_age_ms() -> u64 {
    2_000
}

impl VortexConfig {
    /// Surface parameter values that *parse* but won't behave as a user likely
    /// intends — out-of-range values that get silently clamped at their use site,
    /// and combinations that don't compose with each other. Returns
    /// human-readable warnings; the caller logs each with the dataset context.
    ///
    /// Pure and side-effect-free so the rules stay unit-testable. The actual
    /// clamping still happens at the use sites — this only makes it visible
    /// instead of silent. `available_cores` is the encode-shard ceiling; pass
    /// `cpu_budget::cpu_budget().cayenne_write_concurrency_ceiling()`.
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
                "cayenne_write_concurrency ({write_concurrency}) exceeds the runtime's CPU budget ({cores} cores); encode is CPU-bound so it is capped at {cores} — the surplus only inflates the per-snapshot file count without speeding the write. Set it to {cores} or below."
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
            // Derive intra-file decode concurrency from target partitions and the
            // planned file count.
            scan_concurrency: ScanConcurrency::default(),
            // Balanced file size for scan throughput and write amplification
            target_vortex_file_size_mb: 256,
            // No sort columns by default
            sort_columns: Vec::new(),
            sort_columns_origin: SortColumnsOrigin::default(),
            // Shard key derives from the primary key unless overridden
            shard_key_columns: Vec::new(),
            compression_strategy: CompressionStrategy::default(),
            // `auto`: light encoding for every delta (re-encoded by
            // compaction). Validated at production scale by the SF1000
            // CH-benCHmark HTAP sweep (frees apply CPU → convergence + QPH).
            // Set the param to `7` to opt out (pre-feature behavior).
            delta_encoding: DeltaEncoding::default(),
            upload_concurrency: default_upload_concurrency(),
            write_concurrency: None,
            compaction_trigger_files: default_compaction_trigger_files(),
            bake_deletion_index_trigger: default_bake_deletion_index_trigger(),
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
            stream_publish_interval_ms: default_stream_publish_interval_ms(),
            pk_conflict_detection: PkConflictDetection::default(),
            pk_keyset_cache_mb: None,
            deletion_mode: DeletionMode::default(),
            memory_mode: false,
            cdc_durability: CdcDurability::default(),
            cdc_mem_tier_max_bytes: default_cdc_mem_tier_max_bytes(),
            cdc_mem_tier_shards: default_cdc_mem_tier_shards(),
            cdc_mem_tier_max_age_ms: default_cdc_mem_tier_max_age_ms(),
            cdc_mem_tier_min_flush_bytes: default_cdc_mem_tier_min_flush_bytes(),
            cdc_mem_tier_checkpoint_interval_ms: default_cdc_mem_tier_checkpoint_interval_ms(),
            cdc_mem_tier_seal_age_ms: default_cdc_mem_tier_seal_age_ms(),
            dynamic_tuning: false,
            pinned_tuning_actuators: PinnedTuningActuators::default(),
            // Directory listing stays the scan's file source by default; the
            // manifest is a dual-source supplement until proven complete.
            scan_from_manifest: false,
            schema_evolution: SchemaEvolutionMode::default(),
            goal_replication_lag_secs: None,
            goal_freshness_secs: None,
            goal_query_latency_ms: None,
            goal_qph: None,
            goal_convergence_window_secs: None,
            data_storage_class: StorageClass::default(),
            metastore_storage_class: StorageClass::default(),
            data_storage_write_mbps: None,
            metastore_storage_write_mbps: None,
            force_view_read_schema: false,
            integrity_checksums: false,
            cold_tier_location: None,
            cold_clustering_columns: Vec::new(),
            cold_target_file_size_mb: 512,
            cold_clustering_run_size_mb: None,
            cold_tier_warm_max_bytes: 0,
            cold_tier_warm_max_files: 0,
            cold_tier_background_interval_ms: 60_000,
            cold_tier_gc_interval_ms: 300_000,
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "these tests assert the defaults against the host the test process sees, which is the value the budget resolves to when nothing is configured"
)]
mod tests {
    use super::{PkConflictDetection, ScanConcurrency, VortexConfig};

    /// `scan_concurrency` must survive a metastore round trip, and a config
    /// written before the field existed must still load.
    ///
    /// The mode is serialized through the enum's own string form because
    /// `vortex-datafusion` carries no serde derive. A stored table's config is
    /// deserialized on every open, so a format that only round-trips one way
    /// would fail the table, not just the setting.
    #[test]
    fn scan_concurrency_round_trips_and_tolerates_configs_written_without_it() {
        for mode in [
            ScanConcurrency::Auto,
            ScanConcurrency::Off,
            ScanConcurrency::Explicit(4),
        ] {
            let config = VortexConfig {
                scan_concurrency: mode,
                ..Default::default()
            };
            let encoded = serde_json::to_string(&config).expect("config should serialize");
            let decoded: VortexConfig =
                serde_json::from_str(&encoded).expect("config should deserialize");
            assert_eq!(decoded.scan_concurrency, mode);
        }

        // A config persisted before this field existed carries no key for it.
        let legacy: VortexConfig =
            serde_json::from_str("{}").expect("a config without the field should deserialize");
        assert_eq!(legacy.scan_concurrency, ScanConcurrency::Auto);
    }

    #[test]
    fn test_concurrency_defaults_use_available_parallelism_where_global() {
        let available_parallelism =
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let config = VortexConfig::default();

        assert_eq!(config.upload_concurrency, available_parallelism);
        assert_eq!(config.write_concurrency, None);
        assert_eq!(config.pk_conflict_detection, PkConflictDetection::Auto);
    }

    /// `scan_from_manifest` must default OFF (directory listing) so the
    /// per-snapshot manifest stays a dual-source supplement until it is proven
    /// complete on every path. A regression flipping this default to `true`
    /// would silently make the manifest the authoritative scan source — guard
    /// both the struct default and the serde (empty-config) default.
    #[test]
    fn test_scan_from_manifest_defaults_off() {
        assert!(
            !VortexConfig::default().scan_from_manifest,
            "scan_from_manifest must default to false (directory listing)"
        );
        let from_empty: VortexConfig = serde_json::from_str("{}").expect("valid empty config");
        assert!(
            !from_empty.scan_from_manifest,
            "an empty config must inherit scan_from_manifest = false via serde default"
        );
        let opted_in: VortexConfig = serde_json::from_str(r#"{"scan_from_manifest": true}"#)
            .expect("valid config with scan_from_manifest opt-in");
        assert!(
            opted_in.scan_from_manifest,
            "an explicit scan_from_manifest = true must deserialize as opt-in"
        );
    }

    #[test]
    fn test_vortex_config_deserializes_pk_conflict_detection_default() {
        let config: VortexConfig = serde_json::from_str("{}").expect("valid empty config");

        assert_eq!(config.pk_conflict_detection, PkConflictDetection::Auto);
    }

    /// The mem-tier caps + periodic interval must default to NON-ZERO so the
    /// write-path spill and the periodic checkpoint self-fire (bounding the tier
    /// and advancing the deferred slot ack). A regression to `0`/`u64::MAX`
    /// disables both and reopens the unbounded-tier lag gap. Guards both the
    /// struct default and the serde default (an empty config must inherit them).
    #[test]
    fn test_cdc_mem_tier_caps_default_non_zero() {
        let config = VortexConfig::default();
        assert!(
            config.cdc_mem_tier_max_bytes > 0,
            "cdc_mem_tier_max_bytes must default non-zero so the write-path spill self-fires"
        );
        assert!(
            config.cdc_mem_tier_max_age_ms > 0,
            "cdc_mem_tier_max_age_ms must default non-zero so cold tables checkpoint"
        );
        assert!(
            config.cdc_mem_tier_checkpoint_interval_ms > 0,
            "cdc_mem_tier_checkpoint_interval_ms must default non-zero so the periodic task runs"
        );
        assert!(
            config.cdc_mem_tier_seal_age_ms > 0,
            "cdc_mem_tier_seal_age_ms must default non-zero so sealing (fast durable slot \
             advance) is ON by default — the feature must not be gated behind an opt-in flag"
        );
        assert!(
            config.cdc_mem_tier_seal_age_ms <= config.cdc_mem_tier_max_age_ms,
            "the seal cadence must default at or below the checkpoint age cap, or seals never \
             fire before the checkpoint supersedes them"
        );

        let from_empty: VortexConfig = serde_json::from_str("{}").expect("valid empty config");
        assert_eq!(
            from_empty.cdc_mem_tier_max_bytes, config.cdc_mem_tier_max_bytes,
            "an empty config must inherit the non-zero byte cap via serde default"
        );
        assert_eq!(
            from_empty.cdc_mem_tier_max_age_ms, config.cdc_mem_tier_max_age_ms,
            "an empty config must inherit the non-zero age cap via serde default"
        );
        assert_eq!(
            from_empty.cdc_mem_tier_checkpoint_interval_ms,
            config.cdc_mem_tier_checkpoint_interval_ms,
            "an empty config must inherit the non-zero checkpoint interval via serde default"
        );
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

    /// The datalake name slug is lossy but always path/S3-safe: only
    /// `[A-Za-z0-9_-]` survives, edges are trimmed, and length is capped. These
    /// cases pin the exact contract the datalake directory segment depends on.
    #[test]
    fn test_sanitize_name_slug() {
        use super::TableMetadata;

        // Plain names pass through unchanged.
        assert_eq!(TableMetadata::sanitize_name_slug("orders"), "orders");
        assert_eq!(
            TableMetadata::sanitize_name_slug("taxi_trips-1"),
            "taxi_trips-1"
        );

        // Unsafe characters (dots, spaces, slashes, schema qualifiers) become `_`.
        assert_eq!(
            TableMetadata::sanitize_name_slug("public.orders"),
            "public_orders"
        );
        assert_eq!(TableMetadata::sanitize_name_slug("my table"), "my_table");
        assert_eq!(TableMetadata::sanitize_name_slug("a/b\\c"), "a_b_c");

        // Non-ASCII is replaced (lossy is fine — the UUID suffix disambiguates).
        assert_eq!(TableMetadata::sanitize_name_slug("naïve"), "na_ve");

        // Leading/trailing separators are trimmed.
        assert_eq!(TableMetadata::sanitize_name_slug("__orders__"), "orders");
        assert_eq!(TableMetadata::sanitize_name_slug(".orders."), "orders");

        // An all-symbol name slugs to empty (caller falls back to the id).
        assert_eq!(TableMetadata::sanitize_name_slug("***"), "");
        assert_eq!(TableMetadata::sanitize_name_slug(""), "");
    }

    /// Truncation to `DATALAKE_SLUG_MAX_LEN` must not leave a dangling separator.
    #[test]
    fn test_sanitize_name_slug_caps_length_and_retrims() {
        use super::TableMetadata;

        let long = "a".repeat(200);
        let slug = TableMetadata::sanitize_name_slug(&long);
        assert_eq!(slug.chars().count(), TableMetadata::DATALAKE_SLUG_MAX_LEN);

        // A separator sitting exactly at the truncation boundary is re-trimmed.
        let boundary = format!(
            "{}_tail",
            "a".repeat(TableMetadata::DATALAKE_SLUG_MAX_LEN - 1)
        );
        let slug = TableMetadata::sanitize_name_slug(&boundary);
        assert!(
            !slug.ends_with('_') && !slug.ends_with('-'),
            "truncated slug must not end in a separator, got {slug:?}"
        );
    }

    /// The full datalake segment is `{slug}-{table_id}`, and falls back to the
    /// bare `table_id` when the name slugs to nothing. The `table_id` suffix is
    /// what keeps two distinct tables that slug identically from colliding.
    #[test]
    fn test_datalake_dir_segment() {
        use super::TableMetadata;
        use arrow_schema::Schema;
        use std::sync::Arc;

        let md = |name: &str, id: &str| TableMetadata {
            table_id: id.to_string(),
            table_name: name.to_string(),
            path: String::new(),
            path_is_relative: false,
            schema: Arc::new(Schema::empty()),
            primary_key: Vec::new(),
            on_conflict: None,
            current_snapshot_id: String::new(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
            current_sequence_number: 0,
        };

        let id = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b";
        assert_eq!(
            md("orders", id).datalake_dir_segment(),
            format!("orders-{id}")
        );
        assert_eq!(
            md("public.orders", id).datalake_dir_segment(),
            format!("public_orders-{id}")
        );

        // All-symbol name → bare id (still unique).
        assert_eq!(md("***", id).datalake_dir_segment(), id);

        // Two tables that slug identically stay distinct via their ids.
        let a = "0190a1b2-c3d4-7e5f-8a9b-000000000001";
        let b = "0190a1b2-c3d4-7e5f-8a9b-000000000002";
        assert_ne!(
            md("my table", a).datalake_dir_segment(),
            md("my.table", b).datalake_dir_segment(),
            "identical slugs must remain distinct segments via the table_id suffix"
        );
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

/// Per-file statistics for one Vortex object in a snapshot directory.
///
/// Persisted in `cayenne_snapshot_file_statistics` so listing-time pruning can
/// reuse footer min/max without re-reading every object on each scan. Rows are
/// keyed by `(table_id, snapshot_id, file_path)` and invalidated when
/// `file_size_bytes` no longer matches the object store metadata.
///
/// Consumers must treat these values as optimization hints only.
#[derive(Debug, Clone)]
pub struct SnapshotFileStatistics {
    /// Table this stats entry belongs to (`UUIDv7`)
    pub table_id: String,
    /// Snapshot directory the file was listed from (`UUIDv7`)
    pub snapshot_id: String,
    /// Object-store path of the `.vortex` file (as returned by listing)
    pub file_path: String,
    /// Cached `ObjectMeta::size` at the time stats were captured
    pub file_size_bytes: i64,
    /// Row count from the file footer when stats were captured
    pub num_rows: i64,
    /// Serialized Vortex `FileStatistics` flatbuffer bytes (per-column min, max,
    /// and null count)
    pub statistics_blob: Vec<u8>,
}

/// One row of the authoritative per-snapshot data-file manifest
/// (`cayenne_snapshot_file`). Unlike [`SnapshotFileStatistics`] (a best-effort
/// pruning cache), the manifest is the COMPLETE file set for a snapshot — every
/// data file the scan must read. A new snapshot references an existing file by
/// inserting a row pointing at the same `file_path` (no copy), which is what
/// lets compaction bake only the dead-heavy files and reference the rest in
/// place. `min_sequence`/`max_sequence` carry the file's commit-seq range so a
/// seq-prefix bake (`max_sequence <= T`) is well-defined.
#[derive(Debug, Clone)]
pub struct SnapshotFile {
    /// Table this manifest entry belongs to (`UUIDv7`).
    pub table_id: String,
    /// Snapshot this file is a member of (`UUIDv7`).
    pub snapshot_id: String,
    /// Object-store path of the `.vortex` data file (as returned by listing).
    pub file_path: String,
    /// Live row count in the file.
    pub row_count: i64,
    /// `ObjectMeta::size` of the file in bytes.
    pub file_size_bytes: i64,
    /// Inclusive minimum commit sequence of the rows in this file.
    pub min_sequence: i64,
    /// Inclusive maximum commit sequence of the rows in this file.
    pub max_sequence: i64,
    /// Optional end-to-end integrity digest of the file's bytes, self-describing
    /// as `"<algorithm>:<lowercase-hex>"` (e.g. `"xxh3-128:1a2b…"`). `None` when
    /// integrity checksums were disabled at flush (or for rows written before
    /// the feature). Computed once at publish and verified before first read
    /// when `integrity_checksums` is enabled. See
    /// [`crate::provider::file_digest`].
    pub digest: Option<String>,
}

/// One row of the cold-tier object-store manifest (`cayenne_cold_tier_file`).
///
/// The cold tier is the bottom of the storage cascade (RAM mem-tier →
/// local-disk warm Vortex snapshot → object-store cold). A background promotion
/// stage rewrites settled/aged warm files as read-optimized (Z-order clustered)
/// Vortex files on the cold object store and records one row here per file.
///
/// Unlike [`SnapshotFile`], cold files are **table-scoped** (not a member of any
/// snapshot directory) and append-only: a promoted file is referenced only from
/// this table, never from `cayenne_snapshot_file`. `file_url` is the *absolute*
/// object-store URL (e.g.
/// `s3://bucket/prefix/<table_name>-<table_id>/data/<promotion_id>/<id>.vortex`;
/// see [`TableMetadata::datalake_dir_segment`]), because the cold location may
/// differ from the table's warm path. The embedded
/// `statistics_blob` (serialized Vortex [`FileStatistics`]: per-column min/max/
/// null/sum) lets the scan prune cold files at listing time with no object-store
/// round-trip. `min_sequence`/`max_sequence` carry the file's commit-seq range
/// (cold files hold the oldest, fully-superseded data — below all protected
/// snapshots and retention at promotion time).
#[derive(Debug, Clone)]
pub struct ColdTierFile {
    /// Table this cold file belongs to (`UUIDv7`).
    pub table_id: String,
    /// Absolute object-store URL of the `.vortex` data file on the cold store.
    pub file_url: String,
    /// Live row count in the file (post-merge, single-version-per-key).
    pub row_count: i64,
    /// `ObjectMeta::size` of the file in bytes.
    pub file_size_bytes: i64,
    /// Inclusive minimum commit sequence of the rows in this file.
    pub min_sequence: i64,
    /// Inclusive maximum commit sequence of the rows in this file.
    pub max_sequence: i64,
    /// Serialized Vortex `FileStatistics` flatbuffer (per-column min/max/null/
    /// sum). Always populated at promotion (copied from the written footer) so
    /// listing-time pruning never falls back to a full scan.
    pub statistics_blob: Vec<u8>,
    /// Serialized PK existence bloom (`provider::pk_index::PkBloom`) over this
    /// file's live PK values, built at promotion for upsert-eligible tables so the keyset
    /// rebuild can fold cold-resident keys without scanning the cold store.
    /// `None` (non-upsert table, over the per-file cap, or a legacy row) makes
    /// the rebuild fall back to the exact cold scan. Never consulted for
    /// `DoNothing` (a false positive would wrongly drop a new row).
    pub pk_bloom: Option<Vec<u8>>,
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
    /// Whether [`Self::num_rows`] is a provably-exact live count (`true`) or a
    /// best-effort estimate that may over-count (`false`).
    ///
    /// The mem-tier checkpoint applies a `Delta` whose durable-supersede netting
    /// is best-effort, so it taints this to `false`; a full-rewrite compaction /
    /// overwrite `Set`s an authoritative count and restores `true`. Consumers that
    /// answer `COUNT(*)` from statistics (the `stats_aggregate` fold and the
    /// distributed executor-statistics reporter) must treat a `false` count as
    /// `Precision::Inexact`, so the fold declines and a real scan answers instead
    /// — preventing a drifted count from producing a wrong `COUNT(*)`.
    pub num_rows_exact: bool,
    /// Serialized per-column NDV (distinct-count) `HyperLogLog` sketches
    /// ([`crate::hll::NdvSketches`]), `None` when no NDV-tracked column has a
    /// sketch. Merged across writes register-wise; used to size distributed
    /// joins and group-bys on integer, string, and temporal (date/time/timestamp)
    /// keys. See [`crate::hll`].
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

/// One Cayenne table's disk and metastore footprint, read from the metastore's
/// own accounting rather than by walking the table directory.
///
/// The manifest (`cayenne_snapshot_file`), the deletion-vector catalog, and the
/// cold-tier manifest already record every file's size and row count, so a
/// handful of aggregate queries answer "how big is this dataset, and which
/// layer is growing" without a LIST per snapshot. That matters because the
/// tables this is sampled on are exactly the ones with thousands of files.
///
/// Every field is `i64` because that is what the metastore returns; the callers
/// that publish these as gauges saturate to `u64` at the boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableStorageStats {
    /// DISTINCT physical data files the current snapshot references.
    ///
    /// Deduplicated by path, and a file the current snapshot shares with a
    /// protected one is counted here only — the live pointer owns what it
    /// references, so `current_*` and `protected_*` partition the table rather
    /// than overlapping.
    pub current_files: i64,
    /// On-disk bytes of the current snapshot's data files.
    pub current_bytes: i64,
    /// Rows in the current snapshot's data files, before deletions apply.
    pub current_rows: i64,
    /// DISTINCT physical data files referenced by a live protected snapshot and
    /// NOT by the current one (see [`Self::current_files`] for the ownership
    /// rule).
    pub protected_files: i64,
    /// On-disk bytes of the protected snapshots' data files.
    pub protected_bytes: i64,
    /// Rows in the protected snapshots' data files, before deletions apply.
    pub protected_rows: i64,
    /// Manifest rows naming a snapshot that is no longer live — dead weight in
    /// the metastore until a compaction or overwrite prunes them.
    pub unreachable_manifest_rows: i64,
    /// Manifest rows naming a snapshot that is still live.
    ///
    /// A row is a `(snapshot, file)` pair, so this is at or above
    /// [`Self::distinct_live_files`] by the in-place reference multiplicity. Kept
    /// distinct from the file count because the unreachable remainder has to be
    /// taken against rows, not files.
    pub reachable_manifest_rows: i64,
    /// Distinct `file_path` values referenced by any live snapshot — the number
    /// of real files the reachable manifest rows describe.
    ///
    /// Always `<=` the reachable row count, and usually strictly less: a
    /// manifest row is a `(snapshot, file)` pair, and compaction deliberately
    /// references an un-baked file from a new snapshot in place rather than
    /// copying it, so one file on disk earns a row under every live snapshot
    /// that references it. Without this figure the row count reads as a file
    /// count and overstates the table's real state — by an order of magnitude on
    /// a table with a deep snapshot chain.
    pub distinct_live_files: i64,
    /// Files promoted to the cold object-store tier.
    pub cold_files: i64,
    /// Bytes of the cold-tier files.
    pub cold_bytes: i64,
    /// Rows in the cold-tier files.
    pub cold_rows: i64,
    /// Live deletion-vector files.
    pub delete_files: i64,
    /// On-disk bytes of the deletion-vector files.
    pub delete_file_bytes: i64,
    /// Tombstones recorded across those deletion-vector files.
    pub delete_file_tombstones: i64,
    /// Registered snapshot sequences (the durable protected-snapshot set).
    pub snapshot_sequences: i64,
    /// Per-file pruning-statistics rows.
    pub file_statistics_rows: i64,
    /// Re-insert records held in the metastore.
    pub insert_records: i64,
    /// Inline (level-0) data entries not yet checkpointed to Vortex files.
    pub inlined_entries: i64,
    /// Rows held in those inline entries.
    pub inlined_rows: i64,
    /// Serialized Arrow IPC bytes held inline.
    pub inlined_bytes: i64,
    /// Inline tombstone entries not yet flushed to deletion vectors.
    pub inlined_delete_entries: i64,
    /// Tombstones held in those inline entries.
    pub inlined_delete_rows: i64,
}

impl TableStorageStats {
    /// Total physical bytes the live data files occupy across the warm tiers.
    ///
    /// Safe to add because the tiers partition the file set (see
    /// [`Self::current_files`]); summing the raw manifest rows would not be.
    #[must_use]
    pub const fn live_data_bytes(&self) -> i64 {
        self.current_bytes + self.protected_bytes
    }
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
