/*
Copyright 2025-2026 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Cayenne `TableProvider` implementation.
//!
//! This module defines the main [`CayenneTableProvider`] struct (all shared
//! state and locks live on it — see the field docs, in particular `write_lock`,
//! `visibility_lock`, `listing_fence`, and `scan_state_lock`) plus its builder,
//! and re-exports the items of the submodules the original `table.rs` was
//! mechanically split into: construction (`init`), the `DataFusion` trait surface
//! (`datafusion_impl`), scan planning (`scan`), the write/CDC paths (`write`,
//! `cdc`, `on_conflict`, `deletion_commit`), the inline and in-memory CDC tiers
//! (`inline_cache`, `mem_tier_ops`), caches (`pk_cache`, `column_stats`), and
//! background work (`maintenance`, `compaction_ops`, `refresh`, `snapshot_io`).

use super::constants::{STAGING_DIR_NAME, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME};
use super::delete::{
    CayenneDeletionSink, DeletionIdentifier, DeletionVectorWriteResult, DeletionVectorWriteSpec,
    DeletionVectorWriter, FileBasedDeletionSink, InsertRecordHandling, Int64PkDeletionFilterExec,
    KeyBasedDeletionFilterExec,
};
use super::streaming::StreamingExec;
use crate::catalog::{CatalogError, CatalogResult, MetadataCatalog, SnapshotSequenceCommit};
use crate::metadata::{
    CreateTableOptions, InlinedData, InlinedDataStats, InlinedDelete, PkConflictDetection,
    SnapshotFileStatistics, TableMetadata, TableStatistics,
};
use crate::provider::scan::{CayenneAccelerationExec, round_robin_repartition_if_needed};
use crate::provider::sink::CayenneDataSink;
use crate::provider::{Error, Result};
use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    helpers::{expr_applicable_for_cols, pruned_partition_list},
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
#[cfg(test)]
use datafusion_catalog::TableProvider;
#[cfg(test)]
use datafusion_execution::cache::TableScopedPath;
use datafusion_common::stats::Precision as DFPrecision;
use datafusion_common::{
    ColumnStatistics, Constraints, DFSchema, Result as DataFusionResult, ScalarValue, Statistics,
    project_schema,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::{PartitionedFile, TableSchema, compute_all_files_statistics};
use datafusion_execution::cache::cache_manager::{
    CachedFileList, CachedFileMetadata, FileStatisticsCache,
};
use datafusion_execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, LogicalPlan, Operator, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{PhysicalExpr, create_lex_ordering, create_physical_expr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectStorePath};
use parking_lot::{Mutex as ParkingMutex, RwLock};
use roaring::RoaringBitmap;
#[cfg(test)]
use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task;
use vortex_datafusion::VortexFormat;
use vortex_datafusion::WriteShardConfig;

use super::context::CayenneContext;
use super::deletion_index::{DeletionIndex, KeyDeletionIndex};
use super::deletion_strategy::{
    Int64PkDeletionSnapshot, PkDeletionStrategy, PkDeletionStrategyWithCache, PositionBitmap,
    PositionDeletionVector, RowConverterDeletionSnapshot,
};
use super::memory_account::CayenneMemoryAccount;
use super::staging_wal::PreparedStagedAppend;
use super::vortex_format::PositionDeletionAccessPlanProvider;
use arc_swap::ArcSwap;

const POST_WRITE_MAINTENANCE_DEBOUNCE: Duration = Duration::from_millis(100);
const OBJECT_STORE_MOVE_CONCURRENCY: usize = 16;
/// Default intra-write encode-shard count for an unsorted write when no per-table
/// `cayenne_write_concurrency` is configured. Deliberately small — NOT the host
/// core count: the value is sized per table in isolation, so a high default makes
/// independent tables oversubscribe the box under concurrent CDC. Users raise
/// `cayenne_write_concurrency` explicitly when a table needs more encode
/// parallelism. See `snapshot_write_concurrency`.
pub(crate) const DEFAULT_WRITE_CONCURRENCY: usize = 4;
// Approximate per-entry `HashMap` control/allocation overhead used for the
// cache budget. The exact value is allocator-dependent, so keep this estimate
// centralized with `approx_pk_keyset_entry_bytes`.
const PK_KEYSET_CACHE_HASHMAP_ENTRY_OVERHEAD_BYTES: usize = 16;
const TABLE_STATISTICS_FULL_COLUMN_SYNC_LIMIT: usize = 256;
const PROTECTED_SNAPSHOT_AGE_WARNING_KEY_LIMIT: usize = 1024;
const MIN_CONSECUTIVE_INLIST_REWRITE_VALUES: usize = 4;
/// Upper bound on PK `IN` list cardinality that qualifies for `target_partitions = 1`.
const MAX_PK_SELECTIVE_INLIST_VALUES: usize = 32;
/// Upper bound on PK `BETWEEN` span (inclusive) for selective scan fan-out control.
const MAX_PK_SELECTIVE_RANGE_SPAN: i64 = 32;
/// Maximum tombstone keys pushed into the Vortex scan predicate as `NOT IN`.
const MAX_VORTEX_KEY_DELETE_PUSHDOWN: usize = 256;

// Submodules produced by the mechanical split of the original `table.rs`.
mod cdc;
mod column_stats;
mod compaction_ops;
mod datafusion_impl;
mod deletion_commit;
mod init;
mod inline_cache;
mod maintenance;
mod mem_tier_ops;
mod on_conflict;
mod pk_cache;
mod refresh;
mod scan;
mod snapshot_io;
mod write;

// The split submodules refer to provider-level sibling modules as `super::<mod>`;
// now that the code lives one module deeper, these imports keep those paths
// resolving (`super::compaction::...` etc. from inside a submodule).
use super::{
    compaction, delete, delta_encoding, file_pruning, fsync_tier, retention, tuning, write_budget,
};

// Re-exports preserving the original `provider::table::*` item paths and the
// names visible to `super::*` glob imports in the submodules and tests.
pub use self::cdc::CayenneCdcWrite;
#[cfg(test)]
pub(crate) use self::cdc::SEQ_RESERVE_BLOCK;
pub(crate) use self::cdc::{PreparedOnConflictDeletionPublish, SeqAllocator, reserve_sequences_in};
pub(crate) use self::column_stats::{ColumnStatsAccumulator, RowCountUpdate};
#[cfg(test)]
pub(crate) use self::inline_cache::InlineMemtablePressure;
#[cfg(test)]
pub(crate) use self::inline_cache::{
    INLINE_FLUSH_MAX_BYTES, INLINE_FLUSH_MAX_ROWS, INLINE_FLUSH_MAX_SEGMENTS,
    INLINE_MAX_BUFFER_BYTES, INLINE_MAX_ROWS, inline_memtable_pressure,
};
#[cfg(test)]
pub(crate) use self::on_conflict::PreparedInsertStream;
pub(crate) use self::on_conflict::{
    OnConflictDeletions, OnConflictUpdate, PostValidationState, record_cayenne_write_phase,
};
pub(crate) use self::scan::rewrite_consecutive_inlist_to_range;
// Originally-private items (now `pub(super)` in their submodule) re-imported so
// `super::*` / `super::<Item>` references from sibling submodules keep working.
use self::cdc::InlinedDurableCommit;
use self::column_stats::CachedTableStatistics;
use self::datafusion_impl::{format_bytes, format_bytes_per_sec};
use self::deletion_commit::{
    PROTECTED_MERGE_MAX_WIDTH, PROTECTED_TIER_BASE_BYTES, PROTECTED_TIER_GROWTH,
    PkDeletionSnapshot, ProtectedSnapshotScan, deserialize_delete_keys_from_ipc,
    deserialize_ipc_to_batch, pk_deletion_snapshot_for_strategy, protected_snapshot_size_tier,
    select_protected_snapshot_merge_tier, serialize_batches_to_ipc, subset_merge_write_shape,
};
#[cfg(test)]
use self::deletion_commit::{serialize_delete_keys_to_ipc, tombstone_format};
#[expect(unused_imports)]
use self::inline_cache::{
    InlinedCache, InlinedViewEntry, inline_memtable_pressure_with_thresholds,
};
#[expect(unused_imports)]
use self::maintenance::{
    BoundedWarningKeys, PostWriteMaintenance, PostWriteMaintenanceState, RetentionFailureAction,
    SnapshotMaintenanceTrigger, duration_millis_saturating, oldest_protected_snapshot_age,
    protected_snapshot_age, protected_snapshot_maintenance_trigger,
    should_warn_protected_snapshot_age,
};
#[expect(unused_imports)]
use self::on_conflict::{
    BatchValidationResult, ExtractedPrimaryKeys, InlineAwareDeletionSink, InlinedDataRewrite,
    OnConflictContext, OnConflictDeletionUpdate, OnConflictExt, OnConflictValidationStream,
    PkKeysetInvalidatingDeletionSink, UpsertOptions,
};
#[expect(unused_imports)]
use self::pk_cache::{
    CachedPkIndex, CachedPkKeyset, InlinedDeletionMaps, MAX_PENDING_TOMBSTONE_DELTA_KEYS,
    MAX_PENDING_TOMBSTONE_DELTAS, PK_BLOOM_NUM_HASHES, PK_INDEX_PERSIST_MAX_BYTES,
    PK_INDEX_SIDECAR_MAGIC, PK_INDEX_SIDECAR_VERSION, PendingTombstoneDeltas, PkBloom,
    PkExistenceRef, RowLocation, TombstoneDelta, approx_captured_file_bytes,
    approx_pk_keyset_entry_bytes, deserialize_pk_bloom_sidecar, pk_bloom_hash,
    serialize_pk_bloom_sidecar,
};
#[expect(unused_imports)]
use self::scan::{
    SnapshotFilesForScan, SnapshotScanListingRequest, extract_integer_literal, is_literal_like,
    matches_column, pk_column_equals_literal, pk_selective_in_or_range,
    rewrite_consecutive_inlist_to_range_if_needed, rewritten_scan_filters,
};

/// Cayenne table provider that reads from Vortex virtual files.
///
/// This provider manages a table composed of multiple "virtual files", where each file
/// is a Vortex `ListingTable` at its own directory.
///
/// Currently, the implementation uses a single `ListingTable` that scans the entire table
/// directory. In a future optimization, this could be enhanced to manage multiple
/// `ListingTables` (one per virtual file) and union their results for better control
/// over file-level operations.
pub struct CayenneTableProvider {
    /// Table metadata from the catalog
    table_metadata: TableMetadata,
    /// Reference to the metadata catalog for file operations
    catalog: Arc<dyn MetadataCatalog>,
    /// Underlying Vortex `ListingTable` that scans all virtual files in the table directory.
    /// Note: Each `DataFile` in the catalog represents a subdirectory (virtual file),
    /// but this `ListingTable` currently scans all of them together.
    ///
    /// Held in an [`ArcSwap`] so synchronous `TableProvider` trait methods
    /// (`supports_filters_pushdown` and `statistics`) get a wait-free snapshot
    /// of the current `ListingTable`, and writers can atomically install a new
    /// one without blocking readers' Arc-loads. Read/write *coordination* with
    /// the append-side write barrier (issue #10125) lives in
    /// [`Self::listing_fence`], not in the `ArcSwap` itself.
    listing_table: Arc<ArcSwap<ListingTable>>,
    /// Read/write fence that synchronizes [`Self::scan`] with the append-side
    /// write barrier described in issue #10125 §6.4.
    ///
    /// Scans take `listing_fence.read().await` and hold it across the inner
    /// `DataFusion` listing call so that concurrent file-move + listing-table
    /// swap by a writer cannot interleave with the listing operation. The
    /// writer barrier takes `listing_fence.write().await` for the duration of
    /// its move + cache-invalidate + Arc swap.
    ///
    /// Sync `TableProvider` methods (`statistics`, `supports_filters_pushdown`)
    /// do *not* take the fence — they read a snapshot of the listing table
    /// atomically via [`Self::listing_table`] and never observe partial state.
    listing_fence: Arc<tokio::sync::RwLock<()>>,
    /// File statistics cache used by the direct snapshot scan planner. This
    /// replaces the per-scan `ListingTable` cache while preserving repeated
    /// scan behavior when `collect_statistics` asks us to read Vortex footers.
    scan_file_statistics: Arc<dyn FileStatisticsCache>,
    /// Table-level Vortex statistics cache loaded from the metastore and maintained
    /// after writes. The optimizer-facing `Statistics` and raw `TableStatistics`
    /// blob live under the same lock so clears and updates publish both views
    /// together. This gives `DataFusion` synchronous access to Cayenne stats while
    /// allowing `persist_table_stats` to skip a steady-state catalog read.
    table_statistics: Arc<RwLock<CachedTableStatistics>>,
    /// Serializes the read/merge/upsert/publish stats persistence cycle so
    /// concurrent maintenance tasks cannot merge from the same cached base and
    /// overwrite each other's row-count or column-stat deltas.
    table_statistics_persistence_lock: Arc<tokio::sync::Mutex<()>>,
    /// Optional retention filters that should be applied immediately after writes.
    retention_filters: Vec<Expr>,
    /// Optional builder to construct time-based retention filter.
    ///
    /// Used for period-based retention (e.g. `retention_period: 30d`).
    time_retention_filter_builder: Option<super::retention::TimeRetentionFilterBuilder>,
    /// Context containing Vortex format with caches and configuration.
    /// If the same context is reused across multiple instances, all internal operations
    /// share the same footer and segment caches, enabling shared memory management.
    context: Arc<CayenneContext>,
    /// Strategy for primary key-based deletion filtering.
    /// Contains the deletion caches specific to each strategy variant.
    pk_deletion_strategy: PkDeletionStrategyWithCache,
    /// `RowConverter` for converting primary key columns to byte representation.
    /// Only set for tables with composite or non-integer primary keys.
    pk_row_converter: Option<Arc<RowConverter>>,
    /// Indices of primary key columns in the table schema.
    pk_column_indices: Vec<usize>,
    /// Write lock to serialize insert operations and prevent concurrent write races.
    /// This ensures that:
    /// - Only one `insert()` runs at a time per table
    /// - Parallel chunk writes complete before listing table refresh
    /// - Retention filters are applied atomically after writes
    /// - Statistics are consistent and up-to-date
    ///
    /// Uses `tokio::sync::Mutex` because the lock is held across `.await` points during insert operations.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes staged append visibility flips after Stage A has durably
    /// written its isolated staging WAL. CDC pipelining releases `write_lock`
    /// after Stage A, then Stage B takes this lock for move + listing cache
    /// invalidation so readers still observe one ordered visibility boundary.
    visibility_lock: Arc<tokio::sync::Mutex<()>>,
    /// Makes the deletion-view, protected-snapshots, and inlined-data
    /// visibility appear to change atomically to scans.
    ///
    /// Scans take **read** while capturing the `(deletion_snapshot,
    /// protected_snapshots, inlined_batches)` tuple (a few `ArcSwap`/atomic
    /// loads plus a cache-hit inlined read); writers take **write** around the
    /// matching publish.
    scan_state_lock: Arc<tokio::sync::RwLock<()>>,
    /// Optional object store configuration for remote storage (e.g., S3 Express One Zone).
    /// When set, this object store is registered with `SessionContext` for data file operations.
    object_store_config: Option<crate::metadata::ObjectStoreConfig>,
    /// `RuntimeEnv` identities where `object_store_config` has already been
    /// verified/registered. This avoids probing the registry on every scan in
    /// the common case while still handling distinct query runtimes correctly.
    object_store_registered_runtime_envs: Arc<ParkingMutex<HashSet<usize>>>,
    /// Current snapshot ID, updated after compaction operations.
    ///
    /// This is separate from `table_metadata.current_snapshot_id` because compaction
    /// creates a new snapshot but we don't want to modify the original `TableMetadata`.
    /// Uses `RwLock` for concurrent reads during normal operations with occasional
    /// writes on compaction. The lock is held briefly for string operations.
    current_snapshot_id: Arc<RwLock<String>>,
    /// Protected snapshot IDs that should skip deletion filtering.
    ///
    /// When data is inserted while pending deletions exist, the new data is written
    /// to a new snapshot that is "protected" - deletions that existed at the time
    /// of insert should not apply to this snapshot's data.
    ///
    /// Maps `snapshot_id` -> `minimum_sequence` (all deletes with seq <= `min_seq` don't apply).
    /// At scan time, data from these snapshots is scanned without deletion filtering.
    /// Snapshot-id → max-delete-sequence-at-creation. Wait-free reads via
    /// `ArcSwap`: scan paths take `Arc::clone` instead of cloning the
    /// `HashMap`; writes use `rcu` to publish a copy-on-write update.
    protected_snapshots: Arc<ArcSwap<HashMap<String, i64>>>,
    /// Table-scoped warning dedupe for protected snapshot ids that cannot
    /// provide a `UUIDv7` timestamp for age-triggered maintenance.
    protected_snapshot_age_warning_keys: Arc<ParkingMutex<BoundedWarningKeys>>,
    /// Cached visible primary-key set for auto conflict detection.
    ///
    /// The first auto-mode insert still scans existing data to build the set;
    /// later serialized writes reuse it and publish successful write deltas.
    /// Delete paths invalidate this cache because arbitrary predicates can
    /// remove keys without telling us which keys were affected.
    pk_keyset_cache: Arc<ParkingMutex<Option<CachedPkIndex>>>,
    /// Accounts the keyset + deletion indexes against the query memory
    /// pool. `Arc`-shared with provider clones so they update one reservation.
    table_memory: Arc<CayenneMemoryAccount>,
    /// Coalesces inline-memtable checkpoint checks spawned after inline writes.
    /// The check takes `write_lock` in the background after the scheduling
    /// writer returns, so inline commits do not hold the writer lock while
    /// flushing the memtable to Vortex.
    inline_checkpoint_scheduled: Arc<AtomicBool>,
    /// Cached inlined row count. Maintained while the process is running so
    /// append-heavy inline CDC writes don't query the metastore after every
    /// burst just to decide whether to checkpoint.
    inlined_row_count: Arc<AtomicI64>,
    /// Inline-memtable cache generation counter.
    ///
    /// Incremented (with `Release` ordering) by every
    /// `commit_inlined_data_mutation` and
    /// `clear_inlined_metadata_after_checkpoint`. [`Self::inlined_cache`] is
    /// valid only when its stored generation matches this counter.
    inlined_generation: Arc<AtomicU64>,
    /// Inline-memtable cache STRUCTURAL epoch.
    ///
    /// A strict subset of the `inlined_generation` bumps: incremented (with
    /// `Release` ordering, always paired with a generation bump via
    /// [`Self::bump_inlined_structural_epoch`]) only by mutations that can
    /// retroactively invalidate an already-materialized [`InlinedCache`] entry —
    /// an inline rewrite/removal (`removed_rows > 0`), a newly published
    /// tombstone, a checkpoint clear, an overwrite, or open-time recovery. Pure
    /// appends bump only the generation. A cache miss whose cached
    /// `structural_epoch` still equals this counter therefore proves the only
    /// changes since were appends, so `populate_inlined_cache` can extend the
    /// cached view with just the new rows instead of rebuilding it from the
    /// whole corpus. See the [`InlinedCache`] "Incremental maintenance contract".
    inlined_structural_epoch: Arc<AtomicU64>,
    /// Count of staged inline-conflict tombstones written with `published =
    /// false` whose owning snapshot has not yet finalized (flipped the flag).
    ///
    /// Incremented in `prepare_on_conflict_deletions_for_staged_snapshot` when a
    /// tombstone is staged inert, decremented in
    /// `publish_prepared_on_conflict_deletions` once it is durably published.
    /// The inline checkpoint (`checkpoint_inlined_data`) DEFERS while this is
    /// non-zero: a checkpoint flushes inline data to a file (NOT applying an
    /// inert tombstone, which the read filter skips) and then clears every
    /// tombstone — so running it during the staged window would flush the old
    /// inline row to a file AND drop the tombstone, resurfacing the old version
    /// once the replacement publishes. Deferring is safe: the pressure stays
    /// high and the next inline insert reschedules the checkpoint, by which time
    /// the fast backgrounded finalize has published and decremented this.
    pending_inline_tombstones: Arc<AtomicU64>,
    /// Published inline-visibility watermark: the highest inlined-entry
    /// `sequence_number` whose in-memory visibility has been published.
    ///
    /// A freshly committed inlined entry is durably written to the catalog with
    /// a sequence strictly greater than this watermark, but stays *invisible* to
    /// scans until the writer advances the watermark to that sequence under
    /// `scan_state_lock.write()` — atomically with the paired file
    /// deletion-cache update.
    published_inlined_seq: Arc<AtomicI64>,
    /// In-memory sequence allocator (lever B2). The single source of truth for
    /// every sequence number this table hands out — the staged/sync on-conflict
    /// paths, the inline-insert path, the checkpoint flush, the publish path,
    /// and the DML delete sinks all route through it so memory and the DB row
    /// never diverge in a way that reorders handouts. Behind a
    /// `tokio::sync::Mutex` because a refill awaits the metastore `UPDATE`; the
    /// common (in-block) path takes the lock, does arithmetic, drops it (no
    /// `await`). Shared across `clone_for_write` clones — the provider is a
    /// per-table singleton, so all writers of one table share one allocator.
    seq_allocator: Arc<tokio::sync::Mutex<SeqAllocator>>,
    /// Inline-conflict tombstones (Option D) that Stage-B has activated IN
    /// MEMORY but whose DURABLE `published = 1` flip has been deferred (cycle-4
    /// lever b1★ — Stage-B writer-free).
    ///
    /// # Why this exists
    ///
    /// The read filter (`load_inlined_deletion_maps`) decides whether a tombstone
    /// hides its old inline copy by consulting its durable `published` flag. The
    /// pre-b1★ Stage-B finalize made the tombstone visible by issuing a SEPARATE
    /// autocommit `UPDATE … SET published = 1` (`publish_tombstone_flip`, 88 ms)
    /// while holding the visibility lock + listing fence — and that single-
    /// statement WAL-writer grab ALTERNATED with the next batch's Stage-A
    /// `BEGIN IMMEDIATE`, so each waited ≈ the other's duration (the 463 ms
    /// `stage_tombstone_prepare` lock-wait). b1★ removes Stage-B's durable write
    /// entirely: Stage-B instead (a) inserts the id here so readers apply the
    /// tombstone IMMEDIATELY in memory, and (b) enqueues the id in
    /// [`Self::pending_durable_tombstone_flips`] so the DURABLE flip rides the
    /// NEXT batch's Stage-A folded `BEGIN IMMEDIATE` transaction (or the idle-
    /// table maintenance drain). Stage-B then performs ZERO metastore writes.
    ///
    /// # Why deferring the DURABLE flip is crash-safe
    ///
    /// The durable flip is now purely a convergence step; durability/recovery
    /// rests entirely on the UNCHANGED Option-D invariant (`correctness_audit.md`
    /// §5/§7): a tombstone is written durably `published = false` in Stage-A and
    /// its replacement files' staging WAL is durable BEFORE it; Stage-B's
    /// `apply_under_held_barrier` makes the replacement files durable and removes
    /// the WAL. So at ANY crash point a durable `published = 0` tombstone whose
    /// replacement is durable is healed on reopen by `publish_orphan_inlined_deletes`
    /// (flips ALL `published = 0` → 1 unconditionally) — exactly the existing
    /// "crash before `finish()`" recovery. The in-memory set is lost on crash,
    /// which is correct: it only ever ADVANCED visibility past the durable flag,
    /// never the reverse, so losing it cannot resurface a row (the orphan sweep
    /// re-publishes). No new vanish/double-apply window.
    ///
    /// # Why immediate in-memory visibility avoids a transient duplicate
    ///
    /// If the durable flip were merely deferred WITHOUT this in-memory channel,
    /// the read filter (which reads `published = 1` from SQL) would NOT apply the
    /// tombstone in the inter-batch gap, leaving the old inline row AND its file
    /// replacement both visible — a transient duplicate. This set closes that gap:
    /// `load_inlined_deletion_maps` applies a tombstone whose id is here even
    /// though it is durably `published = 0`. Populated under the listing fence in
    /// `publish_prepared_on_conflict_deletions`, atomically with the structural-
    /// epoch bump, so the cache rebuilds and applies it exactly when the
    /// replacement is in the listing.
    ///
    /// Shared across writer clones (like `pending_inline_tombstones`).
    inlined_locally_published: Arc<ParkingMutex<HashSet<String>>>,
    /// Durable `published = 1` flips owed for tombstones in
    /// [`Self::inlined_locally_published`] but not yet persisted (cycle-4 lever
    /// b1★). Drained by (a) the NEXT staged batch's Stage-A folded transaction
    /// (`commit_on_conflict_deletions_with_tombstone` applies them as extra
    /// `UPDATE … SET published = 1` statements in the SAME `BEGIN IMMEDIATE`, so
    /// they cost no extra writer acquisition), and (b) the background maintenance
    /// tick (`run_maintenance_state`) so a table that goes IDLE (no next batch)
    /// still converges within a bounded time. Once a flip is durably committed,
    /// its id is removed from BOTH this queue and `inlined_locally_published`.
    /// Shared across writer clones.
    pending_durable_tombstone_flips: Arc<ParkingMutex<Vec<String>>>,
    /// Published-but-not-yet-baked inline tombstone REMOVALS, so the inline-cache
    /// delta path can apply them to the structurally-shared base entries without
    /// a structural-epoch bump + O(corpus) full rebuild (cycle-5 TASK 1).
    ///
    /// Before cycle-5, every published tombstone bumped the structural epoch,
    /// forcing the next inline-cache miss to full-rebuild. Under sustained CDC
    /// EVERY upsert batch publishes a tombstone, so the delta path (added for
    /// pure appends) almost never fired (bench #4: 16,471 full rebuilds vs 10
    /// delta populates). A published tombstone only ever REMOVES rows from the
    /// cached view, so it is delta-safe: this queue records the removal (the
    /// deleted keys + `delete_sequence`, both already in hand at publish), the
    /// publish bumps only the generation, and `extend_inlined_cache_delta`
    /// re-filters the reused base entries against just these keys. Bounded by
    /// [`MAX_PENDING_TOMBSTONE_DELTAS`] / [`MAX_PENDING_TOMBSTONE_DELTA_KEYS`]
    /// (over-cap ⇒ next miss full-rebuilds + resets) and drained as stored caches
    /// bake the removals in; cleared on checkpoint/overwrite/recovery. Shared
    /// across writer clones.
    pending_tombstone_deltas: Arc<ParkingMutex<PendingTombstoneDeltas>>,
    /// Cached deserialized inline-memtable batches.
    ///
    /// A generation-matched hit in [`Self::read_inlined_batches`] avoids the
    /// Arrow IPC decode and two metastore round-trips that the function would
    /// otherwise pay on every scan. Stored as `Arc<ArcSwap<…>>` so writer
    /// clones (via [`Self::clone_for_write`]) share the same cache entry and
    /// can invalidate it for all concurrent readers with a single store.
    inlined_cache: Arc<ArcSwap<InlinedCache>>,
    /// In-memory CDC durability tier (`cdc_durability: memory`). A sibling
    /// `ArcSwap` of [`Self::inlined_cache`]: in memory mode the inline CDC write
    /// path appends each batch here (an O(1) `Arc` swap) instead of persisting a
    /// per-batch durable BLOB, and the source slot ack is deferred to a
    /// periodic/cap-triggered checkpoint. Empty (and never appended to) in file
    /// mode, so file-mode reads/writes are byte-identical. Shared across writer
    /// clones so every clone observes the same tier.
    mem_tier: Arc<ArcSwap<crate::provider::mem_tier::MemTier>>,
    /// Serializes mem-tier checkpoints (spills) for this table so a single
    /// checkpoint is in flight at a time. The write path uses
    /// `try_lock()` on this to detect "a checkpoint is already running" and take
    /// the spill-then-fallback path rather than growing the tier unboundedly
    /// (the OOM-safety guard). Shared across writer clones.
    mem_checkpoint_lock: Arc<tokio::sync::Mutex<()>>,
    /// Cross-layer handle the runtime installs in memory mode so
    /// `checkpoint_mem_tier` can advance the source slot AFTER the durable fence
    /// (the slot-deferral correctness seam). `None` in file mode (and for
    /// providers the runtime did not wire up), where the slot advances per-batch
    /// via the normal committer. Shared across writer clones.
    slot_advancer: Arc<ParkingMutex<Option<Arc<dyn crate::provider::mem_tier::SlotAdvancer>>>>,
    /// Per-table RAM-tier byte cap for memory mode (`cayenne_cdc_mem_tier_max_bytes`,
    /// or a memory-aware default when 0). `u64::MAX` when the config value is
    /// non-positive and no default applies (effectively no per-table cap — the
    /// global budget still bounds aggregate RAM).
    mem_tier_max_bytes: u64,
    /// Per-table RAM-tier age cap in ms for memory mode
    /// (`cayenne_cdc_mem_tier_max_age_ms`); 0 disables the age trigger.
    mem_tier_max_age_ms: u64,
    /// Approximate count of new Vortex files created in the *current* snapshot
    /// since the last successful compaction pass (or since table open).
    /// Used as a cheap early-out in `run_one_compaction_pass` so that during
    /// the common "accumulation phase" of many small appends we avoid the
    /// expensive full snapshot listing + picker decision on every write.
    /// Reset to 0 after a compaction rewrite. Conservative: can only cause
    /// extra listings, never missed compactions.
    new_files_since_last_compaction: Arc<AtomicUsize>,
    /// Side-channel carrying `(snapshot_id, ObjectMeta of moved files)` from a
    /// current-snapshot staged move, so the next
    /// `publish_current_snapshot_files_changed_under_held_fence` can DELTA-APPLY
    /// them onto the `DataFusion` list-files cache instead of evicting the whole
    /// snapshot-directory listing and forcing a full re-LIST.
    ///
    /// Written by `move_staging_files_local` / `move_staging_files_s3` (which
    /// know exactly the files they moved) and `take()`n by the publish function —
    /// both run inside the SAME held `listing_fence.write()` critical section, so
    /// the hand-off is never racy and never crosses a fence boundary. `take()`
    /// clears it on consume, so a publish that runs without a preceding move (a
    /// compaction/overwrite refresh, or a standalone publish) sees `None` and
    /// safely falls back to the whole-directory eviction. The recorded
    /// `snapshot_id` is re-checked against the live current snapshot at consume
    /// time, so a stale entry left by a move whose publish was skipped can never
    /// be applied onto a different snapshot's listing. Only current-snapshot moves
    /// populate it; staged→new-snapshot moves (compaction/overwrite) do not.
    #[expect(clippy::type_complexity)]
    last_moved_snapshot_files: Arc<ParkingMutex<Option<(String, Vec<ObjectMeta>)>>>,
    /// Tracks whether a staging WAL may be present (for fast-path short-circuit
    /// of expensive S3 GET / local FS read in `ensure_no_incomplete_write`).
    ///
    /// Initialized to `true` so the check always runs at table open (to detect
    /// incomplete writes from prior crashes). Set to `false` after a clean check
    /// or successful recovery/remove. Set to `true` when `write_staging_wal_for`
    /// (or `write_staging_wal_for_target`) succeeds; set to `false` when
    /// `remove_staging_wal_for` succeeds. If a
    /// `PreparedStagedAppend` is dropped without cleanup the flag stays `true`,
    /// forcing the next writer to re-check disk and recover or error.
    staging_wal_present: Arc<AtomicBool>,
    /// Tracks whether the `_staging/` directory may contain files from a
    /// previous or in-progress write. Used to fast-path `clear_staging_dir`
    /// (which does an expensive recursive delete or S3 List+DeletePrefix on
    /// every append). Initialized true so the first use after open/restart
    /// always cleans any orphan files left by a crash between a clear and the
    /// subsequent WAL write (the pre-WAL orphan case).
    ///
    /// Set true immediately before any code path that will write Vortex files
    /// into the staging directory. Set false after a successful clear or after
    /// a successful staged-append finalize (move + WAL removal) that empties
    /// staging. The `write_lock` serializes writers, so the flag is a reliable
    /// "we left it clean" signal between appends in the same process.
    staging_may_have_files: Arc<AtomicBool>,
    /// Staging snapshot IDs whose WALs belong to prepared appends in this
    /// process. `ensure_no_incomplete_write` ignores these WALs so CDC Stage A
    /// can continue while a previous Stage B is pending; after restart the set
    /// is empty, so the same WALs are treated as crash-recovery input.
    inflight_staging_appends: Arc<ParkingMutex<HashSet<String>>>,
    /// Serializes concurrent compaction passes on this table so a write-driven
    /// inline trigger and the background scheduler can't both rewrite the
    /// current snapshot at the same time. Held across the *entire* trigger
    /// sequence — up to `compaction_max_levels` consecutive snapshot rewrites
    /// per call to [`Self::maybe_compact_small_files`] — so that competing
    /// triggers no-op via `try_lock` rather than chaining onto a backlog. The
    /// per-table write lock continues to serialize ordinary inserts
    /// independently.
    compaction_lock: Arc<tokio::sync::Mutex<()>>,
    /// Coalesces write-driven compaction notifications so a high-ingest table
    /// does not spawn one background compaction task per append while a prior
    /// notification is still pending.
    post_write_compaction_scheduled: Arc<AtomicBool>,
    /// Coalesces write-driven listing refreshes and table-statistics updates
    /// so CDC catch-up bursts do not synchronously pay metastore/listing work
    /// on every append.
    post_write_maintenance: Arc<PostWriteMaintenance>,
    /// Per-table background compaction task, populated by
    /// [`Self::spawn_background_compaction`]. Held by `Arc<OnceLock<…>>` so it
    /// survives [`Self::clone_for_write`] and shares its drop signal across
    /// all clones — when the last `Arc<CayenneTableProvider>` is dropped the
    /// compactor's `JoinHandle::abort` runs and the background task exits.
    background_compactor: Arc<std::sync::OnceLock<super::compaction::BackgroundCompactor>>,
    /// Per-table periodic mem-tier checkpoint task (`cdc_durability: memory`),
    /// populated by [`Self::spawn_background_mem_tier_checkpoint`]. Held by
    /// `Arc<OnceLock<…>>` exactly like [`Self::background_compactor`] so it
    /// survives [`Self::clone_for_write`] and shares its drop signal across
    /// clones; when the last `Arc<CayenneTableProvider>` drops, the task aborts.
    /// `None`/never-spawned in file mode (and when the interval is 0).
    background_mem_tier_checkpointer:
        Arc<std::sync::OnceLock<super::compaction::BackgroundMemTierCheckpointer>>,
}

/// Builder for constructing a `CayenneTableProvider` with optional configuration.
///
/// Use this builder to configure optional parameters before opening an existing table
/// or creating a new one.
///
/// # Example
///
/// ```ignore
/// // Open an existing table
/// let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
///     .with_retention_filters(filters)
///     .with_object_store(config)
///     .open("my_table").await?;
///
/// // Create a new table
/// let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
///     .with_retention_filters(filters)
///     .create(options).await?;
/// ```
#[derive(Clone)]
pub struct CayenneTableProviderBuilder {
    catalog: Arc<dyn MetadataCatalog>,
    runtime_env: Arc<RuntimeEnv>,
    retention_filters: Vec<Expr>,
    time_retention_filter_builder: Option<super::retention::TimeRetentionFilterBuilder>,
    object_store_config: Option<crate::metadata::ObjectStoreConfig>,
    context: Option<Arc<CayenneContext>>,
}

impl CayenneTableProviderBuilder {
    /// Create a new builder with the required catalog and shared `RuntimeEnv`.
    #[must_use]
    pub fn new(catalog: Arc<dyn MetadataCatalog>, runtime_env: Arc<RuntimeEnv>) -> Self {
        Self {
            catalog,
            runtime_env,
            retention_filters: Vec::new(),
            time_retention_filter_builder: None,
            object_store_config: None,
            context: None,
        }
    }

    /// Set retention filters that will be applied after writes.
    ///
    /// These filters cause automatic deletion of rows matching the filter criteria
    /// after each write operation.
    #[must_use]
    pub fn with_retention_filters(mut self, filters: Vec<Expr>) -> Self {
        self.retention_filters = filters;
        self
    }

    /// Set a time-based retention filter builder.
    ///
    /// When set, this builder is used to apply time-based retention filter at scan time.
    #[must_use]
    pub fn with_time_retention_filter_builder(
        mut self,
        builder: super::retention::TimeRetentionFilterBuilder,
    ) -> Self {
        self.time_retention_filter_builder = Some(builder);
        self
    }

    /// Set the object store configuration for remote storage.
    ///
    /// Used for S3 Express One Zone storage where data files are stored remotely
    /// while metadata remains on local disk.
    #[must_use]
    pub fn with_object_store(mut self, config: crate::metadata::ObjectStoreConfig) -> Self {
        self.object_store_config = Some(config);
        self
    }

    /// Set a shared [`CayenneContext`] for this table provider.
    ///
    /// Use this to share a single context (with caches) across multiple table providers
    /// This avoids creating separate caches per partition
    #[must_use]
    pub fn with_context(mut self, context: Arc<CayenneContext>) -> Self {
        self.context = Some(context);
        self
    }

    /// Open an existing table by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be found in the catalog or if the listing
    /// table cannot be created.
    pub async fn open(self, table_name: &str) -> Result<CayenneTableProvider> {
        CayenneTableProvider::new_internal(
            table_name,
            self.catalog,
            self.retention_filters,
            self.time_retention_filter_builder,
            self.object_store_config,
            self.runtime_env,
            self.context,
        )
        .await
        .map_err(Into::into)
    }

    /// Create a new table with the given options.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created in the catalog.
    pub async fn create(self, options: CreateTableOptions) -> CatalogResult<CayenneTableProvider> {
        let table_name = options.table_name.clone();
        let _table_id = self.catalog.create_table(options).await?;

        CayenneTableProvider::new_internal(
            &table_name,
            self.catalog,
            self.retention_filters,
            self.time_retention_filter_builder,
            self.object_store_config,
            self.runtime_env,
            self.context,
        )
        .await
    }
}

impl std::fmt::Debug for CayenneTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneTableProvider")
            .field("table_metadata", &self.table_metadata)
            .finish_non_exhaustive()
    }
}

impl CayenneTableProvider {
    /// Returns the name of this table.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_metadata.table_name
    }

    /// Returns the base path for this table's data.
    #[must_use]
    pub(crate) fn table_path(&self) -> &str {
        &self.table_metadata.path
    }

    /// Returns the table ID from the catalog.
    #[must_use]
    pub(crate) fn table_id(&self) -> &str {
        &self.table_metadata.table_id
    }

    /// Returns a reference to the write lock for serializing insert operations.
    #[must_use]
    pub(crate) fn write_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.write_lock
    }

    #[must_use]
    pub(crate) fn write_lock_arc(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.write_lock)
    }

    #[must_use]
    pub(crate) fn visibility_lock_arc(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.visibility_lock)
    }

    #[must_use]
    pub(crate) fn new_staging_snapshot_id() -> String {
        Self::new_staging_snapshot_id_pair().0
    }

    #[must_use]
    pub(crate) fn new_staging_snapshot_id_pair() -> (String, String) {
        let snapshot_id = uuid::Uuid::now_v7().to_string();
        (format!("{STAGING_DIR_NAME}/{snapshot_id}"), snapshot_id)
    }

    #[must_use]
    fn is_staging_snapshot_id(snapshot_id: &str) -> bool {
        snapshot_id == STAGING_DIR_NAME
            || snapshot_id
                .strip_prefix(STAGING_DIR_NAME)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    pub(crate) fn register_inflight_staging_append(&self, staging_snapshot_id: &str) {
        self.inflight_staging_appends
            .lock()
            .insert(staging_snapshot_id.to_string());
    }

    pub(crate) fn unregister_inflight_staging_append(&self, staging_snapshot_id: &str) {
        self.inflight_staging_appends
            .lock()
            .remove(staging_snapshot_id);
    }

    pub(crate) fn staging_append_is_inflight(&self, staging_snapshot_id: &str) -> bool {
        self.inflight_staging_appends
            .lock()
            .contains(staging_snapshot_id)
    }

    pub(crate) fn has_inflight_staging_appends(&self) -> bool {
        !self.inflight_staging_appends.lock().is_empty()
    }

    pub(crate) fn staging_wal_present(&self) -> &AtomicBool {
        &self.staging_wal_present
    }

    pub(crate) fn staging_may_have_files(&self) -> &AtomicBool {
        &self.staging_may_have_files
    }

    #[must_use]
    pub(crate) fn target_file_size_bytes(&self) -> usize {
        self.context.target_file_size_bytes()
    }

    /// Returns a cheap clone that shares the underlying table state for write operations.
    #[must_use]
    pub fn clone_for_write_operations(&self) -> Self {
        self.clone_for_write()
    }
}

#[cfg(test)]
mod tests;
