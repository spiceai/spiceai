# Cayenne

A lakehouse format for the Vortex columnar file format. Combines pluggable metastore backends (SQLite, Turso) for transactional metadata with Vortex files for columnar data, plus an LSM-style level-0 inline-data tier that absorbs small writes without writing data files.

## Overview

Cayenne provides a lakehouse format that enables efficient CRUD operations on columnar data:

- **Pluggable metastore backends** (`metastore::sqlite::SqliteMetastore`, optional `metastore::turso::TursoMetastore`) for transactional metadata with `BEGIN ... COMMIT` semantics, each managing a per-instance connection pool to lift the cross-table write/read concurrency ceiling.
- **Vortex data files** as the persistent columnar tier, with configurable target file size, compression, and concurrent upload fan-out.
- **Inline-data memtable** (`cayenne_inlined_data` / `cayenne_inlined_delete` tables) absorbs small bursts directly in the metastore as Arrow IPC blobs, flushed to Vortex once accumulated rows / segments / bytes exceed configurable thresholds.
- **Deletion vectors** stored as Arrow IPC files for position-based deletion, plus a bloom-prefiltered in-memory PK index (`DeletionIndex` for Int64 PKs / `KeyDeletionIndex` for composite or non-integer PKs) for key-based deletion. Each index entry *fuses* the highest delete sequence and the highest upsert re-insertion sequence into one `TombstoneEntry`, so the scan hot path answers "deleted, and re-inserted after that?" in a single probe. Sequence-numbered for Iceberg-style upsert semantics.
- **In-memory CDC durability tier** (`cdc_durability: memory`, the default for the small-write CDC profile) absorbs CDC bursts in RAM and defers the source-slot ack to a periodic/cap-triggered checkpoint, with a two-phase off-fence checkpoint that keeps the listing fence out of the encode + metastore I/O.
- **Staging WAL** (`provider/staging_wal.rs`) provides crash-safe append commit via tmp+fsync+rename of the WAL marker, atomic rename of staged Vortex files into the current snapshot, and self-healing recovery on the next provider open.
- **Tiered small-files compaction** (`provider/compaction.rs`) triggered best-effort after writes and periodically by a per-table background compactor, gated by a shared per-accelerator semaphore so a fleet of tables can't oversubscribe the writer pool.
- **CDC apply pipelining** (`provider/mutation_writer::write_cdc_pipelined`): Stage A writes Vortex files into the staging dir under the staging WAL; Stage B (move + listing-cache invalidation, plus on-conflict deletion publish for upserts) is spawned as a finalize task so the next burst's Stage A can begin work. On-conflict upserts pipeline too — they stage into a protected snapshot whose deletions are resolved and published by the finalize task; a burst that replaces *inlined* rows (which the staged on-conflict commit cannot represent, only known after validation) stages optimistically and then falls back to a synchronous publish that reuses the staged files (the `cdc_path_inline_fallback` write-phase metric tracks how often this happens). Stage A and Stage B always preserve burst order.
- **Sequence-based ordering** (Iceberg-style) for correct delete/insert visibility across snapshots.
- **Partitioning** via composite partition keys (with cross-partition atomic-commit coordination in the runtime).
- **PK conflict detection opt-out** (`cayenne_pk_conflict_detection: none`) for append-only CDC workloads where the source enforces PK uniqueness and the ingestion path cannot replay existing rows.
- **MERGE DML** and **DDL** (`CREATE TABLE`, `DROP TABLE`, `CREATE SCHEMA`) handlers behind the `partition-table-provider` feature, for direct SQL DDL/DML against a Cayenne catalog.
- **Snapshot engine** (`CayenneSnapshotEngine` in the runtime) that exports a per-dataset metastore "slice" (versioned JSON) so snapshots are portable across nodes with different data directories and never archive raw `cayenne.db*` files.
- **Logical and physical optimizer rules** that surface Cayenne-friendly join shapes for HTAP workloads (predicate transitive closure for non-key dim filters, dynamic-filter sharing across same-source scans, anti/semi-join sort-merge rewrite above a 10M-row build side, in-list-to-range rewrite).

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│  CayenneTableProvider                                        │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  Metastore (SqliteMetastore or TursoMetastore)        │   │
│  │                                                       │   │
│  │   cayenne_table            cayenne_partition          │   │
│  │   cayenne_delete_file      cayenne_insert_record      │   │
│  │   cayenne_snapshot_sequence                           │   │
│  │   cayenne_table_statistics                            │   │
│  │   cayenne_inlined_data     ← LSM level-0 memtable     │   │
│  │   cayenne_inlined_delete   ← LSM level-0 tombstones   │   │
│  │                                                       │   │
│  │   Pool of K independent connections                   │   │
│  │     (SQLite K = min(parallelism, 32), Turso K = 16)   │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  Vortex Data Lake — listing tables per snapshot dir   │   │
│  │                                                       │   │
│  │  <table_id>/                                          │   │
│  │    ├─ <current_snapshot_id>/                          │   │
│  │    │   ├─ part-001.vortex                             │   │
│  │    │   ├─ part-002.vortex                             │   │
│  │    │   └─ deletions/del-001.arrow                     │   │
│  │    ├─ <staging_snapshot_id>/   ← Stage A buffer       │   │
│  │    │   ├─ _wal.json                                   │   │
│  │    │   └─ part-…vortex                                │   │
│  │    └─ <protected_snapshot_id>/                        │   │
│  │        └─ …                                           │   │
│  │  (For partitioned tables, an additional               │   │
│  │   _partitioned_wal/<commit_id>.json anchors the       │   │
│  │   cross-partition commit on local FS.)                │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  In-memory state                                      │   │
│  │   listing_fence (RwLock) — read/write barrier         │   │
│  │   listing_table (ArcSwap<ListingTable>)               │   │
│  │   scan_file_statistics (footer statistics cache)      │   │
│  │   pk_deletion_strategy (ArcSwap<DeletionSnapshot>)    │   │
│  │   protected_snapshots (ArcSwap<HashMap>)              │   │
│  │   inlined_row_count (AtomicI64) — memtable size       │   │
│  │   inlined_cache (ArcSwap, generation-keyed)           │   │
│  │   pk_keyset_cache (byte-budgeted, ~256 MiB)           │   │
│  │   post_write_maintenance (debounced refresh + stats)  │   │
│  │   background_compactor (per-table)                    │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  Optional: Object Store (S3 Express One Zone,         │   │
│  │   single- or multi-zone)                              │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

## How Cayenne Works

This section walks through Cayenne end-to-end — the lifecycle of a write, the lifecycle of a read, and the background maintenance that keeps both fast — to show how the components in *Key Components* fit together. Everything rests on three cooperating tiers:

- a **transactional metastore** (SQLite or Turso) that holds all metadata — the table row, the current-snapshot pointer, per-snapshot file manifests, deletion-file and insert-record references, sequence numbers, statistics, and the inline memtable — and gives Cayenne atomic multi-row commits;
- **Vortex data files**, immutable once written, grouped under per-snapshot directories on local disk or S3 Express One Zone;
- an **LSM-style level-0 tier** — the metastore-resident *inline memtable* and, for the CDC profile, the *in-RAM mem-tier* — that absorbs small writes without producing a data file per batch.

A `CayenneTableProvider` ties these together behind DataFusion's `TableProvider`, so the same table serves high-rate CDC writes and analytical scans concurrently.

### Sequence numbers and visibility

Every mutation is stamped with a monotonically increasing **sequence number** reserved from the metastore (Iceberg-style). Inserts, deletes, and re-insertions all carry one, and the scan path uses them to resolve "was this row deleted, and if so was it re-inserted after the delete?" deterministically. The authoritative state of a table is its **current snapshot id** (a metastore column); a write or compaction becomes visible to readers by an **atomic flip of that pointer**, never by mutating files in place. Readers take a consistent view by resolving the snapshot pointer once under the `listing_fence` and holding it for the scan.

### The write path (a CDC burst's lifecycle)

The runtime's CDC apply loop (`accelerated_table/refresh_task/changes.rs`) coalesces source change-data-capture envelopes into a burst and calls `write_cdc_append_stream`. As of the dedicated CDC-apply runtime, that loop runs on its own default-priority tokio runtime so it isn't scheduler-starved by background compaction. Per burst:

1. **Conflict preparation.** Under the per-table `write_lock`, if `pk_conflict_detection: auto` the provider builds (or reuses, from the byte-budgeted PK keyset cache) the set of existing primary keys and resolves which incoming rows are *re-insertions* that must tombstone an older copy. `pk_conflict_detection: none` skips this for append-only sources.
2. **Tier selection.** Small batches that fit the per-write admission gate are absorbed into the **inline memtable** (or, under `cdc_durability: memory`, appended to the **in-RAM mem-tier**) rather than written as a Vortex file. Larger batches are encoded to **Vortex files**.
3. **Stage A (durable, on the write path).** Vortex files are written into a *staging* snapshot directory and the file list is made crash-safe by the staging WAL (`tmp + fsync + rename` of `_wal.json`). The burst's source LSN can be acked once Stage A is durable.
4. **Stage B (visible, off the write path).** A backgrounded finalize task moves the staged files into the current snapshot, publishes any on-conflict deletion vectors, flips the snapshot pointer, and invalidates DataFusion's list-files cache. The pipeline preserves burst order via a FIFO, so reads never see a later burst before an earlier one. A burst qualifies for this pipelined split when the table is not partitioned; on-conflict upserts and tables with pending PK deletions stage too (resolved by the finalize task), while partitioned tables take the synchronous publish path.

**Durability modes.** In `file` mode every burst is durable before its ack. In `memory` mode (the default for the small-write CDC profile) the mem-tier lives purely in RAM and the source-slot ack is **deferred** until a periodic/cap-triggered **off-fence checkpoint** flushes the tier to a durable Vortex file — the encode and metastore `BEGIN IMMEDIATE` run *outside* the listing fence, with only the snapshot-pointer flip under it, so a checkpoint never stalls scans. The crash model is simple: the mem-tier is discarded on restart and the source re-streams everything past the last-checkpointed slot, which the PK-idempotent apply reconciles **exactly-once**.

### The read path (a scan's lifecycle)

When DataFusion scans the table, `CayenneTableProvider` builds an execution plan that **unions** the current snapshot's Vortex files with the live contents of the inline memtable and the in-RAM mem-tier, all resolved under the `listing_fence` for a consistent view. On top of that union it applies, transparently:

- **Merge-on-read deletions.** With no primary key, deletions are *position-based*: a per-file `RoaringBitmap` is pushed into the Vortex scan layer so deleted positions never decode. With a primary key, deletions are *key-based*: each scanned row's PK is bloom-prefiltered and then probed against the cached deletion index. That index is a set of frozen, sequence-tagged **runs** plus one mutable tier (`LayeredRuns`); each run carries its own bloom and highest delete sequence, and a single fused probe answers "deleted? and re-inserted after that?" — a protected-snapshot read can skip runs whose sequences are already covered. (A scan of a composite-PK table with no active deletions short-circuits the per-row key encoding entirely.)
- **Snapshot pinning.** An in-flight scan registers a reference on the snapshot it resolved, so concurrent compaction and snapshot GC cannot delete files out from under a running read.
- **Sound ordering.** When the table is configured with sort columns and the snapshot it scans is attested as sorted (`current_sorted_snapshot`), the scan advertises a real `output_ordering`, letting the planner drop redundant sorts; the deletion-filter execs preserve input order so the guarantee survives merge-on-read.

Cayenne's optimizer rules then reshape multi-way HTAP joins — propagating selective dimension filters across join keys, sharing dynamic filters between scans of the same table, rewriting oversized hash joins into spillable sort-merge joins, and serving pre-computed **maintained aggregates** when their freshness epoch matches the scan — so analytical queries stay inside the memory budget and on the fast path.

### Background maintenance

Two background processes keep read performance bounded without blocking writers:

- **Tiered compaction** merges the smallest tier of small Vortex files into target-sized files, rewriting only that incremental subset of the snapshot (not the whole snapshot) and committing via the same atomic snapshot flip. It is triggered best-effort after writes and periodically by a per-table compactor, gated by a shared per-accelerator semaphore so a fleet of tables can't oversubscribe the writer pool. Key-delete tables compact *concurrently* with their writers.
- **Inline/mem-tier flush** checkpoints the level-0 tier to a Vortex file once accumulated rows/segments/bytes cross the flush thresholds, and the **seq-prefix bake** prunes deletion-index runs whose sequences are fully subsumed, shrinking the per-row probe cost over time.

### Why this is HTAP-friendly

The design keeps the write and read paths from contending: state that readers touch (`listing_table`, `protected_snapshots`, the inline/mem-tier handles) is published wait-free via `ArcSwap`, the expensive parts of a write (encode, metastore I/O, checkpoint) run off the listing fence, and visibility is a single pointer flip. Small CDC bursts land in the level-0 tier instead of producing a file each, compaction bounds read amplification, and merge-on-read deletions plus sequence numbering give correct snapshot-isolated results while writes stream in — so one table sustains continuous CDC ingestion and low-latency analytical queries at the same time.

## Key Components

### 1. Metastore backend (`metastore.rs`)

The `MetastoreBackend` trait defines the pluggable storage abstraction:

```rust
#[async_trait]
pub trait MetastoreBackend: Send + Sync {
    async fn init_schema(&self) -> CatalogResult<()>;
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;
    async fn execute_batch(&self, sql: &str) -> CatalogResult<()>;
    async fn execute_transaction_batch(&self, sql: &str) -> CatalogResult<()>;
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static;
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static;
    async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>>;
    async fn shutdown(&self) -> CatalogResult<()>;
}
```

Transactions go through a separate `MetastoreTransaction` trait that owns the connection guard and is consumed by `commit` / `rollback`:

```rust
#[async_trait]
pub trait MetastoreTransaction: Send + Sync {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;
    async fn query_row_values(&self, params: QueryRowParams<'_>) -> CatalogResult<Vec<MetastoreValue>>;
    async fn execute_batch(&self, sql: &str) -> CatalogResult<()>;
    async fn commit(self: Box<Self>) -> CatalogResult<()>;
    async fn rollback(self: Box<Self>) -> CatalogResult<()>;
}
```

Backends translate the `MetastoreValue` enum (`Integer | Text | Bool | Blob | Null`) to and from native column types. The starting BEGIN statement is sent by the backend itself: `BEGIN IMMEDIATE` for SQLite (takes the reserved write lock up front so a subsequent `UPDATE`/`INSERT` cannot upgrade-deadlock against a concurrent writer), and `BEGIN CONCURRENT` for Turso (MVCC writers serialize at commit time on actual conflicts).

**Schema validation.** `metastore::EXPECTED_TABLES` is the canonical list of expected metadata tables and their ordered column names; `validate_existing_schema` is invoked after `init_schema` and returns `CatalogError::SchemaMismatch` (with an actionable "clear your acceleration data" message) when the on-disk schema does not match. Types and constraints are not compared — SQLite/libSQL type affinity makes exact type matching unreliable — but column names and ordering are.

**Implementations:**

- **`SqliteMetastore`** (`metastore/sqlite.rs`): default; `tokio-rusqlite` with WAL mode, `synchronous=NORMAL`, `cache_size=-262144` (~256 MB, sized by `cayenne_metastore_cache_mb`), `temp_store=memory`, foreign keys on, and a 30-second `busy_timeout` (`cayenne_metastore_busy_timeout_ms`). PRAGMA configuration retries up to 5 times with exponential-ish backoff (10/25/50/100/200 ms) when the database is briefly locked at open.

  A round-robin connection **pool** of K independent connections is created lazily on first use (`K = min(available_parallelism, 32)`, falling back to 4 when `available_parallelism()` errors, with a floor of 2). Pool acquisition tries each slot starting at the round-robin index and returns the first one with a free `tokio::sync::Mutex`, falling back to an awaited lock on the starting slot. SQLite WAL mode allows concurrent readers and serializes writers at the engine level; the pool primarily lifts read-side concurrency for metadata-heavy workloads where every scan pays multiple metastore reads. `begin_transaction` holds an `OwnedMutexGuard` on one pool slot for the full transaction lifetime.

- **`TursoMetastore`** (`metastore/turso.rs`): optional, gated on the `turso` feature. libSQL/Turso backend with a fixed `K = 16` pool. Uses `BEGIN CONCURRENT` for MVCC-backed concurrent writers; the journal mode is set to the libSQL MVCC literal at connection open. Same 30-second `busy_timeout`.

### 2. Metadata catalog (`catalog.rs`, `cayenne_catalog.rs`)

`MetadataCatalog` is the higher-level interface that the table provider uses; `CayenneCatalog` is the concrete implementation backed by any `MetastoreBackend`. `is_retryable_write_conflict` is re-exported at the crate root so callers can implement bounded retries against transient `SQLITE_BUSY` / Turso `BEGIN CONCURRENT` conflicts. Selected methods (full signature in `catalog.rs`):

```rust
#[async_trait]
pub trait MetadataCatalog: Send + Sync {
    async fn init(&self) -> CatalogResult<()>;
    async fn list_table_names(&self) -> CatalogResult<Vec<String>>;
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<String>;  // table_id
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;
    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool>;

    // Sequence numbers (reserve reduces round-trips on serialized backends)
    async fn increment_sequence_number(&self, table_id: &str) -> CatalogResult<i64>;
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64>;
    async fn reserve_sequence_numbers(&self, table_id: &str, count: u32) -> CatalogResult<i64>;

    // Delete files (position- and key-based)
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<String>;
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>>;
    async fn remove_delete_files(&self, table_id: &str, ids: &[String]) -> CatalogResult<()>;
    async fn clear_delete_files(&self, table_id: &str) -> CatalogResult<()>;

    // Insert records for upsert re-insertion tracking
    async fn add_insert_records_batch(&self, table_id: &str, pks: Vec<Vec<u8>>, seq: i64) -> CatalogResult<()>;
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>>;
    async fn clear_insert_records(&self, table_id: &str) -> CatalogResult<()>;

    // Snapshot sequences (drives protected-snapshot filtering)
    async fn set_snapshot_sequence(&self, table_id: &str, snapshot_id: &str, seq: i64) -> CatalogResult<()>;
    async fn get_all_snapshot_sequences(&self, table_id: &str) -> CatalogResult<HashMap<String, i64>>;
    async fn clear_snapshot_sequence(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()>;

    // Atomic snapshot pointer flips (compaction and overwrite share retry-on-conflict logic)
    async fn commit_compaction(&self, table_id: &str, new_snapshot_id: &str) -> CatalogResult<()>;
    async fn commit_overwrite(&self, table_id: &str, new_snapshot_id: &str) -> CatalogResult<()>;

    // Partitions
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<String>;
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>>;

    // Persisted table statistics (Vortex FileStatistics flatbuffer)
    async fn upsert_table_statistics(&self, stats: &TableStatistics) -> CatalogResult<()>;
    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>>;
    async fn clear_table_statistics(&self, table_id: &str) -> CatalogResult<()>;

    // Inline-data memtable (small-write LSM level 0, stored as Arrow IPC blobs)
    async fn add_inlined_data(&self, data: InlinedData) -> CatalogResult<String>;
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>>;
    async fn get_inlined_data_for_partition(&self, table_id: &str, partition_key: &str) -> CatalogResult<Vec<InlinedData>>;
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64>;
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats>;
    async fn clear_inlined_data(&self, table_id: &str) -> CatalogResult<()>;

    // Inline tombstones
    async fn add_inlined_delete(&self, delete: InlinedDelete) -> CatalogResult<String>;
    async fn commit_inlined_mutation(&self, ...) -> CatalogResult<()>;  // atomic data+delete update
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>>;
    async fn clear_inlined_deletes(&self, table_id: &str) -> CatalogResult<()>;

    async fn export_dataset_slice(&self, ...) -> CatalogResult<...>;  // for snapshot/restore
    async fn shutdown(&self) -> CatalogResult<()>;
}
```

`table_id` is a `String` (UUIDv7) — not an integer — so identifiers are stable across catalog dumps and snapshots.

### 3. Metadata structures (`metadata.rs`)

- **`TableMetadata`** — table schema, primary key, on-conflict policy, current snapshot id, sequence number, `VortexConfig`.
- **`DataFile`** — virtual file (a directory containing one or more Vortex files), with row count, byte size, partition id, sequence number, and a row-id base. In Cayenne a "file" is a Vortex `ListingTable` rooted at a unique directory rather than a single on-disk file.
- **`DeleteFile`** — deletion vector reference (Arrow IPC file), with `DeletionType` (position- vs key-based) and sequence number. The `deletion_type` is inferred from the file schema at read time rather than persisted as a separate column.
- **`InlinedData`** — Arrow IPC blob stored inline in the metastore, with row count and sequence number.
- **`InlinedDelete`** — inline tombstone for upserted/deleted PKs that haven't yet been checkpointed to a delete-vector file, with row count and sequence number.
- **`InlinedDataStats`** — `{ record_count, entry_count, ipc_bytes }` aggregated from `cayenne_inlined_data` for memtable-pressure decisions.
- **`PartitionMetadata`** — composite partition columns and values (ordered, JSON-encoded), partition path, record/byte counts, plus a slash-separated `composite_key()` used for Hive-style directory naming and uniqueness.
- **`TableStatistics`** — serialized Vortex `FileStatistics` blob plus `num_rows` and an `ndv_sketches` column (per-integer-column HyperLogLog NDV sketches, mergeable across writes via register-wise max — see `hll.rs`); populated from the most recent write's `ColumnStatsAccumulator` and read by the DataFusion planner. The `FileStatistics` blob is stored last-write-wins today, with cross-write merging planned.
- **`VortexConfig`** — Vortex-side tuning. Most fields are configurable per dataset via `cayenne_*` runtime parameters. The runtime applies refresh-mode defaults before parsing explicit params (see *Configuration* below).

```rust
pub struct VortexConfig {
    // Vortex caches and file shape
    pub footer_cache_mb: Option<usize>,       // None unless runtime.params.cayenne_footer_cache_mb is set
    pub segment_cache_mb: usize,              // default 256; configures the shared Vortex segment cache capacity
    pub target_vortex_file_size_mb: usize,    // default 256

    // Encoding / sort
    pub sort_columns: Vec<String>,            // default []
    pub compression_strategy: CompressionStrategy,  // default Btrblocks

    // Writer concurrency
    pub upload_concurrency: usize,            // default available_parallelism()
    pub write_concurrency: Option<usize>,     // None = session target_partitions; the sort-and-rewrite compaction path pins to 1 regardless of this setting

    // Compaction
    pub compaction_trigger_files: usize,                // small-write profile = 4, otherwise = 8
    pub compaction_trigger_protected_snapshots: usize,  // small-write profile = 4, otherwise = 8
    pub compaction_trigger_snapshot_age_ms: u64,        // small-write profile = 60_000, otherwise = 300_000; 0 disables age trigger
    pub compaction_max_levels: usize,                   // default 3
    pub compaction_max_files_per_pick: usize,           // default 32
    pub compaction_background_interval_ms: u64,         // small-write profile = 10_000, otherwise = 30_000; 0 disables background loop

    // Inline-write admission (per-call gate)
    pub inline_max_rows: usize,                         // small-write profile = 1_024, otherwise = 0
    pub inline_max_bytes: usize,                        // small-write profile = 1_048_576, otherwise = 0
    pub inline_max_buffer_bytes: usize,                 // small-write profile = 4_194_304, otherwise = 0

    // Inline-memtable flush triggers (cumulative gate)
    pub inline_flush_max_rows: i64,                     // small-write profile = 2_048, otherwise = 10_000
    pub inline_flush_max_segments: i64,                 // small-write profile = 16, otherwise = 64
    pub inline_flush_max_bytes: i64,                    // small-write profile = 2_097_152, otherwise = 8_388_608

    // PK conflict detection
    pub pk_conflict_detection: PkConflictDetection,     // default Auto; None opts into blind append for CDC
}
```

The block above is a representative subset. Newer per-dataset fields not shown include `deletion_mode`, `cdc_durability` plus the `cdc_mem_tier_*` group, `delta_encoding`, `shard_key_columns`, `bake_deletion_index_trigger`, `dynamic_tuning` + `pinned_tuning_actuators`, the `goal_*` SLO setpoints, `schema_evolution`, and `data_storage_class` / `metastore_storage_class` — all surfaced as `cayenne_*` parameters (see *Configuration parameters*).

The runtime classifies a dataset as the "small-write profile" when its refresh mode is `caching`, `changes`, or `append` with `refresh_check_interval <= 5m`. All other refresh modes (`full`, `snapshot`, `disabled`, manual/cron append, unspecified) get the larger-write defaults. The two threshold groups have different baselines: `VortexConfig::default()` ships the small-write values for the `inline_max_*` admission gate (1024 / 1 MiB / 4 MiB), and the runtime layer zeroes those out for the large-write profile (disabling inlining for big batch loads). For everything else (compaction triggers and `inline_flush_max_*`), `VortexConfig::default()` ships the larger-write values, and the runtime layer overrides them when the small-write profile applies (`runtime/src/dataaccelerator/cayenne/mod.rs::apply_refresh_mode_defaults`).

Two distinct threshold groups for inline data — `inline_max_*` is the *per-write admission* gate ("is this single write small enough to absorb into the memtable?"); `inline_flush_max_*` is the *cumulative flush* gate ("has the accumulated memtable grown enough that we should checkpoint it to Vortex?"). Under `auto`/`adaptive` tuning the `inline_flush_max_*` caps are derived from available memory (`HardwareProfile::inline_flush_caps`); the table values below are the small-write-profile baseline.

### 4. Deletion vectors (`provider/delete/vector_io.rs`)

Two deletion modes, persisted as Arrow IPC files under `<snapshot_id>/deletions/<delete_file_id>.arrow` and referenced by `cayenne_delete_file`:

```rust
pub enum DeletionIdentifier {
    /// Position-based: file-local row positions inside a specific data file.
    /// `pre_sorted` lets callers skip the writer's O(N log N) sort/dedup when
    /// they already supply monotone-unique IDs.
    PositionBased {
        file_path: String,
        row_ids: Vec<u64>,
        pre_sorted: bool,
    },
    /// Key-based: PK bytes (via Arrow's RowConverter); survive partition
    /// reorganization and parallel coalescing.
    KeyBased(Vec<Box<[u8]>>),
}
```

The on-disk schemas:

- **Position-based**: `row_id: UInt64`, `deleted_at: Int64` (microseconds).
- **Key-based**: `row_key: Binary`, `deleted_at: Int64`.

At scan time three filtering strategies are wired up in `PkDeletionStrategyWithCache`:

- **`PositionBased`** (no primary key): per-file `RoaringBitmap` is built into a `VortexAccessPlan` with `Selection::ExcludeRoaring` and pushed into the Vortex scan layer (`provider/vortex_format::DeletionFilteringVortexFormat`). Reads must use `CoalescePartitionsExec` to keep file-local positions consistent.
- **`Int64Pk`** (single Int64 PK): `Int64PkDeletionFilterExec` (`provider/delete/filter_exec.rs`) runs above the file scan. Each row's PK is bloom-prefiltered, then probed against the cached `DeletionIndex` — a *fused* index keyed on `pk` whose entry packs the highest delete sequence and the highest upsert re-insert sequence, so one probe answers both "deleted?" and "re-inserted after the delete?".
- **`RowConverterBased`** (composite or non-integer PK): `KeyBasedDeletionFilterExec` does the same probe against a `KeyDeletionIndex` keyed by the XXH3-128 hash of the `RowConverter` byte key (the bytes are not retained — the hash is the entry identity, fixing the per-entry footprint regardless of PK width).

For the PK strategies the fused index is published as a single atomic snapshot (`Int64PkDeletionSnapshot` / `RowConverterDeletionSnapshot` — `provider/deletion_strategy.rs`), each holding one `Arc<DeletionIndex>` / `Arc<KeyDeletionIndex>` in one `ArcSwap` per table so concurrent scans observe consistent delete/re-insert pairs even mid-upsert. (Earlier revisions published a deleted index plus a companion insert-records index together; the fused entry preserves the same atomicity with one cell.) Indexes are built once off the hot path and frozen; an `extend_max_deletes` / `extend_max_conflicts` write allocates a new `Arc` and atomically swaps it in. Internally each index is an ordered set of frozen **runs** (oldest first) over one small mutable **active** tier (`LayeredRuns` — `provider/deletion_index.rs`). Each run is an immutable `HashMap` tagged with its maximum delete sequence (`max_delete_seq`) plus its own bloom filter; writes land in `active`, which freezes into a new run once it crosses `max(DELTA_MERGE_MIN, base/4)` entries, and runs size-tier-fold (the smallest adjacent pair is merged) when they exceed `MAX_FROZEN_RUNS` — so per-key copy work stays `O(log N)` and the recent runs a probe touches stay shallow and cache-resident. A probe fuses `active` with the applicable runs per-side-max: a main-scan probe consults every run behind the global bloom; a **protected-snapshot** probe with cutoff `S` skips any run whose `max_delete_seq <= S` (its deletes can't be newer than `S`) and consults only the recent, small runs via their per-run blooms — never the large global bloom — so probe cost tracks recent writes, not the total accumulated tombstone count. The seq-prefix bake (`prune_deletes_at_or_below(cutoff)`) drops deletes at or below a cutoff sequence once those tombstones are baked into the data files, shrinking the runs. The bloom filters are keyed on deletion membership (insert-only entries excluded), shared across generations behind an `Arc`, grown in place for newly-deleted keys, and sized with 2x headroom so the rebuild cadence is geometric.

A process-wide `DeletionIndex::shared_empty()` / `KeyDeletionIndex::shared_empty()` is reused across all tables that have no deletions, so the bloom allocation is amortized.

On the **write side**, when a `DELETE`'s filters already encode a concrete set of primary key values, Cayenne extracts them directly from the filter tree and writes a deletion vector without scanning data files. The extractor (`provider/delete/sink/pk_filter_extract.rs`) recognises two shapes:

- **Single PK** as `pk_col IN (v1, v2, ...)`. Integer literals are widened to `i64` and routed through the Int64 deletion-vector writer; non-integer literals are converted via Arrow's `RowConverter` and routed through the key-based writer.
- **Composite PK** as an OR-tree of AND-equality conjunctions (`(pk1 = a AND pk2 = b) OR (pk1 = c AND pk2 = d) OR ...`), with the iterative tree walk capped at `MAX_PK_FILTER_TREE_NODES = 65_536` visited nodes. Every conjunction's column set must cover the PK exactly; otherwise the extractor declines and the slow path runs.

CDC delete envelopes (whose filters are built directly from the source PK list) and DML `DELETE` statements both take this fast path when their filter shape qualifies. The fast path intentionally returns `0` from `delete_from` because the extracted key set is an upper bound on deletions, not a verified per-row count — the user-visible row count for DML comes from the inline path inside `InlineAwareDeletionSink`; CDC ignores the count entirely.

### 5. Table provider (`provider/table.rs`)

DataFusion `TableProvider` implementation. Constructed via `CayenneTableProviderBuilder`. Selected fields (full list in source):

```rust
pub struct CayenneTableProvider {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,

    // Listing-table state and direct scan-planning cache
    listing_table: Arc<ArcSwap<ListingTable>>,        // legacy stats path
    listing_fence: Arc<tokio::sync::RwLock<()>>,      // read/write barrier
    scan_file_statistics: Arc<dyn FileStatisticsCache>,
    table_statistics: Arc<RwLock<CachedTableStatistics>>,  // Statistics view + raw blob under one lock

    // Filters and conflict resolution
    retention_filters: Vec<Expr>,
    time_retention_filter_builder: Option<TimeRetentionFilterBuilder>,
    context: Arc<CayenneContext>,
    pk_deletion_strategy: PkDeletionStrategyWithCache,
    pk_row_converter: Option<Arc<RowConverter>>,
    pk_column_indices: Vec<usize>,

    // Per-table locks
    write_lock: Arc<tokio::sync::Mutex<()>>,
    compaction_lock: Arc<tokio::sync::Mutex<()>>,

    // Object store
    object_store_config: Option<ObjectStoreConfig>,
    object_store_registered_runtime_envs: Arc<ParkingMutex<HashSet<usize>>>,

    // Snapshot state
    current_snapshot_id: Arc<RwLock<String>>,
    protected_snapshots: Arc<ArcSwap<HashMap<String, i64>>>,  // wait-free reads; rcu writes

    // Memtable + maintenance
    inlined_row_count: Arc<AtomicI64>,
    inlined_generation: Arc<AtomicU64>,
    inlined_cache: Arc<ArcSwap<InlinedCache>>,
    new_files_since_last_compaction: Arc<AtomicUsize>,
    staging_wal_present: Arc<AtomicBool>,
    staging_may_have_files: Arc<AtomicBool>,
    post_write_compaction_scheduled: Arc<AtomicBool>,
    post_write_maintenance: Arc<PostWriteMaintenance>,
    background_compactor: Arc<OnceLock<BackgroundCompactor>>,
}
```

Selected fields only — the full struct (~55 fields) also carries the in-memory CDC mem-tier (`mem_tier`, `mem_checkpoint_lock`, `mem_tier_publish_lock`, `slot_advancer`, `background_mem_tier_checkpointer`), the sequence allocator (`seq_allocator`), schema evolution (`table_schema: Arc<ArcSwap<Schema>>`), sorted-scan attestation (`current_sorted_snapshot`), maintained aggregates (`maintained_aggregates`, `maintained_aggregate_epoch`), in-flight-scan GC guards (`snapshot_scan_refs`, `retired_snapshot_dirs`), and the per-table memory account (`table_memory: Arc<CayenneMemoryAccount>`).

Use `CayenneTableProviderBuilder` to construct instances:

```rust
let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
    .with_retention_filters(filters)
    .with_time_retention_filter_builder(builder)
    .with_object_store(config)
    .with_context(context)
    .create(options)  // or .open(table_name)
    .await?;
```

Provides:

- Query execution with key- and position-based deletion-vector filtering, protected-snapshot routing, and inlined-data union.
- Insert operations via DataFusion's `insert_into` API (regular path) and the dedicated `write_cdc_append_stream` (CDC-pipelined path).
- Deletes via DataFusion's SQL `DELETE FROM` path go through `CayenneDeletionSink`, which writes Arrow-IPC deletion vectors. Position-based deletes are wrapped in `PkKeysetInvalidatingDeletionSink` so the cached scan file statistics are dropped after the visible row count changes; PK-based deletes are wrapped in `InlineAwareDeletionSink` so any inline-memtable rows that match the filter are tombstoned in the same call as the file-side deletion vector. (Scan-time filter strategies are read-side — see *Deletion vectors* above.)
- Sequence-based ordering for correct delete/insert visibility.
- Protected snapshot tracking for concurrent access.
- Per-scan `ListingTable` cache and per-`RuntimeEnv` object-store registration short-circuit.

The provider also maintains an **in-memory PK keyset cache** with a 256 MiB byte budget (`PK_KEYSET_CACHE_MAX_BYTES`) so insert-side conflict detection avoids re-scanning Vortex files on every burst. The byte budget protects wide composite-PK tables from blowing memory while still letting narrow-PK tables cache millions of rows. The cumulative size is reported to `runtime.query.memory_limit` accounting.

### 6. CDC apply pipeline (`provider/mutation_writer.rs`, `provider/staging_wal.rs`)

`write_cdc_append_stream` is the entry point used by the runtime's CDC apply loop (`crates/runtime/src/accelerated_table/refresh_task/changes.rs`). Per burst:

1. Acquire `write_lock`.
2. `ensure_no_incomplete_write` — error if a previous burst's WAL is on disk and unreconciled (with an in-process bypass for prepared appends still being finalized, so back-to-back CDC bursts don't block on each other).
3. `prepare_stream_for_insert` — if `pk_conflict_detection: auto` (default), build an existing-PK keyset via `load_existing_keyset` and resolve on-conflict deletions; if `pk_conflict_detection: none`, skip.
4. Decide `can_stage_for_pipeline`: the gate is now *partition-only* (`can_stage_for_pipeline = partition_column.is_none()`, `mutation_writer.rs`). On-conflict upserts and tables with pending PK deletions stage too — they stage optimistically into a protected snapshot whose deletions the finalize task resolves and publishes; only partitioned tables fall back to the synchronous `write_prepared_stream` path that produces a new snapshot under the same visibility flip as the PK / on-conflict deletion vectors. Retention-configured tables also take the pipelined path now that retention runs through `PostWriteMaintenance` after publish rather than inline under the write lock.
5. Whether the burst takes the pipelined or synchronous path, small writes are still funneled through `try_inline_or_restream` first. The inline path is **not** blocked by pending PK deletions: when batches fit the per-write admission gate, `try_inline_batches_with_inlined_deletions` rewrites inline rows that match the pending PK tombstones and writes file-side deletion vectors for PKs whose data lives in Vortex, all under one `commit_inlined_mutation` metastore transaction. (On the synchronous fallback path, `InlineMutationPolicy` blocks the inline path for partitioned tables and retention-configured tables; the pipelined path already excludes partitioned tables via `can_stage_for_pipeline` but not retention.)
6. **Stage A** — `write_to_snapshot` into the staging dir; `write_staging_wal` makes the file list durable via tmp+fsync+rename.
7. Return a `CayenneCdcWrite` holding the staged-write handle and the still-held write lock; the runtime spawns Stage B on a background task.
8. **Stage B** (3-phase `PreparedStagedAppend`: `prepare` → `apply_under_barrier` → `finish`) — under the listing fence: `move_files_to_current_snapshot`, `remove_staging_wal`, `publish_current_snapshot_files_changed` (invalidates DataFusion's list-files cache). The write lock drops when Stage B completes.

Stage A and Stage B preserve burst order via the runtime's `PendingApplyFinalize` FIFO. The runtime acks the source-side LSN after Stage A returns (data durable) without waiting for Stage B (data visible), so PG can recycle WAL ahead of visibility.

When `cdc_durability: memory` (the default for the small-write CDC profile — see *In-memory CDC durability tier* under *Configuration*), the inline path instead appends each batch to an in-RAM tier (`provider/mem_tier.rs`) and defers the source-slot ack until a periodic/cap-triggered off-fence checkpoint flushes the tier to a durable Vortex file, trading per-batch durability cost for a bounded crash-replay window the PK-idempotent apply resolves exactly-once.

CDC apply observability surfaces these OpenTelemetry instruments (`crates/runtime/src/accelerated_table/metrics.rs`):

- `dataset_acceleration_cdc_apply_burst_duration_ms` (histogram)
- `dataset_acceleration_cdc_apply_burst_bytes` (histogram)
- `dataset_acceleration_cdc_apply_burst_envelopes` (histogram)
- `dataset_acceleration_cdc_apply_fixed_cost_ms` (histogram)

### 7. Compaction (`provider/compaction.rs`)

Tiered small-files compaction picks the smallest eligible file tier whose total size and file count exceed thresholds, and rewrites just that picked file subset into a new snapshot through the same `write_to_snapshot` + `commit_compaction` path as writes (an incremental size-tiered pass — `compact_protected_snapshots_subset` — not a whole-snapshot rewrite). The picker is a pure function: it buckets files into `Small` (size `< target_file_size_bytes / 4`), `Mid` (below target), and "settled" (at or above target), and emits a `CompactionCandidate` whenever the smallest non-empty tier has at least `trigger_files` files whose combined size reaches that tier's byte threshold.

Triggered by:

- **Inline post-write trigger** (`schedule_post_write_compaction`): `tokio::spawn` with an `AcqRel` dedup flag so at most one inline pass is queued per table.
- **Background compactor** (`BackgroundCompactor`): per-table periodic task gated by a shared per-accelerator semaphore (`Semaphore::new(available_parallelism())`).
- **Inline memtable flush** (`checkpoint_inlined_data_if_memtable_pressure_exceeded`): drains `cayenne_inlined_data` into a Vortex file when cumulative rows / segments / IPC bytes exceed `inline_flush_max_*`.

All compaction triggers `try_lock` the table write lock and skip if a writer is active. The compaction lock itself serializes concurrent compaction passes so write-driven and background-driven runs cannot overlap.

### 8. Snapshot engine (`metastore::snapshot`, `runtime::dataaccelerator::cayenne::snapshot_engine`)

Snapshots of a Cayenne dataset are taken using a **per-dataset metastore slice** rather than archiving the raw `cayenne.db*` file. The slice is a versioned (`format_version: 1`) JSON document that contains every metastore row belonging to one `cayenne_table` (the table row plus all dependent rows via `table_id`). Path columns are rewritten relative to a writer-side anchor on export, and re-anchored to a reader-side directory on import — so snapshots are portable across nodes whose data directories don't match.

`import_dataset` runs inside a single `BEGIN IMMEDIATE` transaction; FK `ON DELETE CASCADE` clears any prior dependent rows when the existing `cayenne_table` row is deleted.

The runtime engine (`CayenneSnapshotEngine`) excludes `cayenne.db`, `cayenne.db-wal`, and `cayenne.db-shm` from the tar; it inserts the slice at the well-known archive path `metadata/<dataset_name>.slice.json`. This avoids the path-portability, multi-dataset clobbering, and init-race / sidecar problems that motivated the design.

### 9. Catalog provider (`catalog_provider.rs`)

`CayenneCatalogProvider` implements DataFusion's `CatalogProvider` directly, surfacing a Cayenne metadata catalog as a SQL catalog (`from: cayenne` in a spicepod). Schemas (namespaces) are created dynamically through DDL; there are no default `public` schemas. Tables are addressed as `<catalog>.<namespace>.<table>` and the namespace is stored as a prefix on the metadata table name so it survives restarts. `data_dir` and `metadata_dir` default to `{spice_data_base_path}/cayenne_{catalog_name}/data` and `.../metadata` respectively.

### 10. DDL and MERGE (`ddl/`, feature-gated)

Behind the `partition-table-provider` feature flag:

- `CayenneDdlHandler` implements `CatalogDdlHandler` and produces physical plans (`CayenneCreateTableExec`, `CayenneDropTableExec`, `CayenneCreateSchemaExec`) so `CREATE TABLE`, `DROP TABLE`, and `CREATE SCHEMA` work directly against a Cayenne catalog.
- `CayenneDmlHandler` implements `CatalogDmlHandler` and overlays `MERGE INTO` (other DML operations use DataFusion's defaults). MERGE compiles into `CayenneMergeExec` which performs the join, then drives the standard delete + insert paths on the Cayenne table.

These are local single-node implementations. The runtime wraps them with distributed forwarding when a Cayenne table is partitioned across executors.

### 11. Optimizer rules (`logical_optimizer.rs`, `optimizer_rules.rs`)

Cayenne ships several optimizer rules that work together to keep multi-way HTAP joins (chbench q21 and TPC-H q18 were the motivating cases) inside memory budget and on the fast path. Four are logical (`logical_optimizer.rs`) and the rest are physical (`optimizer_rules.rs`):

- **`CayennePropagateFilterAcrossEquiJoinKeys`** (logical) — predicate transitive closure for non-key dim-table filters. DataFusion's stock `infer_join_predicates` only propagates filters that already reference the join key; this rule introduces `Filter(other_side.key IN (SELECT this_side.key FROM dim_subtree))` for `Inner`, `LeftSemi`, and `RightSemi` joins where the selective filter is on a non-key column (e.g. `n_name = 'CHINA'`). Subqueries are aliased with the `PROPAGATED_FILTER_ALIAS_PREFIX` and the rule refuses to re-introduce a propagated filter for the same target key, so it terminates under fixed-point iteration. Only fires when the filtering side is dim-like (at most `MAX_DIM_LIKE_TABLE_SCANS` scans) and the cardinality gates indicate it pays off.
- **`CayenneReassociateCrossJoin`** (logical) — reassociates a left-deep inner join when the SQL `FROM` order leaves an early cross join in front of a later selective join: `(A CROSS B) JOIN C` whose predicates only touch `B`/`C` becomes `A CROSS (B JOIN C)`, preserving the output schema order so the planner doesn't preserve an expensive cross-join shape.
- **`CayenneInListToRangeRewrite`** (logical) — rewrites long consecutive integer `IN` lists into an equivalent inclusive `BETWEEN min AND max` predicate, so PK scans see a tight `[lo, hi]` range the file pruner can use instead of an N-element membership probe.
- **`CayennePushDownSemiJoin`** (logical) — pushes a `LeftSemi`/`RightSemi` join down through inner joins (and identity-preserving `Projection`/`Filter` wrappers) so it prunes the base Cayenne scan sourcing its key *before* the multi-way joins build their non-spillable hash tables. TPC-H q18 is the motivating shape; soundness rests on the reordering law `(R ⋈ T) ⋉ₖ S ≡ (R ⋉ₖ S) ⋈ T`, valid when every semi-join key is sourced solely from one side. Skips scans it can *prove* are below `MIN_SEMI_JOIN_PUSHDOWN_SCAN_ROWS` (100K) rows.
- **Inner-join probe filtering** is normally handled by DataFusion's *native* hash-join dynamic-filter pushdown (no Cayenne rule). For inner joins (the only shape DataFusion pushes join-derived dynamic filters through), `HashJoinExec` plants an `Arc<DynamicFilterPhysicalExpr>` into the right-side scan during the filter-pushdown phase, and the build side populates it at execute-time with a combined predicate: min/max **bounds** (for statistics-based file/row-group/segment pruning) plus a **membership** check — an `InList` for build sides within `datafusion.optimizer.hash_join_inlist_pushdown_max_size` (sized from `runtime.query.memory_limit` per partition by the Spice session builder) or a hash-table lookup for larger ones. This natively supersedes the forked `ExactLeftAccumulator` seam. The optional **`CayenneJoinRewriter`** (physical) re-introduces the `ExactLeftAccumulator` probe for Cayenne probe sides, but is *off by default* — registered only when `cayenne_optimizer_rules.exact_join_filter` is enabled.
- **`CayenneDynamicFilterSharing`** (physical) — when a dynamic filter has been pushed into one `CayenneAccelerationExec`, installs the same `Arc<DynamicFilterPhysicalExpr>` on sibling `CayenneAccelerationExec`s backed by the same underlying table and equi-joined column set. Applies to `Inner`, `LeftSemi`, and `RightSemi` parent joins (anti joins excluded — sharing would drop rows they're meant to preserve).
- **`CayenneAntiJoinSortMergeRewriter`** (physical) — DataFusion's `HashJoinExec` build side is non-spillable. For same-source Cayenne semi/anti joins, the rule rewrites the hash join into a `SortMergeJoinExec` with explicit spillable `SortExec` inputs when the build side is too large to materialize. The **memory-pool-fraction gate is primary**: when a memory pool is wired through config, the rule fires once the estimated build-side bytes exceed `cayenne.sort_merge_memory_pool_fraction` of `runtime.query.memory_limit` (default 0.125) — catching wide-row builds whose row count is modest but whose hash table would still exhaust the pool. When no memory pool is wired (e.g. direct DataFusion users), it **falls back** to the `cayenne.sort_merge_min_rows` exact build-side row count (default 10M). Inner/outer joins keep `HashJoinExec`.
- **`CayenneMaintainedAggregateRewriter`** (physical, default-on) — when a query matches a Cayenne-maintained aggregate view (`group_by` + `Count`/`Sum`/`Avg`), serves the incrementally-maintained aggregate state in place of re-scanning and re-aggregating — but only when the maintained state's freshness epoch matches the scan's snapshot epoch (a DBSP-style incremental-view-maintenance fast path for append-heavy CDC streams; see *Maintained aggregates and NDV sketches* below).

Together these rules turn q18/q21-style multi-way joins from OOM-prone hash-join chains into spillable shapes whose probe sides see propagated filters and shared dynamic filters from the start. The `cayenne` config extension surfaces the row-count and memory thresholds (`CayenneOptimizerConfig`).

### 12. Maintained aggregates and NDV sketches (`maintained_aggregate.rs`, `hll.rs`)

- **Maintained aggregates** (`MaintainedAggregateSpec`) — a Cayenne-maintained aggregate view (`group_by` columns + `Count`/`Sum`/`Avg` expressions) whose state is updated incrementally on the CDC write path under a DBSP-style delta contract: rows apply as positive deltas while the view is known fresh; any operation that would need a retraction it lacks the old row values for marks the view stale. `CayenneMaintainedAggregateRewriter` (§11) serves the state only when its freshness epoch matches the scan's snapshot epoch, so a query never reads a stale view.
- **Incremental HyperLogLog NDV sketches** (`NdvSketches`, `hll.rs`) — per-integer-column distinct-value sketches maintained on the write path and persisted in the `ndv_sketches` column of `cayenne_table_statistics`. Mergeable across writes (register-wise max), so the metastore folds each write's sketch the way it folds min/max. Used mainly to size distributed joins on sparse integer keys (e.g. a CDC `o_custkey` spanning ~1e9 with ~1M distinct).

### 13. Query-path scan optimizations (`provider/table.rs`)

- **Parallel in-RAM scan partitioning** (`partition_memory_batches`) — the memtable / CDC mem-tier / inline corpus is greedily least-loaded-partitioned across up to `target_partitions` cores for morsel-parallel scan execution, with a `MIN_ROWS_PER_PARTITION` floor so a trivially small tier stays single-partition. Partitioned tables route inlined data by partition key and fan out the same way.
- **Sound scan `output_ordering`** — when `sort_columns` are configured, the provider advertises a stable `output_ordering` (`SortExpr`) on a scan *only* when the scanned snapshot id matches `current_sorted_snapshot` (the snapshot a sorted compaction last produced). A compaction/rewrite that could disturb the sort clears the attestation, so the planner never assumes an ordering the files no longer guarantee. The merge-on-read deletion-filter execs preserve input ordering, so the advertised ordering survives delete application.
- **Composite-PK no-tombstone scan short-circuit** — for a key-based (composite or non-integer) PK scan with no active deletions, no inline deletions, and no null PKs, the scan skips the `O(rows × pk_cols)` `RowConverter` encode + filter pass entirely and returns the batch unmodified — the cheaper key-mode analogue of the Int64 PK disjoint-range fast path.

## CRUD operations

### Create table

```rust
let options = CreateTableOptions {
    table_name: "users".to_string(),
    schema: Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])),
    primary_key: vec!["id".to_string()],
    on_conflict: None,
    base_path: "/data/users".to_string(),
    partition_column: None,
    vortex_config: cayenne::metadata::VortexConfig::default(),
};

let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
    .create(options)
    .await?;
```

### Insert data

```rust
use datafusion::prelude::*;
let ctx = SessionContext::new();
ctx.register_table("my_table", Arc::new(provider))?;
ctx.sql("INSERT INTO my_table SELECT * FROM source_table").await?.collect().await?;
```

For CDC apply, the runtime calls `provider.write_cdc_append_stream(stream, &task_ctx)` directly to take the pipelined path.

### Delete

```sql
DELETE FROM users WHERE id IN (1, 2, 3)
```

Deletion vectors are written as Arrow IPC files for PK-keyed and position-keyed deletes; small batches land inline as `InlinedDelete` entries first and are flushed on memtable pressure.

### Query

```sql
SELECT * FROM users WHERE id > 100
```

Deletion vectors, protected snapshots, inlined data union, and time-retention filters are all applied transparently.

## Configuration parameters

The runtime accelerator (`runtime/src/dataaccelerator/cayenne/mod.rs`) recognizes the following core `cayenne_*` spicepod parameters that configure dataset-level behavior. S3 Express One Zone storage parameters are listed in a separate table below. Defaults marked "small-write profile" apply when `refresh_mode` is `caching`, `changes`, or `append` with `refresh_check_interval <= 5m`; otherwise the larger defaults apply.

| Parameter                                        | Description                                                                                                                 | Default                                                                   |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `cayenne_file_path`                              | Data file path. Local path or S3 Express One Zone (`s3://{bucket}--{zone-id}--x-s3/...`). Standard S3 buckets are rejected. | `{spice_data}/{dataset}/`                                                 |
| `cayenne_metadata_dir`                           | SQLite metadata directory.                                                                                                  | `{spice_data}/metadata` or `{cayenne_file_path}/metadata` for local paths |
| `cayenne_metastore`                              | `sqlite` (default) or `turso` (requires `turso` build feature).                                                             | `sqlite`                                                                  |
| `cayenne_unsupported_type_action`                | `error` (default), `string`, `warn`, `ignore`.                                                                              | `error`                                                                   |
| `cayenne_segment_cache_mb`                       | Vortex segment cache in MB.                                                                                                 | `256`                                                                     |
| `cayenne_pk_keyset_cache_mb`                     | Resident PK-keyset cache cap per table (drives the exact-keyset vs. bloom-fallback deletion path). Accounted against `runtime.query.memory_limit`. | memory-derived (~1/32 of total, clamped `[256, 8192]`)      |
| `cayenne_target_file_size_mb`                    | Vortex file target size in MB.                                                                                              | `256`                                                                     |
| `cayenne_sort_columns`                           | Comma-separated sort columns.                                                                                               | (none)                                                                    |
| `cayenne_compression_strategy`                   | `btrblocks` or `zstd`.                                                                                                      | `btrblocks`                                                               |
| `cayenne_delta_encoding`                         | Delta-write encoding level: `auto` (default), `none`, or an explicit level `0`–`10` (`7`+ is full). Tunes per-write delta encoding. | `auto`                                                            |
| `cayenne_pk_conflict_detection`                  | `auto` or `none`.                                                                                                           | `auto`                                                                    |
| `cayenne_upload_concurrency`                     | Concurrent multipart upload fan-out.                                                                                        | `available_parallelism()`                                                 |
| `cayenne_write_concurrency`                      | Writer partition override for unsorted ingests. The sort-and-rewrite compaction path always writes serially.                | `target_partitions`                                                       |
| `cayenne_compaction_trigger_files`               | Small-tier file count trigger.                                                                                              | small = 4, otherwise = 8                                                  |
| `cayenne_compaction_trigger_protected_snapshots` | Protected-snapshot count trigger.                                                                                           | small = 4, otherwise = 8                                                  |
| `cayenne_compaction_trigger_snapshot_age_ms`     | Protected-snapshot age trigger, 0 disables.                                                                                 | small = 60_000, otherwise = 300_000                                       |
| `cayenne_compaction_max_levels`                  | Max consecutive compaction passes per trigger.                                                                              | `3`                                                                       |
| `cayenne_compaction_max_files_per_pick`          | Files retained per candidate.                                                                                               | `32`                                                                      |
| `cayenne_compaction_background_interval_ms`      | Background compactor interval, 0 disables.                                                                                  | small = 10_000, otherwise = 30_000                                        |
| `cayenne_inline_max_rows`                        | Per-write inline admission row cap, 0 disables.                                                                             | small = 1_024, otherwise = 0                                              |
| `cayenne_inline_max_bytes`                       | Per-write inline admission IPC byte cap.                                                                                    | small = 1_048_576, otherwise = 0                                          |
| `cayenne_inline_max_buffer_bytes`                | Buffer cap while deciding whether to inline.                                                                                | small = 4_194_304, otherwise = 0                                          |
| `cayenne_inline_flush_max_rows`                  | Cumulative inline-flush row trigger.                                                                                        | small = 2_048, otherwise = 10_000                                         |
| `cayenne_inline_flush_max_segments`              | Cumulative inline-flush segment trigger.                                                                                    | small = 16, otherwise = 64                                                |
| `cayenne_inline_flush_max_bytes`                 | Cumulative inline-flush IPC-byte trigger.                                                                                   | small = 2_097_152, otherwise = 8_388_608                                  |
| `cayenne_deletion_mode`                          | How PK deletions are recorded/applied: `auto`, `key`, or `position`. `auto` resolves to `key` for `refresh_mode: changes` PK tables (key-deletes compact concurrently with writers and ride the in-memory CDC tier) and to `position` (merge-on-read, pushed into the Vortex scan) everywhere else. PK-less tables are always position-based. | `auto`                          |
| `cayenne_cdc_durability`                         | Durability mode for the inline CDC write path: `memory` (default, eligibility-gated) or `file`. See *In-memory CDC durability tier* below.                                          | `memory`                                                                  |
| `cayenne_cdc_mem_tier_max_bytes`                 | Per-table RAM-tier byte cap before a forced spill+slot-advance (`memory` mode only). `0` disables the per-table cap; the process-global byte budget still bounds aggregate RAM.       | memory-derived (~1/64 of RAM, clamped `[256, 1024]` MiB)                  |
| `cayenne_cdc_mem_tier_max_age_ms`                | Max wall-clock age of a RAM-tier epoch before a forced background checkpoint (`memory` mode only). Bounds crash-replay window + deferred slot ack. `0` disables.                     | `10_000`                                                                  |
| `cayenne_cdc_mem_tier_min_flush_bytes`           | Minimum resident RAM-tier bytes before the periodic background tick checkpoints (`memory` mode only); below this a tick is skipped unless the age cap is reached. `0` flushes every tick. | memory-derived (~1/8 of the derived max-bytes, clamped `[32, 128]` MiB) |
| `cayenne_cdc_mem_tier_checkpoint_interval_ms`    | Periodic background mem-tier checkpoint interval (`memory` mode only); advances the deferred slot ack on idle / pure-upsert streams. `0` disables the periodic task.                 | `1_000`                                                                   |
| `cayenne_shard_key_columns`                      | Comma-separated columns to hash-cluster rows by during intra-write encode sharding. Empty derives the shard key from the primary key.                                                | (derive from PK)                                                          |
| `cayenne_tuning`                                 | Auto-tuning mode: `auto` (static config from environment + inferred schema) or `adaptive` (additionally runs a per-table closed-feedback controller within environment-derived bounds; preview). An explicit per-parameter value overrides the derived value and *pins* that actuator under `adaptive`. | `auto`                          |

**CDC apply-loop coalescing** (runtime-level, per-dataset overrides; tune how the CDC source-reader batches envelopes before each accelerator write): `cdc_prefetch_buffer` (source-reader prefetch channel depth), `cdc_max_coalesced_envelopes` / `cdc_max_coalesced_bytes` (caps on one coalesced burst), `cdc_max_coalesce_age_ms` (linger window before flushing a partial burst), and `cdc_commit_timeout_ms` (source-side commit timeout). Parsed in `runtime/src/dataaccelerator/cayenne/mod.rs`.

**Goal-driven tuning** (`cayenne_tuning: adaptive`): `cayenne_goal_replication_lag`, `cayenne_goal_freshness`, `cayenne_goal_query_latency`, `cayenne_goal_qph`, and `cayenne_goal_convergence_window` declare SLO setpoints the adaptive controller steers toward, within the static `auto` bounds. `cayenne_bake_deletion_index_trigger` sets the deletion-index seq-prefix-bake threshold (a derived value under `auto`; a movable actuator under `adaptive`).

### In-memory CDC durability tier (`cdc_durability: memory`)

`provider/mem_tier.rs` implements the default CDC durability path. Instead of persisting a durable metastore BLOB / staged Vortex write per batch before the source-slot ack, memory mode appends each validated batch to an in-RAM tombstone/data tier (held as a sibling `ArcSwap` of `inlined_cache`, so an append is an O(1) `Arc` swap and reads union the tier under the listing fence with zero copy) and DEFERS the slot ack until a periodic/cap-triggered **checkpoint** flushes the tier to a durable Vortex file. Tombstone maps are structurally-shared persistent `im::HashMap`s, so an append shares structure with the prior tier instead of deep-copying the corpus.

Crash model: the tier is pure RAM and is discarded on crash/restart. The source slot is the single source of truth (it holds at most the last-checkpointed LSN), so the source re-streams every record past that point and the PK-idempotent CDC apply converges exactly-once. The `SlotAdvancer` callback enforces the load-bearing invariant: the slot advances ONLY after the covering checkpoint's Vortex + metastore writes are durable.

The checkpoint is **two-phase and off-fence**: the encode + `BEGIN IMMEDIATE` metastore work runs *outside* the listing fence, and only the brief snapshot-pointer flip happens under it, so a checkpoint doesn't stall scans for the duration of the encode/IO. The tier is bounded on three axes — a per-table byte cap (`cdc_mem_tier_max_bytes`, the synchronous OOM backstop checked per burst), an age cap (`cdc_mem_tier_max_age_ms`), and a periodic background tick (`cdc_mem_tier_checkpoint_interval_ms`, gated by `cdc_mem_tier_min_flush_bytes`) — plus a process-global byte budget (`provider/mem_tier_budget.rs`) that bounds aggregate RAM across a fleet of tables so memory mode can never OOM. Memory mode applies only to the small-write CDC profile and non-partitioned tables; every other profile/source/table silently uses `file`.

### Adaptive tuning controller (`provider/tuning.rs`)

When `cayenne_tuning: adaptive`, a per-table closed-feedback controller runs on the background-compaction tick. It records each CDC write (rows, delete rows, bytes, apply latency, inter-batch arrival gap) into `IngestStats`, folds in environment signals (read amplification, cgroup-aware memory pressure), and on each tick calls `decide` to make at most **one bounded actuator move** subject to hysteresis and a `MIN_DWELL` dwell (30s by default; under goal-driven mode the dwell derives from the configured convergence window). The movable actuators (`enum Actuator`) are `InlineFlushBytes`, `MemTierMaxBytes`, `CompactionIntervalMs`, `CompactionTriggerFiles`, `WriteConcurrency`, `BakeDeletionIndexTrigger`, and `TargetVortexFileSize`; signals include the mutation fraction (`MUTATION_HEAVY_FRACTION`) and arrival burstiness (`BURSTY_ARRIVAL_CV`). Every actuator is clamped to the static `[floor, ceiling]` the `auto` tier derived, so a dynamic decision can only pick a value `auto` could also have picked — the worst case is the worst static config, never worse. An operator-set value pins its actuator (bounds collapse to a point; see `PinnedTuningActuators`). The accounting is always recorded (cheap) even when tuning is off, for observability. `auto` mode runs no loop.

### S3 Express One Zone parameters

When the data tier targets S3 Express One Zone (either via an `s3://...--x-s3/...` path in `cayenne_file_path` or via `cayenne_s3_zone_ids`), the following additional parameters configure the underlying object store. When `cayenne_s3_zone_ids` lists multiple zones, every write is applied to every zone atomically with best-effort rollback on partial failure (`MultiZoneS3ExpressStore`).

| Parameter                     | Description                                                                                                                                              | Default                              |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `cayenne_s3_zone_ids`         | Comma-separated AZ IDs (e.g. `usw2-az1` or `usw2-az1,usw2-az2`). When set without `cayenne_file_path`, the bucket name is auto-generated and created.    | (none)                               |
| `cayenne_s3_region`           | AWS region. Derived from `cayenne_s3_zone_ids` if not set.                                                                                               | (derived)                            |
| `cayenne_s3_endpoint`         | Custom S3 endpoint URL.                                                                                                                                  | (default AWS)                        |
| `cayenne_s3_key`              | AWS access key ID. Secret.                                                                                                                               | (none)                               |
| `cayenne_s3_secret`           | AWS secret access key. Secret.                                                                                                                           | (none)                               |
| `cayenne_s3_session_token`    | AWS session token for temporary credentials. Secret.                                                                                                     | (none)                               |
| `cayenne_s3_auth`             | Authentication mode: `iam_role` (env credentials) or `key` (explicit `cayenne_s3_key`/`cayenne_s3_secret`).                                              | `iam_role`                           |
| `cayenne_s3_client_timeout`   | Timeout for S3 client operations (e.g. `30s`, `5m`).                                                                                                     | `120s`                               |
| `cayenne_s3_allow_http`       | Allow plaintext HTTP connections.                                                                                                                        | `false`                              |
| `cayenne_s3_unsigned_payload` | Skip SHA-256 payload signing for S3 Express requests (session-based auth makes payload signing redundant).                                               | `true`                               |

The runtime-global Vortex footer-metadata cache is sized via `runtime.params.cayenne_footer_cache_mb`; when set, the configured value is persisted in the metastore to detect cross-restart drift. Memory accounting for the PK keyset cache, sort/merge join build-side rewrites, and inline-memtable buffers is integrated with `runtime.query.memory_limit` (the canonical Spicepod v2 path; the legacy `runtime.memory_limit` is auto-migrated with a deprecation warning).

### Metastore (SQLite) tuning parameters

These are runtime-global `runtime.params` (not per-dataset). They tune the SQLite metastore's pragmas; the defaults match the previously-hardcoded values, and are applied once at startup.

| Parameter                                     | Description                                                                                                                                      | Default          |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------- |
| `cayenne_metastore_cache_mb`                  | SQLite page-cache size (per connection × pool).                                                                                                  | `256`            |
| `cayenne_metastore_mmap_mb`                   | SQLite `mmap_size`.                                                                                                                              | `1024` (1 GiB)   |
| `cayenne_metastore_busy_timeout_ms`           | `busy_timeout` — how long a writer waits for the lock before erroring.                                                                          | `30000`          |
| `cayenne_metastore_wal_autocheckpoint_pages`  | Inline WAL auto-checkpoint threshold in pages; `0` disables the inline checkpoint so it never fires inside a hot commit (the background maintenance tick drains the WAL instead). | `0`              |
| `cayenne_metastore_wal_truncate_threshold_mb` | WAL size above which the background-tick checkpoint escalates PASSIVE→TRUNCATE to reclaim the `-wal` file (never on the hot write path).         | `160`            |
| `cayenne_metastore_auto_vacuum`               | `none`, `incremental`, or `full`. Takes effect only on a fresh DB.                                                                              | `none`           |

The metastore always runs in WAL journal mode with `synchronous = NORMAL`.

## Relationship to the DuckLake specification

Cayenne shares some shape with the [DuckLake v1.0 specification](https://ducklake.select/docs/stable/specification/introduction) — both store transactional table metadata in a SQL database and put data in object storage — but the two formats are not interchangeable. The differences are deliberate, driven by Cayenne's use of the Vortex columnar format and the runtime's HTAP / CDC workloads.

### Shared concepts

- Transactional catalog database (Cayenne supports SQLite or Turso; DuckLake also allows DuckDB, Postgres, MySQL).
- Sequence-numbered snapshots for visibility ordering.
- Per-table partition metadata and per-snapshot data layout.
- Delete-file references decoupled from data files (so deletes don't rewrite data).
- Inline data table for small-write absorption (`cayenne_inlined_data` mirrors `ducklake_inlined_data_tables` in concept).

### Major divergences from DuckLake v1.0

| Area                        | DuckLake v1.0                                                                                                                        | Cayenne                                                                                                                                                                        |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Data file format**        | Parquet (mandated)                                                                                                                   | Vortex                                                                                                                                                                         |
| **Catalog table prefix**    | `ducklake_`                                                                                                                          | `cayenne_`                                                                                                                                                                     |
| **Data file metadata**      | Explicit `ducklake_data_file` row per file with column stats in `ducklake_file_column_stats`                                         | No explicit data-file table; Cayenne lets DataFusion's `ListingTable` enumerate Vortex files in each snapshot directory. Table-level stats only in `cayenne_table_statistics`. |
| **Snapshot model**          | Dedicated `ducklake_snapshot` + `ducklake_snapshot_changes` change log                                                               | `current_snapshot_id` field on `cayenne_table` plus `cayenne_snapshot_sequence` for protected-snapshot routing                                                                 |
| **Schema representation**   | Column-level rows in `ducklake_column`, evolution via `ducklake_schema_versions`, `ducklake_column_mapping`, `ducklake_name_mapping` | Schema stored as a JSON blob (`schema_json`) on `cayenne_table`; schema evolution is intentionally simplified                                                                  |
| **Namespaces / schemas**    | `ducklake_schema` supports nested namespaces                                                                                         | Flat per-catalog namespace (the catalog-provider path supports namespaces via name-prefixing on `cayenne_table.table_name`)                                                    |
| **Upsert / PK semantics**   | Snapshot-based merge                                                                                                                 | Iceberg-style PK insert tracking in `cayenne_insert_record`, paired with `cayenne_inlined_delete` tombstones                                                                   |
| **GC**                      | `ducklake_files_scheduled_for_deletion` work queue                                                                                   | Old-snapshot cleanup triggered inline by compaction/sort/overwrite paths                                                                                                       |
| **Views, SQL macros, tags** | First-class (`ducklake_view`, `ducklake_macro*`, `ducklake_tag`, `ducklake_column_tag`)                                              | Not implemented                                                                                                                                                                |
| **Sort metadata**           | `ducklake_sort_expression`, `ducklake_sort_info`                                                                                     | `sort_columns` is a per-dataset config field, not a catalog table                                                                                                              |
| **Variant column stats**    | `ducklake_file_variant_stats`                                                                                                        | Not implemented                                                                                                                                                                |

### What Cayenne implements relative to its own goals

- Table metadata with sequence-numbered operations
- Position- and key-based delete files
- Composite partition keys plus cross-partition atomic-commit coordination
- Tiered small-files compaction (inline + background)
- Inline-data memtable with per-write and cumulative-flush thresholds
- CDC apply pipelining with debounced post-write maintenance
- Protected-snapshot scan routing for upsert correctness
- Portable per-dataset metastore-slice snapshots
- DDL + MERGE handlers (behind the `partition-table-provider` feature)
- Cayenne-aware optimizer rules for HTAP / chbench workloads

### Not implemented (and not currently planned)

- Schema evolution at column-row granularity (column adds / drops / renames / mappings)
- SQL macros, views, table/column tags
- Snapshot expiration and time-travel queries
- Full MVCC

If interoperability with a DuckLake catalog reader is a requirement, Cayenne is not the right tool. If a Vortex-native, CDC-friendly accelerator backed by SQLite or Turso fits the workload, Cayenne is purpose-built for that.

## Database schema

The metastore (SQLite or Turso) materializes these tables. DDL lives in `crates/cayenne/src/metastore/sqlite.rs` (and is mirrored verbatim by Turso). Expected column lists are exported as `metastore::EXPECTED_TABLES` and checked by `validate_existing_schema` at startup.

```sql
CREATE TABLE IF NOT EXISTS cayenne_table (
    table_id TEXT PRIMARY KEY,          -- UUIDv7
    table_name TEXT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    schema_json TEXT NOT NULL,
    primary_key_json TEXT,
    on_conflict_json TEXT,
    current_snapshot_id TEXT NOT NULL DEFAULT '',
    partition_column TEXT,
    vortex_config_json TEXT,
    current_sequence_number BIGINT NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_table_name_unique
    ON cayenne_table(table_name);

CREATE TABLE IF NOT EXISTS cayenne_delete_file (
    delete_file_id TEXT PRIMARY KEY,    -- UUIDv7
    table_id TEXT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    format TEXT NOT NULL,               -- always 'arrow_ipc'
    delete_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    source_data_file_path TEXT,         -- non-NULL only for position-based deletes
    sequence_number BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cayenne_delete_file_table_path
    ON cayenne_delete_file(table_id, path);

CREATE TABLE IF NOT EXISTS cayenne_partition (
    partition_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    partition_columns_json TEXT NOT NULL,   -- ordered list of column names
    partition_values_json TEXT NOT NULL,    -- ordered list of values
    partition_key TEXT NOT NULL,            -- slash-separated composite key
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    record_count BIGINT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    UNIQUE(table_id, partition_key)
);

CREATE TABLE IF NOT EXISTS cayenne_insert_record (
    insert_record_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    pk_bytes BLOB NOT NULL,
    sequence_number BIGINT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    UNIQUE(table_id, pk_bytes)
);

CREATE TABLE IF NOT EXISTS cayenne_snapshot_sequence (
    table_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    PRIMARY KEY (table_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
    table_id TEXT NOT NULL PRIMARY KEY,
    statistics_blob BLOB NOT NULL,      -- Vortex FileStatistics flatbuffer
    num_rows BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cayenne_inlined_data (
    inlined_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    partition_key TEXT,
    data_ipc BLOB NOT NULL,             -- Arrow IPC stream
    record_count BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_data_table_seq
    ON cayenne_inlined_data(table_id, sequence_number);

CREATE TABLE IF NOT EXISTS cayenne_inlined_delete (
    inlined_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    delete_ipc BLOB NOT NULL,           -- Arrow IPC stream of PK row keys / row IDs
    delete_count BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cayenne_inlined_delete_table_seq
    ON cayenne_inlined_delete(table_id, sequence_number);
```

Three further tables — `cayenne_snapshot_file` (the authoritative per-snapshot data-file manifest), `cayenne_snapshot_file_statistics` (per-file Vortex statistics), and `cayenne_pk_index` (the persisted PK keyset) — are also materialized (11 tables total in `EXPECTED_TABLES`); see `metastore/sqlite.rs` for their full DDL.

The DDL source is authoritative; treat this section as a quick reference.

## Usage example

```rust
use cayenne::{
    CayenneCatalog, CayenneTableProviderBuilder,
    metadata::{CreateTableOptions, VortexConfig},
};

let catalog = Arc::new(CayenneCatalog::new("sqlite:///data/catalog.db")?);
catalog.init().await?;

let options = CreateTableOptions {
    table_name: "events".to_string(),
    schema: Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("data", DataType::Utf8, true),
    ])),
    primary_key: vec!["event_id".to_string()],
    on_conflict: None,
    base_path: "/data/events".to_string(),
    partition_column: None,
    vortex_config: VortexConfig::default(),
};

let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
    .create(options)
    .await?;

let ctx = SessionContext::new();
ctx.register_table("events", Arc::new(provider))?;
let df = ctx.sql("SELECT * FROM events WHERE event_id > 1000").await?;
df.show().await?;
```

## Implementation status

### Current status

- Pluggable metastore (SQLite default; Turso optional) with per-instance connection pool
- Position- and key-based deletion vectors (`PositionBased`, `Int64Pk`, `RowConverterBased` strategies)
- Primary keys, upsert on-conflict, retention policies (time- and SQL-based)
- Sequence-based ordering with protected snapshots
- Streaming data ingestion and queries
- File-mode acceleration only
- S3 Express One Zone (single- and multi-zone) data storage; metastore stays on local disk
- Composite partition keys with cross-partition atomic commit coordinator (`partitioned_insert_strategy`)
- Staging WAL with crash-safe recovery; partitioned WAL for cross-partition commits on local FS
- Tiered small-files compaction (inline + background)
- Inline-data memtable (per-write admission + cumulative flush thresholds, both configurable)
- Inline-memtable absorption of writes with concurrent pending PK deletions (inline rewrite + file-side deletion vector applied atomically in one metastore commit)
- CDC apply pipelining with debounced post-write maintenance
- In-memory CDC durability tier (`cdc_durability: memory`, default for the CDC profile) with two-phase off-fence checkpoint, age/byte caps, and a process-global RAM budget
- Configurable deletion mode (`cayenne_deletion_mode: auto`/`key`/`position`); merge-on-read position deletes via `Selection::ExcludeRoaring`
- Optional per-table adaptive tuning controller (`cayenne_tuning: adaptive`, preview) bounded by the static `auto` config
- Fast-path delete that extracts PK values directly from `IN`-list and OR-of-AND-equality filter trees, skipping the data scan
- Per-dataset `cayenne_pk_conflict_detection` opt-out for append-only CDC
- CDC apply observability metrics (`dataset_acceleration_cdc_apply_*`)
- Same-source large-join `HashJoin → SortMergeJoin` rewriter for spillable hash-join build sides
- Logical and physical optimizer rules for HTAP / chbench workloads
- MERGE DML, `CREATE TABLE`, `DROP TABLE`, `CREATE SCHEMA` (feature-gated)
- Portable per-dataset metastore-slice snapshots

### Known limitations

#### Access mode

Cayenne supports `mode: file` only. In-memory mode is not supported.

#### Object storage

Only **S3 Express One Zone** is supported for data storage; standard S3 buckets are explicitly rejected with a clear error. The metastore must stay on local disk regardless.

#### Data types

Some Arrow data types are not natively supported by the Vortex format:

- `Interval`
- `Duration`
- `FixedSizeBinary`
- `Float16` (auto-converted to `Float32`)
- Non-microsecond `Timestamp` units (auto-normalized to microseconds, timezone preserved)

The `cayenne_unsupported_type_action` parameter controls handling:

- `error` (default): fail on unsupported types
- `string`: convert unsupported types to UTF-8 strings
- `warn`: include in schema but may fail on insert
- `ignore`: skip unsupported fields

#### Indexes

Secondary indexes are not supported. Primary keys drive efficient upserts and deletions.

#### Concurrency / MVCC

Cayenne provides Iceberg-style **sequence-number snapshot isolation**: each write commits at a monotonic sequence number, deletions apply by sequence, and readers see a consistent snapshot — so concurrent reads never observe a torn write. What is *not* yet supported is general multi-version concurrency control with **time-travel queries** (reading an arbitrary historical snapshot); snapshot expiration and time-travel are tracked under Future enhancements.

### Future enhancements

- Snapshot expiration and time-travel queries
- Full MVCC support
- Advanced statistics (column-level histograms, sketches) and cross-write merging of `cayenne_table_statistics`
- Additional catalog backends (PostgreSQL, DuckDB)
- Apply-side pipelining at finer granularity (Stage A of burst N+1 overlapping Stage B of burst N without write-lock serialization)
- Cached `insert_into` execution plan reuse across CDC bursts
- Automated replay-or-rollback driver for the partitioned WAL on table open

## Benefits

1. **Efficient deletes**: deletion vectors stored as Arrow IPC files; no data-file rewrites.
2. **ACID metadata**: SQLite (or Turso) provides transaction guarantees for catalog operations.
3. **Performance**: Vortex columnar format with configurable compression and caches; inline memtable absorbs small writes without writing data files.
4. **Crash safety**: staging WAL with tmp+fsync+rename ensures atomic visibility; partitioned WAL anchors cross-partition commits; self-healing recovery on the next open.
5. **Object store support**: native S3 Express One Zone integration, with optional multi-zone atomic replication.
6. **CDC-friendly**: Stage A / Stage B pipelining, debounced maintenance, and optional blind-append mode for append-only ingestion.
7. **Flexibility**: trait-based metastore lets the same catalog logic run against SQLite or Turso.
8. **HTAP-aware**: optimizer rules surface predicate transitive closure, dynamic-filter sharing, and spillable sort-merge joins automatically for Cayenne-backed scans.
9. **Portable snapshots**: per-dataset metastore slices are anchor-relocatable JSON, so snapshots travel cleanly across nodes with different layouts.

## Industry techniques used by Cayenne

Cayenne synthesizes several established database/storage techniques. The list below names each technique, points to where it lives in this crate, and links to the canonical specification or paper.

### Lakehouse formats and metadata catalogs

- **SQL-catalogued lakehouse pattern** (DuckLake-style). Transactional metadata lives in a SQL database; data files live in object storage. The two layers are tied together by snapshot pointers and sequence numbers. Cayenne keeps the high-level shape (catalog DB + Vortex files) but diverges substantively (no per-file row, JSON-blob schema, no views/macros/tags). See the *Relationship to the DuckLake specification* section for the full comparison.
  - [DuckLake Specification v1.0](https://ducklake.select/docs/stable/specification/introduction)
  - DuckDB blog, *"Announcing DuckLake"*: <https://duckdb.org/2025/05/27/ducklake.html>

- **Iceberg-style sequence-number snapshot visibility.** Each table maintains a monotonically increasing sequence; data files, delete files, and snapshots all carry sequence numbers, and a delete file applies only to data files whose sequence is strictly less than the delete's sequence. This is what makes upserts safe without anti-deletion tracking — re-inserting the same PK at a higher sequence makes the old delete a no-op for the new row. Cayenne uses this for `cayenne_snapshot_sequence`, `cayenne_delete_file.sequence_number`, and `cayenne_insert_record.sequence_number`.
  - [Apache Iceberg Spec](https://iceberg.apache.org/spec/)

- **Position- and key-based delete files** (Iceberg "deletion vectors" / equality deletes). Decoupling deletes from data files avoids rewrites when rows are tombstoned. Cayenne implements both modes in `provider/delete/vector_io.rs`. Position-based deletes are pushed into the Vortex scan as a `Selection::ExcludeRoaring(...)`; key-based deletes filter above the scan via `Int64PkDeletionFilterExec` / `KeyBasedDeletionFilterExec`.
  - [Apache Iceberg Spec — Deletes](https://iceberg.apache.org/spec/#delete-formats)
  - Databricks engineering blog, *"Deletion Vectors in Delta Lake"*: <https://www.databricks.com/blog/announcing-general-availability-deletion-vectors>

- **Transactional log over a data lake** (Delta Lake comparison). Cayenne stores commit state in a SQL catalog rather than a per-table `_delta_log/`, which trades the file-log replay simplicity of Delta Lake for the transactional and querying ergonomics of a relational metastore.
  - Armbrust et al., *"Delta Lake: high-performance ACID table storage over cloud object stores"*, VLDB 2020. <https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf>

### Columnar storage and compression

- **Vortex** — Spiral's open-source columnar file format. Persistent storage tier for Cayenne. Provides predicate pushdown, zone maps, footer-level file statistics, and a pluggable compression strategy. The integration lives in `provider/vortex_format.rs`.
  - [spiraldb/vortex](https://github.com/spiraldb/vortex)
  - Spiral blog: <https://spiraldb.com/blog>

- **BtrBlocks** — adaptive columnar compression. Used as Cayenne's default Vortex compression strategy (`cayenne_compression_strategy: btrblocks`). Picks per-block encodings (FOR, dictionary, RLE, etc.) to balance size and decode speed.
  - Kuschewski, Sauerwein, Alhomssi, Haubenschild, Boncz, Neumann, *"BtrBlocks: Efficient Wire-Compatible Compression for Data Lakes"*, SIGMOD 2023. <https://dl.acm.org/doi/10.1145/3589258>

- **Zstandard** — alternative dense compression strategy (`cayenne_compression_strategy: zstd`).
  - [Zstandard format spec](https://datatracker.ietf.org/doc/html/rfc8478)

- **Apache Arrow** — in-memory columnar format and Arrow IPC stream encoding. Cayenne serializes inline-memtable entries and key-based deletion vectors as Arrow IPC blobs in the metastore; deletion-vector files are Arrow IPC files on disk.
  - [Apache Arrow](https://arrow.apache.org/)
  - [Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)

- **Arrow `RowConverter`** for composite primary keys. Converts each row's PK columns into a comparable byte sequence so composite keys can drive hash-based deletion indexes and on-conflict detection.
  - [`arrow-row` crate](https://docs.rs/arrow-row/)

### Write-optimized storage (LSM-tree pattern)

- **Log-Structured Merge-Tree (LSM-Tree) with an in-DB level-0 memtable.** Small writes land as Arrow IPC blobs in `cayenne_inlined_data` (the memtable). When cumulative pressure exceeds `inline_flush_max_*`, the memtable is checkpointed into a Vortex file at the on-disk tier, mirroring the L0 → on-disk-tier flush pattern. The same pattern is applied to deletes via `cayenne_inlined_delete`. The novelty here is that L0 lives inside a *transactional metastore* rather than an in-memory `SkipList`, which makes its commit semantics piggyback on the catalog's BEGIN/COMMIT.
  - O'Neil, Cheng, Gawlick, O'Neil, *"The Log-Structured Merge-Tree (LSM-Tree)"*, *Acta Informatica* 33(4), 1996. Author-hosted PDF: <https://www.cs.umb.edu/~poneil/lsmtree.pdf>

- **Tiered LSM compaction.** Cayenne's small-files compactor (`provider/compaction.rs`) is a tiered picker (Small → Mid → Settled) inspired by classic LSM compaction policy, but adapted to the lakehouse setting: the runner rewrites the picked file subset (the smallest eligible tier) through `write_to_snapshot` into a new snapshot rather than producing strictly one output per pass, with bounds (`compaction_max_levels`, `compaction_max_files_per_pick`) protecting against unbounded write amplification.
  - Luo and Carey, *"LSM-based Storage Techniques: A Survey"*, *VLDB Journal* 29(1), 2020. <https://arxiv.org/abs/1812.07527>
  - RocksDB tiered compaction docs: <https://github.com/facebook/rocksdb/wiki/Universal-Compaction>

- **Write-ahead logging with tmp+fsync+rename atomic publish.** The staging WAL writes a `_wal.json.tmp`, fsyncs it, then renames to `_wal.json`, exploiting the atomic-rename guarantee of POSIX filesystems to make the file list appear in a single observable step. Recovery on table open reconciles the staging directory against the WAL record.
  - Mohan, Haderle, Lindsay, Pirahesh, Schwarz, *"ARIES: a transaction recovery method..."*, ACM TODS 17(1), 1992. <https://dl.acm.org/doi/10.1145/128765.128770>
  - Linux man page `rename(2)`: <https://man7.org/linux/man-pages/man2/rename.2.html>

### Deletion vectors and bitmap indexes

- **Roaring Bitmaps** for position-based deletion vectors. Each per-file deletion bitmap is stored as a `RoaringBitmap` and converted into a `RoaringTreemap` to feed Vortex's `Selection::ExcludeRoaring(...)` access plan. The conversion happens once per published deletion snapshot, not per scan.
  - Chambi, Lemire, Kaser, Godin, *"Better bitmap performance with Roaring bitmaps"*, *Software: Practice and Experience* 46(5), 2016. arXiv: <https://arxiv.org/abs/1402.6407>
  - [RoaringBitmap format specification](https://github.com/RoaringBitmap/RoaringFormatSpec)

- **Bloom filter prefilter on PK deletion probes.** Each `DeletionIndex` / `KeyDeletionIndex` carries a sized bloom filter so the per-row "is this PK deleted?" probe skips the hash-map lookup on a definite miss. The bloom is grown incrementally and rebuilt at entry-count doublings to keep the false-positive rate bounded; an empty index uses a process-wide shared instance to amortize the allocation across tables.
  - Bloom, *"Space/time trade-offs in hash coding with allowable errors"*, *Communications of the ACM* 13(7), 1970. <https://dl.acm.org/doi/10.1145/362686.362692>

- **Atomically-published immutable snapshots via `ArcSwap`** (RCU-like read-side wait-free swap). Deletion indexes, insert records, and per-file position bitmaps are built off the hot path and published with a single `ArcSwap::store`; scans take a wait-free `load_full()`. The pattern is the lock-free / RCU read-side mechanism applied to a single shared pointer.
  - McKenney and Slingwine, *"Read-copy update: Using execution history to solve concurrency problems"*, PDCS 1998. <http://www.rdrop.com/users/paulmck/RCU/rclock_OLS.2001.05.01c.pdf>
  - [`arc-swap` crate](https://docs.rs/arc-swap/)

### Query execution

- **Apache DataFusion** — the embedded query engine Cayenne integrates with as a `TableProvider`. Cayenne plugs into DataFusion's logical and physical optimizer pipelines via `CayennePropagateFilterAcrossEquiJoinKeys`, `CayenneInListToRangeRewrite`, `CayennePushDownSemiJoin`, `CayenneReassociateCrossJoin`, `CayenneAntiJoinSortMergeRewriter`, and `CayenneDynamicFilterSharing`, while relying on DataFusion's native hash-join dynamic-filter pushdown for the ordinary inner-join probe side.
  - [Apache DataFusion](https://datafusion.apache.org/)
  - Andrew Lamb et al., *"Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine"*, SIGMOD 2024 Industrial Track. <https://www.cidrdb.org/cidr2024/papers/p17-lamb.pdf>

- **Predicate transitive closure through equi-join keys.** When a filter sits on a non-key column of one side of an inner/semi join, propagating an `IN (SELECT key FROM filtered_side)` to the opposite side gives the optimizer a tighter cardinality bound to plan against. This is `CayennePropagateFilterAcrossEquiJoinKeys`.
  - Graefe, *"Query Evaluation Techniques for Large Databases"*, *ACM Computing Surveys* 25(2), 1993 — §"Predicate move-around". <https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf>

- **Sort-merge join with spillable inputs** as a fallback for non-spillable hash joins. DataFusion's `HashJoinExec` build side is non-spillable, so above a configurable build-row and memory threshold Cayenne rewrites same-source semi/anti hash joins into `SortMergeJoinExec` over explicit `SortExec` nodes that can spill to disk.
  - Graefe, *"Sort-Merge-Join: An idea whose time has(h) passed?"*, ICDE 1994. <https://ieeexplore.ieee.org/document/283071>
  - DataFusion `SortMergeJoinExec` docs: <https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html>

- **Cross-scan dynamic filter sharing.** When a hash join produces a runtime min/max / in-list filter that has been pushed into one Cayenne scan, the same `Arc<DynamicFilterPhysicalExpr>` is installed on sibling scans backed by the same table and equi-joined column set, so every consumer sees filter values as soon as the producing join accumulates them. The shape mirrors the runtime-filter / "bloom filter join" pattern used in Impala and other MPP engines.
  - Impala blog, *"Runtime Filtering in Impala"*: <https://impala.apache.org/docs/build/html/topics/impala_runtime_filtering.html>

### Concurrency and durability

- **SQLite WAL mode** for the metastore. Allows concurrent readers and a single writer at the engine level; combined with Cayenne's connection pool this lifts the read-side concurrency ceiling.
  - SQLite WAL documentation: <https://www.sqlite.org/wal.html>

- **libSQL `BEGIN CONCURRENT`** (MVCC writers) on Turso. Lets multiple writers run in parallel and serialize at commit time on actual conflicts, rather than at BEGIN time.
  - Turso `BEGIN CONCURRENT`: <https://github.com/tursodatabase/libsql/blob/main/docs/BEGIN_CONCURRENT.md>

- **UUIDv7** for `table_id`, `delete_file_id`, snapshot ids, and other catalog IDs. Time-ordered UUIDs keep newly-created rows clustered in B-tree-ordered SQLite primary indexes, reducing page splits on insert-heavy workloads.
  - [RFC 9562 — UUIDv7](https://www.rfc-editor.org/rfc/rfc9562)

- **POSIX atomic rename** for the staging WAL marker and Vortex file moves into the current snapshot directory.
  - `rename(2)`: <https://man7.org/linux/man-pages/man2/rename.2.html>

- **S3 Express One Zone** for low-latency object storage of the Vortex tier, with optional multi-zone replication and best-effort rollback on partial failure. The metastore stays on local disk (SQLite cannot run on object storage).
  - AWS S3 Express One Zone: <https://aws.amazon.com/s3/storage-classes/express-one-zone/>

### Benchmarks and related work

- **SpiceBench** — Spice.ai's end-to-end benchmark for hybrid lake + database platforms. SpiceBench streams pre-generated data continuously into the system under test while concurrently executing query workloads, capturing the tension between ingestion throughput, materialization freshness, and query latency under load. Cayenne's CDC apply pipelining, inline-memtable absorption, tiered compaction, and post-write maintenance debouncing are all design responses to the workload shape SpiceBench measures; the primary ranking metric (`test_duration_ms`, end-to-end wall-clock from test start through ETL completion) is the metric these mechanisms collectively optimize. Today SpiceBench covers a TPC-H scenario with ClickBench and custom datasets planned, and publishes results at <https://spicebench.com>.
  - Repository: <https://github.com/spiceai/spicebench>
  - Site: <https://spicebench.com>
- The "no-spill build-side memory strategy" documented in `crates/cayenne/src/optimizer_rules.rs` (same-source semi/anti-join `HashJoin` → `SortMergeJoin` rewrite, gated primarily on the build side exhausting a memory-pool fraction and falling back to a 10M-row build-side count) builds on classical join-spilling literature; the rewriter targets the chbench q21 shape specifically.
- **chbench (CH-benCHmark)** — the HTAP benchmark whose q17/q21 multi-way joins drove the predicate-propagation and sort-merge rewrites in `optimizer_rules.rs` and `logical_optimizer.rs`. Used as a chbench-shape micro-driver inside the Spice runtime tests; the broader end-to-end HTAP coverage is provided by SpiceBench.
  - Cole, Funke, Giakoumakis, Guy, Kemper, Krompass, Kuno, Nambiar, Neumann, Poess, Sattler, Seibold, Simon, Waas, *"The mixed workload CH-benCHmark"*, DBTest 2011. <https://dl.acm.org/doi/10.1145/1988842.1988850>

## References

- [DuckLake Specification v1.0](https://ducklake.select/docs/stable/specification/introduction)
- [DuckLake Tables (v1.0)](https://ducklake.select/docs/stable/specification/tables/overview)
- [Vortex Format](https://github.com/spiraldb/vortex)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Arrow](https://arrow.apache.org/)
- [Apache DataFusion](https://datafusion.apache.org/)
- [SQLite WAL](https://www.sqlite.org/wal.html)
- [libSQL / Turso](https://github.com/tursodatabase/libsql)
- [RoaringBitmap format specification](https://github.com/RoaringBitmap/RoaringFormatSpec)
- [RFC 9562 — UUIDv7](https://www.rfc-editor.org/rfc/rfc9562)
- [AWS S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/)
- [SpiceBench](https://github.com/spiceai/spicebench) — Spice.ai's end-to-end HTAP benchmark; published results at [spicebench.com](https://spicebench.com)
