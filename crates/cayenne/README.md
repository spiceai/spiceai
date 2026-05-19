# Cayenne

A lakehouse format for the Vortex accelerator. Combines pluggable metastore backends (SQLite, Turso) for transactional metadata with Vortex files for columnar data, plus an LSM-style level-0 inline-data tier that absorbs small writes without writing data files.

## Overview

Cayenne provides a lakehouse format that enables efficient CRUD operations on columnar data:

- **Pluggable metastore backends** (`metastore::sqlite::SqliteMetastore`, optional `metastore::turso::TursoMetastore`) for transactional metadata with `BEGIN ... COMMIT` semantics.
- **Vortex data files** as the persistent columnar tier, with configurable target file size, compression, and concurrent upload fan-out.
- **Inline-data memtable** (`cayenne_inlined_data` / `cayenne_inlined_delete` tables) absorbs small bursts directly in the metastore as Arrow IPC blobs, flushed to Vortex once accumulated rows / segments / bytes exceed configurable thresholds.
- **Deletion vectors** stored as Arrow IPC files for position-based deletion, plus an in-memory PK index (`DeletionIndex` / `KeyDeletionIndex`) for key-based deletion. Sequence-numbered for Iceberg-style upsert semantics.
- **Staging WAL** (`provider/staging_wal.rs`) provides crash-safe append commit via tmp+fsync+rename of the WAL marker, atomic rename of staged Vortex files into the current snapshot, and self-healing recovery on the next provider open.
- **Tiered small-files compaction** (`provider/compaction.rs`) triggered best-effort after writes and periodically by a per-table background compactor, gated by a shared per-accelerator semaphore so a fleet of tables can't oversubscribe the writer pool.
- **CDC apply pipelining** (`provider/mutation_writer::write_cdc_pipelined`): Stage A writes Vortex files into the staging dir under the staging WAL; Stage B (move + listing-cache invalidation) is spawned as a finalize task so the next burst's Stage A can begin work. Stage A and Stage B always preserve burst order.
- **Sequence-based ordering** (Iceberg-style) for correct delete/insert visibility across snapshots.
- **Partitioning** via composite partition keys; the current public API surface accepts a single partition column.
- **PK conflict detection opt-out** (`cayenne_pk_conflict_detection: none`) for append-only CDC workloads where the source enforces PK uniqueness and the ingestion path cannot replay existing rows.

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
│  │    │   ├─ WAL                                         │   │
│  │    │   └─ part-…vortex                                │   │
│  │    └─ <protected_snapshot_id>/                        │   │
│  │        └─ …                                           │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  In-memory state                                      │   │
│  │   listing_fence (RwLock) — read/write barrier         │   │
│  │   listing_table (ArcSwap<ListingTable>)               │   │
│  │   scan_listing_tables (cache, Mutex<HashMap>)         │   │
│  │   pk_deletion_strategy (ArcSwap<DeletionSnapshot>)    │   │
│  │   protected_snapshots (RwLock<HashMap>)               │   │
│  │   inlined_row_count (AtomicI64) — memtable size       │   │
│  │   post_write_maintenance (debounced refresh + stats)  │   │
│  │   background_compactor (per-table)                    │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  Optional: Object Store (S3, S3 Express One Zone)     │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. Metastore backend (`metastore.rs`)

The `MetastoreBackend` trait defines the pluggable storage abstraction:

```rust
#[async_trait]
pub trait MetastoreBackend: Send + Sync {
    async fn init_schema(&self) -> CatalogResult<()>;
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;
    async fn execute_batch(&self, sql: &str) -> CatalogResult<()>;
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>
    where F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send,
          T: Send;
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>
    where F: FnMut(&dyn MetastoreRow) -> CatalogResult<T> + Send,
          T: Send;
    async fn begin_transaction(&self) -> CatalogResult<Box<dyn MetastoreTransaction>>;
    async fn shutdown(&self) -> CatalogResult<()>;
}
```

Transactions go through a separate `MetastoreTransaction` trait that owns the transaction handle and is consumed by `commit` / `rollback`:

```rust
#[async_trait]
pub trait MetastoreTransaction: Send {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;
    async fn execute_batch(&self, sql: &str) -> CatalogResult<()>;
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T> where ...;
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>> where ...;
    async fn commit(self: Box<Self>) -> CatalogResult<()>;
    async fn rollback(self: Box<Self>) -> CatalogResult<()>;
}
```

**Implementations:**

- **`SqliteMetastore`** (`metastore/sqlite.rs`): default; `tokio-rusqlite` with WAL mode and busy-timeout. All metastore operations serialize through one `tokio::sync::Mutex<Connection>`, so writes across all tables sharing the same metastore are ordered.
- **`TursoMetastore`** (`metastore/turso.rs`): optional, gated on the `turso` feature. libSQL/Turso backend that supports `BEGIN CONCURRENT` for higher write parallelism.

### 2. Metadata catalog (`catalog.rs`)

`MetadataCatalog` is the higher-level interface that the table provider uses; `CayenneCatalog` (`cayenne_catalog.rs`) is the concrete implementation backed by any `MetastoreBackend`. Selected methods (full signature in `catalog.rs`):

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

    // Persisted table statistics (column-level, loaded from Vortex footers)
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
- **`DataFile`** — virtual file (a directory containing one or more Vortex files), with row count, byte size, partition id, sequence number, and a row-id base.
- **`DeleteFile`** — deletion vector reference (Arrow IPC file), with `DeletionType` (position- vs key-based) and sequence number.
- **`InlinedData`** — Arrow IPC blob stored inline in the metastore, with row count and sequence number.
- **`InlinedDelete`** — inline tombstone for upserted/deleted PKs that haven't yet been checkpointed to a delete-vector file.
- **`InlinedDataStats`** — `{ total_rows, segment_count, total_bytes }` aggregated from `cayenne_inlined_data` for memtable-pressure decisions.
- **`PartitionMetadata`** — composite partition key, partition path, record/byte counts.
- **`TableStatistics`** — serialized `FileStatistics` blob plus `num_rows`; populated from Vortex file footers and read by the DataFusion planner.
- **`VortexConfig`** — Vortex-side tuning. Most fields are configurable per dataset via `cayenne_*` runtime parameters. Footer metadata cache sizing is runtime-global and configured with `runtime.params.cayenne_footer_cache_mb`; when set, the configured value is stored in the metastore for compatibility validation during dataset registration. The runtime applies refresh-mode defaults before parsing explicit params: `refresh_mode: caching`, `changes`, and `append` with `refresh_check_interval <= 5m` favor small incremental writes, while manual/cron/long-interval append plus `refresh_mode: full`, `snapshot`, `disabled`, and unspecified refresh modes favor large Vortex writes by default. Append workloads can be small or large depending on caller batch size, so tune the inline and compaction parameters explicitly if refresh cadence does not reflect write size.

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
    pub write_concurrency: Option<usize>,     // None = session target_partitions; forced to 1 if sort_columns set

    // Compaction
    pub compaction_trigger_files: usize,      // default caching/changes/short-append=4, otherwise=8
    pub compaction_trigger_protected_snapshots: usize, // default caching/changes/short-append=4, otherwise=8
    pub compaction_trigger_snapshot_age_ms: u64,  // default caching/changes/short-append=60_000, otherwise=300_000; 0 disables age trigger
    pub compaction_max_levels: usize,         // default 3
    pub compaction_max_files_per_pick: usize, // default 32
    pub compaction_background_interval_ms: u64,  // default caching/changes/short-append=10_000, otherwise=30_000; 0 disables background loop

    // Inline-write admission (per-call gate)
    pub inline_max_rows: usize,               // default caching/changes/short-append=1_024, otherwise=0
    pub inline_max_bytes: usize,              // default caching/changes/short-append=1_048_576, otherwise=0
    pub inline_max_buffer_bytes: usize,       // default caching/changes/short-append=4_194_304, otherwise=0

    // Inline-memtable flush triggers (cumulative gate)
    pub inline_flush_max_rows: i64,           // default caching/changes/short-append=2_048, otherwise=10_000
    pub inline_flush_max_segments: i64,       // default caching/changes/short-append=16, otherwise=64
    pub inline_flush_max_bytes: i64,          // default caching/changes/short-append=2_097_152, otherwise=8_388_608

    // PK conflict detection
    pub pk_conflict_detection: PkConflictDetection,  // default Auto; None opts into blind append for CDC
}
```

Two distinct threshold groups for inline data — `inline_max_*` is the *per-write admission* gate ("is this single write small enough to absorb into the memtable?"); `inline_flush_max_*` is the *cumulative flush* gate ("has the accumulated memtable grown enough that we should checkpoint it to Vortex?").

### 4. Deletion vectors (`provider/delete/vector_io.rs`)

Two deletion modes, persisted as Arrow IPC files referenced by `cayenne_delete_file`:

```rust
pub enum DeletionIdentifier {
    /// Position-based: row positions inside a specific data file.
    PositionBased { row_ids: Vec<i64> },
    /// Key-based: PK bytes; survive partition reorganization and parallel coalescing.
    KeyBased(Vec<Box<[u8]>>),
}
```

At scan time:

- **Position-based** strategy attaches a `RoaringBitmap` per file via `Selection::ExcludeRoaring`, pushed down to the Vortex scan layer (`provider/vortex_format::DeletionFilteringVortexFormat`).
- **Key-based** strategy (Int64 PK or row-key) runs `Int64PkDeletionFilterExec` / `KeyBasedDeletionFilterExec` (`provider/delete/filter_exec.rs`) above the file scan. Each row's PK is bloom-prefiltered, then probed against the cached `DeletionIndex` / `KeyDeletionIndex`.

The deletion index plus its companion insert-records index are published as a single atomic snapshot (`Int64PkDeletionSnapshot` / `RowConverterDeletionSnapshot` — `provider/deletion_strategy.rs`), held in one `ArcSwap` per table so concurrent scans observe consistent `(deleted, insert_records)` pairs even mid-upsert.

### 5. Table provider (`provider/table.rs`)

DataFusion `TableProvider` implementation. Constructed via `CayenneTableProviderBuilder`. The struct holds (abbreviated — full list in source):

```rust
pub struct CayenneTableProvider {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,

    // Listing-table state
    listing_table: Arc<ArcSwap<ListingTable>>,        // legacy stats path
    listing_fence: Arc<tokio::sync::RwLock<()>>,      // read/write barrier
    scan_listing_tables: Arc<ParkingMutex<HashMap<ScanListingTableKey, Arc<ListingTable>>>>,
    table_statistics: Arc<parking_lot::RwLock<Option<Statistics>>>,

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
    current_snapshot_id: Arc<parking_lot::RwLock<String>>,
    protected_snapshots: Arc<parking_lot::RwLock<HashMap<String, i64>>>,

    // Memtable + maintenance
    inlined_row_count: Arc<AtomicI64>,
    new_files_since_last_compaction: Arc<AtomicUsize>,
    staging_wal_present: Arc<AtomicBool>,
    staging_may_have_files: Arc<AtomicBool>,
    post_write_compaction_scheduled: Arc<AtomicBool>,
    post_write_maintenance: Arc<PostWriteMaintenance>,
    background_compactor: Arc<OnceLock<BackgroundCompactor>>,
}
```

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
- Deletes via DataFusion's SQL `DELETE FROM` path.
- Sequence-based ordering for correct delete/insert visibility.
- Protected snapshot tracking for concurrent access.
- Per-scan ListingTable cache and per-`RuntimeEnv` object-store registration short-circuit.

### 6. CDC apply pipeline (`provider/mutation_writer.rs`, `provider/staging_wal.rs`)

`write_cdc_append_stream` is the entry point used by the runtime's CDC apply loop (`crates/runtime/src/accelerated_table/refresh_task/changes.rs`). Per burst:

1. Acquire `write_lock`.
2. `ensure_no_incomplete_write` — error if a previous burst's WAL is on disk and unreconciled.
3. `prepare_stream_for_insert` — if `pk_conflict_detection: auto` (default), build an existing-PK keyset via `load_existing_keyset` and resolve on-conflict deletions; if `pk_conflict_detection: none`, skip.
4. Decide `can_stage_for_pipeline`: simple appends (no sort columns, no partition column, no retention filters, no pending PK deletions, no file/on-conflict deletions) take the pipelined path; others fall back to a fully synchronous write.
5. **Stage A** — `write_to_snapshot` into the staging dir; `write_staging_wal` makes the file list durable via tmp+fsync+rename.
6. Return a `CayenneCdcWrite` holding the staged-write handle and the still-held write lock; the runtime spawns Stage B on a background task.
7. **Stage B** — under the listing fence: `move_files_to_current_snapshot`, `remove_staging_wal`, `publish_current_snapshot_files_changed` (invalidates DataFusion's list-files cache). The write lock drops when Stage B completes.

Stage A and Stage B preserve burst order via the runtime's `PendingApplyFinalize` FIFO. The runtime acks the source-side LSN after Stage A returns (data durable) without waiting for Stage B (data visible), so PG can recycle WAL ahead of visibility.

### 7. Compaction (`provider/compaction.rs`)

Tiered small-files compaction picks the smallest eligible file tier whose total size and file count exceed thresholds, and rewrites the current snapshot through the same `write_to_snapshot` + `commit_compaction` path as writes. Triggered by:

- **Inline post-write trigger** (`schedule_post_write_compaction`): `tokio::spawn` with an `AcqRel` dedup flag so at most one inline pass is queued per table.
- **Background compactor** (`BackgroundCompactor`): per-table periodic task gated by a shared per-accelerator semaphore (`Semaphore::new(available_parallelism())`).
- **Inline memtable flush** (`checkpoint_inlined_data_if_memtable_pressure_exceeded`): drains `cayenne_inlined_data` into a Vortex file when cumulative rows / segments / IPC bytes exceed `inline_flush_max_*`.

All compaction triggers `try_lock` the table write lock and skip if a writer is active.

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
| **Namespaces / schemas**    | `ducklake_schema` supports nested namespaces                                                                                         | Flat table namespace                                                                                                                                                           |
| **Upsert / PK semantics**   | Snapshot-based merge                                                                                                                 | Iceberg-style PK insert tracking in `cayenne_insert_record`, paired with `cayenne_inlined_delete` tombstones                                                                   |
| **GC**                      | `ducklake_files_scheduled_for_deletion` work queue                                                                                   | Old-snapshot cleanup triggered inline by compaction/sort/overwrite paths                                                                                                       |
| **Views, SQL macros, tags** | First-class (`ducklake_view`, `ducklake_macro*`, `ducklake_tag`, `ducklake_column_tag`)                                              | Not implemented                                                                                                                                                                |
| **Sort metadata**           | `ducklake_sort_expression`, `ducklake_sort_info`                                                                                     | `sort_columns` is a per-dataset config field, not a catalog table                                                                                                              |
| **Variant column stats**    | `ducklake_file_variant_stats`                                                                                                        | Not implemented                                                                                                                                                                |

### What Cayenne implements relative to its own goals

- Table metadata with sequence-numbered operations
- Position- and key-based delete files
- Composite partition keys (single partition column via current public API)
- Tiered small-files compaction (inline + background)
- Inline-data memtable with per-write and cumulative-flush thresholds
- CDC apply pipelining with debounced post-write maintenance
- Protected-snapshot scan routing for upsert correctness

### Not implemented (and not currently planned)

- Schema evolution at column-row granularity (column adds / drops / renames / mappings)
- SQL macros, views, table/column tags
- Snapshot expiration and time-travel queries
- Full MVCC

If interoperability with a DuckLake catalog reader is a requirement, Cayenne is not the right tool. If a Vortex-native, CDC-friendly accelerator backed by SQLite or Turso fits the workload, Cayenne is purpose-built for that.

## Database schema

The metastore (SQLite or Turso) materializes these tables. DDL lives in `crates/cayenne/src/metastore/sqlite.rs:198+` and is mirrored by Turso.

```sql
CREATE TABLE IF NOT EXISTS cayenne_table (
    table_id TEXT PRIMARY KEY,          -- UUIDv7
    table_name TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    path_is_relative INTEGER NOT NULL,
    schema_json TEXT NOT NULL,
    primary_key_json TEXT,
    on_conflict_json TEXT,
    current_snapshot_id TEXT NOT NULL DEFAULT '',
    partition_column TEXT,
    vortex_config_json TEXT,
    current_sequence_number INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cayenne_delete_file (
    delete_file_id TEXT PRIMARY KEY,    -- UUIDv7
    table_id TEXT NOT NULL,
    source_data_file_path TEXT,
    path TEXT NOT NULL,
    path_is_relative INTEGER NOT NULL,
    format TEXT NOT NULL,
    delete_count INTEGER NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    deletion_type TEXT NOT NULL,
    sequence_number INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cayenne_partition (
    partition_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    partition_columns_json TEXT NOT NULL,
    partition_values_json TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative INTEGER NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    file_size_bytes INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    UNIQUE(table_id, partition_key)
);

CREATE TABLE IF NOT EXISTS cayenne_insert_record (
    insert_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id TEXT NOT NULL,
    pk_bytes BLOB NOT NULL,
    sequence_number INTEGER NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    UNIQUE(table_id, pk_bytes)
);

CREATE TABLE IF NOT EXISTS cayenne_snapshot_sequence (
    table_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    PRIMARY KEY (table_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS cayenne_table_statistics (
    table_id TEXT PRIMARY KEY,
    num_rows INTEGER NOT NULL,
    statistics_blob BLOB NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cayenne_inlined_data (
    inlined_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    partition_key TEXT,
    data_ipc BLOB NOT NULL,             -- Arrow IPC stream
    record_count INTEGER NOT NULL,
    sequence_number INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);
CREATE INDEX idx_cayenne_inlined_data_table_seq
    ON cayenne_inlined_data(table_id, sequence_number);

CREATE TABLE IF NOT EXISTS cayenne_inlined_delete (
    inlined_delete_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    delete_ipc BLOB NOT NULL,           -- Arrow IPC stream of PK row keys
    sequence_number INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);
CREATE INDEX idx_cayenne_inlined_delete_table_seq
    ON cayenne_inlined_delete(table_id, sequence_number);
```

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

- Pluggable metastore (SQLite default; Turso optional)
- Position- and key-based deletion vectors
- Primary keys, upsert on-conflict, retention policies (time- and SQL-based)
- Sequence-based ordering with protected snapshots
- Streaming data ingestion and queries
- File-mode acceleration
- S3 and S3 Express One Zone support
- Composite partition keys
- Staging WAL with crash-safe recovery
- Tiered small-files compaction (inline + background)
- Inline-data memtable (per-write admission + cumulative flush thresholds, both configurable)
- CDC apply pipelining with debounced post-write maintenance
- Per-dataset `cayenne_pk_conflict_detection` opt-out for append-only CDC
- CDC apply observability metrics (`dataset_acceleration_cdc_apply_*`)
- Same-source large-join `HashJoin → SortMergeJoin` rewriter for spillable hash-join build sides

### Known limitations

#### Access mode

Cayenne supports `mode: file` only. In-memory mode is not supported.

#### Data types

Some Arrow data types are not natively supported by the Vortex format:

- `Interval` types
- `Duration` types
- `Map` types
- `FixedSizeBinary` types
- `Float16` (automatically converted)
- Timestamp units other than microseconds (automatically normalized)

The `cayenne_unsupported_type_action` parameter controls handling:

- `string` (default): convert unsupported types to UTF-8 strings
- `error`: fail on unsupported types
- `warn`: include in schema but may fail on insert
- `ignore`: skip unsupported fields

#### Indexes

Secondary indexes are not supported. Primary keys drive efficient upserts and deletions.

#### MVCC

Full MVCC (multi-version concurrency control) is not supported.

### Future enhancements

- Snapshot expiration and time-travel queries
- Full MVCC support
- Advanced statistics (column-level histograms, sketches)
- Additional catalog backends (PostgreSQL, DuckDB)
- Apply-side pipelining at finer granularity (Stage A of burst N+1 overlapping Stage B of burst N without write-lock serialization)
- Cached `insert_into` execution plan reuse across CDC bursts

## Benefits

1. **Efficient deletes**: deletion vectors stored as Arrow IPC files; no data-file rewrites.
2. **ACID metadata**: SQLite (or Turso) provides transaction guarantees for catalog operations.
3. **Performance**: Vortex columnar format with configurable compression and caches; inline memtable absorbs small writes without writing data files.
4. **Crash safety**: staging WAL with tmp+fsync+rename ensures atomic visibility, with self-healing recovery on next open.
5. **Object store support**: native S3 and S3 Express One Zone integration.
6. **CDC-friendly**: Stage A / Stage B pipelining, debounced maintenance, and optional blind-append mode for append-only ingestion.
7. **Flexibility**: trait-based metastore lets the same catalog logic run against SQLite or Turso.

## Research behind Spice Cayenne

Cayenne is an engineering synthesis of several lines of database research. The
references below are the ones most directly load-bearing for the design decisions
in this crate.

### Lakehouse formats and metadata catalogs

- **DuckLake** — DuckDB's specification for a SQL-catalogued lakehouse. Cayenne
  shares high-level shape with DuckLake (transactional metadata catalog plus
  object-store data) but diverges substantively (Vortex instead of Parquet,
  no per-file data-file table, JSON-blob schema instead of column-level rows,
  no views/macros/tags). See the *Relationship to the DuckLake specification*
  section above for the full table-by-table comparison against v1.0.
  - [DuckLake Specification v1.0](https://ducklake.select/docs/stable/specification/introduction)
  - DuckDB blog: *"Announcing DuckLake"* — <https://duckdb.org/2025/05/27/ducklake.html>
- **Apache Iceberg** — table format with sequence-number-driven snapshot
  visibility and position/equality delete files. Cayenne's
  `cayenne_snapshot_sequence`, sequence-ordered insert/delete semantics, and
  protected-snapshot scan routing follow Iceberg's model. The Iceberg spec is
  authoritative for the visibility rules Cayenne reimplements for Vortex.
  - [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- **Delta Lake** — Databricks' transactional log over Parquet. Not implemented
  by Cayenne, but informs the trade-offs around `_delta_log`-style file logs vs.
  Cayenne's catalog-table approach.
  - Armbrust et al., *"Delta Lake: high-performance ACID table storage over
    cloud object stores"*, VLDB 2020.

### Columnar storage and compression

- **Vortex** — Spiral DB's open-source columnar file format, the persistent
  storage tier for Cayenne. Provides predicate pushdown, zone maps, and a
  pluggable compression strategy.
  - [spiraldb/vortex](https://github.com/spiraldb/vortex)
- **BtrBlocks** — adaptive columnar compression scheme used as one of Vortex's
  strategies; Cayenne exposes it as `cayenne_compression_strategy: btrblocks`.
  Kuschewski et al., *"BtrBlocks: Efficient Wire-Compatible Compression for Data
  Lakes"*, SIGMOD 2023.
- **Apache Arrow** — in-memory columnar format and Arrow IPC stream encoding.
  Cayenne serializes inline-memtable entries and key-based deletion vectors as
  Arrow IPC blobs in the metastore.
  - [Apache Arrow](https://arrow.apache.org/)

### Write-optimized storage (LSM-tree)

- **The Log-Structured Merge-Tree (LSM-Tree)** — O'Neil, Cheng, Gawlick, O'Neil,
  *Acta Informatica* 33(4), 1996. The level-0 ↔ on-disk-tiers structure
  Cayenne uses for inline data (memtable in metastore + flush to Vortex files)
  is the LSM pattern adapted to a transactional metastore.
  - Author-hosted PDF: <https://www.cs.umb.edu/~poneil/lsmtree.pdf>
- **LSM-based Storage Techniques: A Survey** — Luo and Carey, *VLDB Journal*
  29(1), 2020. Surveys compaction strategies and tiering decisions relevant to
  Cayenne's tiered small-files compactor.

### Deletion vectors and bitmap indexes

- **Roaring Bitmaps** — the bitmap encoding used by Cayenne's position-based
  deletion vectors (`Selection::ExcludeRoaring` pushed into Vortex).
  - Chambi, Lemire, Kaser, Godin, *"Better bitmap performance with Roaring
    bitmaps"*, *Software: Practice and Experience* 46(5), 2016.
    arXiv preprint: <https://arxiv.org/abs/1402.6407>
  - [RoaringBitmap format specification](https://github.com/RoaringBitmap/RoaringFormatSpec)

### Query execution

- **Apache DataFusion** — the embedded query engine Cayenne integrates with as
  a `TableProvider`. Cayenne's optimizer rules (`CayenneJoinRewriter`,
  `CayenneAntiJoinSortMergeRewriter`, `CayenneDynamicFilterSharing`,
  `CayennePropagateFilterAcrossEquiJoinKeys`) plug into DataFusion's physical
  and logical optimizer pipelines.
  - [Apache DataFusion](https://datafusion.apache.org/)

### Related work referenced in optimizer-rule design

- The "no-spill build-side memory strategy" documented in
  `crates/cayenne/src/optimizer_rules.rs` (Inner-join → SortMergeJoin rewrite
  above a 10M-row build-side threshold) builds on classical join-spilling
  literature; the rewriter targets the chbench q21 shape specifically.

## References

- [DuckLake Specification v1.0](https://ducklake.select/docs/stable/specification/introduction)
- [DuckLake Tables (v1.0)](https://ducklake.select/docs/stable/specification/tables/overview)
- [Vortex Format](https://github.com/spiraldb/vortex)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Arrow](https://arrow.apache.org/)
- [Apache DataFusion](https://datafusion.apache.org/)
