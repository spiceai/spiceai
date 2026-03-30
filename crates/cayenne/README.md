# Cayenne

A DuckLake-inspired lakehouse format for the Vortex accelerator that combines pluggable metastore backends (SQLite, Turso) for metadata management with Vortex files as the data lake.

## Overview

Cayenne provides a lakehouse format that enables efficient CRUD operations on columnar data with the following features:

- **Pluggable Metastore Backends**: Transactional metadata management with support for SQLite and Turso (optional)
- **Vortex Data Files**: High-performance columnar storage with compression
- **Deletion Vectors**: Efficient delete tracking using Arrow IPC files, supporting both position-based and key-based deletion
- **Sequence-Based Ordering**: Iceberg-style sequence numbers for correct delete/insert ordering across snapshots
- **Partition Metadata**: File-based partitioning; metadata supports composite partition keys (current public API exposes a single partition column)
- **Staging WAL**: Crash-safe write-ahead log for in-progress writes

## Architecture

```text
┌──────────────────────────────────────────┐
│      CayenneTableProvider                │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │   Metastore Backend                │  │
│  │   (SQLite or Turso)                │  │
│  │                                    │  │
│  │  - Table Schemas & Config          │  │
│  │  - Delete File References          │  │
│  │  - Partition Metadata              │  │
│  │  - Insert Records (PK tracking)    │  │
│  │  - Snapshot Sequences              │  │
│  └────────────────────────────────────┘  │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │   Vortex Data Lake                 │  │
│  │                                    │  │
│  │  └─ <table_id>/                    │  │
│  │      ├─ <snapshot_id>/              │  │
│  │      │   ├─ data_001.vortex         │  │
│  │      │   ├─ data_002.vortex         │  │
│  │      │   └─ deletions/              │  │
│  │      │       └─ del_001.arrow       │  │
│  │      └─ <snapshot_id>/              │  │
│  │          └─ ...                     │  │
│  └────────────────────────────────────┘  │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │   Optional: Object Store           │  │
│  │   (S3, S3 Express One Zone)        │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

## Key Components

### 1. Metastore Backend (`metastore.rs`)

The `MetastoreBackend` trait defines a pluggable storage abstraction for metadata:

```rust
#[async_trait]
pub trait MetastoreBackend: Send + Sync {
    async fn init_schema(&self) -> CatalogResult<()>;
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;
    async fn execute_batch(&self, sql: &str) -> CatalogResult<()>;
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>;
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>;
    async fn shutdown(&self) -> CatalogResult<()>;
}
```

Transactions are handled by a separate `MetastoreTransaction` trait:

```rust
#[async_trait]
pub trait MetastoreTransaction: Send + Sync {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()>;
    async fn query_row<F, T>(&self, params: QueryRowParams<'_>, f: F) -> CatalogResult<T>;
    async fn query<F, T>(&self, params: QueryParams<'_>, f: F) -> CatalogResult<Vec<T>>;
    async fn commit(self) -> CatalogResult<()>;
    async fn rollback(self) -> CatalogResult<()>;
}
```

**Implementations:**

- **SQLite** (`metastore/sqlite.rs`): Default backend using rusqlite with WAL mode for concurrent access
- **Turso** (`metastore/turso.rs`): Optional backend using libsql/Turso (requires `turso` feature flag)

### 2. Metadata Catalog (`catalog.rs`)

The `MetadataCatalog` trait defines the interface for metadata operations:

```rust
#[async_trait]
pub trait MetadataCatalog: Send + Sync {
    async fn init(&self) -> CatalogResult<()>;
    async fn list_table_names(&self) -> CatalogResult<Vec<String>>;
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64>;
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;
    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()>;
    async fn increment_sequence_number(&self, table_id: i64) -> CatalogResult<i64>;
    async fn get_sequence_number(&self, table_id: i64) -> CatalogResult<i64>;
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64>;
    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>>;
    async fn remove_delete_files(&self, table_id: i64, delete_file_ids: &[i64]) -> CatalogResult<()>;
    async fn clear_delete_files(&self, table_id: i64) -> CatalogResult<()>;
    async fn add_insert_record(&self, table_id: i64, pk_bytes: Vec<u8>, sequence_number: i64) -> CatalogResult<()>;
    async fn add_insert_records_batch(&self, table_id: i64, pk_bytes_list: Vec<Vec<u8>>, sequence_number: i64) -> CatalogResult<()>;
    async fn get_insert_records(&self, table_id: i64) -> CatalogResult<HashMap<Box<[u8]>, i64>>;
    async fn clear_insert_records(&self, table_id: i64) -> CatalogResult<()>;
    async fn set_snapshot_sequence(&self, table_id: i64, snapshot_id: &str, sequence_number: i64) -> CatalogResult<()>;
    async fn get_snapshot_sequence(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<Option<i64>>;
    async fn get_all_snapshot_sequences(&self, table_id: i64) -> CatalogResult<HashMap<String, i64>>;
    async fn clear_snapshot_sequence(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()>;
    async fn commit_compaction(&self, table_id: i64, new_snapshot_id: &str) -> CatalogResult<()>;
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64>;
    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>>;
    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool>;
    async fn shutdown(&self) -> CatalogResult<()>;
}
```

Implementation: `CayenneCatalog` (`cayenne_catalog.rs`), backed by any `MetastoreBackend`.

### 3. Metadata Structures (`metadata.rs`)

Core data structures:

- **`TableMetadata`**: Table schema, configuration, current snapshot ID, and sequence number
- **`DataFile`**: Reference to a Vortex data file with partition and sequence tracking
- **`DeleteFile`**: Reference to a deletion vector (Arrow IPC file) with sequence number
- **`VortexConfig`**: Vortex file compression and caching configuration

```rust
pub struct VortexConfig {
    pub footer_cache_mb: usize,       // default: 128
    pub segment_cache_mb: usize,      // default: 256
    pub target_vortex_file_size_mb: usize, // default: 128
    pub sort_columns: Vec<String>,    // default: empty
    pub compression_strategy: CompressionStrategy, // default: CompressionStrategy::default()
    pub upload_concurrency: usize,    // default: 4
}
```

### 4. Deletion Vectors (`provider/delete/vector_io.rs`)

Efficient delete tracking without rewriting data files. Deletion vectors are stored as Arrow IPC files and support two modes:

```rust
pub enum DeletionIdentifier {
    /// Position-based: tracks specific row IDs within a data file
    PositionBased { file_path: String, row_ids: Vec<u64> },
    /// Key-based: tracks primary key bytes for cross-file deletion
    KeyBased(Vec<Box<[u8]>>),
}
```

The `DeletionVectorWriter` writes deletion vectors as Arrow IPC files. The `DeletionVectorReader` reads them back for query-time filtering.

### 5. Table Provider (`provider/table.rs`)

DataFusion `TableProvider` implementation with builder pattern:

```rust
pub struct CayenneTableProvider {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<RwLock<Arc<ListingTable>>>,
    retention_filters: Vec<Expr>,
    time_retention_filter_builder: Option<TimeRetentionFilterBuilder>,
    context: Arc<CayenneContext>,
    pk_deletion_strategy: PkDeletionStrategyWithCache,
    pk_row_converter: Option<Arc<RowConverter>>,
    pk_column_indices: Vec<usize>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
    object_store_config: Option<ObjectStoreConfig>,
    current_snapshot_id: Arc<RwLock<String>>,
    protected_snapshots: Arc<RwLock<HashMap<String, i64>>>,
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

- Query execution with automatic deletion vector filtering
- Insert operations via DataFusion's `insert_into()` API
- Delete via DataFusion's SQL `DELETE FROM` path
- Sequence-based ordering for correct delete/insert visibility
- Protected snapshot tracking for concurrent access

## CRUD Operations

### Create Table

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

### Insert Data

```rust
// Insert record batches via DataFusion's insert_into() API
use datafusion::prelude::*;
let ctx = SessionContext::new();
ctx.register_table("my_table", Arc::new(provider))?;
ctx.sql("INSERT INTO my_table SELECT * FROM source_table").await?.collect().await?;
```

### Delete

Deletes are performed through DataFusion's SQL `DELETE FROM` path:

```sql
DELETE FROM users WHERE id IN (1, 2, 3)
```

Deletion vectors are written as Arrow IPC files, avoiding data file rewrites.

### Query with Deletion Filters

Queries automatically apply deletion vectors and sequence-based ordering:

```sql
SELECT * FROM users WHERE id > 100
-- Deletion vectors are applied transparently
```

## DuckLake Specification Alignment

Cayenne implements a subset of the DuckLake v0.3 specification:

### Implemented

- ✅ Table metadata management
- ✅ Delete file tracking with sequence numbers
- ✅ Partition metadata (composite partition keys)

### Minimal/Simplified

- ⚠️ Schema evolution (simplified)
- ⚠️ Statistics tracking (basic)

### Not Implemented (Future)

- ❌ File compaction
- ❌ Snapshot expiration
- ❌ Column mapping
- ❌ MVCC (multi-version concurrency control)

## Database Schema

Cayenne uses these tables in the metastore (SQLite/Turso):

```sql
CREATE TABLE IF NOT EXISTS cayenne_table (
    table_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_uuid TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS cayenne_delete_file (
    delete_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    format TEXT NOT NULL,
    delete_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    source_data_file_path TEXT,
    sequence_number BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cayenne_partition (
    partition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    partition_columns_json TEXT NOT NULL,
    partition_values_json TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    record_count BIGINT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    UNIQUE(table_id, partition_key)
);

CREATE TABLE IF NOT EXISTS cayenne_insert_record (
    insert_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    pk_bytes BLOB NOT NULL,
    sequence_number BIGINT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    UNIQUE(table_id, pk_bytes)
);

CREATE TABLE IF NOT EXISTS cayenne_snapshot_sequence (
    table_id INTEGER NOT NULL,
    snapshot_id TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE,
    PRIMARY KEY (table_id, snapshot_id)
);
```

## Usage Example

```rust
use cayenne::{
    CayenneCatalog, CayenneTableProviderBuilder, CreateTableOptions,
};

// Create catalog (synchronous, returns CatalogResult)
let catalog = Arc::new(CayenneCatalog::new("sqlite:///data/catalog.db")?);
catalog.init().await?;

// Create table
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
    vortex_config: cayenne::metadata::VortexConfig::default(),
};

let provider = CayenneTableProviderBuilder::new(catalog, runtime_env)
    .create(options)
    .await?;

// Insert data via DataFusion's insert_into() API
let ctx = SessionContext::new();
ctx.register_table("events", Arc::new(provider))?;
let batch = create_record_batch()?;
ctx.read_batch(batch)?.write_table("events", DataFrameWriteOptions::new()).await?;

// Query (deletion vectors applied automatically)
let df = ctx.sql("SELECT * FROM events WHERE event_id > 1000").await?;
df.show().await?;
```

## Implementation Status

### Current Status

- ✅ Trait abstractions defined
- ✅ Data structures implemented
- ✅ Deletion vector logic (Arrow IPC, position-based and key-based)
- ✅ SQLite catalog implementation
- ✅ Turso catalog implementation (optional feature)
- ✅ Table provider with scan and deletion filtering
- ✅ Insert operations via DataFusion
- ✅ Delete via DataFusion SQL `DELETE FROM` path
- ✅ Primary key support
- ✅ Streaming data ingestion and queries
- ✅ File-mode acceleration
- ✅ S3 Express One Zone support
- ✅ Partition support (composite partition keys)
- ✅ Upsert on conflict behavior
- ✅ Retention policies (time-based and SQL-based)
- ✅ Sequence-based ordering for delete/insert visibility
- ✅ Protected snapshot tracking
- ✅ Staging WAL for crash-safe writes
- ✅ Compaction via `commit_compaction` API

### Known Limitations

The following limitations apply to the Cayenne accelerator:

#### Access Mode

- **File mode only**: Cayenne only supports file-based acceleration (`mode: file`). In-memory mode is not supported.

#### Data Types

Some Arrow data types are not natively supported by the Vortex format used by Cayenne:

- `Interval` types
- `Duration` types
- `Map` types
- `FixedSizeBinary` types
- `Float16` types (automatically converted)
- Timestamp units other than microseconds (automatically normalized)

To handle unsupported types, use the `cayenne_unsupported_type_action` parameter:

- `string` (default): Convert unsupported types to UTF-8 strings
- `error`: Fail on unsupported types
- `warn`: Include in schema but may fail on insert
- `ignore`: Skip unsupported fields

#### Indexes

- Secondary indexes are not supported. Primary keys are supported for efficient upserts and deletions.

#### MVCC

- Full MVCC (multi-version concurrency control) is not yet supported.

### Future Enhancements

- Full MVCC support
- Advanced statistics
- Additional catalog backends (PostgreSQL, DuckDB)
- Snapshot expiration and time-travel queries

## Benefits

1. **Efficient Deletes**: No data file rewrites, deletion vectors stored as Arrow IPC files
2. **ACID Transactions**: SQLite provides transaction guarantees for metadata
3. **Performance**: Vortex's compression and columnar format with configurable caching
4. **Simplicity**: Single SQLite file for metadata
5. **Flexibility**: Trait-based design allows multiple metastore backends
6. **Crash Safety**: Staging WAL ensures write atomicity
7. **Object Store Support**: Native S3 and S3 Express One Zone integration

## References

- [DuckLake Specification v0.3](https://ducklake.select/docs/stable/specification/introduction.html)
- [DuckLake Tables](https://ducklake.select/docs/stable/specification/tables/overview.html)
- [Vortex Format](https://github.com/spiraldb/vortex)
