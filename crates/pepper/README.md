# Pepper

A minimal DuckLake-inspired implementation for the Vortex accelerator that combines SQLite for metadata management with Vortex files as the data lake.

## Overview

Pepper provides a lakehouse format that enables efficient CRUD operations on columnar data with the following features:

- **SQLite Metadata Catalog**: Transactional metadata management following the DuckLake specification
- **Vortex Data Files**: High-performance columnar storage with compression

**Note**: While the codebase includes a snapshot module (`snapshot.rs`) and a `SnapshotManager` API stub, snapshot functionality is not yet implemented or available in this version of Pepper. Similarly, MVCC and deletion vectors are not supported. These features are planned for future releases.

## Architecture

```
┌─────────────────────────────────────┐
│      Pepper Table                   │
│                                     │
│  ┌──────────────────────────────┐  │
│  │   SQLite Metadata Catalog    │  │
│  │                              │  │
│  │  - Table Schemas            │  │
│  │  - Data File References     │  │
│  │  - Delete File References   │  │
│  └──────────────────────────────┘  │
│                                     │
│  ┌──────────────────────────────┐  │
│  │   Vortex Data Lake           │  │
│  │                              │  │
│  │  ├─ data_001.vortex          │  │
│  │  ├─ data_002.vortex          │  │
│  │  └─ data_003.vortex          │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

## Key Components

### 1. Metadata Catalog (`catalog.rs`)

The `MetadataCatalog` trait defines the interface for metadata operations:

```rust
#[async_trait]
pub trait MetadataCatalog: Send + Sync {
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64>;
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;
    async fn add_data_file(&self, data_file: DataFile) -> CatalogResult<i64>;
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64>;
    // ... more operations
}
```

Implementations:

- `PepperCatalog`: SQLite-based catalog (primary implementation)
- Future: PostgreSQL, DuckDB, etc.

### 2. Metadata Structures (`metadata.rs`)

Core data structures following the DuckLake specification:

- **`Snapshot`** _(planned)_: Point-in-time view with MVCC semantics (future work)
- **`TableMetadata`**: Table schema and configuration
- **`DataFile`**: Reference to a Vortex data file
- **`DeleteFile`**: Reference to a deletion vector (Parquet file)

### 3. Deletion Vectors (`delete_vectors.rs`)

Efficient delete tracking without rewriting data:

```rust
pub struct DeletionVector {
    deleted_rows: HashSet<i64>,
}

impl DeletionVector {
    pub fn delete_row(&mut self, row_id: i64);
    pub fn is_deleted(&self, row_id: i64) -> bool;
}
```

Deletion vectors are stored as Parquet files with a single `row_id` column.

### 4. Table Provider (`provider.rs`)

DataFusion `TableProvider` implementation:

```rust
pub struct PepperTableProvider {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    deletion_filter: Arc<DeletionFilter>,
    listing_table: Arc<ListingTable>,
}
```

Provides:

- Query execution with deletion vector filtering
- Insert operations
- Delete by primary key
- Update by primary key (delete + insert)

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
    base_path: "/data/users".to_string(),
};

let table = PepperTableProvider::create_table(catalog, options).await?;
```

### Insert Data

```rust
// Insert record batches
let rows_inserted = table.insert(record_batch_stream).await?;
```

### Delete by Primary Key

```rust
// Delete rows with specific primary key values
let key_values = vec![
    serialize_key(1),
    serialize_key(2),
];
let rows_deleted = table.delete_by_primary_key(key_values).await?;
```

### Update by Primary Key

```rust
// Update (implemented as delete + insert)
let rows_updated = table.update_by_primary_key(key_values, new_values).await?;
```

### Query with Deletion Filters

Queries automatically apply deletion vectors:

```sql
SELECT * FROM users WHERE id > 100
-- Deletion vectors are applied transparently
```

## DuckLake Specification Alignment

Pepper implements a subset of the DuckLake v0.3 specification:

### Implemented

- ✅ Table metadata management
- ✅ Data file tracking

### Minimal/Simplified

- ⚠️ Schema evolution (simplified)
- ⚠️ Statistics tracking (basic)
- ⚠️ Partitioning (not yet implemented)

### Not Implemented (Future)

- ❌ File compaction
- ❌ Snapshot expiration
- ❌ Column mapping

## Database Schema

Pepper uses these core tables (SQLite):

```sql
CREATE TABLE pepper_snapshot (
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    next_catalog_id BIGINT NOT NULL,
    next_file_id BIGINT NOT NULL
);

CREATE TABLE pepper_table (
    table_id BIGINT PRIMARY KEY,
    table_uuid TEXT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    table_name TEXT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    schema_json TEXT NOT NULL,
    primary_key_json TEXT
);

CREATE TABLE pepper_data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    file_order BIGINT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    file_format TEXT NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    row_id_start BIGINT NOT NULL
);

CREATE TABLE pepper_delete_file (
    delete_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    data_file_id BIGINT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    format TEXT NOT NULL,
    delete_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL
);
```

## Usage Example

```rust
use pepper::{
    PepperCatalog, PepperTableProvider, CreateTableOptions,
};

// Create catalog
let catalog = Arc::new(PepperCatalog::new("sqlite:///data/catalog.db"));
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
    base_path: "/data/events".to_string(),
};

let table = PepperTableProvider::create_table(catalog.clone(), options).await?;

// Insert data
let batch = create_record_batch()?;
table.insert(Box::pin(stream::once(async { Ok(batch) }))).await?;

// Query (deletion vectors applied automatically)
let ctx = SessionContext::new();
ctx.register_table("events", Arc::new(table))?;
let df = ctx.sql("SELECT * FROM events WHERE event_id > 1000").await?;
df.show().await?;
```

## Implementation Status

### Current Status (Phase 1)

- ✅ Trait abstractions defined
- ✅ Data structures implemented
- ✅ Deletion vector logic complete
- ⚠️ SQLite catalog (skeleton only)
- ⚠️ Table provider (skeleton only)

### Next Steps (Phase 2)

1. Complete SQLite catalog implementation with rusqlite
2. Implement file I/O for Vortex data files
3. Implement deletion vector Parquet I/O
4. Complete table provider scan with deletion filtering
5. Implement insert/delete/update operations

### Future Enhancements (Phase 3)

- Compaction and maintenance operations
- Advanced statistics
- Partitioning support
- Performance optimizations
- Additional catalog backends (PostgreSQL, DuckDB)

## Benefits

1. **Efficient Deletes**: No data file rewrites, just append deletion vectors
2. **ACID Transactions**: SQLite provides transaction guarantees
3. **Time Travel**: Query any snapshot in history
4. **Performance**: Vortex's compression and columnar format
5. **Simplicity**: Single SQLite file for metadata
6. **Flexibility**: Trait-based design allows multiple backends

## References

- [DuckLake Specification v0.3](https://ducklake.select/docs/stable/specification/introduction.html)
- [DuckLake Tables](https://ducklake.select/docs/stable/specification/tables/overview.html)
- [Vortex Format](https://github.com/spiraldb/vortex)
