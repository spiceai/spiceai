# Vortex at Spice AI: Columnar Compression for High-Performance Data Acceleration

> How we leverage Vortex, a Linux Foundation columnar format, to power Cayenne's data acceleration

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- [Rust at Spice AI](rust-at-spiceai.md) — Our systems programming foundation
- [Apache Arrow at Spice AI](apache-arrow-at-spiceai.md) — Arrow as our core data format
- [Apache DataFusion at Spice AI](apache-datafusion-at-spiceai.md) — Our SQL query engine foundation
- [DuckDB at Spice AI](duckdb-at-spiceai.md) — Embedded analytics and acceleration
- [Apache Iceberg at Spice AI](apache-iceberg-at-spiceai.md) — Open table format integration
- **Vortex at Spice AI** *(You are here)*
- [Apache Ballista at Spice AI](apache-ballista-at-spiceai.md) — Distributed query execution

---

## Table of Contents

- [What is Vortex?](#what-is-vortex)
- [Why Vortex for Spice?](#why-vortex-for-spice)
- [The Cayenne Architecture](#the-cayenne-architecture)
- [Vortex Compression Strategies](#vortex-compression-strategies)
- [DataFusion Integration](#datafusion-integration)
- [Virtual Files and ListingTables](#virtual-files-and-listingtables)
- [Caching Architecture](#caching-architecture)
- [Deletion Vectors](#deletion-vectors)
- [Type Handling and Conversion](#type-handling-and-conversion)
- [Our Vortex Fork](#our-vortex-fork)
- [Lessons Learned](#lessons-learned)

---

Vortex is a columnar file format from the Linux Foundation designed for high-performance analytics. In Spice, we use Vortex as the data storage layer for **Cayenne**, our lakehouse-style data accelerator that delivers better-than-DuckDB performance without single-file scaling limitations.

## What is Vortex?

[Vortex](https://github.com/vortex-data/vortex) is a modern columnar format that combines:

1. **Encoding-Efficient Compression** — Uses specialized encodings per data type (dictionary, run-length, delta, etc.)
2. **Zero-Copy Arrow Access** — Data decompresses directly to Arrow arrays
3. **Chunked Storage** — Files split into segments for parallelism and granular statistics
4. **SIMD Acceleration** — Vectorized decode paths for modern CPUs
5. **Native Rust Implementation** — Memory-safe, high-performance codebase

Vortex sits between raw Parquet and fully indexed databases—more compressed than Arrow IPC, faster to query than Parquet, simpler than Iceberg.

## Why Vortex for Spice?

When designing Cayenne, we evaluated several storage options:

| Option            | Problem                                                       |
| ----------------- | ------------------------------------------------------------- |
| **DuckDB files**  | Single-file scaling limits, memory overhead, write contention |
| **Parquet**       | Slow decompression for hot data, no native update support     |
| **Arrow IPC**     | Large uncompressed files, expensive to store                  |
| **Iceberg/Delta** | Complex metadata management, catalog server requirements      |

Vortex solved our key challenges:

| Requirement                    | Vortex Solution                                 |
| ------------------------------ | ----------------------------------------------- |
| Better than DuckDB performance | Encoding-efficient compression + zero-copy      |
| No single-file limits          | Multi-file architecture with metadata in SQLite |
| Arrow-native                   | Direct decompression to Arrow arrays            |
| Simple operations              | Just files + SQL metadata, no catalog servers   |
| High concurrency               | Stage files, then single SQL transaction        |

From our v1.9 release notes:

> Cayenne delivers query and ingestion performance better than DuckDB's file-based acceleration without DuckDB's memory overhead and the scaling challenges of single DuckDB files.

## The Cayenne Architecture

Cayenne combines SQLite for metadata with Vortex for data:

```text
┌─────────────────────────────────────────────────────────────┐
│                      Cayenne Table                           │
├──────────────────────────┬──────────────────────────────────┤
│   Metastore (SQLite)     │   Vortex Data Lake               │
│                          │                                   │
│   • Table schemas        │   ├─ snapshot_001/                │
│   • Snapshot tracking    │   │   ├─ file_000001/             │
│   │   • Partition metadata   │   │   │   ├─ chunk_001.vortex │
│   • Data file references │   │   │   └─ chunk_002.vortex     │
│   • Deletion vectors     │   │   └─ file_000002/             │
│   • Statistics           │   │       └─ chunk_001.vortex     │
│                          │   └─ deletions/                   │
│                          │       └─ del_001.arrow            │
└──────────────────────────┴──────────────────────────────────┘
```

### Configuration

```yaml
# spicepod.yaml
datasets:
  - from: s3://my-bucket/data.parquet
    name: accelerated_data
    acceleration:
      enabled: true
      engine: cayenne
      mode: file
      refresh_mode: append
      params:
        cayenne_compression_strategy: btrblocks  # or zstd
        cayenne_footer_cache_mb: '128'
        cayenne_segment_cache_mb: '256'
        cayenne_target_file_size_mb: '128'
```

### Why SQLite + Vortex?

This architecture provides several advantages:

1. **Single SQL query for metadata** — No S3 round trips, no JSON parsing
2. **Atomic transactions** — Stage Vortex files, commit with one SQL transaction
3. **Millions of snapshots** — Just rows in SQLite, not files on disk
4. **No catalog servers** — Everything runs embedded

```rust
/// Cayenne: A minimal DuckLake-inspired lakehouse format using SQLite for metadata
/// and Vortex files as the data lake.
///
/// This module provides a lakehouse format that combines:
/// - SQLite for transactional metadata management (schemas, tables, files)
/// - Vortex files for efficient columnar data storage
```

## Vortex Compression Strategies

Vortex supports multiple compression strategies optimized for different workloads:

### Btrblocks (Default)

The default strategy uses specialized columnar encodings:

| Encoding             | Best For                             |
| -------------------- | ------------------------------------ |
| **Dictionary**       | Low-cardinality strings              |
| **Run-Length (RLE)** | Repeated consecutive values          |
| **Delta**            | Monotonic integers (timestamps, IDs) |
| **Bit-Packing**      | Small integer ranges                 |
| **FSST**             | String compression                   |

Btrblocks automatically selects the best encoding per column based on data statistics.

### Zstd (Compact)

For cold data or maximum compression:

```rust
pub enum CompressionStrategy {
    /// Uses the default Vortex Btrblocks compression.
    #[default]
    Btrblocks,
    /// Uses the Vortex CompactCompressor with Zstd compression.
    Zstd,
}
```

### Implementation

```rust
fn create_listing_options(vortex_config: &VortexConfig) -> ListingOptions {
    let vortex_session = VortexSession::default();

    // Apply compression strategy
    let vortex_session = if matches!(
        vortex_config.compression_strategy,
        CompressionStrategy::Zstd
    ) {
        vortex_session
            .set(WriteStrategyBuilder::new()
                .with_compressor(CompactCompressor::default()))
    } else {
        vortex_session
    };

    let vortex_opts = vortex_datafusion::VortexOptions {
        footer_cache_size_mb: vortex_config.footer_cache_mb,
        segment_cache_size_mb: vortex_config.segment_cache_mb,
    };

    let format = Arc::new(VortexFormat::new_with_options(vortex_session, vortex_opts));
    ListingOptions::new(format).with_session_config_options(&SessionConfig::default())
}
```

### Configuration Options

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VortexConfig {
    /// Footer cache size in MB (default: 128)
    pub footer_cache_mb: usize,
    /// Segment cache size in MB (default: 256)
    pub segment_cache_mb: usize,
    /// Target size for individual Vortex files in MB (default: 128)
    /// Smaller files = better parallelism and predicate pushdown
    pub target_vortex_file_size_mb: usize,
    /// Columns to sort data by on refresh operations
    pub sort_columns: Vec<String>,
    /// Compression strategy (btrblocks or zstd)
    pub compression_strategy: CompressionStrategy,
}
```

## DataFusion Integration

Vortex integrates with DataFusion through the `vortex-datafusion` crate, providing a custom file format that plugs into DataFusion's `ListingTable`:

### VortexFormat

```rust
use vortex_datafusion::VortexFormat;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingOptions};

// Create Vortex format with caching
let vortex_opts = vortex_datafusion::VortexOptions {
    footer_cache_size_mb: 128,
    segment_cache_size_mb: 256,
};

let format = Arc::new(VortexFormat::new_with_options(vortex_session, vortex_opts));
let listing_options = ListingOptions::new(format);

// Create listing table
let config = ListingTableConfig::new(table_url)
    .with_listing_options(listing_options)
    .with_schema(schema);

let listing_table = ListingTable::try_new(config)?;
```

### Query Execution Flow

```text
┌─────────────────┐
│  SQL Query      │
└────────┬────────┘
         ▼
┌─────────────────┐
│  DataFusion     │  Logical planning, optimization
│  Optimizer      │
└────────┬────────┘
         ▼
┌─────────────────┐
│  ListingTable   │  File discovery, partition pruning
│  (VortexFormat) │
└────────┬────────┘
         ▼
┌─────────────────┐
│  VortexExec     │  Parallel chunk reads
│                 │
└────────┬────────┘
         ▼
┌─────────────────┐
│  Arrow          │  Zero-copy decompression
│  RecordBatches  │
└─────────────────┘
```

### Predicate Pushdown

Vortex files contain per-segment statistics enabling predicate pushdown:

```sql
-- Only reads segments where created_at might match
SELECT * FROM accelerated_data
WHERE created_at > '2024-01-01'
```

The `VortexFormat` pushes filters to segment level, skipping segments where statistics prove no matches.

## Virtual Files and ListingTables

A key Cayenne design principle: **files are virtual**. Each "file" in the catalog is actually a Vortex `ListingTable` at a unique directory:

```rust
/// Virtual Files Concept
///
/// An initial design principle in Cayenne is that "files" are virtual files - they are not
/// single physical files, but rather Vortex ListingTables at unique directories. Each
/// DataFile entry in the catalog represents:
///
/// - A unique directory path (e.g., table_dir/file_000001/)
/// - A Vortex ListingTable that manages Vortex files within that directory
/// - Metadata (row count, size) cached from the ListingTable's statistics
```

### Operations on Virtual Files

| Operation     | Implementation                                           |
| ------------- | -------------------------------------------------------- |
| **Reading**   | Query the `ListingTable` at that directory               |
| **Appending** | Write via the `ListingTable` (creates new Vortex chunks) |
| **Deleting**  | Delete the directory                                     |
| **Stats**     | Query `ListingTable` statistics                          |

### DataFile Metadata

```rust
pub struct DataFile {
    /// Unique identifier for this data file
    pub data_file_id: i64,
    /// Table this file belongs to
    pub table_id: i64,
    /// Partition this file belongs to (None for non-partitioned tables)
    pub partition_id: Option<i64>,
    /// Path to the directory containing the ListingTable's Vortex files
    /// This is the "virtual file" - a directory managed by a Vortex ListingTable
    pub path: String,
    /// File format (always "vortex" for Cayenne)
    pub file_format: String,
    /// Number of records in this virtual file (cached from ListingTable stats)
    pub record_count: i64,
    /// Total size of all Vortex files in the ListingTable directory
    pub file_size_bytes: i64,
    /// Sequence number for ordering operations
    pub sequence_number: i64,
}
```

### File Sizing

Vortex automatically splits writes into multiple chunk files:

```rust
/// Target size for individual Vortex files in MB. When writes exceed this size,
/// a new Vortex file will be created in the same listing directory. This allows
/// for better parallelism and more granular statistics for query optimization.
/// Defaults to 128 MB.
pub target_vortex_file_size_mb: usize,
```

## Caching Architecture

Vortex uses a two-tier caching system for read performance:

### Footer Cache

Vortex file footers contain schema and segment metadata. Caching footers avoids repeated disk reads:

```rust
pub struct VortexOptions {
    /// Footer cache size in MB (default: 128)
    pub footer_cache_size_mb: usize,
    // ...
}
```

### Segment Cache

Hot segments stay in memory for repeated access:

```rust
pub struct VortexOptions {
    // ...
    /// Segment cache size in MB (default: 256)
    pub segment_cache_size_mb: usize,
}
```

### Default Configuration

```rust
impl Default for VortexConfig {
    fn default() -> Self {
        Self {
            // Larger caches improve read performance
            footer_cache_mb: 128,
            segment_cache_mb: 256,
            // Smaller files = better parallelism and predicate pushdown
            target_vortex_file_size_mb: 128,
            sort_columns: vec![],
            compression_strategy: CompressionStrategy::default(),
        }
    }
}
```

## Deletion Vectors

Cayenne supports ACID-compliant deletes via deletion vectors—separate files that track deleted rows:

### Three Deletion Strategies

Based on table configuration, Cayenne selects the optimal deletion strategy:

| Strategy              | Use Case                   | Implementation                              |
| --------------------- | -------------------------- | ------------------------------------------- |
| **Position-Based**    | Tables without primary key | `RoaringBitmap` of row positions            |
| **Int64Pk**           | Single Int64 primary key   | `HashSet<i64>` direct lookup                |
| **RowConverterBased** | Composite/non-integer PK   | `HashSet<Box<[u8]>>` via Arrow RowConverter |

```rust
pub enum PkDeletionStrategy {
    /// No primary key - use position-based deletion with RoaringBitmap.
    /// Requires CoalescePartitionsExec to ensure consistent ordering.
    PositionBased,
    /// Single-column Int64 primary key - use direct HashSet<i64> lookup.
    /// Most efficient: no serialization, 8 bytes per key, parallel reads.
    Int64Pk,
    /// Composite or non-integer primary key - use RowConverter + HashSet<Box<[u8]>>.
    /// Handles all PK types but has serialization overhead.
    RowConverterBased,
}
```

### Deletion Identifiers

```rust
pub enum DeletionIdentifier {
    /// Position-based row IDs (for tables without primary key)
    PositionBased(Vec<i64>),
    /// Primary key-based row keys (for tables with primary key)
    KeyBased(Vec<Box<[u8]>>),
}
```

### Memory Efficiency

Position-based deletion uses `RoaringBitmap` for 50-90% memory savings:

```rust
/// Cached deletion vectors (deleted row IDs) for position-based deletion.
/// RoaringBitmap provides 50-90% memory savings vs HashSet for sparse deletions
/// and SIMD-optimized contains operations.
cached_deleted_row_ids: Arc<RwLock<Arc<RoaringBitmap>>>,
```

### Sequence Numbers (Iceberg-Style)

Deletes use sequence numbers for upsert semantics:

```rust
/// Current sequence number for ordering operations (Iceberg-style).
///
/// Monotonically increasing counter used to order deletes and inserts.
/// When data is inserted, it gets the current sequence number.
/// When a delete is written, it also gets the current sequence number.
/// A delete only applies to data with data_sequence < delete_sequence.
///
/// This enables upsert semantics: if a PK is deleted and then re-inserted,
/// the new insert has a higher sequence than the delete, so the delete
/// doesn't apply to the new data.
pub current_sequence_number: i64,
```

### Deletion File Format

Deletion vectors are stored as Arrow IPC files:

```rust
const DELETION_DIR_NAME: &str = "deletions";
const DELETION_FILE_EXTENSION: &str = "arrow";
const DELETION_FILE_FORMAT: &str = "arrow_ipc";
```

## Type Handling and Conversion

Not all Arrow types are supported by Vortex. We handle conversions:

### Unsupported Types

```rust
fn is_vortex_supported_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Map(_, _)
            | DataType::FixedSizeBinary(_)
    )
}
```

### Automatic Conversions

| Source Type       | Vortex Type   | Reason                         |
| ----------------- | ------------- | ------------------------------ |
| Float16           | Float32       | Vortex doesn't support Float16 |
| Timestamp(non-µs) | Timestamp(µs) | Standardize precision          |

### Schema Validation

On table creation, we validate all columns are Vortex-compatible:

```rust
fn validate_schema_for_vortex(schema: &SchemaRef) -> CatalogResult<()> {
    for field in schema.fields() {
        if !is_vortex_supported_type(field.data_type()) {
            return Err(CatalogError::UnsupportedType {
                column: field.name().clone(),
                data_type: field.data_type().clone(),
            });
        }
    }
    Ok(())
}
```

## Our Vortex Fork

We maintain a fork of Vortex:

```toml
vortex = { git = "https://github.com/spiceai/vortex", rev = "b701e1ef..." }
vortex-datafusion = { git = "https://github.com/spiceai/vortex", rev = "b701e1ef..." }
vortex-session = { git = "https://github.com/spiceai/vortex", rev = "b701e1ef..." }
```

### Why Fork?

1. **Arrow version alignment** — Keep Arrow versions synchronized with Spice
2. **DataFusion compatibility** — Match our DataFusion version
3. **Bug fixes** — Ship fixes before upstream merges
4. **Spice-specific features** — Enhancements for our use cases

### Fork Philosophy

Same principles as our DuckDB fork:

- **Minimal divergence** — Stay close to upstream
- **Upstream-first** — Contribute improvements back
- **Tagged releases** — Semantic versioning for stability
- **Regular rebasing** — Periodically rebase onto latest upstream

## Lessons Learned

After building Cayenne with Vortex, here are our key takeaways:

### 1. Encoding-Efficient Compression Beats Generic Compression

Vortex's per-column encoding selection consistently outperforms generic compression like Zstd on structured data. Dictionary encoding for strings, delta for timestamps, and bit-packing for small integers compound to significant savings.

### 2. Virtual Files Simplify Lifecycle Management

Treating files as directories managed by ListingTables simplifies append, compaction, and cleanup. No need to rewrite entire files for updates—just add new chunks.

### 3. SQLite is a Fantastic Embedded Metastore

SQLite handles concurrent reads beautifully, transactions are rock-solid, and the operational model (just a file) is trivially simple. No Zookeeper, no catalog servers.

### 4. Cache Sizing Matters

Footer and segment caches have dramatic impact on read performance. Our defaults (128MB footer, 256MB segment) work well for typical workloads, but expose tuning parameters for users with specific needs.

### 5. Deletion Vectors Enable ACID Without Complexity

Rather than rewriting data files on delete, storing deletion vectors as separate Arrow files provides ACID semantics with minimal write amplification. The sequence number approach enables efficient upserts.

### 6. RoaringBitmap Saves Memory

For position-based deletion, RoaringBitmap provides 50-90% memory savings over HashSet while offering SIMD-accelerated contains operations. Well worth the dependency.

### 7. Multi-File Beats Single-File

DuckDB's single-file model creates contention under concurrent writes and limits scalability. Vortex's multi-file approach with SQLite metadata coordination eliminates these bottlenecks.

### 8. Zero-Copy is Worth the Constraints

Accepting Vortex's type constraints (no Interval, no Duration, etc.) is worth it for true zero-copy Arrow access. The performance difference is substantial.

---

## Conclusion

Vortex is the storage backbone of Cayenne, our high-performance data accelerator. Its encoding-efficient compression, zero-copy Arrow access, and chunked storage model align perfectly with Spice's performance principles.

The combination of Vortex for data and SQLite for metadata provides:

- **Better than DuckDB performance** — Encoding-efficient compression without memory overhead
- **No single-file limits** — Multi-file architecture scales to any dataset size
- **ACID transactions** — SQLite provides rock-solid transaction semantics
- **Simple operations** — Just files and SQL, no complex catalog infrastructure

For data acceleration that needs to be fast, reliable, and scalable, Vortex is our answer.

---

## References

- [Vortex Project](https://github.com/vortex-data/vortex) — Linux Foundation columnar format
- [Cayenne Documentation](https://spiceai.org/docs/components/data-accelerators/cayenne)
- [DuckLake Announcement](https://duckdb.org/2025/05/27/ducklake) — Inspiration for SQLite + files architecture
- [Spice v1.9 Release Notes](../../release_notes/v1.9/v1.9.0-rc.4.md) — Cayenne introduction
- [RoaringBitmap](https://roaringbitmap.org/) — Compressed bitmap for deletion vectors
- [Arrow RowConverter](https://docs.rs/arrow-row/latest/arrow_row/) — Primary key serialization

