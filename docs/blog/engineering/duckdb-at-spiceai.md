# DuckDB at Spice AI: Embedded Analytics for Data Acceleration

> How we integrate DuckDB as both a data connector and acceleration engine

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- [Rust at Spice AI](rust-at-spiceai.md) — Our systems programming foundation
- [Apache Arrow at Spice AI](apache-arrow-at-spiceai.md) — Arrow as our core data format
- [Apache DataFusion at Spice AI](apache-datafusion-at-spiceai.md) — Our SQL query engine foundation
- **DuckDB at Spice AI** *(You are here)*
- [Apache Iceberg at Spice AI](apache-iceberg-at-spiceai.md) — Open table format integration
- [Vortex at Spice AI](vortex-at-spiceai.md) — Columnar compression for Cayenne
- [Apache Ballista at Spice AI](apache-ballista-at-spiceai.md) — Distributed query execution

---

## Table of Contents

- [What is DuckDB?](#what-is-duckdb)
- [Dual-Mode Architecture](#dual-mode-architecture)
- [DuckDB as Data Connector](#duckdb-as-data-connector)
- [DuckDB as Acceleration Engine](#duckdb-as-acceleration-engine)
- [Connection Pooling](#connection-pooling)
- [SQL Dialect Translation](#sql-dialect-translation)
- [Aggregate Pushdown Optimization](#aggregate-pushdown-optimization)
- [Handling Blocking Operations](#handling-blocking-operations)
- [Compound Column Indexes](#compound-column-indexes)
- [Index Tuning](#index-tuning)
- [Partitioned DuckDB](#partitioned-duckdb)
- [The PolyTableProvider Abstraction](#the-polytableprovider-abstraction)
- [Data Retention](#data-retention)
- [Our Fork and Contributions](#our-fork-and-contributions)
- [Lessons Learned](#lessons-learned)

---

DuckDB is an embedded analytical database designed for fast analytical queries. It's like SQLite, but columnar—optimized for OLAP rather than OLTP workloads. In Spice, DuckDB plays two distinct roles: a data connector for querying external DuckDB files, and an acceleration engine for caching data locally.

## What is DuckDB?

DuckDB provides:

1. **Columnar Storage** — Data is stored by column, enabling efficient compression and vectorized processing
2. **Embedded Deployment** — No separate server; the database runs in-process
3. **Native Arrow Support** — Zero-copy data exchange with Arrow arrays
4. **SQL Compatibility** — Familiar SQL syntax with analytical extensions
5. **Portable Files** — Database files work across platforms

These properties make DuckDB ideal for local data acceleration—fast, embedded, and Arrow-native.

## Dual-Mode Architecture

In Spice, DuckDB serves two distinct purposes:

| Mode                    | Purpose                                           | Use Case                                   |
| ----------------------- | ------------------------------------------------- | ------------------------------------------ |
| **Data Connector**      | Connect to external DuckDB files as a data source | Query existing DuckDB databases            |
| **Acceleration Engine** | Cache data locally for fast queries               | Accelerate Snowflake, S3, PostgreSQL, etc. |

```text
┌─────────────────────────────────────────────────────────────┐
│                      User Queries                           │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│   DuckDB Data Connector  │    │   DuckDB Acceleration Engine │
│   (Read external .db)    │    │   (Cache from any source)    │
└──────────────────────────┘    └──────────────────────────────┘
              │                               │
              ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│   external-data.duckdb   │    │   .spice/data/cache.duckdb   │
└──────────────────────────┘    └──────────────────────────────┘
```

## DuckDB as Data Connector

The DuckDB connector opens existing DuckDB files as read-only data sources:

```yaml
# spicepod.yaml
datasets:
  - from: duckdb:customer
    name: customer
    params:
      duckdb_open: ./data/tpch.db
```

### Implementation

```rust
pub fn create_file(
    path: &str,
    params: &ConnectorParams,
) -> AnyErrorResult<DuckDBTableFactory> {
    let pool = Arc::new(
        DuckDbConnectionPool::new_file(path, &AccessMode::ReadOnly)
            .with_unsupported_type_action(
                params.unsupported_type_action.unwrap_or(UnsupportedTypeAction::Error)
            )
    );

    Ok(DuckDBTableFactory::new(pool).with_dialect(new_duckdb_dialect()))
}
```

Key aspects:

- **Read-only access** — External files are never modified
- **SQL federation** — Queries push down to DuckDB
- **Dialect translation** — DataFusion expressions convert to DuckDB SQL

### Query Pushdown

Filters, projections, and aggregations push down to DuckDB:

```sql
-- User query
SELECT name, SUM(amount) FROM duckdb:sales WHERE region = 'NA' GROUP BY name

-- Pushed to DuckDB (via federation)
SELECT name, SUM(amount) FROM sales WHERE region = 'NA' GROUP BY name
```

## DuckDB as Acceleration Engine

More commonly, DuckDB accelerates data from other sources:

```yaml
# spicepod.yaml
datasets:
  - from: postgres:sales
    name: accelerated_sales
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      params:
        duckdb_file: ./cache/sales.duckdb
```

### Acceleration Modes

| Mode     | Storage         | Use Case                         |
| -------- | --------------- | -------------------------------- |
| `memory` | In-memory only  | Fast, ephemeral cache            |
| `file`   | Persistent file | Durable cache, survives restarts |

### Accelerator Implementation

```rust
impl DataAccelerator for DuckDBAccelerator {
    fn name(&self) -> &'static str { "duckdb" }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["db", "ddb", "duckdb"]
    }

    async fn init(&self, source: &dyn AccelerationSource) -> Result<()> {
        let mode = source.acceleration().mode;
        let pool = match mode {
            AccelerationMode::Memory => DuckDbConnectionPool::new_memory()?,
            AccelerationMode::File => {
                let path = source.acceleration().params.get("duckdb_file")?;
                DuckDbConnectionPool::new_file(path, &AccessMode::ReadWrite)?
            }
        };

        // Create table matching source schema
        self.create_table(&pool, source.schema()).await?;

        Ok(())
    }
}
```

### Refresh Strategies

Data flows from source to accelerator:

```rust
pub enum RefreshMode {
    Full,    // Replace all data
    Append,  // Add new rows only
    Changes, // Apply CDC deltas (insert/update/delete)
}
```

For large datasets, `Append` or `Changes` modes avoid full table scans.

## Connection Pooling

DuckDB connections are shared across datasets:

```rust
fn get_pool_max_size(
    num_accelerating_datasets: u32,
    acceleration: &Acceleration,
) -> u32 {
    let explicit_size = acceleration.params
        .get("connection_pool_size")
        .and_then(|s| s.parse::<u32>().ok());

    explicit_size.unwrap_or_else(|| {
        max(DEFAULT_MIN_IDLE_CONNECTIONS, num_accelerating_datasets)
    })
}
```

### Shared Pools for Same File

Multiple datasets accelerated to the same DuckDB file share a connection pool:

```yaml
datasets:
  - from: postgres:customers
    name: customers
    acceleration:
      engine: duckdb
      params:
        duckdb_file: ./cache/shared.duckdb  # Shared pool

  - from: postgres:orders
    name: orders
    acceleration:
      engine: duckdb
      params:
        duckdb_file: ./cache/shared.duckdb  # Same pool
```

## SQL Dialect Translation

DataFusion and DuckDB have different SQL dialects. We translate:

```rust
pub fn new_duckdb_dialect() -> Arc<dyn Dialect> {
    DuckDBDialect::new().with_custom_scalar_overrides(vec![
        // Function name mappings
        (COSINE_DISTANCE_UDF_NAME, Box::new(cosine_distance_to_sql)),
        ("rand", Box::new(rand_to_random)),

        // Regex function mappings
        (REGEXP_LIKE_NAME, Box::new(DuckDBRegexpFunction::Like)),
        (REGEXP_MATCH_NAME, Box::new(DuckDBRegexpFunction::Match)),
        (REGEXP_REPLACE_NAME, Box::new(DuckDBRegexpFunction::Replace)),
    ])
}
```

### Example: Cosine Distance

DataFusion's `cosine_distance(array1, array2)` becomes DuckDB's `array_cosine_distance()`:

```rust
pub fn cosine_distance_to_sql(
    args: &[Expr],
    unparser: &dyn Unparser,
) -> Result<Option<ast::Expr>> {
    // Convert: cosine_distance(make_array(1.0, 2.0), col)
    // To:      array_cosine_distance([1.0, 2.0]::FLOAT[2], col)

    let left = unparse_expr(&args[0], unparser)?;
    let right = unparse_expr(&args[1], unparser)?;

    Ok(Some(ast::Expr::Function(ast::Function {
        name: "array_cosine_distance".into(),
        args: vec![add_type_cast(left), right],
        ..Default::default()
    })))
}
```

### Example: Random Function

```rust
pub fn rand_to_random(
    _args: &[Expr],
    _unparser: &dyn Unparser,
) -> Result<Option<ast::Expr>> {
    // DataFusion: rand()
    // DuckDB: random()
    Ok(Some(ast::Expr::Function(ast::Function {
        name: "random".into(),
        args: vec![],
        ..Default::default()
    })))
}
```

## Aggregate Pushdown Optimization

For DuckDB-accelerated tables, we optionally push aggregations to DuckDB:

### Supported Functions

```rust
static SUPPORTED_AGG_FUNCTIONS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        // Basic
        "avg", "count", "max", "min", "sum",
        // Bitwise
        "bit_and", "bit_or", "bit_xor",
        // Boolean
        "bool_and", "bool_or",
        // Statistical
        "corr", "covar_pop", "covar_samp",
        "stddev_pop", "stddev_samp",
        "var_pop", "var_samp",
        // Approximate
        "approx_percentile_cont",
    ])
});
```

### Enabling Pushdown

Via schema metadata on the accelerated table:

```yaml
acceleration:
  engine: duckdb
  params:
    optimizer_duckdb_aggregate_pushdown: enabled
```

### How It Works

The optimizer rule detects aggregate queries over DuckDB tables:

```rust
impl OptimizerRule for DuckDBAggPushdown {
    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Aggregate(agg) = &plan {
            // Check if source is DuckDB-accelerated
            if is_duckdb_accelerated(&agg.input) {
                // Check all aggregate functions are supported
                if all_supported(&agg.aggr_expr) {
                    // Rewrite to federated scan with aggregate
                    return Ok(Transformed::yes(
                        create_federated_aggregate(&agg)
                    ));
                }
            }
        }
        Ok(Transformed::no(plan))
    }
}
```

Result:

```sql
-- Original: DataFusion computes aggregate
SELECT region, AVG(sales) FROM accelerated_table GROUP BY region
-- Reads all rows, aggregates in DataFusion

-- With pushdown: DuckDB computes aggregate
SELECT region, AVG(sales) FROM accelerated_table GROUP BY region
-- Only aggregated results returned, DuckDB does the work
```

## Handling Blocking Operations

DuckDB's Rust bindings are synchronous. We wrap all calls in `spawn_blocking`:

```rust
impl DeletionSink for DuckDBDeletionSink {
    async fn delete_from(&self) -> Result<u64> {
        let pool = Arc::clone(&self.pool);
        let table_name = self.table_name.clone();
        let filters = self.filters.clone();

        tokio::task::spawn_blocking(move || {
            let mut db_conn = pool.connect_sync()?;
            let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

            let tx = duckdb_conn.conn.transaction()?;

            // Convert DataFusion filters to DuckDB SQL
            let sql_filters = filters.iter()
                .map(|f| expr::to_sql_with_engine(f, Some(Engine::DuckDB)))
                .collect::<Result<Vec<_>>>()?;

            let where_clause = sql_filters.join(" AND ");
            let sql = format!("DELETE FROM {} WHERE {}", table_name, where_clause);

            let deleted = tx.execute(&sql, [])?;
            tx.commit()?;

            Ok(deleted)
        }).await?
    }
}
```

This pattern ensures DuckDB operations never block the async runtime.

## Compound Column Indexes

A key enhancement in our DuckDB integration is support for **compound column indexes**—indexes spanning multiple columns for optimized multi-column lookups.

### Why Compound Indexes?

Single-column indexes work well for simple predicates, but real-world queries often filter on multiple columns:

```sql
-- Common pattern: filter by multiple columns
SELECT * FROM blockchain_logs
WHERE log_index = 42 AND transaction_hash = '0xabc...'
```

Without a compound index, DuckDB must scan more data or use inefficient index intersection strategies.

### Syntax

In your spicepod, compound indexes use parenthesized column lists:

```yaml
# spicepod.yaml
datasets:
  - from: eth:logs
    name: ethereum_logs
    acceleration:
      enabled: true
      engine: duckdb
      indexes:
        (log_index, transaction_hash): enabled
        block_number: enabled
        (block_number, log_index): unique
```

The `(col1, col2)` syntax creates a multi-column index in the specified order.

### Parsing ColumnReference

Our `ColumnReference` type parses both single and compound column specifications:

```rust
// Single column
let single = ColumnReference::try_from("block_number")?;
// single.iter() yields ["block_number"]

// Compound columns
let compound = ColumnReference::try_from("(log_index, transaction_hash)")?;
// compound.iter() yields ["log_index", "transaction_hash"]

// Invalid syntax detected
let err = ColumnReference::try_from("(foo,bar");
// Error: "The column reference \"(foo,bar\" is missing a closing parenthesis."
```

The index configuration flows through our acceleration layer:

```rust
pub struct Acceleration {
    // ...
    pub indexes: HashMap<ColumnReference, IndexType>,
    pub primary_key: Option<ColumnReference>,
    pub on_conflict: HashMap<ColumnReference, OnConflictBehavior>,
}
```

### Index Types

| Type      | Description             | Use Case                    |
| --------- | ----------------------- | --------------------------- |
| `enabled` | Standard index          | General query acceleration  |
| `unique`  | Unique constraint index | Primary keys, deduplication |

### Generated SQL

For compound indexes, we generate appropriate DuckDB DDL:

```sql
-- For: (log_index, transaction_hash): enabled
CREATE INDEX idx_logs_compound
ON ethereum_logs (log_index, transaction_hash);

-- For: (block_number, log_index): unique
CREATE UNIQUE INDEX idx_logs_block_unique
ON ethereum_logs (block_number, log_index);
```

### Query Optimization

DuckDB's query optimizer uses compound indexes when predicates match the index prefix:

```sql
-- ✅ Uses compound index (log_index, transaction_hash)
SELECT * FROM logs WHERE log_index = 42 AND transaction_hash = '0xabc...'

-- ✅ Uses compound index (prefix match)
SELECT * FROM logs WHERE log_index = 42

-- ❌ Cannot use index (transaction_hash isn't the prefix)
SELECT * FROM logs WHERE transaction_hash = '0xabc...'
```

This follows the "leftmost prefix" rule common in B-tree indexes.

## Index Tuning

DuckDB supports ART (Adaptive Radix Tree) indexes. We expose tuning parameters:

```yaml
acceleration:
  engine: duckdb
  params:
    duckdb_index_scan_percentage: '0.10'  # Use index if <10% of table
    duckdb_index_scan_max_count: '10000'  # Use index if <10K rows
```

### Configuration

```rust
pub(crate) struct IndexScanPercentage;  // Default: 0.001 (0.1%)
pub(crate) struct IndexScanMaxCount;     // Default: 2048 rows
```

These settings control when DuckDB uses an index scan vs. a full table scan.

## Partitioned DuckDB

For large datasets, a single DuckDB file becomes a bottleneck—large files are slow to load, hard to manage, and impossible to parallelize. **Partitioned DuckDB** solves this by splitting data across multiple partitions.

### Why Partition?

| Problem                     | Solution                                |
| --------------------------- | --------------------------------------- |
| Single file grows unbounded | Data distributed across partition files |
| Full table scans are slow   | Partition pruning skips irrelevant data |
| One writer bottleneck       | Parallel writes to different partitions |
| Backup/restore complexity   | Manage partitions independently         |

### Two Partition Modes

We support two partitioning strategies:

| Mode     | Storage                            | Use Case                                        |
| -------- | ---------------------------------- | ----------------------------------------------- |
| `files`  | Separate `.db` file per partition  | Maximum isolation, easy cleanup                 |
| `tables` | Single `.db` file, multiple tables | Shared connection pool, simpler file management |

### Spicepod Configuration

```yaml
# spicepod.yaml
datasets:
  - from: eth:logs
    name: ethereum_logs
    acceleration:
      enabled: true
      engine: partitioned_duckdb
      partition_by: block_date
      params:
        partition_mode: files  # or "tables"
        duckdb_data_dir: ./data/partitions/
```

### Files Mode: Hive-Style Partitioning

In `files` mode, each partition lives in its own DuckDB file within a Hive-style directory:

```text
./data/partitions/ethereum_logs/
├── block_date=2024-01-01/
│   └── data.db
├── block_date=2024-01-02/
│   └── data.db
├── block_date=2024-01-03/
│   └── data.db
└── checkpoint.db  # Metadata tracking
```

**Implementation:**

```rust
pub(crate) struct PartitionedDuckDBAccelerator {
    base_accelerator: DuckDBAccelerator,
    table_provider: Mutex<Option<Arc<PartitionTableProvider>>>,
    is_initialized: AtomicBool,
    duckdb_factory: DuckDBTableProviderFactory,
}

impl PartitionCreator for DuckDBPartitionCreator {
    async fn create_partition(
        &self,
        partition_value: ScalarValue,
    ) -> Result<Partition, Error> {
        let mut cmd = self.cmd.clone();

        // Build Hive-style path: block_date=2024-01-01/data.db
        let hive_path = to_hive_partition_dir(&[
            (self.partition_by.clone(), partition_value.clone())
        ])?;

        let duckdb_path = self.partition_dir
            .join(&hive_path)
            .join("data.db");

        cmd.options.insert("open".to_string(), duckdb_path.display().to_string());

        let table_provider = create_table_provider(&self.duckdb_factory, &cmd, None).await?;

        Ok(Partition { partition_value, table_provider })
    }
}
```

On startup, the accelerator **discovers existing partitions** by scanning the directory:

```rust
async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, Error> {
    let hive_partitions = discover_hive_partitions(
        &schema,
        &self.partition_dir,
        std::slice::from_ref(&self.partition_by),
    )?;

    let mut partitions = Vec::with_capacity(hive_partitions.len());
    for (keys, path) in hive_partitions {
        let partition_value = keys.pop().context(NoPartitionValueSnafu)?;
        let table_provider = create_table_provider(...).await?;
        partitions.push(Partition { partition_value, table_provider });
    }

    Ok(partitions)
}
```

### Tables Mode: Shared File, Multiple Tables

In `tables` mode, all partitions share a single DuckDB file with separate tables per partition:

```text
./data/ethereum_logs.duckdb
├── Table: ethereum_logs_2024_01_01
├── Table: ethereum_logs_2024_01_02
└── Table: ethereum_logs_2024_01_03
```

**Implementation:**

```rust
pub(crate) struct TablesModePartitionedDuckDBAccelerator {
    base_accelerator: DuckDBAccelerator,
    duckdb_factory: DuckDBTableProviderFactory,
}

impl TablesModePartitionedDuckDBAccelerator {
    /// Returns a shared connection pool for all partition tables
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<DuckDbConnectionPool>> {
        let duckdb_path = self.file_path(source)?;

        let pool_size = source.acceleration()
            .and_then(|a| a.params.get("connection_pool_size"))
            .and_then(|s| s.parse::<u32>().ok());

        get_pool(&self.duckdb_factory, &duckdb_path, pool_size).await
    }
}
```

**Tradeoffs:**

| Aspect            | Files Mode          | Tables Mode              |
| ----------------- | ------------------- | ------------------------ |
| Connection pools  | One per partition   | Shared across partitions |
| File management   | Many small files    | Single larger file       |
| Partition cleanup | Delete directory    | DROP TABLE               |
| Concurrent writes | Maximum parallelism | Pool-limited             |
| File locking      | Isolated            | Shared lock              |

### Partition Pruning

The `PartitionTableProvider` implements filter pushdown to skip irrelevant partitions:

```rust
impl TableProvider for PartitionTableProvider {
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Extract partition filters
        let partition_filters = extract_partition_filters(
            filters,
            &self.partition_columns,
        );

        // Prune partitions that don't match
        let matching_partitions = self.partitions.iter()
            .filter(|p| matches_filters(&p.partition_value, &partition_filters))
            .collect();

        // Build union of matching partition scans
        self.build_partition_union_scan(matching_partitions, ...).await
    }
}
```

**Example query:**

```sql
-- Only scans partition for 2024-01-15
SELECT * FROM ethereum_logs
WHERE block_date = '2024-01-15' AND log_index = 42
```

## The PolyTableProvider Abstraction

Accelerated tables need to support multiple operations: reads, writes, and deletes. Rather than implementing a monolithic provider, we use the **PolyTableProvider** pattern.

### The Problem

A DuckDB-accelerated table needs to:

1. **Read** — Scan data with filter/projection pushdown
2. **Write** — Insert new data or upsert
3. **Delete** — Remove data matching predicates
4. **Federate** — Push queries to DuckDB when beneficial

Each operation has different optimal implementations. A single `TableProvider` would become unwieldy.

### The Solution: PolyTableProvider

`PolyTableProvider` wraps three specialized providers:

```rust
#[derive(Debug, Clone)]
pub struct PolyTableProvider {
    write: Arc<dyn TableProvider>,           // For INSERT operations
    delete: Arc<dyn DeletionTableProvider>,  // For DELETE operations
    fed: Arc<dyn TableProvider>,             // For federated reads
    schema_metadata: HashMap<String, String>,
}

impl PolyTableProvider {
    pub fn new(
        write: Arc<dyn TableProvider>,
        delete: Arc<dyn DeletionTableProvider>,
        fed: Arc<dyn TableProvider>,
    ) -> Self {
        PolyTableProvider { write, delete, fed, schema_metadata: HashMap::new() }
    }
}
```

### Delegation Pattern

Each operation delegates to the appropriate provider:

```rust
#[async_trait]
impl DeletionTableProvider for PolyTableProvider {
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate to delete provider
        self.delete.delete_from(state, filters).await
    }
}

#[async_trait]
impl TableProvider for PolyTableProvider {
    fn schema(&self) -> SchemaRef {
        let schema = self.write.schema().as_ref().clone();
        Arc::new(schema.with_metadata(self.schema_metadata.clone()))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate to write provider
        self.write.insert_into(state, input, insert_op).await
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Could use fed provider for federated scans
        // or write provider for direct scans
        self.fed.scan(state, projection, filters, limit).await
    }
}
```

### Federation Support

The `fed` provider implements federation, pushing queries to DuckDB:

```rust
impl FederationProvider for PolyTableProvider {
    fn name(&self) -> &'static str {
        "FederationProviderForPolyTableProvider"
    }

    fn compute_context(&self) -> Option<String> {
        // Check if underlying fed provider supports federation
        self.get_federation_provider()
            .and_then(|f| f.compute_context())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        // Delegate federation analysis to wrapped provider
        self.get_federation_provider()
            .and_then(|f| f.analyzer(plan))
    }
}
```

### Why This Pattern?

| Benefit                    | Description                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| **Separation of concerns** | Each provider handles one operation well                          |
| **Testability**            | Each provider can be tested independently                         |
| **Flexibility**            | Different accelerators can use different provider implementations |
| **Composability**          | Easy to add new behaviors (caching, metrics, etc.)                |

### Usage in Accelerated Tables

When a DuckDB-accelerated table is created, we compose the PolyTableProvider:

```rust
// Create specialized providers
let write_provider = DuckDBWriteProvider::new(pool.clone(), schema.clone());
let delete_provider = DuckDBDeleteProvider::new(pool.clone(), table_name.clone());
let fed_provider = FederatedTableProviderAdaptor::new(
    DuckDBTableSource::new(pool.clone(), table_name.clone())
);

// Compose into PolyTableProvider
let poly = PolyTableProvider::new(
    Arc::new(write_provider),
    Arc::new(delete_provider),
    Arc::new(fed_provider),
);

// Register with DataFusion
session.register_table(table_name, Arc::new(poly))?;
```

This pattern allows the same table to be queried (with federation), written to (with upserts), and pruned (with deletes) through a single unified interface.

## Data Retention

For accelerated tables, retention policies automatically delete old data:

```yaml
acceleration:
  engine: duckdb
  retention:
    enabled: true
    period: 30d  # Keep 30 days
    column: created_at
```

### Retention Implementation

After each data write, a retention handler executes:

```rust
fn make_retention_write_handler(
    dataset_name: String,
    parsed_delete: Delete,
) -> WriteCompletionHandler {
    Arc::new(move |tx, table_manager, _schema, _inserted_rows| {
        // Reconstruct DELETE with internal table name
        let internal_table = table_manager.get_internal_name(&dataset_name)?;

        let sql = reconstruct_retention_sql_with_table_name(
            &parsed_delete,
            &internal_table,
        )?;

        tx.execute(&sql, [])?;
        Ok(())
    })
}
```

## Our Fork and Contributions

We maintain a fork of `duckdb-rs`:

```toml
duckdb = { git = "https://github.com/spiceai/duckdb-rs", rev = "bcc1d9b" }
```

### Why Fork?

1. **Arrow compatibility** — Keep Arrow versions aligned with our stack
2. **Bug fixes** — Ship fixes before upstream merges
3. **Custom features** — Spice-specific enhancements
4. **Version pinning** — Control upgrade timing for stability

### Fork Philosophy

We follow a principled approach to forking:

- **Minimal divergence** — Stay as close to upstream as possible
- **Upstream-first** — Contribute improvements back to duckdb-rs
- **Tagged releases** — Our fork uses semantic versioning (`spiceai-1.4.2`)
- **Regular rebasing** — Periodically rebase onto latest upstream

### Key Modifications

Our fork includes several Spice-specific enhancements:

#### Arrow Version Alignment

We pin Arrow to the exact version used throughout Spice:

```toml
# Our fork pins these to match Spice's stack
arrow = "55.0"
arrow-schema = "55.0"
arrow-array = "55.0"
```

This avoids version conflicts and ensures zero-copy Arrow interop works correctly.

#### Connection Pool Integration

Enhanced connection pooling that integrates with our `DuckDBTableProviderFactory`:

```rust
// Pool sharing across datasets using same file
pub async fn get_or_init_instance_with_builder(
    &self,
    builder: DuckDbConnectionPoolBuilder,
) -> Result<DuckDbConnectionPool> {
    // Return existing pool or create new one
    // Handles file locking and concurrent access
}
```

#### Settings Registry

Extensible settings system for DuckDB configuration:

```rust
let factory = DuckDBTableProviderFactory::new(AccessMode::ReadWrite)
    .with_dialect(new_duckdb_dialect())
    .with_settings_registry(
        DuckDBSettingsRegistry::new()
            .with_setting(Box::new(OrderByNonIntegerLiteral))
    );
```

#### Function Support Control

Block Spice-specific functions from being pushed to DuckDB:

```rust
.with_function_support(deny_spice_specific_functions())
```

This prevents pushing UDFs that only exist in DataFusion.

### Upstream Contributions

We contribute fixes and improvements back to the main duckdb-rs repository. Recent contributions include:

- Arrow compatibility fixes
- Connection pool improvements
- Error message enhancements
- Performance optimizations

## Lessons Learned

After extensive DuckDB integration, here are our takeaways:

### 1. spawn_blocking is Essential

DuckDB is synchronous. Never call it directly from async code—always use `spawn_blocking`. We learned this the hard way with blocked async runtimes.

### 2. Connection Pooling Matters

DuckDB file locks mean concurrent access needs careful management. Connection pools with appropriate sizing prevent contention.

### 3. Dialect Translation is Tricky

Small differences add up: `rand()` vs `random()`, regex function names, type casting syntax. Maintain a comprehensive translation layer.

### 4. Aggregate Pushdown is Worth It

For analytical queries, pushing aggregations to DuckDB is dramatically faster than pulling all rows to DataFusion. The optimizer rule complexity pays off.

### 5. In-Memory vs. File Modes Have Tradeoffs

| Aspect       | Memory Mode      | File Mode            |
| ------------ | ---------------- | -------------------- |
| Speed        | Fastest          | Fast (with disk I/O) |
| Persistence  | Lost on restart  | Survives restarts    |
| Memory usage | Entire dataset   | Buffer cache only    |
| Startup      | Must reload data | Instant              |

Choose based on your durability and memory requirements.

### 6. Compound Indexes Need Thoughtful Design

Order matters in compound indexes. Design them based on your most common query patterns, putting high-cardinality filter columns first.

### 7. Partitioning Strategy Depends on Workload

Files mode gives maximum isolation but more file handles. Tables mode shares connection pools but complicates concurrent writes. Profile before choosing.

### 8. Provider Composition > Monolithic Providers

The PolyTableProvider pattern—wrapping specialized providers for read/write/delete—is more maintainable than one giant provider. Separation of concerns applies to data access too.

### 9. Index Tuning is Workload-Specific

Default index settings aren't optimal for all workloads. Expose tuning parameters so users can adjust for their query patterns.

### 10. Fork Carefully, Contribute Upstream

Maintaining a fork requires discipline. Stay close to upstream, contribute back, and document divergences. A well-maintained fork is sustainable; a drifting fork becomes a liability.

---

## Conclusion

DuckDB is a powerful embedded analytical database that slots perfectly into our acceleration architecture. Its columnar storage, native Arrow support, and SQL compatibility make it an ideal local cache for remote data sources.

The dual-mode architecture—connector for external DuckDB files, accelerator for any data source—provides flexibility. And optimizations like aggregate pushdown enable analytical queries that would otherwise require expensive data movement.

For local data acceleration, DuckDB is hard to beat.

---

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Rust Bindings](https://github.com/duckdb/duckdb-rs)
- [DuckDB Arrow Integration](https://duckdb.org/docs/guides/python/sql_on_arrow)
- [ART Indexes in DuckDB](https://duckdb.org/docs/sql/indexes)
- [datafusion-table-providers](https://github.com/datafusion-contrib/datafusion-table-providers)

