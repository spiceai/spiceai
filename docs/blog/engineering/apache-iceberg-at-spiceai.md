# Apache Iceberg at Spice AI: Open Table Format Integration

> How we connect to Iceberg catalogs and tables for data lakehouse queries

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- [Rust at Spice AI](rust-at-spiceai.md) — Our systems programming foundation
- [Apache Arrow at Spice AI](apache-arrow-at-spiceai.md) — Arrow as our core data format
- [Apache DataFusion at Spice AI](apache-datafusion-at-spiceai.md) — Our SQL query engine foundation
- [DuckDB at Spice AI](duckdb-at-spiceai.md) — Embedded analytics acceleration
- **Apache Iceberg at Spice AI** *(You are here)*
- [Vortex at Spice AI](vortex-at-spiceai.md) — Columnar compression for Cayenne
- [Apache Ballista at Spice AI](apache-ballista-at-spiceai.md) — Distributed query execution

---

## Table of Contents

- [What is Apache Iceberg?](#what-is-apache-iceberg)
- [Why Iceberg Matters](#why-iceberg-matters)
- [Iceberg Architecture](#iceberg-architecture)
- [Catalog Integration](#catalog-integration)
- [IcebergCatalogProvider Implementation](#icebergcatalogprovider-implementation)
- [Table Provider and Queries](#table-provider-and-queries)
- [Error Handling](#error-handling)
- [Our Fork and Contributions](#our-fork-and-contributions)
- [Lessons Learned](#lessons-learned)

---

Apache Iceberg is an open table format for huge analytic datasets. Unlike older formats like Hive, Iceberg provides ACID transactions, schema evolution, hidden partitioning, and time travel—all while storing data in open Parquet files.

At Spice, Iceberg is a first-class data source. Users can connect to Iceberg catalogs (REST, Glue, Hadoop) and query tables with full SQL semantics. Combined with our acceleration layer, Iceberg tables can be cached locally for sub-millisecond query performance.

## What is Apache Iceberg?

Iceberg is a table format specification that defines:

1. **Metadata Layer** — JSON and Avro files tracking table schema, partitions, and data files
2. **Data Layer** — Parquet (or ORC/Avro) files containing the actual data
3. **Catalog API** — Standard interface for table discovery and management

```text
┌─────────────────────────────────────────────────────────────┐
│                      Iceberg Catalog                        │
│              (REST, Glue, Hive, Hadoop)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Table Metadata                          │
│         (Schema, Partitions, Snapshots, Files)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Data Files                            │
│               (Parquet on S3, GCS, HDFS)                    │
└─────────────────────────────────────────────────────────────┘
```

## Why Iceberg Matters

Iceberg solves problems that plagued earlier data lake formats:

### ACID Transactions

Iceberg provides atomic writes. Concurrent writers don't corrupt data, and readers never see partial writes.

### Schema Evolution

Add, rename, or remove columns without rewriting data files. Iceberg tracks schema versions in metadata.

### Hidden Partitioning

Unlike Hive-style partitioning that leaks into SQL queries (`WHERE year=2024 AND month=03`), Iceberg handles partitioning transparently. Users write `WHERE date > '2024-03-01'` and Iceberg prunes partitions automatically.

### Time Travel

Query data as of a specific timestamp or snapshot ID. Iceberg retains historical metadata for rollback and auditing.

### Open Format

Data lives in standard Parquet files. Any Parquet-compatible tool can read the raw data. Iceberg adds the table abstraction on top.

## Iceberg Architecture

In Spice, we integrate Iceberg through:

1. **Catalog Connectors** — Connect to REST, Glue, or Hadoop catalogs
2. **CatalogProvider** — DataFusion's interface for discovering schemas and tables
3. **TableProvider** — DataFusion's interface for querying individual tables

```text
┌─────────────────────────────────────────────────────────────┐
│                       User Query                            │
│            SELECT * FROM iceberg.db.table                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 IcebergCatalogProvider                      │
│          Implements DataFusion CatalogProvider              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 IcebergSchemaProvider                       │
│          Implements DataFusion SchemaProvider               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 IcebergTableProvider                        │
│   From iceberg-datafusion crate, handles scans              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Parquet Files                            │
│              (S3, GCS, HDFS, Local FS)                      │
└─────────────────────────────────────────────────────────────┘
```

## Catalog Integration

Spice supports multiple Iceberg catalog types:

### REST Catalog

The standard HTTP API for Iceberg catalogs:

```yaml
catalogs:
  - name: iceberg_catalog
    from: iceberg:rest
    params:
      iceberg_rest_uri: https://iceberg-catalog.example.com
      iceberg_rest_warehouse: my_warehouse
```

### AWS Glue Catalog

For AWS-native data lakes:

```yaml
catalogs:
  - name: glue_catalog
    from: iceberg:glue
    params:
      aws_region: us-east-1
      glue_database: analytics
```

### Hadoop Catalog

For file-based catalogs (S3, HDFS, local):

```yaml
catalogs:
  - name: hadoop_catalog
    from: iceberg:hadoop
    params:
      iceberg_hadoop_warehouse: s3://my-bucket/warehouse
```

### Catalog Module Structure

Our catalog integration lives in `crates/data_components/src/iceberg/catalog/`:

```text
iceberg/
├── mod.rs           # Module exports
├── provider.rs      # IcebergCatalogProvider, IcebergSchemaProvider
└── catalog/
    ├── mod.rs       # Error types
    ├── rest/        # REST catalog implementation
    └── hadoop/      # Hadoop catalog implementation
```

## IcebergCatalogProvider Implementation

We implement DataFusion's `CatalogProvider` trait to expose Iceberg catalogs:

```rust
/// Provides an interface to manage and access multiple schemas
/// within an Iceberg Catalog.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    /// Keys are namespace names, values are SchemaProviders
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    /// Asynchronously constructs a new IcebergCatalogProvider
    /// by fetching namespaces from the Iceberg catalog.
    pub async fn try_new(
        client: Arc<dyn Catalog>,
        root_namespace: Option<NamespaceIdent>,
        includes: Option<&GlobSet>,
    ) -> Result<Self> {
        // Limit concurrent namespace loading
        let load_semaphore = Arc::new(Semaphore::new(10));

        // List namespaces from the catalog
        let schema_names: Vec<_> = client
            .list_namespaces(root_namespace.as_ref())
            .await?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        // Load schema providers in parallel
        let providers = try_join_all(
            schema_names.iter().map(|name| {
                IcebergSchemaProvider::try_new(
                    Arc::clone(&client),
                    NamespaceIdent::new(name.clone()),
                    Arc::clone(&load_semaphore),
                    includes,
                )
            })
        ).await?;

        // Build the schemas map
        let schemas: HashMap<String, Arc<dyn SchemaProvider>> = schema_names
            .into_iter()
            .zip(providers)
            .map(|(name, provider)| (name, Arc::new(provider) as _))
            .collect();

        Ok(IcebergCatalogProvider { schemas })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}
```

### IcebergSchemaProvider

Each namespace becomes a `SchemaProvider` containing tables:

```rust
/// Represents a SchemaProvider for an Iceberg namespace,
/// managing access to tables within that namespace.
#[derive(Debug)]
pub(crate) struct IcebergSchemaProvider {
    /// Keys are table names, values are TableProviders
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl IcebergSchemaProvider {
    pub async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        semaphore: Arc<Semaphore>,
        includes: Option<&GlobSet>,
    ) -> Result<Self> {
        // List tables in the namespace
        let table_idents = client.list_tables(&namespace).await?;

        // Filter by include patterns
        let filtered_tables = match includes {
            Some(glob) => table_idents
                .into_iter()
                .filter(|t| glob.is_match(t.name()))
                .collect(),
            None => table_idents,
        };

        // Load table providers with concurrency limit
        let tables = try_join_all(
            filtered_tables.iter().map(|ident| {
                let permit = semaphore.acquire();
                async move {
                    let _permit = permit.await?;
                    let table = client.load_table(ident).await?;
                    let provider = IcebergTableProvider::try_new(table).await?;
                    Ok((ident.name().to_string(), Arc::new(provider) as _))
                }
            })
        ).await?;

        Ok(IcebergSchemaProvider {
            tables: tables.into_iter().collect(),
        })
    }
}

impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any { self }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).cloned()
    }
}
```

### Refreshable Catalogs

Catalogs can refresh to pick up new tables:

```rust
#[async_trait]
impl RefreshableCatalogProvider for IcebergCatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Re-scan the catalog for new namespaces and tables
        // Currently a stub for future enhancement
        Ok(())
    }
}
```

## Table Provider and Queries

Individual Iceberg tables are exposed via `IcebergTableProvider` from the `iceberg-datafusion` crate:

```rust
use iceberg_datafusion::IcebergTableProvider;

// Load table from catalog
let iceberg_table = catalog.load_table(&table_ident).await?;

// Create DataFusion TableProvider
let provider = IcebergTableProvider::try_new(iceberg_table).await?;
```

### Query Execution

Queries execute by:

1. **Metadata scan** — Read Iceberg manifest files to find relevant data files
2. **Predicate pushdown** — Filter manifests and files by partition predicates
3. **Parquet scan** — Read only required Parquet files and row groups

```sql
-- This query:
SELECT * FROM iceberg_catalog.analytics.events
WHERE event_date > '2024-01-01'
  AND event_type = 'purchase'

-- Iceberg optimizes by:
-- 1. Pruning partitions where event_date <= '2024-01-01'
-- 2. Using column statistics to skip files without 'purchase' values
-- 3. Reading only matching row groups from remaining Parquet files
```

### Acceleration

Iceberg tables can be accelerated like any other source:

```yaml
datasets:
  - from: iceberg:analytics.events
    name: accelerated_events
    acceleration:
      enabled: true
      engine: duckdb
      refresh_sql: |
        SELECT * FROM analytics.events WHERE event_date > NOW() - INTERVAL '7 days'
```

This caches the last 7 days of events locally for fast querying.

## Error Handling

We use SNAFU for actionable error messages:

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "An unknown error occurred while interacting with the Iceberg catalog. \
        Report an issue at https://github.com/spiceai/spiceai/issues {source}"
    ))]
    Unknown { source: iceberg::Error },

    #[snafu(display(
        "The data in the Iceberg table is invalid. \
        The table may be corrupted or incomplete. {source}"
    ))]
    DataInvalid { source: iceberg::Error },

    #[snafu(display(
        "This Iceberg feature is not yet supported. \
        Report an issue at https://github.com/spiceai/spiceai/issues {source}"
    ))]
    FeatureUnsupported { source: iceberg::Error },

    #[snafu(display(
        "The namespace '{namespace}' does not exist in the Iceberg catalog, \
        verify the namespace name and try again."
    ))]
    NamespaceDoesNotExist { namespace: String },

    #[snafu(display(
        "Failed to connect to the Iceberg catalog or object store at {url}: {source}. \
        Verify the Iceberg catalog is accessible and try again."
    ))]
    FailedToConnect { url: String, source: iceberg::Error },

    #[snafu(display("TLS/SSL certificate error connecting to {}: {}", url, detail))]
    CertificateError {
        url: String,
        detail: String,
        source: iceberg::Error,
    },
}
```

### Error Mapping

We map Iceberg's generic errors to specific, actionable messages:

```rust
pub fn handle_iceberg_error(error: iceberg::Error) -> Error {
    match error.kind() {
        iceberg::ErrorKind::DataInvalid => {
            let err_msg = error.to_string();

            // Detect specific conditions from error message
            if err_msg.contains("NoSuchNamespaceException")
                || err_msg.contains("Namespace does not exist")
            {
                // Extract namespace and return specific error
                return Error::NamespaceDoesNotExist { ... };
            }

            Error::DataInvalid { source: error }
        }
        iceberg::ErrorKind::FeatureUnsupported => {
            Error::FeatureUnsupported { source: error }
        }
        _ => Error::Unknown { source: error }
    }
}
```

## Our Fork and Contributions

We maintain a fork of `iceberg-rust`:

```toml
iceberg = { git = "https://github.com/spiceai/iceberg-rust", rev = "5ab32ec" }
iceberg-catalog-rest = { git = "https://github.com/spiceai/iceberg-rust", rev = "5ab32ec" }
iceberg-datafusion = { git = "https://github.com/spiceai/iceberg-rust", rev = "5ab32ec" }
```

### Why Fork?

1. **Version alignment** — Keep DataFusion and Arrow versions compatible
2. **Bug fixes** — Ship fixes before they're merged upstream
3. **Feature development** — Test new features in production

### Contributions

We contribute improvements back to the iceberg-rust community:

- Bug fixes
- Performance improvements
- New catalog implementations
- Documentation

## Lessons Learned

After integrating Iceberg, here are our takeaways:

### 1. Catalog Discovery is Slow

Enumerating all namespaces and tables involves many metadata requests. We use:

- **Concurrency limiting** — Semaphore to prevent overwhelming the catalog
- **Lazy loading** — Only load tables when queried
- **Include patterns** — Filter to only relevant tables

```rust
let load_semaphore = Arc::new(Semaphore::new(10));
```

### 2. Metadata vs. Data Location Matters

Iceberg metadata (manifests, snapshot files) and data (Parquet files) can be in different locations. Users often configure:

- REST catalog with S3 data files
- Glue catalog with cross-account S3 access
- Hadoop catalog on HDFS

Ensure object store credentials cover both locations.

### 3. Schema Evolution is Seamless

Iceberg handles schema changes gracefully. When the upstream table adds columns, our provider automatically sees them—no configuration changes needed.

### 4. Predicate Pushdown is Critical

Iceberg's partition pruning and file-level min/max statistics dramatically reduce data scanned. Always use predicates that Iceberg can optimize:

```sql
-- Good: Iceberg can prune partitions
SELECT * FROM events WHERE event_date > '2024-01-01'

-- Less optimal: Function prevents pushdown
SELECT * FROM events WHERE YEAR(event_date) = 2024
```

### 5. Acceleration Complements Iceberg

Iceberg provides durability and organization; acceleration provides speed. The combination gives you:

- **Source of truth** — Iceberg tables in your data lake
- **Fast queries** — DuckDB-cached subsets for applications

---

## Conclusion

Apache Iceberg brings modern table semantics to data lakes—ACID transactions, schema evolution, and time travel in an open format. Our integration exposes Iceberg catalogs as DataFusion catalog providers, enabling full SQL access to Iceberg tables.

Combined with our acceleration layer, you get the best of both worlds: Iceberg's reliability and openness with local query performance. Connect your Iceberg catalog once, query it instantly.

---

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg Rust Implementation](https://github.com/apache/iceberg-rust)
- [iceberg-datafusion Integration](https://github.com/apache/iceberg-rust/tree/main/crates/iceberg-datafusion)
- [Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [Apache Parquet](https://parquet.apache.org/)

