# Apache DataFusion at Spice AI: Building on a Modern Query Engine

> How we extend DataFusion with custom TableProviders, optimizer rules, and UDFs for federated SQL

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- [Rust at Spice AI](rust-at-spiceai.md) — Our systems programming foundation
- [Apache Arrow at Spice AI](apache-arrow-at-spiceai.md) — Arrow as our core data format
- **Apache DataFusion at Spice AI** *(You are here)*
- [DuckDB at Spice AI](duckdb-at-spiceai.md) — Embedded analytics acceleration
- [Apache Iceberg at Spice AI](apache-iceberg-at-spiceai.md) — Open table format integration
- [Vortex at Spice AI](vortex-at-spiceai.md) — Columnar compression for Cayenne
- [Apache Ballista at Spice AI](apache-ballista-at-spiceai.md) — Distributed query execution

---

## Table of Contents

- [What is Apache DataFusion?](#what-is-apache-datafusion)
- [Why DataFusion?](#why-datafusion)
- [SessionState Configuration](#sessionstate-configuration)
- [Custom TableProvider Implementations](#custom-tableprovider-implementations)
- [SQL Federation](#sql-federation)
- [Custom Optimizer Rules](#custom-optimizer-rules)
- [User-Defined Functions](#user-defined-functions)
- [Physical Execution Extensions](#physical-execution-extensions)
- [Our Fork and Contributions](#our-fork-and-contributions)
- [Lessons Learned](#lessons-learned)

---

Apache DataFusion is a fast, extensible query engine written in Rust. It provides SQL and DataFrame APIs, a query planner, a cost-based optimizer, and a multi-threaded execution engine—all built on Apache Arrow.

At Spice, DataFusion is the heart of our query processing. We've extended it with custom table providers for 20+ data sources, optimizer rules for federation and acceleration, and UDFs for AI inference, vector search, and text search.

## What is Apache DataFusion?

DataFusion provides the complete query execution pipeline:

```text
SQL → Parser → Logical Plan → Optimizer → Physical Plan → Execution → Arrow Results
```

Each stage is extensible:

| Stage             | Extension Point             | Spice Extensions                                      |
| ----------------- | --------------------------- | ----------------------------------------------------- |
| Parsing           | Custom SQL syntax           | —                                                     |
| Logical Planning  | TableProvider, AnalyzerRule | 20+ data connectors, federation analyzer              |
| Optimization      | OptimizerRule               | Cache invalidation, DuckDB pushdown                   |
| Physical Planning | ExtensionPlanner            | Index scans, fallback execution                       |
| Execution         | ExecutionPlan               | Schema casting, managed streams                       |
| Functions         | ScalarUDF, TableFunction    | `ai()`, `embed()`, `vector_search()`, `text_search()` |

## Why DataFusion?

We evaluated several query engines before choosing DataFusion:

### Native Rust and Arrow

DataFusion is written in Rust and uses Arrow as its native format—exactly matching our architecture. No FFI overhead, no format conversions.

### Extensibility

Every component can be replaced or extended. We can add custom data sources, optimizer rules, and execution plans without forking the core engine.

### Active Community

DataFusion has an active community with regular releases. We contribute upstream when our extensions benefit the broader ecosystem.

### Performance

DataFusion's execution engine uses:

- **Vectorized processing** with Arrow arrays
- **Push-based execution** for streaming
- **Partition-aware parallelism** that scales with CPU cores
- **Predicate and projection pushdown** to minimize data movement

## SessionState Configuration

DataFusion's `SessionState` holds all configuration for query execution. Here's how we configure ours:

```rust
pub fn create_session_state(&self) -> SessionState {
    let config = SessionConfig::new()
        .with_information_schema(true)
        .set_bool("datafusion.catalog.has_header", true)
        .set_str("datafusion.sql_parser.dialect", "PostgreSQL")
        // Case-sensitive identifiers
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false);

    let mut state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_query_planner(Arc::new(
            ExtensionPlanQueryPlanner::from_extension_planners(
                default_extension_planners()
            ),
        ))
        .with_runtime_env(runtime_env(
            self.memory_limit,
            self.temp_directory.clone(),
            self.io_runtime.clone(),
        ))
        .with_analyzer_rules(custom_analyzer_rules())
        .build();

    // Register physical optimizer rules
    state = state.with_physical_optimizer_rules(
        self.physical_optimizer_rules()
    );

    state
}
```

### Key Configuration Choices

**PostgreSQL Dialect**: We use PostgreSQL syntax for familiar SQL semantics.

**Case-Sensitive Identifiers**: Disabled normalization preserves column case from source systems.

**Custom Analyzer Rules**: Our federation analyzer runs first, before DataFusion's defaults.

**Separate IO Runtime**: Query execution runs on a dedicated Tokio runtime to avoid blocking HTTP handlers.

## Custom TableProvider Implementations

`TableProvider` is the interface between DataFusion and data sources. We implement it for every connector:

### The Architecture: AcceleratedTable → FederatedTable → Connector

```text
┌─────────────────────────────────────────────────────────┐
│                    AcceleratedTable                     │
│  Wraps federated source with local cache               │
│  Handles refresh, fallback, zero-results policies      │
├─────────────────────────────────────────────────────────┤
│                    FederatedTable                       │
│  Supports immediate or deferred connection             │
│  Enables SQL pushdown to source                        │
├─────────────────────────────────────────────────────────┤
│               Connector TableProvider                   │
│  PostgreSQL, Snowflake, S3, DuckDB, etc.              │
└─────────────────────────────────────────────────────────┘
```

### AcceleratedTable

```rust
pub struct AcceleratedTable {
    dataset_name: TableReference,
    accelerator: Arc<dyn TableProvider>,     // Local cache (DuckDB, SQLite, Arrow)
    federated: Arc<FederatedTable>,          // Source data
    zero_results_action: ZeroResultsAction,  // Fallback behavior
    refresh_mode: RefreshMode,               // Full, Append, Changes
}
```

`AcceleratedTable` provides:

- **Local query execution** against the accelerator
- **Background refresh** from the federated source
- **Fallback to source** when local returns zero results (configurable)
- **Caching semantics** with TTL and invalidation

### FederatedTable

```rust
pub enum FederatedTable {
    // TableProvider available immediately
    Immediate(Arc<dyn TableProvider>),

    // Retries connection in background, serves stale data from checkpoint
    Deferred(DeferredTableProvider),
}
```

`Deferred` mode enables resilient startup—if a source is temporarily unavailable, Spice starts with cached data and retries in the background.

### Example: FlightSQL TableProvider

```rust
impl TableProvider for FlightSQLTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // All filters can be pushed to the remote SQL server
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Build SQL with pushed-down predicates and projections
        let sql = self.build_query(projection, filters, limit)?;

        // Return execution plan that streams Arrow from Flight server
        Ok(Arc::new(FlightExec::new(self.client.clone(), sql)))
    }
}
```

### Data Source Coverage

We implement `TableProvider` for 20+ sources:

| Category    | Sources                                          |
| ----------- | ------------------------------------------------ |
| Databases   | PostgreSQL, MySQL, SQLite, DuckDB, Oracle, MSSQL |
| Warehouses  | Snowflake, Databricks, BigQuery, Redshift        |
| Lakes       | Delta Lake, Iceberg, S3, Azure Blob, GCS         |
| Streaming   | Kafka, Debezium, DynamoDB Streams                |
| APIs        | GraphQL, HTTP/REST, GitHub, SharePoint           |
| Specialized | ClickHouse, MongoDB, Turso, FTP/SFTP             |

## SQL Federation

For sources that support SQL (databases, warehouses), we push queries down rather than pulling all data:

```sql
-- User query
SELECT name, SUM(amount) FROM sales
WHERE region = 'NA' AND date > '2024-01-01'
GROUP BY name

-- What we push to Snowflake (via Arrow Flight SQL)
SELECT name, SUM(amount) FROM sales
WHERE region = 'NA' AND date > '2024-01-01'
GROUP BY name

-- Only aggregated results flow over the network
```

### Federation Architecture

We use the `datafusion-federation` crate to handle query pushdown:

```rust
impl FlightSQLTable {
    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_name = self.table_reference.clone();
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));

        Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        ))
    }
}
```

### Dialect Translation

Different databases have different SQL dialects. We translate DataFusion expressions:

```rust
pub fn new_duckdb_dialect() -> Arc<dyn Dialect> {
    DuckDBDialect::new().with_custom_scalar_overrides(vec![
        // cosine_distance → array_cosine_distance
        (COSINE_DISTANCE_UDF_NAME, Box::new(duckdb::cosine_distance_to_sql)),
        // rand() → random()
        ("rand", Box::new(duckdb::rand_to_random)),
        // regexp_like → regexp_matches
        (REGEXP_LIKE_NAME, Box::new(duckdb::regexp_like_to_sql)),
    ])
}
```

## Custom Optimizer Rules

DataFusion's optimizer is a pipeline of transformation rules. We add custom rules for our needs:

### Cache Invalidation Rule

When a DML statement (INSERT, UPDATE, DELETE) completes, we invalidate affected caches:

```rust
impl OptimizerRule for CacheInvalidationOptimizerRule {
    fn name(&self) -> &'static str {
        "cache_invalidation"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|plan| match plan {
            LogicalPlan::Dml(dml) => {
                // Wrap DML with cache invalidation node
                let node = CacheInvalidationNode::new(
                    LogicalPlan::Dml(dml),
                    table_name,
                    Weak::clone(&self.caching),
                );
                Ok(Transformed::yes(LogicalPlan::Extension(
                    Extension { node: Arc::new(node) }
                )))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}
```

### DuckDB Aggregate Pushdown

For DuckDB-accelerated tables, we push aggregations to DuckDB:

```rust
static SUPPORTED_AGG_FUNCTIONS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        // Basic aggregates
        "avg", "count", "max", "min", "sum",
        // Statistical
        "corr", "covar_pop", "stddev_pop", "var_pop",
        // Boolean
        "bool_and", "bool_or",
        // Approximate
        "approx_percentile_cont",
    ])
});
```

When enabled, the optimizer rewrites:

```sql
-- Original (DataFusion executes aggregate)
SELECT region, SUM(sales) FROM duckdb_table GROUP BY region

-- Rewritten (DuckDB executes aggregate via SQL federation)
SELECT region, SUM(sales) FROM duckdb_table GROUP BY region
-- Pushed as native DuckDB SQL
```

### Physical Optimizer: Empty Hash Join

If we can prove one side of a join is empty at planning time, we skip execution:

```rust
impl PhysicalOptimizerRule for EmptyHashJoinExecPhysicalOptimization {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };

            let is_empty = match join.join_type() {
                JoinType::Inner =>
                    guaranteed_empty(join.left()) || guaranteed_empty(join.right()),
                JoinType::Left =>
                    guaranteed_empty(join.left()),
                // ... other join types
            };

            if is_empty {
                Ok(Transformed::yes(Arc::new(EmptyExec::new(join.schema()))))
            } else {
                Ok(Transformed::no(plan))
            }
        }).data()
    }
}
```

## User-Defined Functions

DataFusion supports scalar UDFs, aggregate UDFs, and table-valued functions. We use all three:

### Scalar UDFs

Simple functions that operate on individual values:

```rust
pub struct CosineDistance;

impl ScalarUDFImpl for CosineDistance {
    fn name(&self) -> &str { "cosine_distance" }

    fn signature(&self) -> &Signature {
        Signature::exact(vec![DataType::List, DataType::List], Volatility::Immutable)
    }

    fn return_type(&self, _args: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let left = args[0].as_array();
        let right = args[1].as_array();
        Ok(ColumnarValue::Array(compute_cosine_distance(left, right)?))
    }
}
```

### Async Scalar UDFs for AI

LLM calls are async. DataFusion's `AsyncScalarUDFImpl` trait enables this:

```rust
pub struct Ai {
    model_store: Arc<RwLock<ChatModelStore>>,
}

#[async_trait]
impl AsyncScalarUDFImpl for Ai {
    fn name(&self) -> &str { "ai" }

    async fn invoke_async(
        &self,
        args: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        let prompt = extract_string(&args.args[0])?;
        let model_name = extract_string(&args.args[1])?;

        let model = self.model_store.read().get(&model_name)?;
        let response = model.complete(&prompt).await?;

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(response))))
    }
}
```

Usage:

```sql
SELECT ai('Summarize this text: ' || content, 'gpt-4') as summary
FROM documents
```

### Table-Valued Functions

`vector_search()` and `text_search()` return tables:

```rust
impl TableFunctionImpl for VectorSearchTableFunc {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let parsed = Self::parse_args(args)?;
        let df = self.df.upgrade().context("Runtime dropped")?;

        let table = df.get_table_sync(&parsed.table)?;
        let embedding_table = find_embedding_table(&table)?;

        Ok(Arc::new(VectorSearchUDTFProvider {
            args: parsed,
            underlying: table,
            embedding_models: embedding_table.embedding_models,
        }))
    }
}
```

Usage:

```sql
SELECT * FROM vector_search(
    'documents',
    'embedding_column',
    'search query text',
    10  -- top k
)
```

### UDF Registration

All UDFs are registered at runtime startup:

```rust
pub async fn register_udfs(runtime: &crate::Runtime) {
    let ctx = &runtime.df.ctx;

    // Scalar UDFs
    ctx.register_udf(CosineDistance::new().into());
    ctx.register_udf(Bucket::new().into());
    ctx.register_udf(Truncate::new().into());

    // Async UDFs for AI
    #[cfg(feature = "models")]
    {
        ctx.register_udf(Embed::new(runtime.embeds()).into());
        ctx.register_udf(
            Ai::new(runtime.completion_llms())
                .into_async_udf()
                .into_scalar_udf(),
        );
    }

    // Table-valued functions
    ctx.register_udtf("vector_search", Arc::new(VectorSearchTableFunc::new(...)));
    ctx.register_udtf("text_search", Arc::new(TextSearchTableFunc::new(...)));
}
```

## Physical Execution Extensions

Sometimes we need custom execution behavior beyond logical planning:

### FallbackOnZeroResultsScanExec

If an accelerated table returns zero rows, optionally fall back to the source:

```rust
pub struct FallbackOnZeroResultsScanExec {
    input: Arc<dyn ExecutionPlan>,
    fallback_table_provider: FallbackAsyncTableProvider,
    fallback_scan_params: TableScanParams,
}

impl ExecutionPlan for FallbackOnZeroResultsScanExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context.clone())?;

        // Wrap stream to detect zero results and trigger fallback
        Ok(Box::pin(FallbackStream::new(
            input_stream,
            self.fallback_table_provider.clone(),
            self.fallback_scan_params.clone(),
            context,
        )))
    }
}
```

### SchemaCastScanExec

Handles schema evolution by casting during streaming:

```rust
pub struct SchemaCastScanExec {
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
}

impl ExecutionPlan for SchemaCastScanExec {
    fn execute(...) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(SchemaCastStream::new(
            input_stream,
            Arc::clone(&self.target_schema),
        )))
    }
}
```

### Extension Planners

Custom logical plan nodes need physical planners:

```rust
pub fn default_extension_planners() -> Vec<Arc<dyn ExtensionPlanner>> {
    vec![
        Arc::new(IndexTableScanExtensionPlanner::new()),
        Arc::new(FederatedPlanner::new()),
        Arc::new(CacheInvalidationExtensionPlanner::new()),
        #[cfg(feature = "duckdb")]
        DuckDBLogicalExtensionPlanner::new(),
    ]
}
```

## Our Fork and Contributions

We maintain a fork of DataFusion at `spiceai/datafusion`:

```toml
datafusion = { git = "https://github.com/spiceai/datafusion", rev = "10b5cc5" }
```

### Why Fork?

1. **Faster iteration** — We can ship features before they're merged upstream
2. **Custom patches** — Some changes are Spice-specific
3. **Stability** — We control our update cadence

### Upstream Contributions

We contribute generally-useful improvements back to DataFusion:

- Bug fixes
- Performance improvements
- Documentation
- New features that benefit the community

We stay close to upstream `main` and regularly rebase our fork.

## Lessons Learned

Building on DataFusion for two years has taught us:

### 1. TableProvider is Incredibly Powerful

The `TableProvider` abstraction lets us add any data source without modifying DataFusion. We've implemented 20+ connectors this way.

### 2. Optimizer Rules Compose Well

Each rule does one thing. Cache invalidation, aggregate pushdown, and empty join elimination all coexist without conflicts.

### 3. Physical Planning is the Escape Hatch

When logical transformations aren't enough, custom `ExecutionPlan` implementations let us do anything—fallback streams, schema casting, managed runtimes.

### 4. Schema Metadata is Your Friend

Arrow schema metadata flows through the entire pipeline. We use it for:

- Source tracking (which connector)
- Acceleration status (accelerated vs. federated)
- Optimization hints (enable aggregate pushdown)

### 5. Async UDFs Open New Possibilities

DataFusion's async UDF support enables SQL-embedded AI:

```sql
SELECT ai('Summarize: ' || text) FROM articles
```

This wouldn't be possible with synchronous-only UDFs.

### 6. Federation Requires Dialect Awareness

Different databases have different SQL. Plan for dialect translation from the start, not as an afterthought.

---

## Conclusion

Apache DataFusion provides the foundation for Spice's query engine—parsing, planning, optimization, and execution all in Rust with native Arrow support. Its extensibility lets us add custom table providers, optimizer rules, and UDFs without forking the core engine.

The key insight: DataFusion is designed to be extended. Every interface—`TableProvider`, `OptimizerRule`, `ExecutionPlan`, `ScalarUDFImpl`—is a stable extension point. Build on these abstractions, contribute back what you can, and you get a production-grade query engine with your custom capabilities.

---

## References

- [Apache DataFusion Documentation](https://datafusion.apache.org/)
- [DataFusion GitHub](https://github.com/apache/datafusion)
- [DataFusion Examples](https://github.com/apache/datafusion/tree/main/datafusion-examples)
- [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation)
- [Writing a Custom TableProvider](https://datafusion.apache.org/library-user-guide/adding-a-data-source.html)

