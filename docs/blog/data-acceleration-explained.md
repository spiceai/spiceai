# Data Lake and Database Acceleration: What It Is and Why You Need It

> Reduce query latency from seconds to milliseconds by bringing data closer to your application

---

## Table of Contents

- [The Problem: Network Latency Dominates Query Time](#the-problem-network-latency-dominates-query-time)
- [How Data Acceleration Relates to Concepts You Know](#how-data-acceleration-relates-to-concepts-you-know)
- [What Data Acceleration Does](#what-data-acceleration-does)
- [Use Cases: When Acceleration Matters](#use-cases-when-acceleration-matters)
- [Beyond Performance: The Data Substrate](#beyond-performance-the-data-substrate)
- [How Data Acceleration Works in Spice](#how-data-acceleration-works-in-spice)
- [Acceleration Engines](#acceleration-engines)
- [Architecture: How It Fits Together](#architecture-how-it-fits-together)
- [Under the Hood: How It Actually Works](#under-the-hood-how-it-actually-works)
- [Getting Started](#getting-started)
- [Conclusion](#conclusion)

---

Modern data applications face a fundamental tension: data lives in cloud data lakes, warehouses, and databases—often across multiple regions—while applications demand sub-millisecond query performance. Every network round-trip, every cross-region hop, every cold-start query compilation adds latency that compounds into poor user experiences.

**Data acceleration** solves this problem by materializing data locally—co-located with your application—and keeping it synchronized with the authoritative source. Think of it as a smart, application-aware cache that understands your data's semantics, not just its bytes.

## The Problem: Network Latency Dominates Query Time

Consider a typical analytics dashboard backed by Snowflake or Databricks:

```text
Application → Network (50-200ms) → Data Warehouse → Query Compilation (100-500ms) → Execution → Network → Response
```

Even a simple `SELECT` can take 500ms-2s when the warehouse needs to spin up compute, compile the query, and transfer results across the network. For AI-powered applications running dozens of queries per request, this latency is catastrophic.

Worse, many data sources weren't designed for low-latency operational workloads:

- **Data lakes** (S3, Delta Lake, Iceberg) are optimized for batch processing, not point queries
- **Cloud warehouses** (Snowflake, BigQuery) have cold-start overhead and per-query costs
- **Cross-region databases** suffer from physics—light only travels so fast

## How Data Acceleration Relates to Concepts You Know

If you've been in the data space, you've likely encountered related concepts:

### Reverse ETL

Reverse ETL tools (Hightouch, Census, etc.) sync data from warehouses back to operational systems—Salesforce, HubSpot, Braze. The insight: data locked in a warehouse isn't useful; it needs to flow to where decisions happen.

Data acceleration takes this further. Instead of pushing data to third-party SaaS tools, you materialize it locally where *your* application runs. Same insight (data needs to be where it's used), different destination (your application instead of external tools).

### Serving Layers and Feature Stores

ML feature stores (Feast, Tecton) solve a similar problem for machine learning: training happens on batch data in warehouses, but inference needs millisecond-latency feature lookups. Feature stores materialize features into low-latency stores for serving.

Data acceleration generalizes this pattern beyond ML features to any data your application needs—reference tables, user profiles, configuration, analytics aggregates.

### Caching Layers

Redis and Memcached have served as application caches for decades. But they require you to manage cache invalidation, TTLs, and serialization yourself. Data acceleration provides caching semantics with SQL query capabilities and automatic synchronization.

## What Data Acceleration Does

Data acceleration materializes a working subset of your data into a local, high-performance query engine—typically an embedded database like DuckDB or SQLite. This local copy:

1. **Eliminates network latency** — Queries execute against local storage
2. **Uses purpose-built engines** — DuckDB/SQLite are optimized for analytical and operational queries
3. **Stays synchronized** — Background processes keep local data fresh
4. **Supports intelligent filtering** — Only accelerate what you need, not entire datasets

The result: queries that took 500ms now complete in 5ms—a 100x improvement.

## Use Cases: When Acceleration Matters

### 1. Real-Time Dashboards and Analytics

Trading dashboards, operational metrics, and customer-facing analytics demand instant response times. Users expect charts to render in under 100ms.

**Without acceleration:** Each chart widget fires a query to a remote warehouse. With 10 widgets, you're looking at 5-10 seconds of load time.

**With acceleration:** Queries execute against local DuckDB. The entire dashboard renders in under 200ms.

### 2. AI and RAG Applications

Large Language Model (LLM) applications using Retrieval-Augmented Generation (RAG) often execute multiple queries per request:

1. Vector search for relevant documents
2. Metadata lookups for context
3. Structured data queries for grounding

Each query adds latency. With 5 queries averaging 200ms each, your AI response time starts at 1 second—before the LLM even begins generating.

**With acceleration:** Vector indexes and metadata tables are local. Query time drops to single-digit milliseconds.

### 3. AI Data Sandboxing and Isolation

When AI agents query your data, you face a fundamental tension: agents need broad data access to be useful, but unrestricted access to production databases is a security and stability nightmare.

Data acceleration provides a natural **isolation boundary**:

- **Read-only by design** — Accelerated data is a materialized copy; agents can't modify the source
- **Scoped access** — Use `refresh_sql` to expose only the data an agent should see
- **Resource isolation** — Runaway agent queries don't impact production databases
- **Audit trail** — All queries hit the local acceleration layer, making logging straightforward

This pattern—giving AI agents a sandboxed, read-only view of production data—is becoming essential as organizations deploy autonomous agents that need to query enterprise data.

```yaml
# Accelerate only what the AI agent needs
datasets:
  - from: postgres:production.customers
    name: agent_customers
    acceleration:
      enabled: true
      refresh_sql: |
        SELECT id, name, tier, region 
        FROM customers 
        WHERE pii_redacted = true
```

### 4. Edge and Embedded Applications

IoT gateways, retail point-of-sale systems, and mobile applications often operate with intermittent connectivity. They need to query data even when the network is unavailable.

**With acceleration:** Data is materialized locally, enabling offline operation with eventual consistency when connectivity returns.

### 5. Multi-Tenant SaaS Platforms

SaaS applications serving thousands of tenants can't afford to hit a central data warehouse for every request. The per-query costs alone would be prohibitive.

**With acceleration:** Tenant-specific data subsets are accelerated locally, reducing both latency and cloud data warehouse costs.

### 6. Operational Data Serving

Business intelligence tools like Tableau, Looker, and Power BI can query warehouses directly—but response times suffer, and per-query costs add up. Many organizations build dedicated "semantic layers" or "metrics layers" to serve BI tools.

Data acceleration serves as this operational layer. BI tools query Spice over standard protocols (PostgreSQL wire protocol, ODBC/JDBC), getting sub-second response times without hitting the warehouse for every dashboard refresh.

## Beyond Performance: The Data Substrate

While latency reduction is the most visible benefit, data acceleration fundamentally changes your architecture. The acceleration layer becomes a **data substrate**—a standardized abstraction that decouples applications from underlying storage.

### Vendor Abstraction and Cloud Portability

Modern enterprises are increasingly concerned about cloud vendor lock-in. Heavy reliance on AWS-specific services (DynamoDB, Aurora, ElastiCache) or GCP/Azure equivalents creates non-negotiable pricing exposure and limits strategic optionality.

Data acceleration provides a critical abstraction layer:

- **Backend portability**: Your application queries the acceleration layer via standard SQL. The underlying storage—whether DynamoDB, Scylla, PostgreSQL, or S3—becomes an implementation detail you can change without rewriting application logic.
- **Cloud-agnostic interface**: Whether deployed on AWS, Azure, GCP, on-premise, or at the edge, the query interface remains identical. This provides leverage in hyperscaler negotiations.
- **Technology migration path**: Considering a migration from DynamoDB to ScyllaDB for cost reasons? The application layer doesn't need to know. Swap the backend connector, and queries continue working.

```yaml
# Today: DynamoDB backend
datasets:
  - from: dynamodb:user_sessions
    name: sessions
    acceleration:
      enabled: true
      engine: duckdb

# Tomorrow: Migrate to ScyllaDB—application code unchanged
datasets:
  - from: scylladb:user_sessions
    name: sessions
    acceleration:
      enabled: true
      engine: duckdb
```

### Resilience and Fault Tolerance

Data acceleration inherently improves system resilience:

- **Source outage isolation**: If your upstream database experiences an outage, accelerated queries continue serving from local data. Your dashboard doesn't go blank because Snowflake is having a bad day.
- **Graceful degradation**: Configure `on_zero_results: use_source` for fallback behavior. Accelerated data serves reads; federation handles edge cases.
- **Distributed systems out of the box**: Partitioning, replication, fault tolerance, and load balancing are handled by the acceleration layer—not custom code in every microservice.
- **Consistent failure handling**: A unified approach to resilience across services simplifies failure remediation and improves behavior during incidents.

### Infrastructure Cost Savings

Acceleration directly reduces infrastructure spend:

**Database load reduction**: High-read workloads against managed databases (DynamoDB, Aurora, RDS) are expensive. By serving reads from a local acceleration layer, you dramatically reduce database operations—and your monthly bill.

**Cache layer replacement**: Traditional caching with Redis/ElastiCache requires managing keys, types, serialization, and invalidation logic. Acceleration provides caching semantics with SQL query capabilities—reducing client-side engineering complexity and eliminating a separate infrastructure component.

**Search and analytics savings**: Instead of spinning up Spark/EMR clusters or building Firehose pipelines, accelerate data from S3/Iceberg/Parquet locally. You get the reliability and low cost of object storage with the query performance of a local engine.

### Engineering Efficiency

**Standardization on SQL and open formats**: Teams query federated data without learning vendor-specific syntax. Standard SQL works across all connectors. Open formats (Apache Iceberg, Parquet, Arrow) ensure interoperability and make onboarding easier—new engineers work with familiar technologies.

**Zero-ETL, zero-copy architecture**: Traditional approaches require ETL pipelines to move data between services. Acceleration creates a federated query mesh—less data movement, lower latency, reduced complexity, and fewer centralized points of failure.

**Reduced coordination overhead**: Without acceleration, every team builds their own caching, retry, and failover logic. With a shared acceleration layer, these distributed systems challenges are solved once and reused everywhere.

### Observability Built-In

Spice integrates natively with standard monitoring stacks (Prometheus, Datadog, OpenTelemetry):

- **Query metrics**: Latency percentiles, throughput, error rates per dataset
- **Refresh monitoring**: Ingestion rates, sync lag, failed refreshes
- **Cache analytics**: Hit/miss ratios reveal opportunities for configuration tuning
- **Source comparison**: Measure accelerated vs. federated query performance to quantify impact

This observability is available out of the box—no custom instrumentation required.

## How Data Acceleration Works in Spice

Spice.ai OSS implements data acceleration through its federation and materialization architecture. Here's how it works:

### Step 1: Connect to Your Data Sources

Spice connects to 30+ data sources—Snowflake, Databricks, PostgreSQL, S3, Delta Lake, and more—through its federated query engine.

```yaml
datasets:
  - from: databricks:analytics.events
    name: user_events
```

### Step 2: Enable Acceleration

Add an `acceleration` block to materialize data locally:

```yaml
datasets:
  - from: databricks:analytics.events
    name: user_events
    acceleration:
      enabled: true
      engine: duckdb        # or sqlite, postgres, arrow
      refresh_mode: append
      refresh_check_interval: 10s
```

### Step 3: Query Transparently

Your application queries Spice using standard SQL over Arrow Flight, PostgreSQL wire protocol, or HTTP. Spice routes queries to the local acceleration—your application doesn't know (or care) where the data lives.

```python
import spicepy

client = spicepy.Client()
df = client.query("SELECT * FROM user_events WHERE user_id = 12345")
```

### Refresh Modes: Keeping Data Fresh

The key to effective acceleration is keeping local data synchronized with the source. Spice supports four refresh strategies:

| Mode              | Behavior                                          | Best For                                  |
| ----------------- | ------------------------------------------------- | ----------------------------------------- |
| **Full**          | Replace entire dataset on each refresh            | Small, frequently-changing reference data |
| **Append**        | Add new rows based on a time column               | Time-series, logs, append-only data       |
| **Changes (CDC)** | Apply incremental changes via Change Data Capture | Transactional tables with updates/deletes |
| **Caching**       | Read-through cache keyed by query                 | API responses, search results             |

#### Full Refresh

Replaces the entire local dataset on each interval. Simple but only practical for smaller datasets.

```yaml
acceleration:
  refresh_mode: full
  refresh_check_interval: 1h
```

#### Append Refresh

For time-series and log data, Spice tracks a `time_column` and only fetches rows newer than the local maximum:

```yaml
time_column: created_at
acceleration:
  refresh_mode: append
  refresh_check_interval: 30s
```

Spice pushes the time filter down to the source, so only new data transfers over the network.

#### Change Data Capture (CDC)

For tables with updates and deletes, Spice integrates with Debezium to capture and apply incremental changes:

```yaml
datasets:
  - from: debezium:cdc_server
    name: orders
    acceleration:
      refresh_mode: changes
```

CDC ensures local data reflects the exact state of the source—including updates and deletes—without full table scans.

#### Caching Mode

For HTTP and API-backed datasets, caching mode stores results keyed by request parameters:

```yaml
acceleration:
  refresh_mode: caching
  caching_ttl: 5m
```

### Filtered Refresh: Accelerate Only What You Need

You rarely need an entire data lake table locally. Spice supports filtering at refresh time:

```yaml
acceleration:
  refresh_sql: |
    SELECT * FROM user_events 
    WHERE region = 'us-west' 
    AND created_at > now() - interval '7 days'
  refresh_data_window: 7d
```

Filters push down to the source—only matching data transfers over the network. This is especially powerful for:

- **Multi-tenant isolation**: Each deployment accelerates only its tenant's data
- **Time-windowed analytics**: Only recent data needs sub-millisecond access
- **Cost control**: Reduce data egress from cloud warehouses

### Zero-Result Fallback

What happens when a query needs data that isn't accelerated? Configure fallback behavior:

```yaml
acceleration:
  refresh_sql: SELECT * FROM events WHERE region = 'us-west'
  on_zero_results: use_source
```

With `on_zero_results: use_source`, queries returning zero results automatically fall back to the federated source. Users querying for `region = 'us-east'` transparently hit Databricks, while `us-west` queries are served locally.

### Retention Policies: Managing Local Storage

Accelerated data doesn't need to live forever. Configure automatic eviction:

```yaml
time_column: created_at
acceleration:
  retention_check_enabled: true
  retention_period: 30d
  retention_check_interval: 1h
```

Spice automatically evicts rows older than the retention period, keeping local storage bounded.

### Fast Cold Starts with Snapshots

Waiting for a full refresh on startup defeats the purpose of acceleration. Spice supports snapshots—pre-built database files stored in object storage:

```yaml
snapshots:
  enabled: true
  store: s3://my-bucket/spice-snapshots/

datasets:
  - from: databricks:analytics.events
    name: user_events
    acceleration:
      enabled: true
      snapshots: enabled
```

On startup, Spice downloads the latest snapshot and immediately begins serving queries. The first refresh only fetches data newer than the snapshot.

## Acceleration Engines

Spice supports multiple acceleration engines, each with different trade-offs:

| Engine         | Storage     | Best For                                      |
| -------------- | ----------- | --------------------------------------------- |
| **Arrow**      | In-memory   | Fastest queries, volatile (lost on restart)   |
| **DuckDB**     | File-based  | Analytical queries, persistent, columnar      |
| **SQLite**     | File-based  | Transactional patterns, broad compatibility   |
| **PostgreSQL** | External DB | Existing Postgres infrastructure, replication |

### Arrow (In-Memory)

The fastest option—data lives entirely in memory as Apache Arrow arrays:

```yaml
acceleration:
  engine: arrow
  mode: memory
```

Ideal for small-to-medium datasets where restart time isn't critical. Query performance is exceptional because there's no serialization overhead.

### DuckDB (Recommended Default)

An embedded analytical database optimized for OLAP workloads:

```yaml
acceleration:
  engine: duckdb
  mode: file
  duckdb_file: /data/accelerated.duckdb
```

DuckDB's columnar storage and vectorized execution make it ideal for analytics. File-based mode persists data across restarts.

### SQLite

The ubiquitous embedded database, best for operational/transactional query patterns:

```yaml
acceleration:
  engine: sqlite
  mode: file
  sqlite_file: /data/accelerated.db
```

### PostgreSQL

Use an external PostgreSQL instance when you need the full power of Postgres or want to share accelerated data across Spice instances:

```yaml
acceleration:
  engine: postgres
  params:
    pg_connection_string: postgresql://user:pass@host:5432/db
```

## Architecture: How It Fits Together

```text
┌─────────────────────────────────────────────────────────────┐
│                      Your Application                        │
│         (Arrow Flight / PostgreSQL / HTTP / GraphQL)         │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                         Spice Runtime                        │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                    Query Engine                          ││
│  │              (Apache DataFusion)                         ││
│  └──────────────────────┬──────────────────────────────────┘│
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────────┐│
│  │              Acceleration Layer                          ││
│  │   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────┐   ││
│  │   │ DuckDB  │ │ SQLite  │ │  Arrow  │ │ PostgreSQL  │   ││
│  │   └─────────┘ └─────────┘ └─────────┘ └─────────────┘   ││
│  └──────────────────────┬──────────────────────────────────┘│
│                         │ (Background Refresh)               │
│  ┌──────────────────────▼──────────────────────────────────┐│
│  │              Federation Layer                            ││
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌───────┐  ││
│  │  │Snowflake│ │ S3     │ │Postgres│ │Databricks│ │ ... │  ││
│  │  └────────┘ └────────┘ └────────┘ └────────┘ └───────┘  ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

1. **Federation Layer** — Connects to remote data sources
2. **Acceleration Layer** — Materializes data locally in embedded databases
3. **Query Engine** — Routes queries to accelerated data; falls back to federation when needed
4. **Your Application** — Queries via standard protocols, unaware of the underlying complexity

## Under the Hood: How It Actually Works

For engineers who want to understand the internals, here's how Spice implements data acceleration at the runtime level.

### Query Execution Pipeline

When your application submits a SQL query, it flows through several layers:

```text
SQL Query
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Query Parsing (Apache DataFusion)                       │
│     • SQL → LogicalPlan (abstract syntax tree)              │
│     • Schema validation against registered tables           │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Analyzer Rules                                          │
│     • Federation analysis (can this push down?)             │
│     • Index optimization (use secondary indexes?)           │
│     • Filter pushdown determination                         │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Physical Planning                                       │
│     • LogicalPlan → ExecutionPlan (concrete operators)      │
│     • Partition assignment, parallelism decisions           │
│     • FederatedPlanner for remote query generation          │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Execution                                               │
│     • Execute against AcceleratedTable.scan()               │
│     • Stream RecordBatches back to client                   │
│     • Handle zero-result fallback if configured             │
└─────────────────────────────────────────────────────────────┘
```

Spice is built on [Apache DataFusion](https://datafusion.apache.org/), a Rust-native query engine. DataFusion provides the SQL parser, logical planner, optimizer, and physical execution framework. Spice extends it with custom `TableProvider` implementations that know how to route between accelerated and federated data.

### The AcceleratedTable: Core Abstraction

The heart of acceleration is the `AcceleratedTable` struct, which implements DataFusion's `TableProvider` trait. When a query hits an accelerated dataset, the `scan()` method decides where to execute:

```rust
// Simplified from crates/runtime/src/accelerated_table/mod.rs
async fn scan(&self, state, projection, filters, limit) -> ExecutionPlan {
    // Check if acceleration is still loading
    if !self.refresher().initial_load_completed() {
        match self.ready_state {
            ReadyState::OnLoad => {
                // Return error - data not ready yet
                return Err(acceleration_not_ready_error());
            }
            ReadyState::OnRegistration => {
                // Fall back to federated source during initial load
                return self.federated.scan(state, projection, filters, limit);
            }
        }
    }
    
    // Query the local accelerator
    let input = self.accelerator.scan(state, projection, filters, limit).await?;
    
    // Wrap with fallback logic if configured
    match self.zero_results_action {
        ZeroResultsAction::ReturnEmpty => input,
        ZeroResultsAction::UseSource => {
            FallbackOnZeroResultsScanExec::new(input, federated_provider)
        }
    }
}
```

Key design decisions:

1. **Ready state handling**: During initial load, queries can either error (`on_load`) or transparently fall back to federation (`on_registration`)
2. **Deferred execution**: The fallback `TableProvider` is constructed lazily—only if needed
3. **Schema preservation**: A `SchemaCastScanExec` wrapper ensures the output schema matches regardless of which path executed

### Zero-Result Fallback: Streaming Decision

The `FallbackOnZeroResultsScanExec` is particularly clever. It doesn't buffer the entire result to check if it's empty—that would defeat the purpose of streaming. Instead:

```rust
// Simplified from crates/runtime-datafusion/src/execution_plan/fallback_on_zero_results.rs
fn execute(&self, partition, context) -> SendableRecordBatchStream {
    let input_stream = self.input.execute(partition, context)?;
    
    stream::once(async move {
        // Try to get the first batch from accelerated data
        match input_stream.next().await {
            Some(Ok(batch)) if batch.num_rows() > 0 => {
                // Got data! Return it and continue streaming
                return chain(once(batch), input_stream);
            }
            None | Some(Ok(batch)) if batch.num_rows() == 0 => {
                // Empty! Fall back to federated source
                let fallback_plan = federated_provider.scan(...).await?;
                return fallback_plan.execute(partition, context);
            }
        }
    })
}
```

The decision happens on the *first batch*. If accelerated data exists, streaming continues from the accelerator. If empty, it transparently switches to federation—the caller never knows.

### Federation: Pushing Queries to Remote Sources

For federated queries (non-accelerated or fallback), Spice uses `datafusion-federation` to push SQL to remote sources. The key insight: instead of pulling all data locally and filtering, generate SQL that the remote database can execute.

```rust
// Each connector implements SQLExecutor
impl SQLExecutor for SnowflakeTable {
    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SnowflakeDialect {})
    }
    
    fn execute(&self, query: &str, schema: SchemaRef) -> SendableRecordBatchStream {
        // Send SQL directly to Snowflake, stream Arrow results back
        query_to_stream(self.client.clone(), query.to_string())
    }
}
```

The `FederationAnalyzer` walks the logical plan and identifies sub-trees that can be pushed to the same remote source. For example:

```sql
SELECT * FROM snowflake_table WHERE region = 'us-west' AND date > '2024-01-01'
```

Instead of pulling all rows and filtering locally, Spice generates:

```sql
-- Sent to Snowflake
SELECT * FROM source_table WHERE region = 'us-west' AND date > '2024-01-01'
```

The `supports_filters_pushdown()` trait method lets each connector declare which filter types it can handle natively.

### Refresh Task: Background Synchronization

The `RefreshTask` runs in the background, keeping accelerated data synchronized with the source:

```rust
// Simplified from crates/runtime/src/accelerated_table/refresh_task.rs
async fn run_once(&self, refresh: &Refresh) -> Result<()> {
    let data_update = match refresh.mode {
        RefreshMode::Full => {
            // Fetch entire dataset, overwrite local
            self.get_full_update(refresh).await?
        }
        RefreshMode::Append => {
            // Fetch only rows newer than max(time_column)
            self.get_incremental_append_update(refresh).await?
        }
        RefreshMode::Changes => {
            // CDC stream handled separately
            unreachable!()
        }
        RefreshMode::Caching => {
            // Refresh stale cache entries based on TTL
            return self.refresh_stale_cached_rows(refresh).await;
        }
    };
    
    // Stream data into accelerator via INSERT/OVERWRITE
    self.write_streaming_data_update(data_update).await
}
```

For append mode, the task queries the accelerator for `MAX(time_column)`, then issues a filtered query to the source:

```sql
SELECT * FROM source_table WHERE created_at > '2024-01-15T10:30:00Z'
```

This filter is pushed down to the remote source—only new data transfers over the network.

### Caching Mode: HTTP-Aware Acceleration

Caching mode solves a different problem: accelerating API responses where request parameters act as cache keys.

```rust
// Simplified from crates/runtime/src/accelerated_table/caching.rs
enum CacheFreshness {
    Fresh,   // Within max_age TTL
    Stale,   // Past max_age but within stale_while_revalidate
    Expired, // Past both TTLs - cache miss
}

fn check_cache_freshness(batches: &[RecordBatch], max_age: Duration, swr: Option<Duration>) {
    // Check fetched_at timestamp column against TTLs
    let fetched_at = get_timestamp_from_batch(batches)?;
    let age = now() - fetched_at;
    
    if age < max_age { CacheFreshness::Fresh }
    else if swr.is_some() && age < max_age + swr { CacheFreshness::Stale }
    else { CacheFreshness::Expired }
}
```

The `CachingAccelerationScanExec` implements stale-while-revalidate semantics:

1. **Fresh**: Return cached data immediately
2. **Stale**: Return cached data *and* trigger background refresh
3. **Expired**: Fetch from source, cache, return

This pattern is common in CDNs and HTTP caches—Spice brings it to SQL.

### Interesting Technical Challenges

**Schema Evolution**: What happens if the source schema changes between refreshes? The `FederatedTable` compares schemas against the acceleration checkpoint. If they diverge, it creates a `DeferredTableProvider` that keeps retrying until the source is available with the new schema—while continuing to serve stale data.

**Concurrent Refresh Control**: Multiple accelerated tables might refresh simultaneously, overwhelming the source. A `Semaphore` limits parallel refreshes, and refresh jitter (random delay) prevents thundering herd on restart.

**Memory Pressure**: For large datasets, Spice monitors memory usage during refresh and can pause if thresholds are exceeded. The `ResourceMonitor` checks after each batch:

```rust
if let Some(ref monitor) = resource_monitor {
    monitor.check_memory_usage(&dataset_name);
}
```

**Retry with Fibonacci Backoff**: Transient failures (network blips, warehouse cold starts) trigger automatic retry with Fibonacci backoff—1s, 1s, 2s, 3s, 5s, 8s...—avoiding both thundering herd and excessive delay.

### Why Rust and DataFusion?

Spice is written in Rust for several reasons:

1. **Predictable latency**: No garbage collection pauses
2. **Memory safety**: Arrow's zero-copy semantics require careful memory management
3. **DataFusion ecosystem**: Native integration with Arrow, Parquet, and the broader Arrow ecosystem
4. **Embedding**: DuckDB and SQLite embed naturally as Rust libraries

DataFusion specifically provides:

- SQL parsing and planning
- Vectorized execution (SIMD-accelerated on arm64/amd64)
- Extensible `TableProvider` trait for custom data sources
- Arrow-native data representation (no serialization between components)

## Getting Started

Install Spice and create a `spicepod.yml`:

```yaml
version: v1beta1
kind: Spicepod
name: my-app

datasets:
  - from: s3://my-bucket/events/
    name: events
    params:
      file_format: parquet
    time_column: event_time
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: append
      refresh_check_interval: 1m
```

Run Spice:

```bash
spice run
```

Query your accelerated data:

```bash
spice sql
sql> SELECT count(*) FROM events WHERE event_time > now() - interval '1 hour';
```

The first query waits for the initial refresh. Subsequent queries execute in milliseconds against local DuckDB.

## Conclusion

Data acceleration bridges the gap between where data lives (cloud data lakes and warehouses) and where it needs to be for high-performance applications (locally, co-located with your application). By intelligently materializing and synchronizing data, you get:

- **100x faster queries** — Milliseconds instead of seconds
- **Reduced cloud costs** — Fewer database operations, less data egress, simplified infrastructure
- **Vendor optionality** — Decouple from cloud-specific services; migrate backends without rewriting applications
- **Resilience** — Local data survives source outages; distributed systems complexity handled for you
- **Engineering velocity** — Standardized SQL interfaces, zero-ETL architecture, built-in observability
- **Simplified architecture** — One query interface, automatic routing, consistent patterns across services

Whether you're building real-time dashboards, AI applications, or edge systems—or modernizing enterprise infrastructure to reduce cloud lock-in—data acceleration turns sluggish data access into a competitive advantage.

---

*Ready to accelerate your data? [Get started with Spice.ai OSS](https://github.com/spiceai/spiceai) — it's open source and free.*

