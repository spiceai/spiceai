# Operational Data Lakehouse: Serving Real-Time Data and AI from Object Storage

*How to retain the scalability of object storage and governance of open table formats while enabling sub-second query performance for applications.*

> **Part of the Spice Use Case Series**: This article is one of nine deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [The Data Lakehouse Challenge](#the-data-lakehouse-challenge)
3. [Why Object Storage + Open Formats Matter](#why-object-storage--open-formats-matter)
4. [How Spice Enables Operational Lakehouses](#how-spice-enables-operational-lakehouses)
5. [Architecture Patterns](#architecture-patterns)
6. [Real-World Use Cases](#real-world-use-cases)
7. [Getting Started](#getting-started)
8. [Conclusion](#conclusion)

---

## Introduction

Data lakehouses promise the best of both worlds: the scalability and cost-efficiency of data lakes with the governance and query performance of data warehouses. Built on object storage (S3, Azure Blob, GCS) and open table formats (Iceberg, Delta Lake, Hudi), lakehouses have become the foundation for modern data platforms.

But there's a gap: while lakehouses excel at analytical workloads, they struggle with the operational demands of real-time applications and AI agents that need sub-second query latency.

**Operational data lakehouses** bridge this gap—bringing application-tier performance to lakehouse data without sacrificing the scalability, governance, and cost benefits that made lakehouses attractive in the first place.

---

## The Data Lakehouse Challenge

Modern lakehouse architectures face a fundamental tension:

### The Scalability Side

Object storage provides:

- **Infinite scale**: Petabytes without infrastructure management
- **Cost efficiency**: Pay only for storage used
- **Durability**: 11 9s of durability (99.999999999%)
- **Decoupled compute**: Any engine can query the data

Open table formats add:

- **ACID transactions**: Consistent reads and writes
- **Time travel**: Query historical snapshots
- **Schema evolution**: Safely modify schemas
- **Partitioning**: Efficient data pruning

### The Performance Reality

Direct queries to object storage are slow:

```text
User Request → Application → S3 Query → 2-15 second response
                                          ↑
                                     Network latency +
                                     Object listing +
                                     Parquet scan
```

For analytical dashboards refreshing hourly, this is acceptable. For applications serving users in real-time, it's not.

### Traditional Solutions (and Their Drawbacks)

| Solution                    | Drawback                                                  |
| --------------------------- | --------------------------------------------------------- |
| **Caching layers**          | Data staleness, cache invalidation complexity             |
| **Data warehouses**         | Data duplication, ETL pipelines, governance fragmentation |
| **Specialized serving DBs** | More systems to manage, sync challenges                   |
| **Larger clusters**         | Cost explosion, still limited by object storage latency   |

---

## Why Object Storage + Open Formats Matter

Before solving the performance problem, let's understand why lakehouses are worth optimizing:

### Governance Benefits

Open table formats like Apache Iceberg provide:

```text
┌─────────────────────────────────────────────────────┐
│                 Iceberg Table                        │
├─────────────────────────────────────────────────────┤
│ • Schema enforcement and evolution                   │
│ • Partition pruning metadata                         │
│ • Transaction isolation (snapshot isolation)         │
│ • Audit trail (every change recorded)                │
│ • Time travel queries (SELECT AS OF TIMESTAMP)       │
└─────────────────────────────────────────────────────┘
```

### Cost Structure

Object storage economics favor lakehouses:

| Tier                 | Cost (per GB/month) | Use Case        |
| -------------------- | ------------------- | --------------- |
| S3 Standard          | $0.023              | Active data     |
| S3 Infrequent Access | $0.0125             | Historical data |
| S3 Glacier           | $0.004              | Archive         |

Compare to data warehouses at $20-40/TB/month—10-100x more expensive.

### Interoperability

Open formats prevent vendor lock-in:

```text
                     ┌─────────────────┐
                     │  Iceberg Table  │
                     │  (S3 + Parquet) │
                     └────────┬────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
   ┌─────────┐          ┌─────────┐          ┌─────────┐
   │  Spark  │          │  Trino  │          │  Spice  │
   └─────────┘          └─────────┘          └─────────┘
```

Any engine can read the data. No proprietary formats. No migration nightmares.

---

## How Spice Enables Operational Lakehouses

Spice bridges the gap between lakehouse scale and application-tier performance through intelligent data acceleration:

### The Core Concept: Materialized Working Sets

Instead of querying object storage directly, Spice maintains local, accelerated copies of the data your application actually needs:

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Data Lakehouse                            │
│                     (S3 + Iceberg/Delta)                         │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ Orders (5TB) │  │ Products(2TB)│  │ Customers(1TB)│           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
└─────────────────────────────────────────────────────────────────┘
                              │
                    Materialization
                    (filtered + accelerated)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Spice Runtime                             │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Accelerated Working Sets                                  │   │
│  │                                                           │   │
│  │ • Recent Orders (last 90 days): 50GB in DuckDB           │   │
│  │ • Active Products: 100MB in Arrow                        │   │
│  │ • Customer Profiles: 2GB in SQLite                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Query Latency: 10-100ms (was 2-15 seconds)                     │
└─────────────────────────────────────────────────────────────────┘
```

### Key Capabilities

#### 1. Transparent Acceleration

Applications query Spice; Spice handles acceleration transparently:

```yaml
datasets:
  - name: orders
    from: iceberg:s3://datalake/warehouse/orders
    acceleration:
      enabled: true
      engine: duckdb
      refresh_schedule: "0 */5 * * * *"  # Every 5 minutes
      refresh_sql: >
        SELECT * FROM orders 
        WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
```

#### 2. Open Format Support

Native connectors for Iceberg, Delta Lake, and direct Parquet:

```yaml
datasets:
  # Apache Iceberg
  - name: transactions
    from: iceberg:s3://warehouse/transactions

  # Delta Lake
  - name: products  
    from: delta_lake:s3://warehouse/products

  # Raw Parquet on S3
  - name: logs
    from: s3://logs/application/
    params:
      file_format: parquet
```

#### 3. Multi-Engine Acceleration

Choose the right engine for each workload:

| Engine         | Best For               | Characteristics                 |
| -------------- | ---------------------- | ------------------------------- |
| **Arrow**      | In-memory analytics    | Fastest, volatile               |
| **DuckDB**     | OLAP workloads         | Analytical queries, compression |
| **SQLite**     | OLTP patterns          | Point lookups, transactions     |
| **PostgreSQL** | Full SQL compatibility | Complex queries, joins          |
| **Cayenne**    | Large datasets         | Vortex compression + SQLite     |

#### 4. Fast Cold Starts with Snapshots

Bootstrap acceleration from S3 snapshots instead of re-querying sources:

```yaml
datasets:
  - name: customer_profiles
    from: iceberg:s3://warehouse/customers
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      snapshots:
        enabled: true
        path: s3://acceleration-snapshots/customers/
```

Cold start: seconds instead of minutes.

#### 5. Iceberg Writes

Write back to Iceberg tables using standard SQL:

```sql
-- Aggregate and write to Iceberg
INSERT INTO iceberg_summary
SELECT 
    date_trunc('hour', event_time) as hour,
    event_type,
    COUNT(*) as event_count
FROM events
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY 1, 2;
```

---

## Architecture Patterns

### Pattern 1: Sidecar Acceleration

Deploy Spice alongside each application:

```text
┌─────────────────────────────────────────────────┐
│               Kubernetes Cluster                 │
├─────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────┐ │
│  │            Application Pod                  │ │
│  │  ┌─────────────┐     ┌─────────────────┐   │ │
│  │  │   App       │────▶│  Spice Sidecar  │   │ │
│  │  │ Container   │     │  (accelerated)  │   │ │
│  │  └─────────────┘     └────────┬────────┘   │ │
│  └───────────────────────────────┼────────────┘ │
│                                  │              │
│                          Refresh Data           │
│                                  │              │
│                                  ▼              │
│                     ┌────────────────────┐      │
│                     │    S3 Lakehouse    │      │
│                     └────────────────────┘      │
└─────────────────────────────────────────────────┘
```

**Benefits**:

- Co-located data reduces network latency
- Isolation between applications
- Independent scaling

### Pattern 2: Shared Acceleration Layer

Centralized Spice cluster serving multiple applications:

```text
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │  App 1  │  │  App 2  │  │  App 3  │  │Dashboard│            │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘            │
│       │            │            │            │                  │
│       └────────────┴──────┬─────┴────────────┘                  │
│                           │                                      │
│                           ▼                                      │
│               ┌───────────────────────┐                         │
│               │    Spice Cluster      │                         │
│               │  (shared acceleration)│                         │
│               └───────────┬───────────┘                         │
│                           │                                      │
│                           ▼                                      │
│               ┌───────────────────────┐                         │
│               │    S3 Lakehouse       │                         │
│               └───────────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits**:

- Shared acceleration reduces duplication
- Centralized management
- Cost efficiency

### Pattern 3: Tiered Acceleration

Different acceleration strategies by data temperature:

```yaml
datasets:
  # Hot data: in-memory for fastest access
  - name: realtime_metrics
    from: delta_lake:s3://warehouse/metrics
    acceleration:
      enabled: true
      engine: arrow  # In-memory
      refresh_schedule: "*/30 * * * * *"  # Every 30 seconds
      refresh_sql: >
        SELECT * FROM realtime_metrics
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'

  # Warm data: DuckDB for analytical queries
  - name: daily_aggregates
    from: iceberg:s3://warehouse/daily_aggs
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_schedule: "0 0 * * * *"  # Hourly

  # Cold data: federated queries to source
  - name: historical_data
    from: iceberg:s3://warehouse/history
    # No acceleration - query lakehouse directly
```

---

## Real-World Use Cases

### Real-Time Analytics Dashboard

Serve sub-second analytics from lakehouse data:

```yaml
datasets:
  - name: sales_metrics
    from: delta_lake:s3://warehouse/sales
    acceleration:
      enabled: true
      engine: duckdb
      refresh_schedule: "*/5 * * * *"  # Every 5 minutes
      refresh_sql: >
        SELECT 
          date_trunc('hour', sale_time) as hour,
          region,
          product_category,
          SUM(revenue) as revenue,
          COUNT(*) as transactions
        FROM sales
        WHERE sale_time >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY 1, 2, 3
```

```sql
-- Dashboard query: now 50ms instead of 8 seconds
SELECT 
    hour,
    region,
    SUM(revenue) as total_revenue
FROM sales_metrics
WHERE hour >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY hour, region
ORDER BY hour DESC;
```

### AI Feature Serving

Serve ML features from lakehouse to models:

```yaml
datasets:
  - name: user_features
    from: iceberg:s3://warehouse/user_features
    acceleration:
      enabled: true
      engine: sqlite  # Fast point lookups
      mode: file
      refresh_schedule: "0 */15 * * * *"  # Every 15 minutes
```

```sql
-- Feature lookup for ML inference: <10ms
SELECT 
    user_id,
    avg_order_value,
    days_since_last_order,
    lifetime_value_segment
FROM user_features
WHERE user_id = '12345';
```

### Event-Driven Applications

Accelerate recent events while maintaining full history in lakehouse:

```yaml
datasets:
  - name: events
    from: delta_lake:s3://warehouse/events
    acceleration:
      enabled: true
      engine: duckdb
      refresh_schedule: "*/2 * * * *"  # Every 2 minutes
      refresh_sql: >
        SELECT * FROM events
        WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
```

### Hybrid Query Patterns

Combine accelerated recent data with federated historical queries:

```sql
-- Fast path: query accelerated data
SELECT * FROM orders 
WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
  AND customer_id = 'cust-123';

-- Slow path: federated query for historical data
SELECT * FROM orders_history
WHERE order_date < CURRENT_DATE - INTERVAL '90 days'
  AND customer_id = 'cust-123';
```

---

## Getting Started

### 1. Connect to Your Lakehouse

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: operational-lakehouse

catalogs:
  - name: lakehouse
    from: iceberg:s3://my-warehouse/
    params:
      aws_region: us-east-1
      aws_access_key_id: ${secrets:AWS_ACCESS_KEY_ID}
      aws_secret_access_key: ${secrets:AWS_SECRET_ACCESS_KEY}
```

### 2. Define Accelerated Datasets

```yaml
datasets:
  # Accelerate frequently-accessed tables
  - name: products
    from: lakehouse.catalog.products
    acceleration:
      enabled: true
      engine: duckdb
      refresh_schedule: "0 */10 * * * *"

  # Filter to working set
  - name: recent_orders
    from: lakehouse.catalog.orders
    acceleration:
      enabled: true
      engine: arrow
      refresh_sql: >
        SELECT * FROM orders 
        WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
      refresh_schedule: "*/5 * * * *"
```

### 3. Enable Snapshots for Fast Recovery

```yaml
datasets:
  - name: customer_profiles
    from: lakehouse.catalog.customers
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      snapshots:
        enabled: true
        path: s3://spice-snapshots/customers/
```

### 4. Start Querying

```bash
# Start Spice
spiced

# Query with sub-second latency
spice sql "SELECT * FROM recent_orders WHERE customer_id = '12345'"
```

---

## Conclusion

Operational data lakehouses deliver on the promise of unified, scalable data platforms without sacrificing application performance. By intelligently accelerating working sets of data close to applications, organizations can:

- **Preserve governance**: All data remains in the lakehouse with full lineage
- **Maintain cost efficiency**: Pay for object storage, not expensive compute tiers
- **Enable real-time applications**: Sub-second queries on lakehouse data
- **Avoid data duplication**: Acceleration is transparent, not a separate copy

Spice makes this architecture accessible with simple YAML configuration, support for major lakehouse formats (Iceberg, Delta Lake), and flexible acceleration engines for different workload patterns.

---

## Related Articles in This Series

- **[Caching](caching-explained.md)**: Deep-dive into caching patterns and strategies for data applications
- **[SQL Federation](sql-federation-explained.md)**: Query lakehouse data alongside transactional databases without ETL
- **[Application Search](application-search-explained.md)**: Enable search over lakehouse data with hybrid vector and text search
- **[RAG (Retrieval-Augmented Generation)](rag-explained.md)**: Ground AI responses in lakehouse data
- **[LLM Inference](llm-inference-explained.md)**: Analyze and enrich lakehouse data with AI

---

## Further Reading

- [Apache Iceberg Data Connector](https://spiceai.org/docs/components/data-connectors/iceberg)
- [Delta Lake Data Connector](https://spiceai.org/docs/components/data-connectors/delta-lake)
- [Data Acceleration Documentation](https://spiceai.org/docs/features/data-acceleration)
- [Acceleration Snapshots Guide](https://spiceai.org/docs/features/snapshots)

