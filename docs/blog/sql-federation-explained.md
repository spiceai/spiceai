# SQL Federation: Querying Across Any Database, Data Warehouse, or Data Lake

*How to execute unified SQL queries across heterogeneous data sources with intelligent query push-down.*

> **Part of the Spice Use Case Series**: This article is one of nine deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [The Data Fragmentation Problem](#the-data-fragmentation-problem)
3. [What is SQL Federation?](#what-is-sql-federation)
4. [How Spice Implements Federation](#how-spice-implements-federation)
5. [Query Push-Down: The Key to Performance](#query-push-down-the-key-to-performance)
6. [Federation Patterns](#federation-patterns)
7. [Real-World Use Cases](#real-world-use-cases)
8. [Getting Started](#getting-started)
9. [Conclusion](#conclusion)

---

## Introduction

Enterprise data lives in many places: PostgreSQL for transactions, Snowflake for analytics, S3 for data lakes, Salesforce for CRM, and dozens of other systems. Each has its own query language, connection protocol, and performance characteristics.

**SQL Federation** solves this by providing a unified SQL interface that routes queries to the appropriate systems, pushing computation close to data when possible, and seamlessly combining results.

Instead of learning five different query languages or building complex ETL pipelines, developers write SQL once and query everywhere.

---

## The History of Data Federation

The challenge of querying distributed data isn't new. The industry has approached it in waves:

### Phase 1: Data Warehouses (1990s-2000s)

The original solution: copy everything into one place.

```text
Source A ─┐
Source B ─┼─→ ETL ─→ Data Warehouse ─→ Queries
Source C ─┘
```

**Trade-offs**:

- ✓ Single query interface
- ✗ Data staleness (hours to days)
- ✗ Storage duplication costs
- ✗ ETL pipeline maintenance

### Phase 2: Federated Database Systems (2000s)

Academic systems like IBM's Garlic and commercial products attempted virtual integration:

```sql
-- Conceptual federated query
SELECT * FROM oracle.customers c
JOIN db2.orders o ON c.id = o.customer_id
```

**Trade-offs**:

- ✓ No data movement
- ✗ Poor performance (pulled all data centrally)
- ✗ Limited SQL dialect translation
- ✗ Complex deployment

### Phase 3: Data Virtualization (2010s)

Products like Denodo and Dremio improved on federation with caching and optimization:

```text
                        ┌─────────────────┐
    Query ─────────────→│ Virtualization  │
                        │     Layer       │
                        └────────┬────────┘
                                 │
           ┌─────────────────────┼─────────────────────┐
           ▼                     ▼                     ▼
    ┌───────────┐         ┌───────────┐         ┌───────────┐
    │  Oracle   │         │ Hadoop    │         │ Salesforce│
    └───────────┘         └───────────┘         └───────────┘
```

**Trade-offs**:

- ✓ Better performance with caching
- ✗ Expensive licensing
- ✗ Often became another data silo

### Phase 4: Modern Federation with Query Push-Down (2020s)

The key innovation: **push computation to data sources** rather than pulling data centrally.

```text
┌──────────────────────────────────────────────────────────────┐
│  SELECT c.name, SUM(o.total)                                  │
│  FROM customers c JOIN orders o ON c.id = o.customer_id      │
│  WHERE o.date > '2024-01-01'                                  │
│  GROUP BY c.name                                              │
└────────────────────────────┬─────────────────────────────────┘
                             │
     ┌───────────────────────┴────────────────────────┐
     │                Push-Down Optimizer             │
     └────────────────┬──────────────┬────────────────┘
                      │              │
                      ▼              ▼
  ┌───────────────────────┐  ┌────────────────────────────┐
  │ SELECT * FROM customers│  │ SELECT customer_id,        │
  │ (source-native)        │  │        SUM(total)          │
  └───────────────────────┘  │ FROM orders                 │
                             │ WHERE date > '2024-01-01'   │
                             │ GROUP BY customer_id        │
                             │ (aggregation pushed down!)  │
                             └────────────────────────────┘
```

By pushing filters, projections, and even aggregations to source systems, modern federation minimizes data movement and leverages each system's native optimization.

---

## The Data Fragmentation Problem

Modern enterprises face significant data fragmentation:

### The Typical Data Landscape

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Enterprise Data Sources                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Transactional           Analytical          Object Storage      │
│  ┌──────────────┐       ┌──────────────┐    ┌──────────────┐    │
│  │ PostgreSQL   │       │ Snowflake    │    │ S3/Parquet   │    │
│  │ MySQL        │       │ BigQuery     │    │ Delta Lake   │    │
│  │ SQL Server   │       │ Redshift     │    │ Iceberg      │    │
│  └──────────────┘       └──────────────┘    └──────────────┘    │
│                                                                  │
│  SaaS/APIs              Streaming           NoSQL               │
│  ┌──────────────┐       ┌──────────────┐    ┌──────────────┐    │
│  │ Salesforce   │       │ Kafka        │    │ MongoDB      │    │
│  │ HubSpot      │       │ Kinesis      │    │ DynamoDB     │    │
│  │ Zendesk      │       │ Debezium     │    │ ClickHouse   │    │
│  └──────────────┘       └──────────────┘    └──────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Pain Points

#### 1. Multiple Query Languages

```sql
-- PostgreSQL
SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days';

-- Snowflake
SELECT * FROM orders WHERE created_at > DATEADD(day, -7, CURRENT_TIMESTAMP());

-- BigQuery
SELECT * FROM orders WHERE created_at > DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
```

#### 2. Manual Data Integration

```text
Application needs:
- Customer data from Salesforce
- Orders from PostgreSQL
- Clickstream from S3

Traditional approach:
1. Query Salesforce API → Process JSON
2. Query PostgreSQL → Process ResultSet
3. Query S3 → Process Parquet
4. Join in application code (slow, error-prone)
```

#### 3. ETL Complexity

```text
S3 → ETL → Warehouse → BI Tool
          ↓
   Latency: hours to days
   Cost: compute + storage duplication
   Maintenance: pipeline orchestration
```

---

## What is SQL Federation?

SQL Federation provides a single SQL interface that queries multiple data sources:

```sql
-- One query, multiple sources
SELECT 
    c.name,
    c.email,
    o.order_total,
    o.order_date,
    p.product_name
FROM salesforce.customers c           -- Salesforce CRM
JOIN postgres.orders o ON c.id = o.customer_id  -- PostgreSQL
JOIN s3.products p ON o.product_id = p.id       -- S3/Parquet
WHERE o.order_date > '2024-01-01';
```

### How Federation Works

```text
┌──────────────────────────────────────────────────────────────┐
│                      Federated Query                          │
│  SELECT c.name, o.total FROM customers c JOIN orders o...    │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                    Federation Engine                          │
│                                                               │
│  1. Parse unified SQL                                         │
│  2. Identify source tables                                    │
│  3. Generate source-specific queries                          │
│  4. Push down filters/projections                             │
│  5. Execute in parallel                                       │
│  6. Combine results                                           │
└──────────┬─────────────────┬─────────────────┬───────────────┘
           │                 │                 │
           ▼                 ▼                 ▼
    ┌───────────┐     ┌───────────┐     ┌───────────┐
    │ Salesforce│     │ PostgreSQL│     │    S3     │
    │           │     │           │     │  Parquet  │
    └───────────┘     └───────────┘     └───────────┘
```

---

## How Spice Implements Federation

Spice's federation is built on Apache DataFusion with custom connectors for each data source:

### Unified Catalog

All data sources appear as tables in a unified schema:

```yaml
# spicepod.yaml
datasets:
  # PostgreSQL tables
  - name: orders
    from: postgres:ecommerce.orders
    
  # Snowflake analytics
  - name: customer_analytics
    from: snowflake:analytics.customer_metrics
    
  # S3 data lake
  - name: events
    from: s3://data-lake/events/
    params:
      file_format: parquet
      
  # Salesforce CRM
  - name: contacts
    from: salesforce:contacts
```

Query them as if they're in one database:

```sql
SELECT 
    c.name,
    ca.lifetime_value,
    COUNT(o.id) as order_count
FROM contacts c
JOIN customer_analytics ca ON c.id = ca.customer_id
JOIN orders o ON c.id = o.customer_id
GROUP BY c.name, ca.lifetime_value;
```

### Cross-Source Joins

Spice intelligently handles joins across different systems:

```sql
-- Join Salesforce contacts with S3 event data
SELECT 
    c.name,
    COUNT(e.event_id) as events
FROM salesforce.contacts c
JOIN s3.events e ON c.id = e.user_id
WHERE e.event_date > '2024-01-01'
GROUP BY c.name;
```

The engine:

1. Pushes filters to each source
2. Retrieves minimal required columns
3. Performs join locally using Arrow
4. Returns unified results

### Spice Federation Internals

Under the hood, Spice's federation is built on several key abstractions:

**The `DataConnector` Trait**: Each data source implements this trait, providing methods to:

- Connect and authenticate
- Return a `TableProvider` for each table
- Declare which operations can be pushed down

**The `FederatedTable` Enum**: Wraps table providers with two variants:

```rust
pub enum FederatedTable {
    // Table provider available immediately
    Immediate(Arc<dyn TableProvider>),
    
    // Keep retrying until source becomes available
    // (serves stale data from acceleration in the meantime)
    Deferred(DeferredTableProvider),
}
```

This enables graceful degradation—if a source goes down, Spice continues serving cached data while retrying in the background.

**The `DataConnectorFactory` Pattern**: Connectors register themselves at compile time using Rust's `linkme` crate:

```rust
// Each connector registers itself
register_data_connector!("postgres", PostgresFactory);
register_data_connector!("snowflake", SnowflakeFactory);
register_data_connector!("s3", S3Factory);
```

This distributed registration means adding a new connector doesn't require modifying a central registry.

**Push-Down via `supports_filters_pushdown()`**: Each `TableProvider` implements this method to declare which filters it can handle natively:

```rust
fn supports_filters_pushdown(&self, filters: &[Expr]) 
    -> Result<Vec<TableProviderFilterPushDown>>
```

Returns `Exact` (source handles completely), `Inexact` (source filters but verify locally), or `Unsupported` (filter locally).

### Dialect Translation

Spice translates SQL to source-specific dialects:

```text
Unified SQL:
SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days'

Translated to:
├─ PostgreSQL: ... WHERE created_at > NOW() - INTERVAL '7 days'
├─ Snowflake: ... WHERE created_at > DATEADD(day, -7, CURRENT_TIMESTAMP())
├─ BigQuery: ... WHERE created_at > DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
└─ MySQL: ... WHERE created_at > DATE_SUB(NOW(), INTERVAL 7 DAY)
```

---

## Query Push-Down: The Key to Performance

The difference between slow and fast federation is **query push-down**—executing as much computation as possible at the source.

### Without Push-Down (Slow)

```text
Query: SELECT * FROM orders WHERE status = 'shipped' AND total > 100

Without push-down:
1. Fetch ALL orders from source → 10M rows, 2GB transfer
2. Filter locally → 50K matching rows
3. Return results

Time: 45 seconds
Network: 2GB
```

### With Push-Down (Fast)

```text
Query: SELECT * FROM orders WHERE status = 'shipped' AND total > 100

With push-down:
1. Push filter to source: SELECT * FROM orders WHERE status = 'shipped' AND total > 100
2. Source executes filter → 50K rows, 10MB transfer
3. Return results

Time: 2 seconds
Network: 10MB
```

### What Spice Pushes Down

| Operation       | Push-Down Support | Example                            |
| --------------- | ----------------- | ---------------------------------- |
| **Filter**      | ✅ Full            | `WHERE status = 'active'`          |
| **Projection**  | ✅ Full            | `SELECT id, name` (not `SELECT *`) |
| **Limit**       | ✅ Full            | `LIMIT 100`                        |
| **Aggregation** | ✅ Partial         | `COUNT(*)`, `SUM()`, `AVG()`       |
| **Sorting**     | ✅ Partial         | `ORDER BY created_at DESC`         |
| **Joins**       | ✅ Same-source     | Joins within same database         |

### Inspecting Query Plans

See what gets pushed down:

```sql
EXPLAIN SELECT * FROM orders 
WHERE status = 'shipped' 
  AND created_at > '2024-01-01'
LIMIT 100;
```

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ plan_type     │ plan                                                        │
├───────────────┼─────────────────────────────────────────────────────────────┤
│ logical_plan  │ Federated                                                   │
│               │   Limit: 100                                                │
│               │     Filter: status = 'shipped' AND created_at > '2024-01-01'│
│               │       TableScan: orders                                     │
│ physical_plan │ VirtualExecutionPlan                                        │
│               │   compute_context=postgres                                  │
│               │   initial_sql=SELECT ... FROM orders                        │
│               │               WHERE status = 'shipped'                      │
│               │                 AND created_at > '2024-01-01'               │
│               │               LIMIT 100                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Federation Patterns

### Pattern 1: Virtual Data Warehouse

Treat all sources as one logical warehouse:

```yaml
# Virtual warehouse spanning multiple systems
datasets:
  # Transactional
  - name: orders
    from: postgres:sales.orders
    
  - name: customers
    from: postgres:crm.customers
    
  # Analytical
  - name: order_metrics
    from: snowflake:analytics.daily_orders
    
  # Real-time
  - name: inventory
    from: dynamodb:inventory
```

```sql
-- Query like a single warehouse
SELECT 
    c.region,
    SUM(o.total) as revenue,
    AVG(om.order_frequency) as avg_frequency,
    SUM(i.quantity) as current_stock
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN order_metrics om ON c.id = om.customer_id
JOIN inventory i ON o.product_id = i.product_id
GROUP BY c.region;
```

### Pattern 2: Federation + Acceleration

Accelerate frequently-accessed tables while federating others:

```yaml
datasets:
  # Accelerated: fast, local queries
  - name: products
    from: postgres:catalog.products
    acceleration:
      enabled: true
      engine: duckdb
      refresh_schedule: "0 * * * *"  # Hourly
      
  # Federated: query source directly
  - name: orders
    from: postgres:sales.orders
    # No acceleration - always fresh
    
  # Accelerated with filter
  - name: recent_customers
    from: snowflake:customers
    acceleration:
      enabled: true
      refresh_sql: "SELECT * FROM customers WHERE updated_at > CURRENT_DATE - 90"
```

```sql
-- Mix of accelerated and federated
SELECT 
    p.name,                    -- Accelerated (10ms)
    COUNT(o.id) as orders      -- Federated (200ms)
FROM products p
JOIN orders o ON p.id = o.product_id
GROUP BY p.name;
```

### Pattern 3: Multi-Cloud Federation

Query across cloud providers:

```yaml
datasets:
  # AWS
  - name: s3_events
    from: s3://analytics/events/
    params:
      aws_region: us-east-1
      
  # GCP
  - name: bigquery_metrics
    from: bigquery:analytics.metrics
    params:
      project_id: my-project
      
  # Azure
  - name: synapse_logs
    from: synapse:logging.app_logs
```

### Pattern 4: SaaS Integration

Federate SaaS data alongside databases:

```yaml
datasets:
  # Salesforce
  - name: opportunities
    from: salesforce:opportunities
    
  # HubSpot
  - name: contacts
    from: hubspot:contacts
    
  # Internal DB
  - name: deals
    from: postgres:sales.deals
```

```sql
-- Join CRM systems
SELECT 
    o.name as opportunity,
    c.email as contact_email,
    d.contract_value
FROM opportunities o
JOIN contacts c ON o.contact_id = c.id
JOIN deals d ON o.deal_id = d.id
WHERE o.stage = 'Negotiation';
```

---

## Real-World Use Cases

### Customer 360 View

Combine data from multiple systems:

```yaml
datasets:
  - name: crm_contacts
    from: salesforce:contacts
    
  - name: support_tickets
    from: zendesk:tickets
    
  - name: transactions
    from: postgres:billing.transactions
    
  - name: product_usage
    from: snowflake:analytics.usage_events
```

```sql
-- Complete customer view
SELECT 
    c.name,
    c.email,
    c.account_tier,
    COUNT(DISTINCT t.id) as total_purchases,
    SUM(t.amount) as lifetime_value,
    COUNT(DISTINCT st.id) as support_tickets,
    AVG(pu.daily_sessions) as avg_daily_usage
FROM crm_contacts c
LEFT JOIN transactions t ON c.id = t.customer_id
LEFT JOIN support_tickets st ON c.email = st.requester_email
LEFT JOIN product_usage pu ON c.id = pu.user_id
WHERE c.id = '12345'
GROUP BY c.name, c.email, c.account_tier;
```

### Real-Time Analytics Dashboard

Combine historical and real-time data:

```yaml
datasets:
  # Historical aggregates (accelerated)
  - name: daily_metrics
    from: snowflake:analytics.daily_rollups
    acceleration:
      enabled: true
      engine: duckdb
      
  # Real-time events (federated)
  - name: live_events
    from: kafka:events
    params:
      topic: user-events
```

```sql
-- Combine historical and real-time
SELECT 
    date,
    COALESCE(dm.active_users, 0) as historical_users,
    COUNT(DISTINCT le.user_id) as realtime_users
FROM generate_series(
    CURRENT_DATE - INTERVAL '7 days',
    CURRENT_DATE,
    INTERVAL '1 day'
) as date
LEFT JOIN daily_metrics dm ON dm.date = date::date
LEFT JOIN live_events le ON DATE(le.timestamp) = date::date
GROUP BY date
ORDER BY date;
```

### Legacy System Migration

Query old and new systems together:

```yaml
datasets:
  # Legacy Oracle
  - name: legacy_orders
    from: oracle:legacy.orders
    
  # New PostgreSQL
  - name: new_orders
    from: postgres:modern.orders
```

```sql
-- Union old and new
SELECT 
    'legacy' as source,
    order_id,
    customer_id,
    total,
    created_at
FROM legacy_orders
WHERE created_at < '2024-01-01'

UNION ALL

SELECT 
    'new' as source,
    order_id,
    customer_id,
    total,
    created_at
FROM new_orders
WHERE created_at >= '2024-01-01';
```

### Data Quality Validation

Compare data across systems:

```sql
-- Validate data consistency
SELECT 
    'order_count_mismatch' as issue,
    s.date,
    s.count as source_count,
    w.count as warehouse_count
FROM (
    SELECT DATE(created_at) as date, COUNT(*) as count
    FROM postgres.orders
    GROUP BY DATE(created_at)
) s
JOIN (
    SELECT date, order_count as count
    FROM snowflake.daily_order_counts
) w ON s.date = w.date
WHERE s.count != w.count;
```

---

## Getting Started

### 1. Install Spice

```bash
curl https://install.spiceai.org | /bin/bash
```

### 2. Configure Data Sources

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: federation-demo

datasets:
  # PostgreSQL
  - name: orders
    from: postgres:public.orders
    params:
      pg_host: localhost
      pg_port: "5432"
      pg_user: ${secrets:PG_USER}
      pg_pass: ${secrets:PG_PASS}
      pg_db: ecommerce
      
  # S3 Parquet
  - name: products
    from: s3://my-bucket/products/
    params:
      file_format: parquet
      aws_region: us-east-1
```

### 3. Start Spice

```bash
spiced
```

### 4. Query Across Sources

```bash
# Interactive SQL
spice sql

# Or via API
curl -X POST http://localhost:8090/v1/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM orders o JOIN products p ON o.product_id = p.id LIMIT 10"}'
```

---

## Conclusion

SQL Federation transforms how organizations work with distributed data:

- **Unified interface**: One SQL dialect for all sources
- **No ETL required**: Query data where it lives
- **Intelligent push-down**: Fast queries through optimization
- **Flexible architecture**: Mix federation with acceleration

Spice makes federation accessible with a simple configuration-driven approach, support for 20+ data sources, and enterprise-grade push-down optimization.

---

## Related Articles in This Series

- **[Caching](caching-explained.md)**: Caching federated query results for performance
- **[Operational Data Lakehouse](operational-data-lakehouse-explained.md)**: Accelerating federated data for real-time access
- **[Hybrid SQL Search](hybrid-sql-search-explained.md)**: Adding search capabilities to federated queries
- **[RAG (Retrieval-Augmented Generation)](rag-explained.md)**: Using federation to build cross-source RAG contexts
- **[LLM Inference](llm-inference-explained.md)**: Analyzing federated data with AI

---

## Further Reading

- [Federated SQL Query Recipe](https://github.com/spiceai/cookbook/blob/trunk/federation/README.md)
- [Data Connectors Reference](https://spiceai.org/docs/components/data-connectors)
- [Query Federation Documentation](https://spiceai.org/docs/features/query-federation)
- [Apache DataFusion](https://datafusion.apache.org/)

