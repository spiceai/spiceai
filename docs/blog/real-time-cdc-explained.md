# Real-Time CDC for AI Applications

*How Change Data Capture enables AI systems to work with live data instead of stale snapshots.*

> **Part of the Spice Use Case Series**: This article is one of our deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [The Freshness Problem](#the-freshness-problem)
3. [What is CDC?](#what-is-cdc)
4. [CDC Patterns for AI](#cdc-patterns-for-ai)
5. [Implementation with Spice](#implementation-with-spice)
6. [Real-World Use Cases](#real-world-use-cases)
7. [Performance Considerations](#performance-considerations)
8. [Getting Started](#getting-started)
9. [Conclusion](#conclusion)

---

## Introduction

AI applications are only as good as their data. Yet most AI systems operate on **stale data**:

| Data Type        | Typical Freshness | AI Impact             |
| ---------------- | ----------------- | --------------------- |
| RAG embeddings   | Days to weeks     | Outdated answers      |
| Feature stores   | Hours             | Stale predictions     |
| Analytics        | Daily batch       | Delayed insights      |
| Customer context | Batch sync        | Wrong recommendations |

**Change Data Capture (CDC)** closes this gap—streaming database changes in real-time to keep AI systems synchronized with source-of-truth data.

---

## The Freshness Problem

### How AI Data Gets Stale

```text
Source Database                        AI Application
      │                                      │
      │ 10:00 AM: Customer upgrades          │
      │           to premium tier            │
      │                                      │
      │                                      │ 10:05 AM: Customer asks
      │                                      │           about premium features
      │                                      │
      │                                      │ AI: "Upgrade to premium
      │                                      │      to access this feature"
      │                                      │
      │                                      │ ❌ Wrong! Customer already upgraded
      │                                      │
      │──────── 6:00 PM: Daily ETL ─────────▶│
      │                                      │
      │                                      │ Finally knows about upgrade
      │                                      │
```

### The Cost of Stale Data

| Issue                    | Business Impact        |
| ------------------------ | ---------------------- |
| Outdated recommendations | Lost sales, poor UX    |
| Wrong customer context   | Support escalations    |
| Stale inventory data     | Overselling, stockouts |
| Delayed fraud signals    | Financial losses       |
| Old compliance data      | Regulatory risk        |

---

## What is CDC?

Change Data Capture captures row-level changes from a database transaction log:

```text
Transaction Log (WAL/Binlog)
──────────────────────────────────────────────────────────
│ LSN 1001 │ INSERT │ customers │ {id: 5, name: "Acme"} │
│ LSN 1002 │ UPDATE │ orders    │ {id: 99, status: "shipped"} │
│ LSN 1003 │ DELETE │ temp_data │ {id: 42} │
──────────────────────────────────────────────────────────
                            │
                            ▼
                     CDC Connector
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Change Events Stream                          │
│                                                                  │
│  {                                                               │
│    "op": "c",           // c=create, u=update, d=delete         │
│    "ts_ms": 1699529401,                                         │
│    "table": "customers",                                        │
│    "before": null,                                              │
│    "after": {"id": 5, "name": "Acme", "tier": "premium"}       │
│  }                                                               │
└─────────────────────────────────────────────────────────────────┘
```

### CDC vs Traditional ETL

| Aspect              | Batch ETL         | CDC               |
| ------------------- | ----------------- | ----------------- |
| **Latency**         | Hours/Days        | Seconds/Minutes   |
| **Load on source**  | High (full scans) | Low (log reading) |
| **Data volume**     | Full tables       | Only changes      |
| **Complexity**      | Simple            | Medium            |
| **Change tracking** | Lost              | Preserved         |

---

## CDC Patterns for AI

### Pattern 1: Incremental Embedding Updates

Keep vector embeddings synchronized without full reprocessing:

```text
┌──────────────┐     CDC      ┌─────────────────┐
│  PostgreSQL  │─────────────▶│ Change Stream   │
│              │              │                 │
│ documents    │              │ • INSERT → Embed & insert
│ table        │              │ • UPDATE → Re-embed & update
│              │              │ • DELETE → Remove from index
└──────────────┘              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │ Vector Index    │
                              │ (always current)│
                              └─────────────────┘
```

**Result**: RAG applications always search current content.

### Pattern 2: Real-Time Feature Updates

Stream feature updates for ML models:

```yaml
# Streaming feature pipeline
datasets:
  - name: customer_features
    from: postgres:cdc:customers
    
  # Computed features stay fresh
  - name: customer_lifetime_value
    query: |
      SELECT 
        customer_id,
        SUM(amount) as total_spend,
        COUNT(*) as order_count,
        MAX(order_date) as last_order
      FROM orders
      GROUP BY customer_id
    acceleration:
      enabled: true
      refresh_mode: changes
```

### Pattern 3: Streaming Context for Agents

AI agents with real-time awareness:

```text
Agent Query: "What's the status of order #12345?"

                      ┌─────────────────────────────────┐
                      │          AI Agent               │
                      └───────────────┬─────────────────┘
                                      │
                                      ▼
                      ┌─────────────────────────────────┐
                      │       Data Substrate            │
                      │   (CDC-refreshed tables)        │
                      └───────────────┬─────────────────┘
                                      │
                                      ▼
                      ┌─────────────────────────────────┐
                      │  Order #12345: SHIPPED          │
                      │  Updated 30 seconds ago         │
                      │  Carrier: UPS                   │
                      │  ETA: Tomorrow 2pm              │
                      └─────────────────────────────────┘

Agent knows real-time status, not yesterday's batch data.
```

### Pattern 4: Event-Triggered Processing

Trigger AI processing on data changes:

```text
Database Change Event
        │
        ▼
┌───────────────────┐
│ Change Handler    │
│                   │
│ if new_order:     │
│   → fraud_check() │──────▶ ML Inference
│   → notify()      │
│                   │
│ if profile_update:│
│   → re_embed()    │──────▶ Vector Update
│   → rerank()      │
└───────────────────┘
```

---

## Implementation with Spice

### Basic CDC Configuration

Connect to PostgreSQL with logical replication:

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: cdc-example

datasets:
  # CDC-enabled dataset
  - name: orders
    from: postgres:sales.orders
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: changes  # CDC mode
      refresh_check_interval: 1s
```

### CDC with Debezium

For production CDC pipelines:

```yaml
datasets:
  - name: customers
    from: debezium:mysql.inventory.customers
    params:
      kafka_bootstrap_servers: localhost:9092
    acceleration:
      enabled: true
      refresh_mode: changes
```

### CDC for Embeddings

Automatically re-embed changed documents:

```yaml
datasets:
  - name: knowledge_base
    from: postgres:content.articles
    acceleration:
      enabled: true
      refresh_mode: changes
      on_change:
        - regenerate_embeddings
    embeddings:
      - column: content
        use: openai_embeddings
        chunking:
          enabled: true
          target_chunk_size: 512
```

### CDC Event Processing

Handle change events programmatically:

```python
import spicepy as spice
from spicepy.events import ChangeEvent

client = spice.Client()

# Subscribe to changes
async for event in client.subscribe_changes("orders"):
    if event.op == "INSERT":
        # New order - check for fraud
        result = await client.query(f"""
            SELECT ai(
                'Analyze this order for fraud risk: ' || 
                '{event.after}',
                'gpt-4'
            ) as risk_analysis
        """)
        
        if "high risk" in result['risk_analysis']:
            await alert_fraud_team(event.after['order_id'])
            
    elif event.op == "UPDATE":
        # Order updated - notify customer
        if event.after['status'] != event.before['status']:
            await notify_customer(
                event.after['customer_id'],
                event.after['status']
            )
```

---

## Real-World Use Cases

### Use Case 1: Real-Time Customer Support

```yaml
# Customer context always current
datasets:
  - name: customer_profile
    from: postgres:cdc:crm.customers
    acceleration:
      enabled: true
      refresh_mode: changes
      
  - name: recent_orders
    from: postgres:cdc:sales.orders
    acceleration:
      enabled: true
      refresh_mode: changes
      
  - name: support_history
    from: postgres:cdc:support.tickets
    embeddings:
      - column: description
        use: openai_embeddings
    acceleration:
      enabled: true
      refresh_mode: changes
```

**Agent capability**:

```sql
-- Support agent query (all data is real-time)
SELECT 
    c.name,
    c.tier,  -- Updated 5 seconds ago when upgraded
    o.last_order_status,  -- Real-time tracking
    vs.similar_tickets  -- From current ticket history
FROM customer_profile c
JOIN recent_orders o ON c.id = o.customer_id
JOIN vector_search(support_history, 'billing issue', 5) vs 
    ON c.id = vs.customer_id
WHERE c.id = 'cust_123';
```

### Use Case 2: Live Inventory AI

```yaml
datasets:
  - name: inventory
    from: postgres:cdc:warehouse.stock
    acceleration:
      enabled: true
      refresh_mode: changes
      refresh_check_interval: 500ms  # Sub-second updates
      
  - name: demand_forecast
    from: snowflake:analytics.forecasts
    acceleration:
      enabled: true
      
models:
  - name: restock_advisor
    from: openai
```

**Query**:

```sql
-- AI recommendation with live inventory
SELECT 
    i.product_id,
    i.current_stock,  -- Real-time
    f.predicted_demand,
    ai(
        'Should we reorder? Current: ' || i.current_stock || 
        ' Predicted demand: ' || f.predicted_demand,
        'restock_advisor'
    ) as recommendation
FROM inventory i
JOIN demand_forecast f ON i.product_id = f.product_id
WHERE i.current_stock < f.predicted_demand * 1.2;
```

### Use Case 3: Compliance Monitoring

```yaml
datasets:
  - name: transactions
    from: postgres:cdc:core.transactions
    acceleration:
      enabled: true
      refresh_mode: changes
      
  - name: compliance_rules
    from: s3://compliance/rules/
    embeddings:
      - column: rule_text
        use: openai_embeddings
```

**Real-time compliance check**:

```sql
-- Check each transaction against relevant rules
SELECT 
    t.transaction_id,
    t.amount,
    t.type,
    vs.rule_id,
    vs.rule_text,
    ai(
        'Does this transaction violate this rule? ' ||
        'Transaction: ' || t.description || 
        ' Rule: ' || vs.rule_text,
        'gpt-4'
    ) as compliance_check
FROM transactions t
CROSS JOIN LATERAL (
    SELECT * FROM vector_search(
        compliance_rules, 
        t.description, 
        3
    )
) vs
WHERE t.timestamp > NOW() - INTERVAL '1 minute';  -- Last minute only
```

---

## Performance Considerations

### CDC Lag Management

```text
Target: CDC Lag < Query Freshness Requirement

┌─────────────────────────────────────────────────────────────────┐
│ Latency Budget: 5 seconds                                       │
│                                                                  │
│ Source DB → CDC Connector: ~100ms                               │
│ CDC Connector → Message Queue: ~50ms                            │
│ Message Queue → Spice: ~100ms                                   │
│ Spice Processing: ~50ms                                         │
│ Total: ~300ms                                                   │
│                                                                  │
│ ✅ Well within budget                                           │
└─────────────────────────────────────────────────────────────────┘
```

### Handling High-Volume Changes

```yaml
datasets:
  - name: high_volume_events
    from: postgres:cdc:events.clicks
    acceleration:
      enabled: true
      refresh_mode: changes
      params:
        batch_size: 10000  # Batch CDC events
        flush_interval: 1s  # Max delay
```

### Embedding Update Optimization

```yaml
embeddings:
  - column: content
    use: openai_embeddings
    params:
      # Only re-embed if content actually changed
      change_detection: content_hash
      # Batch embedding requests
      batch_size: 100
      # Rate limit to avoid API throttling
      rate_limit: 1000/minute
```

---

## Getting Started

### Step 1: Enable CDC on Source Database

**PostgreSQL**:

```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Create publication
CREATE PUBLICATION spice_pub FOR TABLE customers, orders;
```

**MySQL**:

```sql
-- Enable binlog
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
```

### Step 2: Configure Spice CDC Dataset

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: cdc-quickstart

datasets:
  - name: orders
    from: postgres:sales.orders
    params:
      connection_string: ${secrets:POSTGRES_URL}
    acceleration:
      enabled: true
      refresh_mode: changes
```

### Step 3: Start Spice

```bash
spice run
```

### Step 4: Verify CDC

```bash
spice sql

sql> SELECT * FROM orders ORDER BY updated_at DESC LIMIT 5;

# Make a change in source database
# Watch it appear in Spice within seconds
```

---

## Conclusion

CDC transforms AI applications from **periodic batch** to **continuous sync**:

| Aspect            | Before CDC       | With CDC       |
| ----------------- | ---------------- | -------------- |
| Data freshness    | Hours/Days       | Seconds        |
| Embedding updates | Full rebuild     | Incremental    |
| Agent context     | Stale            | Real-time      |
| User experience   | "Data not found" | Always current |

AI systems need current data. CDC provides it—efficiently, reliably, in real-time.

---

## Related Articles in This Series

- **[Data Acceleration](data-acceleration-explained.md)**: Caching strategies for performance
- **[The Data Substrate](data-substrate-explained.md)**: Unified data foundation
- **[SQL Federation](sql-federation-explained.md)**: Querying across sources
- **[RAG Explained](rag-explained.md)**: Building retrieval-augmented generation

---

## Further Reading

- [Spice CDC Documentation](https://spiceai.org/docs/cdc)
- [Debezium Documentation](https://debezium.io/)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)

