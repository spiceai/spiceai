# SQL Federation

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

**SQL Federation: Query Any Data Source With One Interface**

Most organizations have data in 5-15 different systems. PostgreSQL for transactions. Snowflake for analytics. S3 for the data lake. Salesforce for CRM. MongoDB for documents.

Traditionally, to analyze data across these sources, you build ETL pipelines. Extract, transform, load into a warehouse. Then query the warehouse.

SQL Federation takes a different approach: query the sources directly, through a unified SQL interface.

```
┌─────────────────────────────────────────────────────────────────┐
│              SQL FEDERATION: HOW IT WORKS                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ETL APPROACH (traditional):                                    │
│                                                                  │
│   PostgreSQL ─┐                                                  │
│               │                                                  │
│   Snowflake  ─┼──→ ETL Jobs ──→ Warehouse ──→ Query             │
│               │         │                                        │
│   S3         ─┘         ▼                                        │
│                  Costs:                                          │
│                  • Storage: 3-10x duplication                    │
│                  • Freshness: Hours to days stale               │
│                  • Maintenance: Pipeline code, monitoring        │
│                  • Flexibility: Schema changes break pipelines  │
│                                                                  │
│   FEDERATION APPROACH:                                           │
│                                                                  │
│   PostgreSQL ◄──┐                                                │
│                 │                                                │
│   Snowflake  ◄──┼─── Federation Engine ◄── SQL Query            │
│                 │           │                                    │
│   S3         ◄──┘           ▼                                    │
│                      Push computation to sources                 │
│                      Return only matching results                │
│                                                                  │
│   KEY MECHANISM: Query Push-Down                                 │
│                                                                  │
│   Query: SELECT * FROM pg.orders WHERE date > '2024-01-01'       │
│                                                                  │
│   Without push-down:                                             │
│   └─ Fetch ALL orders from PostgreSQL                           │
│   └─ Filter locally                                              │
│   └─ Network: millions of rows                                   │
│                                                                  │
│   With push-down:                                                │
│   └─ Send "WHERE date > '2024-01-01'" to PostgreSQL             │
│   └─ PostgreSQL filters using its indexes                       │
│   └─ Network: only matching rows                                 │
│                                                                  │
│   Push-down extends to:                                          │
│   • Projections (SELECT specific columns)                        │
│   • Aggregations (SUM, COUNT, AVG computed at source)           │
│   • Joins (when both tables on same source)                      │
│   • Limits (LIMIT 100 applied at source)                         │
└─────────────────────────────────────────────────────────────────┘
```

**The evolution of federation technology:**

**1990s - Data Warehouses**: Copy everything centrally. Simple but stale.

**2000s - Federated Databases**: Query remote sources, but pull all data locally for processing. Poor performance.

**2010s - Data Virtualization**: Added caching and smarter planning. Expensive, often became another silo.

**2020s - Push-Down Federation**: Push computation to sources. Only results cross the network. Leverage each source's native optimization.

**Tradeoffs to understand:**

- **Source load**: Federation queries hit your source systems. They need capacity.
- **Network dependency**: Query latency includes network round-trips.
- **Optimization limits**: Cross-source joins can't use source-side optimization.
- **Caching complexity**: When to cache vs. query live is a design decision.

**When federation works well:**
- Real-time requirements (can't wait for ETL)
- Schema volatility (sources change frequently)
- Exploratory queries (don't know what you need yet)
- Cost sensitivity (don't want to store data twice)

**When ETL is still right:**
- Heavy analytical workloads (aggregations across billions of rows)
- Strict latency requirements (pre-computed beats live queries)
- Complex transformations (business logic too complex for SQL)

From experience: I've built hundreds of ETL pipelines. Most of them existed because our query systems couldn't talk to multiple sources—not because ETL was the best architecture. Federation eliminates that constraint.

---

## X

SQL Federation: the evolution

Phase 1 (90s): Data Warehouses
Source → ETL → Warehouse → Query
✗ Data staleness, storage duplication, pipeline maintenance

Phase 2 (00s): Federated DBs
SELECT * FROM oracle.customers JOIN db2.orders
✗ Pulled all data centrally, poor performance

Phase 3 (10s): Data Virtualization
Added caching layer
✗ Expensive licensing, another silo

Phase 4 (20s): Push-down Federation
Push WHERE, GROUP BY, projections to source systems
✓ Minimal data movement, native optimization

```sql
-- One query, multiple sources
SELECT c.name, SUM(o.total)
FROM postgres.customers c
JOIN snowflake.orders o ON c.id = o.customer_id
WHERE o.date > '2024-01-01'
```

The source systems do the heavy lifting.
