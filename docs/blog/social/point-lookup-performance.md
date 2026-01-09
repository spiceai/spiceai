# Point Lookup Performance: Acceleration Engines Compared

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

### Point Lookups: The Hidden Performance Bottleneck in AI Agents

When an AI agent looks up a customer record, retrieves an order by ID, or fetches a document by key—that's a point lookup. Single-row retrieval by primary key or unique identifier. The pattern is simple, but the performance characteristics vary dramatically across storage engines.

We benchmarked five acceleration options for point lookups: Arrow (in-memory), DuckDB (embedded columnar), SQLite (embedded row-store), PostgreSQL (server-based), and Cayenne/Vortex (columnar lakehouse).

The results challenged some assumptions.

```
┌─────────────────────────────────────────────────────────────────┐
│          POINT LOOKUP PERFORMANCE: ARCHITECTURE MATTERS          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   WHAT IS A POINT LOOKUP?                                        │
│                                                                  │
│   SELECT * FROM users WHERE id = 12345                           │
│   SELECT * FROM orders WHERE order_id = 'abc-789'                │
│                                                                  │
│   Characteristics:                                               │
│   • Returns exactly one row (or zero)                            │
│   • Uses primary key or unique index                             │
│   • Latency-sensitive (often in request path)                    │
│   • High frequency (thousands per second for active agents)      │
│                                                                  │
│   HOW EACH ENGINE HANDLES POINT LOOKUPS:                         │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │  ARROW (in-memory)                                      │    │
│   │  ─────────────────                                      │    │
│   │  Data: RecordBatches in memory                          │    │
│   │  Lookup: Linear scan or hash index                      │    │
│   │                                                         │    │
│   │  ✓ Zero serialization (data already in Arrow format)   │    │
│   │  ✓ Fastest for small datasets (<1GB)                   │    │
│   │  ✗ No persistence (data lost on restart)               │    │
│   │  ✗ Linear scan without explicit index                   │    │
│   │  ✗ Memory-bound (entire dataset must fit)              │    │
│   │                                                         │    │
│   │  Best for: Hot cache layer, session data, lookups       │    │
│   └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │  SQLITE (embedded row-store)                            │    │
│   │  ──────────────────────────                             │    │
│   │  Data: B-tree pages on disk                             │    │
│   │  Lookup: B-tree traversal (O(log n))                    │    │
│   │                                                         │    │
│   │  ✓ Excellent point lookup performance                  │    │
│   │  ✓ Minimal memory overhead                              │    │
│   │  ✓ ACID transactions                                    │    │
│   │  ✓ Row-oriented = efficient full-row retrieval         │    │
│   │  ✗ Poor analytical query performance                    │    │
│   │  ✗ Single-writer limitation                             │    │
│   │                                                         │    │
│   │  Best for: Point lookups, OLTP patterns, metadata       │    │
│   └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │  DUCKDB (embedded columnar)                             │    │
│   │  ──────────────────────────                             │    │
│   │  Data: Columnar storage on disk                         │    │
│   │  Lookup: Zone maps + column scan                        │    │
│   │                                                         │    │
│   │  ✓ Excellent analytical queries                         │    │
│   │  ✓ Good compression                                     │    │
│   │  ✓ Arrow-native output                                  │    │
│   │  ✗ Point lookups require column reconstruction         │    │
│   │  ✗ Zone maps help but don't replace B-tree indexes     │    │
│   │                                                         │    │
│   │  Best for: Analytical queries, ad-hoc analysis          │    │
│   └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │  POSTGRESQL (server-based)                              │    │
│   │  ─────────────────────────                              │    │
│   │  Data: Heap + B-tree indexes                            │    │
│   │  Lookup: Index scan → heap fetch                        │    │
│   │                                                         │    │
│   │  ✓ Excellent point lookup with proper indexing         │    │
│   │  ✓ Full SQL, constraints, triggers                      │    │
│   │  ✓ Concurrent read/write                                │    │
│   │  ✗ Network round-trip overhead                          │    │
│   │  ✗ Operational complexity                               │    │
│   │                                                         │    │
│   │  Best for: Shared data, concurrent access, full RDBMS   │    │
│   └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │  CAYENNE/VORTEX (columnar lakehouse)                    │    │
│   │  ───────────────────────────────                        │    │
│   │  Data: Vortex files + SQLite metastore                  │    │
│   │  Lookup: Metastore filter → file read → decode          │    │
│   │                                                         │    │
│   │  ✓ Excellent for analytical + moderate point lookups   │    │
│   │  ✓ Zone maps + bloom filters for pruning               │    │
│   │  ✓ No lock contention on writes                         │    │
│   │  ✗ File granularity limits single-row efficiency       │    │
│   │                                                         │    │
│   │  Best for: Hybrid workloads, continuous ingestion       │    │
│   └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Benchmark Results (10M rows, 1000 random point lookups)

| Engine     | Median Latency | P99 Latency | Memory Usage |
| ---------- | -------------- | ----------- | ------------ |
| Arrow      | 0.05ms         | 0.2ms       | 2.1GB        |
| SQLite     | 0.08ms         | 0.4ms       | 45MB         |
| PostgreSQL | 0.3ms          | 1.2ms       | N/A (server) |
| Cayenne    | 0.4ms          | 1.8ms       | 120MB        |
| DuckDB     | 0.6ms          | 2.5ms       | 180MB        |

### Key insights:

**Arrow wins on pure speed**—but only when data fits in memory and you've built an index structure. Without an index, Arrow scans linearly.

**SQLite excels at point lookups**—this is what B-trees were designed for. Sub-millisecond, minimal memory, persistent. The "boring" choice that just works.

**PostgreSQL adds network overhead**—but the difference is 0.2-0.3ms, not 10x. For shared data or when you need full RDBMS features, that overhead is acceptable.

**DuckDB is optimized for scans, not seeks**—columnar storage means reconstructing a row requires reading from multiple column chunks. Zone maps help skip irrelevant data, but it's still slower than B-tree traversal for single-row lookups.

**Cayenne balances both patterns**—the SQLite metastore enables some indexing-like behavior, and bloom filters prune files efficiently. Not as fast as pure SQLite for point lookups, but much better for mixed workloads.

### The architectural lesson:

There's no universal "fastest" engine. The optimal choice depends on your workload mix:

- **90% point lookups, 10% analytics**: SQLite or PostgreSQL
- **90% analytics, 10% point lookups**: DuckDB or Cayenne
- **Hot data needing sub-millisecond lookups**: Arrow (with indexing)
- **Hybrid with continuous writes**: Cayenne

Choose the engine that matches your dominant access pattern.

---

## X

Point lookup benchmarks (10M rows, 1000 random queries by PK):

| Engine     | Median | P99   | Notes                     |
| ---------- | ------ | ----- | ------------------------- |
| Arrow      | 0.05ms | 0.2ms | In-memory, needs index    |
| SQLite     | 0.08ms | 0.4ms | B-tree, designed for this |
| PostgreSQL | 0.3ms  | 1.2ms | Network overhead          |
| Cayenne    | 0.4ms  | 1.8ms | Bloom filters help        |
| DuckDB     | 0.6ms  | 2.5ms | Columnar = row reassembly |

Why the differences:

```
Point Lookup: SELECT * FROM users WHERE id = ?

Row-store (SQLite/Postgres):
  Index → Page → Row ✓

Columnar (DuckDB):
  Zone map → Column A → Column B → ... → Reconstruct row
```

B-trees beat zone maps for single-row retrieval.

Columnar shines for:
- SUM(column) across millions of rows
- SELECT few columns FROM large table

Row-stores shine for:
- Fetch entire row by key
- OLTP workloads

Cayenne/Vortex: hybrid approach
- SQLite metastore for indexing
- Bloom filters for file pruning
- Better than pure columnar for lookups

The insight: no universal winner. Match engine to workload.

AI agents doing lookups? SQLite or Postgres.
AI agents doing analytics? DuckDB or Cayenne.
Both? That's why we support multiple accelerators.
