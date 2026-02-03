# Point Lookup Performance: Acceleration Engines Compared

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

Point Lookups: The Hidden Performance Bottleneck in AI Agents

When an AI agent looks up a customer record, retrieves an order by ID, or fetches a document by key—that's a point lookup. Single-row retrieval by primary key or unique identifier. The pattern is simple, but the performance characteristics vary dramatically across storage engines.

We benchmarked five acceleration options for point lookups: Arrow (in-memory), DuckDB (embedded columnar), SQLite (embedded row-store), PostgreSQL (server-based), and Cayenne/Vortex (columnar lakehouse).

The results challenged some assumptions.

What is a point lookup? SELECT FROM users WHERE id = 12345. Returns exactly one row or zero. Uses primary key or unique index. Latency-sensitive—often in request path. High frequency—thousands per second for active agents.

How each engine handles point lookups:

Arrow (in-memory): Data lives as RecordBatches in memory. Lookup uses linear scan or hash index. Zero serialization since data is already in Arrow format. Fastest for small datasets under 1GB. No persistence—data lost on restart. Memory-bound—entire dataset must fit.

SQLite (embedded row-store): Data stored as B-tree pages on disk. Lookup is B-tree traversal, O(log n). Excellent point lookup performance. Minimal memory overhead. ACID transactions. Row-oriented means efficient full-row retrieval. Poor for analytical queries. Single-writer limitation.

DuckDB (embedded columnar): Columnar storage on disk. Lookup uses zone maps plus column scan. Excellent for analytical queries. Good compression. Arrow-native output. Point lookups require column reconstruction. Zone maps help but don't replace B-tree indexes.

PostgreSQL (server-based): Heap storage plus B-tree indexes. Lookup is index scan then heap fetch. Excellent point lookup with proper indexing. Full SQL, constraints, triggers. Concurrent read/write. Network round-trip overhead. Operational complexity.

Cayenne/Vortex (columnar lakehouse): Vortex files plus SQLite metastore. Lookup uses metastore filter, file read, then decode. Excellent for analytical plus moderate point lookups. Zone maps and bloom filters for pruning. No lock contention on writes. File granularity limits single-row efficiency.

Benchmark results with 10M rows and 1000 random point lookups: Arrow median 0.05ms (P99 0.2ms), SQLite median 0.08ms (P99 0.4ms), PostgreSQL median 0.3ms (P99 1.2ms), Cayenne median 0.4ms (P99 1.8ms), DuckDB median 0.6ms (P99 2.5ms).

Key insights:

Arrow wins on pure speed—but only when data fits in memory and you've built an index structure. Without an index, Arrow scans linearly.

SQLite excels at point lookups—this is what B-trees were designed for. Sub-millisecond, minimal memory, persistent. The "boring" choice that just works.

PostgreSQL adds network overhead—but the difference is 0.2-0.3ms, not 10x. For shared data or when you need full RDBMS features, that overhead is acceptable.

DuckDB is optimized for scans, not seeks—columnar storage means reconstructing a row requires reading from multiple column chunks.

Cayenne balances both patterns—the SQLite metastore enables some indexing-like behavior, and bloom filters prune files efficiently.

The architectural lesson: There's no universal "fastest" engine. 90% point lookups means SQLite or PostgreSQL. 90% analytics means DuckDB or Cayenne. Hot data needing sub-millisecond lookups means Arrow with indexing. Hybrid with continuous writes means Cayenne.

---

## X (5 posts, 280 characters each)

Post 1:
Point lookups: SELECT WHERE id = X. AI agents do thousands per second. We benchmarked Arrow, SQLite, DuckDB, PostgreSQL, and Cayenne. The results challenged assumptions about which engines are "fastest."

Post 2:
Benchmark results (10M rows, 1000 lookups): Arrow 0.05ms, SQLite 0.08ms, PostgreSQL 0.3ms, Cayenne 0.4ms, DuckDB 0.6ms. B-trees (SQLite/Postgres) beat zone maps (DuckDB) for single-row retrieval.

Post 3:
Why the difference: Row-stores go Index then Page then Row. Columnar stores go Zone map then Column A then Column B then reconstruct row. Point lookups favor row-oriented storage.

Post 4:
Columnar shines for SUM across millions of rows and SELECT few columns. Row-stores shine for fetch entire row by key. Cayenne/Vortex is hybrid: SQLite metastore plus bloom filters.

Post 5:
The insight: no universal winner. Match engine to workload. AI agents doing lookups? SQLite or Postgres. Doing analytics? DuckDB or Cayenne. Both? That's why we support multiple accelerators.
