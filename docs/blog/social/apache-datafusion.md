# Apache DataFusion - 4-Part Series

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

Target: LinkedIn posts ~3000 characters each. X posts 280 characters max.

---

## Part 1: What is Apache DataFusion?

### LinkedIn (~3000 chars)

⚡ Apache DataFusion: The Query Engine Powering the Next Generation of Data Systems

If you're building anything that processes data at scale—analytics, observability, data lakes, AI applications—you should know about Apache DataFusion.

DataFusion is an extensible SQL query engine written in Rust that uses Apache Arrow as its in-memory format. As of late 2024, it became the fastest single-node engine for querying Apache Parquet files on ClickBench—faster than DuckDB, chDB, and ClickHouse on the same hardware. The first Rust-based engine to hold that position.

But here's the key insight: DataFusion isn't a database. It's the engine you use to build one.

🔧 WHAT DATAFUSION DOES

DataFusion takes SQL queries (or DataFrame operations) and executes them with maximum efficiency:

• Parses SQL into an abstract syntax tree using a PostgreSQL-compatible dialect
• Plans the query into a logical representation
• Optimizes the plan with techniques like predicate pushdown, join reordering, and constant folding
• Creates a physical execution plan tailored to the data sources
• Executes using vectorized Arrow processing with SIMD acceleration

The output: streaming Arrow RecordBatches that are zero-copy compatible with any Arrow-based system—Python (pandas, polars), Spark, and dozens of other tools.

🚀 KEY PERFORMANCE TECHNIQUES

What makes DataFusion fast isn't magic—it's systematic application of database research:

Dynamic Filters: When you run ORDER BY timestamp DESC LIMIT 10, the TopK operator tracks the current top values and pushes that information back to the scan. Files with timestamps older than the current top 10? Skipped entirely without reading. This achieves 25x speedups on common patterns.

Hierarchical Pruning: DataFusion prunes at every level: File → Row Group → Page → Row. Each level uses min/max statistics to skip irrelevant data before reading it from disk.

Late Materialization: When filtering, DataFusion evaluates filter columns first, builds a selection mask, then decodes only the matching rows from other columns. A single optimization reduced ClickBench time by 15%.

StringView: Arrow's German-style strings store short strings inline (≤12 bytes) and long strings as pointers. This eliminates copies in comparisons, filters, and aggregations—critical for string-heavy analytics.

💡 WHY IT MATTERS

Before DataFusion, building a high-performance query engine meant years of work or licensing proprietary technology. Now, any team can embed a production-grade SQL engine in weeks and focus on what makes their system unique.

DataFusion is an Apache Software Foundation project with 700+ contributors, backed by companies like InfluxData, Apple, Cloudflare, and Coralogix. It's the foundation for the next generation of data infrastructure.

### First Reply (with links)

References:

• Apache DataFusion: https://datafusion.apache.org/
• ClickBench results: https://benchmark.clickhouse.com/
• Apache Arrow: https://arrow.apache.org/
• DataFusion GitHub: https://github.com/apache/datafusion
• DataFusion Blog: https://datafusion.apache.org/blog/

---

### X Posts (280 chars max)

1/ What is Apache DataFusion? A query engine written in Rust, using Arrow as its in-memory format. Now the fastest single-node Parquet engine on ClickBench. Not a database—the engine you use to build one.

2/ DataFusion's job: SQL in, Arrow RecordBatches out. Parse → Plan → Optimize → Execute. Vectorized processing, streaming results, zero-copy Arrow compatibility. The query engine abstracted into a library.

---

## Part 2: The Research Behind DataFusion

### LinkedIn (~3000 chars)

📚 Applied Database Research in Apache DataFusion

DataFusion didn't invent these techniques—it implemented them. Built on Apache Arrow's columnar format, DataFusion translates decades of database research into production-ready code. Here's how theory becomes practice:

🔬 DYNAMIC FILTERS

The research: "sideways information passing"—runtime statistics flow between operators to prune data early (Shrinivas et al., Vertica, ICDE 2013).

The implementation: DataFusion's TopK operator tracks current top values and pushes filters back to scans. Query for the 10 most recent events? As execution progresses, the scan learns "skip files older than X" and prunes without reading. Same for hash joins—dimension table key ranges filter fact table scans before I/O.

The result: 25x speedups on common patterns. Snowflake announced similar features in 2024; DataFusion shipped them in open source months earlier.

⚡ LATE MATERIALIZATION

The research: delay column decoding until after filtering (extended by Chen et al., Selective Late Materialization, VLDB 2025).

The implementation: DataFusion's Parquet reader evaluates filter columns first, builds a selection mask, then decodes only matching rows from remaining columns. A 700-line PR interleaved filter evaluation with output generation, caching at most 2 pages per column.

The result: 15% overall ClickBench speedup, some queries 2x faster.

📊 VECTORIZED COLUMNAR EXECUTION

The research: PAX layout for cache performance (Ailamaki et al., VLDB 2001), vector-at-a-time processing (Boncz et al., MonetDB/X100, CIDR 2005).

The implementation: Arrow's columnar format stores data in row groups, organized by column within each group. Batch processing amortizes interpretation overhead. SIMD instructions process 4-8 values simultaneously. Gandiva adds LLVM compilation for expressions—3-5x faster string operations.

The result: Zero-copy data sharing across Python, Spark, and dozens of Arrow-compatible tools.

📈 ADDITIONAL APPLIED TECHNIQUES

Multi-Column Grouping: Traditional GROUP BY copies values twice. Jay Zhan's columnar group storage copies once—critical for high-cardinality, memory-bound queries.

Query Unnesting: Decorrelates nested subqueries to improve plan quality (Neumann & Kemper, BTW 2015). Complex TPC-H queries go from 25 minutes to instant.

Hierarchical Pruning: File → Row Group → Page → Row. Each level uses min/max statistics to skip irrelevant data before reading.

Sort-Based Windows: Analyze existing data ordering to minimize re-sorting (Leis et al., VLDB 2015).

📊 BENCHMARKS

ClickBench: DataFusion is the fastest single-node engine for Parquet queries—ahead of DuckDB, chDB, and ClickHouse.

Comet: Translates Spark physical plans to DataFusion execution. Existing Spark workloads get 2-3x speedups without code changes.

This is applied research at scale: peer-reviewed techniques, production-hardened implementations, Apache 2.0 licensed.

### First Reply (with links)

Research papers and implementation details:

• Shrinivas et al., Vertica materialization: https://15721.courses.cs.cmu.edu/spring2024/papers/04-execution1/shrinivas-icde2013.pdf
• Ailamaki et al., PAX layout: https://www.vldb.org/conf/2001/P169.pdf
• Boncz et al., MonetDB/X100: https://dl.acm.org/doi/10.1145/1167350.1167354
• Neumann & Kemper, Query Unnesting: https://www.semanticscholar.org/paper/Unnesting-Arbitrary-Queries-Neumann-Kemper/3112928019f64d8c388e8cfbae34b9887c789213
• Dynamic Filters blog: https://datafusion.apache.org/blog/2024/11/17/dynamic-filters-for-topk-queries/
• Late Materialization PR: https://github.com/apache/datafusion/pull/14555

---

### X Posts (280 chars max)

1/ DataFusion applies database research: dynamic filters from Vertica (25x faster), late materialization (15% ClickBench gain), vectorized execution from MonetDB. Theory → production code → Apache 2.0 licensed.

2/ Applied research in DataFusion: TopK pushes filters to scans, Parquet decodes only matching rows, Arrow enables SIMD on columnar data. Result: fastest single-node Parquet engine on ClickBench.

---

## Part 3: The DataFusion Ecosystem

### LinkedIn (~3000 chars)

🌍 The Companies and People Building Apache DataFusion

Apache DataFusion is an Apache Software Foundation project—vendor-neutral, community-governed, Apache 2.0 licensed. But behind the project is a remarkable ecosystem of companies and contributors building production systems.

This is what sustainable open source looks like.

🏢 COMPANIES USING DATAFUSION IN PRODUCTION

InfluxData: The time-series database company rebuilt InfluxDB 3.0 on DataFusion. Andrew Lamb, who spent years at Vertica building one of the most successful analytical databases, now leads much of DataFusion's core development as a PMC member.

Apple: Uses DataFusion for internal data processing systems. Multiple Apple engineers contribute upstream.

Cloudflare: Powers analytics at the edge using DataFusion. When you're processing logs at Cloudflare's scale, query engine performance is existential.

Coralogix: Built their log analytics platform on DataFusion. They've contributed optimizations that benefit everyone.

Alibaba Cloud: PolarDB incorporates DataFusion techniques for analytical workloads.

Wherobots: Geospatial analytics at scale. DataFusion handles SQL; they add spatial extensions.

Pydantic/Logfire: Their observability platform runs on DataFusion. Adrian Garcia Badaracco contributed the dynamic filters that enabled 25x speedups on TopK queries.

Synnada: CEO Mehmet Ozan Kabak rallied the community around performance, leading to the ClickBench breakthrough.

Rerun.io: Building data visualization for robotics, sponsored metadata handling improvements.

👥 KEY CONTRIBUTORS

The project's strength comes from its contributors:

Andrew Lamb (InfluxData): PMC member, primary architect of many core features. His 20+ years in databases (including Vertica) shows in DataFusion's design.

Daniël Heres: Core contributor and reviewer on major optimizations. Consistently high-quality reviews.

Jay Zhan: Multi-column grouping optimization, expression improvements. Made GROUP BY dramatically faster.

Xiangpeng Hao (UW Madison PhD): StringView implementation, late materialization, Parquet optimizations. Academic rigor meets production engineering.

Qi Zhu (Cloudera): Parquet indexing, dynamic filters. Deep expertise in storage formats.

Andy Grove: Co-creator of DataFusion, now at Apple. Started the project that became Apache DataFusion.

🚀 APACHE DATAFUSION COMET

DataFusion also powers Apache DataFusion Comet—a Spark accelerator that translates Spark physical plans to DataFusion execution. Existing Spark workloads get 2-3x speedups without code changes.

This is the leverage of building on shared infrastructure.

🤝 THE COMMUNITY MODEL

700+ contributors from North America, South America, Europe, Asia, Africa, and Australia. Undergraduates to senior engineers. No single company controls the project.

Why do competitors contribute to the same project? Because the alternative—each building their own query engine—is worse for everyone. DataFusion solves hard problems they all share. The result is technology none could build alone.

This is what modern open source infrastructure looks like.

### First Reply (with links)

Learn more about the ecosystem:

• Apache DataFusion: https://datafusion.apache.org/
• InfluxDB 3.0: https://www.influxdata.com/
• Coralogix: https://coralogix.com/
• Pydantic Logfire: https://pydantic.dev/logfire
• Apache DataFusion Comet: https://datafusion.apache.org/comet/
• DataFusion Contributors: https://github.com/apache/datafusion/graphs/contributors
• Andrew Lamb on LinkedIn: https://www.linkedin.com/in/apachelamb/

---

### X Posts (280 chars max)

1/ Who's behind DataFusion? InfluxData, Apple, Cloudflare, Alibaba, Coralogix, Pydantic, and 700+ contributors. Apache Foundation governance. No single company controls it—everyone contributes because it solves shared problems.

2/ Key DataFusion contributors: Andrew Lamb (InfluxData, 20+ yrs databases), Xiangpeng Hao (PhD, StringView), Jay Zhan (grouping optimization), Andy Grove (co-creator). Community-built infrastructure.

---

## Part 4: How Spice.ai Uses DataFusion

### LinkedIn (~3000 chars)

🌶️ How Spice.ai Uses Apache DataFusion

Spice.ai is a SQL query, search, and AI inference engine for data apps and agents. At its core: Apache DataFusion.

Here's how we built on open source to ship faster.

🎯 WHY WE CHOSE DATAFUSION

We needed a query engine that could:

• Federate queries across 20+ data sources (PostgreSQL, MySQL, S3, Snowflake, BigQuery, Databricks, and more)
• Accelerate results with materialization to DuckDB, SQLite, or Arrow
• Support vector search for AI applications
• Run AI inference through SQL
• Handle enterprise-scale query volumes with sub-second latency

Building this from scratch? We estimated 18+ months for a basic implementation. With DataFusion: 3 months to first production release.

That's not an exaggeration. DataFusion handles the hardest 80% of building a query engine. We focused on the 20% that makes Spice unique.

🔌 HOW WE EXTEND DATAFUSION

DataFusion is designed for extension. Here's our architecture:

TableProvider per data source: Each connector (PostgreSQL, MySQL, S3, Snowflake, etc.) implements DataFusion's TableProvider trait. DataFusion handles SQL parsing, optimization, and execution. We handle connection pooling, authentication, and source-specific filter pushdown.

This separation of concerns is powerful. When DataFusion improves—and it improves every month—we get those benefits automatically.

Federation via OptimizerRule: Our custom optimizer rules detect when entire query subtrees can be pushed to remote sources. A query joining PostgreSQL and MySQL tables? The PostgreSQL portion runs on PostgreSQL, the MySQL portion on MySQL, DataFusion handles the final join.

This isn't just syntax translation. We push predicates, projections, aggregations, and sorts to sources that can execute them efficiently.

Acceleration layer: AcceleratedTable wraps FederatedTable wraps connector TableProvider. Query results materialize to DuckDB or Arrow for sub-millisecond repeated queries. The acceleration is transparent—same SQL, faster results.

AI inference as UDFs: SELECT embedding(text_column) FROM documents calls embedding models. SELECT llm_complete(prompt) FROM queries runs LLM inference. Standard SQL, powered by DataFusion's UDF system.

This is the key insight: SQL is the universal interface for data. By making AI accessible through SQL, we let developers use existing tools, existing skills, existing infrastructure.

⚙️ WHAT DATAFUSION HANDLES FOR US

• SQL parsing with PostgreSQL dialect compatibility
• Query optimization (predicate pushdown, projection pushdown, join reordering, constant folding)
• Parallel execution across all available cores
• Memory management with automatic spilling to disk
• Arrow-native processing with SIMD acceleration

We didn't have to implement any of this. We inherited years of work from hundreds of contributors.

✨ THE RESULT

Spice.ai runs in production at enterprises processing billions of queries. Fortune 500 companies trust it for real-time analytics. AI applications use it for retrieval-augmented generation.

The query engine foundation came from DataFusion and the open source community. Our contribution: 15+ data connectors, federation logic, and AI integration.

We've also contributed back—bug fixes, documentation, and feature improvements that benefit everyone using DataFusion.

This is the power of building on open source infrastructure. We shipped faster. We built on a better foundation. And we're part of something larger than ourselves.

### First Reply (with links)

Learn more:

• Spice.ai: https://spice.ai/
• Spice.ai GitHub: https://github.com/spiceai/spiceai
• Spice.ai Docs: https://docs.spice.ai/
• Apache DataFusion: https://datafusion.apache.org/
• DataFusion TableProvider API: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html

---

### X Posts (280 chars max)

1/ How Spice.ai uses DataFusion: 20+ data sources via TableProvider, federation via OptimizerRule, acceleration to DuckDB/Arrow, AI inference as UDFs. We estimated 18 months to build. DataFusion: 3 months.

2/ Spice.ai's DataFusion stack: TableProvider → FederatedTable → AcceleratedTable. DataFusion handles SQL, optimization, execution. We handle sources, federation, AI. Open source leverage.

---

## Part 5: DataFusion Extensibility & Our Contribution to the Ecosystem

### LinkedIn (~3000 chars)

🔌 DataFusion's Extensibility: How We Contributed datafusion-table-providers to the Ecosystem

One of DataFusion's greatest strengths isn't just performance—it's extensibility. The architecture is designed for extension at every layer: custom data sources, optimizer rules, functions, and execution strategies.

At Spice.ai, we've built extensively on this foundation. And we've contributed back.

🧩 THE EXTENSIBILITY MODEL

DataFusion's power comes from well-designed extension points:

TableProvider: The interface for any data source. Implement scan(), schema(), and a few other methods, and your data source works with DataFusion's full SQL engine. No need to understand query optimization—DataFusion handles it.

OptimizerRule: Custom rules that transform logical or physical plans. Want to push filters to a remote database? Write a rule that detects pushable predicates and rewrites the plan.

UDFs (User-Defined Functions): Scalar, aggregate, and window functions. Add domain-specific logic without modifying DataFusion core.

TableFactory: Register custom table types that can be created with CREATE EXTERNAL TABLE statements.

This isn't just theoretical extensibility. It's battle-tested by dozens of production systems.

🎁 OUR CONTRIBUTION: datafusion-table-providers

In July 2024, we contributed datafusion-table-providers to the DataFusion ecosystem. This open source project provides production-ready TableProvider implementations for:

• PostgreSQL: Full SQL pushdown, connection pooling, prepared statements
• MySQL: Same capabilities, battle-tested at scale
• DuckDB: In-process OLAP acceleration
• SQLite: Lightweight embedded database support
• ODBC: Connect to any ODBC-compatible data source
• Flight SQL: Apache Arrow Flight protocol for high-performance data transfer

These aren't toy implementations. They're the same connectors that power Spice.ai in production, processing billions of queries for enterprise customers.

The contribution reflects a core belief: the data infrastructure community is stronger when we build together.

🔗 WHY CONTRIBUTE UPSTREAM?

We could have kept these connectors proprietary. Many companies do. But here's why we chose to contribute:

Maintenance burden: When the community maintains shared code, everyone benefits from bug fixes and improvements. A PostgreSQL protocol change? The community fixes it once, not separately in every project.

Ecosystem growth: More DataFusion users means more contributors, more optimization work, more battle-testing. The rising tide lifts all boats.

Talent and trust: Contributing to Apache projects builds credibility. Engineers want to work on impactful open source. Customers trust vendors who give back.

📈 THE RESULT

datafusion-table-providers is now used by multiple projects beyond Spice.ai. It's part of the official datafusion-contrib organization, maintained by the community.

This is sustainable open source: companies contribute because it serves their interests AND the ecosystem's interests. The result is infrastructure none could build alone.

If you're building on DataFusion and need database connectivity, check out datafusion-table-providers. And if you have improvements, contribute them back. That's how we all move faster.

### First Reply (with links)

Learn more about datafusion-table-providers:

• GitHub: https://github.com/datafusion-contrib/datafusion-table-providers
• Our announcement blog: https://spice.ai/blog/contribution-of-tableproviders-to-datafusion
• DataFusion TableProvider docs: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
• How we use DataFusion at Spice AI: https://spice.ai/blog/how-we-use-apache-datafusion-at-spice-ai
• DataFusion extensibility guide: https://datafusion.apache.org/library-user-guide/

---

### X Posts (280 chars max)

1/ We contributed datafusion-table-providers to the DataFusion ecosystem: production-ready connectors for PostgreSQL, MySQL, DuckDB, SQLite, ODBC, Flight SQL. Open source, community-maintained. https://github.com/datafusion-contrib/datafusion-table-providers

2/ DataFusion's extensibility: TableProvider for data sources, OptimizerRule for query rewriting, UDFs for custom functions. Battle-tested by InfluxData, Apple, Cloudflare, and Spice.ai. The architecture that makes extension natural.
