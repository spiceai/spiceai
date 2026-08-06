<p align="center">
  <img src="https://github.com/user-attachments/assets/13ff4c9d-d6a7-4c20-9408-45573c508c41" alt="spice oss logo" width="600"/>
</p>
<div align="center">

[![CodeQL](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml?query=branch%3Atrunk+event%3Apush)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Slack](https://img.shields.io/badge/Slack-Join%20Us-4A154B?logo=slack)](https://spice.ai/slack)
[![Follow on X](https://img.shields.io/twitter/follow/spice_ai.svg?style=social&logo=x)](https://x.com/intent/follow?screen_name=spice_ai)
[![GitHub stars](https://img.shields.io/github/stars/spiceai/spiceai?style=social)](https://github.com/spiceai/spiceai/stargazers)

</div>

<div align="center">

[![GitHub Actions Workflow Status - build](https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/build_nightly.yml?branch=trunk&label=build)](https://github.com/spiceai/spiceai/actions/workflows/build_nightly.yml?query=branch%3Atrunk)
[![GitHub Actions Workflow Status - docker build](https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/spiced_docker_dev.yml?branch=trunk&label=docker%20build)](https://github.com/spiceai/spiceai/actions/workflows/spiced_docker_dev.yml?query=branch%3Atrunk)
[![GitHub Actions Workflow Status - unit tests](https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/build_and_release.yml?branch=trunk&label=unit%20tests)](https://github.com/spiceai/spiceai/actions/workflows/build_and_release.yml?query=branch%3Atrunk)
[![GitHub Actions Workflow Status - integration tests](https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/integration.yml?branch=trunk&label=integration%20tests)](https://github.com/spiceai/spiceai/actions/workflows/integration.yml?query=branch%3Atrunk)

</div>

<p align="center">
  <a href="https://spiceai.org/docs">📄 Docs</a> | <a href="#%EF%B8%8F-quickstart-local-machine">⚡️ Quickstart</a> | <a href="https://github.com/spiceai/cookbook">🧑‍🍳 Cookbook</a> | <a href="https://github.com/spiceai/skills">🤖 AI Skills</a> | <a href="https://spice.ai/blog">📰 Blog</a>
</p>

**Spice** is a portable, accelerated SQL query, search, and LLM-inference engine, written in Rust, for data-grounded AI apps and agents. Run it as a sidecar next to your application — or scale to a multi-node distributed cluster — to get **millisecond data and AI on localhost**, backed by your existing data sources.

<img width="740" alt="Spice.ai Open Source accelerated data query and LLM-inference engine" src="https://github.com/user-attachments/assets/9db94f9c-10a1-47b0-ab45-05aa964590ff" />

🎯 **Goal**: Build data-grounded AI apps and agents in minutes, not months. No pipelines. No glue. Just SQL, search, and inference — federated across your data, accelerated locally, served on localhost.

> 🆕 **New in Spice 2.0 — add a real-time analytics node to your operational database.** Point Spice at **PostgreSQL, MySQL, or MongoDB** and it maintains a sandboxed, analytics-ready replica with high-throughput **CDC replication** — **sub-second queries, ~2-second freshness, and zero analytical load on production**. No ETL, no Debezium, no Kafka required. [Read the Spice 2.0 launch →](https://spice.ai/blog/spice-2-0-is-now-available)

## Why Spice?

- ⚡ **Real-time analytics node for your operational database** — Add a sandboxed analytics replica to **PostgreSQL, MySQL, and MongoDB** via native CDC (WAL, binlog, change streams) plus DynamoDB Streams — **~2-second freshness, zero load on production, no ETL, no Debezium or Kafka required**.
- 🚀 **Localhost latency at any scale** — Millisecond queries against a sandboxed working set on each pod, transparently delegated to a distributed cluster for the long tail.
- 🦀 **Built in Rust** on industry-leading open foundations: [Apache DataFusion](https://datafusion.apache.org), [Apache Ballista](https://datafusion.apache.org/ballista/), [Apache Arrow](https://arrow.apache.org), [Apache Iceberg](https://iceberg.apache.org), [Vortex](https://github.com/vortex-data/vortex), [DuckDB](https://duckdb.org), and [SQLite](https://www.sqlite.org).
- ⚡ **Distributed query without the operational tax** — Apache Ballista with multi-active schedulers coordinated through object storage. **2.9x faster than single-node DataFusion** on TPC-H SF100, **8x less RAM than Spark**.
- 💎 **Spice Cayenne** data accelerator on Vortex (GA) — **1.5x faster than DuckDB with 3x less memory** on TPC-H SF100, **26x faster than Spice 1.x on TPC-DS SF100**, **100x faster random access vs. Parquet**.
- 🔍 **Petabyte-scale hybrid search** — Native Amazon S3 Vectors, Tantivy BM25, DuckDB HNSW, and Elasticsearch kNN, with reciprocal rank fusion (RRF) and reranker UDTFs — all in a single SQL query.
- 🤖 **AI-native runtime** — OpenAI-compatible APIs, MCP server + gateway, LLM memory, NSQL text-to-SQL, multi-vector ColBERT-style embeddings, provider-aware prompt caching.
- 🔗 **30+ data connectors** with advanced query push-down — federate Postgres, MySQL, Snowflake, Databricks, Iceberg, Delta Lake, S3, Spark, MSSQL, DynamoDB, MongoDB, GitHub, SharePoint, Kafka, and more.
- 📝 **Open table formats, first-class** — Query, **accelerate, and write** to Apache Iceberg with ACID guarantees via standard SQL `INSERT INTO`. No Spark required.
- 🛡️ **Enterprise-ready** — HashiCorp Vault and Azure Key Vault secret stores, mTLS, read-only API keys, observability via OpenTelemetry, and an extensibility model used in production at companies like Twilio and Barracuda.

📣 **Latest:** [**Spice 2.0 is now available**](https://spice.ai/blog/spice-2-0-is-now-available) — real-time analytical query on operational data, without ETL: **~170x faster CDC ingest**, **2-second freshness**, **1,046 QPH of HTAP analytics at SF1000 under a 266,000+ tpmC live transactional load**. | Read the [Cluster-Sidecar Architecture](https://spice.ai/blog/cluster-sidecar-architecture) and [Apache Ballista](https://spice.ai/blog/apache-ballista-at-spice-ai) deep dives.

<div align="center">
  <picture>
    <img width="600" alt="How Spice works." src="https://github.com/spiceai/spiceai/assets/80174/7d93ae32-d6d8-437b-88d3-d64fe089e4b7" />
  </picture>
</div>

## What you get

Spice provides five APIs and interfaces in a lightweight, portable runtime (single binary or container):

1. **SQL Query & Search**: HTTP, Arrow Flight, Arrow Flight SQL, ODBC, JDBC, and ADBC APIs; `vector_search`, `text_search`, `rrf`, and `rerank` UDTFs.
2. **Text-to-SQL (NSQL)**: Natural-language SQL generation grounded in your federated schema with built-in sampling tools — usable from the HTTP API, the SQL REPL, or directly inside agent tool calls.
3. **OpenAI-Compatible APIs**: Hosted LLM gateway (OpenAI, Anthropic, xAI, Bedrock) and local model serving (CUDA/Metal accelerated). Includes the OpenAI Responses API, web search, and tool calls.
4. **Iceberg Catalog REST APIs**: A unified Iceberg REST Catalog API for query and write.
5. **MCP HTTP+SSE APIs**: Model Context Protocol server *and* gateway with Streamable HTTP transport.

## 🎥 Watch & Learn

- 🎓 [**CMU Databases: Accelerating Data and AI with Spice.ai Open-Source**](https://www.youtube.com/watch?v=tyM-ec1lKfU) — Luke Kim at the Carnegie Mellon Database Group
- ☁️ [**AWS re:Invent 2025 (STG364): How Spice AI operationalizes data lakes for AI using Amazon S3**](https://www.youtube.com/watch?v=KuWI0yDOnIU)
- 🔍 [**How to search with Amazon S3 Vectors**](https://www.youtube.com/watch?v=QPbqPf5W36g)
- 💎 [**Introducing the Spice Cayenne Data Accelerator**](https://www.youtube.com/watch?v=HTdv6-cxKV4)
- 🧊 [**Writing to Apache Iceberg Tables with Spice.ai**](https://www.youtube.com/watch?v=LGGFmNN9-3w)
- 🔌 [**Using Spice as an MCP Server and Gateway**](https://www.youtube.com/watch?v=0Cm-_xqVBFU)
- 🛠️ [**How to Query Data using Spice, OpenAI, and MCP**](https://www.youtube.com/watch?v=TFAu4qxjTPk)

📺 More on the [Spice.ai YouTube channel](https://www.youtube.com/playlist?list=PLesJrUXEx3U9anekJvbjyyTm7r9A26ugK).

## What's New

### Analytics node for operational databases — real-time CDC, no ETL

Add a sandboxed, analytics-ready replica alongside **PostgreSQL, MySQL, and MongoDB** in minutes — **~2-second end-to-end freshness, zero analytical load on production, and no ETL**. Spice replicates committed inserts, updates, and deletes directly from the native change log at up to **~170x the ingest throughput of Spice 1.x**, so production never runs a single analytical query. It's incrementally adoptable: start with **1 table** and be querying operational data in minutes, then join across replicated sources in a single SQL query. In the CH-BenCHmark HTAP benchmark, **1 Spice node served 1,046 analytical queries/hour at SF1000 (1,000 warehouses, 300M+ rows) while the source sustained a 266,000+ tpmC live transactional load**. [Read the Spice 2.0 launch →](https://spice.ai/blog/spice-2-0-is-now-available)

- **PostgreSQL (WAL), MySQL (binlog), and MongoDB (change streams)** — native replication with auto-managed replication state (slots, binlog positions, resume tokens) and bootstrapped initial snapshots. **No Debezium or Kafka required.**
- **DynamoDB Streams** — two-tier acceleration that fans out from a central Spice layer to thousands of edge sidecars with sub-second propagation. Used in production for global control-plane sync. [Read the pattern →](https://spice.ai/blog/real-time-acceleration-with-dynamodb-streams)
- **Debezium** — Kafka consumer (`from: debezium:…`) or **push ingest without Kafka** (`from: cdc:…` + `POST /v1/datasets/{name}/cdc`, JSON/Avro).

### Cluster-Sidecar Architecture: localhost latency, cluster scale

Each application gets a complete data plane on `localhost`. A lightweight Spice sidecar runs in the application pod, serves SQL/search/LLM-inference from a scoped working set, and transparently delegates the long tail to a central Spice cluster (Ballista distributed query, Cayenne acceleration, hybrid search indexing) over Arrow Flight. Three latency tiers: results cache (microseconds) → local working set (single-digit milliseconds) → cluster delegation. The application **never holds credentials** to Postgres, S3, Snowflake, or Iceberg — only a token to its sidecar. [Read the architecture deep dive →](https://spice.ai/blog/cluster-sidecar-architecture)

### Apache Ballista distributed query

Spice extends Apache Ballista with **multi-active scheduler HA coordinated through object storage** (no etcd, ZooKeeper, or Redis required), bidirectional gRPC control streams, mandatory mTLS, multiple shuffle backends (local, in-memory, S3/Azure/GCS), Vortex-encoded shuffle data, and distributed embeddings inside SQL. **TPC-H SF100: 2.9x faster on 3 executors than 1 node. 8x less RAM than Apache Spark with 2–8x better query performance** — now generally available. [Read the engineering deep dive →](https://spice.ai/blog/apache-ballista-at-spice-ai)

### Spice Cayenne — next-gen data acceleration on Vortex

Cayenne pairs the [Vortex columnar format](https://github.com/vortex-data/vortex) with SQLite metadata to deliver multi-file acceleration without DuckDB's single-file ceiling or memory overhead. **Now GA** with atomic WAL-staged writes, high-throughput CDC ingestion, `MERGE INTO`, and SQL-defined partitioning. **TPC-H SF100: 1.5x faster than DuckDB with 3x less memory. TPC-DS SF100: 26x faster than Spice 1.x. ClickBench: 14% faster, 3.4x less memory.** Vortex itself is **100x faster on random access**, **10–20x faster on full scans**, and **5x faster writes** than Parquet — compute kernels run directly on encoded data, skipping decompression entirely for many operations. [Read the Vortex deep dive →](https://spice.ai/blog/vortex-at-spice-ai-the-columnar-format-for-data-intensive-workloads)

### Apache Iceberg: query, accelerate, and write

Connect to any Iceberg catalog (REST, AWS Glue, Hadoop), query tables with full SQL semantics, selectively accelerate hot datasets for **sub-10ms reads** (down from 500ms–5s on S3), and write back with ACID guarantees via Iceberg's optimistic concurrency protocol — using standard SQL `INSERT INTO`. No Spark required. [Read the Iceberg deep dive →](https://spice.ai/blog/apache-iceberg-at-spice-ai)

### Petabyte-scale hybrid search

Native **Amazon S3 Vectors** (Day 1 launch partner) for billions of vectors at up to 90% lower cost than traditional vector DBs. Plus DuckDB HNSW and Elasticsearch kNN as `.vectors.engine` backends. Spice manages the full lifecycle — ingestion → embedding (AWS Bedrock, HuggingFace, OpenAI, Model2Vec for 500x faster static embeddings, multi-vector ColBERT-style late interaction with MaxSim) → indexing → query. SQL-integrated via `vector_search`, `text_search`, `rrf` (reciprocal rank fusion), and `rerank` UDTFs.

```sql
SELECT * FROM rerank(
  rrf(
    vector_search('docs', 'how does Spice accelerate Iceberg?'),
    text_search('docs', 'how does Spice accelerate Iceberg?')
  ),
  document => content
) LIMIT 10;
```

### Multi-tenancy for AI agents — without per-tenant pipelines

Spin up one Spice runtime per tenant or agent — each with its own sandboxed datasets, accelerators, secrets, and policies. Or share a runtime with config-level tenant isolation. Or do both with a hybrid model. The lightweight **~140MB** runtime makes "one Spicepod per tenant" actually viable — even at **thousands of tenants**. [Read the patterns →](https://spice.ai/blog/multi-tenancy-for-ai-agents-without-pipelines)

### Spice Skills for AI coding agents

Drop-in skills for Claude Code, Cursor, and any agent that supports the open Agent Skills format. Skills auto-activate to set up datasets, connect data sources, configure acceleration, run federated queries, and wire models — without you re-explaining Spice's configuration model.

In Claude Code:

```text
/plugin marketplace add spiceai/skills
```

[github.com/spiceai/skills](https://github.com/spiceai/skills) | [Read the announcement →](https://spice.ai/blog/introducing-spice-skills-for-ai-coding-agents)

### Acceleration Snapshots

Bootstrap accelerated datasets from S3 in **seconds, not minutes**. Cold-start ephemeral pods with pre-built Vortex/DuckDB/SQLite files. Recover from federated source outages by serving from the last known good snapshot. Critical for sidecar deployments and serverless environments.

### Enterprise hardening (latest)

- **HashiCorp Vault** and **Azure Key Vault** secret stores
- **Read-only API keys** enforced on Flight DoGet and async query paths
- **Provider-aware LLM prompt caching** for cost reduction
- **mTLS** for all internal cluster communication; OpenTelemetry metric export with delta temporality
- **Streamable HTTP MCP transport**, MCP gateway, MCP server
- **30+ data connectors** with shared HTTP rate control, dynamic headers, schema decomposition

## How is Spice different?

1. **Cluster-sidecar architecture** — Each application gets its own Spice sidecar serving SQL, search, and LLM inference on `localhost`, transparently delegating the long tail to a central Spice cluster (Ballista distributed query, Cayenne acceleration, hybrid search indexing) over Arrow Flight. You get three latency tiers in one engine: **results cache (microseconds) → local working set (single-digit milliseconds) → cluster delegation (distributed)**. No other open-source runtime gives you all three behind one connection. [Read the architecture →](https://spice.ai/blog/cluster-sidecar-architecture)
2. **Structural data sandboxing** — Datasets a sidecar doesn't declare in its `spicepod.yaml` are *physically absent from the catalog*, not filtered at query time. The application never holds credentials to Postgres, S3, Snowflake, or Iceberg — only a token to its sidecar. A compromised pod gets a loopback scoped to that tenant's working set, not database credentials.
3. **Ingest once, serve everywhere** — The cluster ingests each source dataset once and produces one authoritative materialization that every sidecar pulls. Source systems see one stable connection pool, not one per pod. Pull-based refresh + acceleration snapshots in S3 mean cold starts in seconds and graceful degradation when the cluster is unreachable.
4. **AI-Native Runtime** — Data query and AI inference live in one engine, so retrieval, ranking, and generation happen in one query plan, in one process — `vector_search`, `text_search`, `rrf`, `rerank`, NSQL, and tool calls are all SQL primitives.
5. **Dual-engine acceleration** — Per-dataset choice of OLAP (Cayenne/Vortex, Arrow, DuckDB) and OLTP (SQLite, PostgreSQL) engines, so you can match workload to engine instead of forcing everything into one shape.
6. **Edge to cloud, single binary** — Runs on a laptop, as a Kubernetes sidecar, as a microservice, or as a multi-node Ballista cluster across edge, on-prem, and public clouds. Self-hosted OSS, Spice Cloud (managed cluster), and Spice.ai Enterprise (on-prem full stack) all use identical `spicepod.yaml` manifests — no app changes to migrate.

If you build with **DataFusion**, **DuckDB**, **Vortex**, **Iceberg**, or **Ballista**, Spice gives you a flexible, production-ready engine you can just use — instead of stitching them together yourself.

## Example Use-Cases

### Real-time Analytics on Operational Data (no ETL)

- **Analytics node for PostgreSQL, MySQL, and MongoDB**: Point Spice at a live operational database and it maintains a continuously updated, sandboxed analytics replica via native CDC — **sub-second queries, ~2-second freshness, and zero analytical queries against production**. Start with one table, then join across replicated sources in one SQL query. [CDC Docs](https://spiceai.org/docs/features/cdc)
- **HTAP at scale**: Sustain analytics and transactions on the same data — **1,046 analytical QPH at SF1000 under a 266,000+ tpmC transactional load** in CH-BenCHmark, all served from the replica. [Spice 2.0 launch →](https://spice.ai/blog/spice-2-0-is-now-available)
- **Bring your own BI tools**: Query the replica from Power BI, Tableau, Looker, and Apache Superset over Arrow Flight SQL, ODBC, and JDBC — or from Python and the Go, Rust, Java, and JavaScript SDKs.

### Data-grounded Agentic AI Applications

- **OpenAI-compatible AI Gateway**: Hosted (OpenAI, Anthropic, xAI, Bedrock) or local models (Llama, NVIDIA NIM) with Responses API, streaming tool calls, web search, and provider-aware prompt caching. [AI Gateway Recipe](https://github.com/spiceai/cookbook/blob/trunk/openai_sdk/README.md)
- **Federated Data Access**: SQL and NSQL (text-to-SQL) across 30+ sources with advanced push-down, scaling to multi-node Ballista. [Federated SQL Query Recipe](https://github.com/spiceai/cookbook/blob/trunk/federation/README.md)
- **Search and RAG**: Petabyte-scale vector search via Amazon S3 Vectors, BM25 full-text via Tantivy, ColBERT-style multi-vector embeddings with MaxSim, hybrid search with RRF, rerank UDTF. [Amazon S3 Vectors Recipe](https://github.com/spiceai/cookbook/tree/trunk/vectors/s3/README.md)
- **LLM Memory and Observability**: Persistent agent memory + deep visibility into data flows, model performance, and traces. [LLM Memory Recipe](https://github.com/spiceai/cookbook/blob/trunk/llm-memory/README.md) | [Observability Docs](https://spiceai.org/docs/features/observability)

### Database CDN and Query Mesh

- **Co-located acceleration**: Materialize working sets as Cayenne (Vortex), Arrow, SQLite, DuckDB, or Postgres alongside your app for sub-second query. Bootstrap from S3 snapshots. [DuckDB Accelerator Recipe](https://github.com/spiceai/cookbook/blob/trunk/duckdb/accelerator/README.md)
- **Resiliency**: Maintain availability with local replicas of critical datasets; recover from source outages from snapshots. [Local Dataset Replication Recipe](https://github.com/spiceai/cookbook/blob/trunk/localpod/README.md)
- **Responsive dashboards**: Sub-second BI with configurable refresh and CDC. [Sales BI Demo](https://github.com/spiceai/cookbook/blob/trunk/sales-bi/README.md)
- **Legacy modernization**: One endpoint that federates legacy systems with modern infrastructure. [Federation Recipe](https://github.com/spiceai/cookbook/blob/trunk/federation/README.md)

### Multi-Tenant AI Agents

- **One Spicepod per tenant or per agent** — sandboxed datasets, sources, secrets, and policies per agent. The runtime is light enough to make this actually viable. [Patterns →](https://spice.ai/blog/multi-tenancy-for-ai-agents-without-pipelines)

### Retrieval-Augmented Generation (RAG)

- **Hybrid search in SQL**: Combine vector + BM25 with RRF and rerank, in one query plan, against your own data — accelerated.
- **Semantic Knowledge Layer**: Define a semantic context model so agents understand the shape and meaning of your data. [Semantic Model Docs](https://spiceai.org/docs/features/semantic-model)
- **Text-to-SQL**: Built-in NSQL with sampling tools for grounded SQL generation. [Text-to-SQL Recipe](https://github.com/spiceai/cookbook/blob/trunk/text-to-sql/README.md)

## FAQ

- **Is Spice a cache?** Not exactly — think of Spice acceleration as an *active* cache: a materialization or data prefetcher. A cache fetches on miss; Spice prefetches and materializes filtered data on an interval, trigger, or via CDC. Spice also supports [results caching](https://spiceai.org/docs/features/caching).
- **Is Spice a CDN for databases?** Yes — a common use-case is shipping a working set of a database, data lake, or data warehouse to where it's most frequently accessed: data-intensive applications and AI context.
- **Can I use Spice without Spice Cloud?** Yes, the entire runtime is open-source under Apache 2.0. Spice Cloud is an optional managed cluster.

[➡️ Docs FAQ](https://spiceai.org/docs/faq)

### Watch a 30-sec BI dashboard acceleration demo

<https://github.com/spiceai/spiceai/assets/80174/7735ee94-3f4a-4983-a98e-fe766e79e03a>

See more demos on [YouTube](https://www.youtube.com/playlist?list=PLesJrUXEx3U9anekJvbjyyTm7r9A26ugK).

## Supported Data Connectors

| Name                               | Description                           | Status            | Protocol/Format              |
| ---------------------------------- | ------------------------------------- | ----------------- | ---------------------------- |
| `databricks (mode: delta_lake)`    | [Databricks][databricks]              | Stable            | S3/Delta Lake                |
| `delta_lake`                       | Delta Lake                            | Stable            | Delta Lake                   |
| `dremio`                           | [Dremio][dremio]                      | Stable            | Arrow Flight                 |
| `duckdb`                           | DuckDB                                | Stable            | Embedded                     |
| `file`                             | File                                  | Stable            | Parquet, CSV                 |
| `github`                           | GitHub                                | Stable            | GitHub API                   |
| `postgres`                         | PostgreSQL (with native WAL CDC)      | Stable            |                              |
| `s3`                               | [S3][s3]                              | Stable            | Parquet, CSV                 |
| `mysql`                            | MySQL (with native binlog CDC)        | Stable            |                              |
| `spice.ai`                         | [Spice.ai][spiceai]                   | Stable            | Arrow Flight                 |
| `dynamodb`                         | Amazon DynamoDB (with Streams)        | Stable            |                              |
| `graphql`                          | GraphQL                               | Release Candidate | JSON                         |
| `cosmosdb`                         | Azure Cosmos DB (NoSQL)               | Release Candidate |                              |
| `git`                              | Git repositories                      | Release Candidate |                              |
| `snowflake`                        | Snowflake                             | Release Candidate | Arrow                        |
| `adbc`                             | ADBC                                  | Release Candidate | Arrow                        |
| `iceberg`                          | [Apache Iceberg][iceberg] (read+write) | Release Candidate | Parquet                      |
| `databricks (mode: spark_connect)` | [Databricks][databricks]              | Beta              | [Spark Connect][spark]       |
| `ducklake`                         | [DuckLake][ducklake]                  | Beta              | Parquet                      |
| `flightsql`                        | FlightSQL                             | Beta              | Arrow Flight SQL             |
| `mssql`                            | Microsoft SQL Server                  | Beta              | Tabular Data Stream (TDS)    |
| `odbc`                             | ODBC                                  | Beta              | ODBC                         |
| `spark`                            | Spark                                 | Beta              | [Spark Connect][spark]       |
| `sharepoint`                       | Microsoft SharePoint                  | Beta              | Object-store listing         |
| `oracle`                           | Oracle                                | Alpha             | [Oracle ODPI-C][ODPIC]       |
| `abfs`                             | Azure BlobFS                          | Alpha             | Parquet, CSV                 |
| `clickhouse`                       | ClickHouse                            | Alpha             |                              |
| `debezium`                         | Debezium CDC (Kafka consumer)         | Alpha             | Kafka + JSON                 |
| `cdc`                              | Debezium push ingest (no Kafka)       | Alpha             | JSON + Avro HTTP             |
| `elasticsearch`                    | Elasticsearch (BM25 + kNN + RRF)      | Alpha             |                              |
| `gcs`, `gs`                        | [Google Cloud Storage][gcs]           | Alpha             | Parquet, CSV, JSON           |
| `kafka`                            | Kafka                                 | Alpha             | Kafka + JSON                 |
| `ftp`, `sftp`                      | FTP/SFTP                              | Alpha             | Parquet, CSV                 |
| `glue`                             | [AWS Glue][glue]                      | Alpha             | Iceberg, Parquet, CSV        |
| `http`, `https`                    | HTTP(s) (dynamic headers, pagination) | Alpha             | Parquet, CSV, JSON           |
| `imap`                             | IMAP                                  | Alpha             | IMAP Emails                  |
| `localpod`                         | [Local dataset replication][localpod] | Alpha             |                              |
| `mongodb`                          | MongoDB (with change-stream CDC)      | Alpha             |                              |
| `scylladb`                         | ScyllaDB                              | Alpha             |                              |
| `smb`                              | SMB 3.1.1                             | Alpha             | SMB                          |
| `nfs`                              | NFS                                   | Alpha             | Parquet, CSV, JSON           |

[databricks]: https://github.com/spiceai/cookbook/blob/trunk/databricks/README.md
[ducklake]: https://ducklake.select/
[spark]: https://spark.apache.org/docs/latest/spark-connect-overview.html
[gcs]: docs/features/gcs-connector.md
[s3]: https://github.com/spiceai/cookbook/tree/trunk/s3#readme
[spiceai]: https://github.com/spiceai/cookbook/tree/trunk/spiceai#readme
[dremio]: https://github.com/spiceai/cookbook/tree/trunk/dremio#readme
[localpod]: https://github.com/spiceai/cookbook/blob/trunk/localpod/README.md
[iceberg]: https://github.com/spiceai/cookbook/tree/trunk/catalogs/iceberg#readme
[glue]: https://github.com/spiceai/cookbook/tree/trunk/glue/README.md
[ODPIC]: https://oracle.github.io/odpi/

## Supported Data Accelerators

| Name       | Description                       | Status            | Engine Modes     |
| ---------- | --------------------------------- | ----------------- | ---------------- |
| `cayenne`  | [Spice Cayenne (Vortex)][cayenne] | Stable            | `file`           |
| `arrow`    | [In-Memory Arrow Records][arrow]  | Stable            | `memory`         |
| `duckdb`   | Embedded [DuckDB][duckdb]         | Stable            | `memory`, `file` |
| `postgres` | Attached [PostgreSQL][postgres]   | Release Candidate | N/A              |
| `sqlite`   | Embedded [SQLite][sqlite]         | Release Candidate | `memory`, `file` |

[arrow]: https://spiceai.org/docs/components/data-accelerators/arrow
[cayenne]: https://spiceai.org/docs/components/data-accelerators/cayenne
[duckdb]: https://spiceai.org/docs/components/data-accelerators/duckdb
[postgres]: https://spiceai.org/docs/components/data-accelerators/postgres
[sqlite]: https://spiceai.org/docs/components/data-accelerators/sqlite

## Supported Model Providers

| Name          | Description                                  | Status            | ML Format(s) | LLM Format(s)                   |
| ------------- | -------------------------------------------- | ----------------- | ------------ | ------------------------------- |
| `openai`      | OpenAI (or compatible) LLM endpoint          | Release Candidate | -            | OpenAI-compatible HTTP endpoint |
| `file`        | Local filesystem                             | Release Candidate | ONNX         | GGUF, GGML, SafeTensor          |
| `huggingface` | Models hosted on HuggingFace                 | Release Candidate | ONNX         | GGUF, GGML, SafeTensor          |
| `spice.ai`    | Models hosted on the Spice.ai Cloud Platform, or served by another Spice runtime |                   | -            | OpenAI-compatible HTTP endpoint |
| `azure`       | Azure OpenAI                                 |                   | -            | OpenAI-compatible HTTP endpoint |
| `bedrock`     | Amazon Bedrock (Nova models)                 | Alpha             | -            | OpenAI-compatible HTTP endpoint |
| `anthropic`   | Models hosted on Anthropic                   | Alpha             | -            | OpenAI-compatible HTTP endpoint |
| `xai`         | Models hosted on xAI                         | Alpha             | -            | OpenAI-compatible HTTP endpoint |

## Supported Embeddings Providers

| Name          | Description                                  | Status            | ML Format(s) | LLM Format(s)                          |
| ------------- | -------------------------------------------- | ----------------- | ------------ | -------------------------------------- |
| `openai`      | OpenAI (or compatible) embeddings endpoint   | Release Candidate | -            | OpenAI-compatible embeddings endpoint  |
| `file`        | Local filesystem                             | Release Candidate | ONNX         | GGUF, GGML, SafeTensor                 |
| `huggingface` | Models hosted on HuggingFace                 | Release Candidate | ONNX         | GGUF, GGML, SafeTensor                 |
| `model2vec`   | Static embeddings (500x faster)              | Release Candidate | Model2Vec    | -                                      |
| `azure`       | Azure OpenAI                                 | Alpha             | -            | OpenAI-compatible HTTP endpoint        |
| `bedrock`     | AWS Bedrock (Titan, Cohere, Nova, Nova 2)    | Alpha             | -            | OpenAI-compatible HTTP endpoint        |

## Supported Vector Engines

Configured as `.vectors.engine` on a column-level embedding.

| Name            | Description                                                          | Status |
| --------------- | -------------------------------------------------------------------- | ------ |
| `s3_vectors`    | Amazon S3 Vectors for petabyte-scale vector storage and querying     | Alpha  |
| `duckdb`        | DuckDB with HNSW vector index                                        | Alpha  |
| `elasticsearch` | Elasticsearch with kNN                                               | Alpha  |

## Change Forwarding to Drasi (Alpha)

> **Alpha** — Drasi support is in preview and should not be used in production.

Configured as `.drasi` on a dataset accelerated with `refresh_mode: changes`. Publishes the dataset's change-data-capture stream to a [Drasi](https://drasi.io) source, so Drasi continuous queries react to the same changes Spice applies to the local accelerator. One row becomes one graph node: the primary key derives the element id, the source table name becomes the node label, and the row's columns become node properties.

Forwarding runs before the change is acknowledged to the source, so delivery is at-least-once — a change is replayed rather than lost if Drasi is unreachable.

```yaml
datasets:
  - from: postgres:public.orders
    name: orders
    acceleration:
      enabled: true
      engine: cayenne
      refresh_mode: changes
    drasi:
      source_id: spice-cdc
      delivery: queued          # or `acknowledged` (default)
      params:
        drasi_http_endpoint: http://localhost:9000
```

| Transport | Description                                                                    | Status |
| --------- | ------------------------------------------------------------------------------ | ------ |
| `http`    | Batched POST to a Drasi Server HTTP source                                     | Alpha  |
| `redis`   | CloudEvents envelopes on the Redis stream a Drasi platform source consumes      | Alpha  |

`delivery` selects when a change counts as handed off, which is the throughput/durability trade:

| `delivery` | Replication | On failure |
| ---------- | ----------- | ---------- |
| `acknowledged` (default) | Advances only once Drasi has the change, so nothing is lost — a stall or crash replays it. A slow or unreachable Drasi slows or stops replication. | `on_delivery_error`: `block` (default) retries indefinitely; `skip` gives up after a bounded budget and continues; `fail` stops the stream. |
| `queued` | Never waits for Drasi — the change is queued locally and the replication position acknowledged immediately, with delivery retried in the background. | Written to a durable dead-letter store under `.spice/data/drasi` and retried until it lands, surviving a restart. |

Use `queued` when Drasi is a downstream consumer whose availability should not pace replication; keep `acknowledged` when no change may be missed.

Under `queued`, the replication log is no longer what replays a failure — the dead-letter store is. Because an insert or update is a full-state replace keyed by element id, redelivery must not be overtaken by newer changes for the same row, so the store is stop-the-line: once anything is pending, later changes queue behind it and delivery resumes only once it drains. Drasi's view of a dataset advances in order or not at all. The store is capped (1024 batches per component); past that the oldest is discarded and counted, since the newest state for a row is the state worth keeping.

`forwarding: disabled` keeps a whole block in place without publishing anything, so it can be switched off and back on without reconstructing the endpoint, labels and keys.

Spice's own operational tables can be forwarded the same way, so continuous queries can react to events like a query exceeding its budget or a refresh failing. These are configured under `runtime` because they are not CDC-fed from an external source, and they default to `on_delivery_error: skip` — blocking the runtime's telemetry writer on a downstream outage buys nothing:

```yaml
runtime:
  drasi:
    source_id: spice-runtime
    params:
      drasi_http_endpoint: http://localhost:9000
    tables:
      - name: task_history
```

Runtime tables are always queued — they have no replication position to hold, so there is nothing for blocking to protect. Only the tables named are forwarded. A table's element id comes from its declared primary key (`task_history` uses `span_id`); a table that declares none — such as `runtime.metrics` — must name its identifying columns with `key:`, since a synthesized id would publish a duplicate node on every delivery retry.

## Supported Catalogs

Catalog Connectors connect to external catalog providers and make their tables available for federated SQL query in Spice. The schema hierarchy of the external catalog is preserved.

| Name            | Description             | Status | Protocol/Format              |
| --------------- | ----------------------- | ------ | ---------------------------- |
| `spice.ai`      | Spice.ai Cloud Platform | Stable | Arrow Flight                 |
| `unity_catalog` | Unity Catalog           | Stable | Delta Lake                   |
| `databricks`    | Databricks              | Beta   | Spark Connect, S3/Delta Lake |
| `iceberg`       | Apache Iceberg          | Beta   | Parquet                      |
| `ducklake`      | DuckLake                | Beta   | Parquet                      |
| `glue`          | AWS Glue                | Alpha  | CSV, Parquet, Iceberg        |
| `pg`            | PostgreSQL (with native WAL CDC catalog acceleration) | Alpha | PostgreSQL Wire Protocol |

## Supported Secret Stores

| Name                  | Description           | Status            |
| --------------------- | --------------------- | ----------------- |
| `env`                 | Environment variables | Stable            |
| `kubernetes`          | Kubernetes secrets    | Stable            |
| `keyring`             | OS keychain           | Stable            |
| `aws_secrets_manager` | AWS Secrets Manager   | Stable            |
| `hashicorp_vault`     | HashiCorp Vault       | Release Candidate |
| `azure_keyvault`      | Azure Key Vault       | Release Candidate |

## ⚡️ Quickstart (Local Machine)

<https://github.com/spiceai/spiceai/assets/88671039/85cf9a69-46e7-412e-8b68-22617dcbd4e0>

### Installation

Install the Spice CLI:

On **macOS, Linux, and WSL**:

```bash
curl https://install.spiceai.org | /bin/bash
```

Or using `brew`:

```bash
brew install spiceai/spiceai/spice
```

On **Windows** using PowerShell:

```powershell
iex ((New-Object System.Net.WebClient).DownloadString("https://install.spiceai.org/Install.ps1"))
```

> **Note:** Native Windows runtime builds are not provided in v2.0+. Use [WSL](https://learn.microsoft.com/en-us/windows/wsl/) for local development.

### Usage

**Step 1.** Initialize a new Spice app with the `spice init` command:

```bash
spice init spice_qs
```

A `spicepod.yaml` file is created in the `spice_qs` directory. Change to that directory:

```bash
cd spice_qs
```

**Step 2.** Start the Spice runtime:

```bash
spice run
```

Example output will be shown as follows:

```bash
2025/01/20 11:26:10 INFO Spice.ai runtime starting...
2025-01-20T19:26:10.679068Z  INFO runtime::init::dataset: No datasets were configured. If this is unexpected, check the Spicepod configuration.
2025-01-20T19:26:10.679716Z  INFO runtime::flight: Spice Runtime Flight listening on 127.0.0.1:50051
2025-01-20T19:26:10.679786Z  INFO runtime::metrics_server: Spice Runtime Metrics listening on 127.0.0.1:9090
2025-01-20T19:26:10.680140Z  INFO runtime::http: Spice Runtime HTTP listening on 127.0.0.1:8090
2025-01-20T19:26:10.879126Z  INFO runtime::init::results_cache: Initialized sql results cache; max size: 128.00 MiB, item ttl: 1s
```

The runtime is now started and ready for queries.

**Step 3.** In a new terminal window, add the `spiceai/quickstart` Spicepod. A Spicepod is a package of configuration defining datasets and ML models.

```bash
spice add spiceai/quickstart
```

The `spicepod.yaml` file will be updated with the `spiceai/quickstart` dependency.

```yaml
version: v1
kind: Spicepod
name: spice_qs
dependencies:
  - spiceai/quickstart
```

The `spiceai/quickstart` Spicepod will add a `taxi_trips` data table to the runtime which is now available to query by SQL.

```bash
2025-01-20T19:26:30.011633Z  INFO runtime::init::dataset: Dataset taxi_trips registered (s3://spiceai-demo-datasets/taxi_trips/2024/), acceleration (arrow), results cache enabled.
2025-01-20T19:26:30.013002Z  INFO runtime::accelerated_table::refresh_task: Loading data for dataset taxi_trips
2025-01-20T19:26:40.312839Z  INFO runtime::accelerated_table::refresh_task: Loaded 2,964,624 rows (399.41 MiB) for dataset taxi_trips in 10s 299ms
```

**Step 4.** Start the Spice SQL REPL:

```bash
spice sql
```

The SQL REPL interface will be shown:

```bash
Welcome to the Spice.ai SQL REPL! Type 'help' for help.

show tables; -- list available tables
sql>
```

Enter `show tables;` to display the available tables for query:

```bash
sql> show tables;
+---------------+--------------+---------------+------------+
| table_catalog | table_schema | table_name    | table_type |
+---------------+--------------+---------------+------------+
| spice         | public       | taxi_trips    | BASE TABLE |
| spice         | runtime      | query_history | BASE TABLE |
| spice         | runtime      | metrics       | BASE TABLE |
+---------------+--------------+---------------+------------+

Time: 0.022671708 seconds. 3 rows.
```

Enter a query to display the longest taxi trips:

```sql
SELECT trip_distance, total_amount FROM taxi_trips ORDER BY trip_distance DESC LIMIT 10;
```

Output:

```bash
+---------------+--------------+
| trip_distance | total_amount |
+---------------+--------------+
| 312722.3      | 22.15        |
| 97793.92      | 36.31        |
| 82015.45      | 21.56        |
| 72975.97      | 20.04        |
| 71752.26      | 49.57        |
| 59282.45      | 33.52        |
| 59076.43      | 23.17        |
| 58298.51      | 18.63        |
| 51619.36      | 24.2         |
| 44018.64      | 52.43        |
+---------------+--------------+

Time: 0.045150667 seconds. 10 rows.
```

## ⚙️ Container & Cluster Deployment

### Docker

```bash
docker pull spiceai/spiceai
```

```dockerfile
FROM spiceai/spiceai:latest
```

### Helm (Kubernetes)

```bash
helm repo add spiceai https://helm.spiceai.org
helm install spiceai spiceai/spiceai
```

### AWS Marketplace

Spice is available in the [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-jmf6jskjvnq7i).

### Distributed cluster (Apache Ballista)

Run Spice as a multi-node cluster: start scheduler nodes with `--role scheduler` and start executor nodes with `--scheduler-address <scheduler-url>` to join them. Multi-active schedulers coordinate through your object store (configured via `runtime.scheduler.state_location`) — no etcd, ZooKeeper, or Redis. mTLS certificates are managed via the Spice CLI. See the [Ballista architecture deep dive](https://spice.ai/blog/apache-ballista-at-spice-ai) and the [distributed query docs](https://spiceai.org/docs/features/query-federation).

## 🏎️ Next Steps

### Add Spice Skills to your AI coding agent

Drop-in skills for Claude Code, Cursor, and more.

In Claude Code (slash command):

```text
/plugin marketplace add spiceai/skills
```

In Cursor and other agents (shell):

```bash
npx skills add spiceai/skills
```

### Explore the Spice.ai Cookbook

86+ recipes and end-to-end examples — federation, acceleration, search, RAG, agents, CDC, and more — at [github.com/spiceai/cookbook](https://github.com/spiceai/cookbook#readme).

### Use the Spice.ai Cloud Platform (optional)

Access ready-to-use Spicepods and datasets hosted on the Spice.ai Cloud Platform with the open-source Spice runtime. Browse public Spicepods at [spicerack.org](https://spicerack.org/).

To use public datasets, create a free account on Spice.ai:

1. Visit [spice.ai](https://spice.ai/) and click **Try for Free**.
2. After creating an account, create an app to generate an API key.

Once set up, you can access ready-to-use Spicepods including datasets. For this demonstration, use the `taxi_trips` dataset from the [Spice.ai Quickstart](https://spice.ai/spiceai/quickstart).

**Step 1.** Initialize a new project.

```bash
spice init spice_app
cd spice_app
```

**Step 2.** Log in and authenticate. A pop-up browser window will prompt you to authenticate:

```bash
spice login
```

**Step 3.** Start the runtime:

```bash
spice run
```

**Step 4.** Configure the dataset:

In a new terminal window:

```bash
spice dataset configure
```

```bash
dataset name: (spice_app) taxi_trips
description: Taxi trips dataset
from: spice.ai/spiceai/quickstart/datasets/taxi_trips
Locally accelerate (y/n)? y
```

**Step 5.** Query from the SQL REPL:

```bash
spice sql
```

```sql
SELECT tpep_pickup_datetime, passenger_count, trip_distance from taxi_trips LIMIT 10;
```

### 📄 Documentation

Comprehensive documentation at [spiceai.org/docs](https://spiceai.org/docs/).

### 🔌 Extensibility

Spice.ai is designed to be extensible. See [EXTENSIBILITY.md](./docs/EXTENSIBILITY.md) to build custom [Data Connectors](https://spiceai.org/docs/components/data-connectors), [Data Accelerators](https://spiceai.org/docs/components/data-accelerators), [Catalog Connectors](https://spiceai.org/docs/components/catalogs), [Secret Stores](https://spiceai.org/docs/components/secret-stores), [Models](https://spiceai.org/docs/components/models), or [Embeddings](https://spiceai.org/docs/components/embeddings).

### 🔨 Releases & Roadmap

🚀 See the full [Roadmap](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md). Recent releases and what's next:

- **[v2.0](https://spiceai.org/releases/v2.0-stable)** (shipped, June 2026) — Spice Cayenne GA, multi-active HA distributed query GA, native CDC (PostgreSQL WAL, MongoDB change streams, Debezium), DML/DDL write-back, mTLS + OIDC, HashiCorp Vault & Azure Key Vault, and SQL/HTTP UDFs. [Read the launch →](https://spice.ai/blog/spice-2-0-is-now-available)
- **[v2.1](https://spiceai.org/releases/v2.1.0)** (shipped, July 2026) — High-throughput Cayenne CDC (in-memory tier + dedicated compaction runtime), PostgreSQL replication at scale (shared replication slot), distributed Iceberg scans and broadcast joins, DataFusion v54, tensor-parallel GLM inference, and adaptive self-tuning (experimental).
- **[v2.2](https://github.com/spiceai/spiceai/milestone/99)** (upcoming, targeting September 2026) — MySQL binlog CDC (already on `trunk`), webhooks, and reactive event-driven actions (Drasi-based).

### 🤝 Connect with us

- 📰 Read our [blog](https://spice.ai/blog) for engineering deep dives
- 💬 Join the conversation on [Slack](https://spice.ai/slack), [X](https://twitter.com/spice_ai), or [LinkedIn](https://www.linkedin.com/company/74148478)
- 🐛 [File an issue](https://github.com/spiceai/spiceai/issues/new) — we triage fast
- 💼 We're hiring! See [spice.ai/careers](https://spice.ai/careers)
- 🛠️ Contribute code or docs (see [CONTRIBUTING.md](CONTRIBUTING.md))
- ✉️ Send feedback to [hey@spice.ai](mailto:hey@spice.ai)

⭐️ **Star this repo** to follow along — it helps us a ton, and you'll see new releases as they ship. 🙏
