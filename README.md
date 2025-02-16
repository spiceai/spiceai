<p align="center">
  <img src="https://github.com/user-attachments/assets/13ff4c9d-d6a7-4c20-9408-45573c508c41" alt="spice oss logo" width="600"/>
</p>
<p align="center">
  <a href="https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml"><img src="https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push" alt="CodeQL"/></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License: Apache-2.0"/></a>
  <a href="https://discord.gg/kZnTfneP5u"><img src="https://img.shields.io/discord/803820740868571196" alt="Discord"/></a>
  <a href="https://x.com/intent/follow?screen_name=spice_ai"><img src="https://img.shields.io/twitter/follow/spice_ai.svg?style=social&logo=x" alt="Follow on X"/></a>
</p>

<p align="center">
  <a href="https://github.com/spiceai/spiceai/actions/workflows/build_and_release.yml"><img alt="GitHub Actions Workflow Status - build" src="https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/build_and_release.yml?branch=trunk" /></a>
  <a href="https://github.com/spiceai/spiceai/actions/workflows/spiced_docker_nightly.yml"><img alt="GitHub Actions Workflow Status - docker build" src="https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/spiced_docker_nightly.yml?label=docker%20build" /></a>
  <a href="https://github.com/spiceai/spiceai/actions/workflows/pr.yml"><img alt="GitHub Actions Workflow Status - unit tests" src="https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/pr.yml?event=merge_group&label=unit%20tests" /></a>
  <a href="https://github.com/spiceai/spiceai/actions/workflows/integration.yml"><img alt="GitHub Actions Workflow Status - integration tests" src="https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/integration.yml?event=pull_request&label=integration%20tests" /></a>
  <a href="https://github.com/spiceai/spiceai/actions/workflows/integration_models.yml"><img alt="GitHub Actions Workflow Status - integration tests (models)" src="https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/integration_models.yml?event=pull_request&label=integration%20tests%20(models)" /></a>
  <a href="https://github.com/spiceai/spiceai/actions/workflows/benchmarks.yml"><img alt="GitHub Actions Workflow Status - benchmark tests" src="https://img.shields.io/github/actions/workflow/status/spiceai/spiceai/benchmarks.yml?branch=trunk&label=benchmark%20tests" /></a>
</p>

<p align="center">
  <a href="https://spiceai.org/docs">Documentation</a> | <a href="#installation">Installation</a> | <a href="https://github.com/spiceai/cookbook">Cookbook</a>
</p>

**Spice** is a SQL query and AI compute engine, written in Rust, for data-driven apps and agents.

<img width="740" alt="Spice.ai Open Source accelerated data query and LLM-inference engine" src="https://github.com/user-attachments/assets/9db94f9c-10a1-47b0-ab45-05aa964590ff" />

Spice provides three industry standard APIs in a lightweight, portable runtime (single ~140 MB binary):

1. **SQL Query APIs**: Arrow Flight, Arrow Flight SQL, ODBC, JDBC, and ADBC.
2. **OpenAI-Compatible APIs**: HTTP APIs compatible the OpenAI SDK, AI SDK with local model serving (CUDA/Metal accelerated) and gateway to hosted models.
3. **Iceberg Catalog REST APIs**: A unified Iceberg Catalog API.

🎯 Goal: Developers can focus on building data apps and AI agents confidently, knowing they are grounded in data.

Spice is primarily used for:

- **Data Federation**: SQL query across any database, data warehouse, or data lake. [Learn More](https://spiceai.org/docs/features/federated-queries).
- **Data Materialization and Acceleration**: Materialize, accelerate, and cache database queries. [Read the MaterializedView interview - Building a CDN for Databases](https://materializedview.io/p/building-a-cdn-for-databases-spice-ai)
- **AI apps and agents**: An AI-database powering retrieval-augmented generation (RAG) and intelligent agents. [Learn More](https://spiceai.org/docs/use-cases/rag).

If you want to build with DataFusion or using DuckDB, Spice provides a simple, flexible, and production-ready engine you can just use.

📣 Read the [Spice.ai OSS announcement blog post](https://blog.spiceai.org/posts/2024/03/28/adding-spice-the-next-generation-of-spice.ai-oss/).

Spice is built-on industry leading technologies including [Apache DataFusion](https://datafusion.apache.org), Apache Arrow, Arrow Flight, SQLite, and DuckDB.

<div align="center">
  <picture>
    <img width="600" alt="How Spice works." src="https://github.com/spiceai/spiceai/assets/80174/7d93ae32-d6d8-437b-88d3-d64fe089e4b7" />
  </picture>
</div>

🎥 [Watch the CMU Databases Accelerating Data and AI with Spice.ai Open-Source](https://www.youtube.com/watch?v=tyM-ec1lKfU)

## Why Spice?

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/spiceai/spiceai/assets/80174/96b5fcef-a550-4ce8-a74a-83931275e83e">
    <img width="800" alt="Spice.ai" src="https://github.com/spiceai/spiceai/assets/80174/29e4421d-8942-4f2a-8397-e9d4fdeda36b" />
  </picture>
</div>

Spice simplifies building data-driven AI applications and agents by making it fast and easy to query, federate, and accelerate data from one or more sources using SQL, while grounding AI in real-time, reliable data. Co-locate datasets with apps and AI models to power AI feedback loops, enable RAG and search, and deliver fast, low-latency data-query and AI-inference with full control over cost and performance.

### How is Spice different?

1. **AI-Native Runtime**: Spice combines data query and AI inference in a single engine, for data-grounded AI and accurate AI.

2. **Application-Focused**: Designed to run distributed at the application and agent level, often as a 1:1 or 1:N mapping between app and Spice instance, unlike traditional data systems built for many apps on one centralized database. It’s common to spin up multiple Spice instances—even one per tenant or customer.

3. **Dual-Engine Acceleration**: Supports both **OLAP** (Arrow/DuckDB) and **OLTP** (SQLite/PostgreSQL) engines at the dataset level, providing flexible performance across analytical and transactional workloads.

4. **Disaggregated Storage**: Separation of compute from disaggregated storage, co-locating local, materialized working sets of data with applications, dashboards, or ML pipelines while accessing source data in its original storage.

5. **Edge to Cloud Native**: Deploy as a standalone instance, Kubernetes sidecar, microservice, or cluster—across edge/POP, on-prem, and public clouds. Chain multiple Spice instances for tier-optimized, distributed deployments.

## How does Spice compare?

### Data Query and Analytics

| Feature                          | **Spice**                              | Trino / Presto       | Dremio                | ClickHouse          | Materialize          |
| -------------------------------- | -------------------------------------- | -------------------- | --------------------- | ------------------- | -------------------- |
| **Primary Use-Case**             | Data & AI apps/agents                  | Big data analytics   | Interactive analytics | Real-time analytics | Real-time analytics  |
| **Primary deployment model**     | Sidecar                                | Cluster              | Cluster               | Cluster             | Cluster              |
| **Federated Query Support**      | ✅                                     | ✅                   | ✅                    | ―                   | ―                    |
| **Acceleration/Materialization** | ✅ (Arrow, SQLite, DuckDB, PostgreSQL) | Intermediate storage | Reflections (Iceberg) | Materialized views  | ✅ (Real-time views) |
| **Catalog Support**              | ✅ (Iceberg, Unity Catalog)            | ✅                   | ✅                    | ―                   | ―                    |
| **Query Result Caching**         | ✅                                     | ✅                   | ✅                    | ✅                  | Limited              |
| **Multi-Modal Acceleration**     | ✅ (OLAP + OLTP)                       | ―                    | ―                     | ―                   | ―                    |
| **Change Data Capture (CDC)**    | ✅ (Debezium)                          | ―                    | ―                     | ―                   | ✅ (Debezium)        |

### AI Apps and Agents

| Feature                       | **Spice**                            | LangChain          | LlamaIndex | AgentOps.ai      | Ollama                        |
| ----------------------------- | ------------------------------------ | ------------------ | ---------- | ---------------- | ----------------------------- |
| **Primary Use-Case**          | Data & AI apps                       | Agentic workflows  | RAG apps   | Agent operations | LLM apps                      |
| **Programming Language**      | Any language (HTTP interface)        | JavaScript, Python | Python     | Python           | Any language (HTTP interface) |
| **Unified Data + AI Runtime** | ✅                                   | ―                  | ―          | ―                | ―                             |
| **Federated Data Query**      | ✅                                   | ―                  | ―          | ―                | ―                             |
| **Accelerated Data Access**   | ✅                                   | ―                  | ―          | ―                | ―                             |
| **Tools/Functions**           | ✅                                   | ✅                 | ✅         | Limited          | Limited                       |
| **LLM Memory**                | ✅                                   | ✅                 | ―          | ✅               | ―                             |
| **Evaluations (Evals)**       | ✅                                   | Limited            | ―          | Limited          | ―                             |
| **Search**                    | ✅ (VSS)                             | ✅                 | ✅         | Limited          | Limited                       |
| **Caching**                   | ✅ (Query and results caching)       | Limited            | ―          | ―                | ―                             |
| **Embeddings**                | ✅ (Built-in & pluggable models/DBs) | ✅                 | ✅         | Limited          | ―                             |

✅ = Fully supported
❌ = Not supported
Limited = Partial or restricted support

## Example Use-Cases

### Data-grounded Agentic AI Applications

- **OpenAI-compatible API**: Connect to hosted models (OpenAI, Anthropic, xAI) or deploy locally (Llama, NVIDIA NIM). [AI Gateway Recipe](https://github.com/spiceai/cookbook/blob/trunk/openai_sdk/README.md)
- **Federated Data Access**: Query using SQL and NSQL (text-to-SQL) across databases, data warehouses, and data lakes with advanced query push-down for fast retrieval across disparate data sources. [Federated SQL Query Recipe](https://github.com/spiceai/cookbook/blob/trunk/federation/README.md)
- **Search and RAG**: Search and retrieve context with accelerated embeddings for retrieval-augmented generation (RAG) workflows. [Vector Search over GitHub Files](https://github.com/spiceai/cookbook/blob/trunk/search_github_files/README.md)
- **LLM Memory and Observability**: Store and retrieve history and context for AI agents while gaining deep visibility into data flows, model performance, and traces. [LLM Memory Recipe](https://github.com/spiceai/cookbook/blob/trunk/llm-memory/README.md) | [Monitoring Features Documentation](https://spiceai.org/docs/features/monitoring)

### Database CDN and Query Mesh

- **Data Acceleration**: Co-locate materialized datasets in Arrow, SQLite, and DuckDB with applications for sub-second query. [DuckDB Data Accelerator Recipe](https://github.com/spiceai/cookbook/blob/trunk/duckdb/accelerator/README.md)
- **Resiliency and Local Dataset Replication**: Maintain application availability with local replicas of critical datasets. [Local Dataset Replication Recipe](https://github.com/spiceai/cookbook/blob/trunk/localpod/README.md)
- **Responsive Dashboards**: Enable fast, real-time analytics by accelerating data for frontends and BI tools. [Sales BI Dashboard Demo](https://github.com/spiceai/cookbook/blob/trunk/sales-bi/README.md)
- **Simplified Legacy Migration**: Use a single endpoint to unify legacy systems with modern infrastructure, including federated SQL querying across multiple sources. [Federated SQL Query Recipe](https://github.com/spiceai/cookbook/blob/trunk/federation/README.md)

### Retrieval-Augmented Generation (RAG)

- **Unified Search with Vector Similarity**: Perform efficient vector similarity search across structured and unstructured data sources. [Vector Search over GitHub Files](https://github.com/spiceai/cookbook/blob/trunk/search_github_files/README.md)
- **Semantic Knowledge Layer**: Define a semantic context model to enrich data for AI. [Semantic Model Feature Documentation](https://spiceai.org/docs/features/semantic-model)
- **Text-to-SQL**: Convert natural language queries into SQL using built-in NSQL and sampling tools for accurate query. [Text-to-SQL Recipe](https://github.com/spiceai/cookbook/blob/trunk/text-to-sql/README.md)
- **Model and Data Evaluations**: Assess model performance and data quality with integrated evaluation tools. [Language Model Evaluations Recipe](https://github.com/spiceai/cookbook/blob/trunk/evals/README.md)

## FAQ

- **Is Spice a cache?** No specifically; you can think of Spice data acceleration as an _active_ cache, materialization, or data prefetcher. A cache would fetch data on a cache-miss while Spice prefetches and materializes filtered data on an interval, trigger, or as data changes using CDC. In addition to acceleration Spice supports [results caching](https://spiceai.org/docs/features/caching).

- **Is Spice a CDN for databases?** Yes, a common use-case for Spice is as a CDN for different data sources. Using CDN concepts, Spice enables you to ship (load) a working set of your database (or data lake, or data warehouse) where it's most frequently accessed, like from a data-intensive application or for AI context.

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
| `postgres`                         | PostgreSQL                            | Stable            |                              |
| `s3`                               | [S3][s3]                              | Stable            | Parquet, CSV                 |
| `mysql`                            | MySQL                                 | Stable            |                              |
| `graphql`                          | GraphQL                               | Release Candidate | JSON                         |
| `spice.ai`                         | [Spice.ai][spiceai]                   | Release Candidate | Arrow Flight                 |
| `databricks (mode: spark_connect)` | [Databricks][databricks]              | Beta              | [Spark Connect][spark]       |
| `flightsql`                        | FlightSQL                             | Beta              | Arrow Flight SQL             |
| `iceberg`                          | [Apache Iceberg][iceberg]             | Beta              | Parquet                      |
| `mssql`                            | Microsoft SQL Server                  | Beta              | Tabular Data Stream (TDS)    |
| `odbc`                             | ODBC                                  | Beta              | ODBC                         |
| `snowflake`                        | Snowflake                             | Beta              | Arrow                        |
| `spark`                            | Spark                                 | Beta              | [Spark Connect][spark]       |
| `abfs`                             | Azure BlobFS                          | Alpha             | Parquet, CSV                 |
| `clickhouse`                       | Clickhouse                            | Alpha             |                              |
| `debezium`                         | Debezium CDC                          | Alpha             | Kafka + JSON                 |
| `dynamodb`                         | Amazon DynamoDB                       | Alpha             |                              |
| `ftp`, `sftp`                      | FTP/SFTP                              | Alpha             | Parquet, CSV                 |
| `http`, `https`                    | HTTP(s)                               | Alpha             | Parquet, CSV                 |
| `imap`                             | IMAP                                  | Alpha             | IMAP Emails                  |
| `localpod`                         | [Local dataset replication][localpod] | Alpha             |                              |
| `sharepoint`                       | Microsoft SharePoint                  | Alpha             | Unstructured UTF-8 documents |
| `mongodb`                          | MongoDB                               | Coming Soon       |                              |
| `elasticsearch`                    | ElasticSearch                         | Roadmap           |                              |

[databricks]: https://github.com/spiceai/cookbook/tree/trunk/databricks/delta_lake
[spark]: https://spark.apache.org/docs/latest/spark-connect-overview.html
[s3]: https://github.com/spiceai/cookbook/tree/trunk/s3#readme
[spiceai]: https://github.com/spiceai/cookbook/tree/trunk/spiceai#readme
[dremio]: https://github.com/spiceai/cookbook/tree/trunk/dremio#readme
[localpod]: https://github.com/spiceai/cookbook/blob/trunk/localpod/README.md
[iceberg]: https://github.com/spiceai/cookbook/tree/trunk/catalogs/iceberg#readme

## Supported Data Accelerators

| Name       | Description                      | Status            | Engine Modes     |
| ---------- | -------------------------------- | ----------------- | ---------------- |
| `arrow`    | [In-Memory Arrow Records][arrow] | Stable            | `memory`         |
| `duckdb`   | Embedded [DuckDB][duckdb]        | Stable            | `memory`, `file` |
| `postgres` | Attached [PostgreSQL][postgres]  | Release Candidate | N/A              |
| `sqlite`   | Embedded [SQLite][sqlite]        | Release Candidate | `memory`, `file` |

[arrow]: https://spiceai.org/docs/components/data-accelerators/arrow
[duckdb]: https://spiceai.org/docs/components/data-accelerators/duckdb
[postgres]: https://spiceai.org/docs/components/data-accelerators/postgres
[sqlite]: https://spiceai.org/docs/components/data-accelerators/sqlite

## Supported Model Providers

| Name          | Description                                  | Status            | ML Format(s) | LLM Format(s)                   |
| ------------- | -------------------------------------------- | ----------------- | ------------ | ------------------------------- |
| `openai`      | OpenAI (or compatible) LLM endpoint          | Release Candidate | -            | OpenAI-compatible HTTP endpoint |
| `file`        | Local filesystem                             | Release Candidate | ONNX         | GGUF, GGML, SafeTensor          |
| `huggingface` | Models hosted on HuggingFace                 | Release Candidate | ONNX         | GGUF, GGML, SafeTensor          |
| `spice.ai`    | Models hosted on the Spice.ai Cloud Platform |                   | ONNX         | OpenAI-compatible HTTP endpoint |
| `azure`       | Azure OpenAI                                 |                   | -            | OpenAI-compatible HTTP endpoint |
| `anthropic`   | Models hosted on Anthropic                   | Alpha             | -            | OpenAI-compatible HTTP endpoint |
| `xai`         | Models hosted on xAI                         | Alpha             | -            | OpenAI-compatible HTTP endpoint |

## Supported Embeddings Providers

| Name          | Description                         | Status            | ML Format(s) | LLM Format(s)\*                 |
| ------------- | ----------------------------------- | ----------------- | ------------ | ------------------------------- |
| `openai`      | OpenAI (or compatible) LLM endpoint | Release Candidate | -            | OpenAI-compatible HTTP endpoint |
| `file`        | Local filesystem                    | Release Candidate | ONNX         | GGUF, GGML, SafeTensor          |
| `huggingface` | Models hosted on HuggingFace        | Release Candidate | ONNX         | GGUF, GGML, SafeTensor          |
| `azure`       | Azure OpenAI                        | Alpha             | -            | OpenAI-compatible HTTP endpoint |

## Supported Catalogs

Catalog Connectors connect to external catalog providers and make their tables available for federated SQL query in Spice. Configuring accelerations for tables in external catalogs is not supported. The schema hierarchy of the external catalog is preserved in Spice.

| Name            | Description             | Status      | Protocol/Format              |
| --------------- | ----------------------- | ----------- | ---------------------------- |
| `unity_catalog` | Unity Catalog           | Stable      | Delta Lake                   |
| `databricks`    | Databricks              | Beta        | Spark Connect, S3/Delta Lake |
| `iceberg`       | Apache Iceberg          | Beta        | Parquet                      |
| `spice.ai`      | Spice.ai Cloud Platform | Beta        | Arrow Flight                 |
| `glue`          | AWS Glue                | Coming Soon | JSON, Parquet, Iceberg       |

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
2025-01-20T19:26:10.682080Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
2025-01-20T19:26:10.879126Z  INFO runtime::init::results_cache: Initialized results cache; max size: 128.00 MiB, item ttl: 1s
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

The SQL REPL inferface will be shown:

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

## ⚙️ Runtime Container Deployment

Using the [Docker image](https://hub.docker.com/r/spiceai/spiceai) locally:

```bash
docker pull spiceai/spiceai
```

In a Dockerfile:

```dockerfile
from spiceai/spiceai:latest
```

Using Helm:

```bash
helm repo add spiceai https://helm.spiceai.org
helm install spiceai spiceai/spiceai
```

## 🏎️ Next Steps

### Explore the Spice.ai Cookbook

The Spice.ai Cookbook is a collection of recipes and examples for using Spice. Find it at [https://github.com/spiceai/cookbook](https://github.com/spiceai/cookbook#readme).

### Using Spice.ai Cloud Platform

Access ready-to-use Spicepods and datasets hosted on the Spice.ai Cloud Platform using the Spice runtime. A list of public Spicepods is available on Spicerack: [https://spicerack.org/](https://spicerack.org/).

To use public datasets, create a free account on Spice.ai:

1. Visit [spice.ai](https://spice.ai/) and click **Try for Free**.
   ![Try for Free](https://github.com/spiceai/spiceai/assets/112157037/27fb47ed-4825-4fa8-94bd-48197406cfaa)

2. After creating an account, create an app to generate an API key.
   ![Create App](https://github.com/spiceai/spiceai/assets/112157037/d2446406-1f06-40fb-8373-1b6d692cb5f7)

Once set up, you can access ready-to-use Spicepods including datasets. For this demonstration, use the `taxi_trips` dataset from the [Spice.ai Quickstart](https://spice.ai/spiceai/quickstart).

**Step 1.** Initialize a new project.

```bash
# Initialize a new Spice app
spice init spice_app

# Change to app directory
cd spice_app
```

**Step 2.** Log in and authenticate from the command line using the `spice login` command. A pop up browser window will prompt you to authenticate:

```bash
spice login
```

**Step 3.** Start the runtime:

```bash
# Start the runtime
spice run
```

**Step 4.** Configure the dataset:

In a new terminal window, configure a new dataset using the `spice dataset configure` command:

```bash
spice dataset configure
```

Enter a dataset name that will be used to reference the dataset in queries. This name does not need to match the name in the dataset source.

```bash
dataset name: (spice_app) taxi_trips
```

Enter the description of the dataset:

```bash
description: Taxi trips dataset
```

Enter the location of the dataset:

```bash
from: spice.ai/spiceai/quickstart/datasets/taxi_trips
```

Select `y` when prompted whether to accelerate the data:

```bash
Locally accelerate (y/n)? y
```

You should see the following output from your runtime terminal:

```bash
2024-12-16T05:12:45.803694Z  INFO runtime::init::dataset: Dataset taxi_trips registered (spice.ai/spiceai/quickstart/datasets/taxi_trips), acceleration (arrow, 10s refresh), results cache enabled.
2024-12-16T05:12:45.805494Z  INFO runtime::accelerated_table::refresh_task: Loading data for dataset taxi_trips
2024-12-16T05:13:24.218345Z  INFO runtime::accelerated_table::refresh_task: Loaded 2,964,624 rows (8.41 GiB) for dataset taxi_trips in 38s 412ms.
```

**Step 5.** In a new terminal window, use the Spice SQL REPL to query the dataset

```bash
spice sql
```

```bash
SELECT tpep_pickup_datetime, passenger_count, trip_distance from taxi_trips LIMIT 10;
```

The output displays the results of the query along with the query execution time:

```bash
+----------------------+-----------------+---------------+
| tpep_pickup_datetime | passenger_count | trip_distance |
+----------------------+-----------------+---------------+
| 2024-01-11T12:55:12  | 1               | 0.0           |
| 2024-01-11T12:55:12  | 1               | 0.0           |
| 2024-01-11T12:04:56  | 1               | 0.63          |
| 2024-01-11T12:18:31  | 1               | 1.38          |
| 2024-01-11T12:39:26  | 1               | 1.01          |
| 2024-01-11T12:18:58  | 1               | 5.13          |
| 2024-01-11T12:43:13  | 1               | 2.9           |
| 2024-01-11T12:05:41  | 1               | 1.36          |
| 2024-01-11T12:20:41  | 1               | 1.11          |
| 2024-01-11T12:37:25  | 1               | 2.04          |
+----------------------+-----------------+---------------+

Time: 0.00538925 seconds. 10 rows.
```

You can experiment with the time it takes to generate queries when using non-accelerated datasets. You can change the acceleration setting from `true` to `false` in the datasets.yaml file.

### 📄 Documentation

Comprehensive documentation is available at [spiceai.org/docs](https://spiceai.org/docs/).

Over 45 quickstarts and samples available in the [Spice Cookbook](https://github.com/spiceai/cookbook#spiceai-oss-cookbook).

### 🔌 Extensibility

Spice.ai is designed to be extensible with extension points documented at [EXTENSIBILITY.md](./docs/EXTENSIBILITY.md). Build custom [Data Connectors](https://spiceai.org/docs/components/data-connectors), [Data Accelerators](https://spiceai.org/docs/components/data-accelerators), [Catalog Connectors](https://spiceai.org/docs/components/catalogs), [Secret Stores](https://spiceai.org/docs/components/secret-stores), [Models](https://spiceai.org/docs/components/models), or [Embeddings](https://spiceai.org/docs/components/embeddings).

### 🔨 Upcoming Features

🚀 See the [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md) for upcoming features.

### 🤝 Connect with us

We greatly appreciate and value your support! You can help Spice in a number of ways:

- Build an app with Spice.ai and send us feedback and suggestions at [hey@spice.ai](mailto:hey@spice.ai) or on [Discord](https://discord.gg/kZnTfneP5u), [X](https://twitter.com/spice_ai), or [LinkedIn](https://www.linkedin.com/company/74148478).
- [File an issue](https://github.com/spiceai/spiceai/issues/new) if you see something not quite working correctly.
- Join our team ([We’re hiring!](https://spice.ai/careers))
- Contribute code or documentation to the project (see [CONTRIBUTING.md](CONTRIBUTING.md)).
- Follow our blog at [blog.spiceai.org](https://blog.spiceai.org)

⭐️ star this repo! Thank you for your support! 🙏
