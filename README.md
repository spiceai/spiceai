<p align="center">
  <img src="https://github.com/user-attachments/assets/13ff4c9d-d6a7-4c20-9408-45573c508c41" alt="spice oss logo" width="600"/>
</p>
<p align="center">
  <a href="https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml"><img src="https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push" alt="CodeQL"/></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License: Apache-2.0"/></a>
  <a href="https://discord.gg/kZnTfneP5u"><img src="https://img.shields.io/discord/803820740868571196" alt="Discord"/></a>
  <a href="https://x.com/intent/follow?screen_name=spice_ai"><img src="https://img.shields.io/twitter/follow/spice_ai.svg?style=social&logo=x" alt="Follow on X"/></a>
</p>

**Spice** is a portable SQL query and AI compute engine, written in Rust, for data-driven apps and agents.

Spice provides three industry standard APIs in a single, lightweight runtime (single ~140 MB binary):

1. **SQL Query APIs**: Arrow Flight, Arrow Flight SQL, ODBC, JDBC, and ADBC.
2. **OpenAI-Compatible API**: OpenAI SDK and AI SDK compatible local model serving (CUDA/Metal accelerated) and gateway.
3. **Iceberg Catalog REST APIs**: A unified Iceberg Catalog API.

Spice is primarily used for:

- **Data Federation**: SQL query across any database, data warehouse, or data lake. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/federation#readme).
- **Data Materialization and Acceleration**: Materialize, accelerate, and cache database queries. 🎓 [Learn more in the MaterializedView interview - Building a CDN for Databases](https://materializedview.io/p/building-a-cdn-for-databases-spice-ai)
- **AI apps and agents**: An AI-database powering retrieval-augmented generation (RAG) workflows and intelligent agents. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/rag#readme).

📣 Read the [Spice.ai OSS announcement blog post](https://blog.spiceai.org/posts/2024/03/28/adding-spice-the-next-generation-of-spice.ai-oss/).

Spice is built-with industry leading technologies such as [Apache DataFusion](https://datafusion.apache.org), Apache Arrow, Apache Arrow Flight, SQLite, and DuckDB.

<div align="center">
  <picture>
    <img width="600" alt="How Spice works." src="https://github.com/spiceai/spiceai/assets/80174/7d93ae32-d6d8-437b-88d3-d64fe089e4b7" />
  </picture>
</div>

🎥 [Watch the CMU Databases Accelerating Data and AI with Spice.ai Open-Source](https://www.youtube.com/watch?v=tyM-ec1lKfU)

## Why Spice?

Spice simplifies building data-driven AI applications and agents by making it fast and easy to query, federate, and accelerate data from one or more sources using SQL, while grounding AI in real-time, reliable data. Co-locate datasets with apps and AI models to power AI feedback loops, enable RAG and search, and deliver fast, low-latency data-query and AI-inference with full control over cost and performance.

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/spiceai/spiceai/assets/80174/96b5fcef-a550-4ce8-a74a-83931275e83e">
    <img width="800" alt="Spice.ai" src="https://github.com/spiceai/spiceai/assets/80174/29e4421d-8942-4f2a-8397-e9d4fdeda36b" />
  </picture>
</div>

### How is Spice different?

1. **AI-Native Runtime**: Spice combines data query and AI inference in a single engine, enabling a data-grounded AI and fast-feedback for accurate, trustworthy AI.

2. **Application-Focused**: Designed to run distributed at the application and agent level, often as a 1:1 or 1:N mapping between app and Spice instance, unlike traditional data systems built for many apps on one centralized database. It’s common to spin up multiple Spice instances—even one per tenant or customer.

3. **Dual-Engine Acceleration**: Supports both **OLAP** (Arrow/DuckDB) and **OLTP** (SQLite/PostgreSQL) engines at the dataset level, providing flexible performance across analytical and transactional workloads.

4. **Disaggregated Storage**: Separation of compute from disaggregated storage, bringing local, materialized working sets of data close to applications, dashboards, or ML pipelines while keeping primary data in its original storage location.

5. **Edge to Cloud Native**: Deploy as a standalone instance, Kubernetes sidecar, microservice, or cluster—across edge/POP, on-prem, and public clouds. Chain multiple Spice instances for tier-optimized, distributed deployments.

## How does Spice compare?

### **Data Query and Analytics**

| Feature                          | **Spice**                              | Materialize          | Trino / Presto       | Dremio                | ClickHouse          |
| -------------------------------- | -------------------------------------- | -------------------- | -------------------- | --------------------- | ------------------- |
| **Primary Use-Case**             | Data & AI apps/agents                  | Real-time analytics  | Big data analytics   | Interactive analytics | Real-time analytics |
| **Federated Query Support**      | ✅                                     | ❌                   | ✅                   | ✅                    | ❌                  |
| **Acceleration/Materialization** | ✅ (Arrow, SQLite, DuckDB, PostgreSQL) | ✅ (Real-time views) | Intermediate storage | Reflections (Iceberg) | Materialized views  |
| **Catalog Support**              | ✅ (Iceberg, Unity Catalog)            | ❌                   | ✅                   | ✅                    | ❌                  |
| **Edge to Cloud Deployment**     | ✅                                     | ❌                   | ❌                   | ❌                    | ❌                  |
| **Query Result Caching**         | ✅                                     | Limited              | ✅                   | ✅                    | ✅                  |
| **Multi-Modal Acceleration**     | ✅ (OLAP + OLTP)                       | ❌                   | ❌                   | ❌                    | ❌                  |
| **Change Data Capture (CDC)**    | ✅ (Debezium)                          | ✅ (Debezium)        | ❌                   | ❌                    | ❌                  |

### **AI Apps and Agents**

| Feature                       | **Spice**                            | LangChain          | LlamaIndex | AgentOps.ai      | Ollama        |
| ----------------------------- | ------------------------------------ | ------------------ | ---------- | ---------------- | ------------- |
| **Primary Use-Case**          | Data & AI apps                       | Agentic workflows  | RAG apps   | Agent operations | LLM apps      |
| **Unified Data + AI Runtime** | ✅                                   | ❌                 | ❌         | ❌               | ❌            |
| **Federated Data Query**      | ✅                                   | ❌                 | ❌         | ❌               | ❌            |
| **Accelerated Data Access**   | ✅                                   | ❌                 | ❌         | ❌               | ❌            |
| **Tools/Functions**           | ✅                                   | ✅                 | ✅         | Limited tools    | Limited tools |
| **LLM Memory**                | ✅                                   | ✅                 | ❌         | ✅               | ❌            |
| **Evaluations (Evals)**       | ✅                                   | Limited            | ❌         | Limited          | ❌            |
| **Search**                    | ✅ (VSS)                             | ✅                 | ✅         | Limited          | Limited       |
| **Caching**                   | ✅ (Query and results caching)       | Limited            | ❌         | ❌               | ❌            |
| **Embeddings**                | ✅ (Built-in & pluggable models/DBs) | ✅                 | ✅         | Limited          | ❌            |
| **Edge to Cloud Deployment**  | ✅                                   | Limited            | Limited    | Limited          | ❌            |
| **Programming Language**      | Any language (HTTP interface), CLI   | JavaScript, Python | Python     | Python, CLI      | Python, CLI   |

✅ = Fully supported  
❌ = Not supported  
Limited = Partial or restricted support

## Example Use-Cases

### **Data-grounded Agentic AI Applications**

- **Federated SQL Query**: Query data across databases, warehouses, and lakes with advanced push-down optimizations for reduced latency. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/federated-sql#readme).
- **AI Gateway**: Integrate hosted models (OpenAI, Anthropic) or local ones (Llama, NVIDIA NIM) with ease. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/llama-gateway#readme).
- **Vector Similarity Search**: Retrieve embeddings and unstructured data efficiently, enabling RAG workflows. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/github-search#readme).
- **Monitoring & Observability**: Gain deep visibility into data flows, model performance, and compliance audits. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/monitoring#readme).

### **Database CDN and Query Mesh**

- **Data Acceleration & CDC**: Materialize datasets close to applications with real-time updates and minimal overhead. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/duckdb-accelerator#readme).
- **Faster Applications**: Co-locate hot data with applications for high throughput and low latency. [Try the CQRS Cookbook](https://github.com/spiceai/cookbook/tree/trunk/cqrs#readme).
- **Responsive Dashboards**: Materialize data for BI tools like Superset, enabling fast, real-time analytics. [Watch the Demo](https://github.com/spiceai/cookbook/blob/trunk/sales-bi/README.md).
- **Enhanced Resilience**: Maintain application availability with local replicas of critical datasets. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/resilience#readme).
- **Access Disparate Data**: Federate SQL queries across multiple databases, warehouses, and lakes for seamless integration. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/federation#readme).
- **Simplified Legacy Migration**: Use a single endpoint to unify legacy systems with modern infrastructure. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/legacy-migration#readme).

### **Retrieval-Augmented Generation (RAG)**

- **Unified Search**: Perform vector similarity search across structured and unstructured data sources. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/search#readme).
- **Knowledge Index**: Build an indexed, fast-access knowledge layer spanning legacy and modern systems. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/knowledge-index#readme).
- **Data-Driven AI**: Combine SQL queries with vector search to empower LLMs with precise, context-aware data for reliable generation. [Learn More](https://github.com/spiceai/cookbook/tree/trunk/data-driven-ai#readme).

## FAQ

- **Is Spice a cache?** No, however you can think of Spice data materialization like an _active_ cache or data prefetcher. A cache would fetch data on a cache-miss while Spice prefetches and materializes filtered data on an interval or as new data becomes available. In addition to materialization Spice supports [results caching](https://docs.spiceai.org/features/caching).

- **Is Spice a CDN for databases?** Yes, you can think of Spice like a CDN for different data sources. Using CDN concepts, Spice enables you to ship (load) a working set of your database (or data lake, or data warehouse) where it's most frequently accessed, like from a data application or for AI-inference.

### Watch a 30-sec BI dashboard acceleration demo

<https://github.com/spiceai/spiceai/assets/80174/7735ee94-3f4a-4983-a98e-fe766e79e03a>

See more demos on [YouTube](https://www.youtube.com/playlist?list=PLesJrUXEx3U9anekJvbjyyTm7r9A26ugK).

## Supported Data Connectors

| Name                               | Description                           | Status            | Protocol/Format              |
| ---------------------------------- | ------------------------------------- | ----------------- | ---------------------------- |
| `github`                           | GitHub                                | Stable            |                              |
| `duckdb`                           | DuckDB                                | Release Candidate |                              |
| `graphql`                          | GraphQL                               | Release Candidate | JSON                         |
| `mysql`                            | MySQL                                 | Release Candidate |                              |
| `postgres`                         | PostgreSQL                            | Release Candidate |                              |
| `s3`                               | [S3][s3]                              | Release Candidate | Parquet, CSV                 |
| `databricks (mode: delta_lake)`    | [Databricks][databricks]              | Release Candidate | S3/Delta Lake                |
| `file`                             | File                                  | Release Candidate | Parquet, CSV                 |
| `databricks (mode: spark_connect)` | [Databricks][databricks]              | Beta              | [Spark Connect][spark]       |
| `delta_lake`                       | Delta Lake                            | Beta              | Delta Lake                   |
| `flightsql`                        | FlightSQL                             | Beta              | Arrow Flight SQL             |
| `mssql`                            | Microsoft SQL Server                  | Beta              | Tabular Data Stream (TDS)    |
| `odbc`                             | ODBC                                  | Beta              | ODBC                         |
| `spiceai`                          | [Spice.ai][spiceai]                   | Beta              | Arrow Flight                 |
| `abfs`                             | Azure BlobFS                          | Alpha             | Parquet, CSV                 |
| `clickhouse`                       | Clickhouse                            | Alpha             |                              |
| `debezium`                         | Debezium CDC                          | Alpha             | Kafka + JSON                 |
| `dremio`                           | [Dremio][dremio]                      | Alpha             | Arrow Flight                 |
| `ftp`, `sftp`                      | FTP/SFTP                              | Alpha             | Parquet, CSV                 |
| `http`, `https`                    | HTTP(s)                               | Alpha             | Parquet, CSV                 |
| `iceberg`                          | [Apache Iceberg][iceberg]             | Alpha             | Parquet                      |
| `localpod`                         | [Local dataset replication][localpod] | Alpha             |                              |
| `sharepoint`                       | Microsoft SharePoint                  | Alpha             | Unstructured UTF-8 documents |
| `snowflake`                        | Snowflake                             | Alpha             | Arrow                        |
| `spark`                            | Spark                                 | Alpha             | [Spark Connect][spark]       |
| `documentdb`                       | DocumentDB                            | Coming Soon       |                              |
| `mongodb`                          | MongoDB                               | Coming Soon       |                              |

[databricks]: https://github.com/spiceai/cookbook/tree/trunk/databricks/delta_lake
[spark]: https://spark.apache.org/docs/latest/spark-connect-overview.html
[s3]: https://github.com/spiceai/cookbook/tree/trunk/s3#readme
[spiceai]: https://github.com/spiceai/cookbook/tree/trunk/spiceai#readme
[dremio]: https://github.com/spiceai/cookbook/tree/trunk/dremio#readme
[localpod]: https://github.com/spiceai/cookbook/blob/trunk/localpod/README.md
[iceberg]: https://github.com/spiceai/cookbook/tree/trunk/catalogs/iceberg#readme

## Supported Data Accelerators

| Name       | Description                     | Status            | Engine Modes     |
| ---------- | ------------------------------- | ----------------- | ---------------- |
| `arrow`    | In-Memory Arrow Records         | Release Candidate | `memory`         |
| `duckdb`   | Embedded [DuckDB][duckdb]       | Release Candidate | `memory`, `file` |
| `postgres` | Attached [PostgreSQL][postgres] | Release Candidate | N/A              |
| `sqlite`   | Embedded [SQLite][sqlite]       | Release Candidate | `memory`, `file` |

[duckdb]: https://docs.spiceai.org/data-accelerators/duckdb
[postgres]: https://github.com/spiceai/cookbook/tree/trunk/postgres/accelerator#postgresql-data-accelerator
[sqlite]: https://docs.spiceai.org/data-accelerators/sqlite

## Supported Model Providers

| Name          | Description                                  | ML Format(s) | LLM Format(s)                   |
| ------------- | -------------------------------------------- | ------------ | ------------------------------- |
| `file`        | Local filesystem                             | ONNX         | GGUF, GGML, SafeTensor          |
| `huggingface` | Models hosted on HuggingFace                 | ONNX         | GGUF, GGML, SafeTensor          |
| `spice.ai`    | Models hosted on the Spice.ai Cloud Platform | ONNX         | OpenAI-compatible HTTP endpoint |
| `openai`      | OpenAI (or compatible) LLM endpoint          | -            | OpenAI-compatible HTTP endpoint |
| `azure`       | Azure OpenAI                                 | -            | OpenAI-compatible HTTP endpoint |
| `anthropic`   | Models hosted on Anthropic                   | -            | OpenAI-compatible HTTP endpoint |
| `xai`         | Models hosted on xAI                         | -            | OpenAI-compatible HTTP endpoint |

## Supported Catalogs

Catalog Connectors connect to external catalog providers and make their tables available for federated SQL query in Spice. Configuring accelerations for tables in external catalogs is not supported. The schema hierarchy of the external catalog is preserved in Spice.

| Name            | Description             | Status      | Protocol/Format              |
| --------------- | ----------------------- | ----------- | ---------------------------- |
| `databricks`    | Databricks              | Alpha       | Spark Connect, S3/Delta Lake |
| `unity_catalog` | Unity Catalog           | Alpha       | Delta Lake                   |
| `spice.ai`      | Spice.ai Cloud Platform | Alpha       | Arrow Flight                 |
| `glue`          | AWS Glue                | Coming Soon | JSON, Parquet, Iceberg       |

## ⚡️ Quickstart (Local Machine)

<https://github.com/spiceai/spiceai/assets/88671039/85cf9a69-46e7-412e-8b68-22617dcbd4e0>

**Step 1.** Install the Spice CLI:

On **macOS, Linux, and WSL**:

```bash
curl https://install.spiceai.org | /bin/bash
```

Or using `brew`:

```bash
brew install spiceai/spiceai/spice
```

On **Windows**:

```bash
curl -L "https://install.spiceai.org/Install.ps1" -o Install.ps1 && PowerShell -ExecutionPolicy Bypass -File ./Install.ps1
```

**Step 2.** Initialize a new Spice app with the `spice init` command:

```bash
spice init spice_qs
```

A `spicepod.yaml` file is created in the `spice_qs` directory. Change to that directory:

```bash
cd spice_qs
```

**Step 3.** Start the Spice runtime:

```bash
spice run
```

Example output will be shown as follows:

```bash
Spice.ai runtime starting...
2024-08-05T13:02:40.247484Z  INFO runtime::flight: Spice Runtime Flight listening on 127.0.0.1:50051
2024-08-05T13:02:40.247490Z  INFO runtime::metrics_server: Spice Runtime Metrics listening on 127.0.0.1:9090
2024-08-05T13:02:40.247949Z  INFO runtime: Initialized results cache; max size: 128.00 MiB, item ttl: 1s
2024-08-05T13:02:40.248611Z  INFO runtime::http: Spice Runtime HTTP listening on 127.0.0.1:8090
2024-08-05T13:02:40.252356Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
```

The runtime is now started and ready for queries.

**Step 4.** In a new terminal window, add the `spiceai/quickstart` Spicepod. A Spicepod is a package of configuration defining datasets and ML models.

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
2024-08-05T13:04:56.742779Z  INFO runtime: Dataset taxi_trips registered (s3://spiceai-demo-datasets/taxi_trips/2024/), acceleration (arrow, 10s refresh), results cache enabled.
2024-08-05T13:04:56.744062Z  INFO runtime::accelerated_table::refresh_task: Loading data for dataset taxi_trips
2024-08-05T13:05:03.556169Z  INFO runtime::accelerated_table::refresh_task: Loaded 2,964,624 rows (421.71 MiB) for dataset taxi_trips in 6s 812ms.
```

**Step 5.** Start the Spice SQL REPL:

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

You can use any number of predefined datasets available from the Spice.ai Cloud Platform in the Spice runtime.

A list of publicly available datasets from the Spice.ai Cloud Platform can be found on Spicerack: [https://spicerack.org/](https://spicerack.org/).

In order to access public datasets from Spice.ai, you will first need to create an account with Spice.ai by selecting the free tier membership.

Navigate to [spice.ai](https://spice.ai/) and create a new account by clicking on Try for Free.

<img width="500" alt="spiceai_try_for_free-1" src="https://github.com/spiceai/spiceai/assets/112157037/27fb47ed-4825-4fa8-94bd-48197406cfaa">

After creating an account, you will need to create an app in order to create to an API key.

![create_app-1](https://github.com/spiceai/spiceai/assets/112157037/d2446406-1f06-40fb-8373-1b6d692cb5f7)

You will now be able to access datasets from Spice.ai. For this demonstration, we will be using the `taxi_trips` dataset from the <https://spice.ai/spiceai/quickstart> Spice.ai app.

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

Comprehensive documentation is available at [docs.spiceai.org](https://docs.spiceai.org/).

Over 45 quickstarts and samples available in the [Spice Cookbook](https://github.com/spiceai/cookbook#spiceai-oss-cookbook).

### 🔌 Extensibility

Spice.ai is designed to be extensible with extension points documented at [EXTENSIBILITY.md](./docs/EXTENSIBILITY.md). Build custom [Data Connectors](https://docs.spiceai.org/components/data-connectors), [Data Accelerators](https://docs.spiceai.org/components/data-accelerators), [Catalog Connectors](https://docs.spiceai.org/components/catalogs), [Secret Stores](https://docs.spiceai.org/components/secret-stores), [Models](https://docs.spiceai.org/components/models), or [Embeddings](https://docs.spiceai.org/components/embeddings).

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
