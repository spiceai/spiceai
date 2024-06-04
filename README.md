# Spice.ai OSS

[![CodeQL](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.gg/kZnTfneP5u)
[![Follow on X](https://img.shields.io/twitter/follow/spice_ai.svg?style=social&logo=x)](https://twitter.com/intent/follow?screen_name=spice_ai)

## What is Spice?

**Spice** is a portable runtime providing developers with a unified SQL interface to materialize, accelerate, and query data sourced from any database, data warehouse, or data lake.

📣 Read the [Spice.ai OSS announcement blog post](https://blog.spiceai.org/posts/2024/03/28/adding-spice-the-next-generation-of-spice.ai-oss/).

Spice connects, fuses, and delivers data to applications and AI, acting as an application-specific, tier-optimized Database CDN.

The Spice runtime is written in Rust and is built-with industry leading technologies like [Apache DataFusion](https://datafusion.apache.org), Apache Arrow, Apache Arrow Flight, SQlite, and DuckDB.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/spiceai/spiceai/assets/80174/96b5fcef-a550-4ce8-a74a-83931275e83e">
  <img width="900" alt="Spice.ai" src="https://github.com/spiceai/spiceai/assets/80174/f71f227d-d7cd-418c-85b9-5c663a728491" />
</picture>

## Why Spice?

Spice makes querying data by SQL across one or more data sources simple and fast. Easily co-locate a managed working set of data with your application or ML, accelerated with in-memory Arrow, with SQLite/DuckDB, or with attached PostgreSQL for high-performance, low-latency queries. Accelerated engines run tier-native in your infrastructure giving you flexibility and control over cost and performance.

### How is Spice different?

1. Tier-optimized Acceleration with **both OLAP (Arrow/DuckDB) and OLTP (SQLite/PostgreSQL)** databases at dataset granularity compared to other OLAP only or OLTP only systems.

2. **Separation of materialization and storage/compute** compared with monolith data systems and data lakes. Keep compute colocated with source data while bringing a materialized working set next to your application, dashboard, or data/ML pipeline.

3. **Edge to cloud native**. Designed to be deployed standalone, as a container sidecar, as a microservice, in a cluster across laptops, the Edge, On-Prem, to a POP, and to all public clouds. Spice instances can also be chained, and deployed distributed across tiers of infrastructure.

### Before Spice

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/spiceai/spiceai/assets/80174/64a3216e-0bbb-48b0-bf98-72e656d690af">
  <img width="750" alt="Before Spice" src="https://github.com/spiceai/spiceai/assets/80174/0550d682-cf3b-4b1b-a3bd-d8b3ad7d8caf" />
</picture>

### With Spice

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/spiceai/spiceai/assets/80174/02dbedb4-b209-4d08-bf83-4785a1bf886f">
  <img width="900" alt="With Spice" src="https://github.com/spiceai/spiceai/assets/80174/b57514fe-d53d-42de-b8f0-97ae313c5708" />
</picture>

### Example Use-Cases

**1. Faster applications and frontends.** Accelerate and co-locate datasets with applications and frontends, to serve more concurrent queries and users with faster page loads and data updates. [Try the CQRS sample app](https://github.com/spiceai/samples/tree/trunk/acceleration#local-materialization-and-acceleration-cqrs-sample)

**2. Faster dashboards, analytics, and BI.** Faster, more responsive dashboards without massive compute costs. [Watch the Apache Superset demo](https://github.com/spiceai/samples/blob/trunk/sales-bi/README.md)

**3. Faster data pipelines, machine learning training and inferencing.** Co-locate datasets in pipelines where the data is needed to minimize data-movement and improve query performance. [Predict hard drive failure with the SMART data demo](https://github.com/spiceai/demos/tree/trunk/smart-demo#spiceai-smart-demo)

**4. Easily query many data sources.** Federated SQL query across databases, data warehouses, and data lakes using [Data Connectors](https://docs.spiceai.org/data-connectors).

### FAQ

- **Is Spice a cache?** No, however you can think of Spice data materialization like an _active_ cache or data prefetcher. A cache would fetch data on a cache-miss while Spice prefetches and materializes filtered data on an interval or as new data becomes available. In addition to materialization Spice supports [results caching](https://docs.spiceai.org/features/caching).

- **Is Spice a CDN for databases?** Yes, you can think of Spice like a CDN for different data sources. Using CDN concepts, Spice enables you to ship (load) a working set of your database (or data lake, or data warehouse) where it's most frequently accessed, like from a data application or for AI-inference.

### Watch a 30-sec BI dashboard acceleration demo

https://github.com/spiceai/spiceai/assets/80174/7735ee94-3f4a-4983-a98e-fe766e79e03a

### Supported Data Connectors

Currently supported data connectors for upstream datasets. More coming soon.

| Name          | Description                                                                                    | Status | Protocol/Format                                                                                    | Refresh Modes    |
| ------------- | ---------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------- | ---------------- |
| `databricks`  | [Databricks](https://github.com/spiceai/quickstarts/tree/trunk/databricks#spice-on-databricks) | Alpha  | [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)<br>S3/Delta Lake | `full`           |
| `postgres`    | PostgreSQL                                                                                     | Alpha  |                                                                                                    | `full`           |
| `spiceai`     | [Spice.ai](https://github.com/spiceai/quickstarts/tree/trunk/spiceai#readme)                   | Alpha  | Arrow Flight                                                                                       | `append`, `full` |
| `s3`          | [S3](https://github.com/spiceai/quickstarts/tree/trunk/s3#readme)                              | Alpha  | Parquet, CSV                                                                                       | `full`           |
| `dremio`      | [Dremio](https://github.com/spiceai/quickstarts/tree/trunk/dremio#readme)                      | Alpha  | Arrow Flight                                                                                       | `full`           |
| `mysql`       | MySQL                                                                                          | Alpha  |                                                                                                    | `full`           |
| `duckdb`      | DuckDB                                                                                         | Alpha  |                                                                                                    | `full`           |
| `clickhouse`  | Clickhouse                                                                                     | Alpha  |                                                                                                    | `full`           |
| `odbc`        | ODBC                                                                                           | Alpha  | ODBC                                                                                               | `full`           |
| `spark`       | Spark                                                                                          | Alpha  | [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)                  | `full`           |
| `flightsql`   | Apache Arrow Flight SQL                                                                        | Alpha  | Arrow Flight SQL                                                                                   | `full`           |
| `snowflake`   | Snowflake                                                                                      | Alpha  | Arrow                                                                                              | `full`           |
| `ftp`, `sftp` | FTP/SFTP                                                                                       | Alpha  | Parquet, CSV                                                                                       | `full`           |

### Supported Data Stores/Accelerators

Currently supported data stores for local materialization/acceleration. More coming soon.

| Name       | Description                                                                                                   | Status | Engine Modes     |
| ---------- | ------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| `arrow`    | In-Memory Arrow Records                                                                                       | Alpha  | `memory`         |
| `duckdb`   | Embedded [DuckDB](https://docs.spiceai.org/data-accelerators/duckdb)                                          | Alpha  | `memory`, `file` |
| `sqlite`   | Embedded [SQLite](https://docs.spiceai.org/data-accelerators/sqlite)                                          | Alpha  | `memory`, `file` |
| `postgres` | Attached [PostgreSQL](https://github.com/spiceai/quickstarts/tree/trunk/postgres#postgresql-data-accelerator) | Alpha  | `file`           |

### Intelligent Applications

Spice enables developers to build both data _and_ AI-driven applications by co-locating data _and_ ML models with applications. Read more about the vision to enable the development of [intelligent AI-driven applications](https://docs.spiceai.org/intelligent-applications).

⚠️ **DEVELOPER PREVIEW** Spice is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release. If you are interested in running Spice in production, please get in touch so we can support you (See Connect with us below).

## ⚡️ Quickstart (Local Machine)

https://github.com/spiceai/spiceai/assets/88671039/85cf9a69-46e7-412e-8b68-22617dcbd4e0

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
Using latest 'local' runtime version.
2024-06-03T23:21:26.819978Z  INFO spiced: Metrics listening on 127.0.0.1:9000
2024-06-03T23:21:26.821863Z  INFO runtime::http: Spice Runtime HTTP listening on 127.0.0.1:3000
2024-06-03T23:21:26.821898Z  INFO runtime::flight: Spice Runtime Flight listening on 127.0.0.1:50051
2024-06-03T23:21:26.821958Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
2024-06-03T23:21:26.822128Z  INFO runtime: Initialized results cache; max size: 128.00 MiB, item ttl: 1s
```

The runtime is now started and ready for queries.

**Step 4.** In a new terminal window, add the `spiceai/quickstart` Spicepod. A Spicepod is a package of configuration defining datasets and ML models.

```bash
spice add spiceai/quickstart
```

The `spicepod.yaml` file will be updated with the `spiceai/quickstart` dependency.

```yaml
version: v1beta1
kind: Spicepod
name: PROJECT_NAME
dependencies:
  - spiceai/quickstart
```

The `spiceai/quickstart` Spicepod will add a `taxi_trips` data table to the runtime which is now available to query by SQL.

```bash
2024-06-03T23:21:29.721705Z  INFO runtime: Registered dataset taxi_trips
2024-06-03T23:21:29.722839Z  INFO runtime::accelerated_table::refresh: Loading data for dataset taxi_trips
2024-06-03T23:21:50.813510Z  INFO runtime::accelerated_table::refresh: Loaded 2,964,624 rows (421.71 MiB) for dataset taxi_trips in 21s 90ms.
```

**Step 5.** Start the Spice SQL REPL:

```bash
spice sql
```

The SQL REPL inferface will be shown:

```
Welcome to the Spice.ai SQL REPL! Type 'help' for help.

show tables; -- list available tables
sql>
```

Enter `show tables;` to display the available tables for query:

```
sql> show tables
+---------------+--------------+---------------+------------+
| table_catalog | table_schema | table_name    | table_type |
+---------------+--------------+---------------+------------+
| spice         | public       | taxi_trips    | BASE TABLE |
| spice         | runtime      | metrics       | BASE TABLE |
| spice         | runtime      | query_history | BASE TABLE |
+---------------+--------------+---------------+------------+

Time: 0.007505084 seconds. 1 rows.
```

Enter a query to display the longest taxi trips:

```
sql> SELECT trip_distance, total_amount FROM taxi_trips ORDER BY trip_distance DESC LIMIT 10;
```

Output:

```
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

Time: 0.015596458 seconds. 10 rows.
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

You can use any number of predefined datasets available from the Spice.ai Cloud Platform in the Spice runtime.

A list of publicly available datasets from Spice.ai can be found here: [https://docs.spice.ai/building-blocks/datasets](https://docs.spice.ai/building-blocks/datasets).

In order to access public datasets from Spice.ai, you will first need to create an account with Spice.ai by selecting the free tier membership.

Navigate to [spice.ai](https://spice.ai/) and create a new account by clicking on Try for Free.

<img width="500" alt="spiceai_try_for_free-1" src="https://github.com/spiceai/spiceai/assets/112157037/27fb47ed-4825-4fa8-94bd-48197406cfaa">

After creating an account, you will need to create an app in order to create to an API key.

![create_app-1](https://github.com/spiceai/spiceai/assets/112157037/d2446406-1f06-40fb-8373-1b6d692cb5f7)

You will now be able to access datasets from Spice.ai. For this demonstration, we will be using the `spice.ai/eth.recent_blocks` dataset.

**Step 1.** Log in and authenticate from the command line using the `spice login` command. A pop up browser window will prompt you to authenticate:

```bash
spice login
```

**Step 2.** Initialize a new project and start the runtime:

```bash
# Initialize a new Spice app
spice init spice_app

# Change to app directory
cd spice_app

# Start the runtime
spice run
```

**Step 3.** Configure the dataset:

In a new terminal window, configure a new dataset using the `spice dataset configure` command:

```bash
spice dataset configure
```

You will be prompted to enter a name. Enter a name that represents the contents of the dataset

```bash
dataset name: (spice_app) eth_recent_blocks
```

Enter the description of the dataset:

```
description: eth recent blocks
```

Enter the location of the dataset:

```bash
from: spice.ai/eth.recent_blocks
```

Select `y` when prompted whether to accelerate the data:

```bash
Locally accelerate (y/n)? y
```

You should see the following output from your runtime terminal:

```bash
2024-06-03T23:25:59.514395Z  INFO runtime: Registered dataset eth_recent_blocks
2024-06-03T23:25:59.514624Z  INFO runtime::accelerated_table::refresh: Loading data for dataset eth_recent_blocks
2024-06-03T23:26:00.758813Z  INFO runtime::accelerated_table::refresh: Loaded 143 rows (6.22 MiB) for dataset eth_recent_blocks in 1s 244ms.
```

**Step 4.** In a new terminal window, use the Spice SQL REPL to query the dataset

```bash
spice sql
```

```sql
SELECT number, size, gas_used from eth_recent_blocks LIMIT 10;
```

The output displays the results of the query along with the query execution time:

```bash
+----------+--------+----------+
| number   | size   | gas_used |
+----------+--------+----------+
| 19281345 | 400378 | 16150051 |
| 19281344 | 200501 | 16480224 |
| 19281343 | 97758  | 12605531 |
| 19281342 | 89629  | 12035385 |
| 19281341 | 133649 | 13335719 |
| 19281340 | 307584 | 18389159 |
| 19281339 | 89233  | 13391332 |
| 19281338 | 75250  | 12806684 |
| 19281337 | 100721 | 11823522 |
| 19281336 | 150137 | 13418403 |
+----------+--------+----------+

Time: 0.004057791 seconds. 10 rows.
```

You can experiment with the time it takes to generate queries when using non-accelerated datasets. You can change the acceleration setting from `true` to `false` in the datasets.yaml file.

### 📄 Documentation

Comprehensive documentation is available at [docs.spiceai.org](https://docs.spiceai.org/).

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
