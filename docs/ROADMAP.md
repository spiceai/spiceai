# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## v1.0-stable (Jan 2025)

- Bugfixes and performance improvements
- Data Accelerators: Arrow, DuckDB (Stable)
- Model Providers: OpenAI, Hugging Face, File (Stable)
- Catalog Providers: Unity Catalog, Spice.ai Cloud Platform (Stable)
- Databricks DeltaLake data connector (Stable)
- Spice.ai Cloud Platform data connector (Stable)
- S3 data connector (Stable)
- PostgreSQL Data Connector (Stable)
- MySQL Data Connector (Stable)
- GitHub Data Connector (Stable)
- DuckDB data connector (Stable)
- File data connector (Stable)
- Dremio data connector (Stable)
- Model Providers: Anthropic, Groq, Grok (Beta)
- Catalog Providers: Databricks (Beta), Iceberg Tables (Alpha)
- Snowflake Data Connector (Beta)
- Spark Data Connector (Beta)
- DynamoDB data connector (Alpha)

### v1.0-stable Data Connector Support

| Name                            | Status |
| ------------------------------- | ------ |
| `databricks (mode: delta_lake)` | Stable |
| `delta_lake`                    | Stable |
| `dremio`                        | Stable |
| `duckdb`                        | Stable |
| `file`                          | Stable |
| `github`                        | Stable |
| `mysql`                         | Stable |
| `postgres`                      | Stable |
| `s3`                            | Stable |
| `databricks (mode: spark)`      | Beta   |
| `mssql`                         | Beta   |
| `odbc`                          | Beta   |
| `snowflake`                     | Beta   |
| `spiceai`                       | Beta   |
| `spark`                         | Beta   |
| `clickhouse`                    | Alpha  |
| `debezium`                      | Alpha  |
| `dynamodb`                      | Alpha  |
| `flightsql`                     | Alpha  |
| `ftp`, `sftp`                   | Alpha  |
| `http`, `https`                 | Alpha  |
| `sharepoint`                    | Alpha  |

### v1.0-stable Data Accelerator Support

| Name         | Status            |
| ------------ | ----------------- |
| `arrow`      | Stable            |
| `duckdb`     | Stable            |
| `postgresql` | Release Candidate |
| `sqlite`     | Release Candidate |

### v1.0-stable Catalog Provider Support

| Name            | Status |
| --------------- | ------ |
| `unity_catalog` | Stable |
| `spice.ai`      | Stable |
| `databricks`    | Beta   |
| `iceberg`       | Alpha  |

### v1.0-stable Model Provider Support

| Name          | Status            |
| ------------- | ----------------- |
| `openai`      | Release Candidate |
| `huggingface` | Release Candidate |
| `file`        | Release Candidate |
| `anthropic`   | Alpha             |
| `azure`       | Alpha             |
| `groq`        | Alpha             |
| `grok`        | Alpha             |

## v1.1 (Feb 2025)

- Enterprise Search and Retrieval improvements
  - [#3006](https://github.com/spiceai/spiceai/issues/3006) Search with keyword filtering
  - [#3016](https://github.com/spiceai/spiceai/issues/3016) Hybrid-Search (DB + Vector)
  - [#3015](https://github.com/spiceai/spiceai/issues/3015) DataFusion Search
- [#3318](https://github.com/spiceai/spiceai/issues/3318) FinanceBench in CI
- Model Providers: OpenAI, HuggingFace, File (Stable)
- Model Providers: Anthropic, Azure OpenAI, Grok (Beta)
- Data Accelerators: PostgreSQL (Stable)

## v1.2 (Mar 2025)

- AWS Glue Catalog Connector (Alpha)
- [#3018](https://github.com/spiceai/spiceai/issues/3018) Results caching for embeddings and search

## v1.3 (Q1 2025)

- Ingestion
  - PostgreSQL write
  - Delta Lake write
  - Iceberg write

## v2 (H2 2025)

- Policy
  - Security
  - Data Access
- Extensible Middleware
- AuthN Providers

## Features being considered

- Data Connectors

  - S3 Tables
  - ElasticSearch
  - MotherDuck
  - BigQuery
  - Kafka
  - Google Docs

- APIs

  - GraphQL API
  - Key/Value API
    - SlateDB data accelerator
    - RocksDB data accelerator
  - PostgreSQL API

- AI
  - Vision
