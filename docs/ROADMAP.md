# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## [v1.0-rc.2 (Dec 2024)](https://github.com/spiceai/spiceai/milestone/48)

- Bugfixes and performance improvements
- GraphQL data connector (Release Candidate)
- Databricks DeltaLake data connector (Release Candidate)
- Spice.ai Cloud Platform data connector (Release Candidate)
- ODBC Data Connector - Athena, Databricks, MySQL (Release Candidate)
- Dremio data connector (Release Candidate)
- MS SQL data connector (Beta)
- File data connector (Beta)
- S3 data connector Iceberg support (Beta)
- AI Evals (AI-as-a-Judge)
  - [#3320](https://github.com/spiceai/spiceai/issues/3320) Eval datasets

## v1.0-rc.3 (Dec 2024)

- Bugfixes and performance improvements
- MS SQL data connector (Release Candidate)
- File data connector (Release Candidate)
- Data Accelerators: SQLite & PostgreSQL (Release Candidate)
- DynamoDB data connector (Alpha)
- [#3318](https://github.com/spiceai/spiceai/issues/3318) FinanceBench in CI
- [#3018](https://github.com/spiceai/spiceai/issues/3018) Results caching for embeddings and search

## v1.0-stable (Jan 2025)

- Bugfixes and performance improvements
- CLI
  - Spicepod publish to [spicerack.org](https://spicerack.org)
- Data Accelerators: Arrow, DuckDB, SQLite, PostgreSQL (Stable)
- Model Providers: OpenAI, Hugging Face (Stable)
- Catalog Providers: Unity Catalog, Spice.AI Cloud Platform (Stable)
- Databricks DeltaLake data connector (Stable)
- Spice.ai Cloud Platform data connector (Stable)
- S3 data connector (Stable)
- ODBC Data Connector (Stable)
- PostgreSQL Data Connector (Stable)
- MySQL Data Connector (Stable)
- DuckDB Data Connector (Stable)
- GitHub Data Connector (Stable)
- GraphQL Data Connector (Stable)
- MS SQL data connector (Stable)
- File data connector (Stable)
- Dremio data connector (Stable)
- Model Providers: Anthropic, Groq, Grok, File (Beta)
- Catalog Providers: Databricks (Beta)
- FTP/SFTP Data Connector (Beta)
- Snowflake Data Connector (Beta)
- Spark Data Connector (Beta)

### v1.0-stable Data Connector Support

| Name            | Status |
| --------------- | ------ |
| `databricks`    | Stable |
| `delta_lake`    | Stable |
| `postgres`      | Stable |
| `spiceai`       | Stable |
| `s3`            | Stable |
| `odbc`          | Stable |
| `mysql`         | Stable |
| `file`          | Stable |
| `duckdb`        | Stable |
| `github`        | Stable |
| `graphql`       | Stable |
| `dremio`        | Stable |
| `mssql`         | Stable |
| `spark`         | Beta   |
| `snowflake`     | Beta   |
| `ftp`, `sftp`   | Beta   |
| `http`, `https` | Beta   |
| `clickhouse`    | Alpha  |
| `flightsql`     | Alpha  |
| `debezium`      | Alpha  |
| `sharepoint`    | Alpha  |

### v1.0-stable Data Accelerator Support

| Name         | Status |
| ------------ | ------ |
| `arrow`      | Stable |
| `duckdb`     | Stable |
| `sqlite`     | Stable |
| `postgresql` | Stable |

### v1.0-stable Catalog Provider Support

| Name            | Status |
| --------------- | ------ |
| `unity_catalog` | Stable |
| `spice.ai`      | Stable |
| `databricks`    | Beta   |

### v1.0-stable Model Provider Support

| Name          | Status |
| ------------- | ------ |
| `openai`      | Stable |
| `huggingface` | Stable |
| `file`        | Beta   |
| `anthropic`   | Beta   |
| `azure`       | Beta   |
| `groq`        | Beta   |
| `grok`        | Alpha  |

## v1.1 (Feb 2025)

- Bugfixes
- Enterprise Search improvements
  - [#3006](https://github.com/spiceai/spiceai/issues/3006) Search with keyword filtering
  - [#3016](https://github.com/spiceai/spiceai/issues/3016) Hybrid-Search (DB + Vector)
  - [#3015](https://github.com/spiceai/spiceai/issues/3015) DataFusion Search

## v1.2 (Mar 2025)

## v1.3 (April 2025)

## v2 (2025)

- Ingestion
  - Delta Lake write
  - Iceberg write
- Policy
  - Security
  - Data Access
- Extensible Middleware
  - AuthN Providers

## Features being considered

- Data Connectors
  - MotherDuck
  - BigQuery
  - Kafka
  - Google Docs
- Catalog Providers

  - AWS Glue

- APIs
  - GraphQL API
  - Key/Value API
    - SlateDB data accelerator
    - RocksDB data accelerator
  - PostgreSQL API
