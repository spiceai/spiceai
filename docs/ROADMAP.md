# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## [v1.0-rc.1 (Nov 2024)](https://github.com/spiceai/spiceai/milestone/45)

- Bugfixes and performance improvements
- Data accelerators: Arrow, DuckDB, SQLite, PostgreSQL (Beta)
- S3 data connector (Release Candidate)
- PostgreSQL data connector (Release Candidate)
- MySQL data connector (Release Candidate)
- GitHub data connector (Release Candidate)

## [v1.0-rc.2 (Nov 2024)](https://github.com/spiceai/spiceai/milestone/48)

- Bugfixes and performance improvements
- GraphQL data connector (Release Candidate)
- Databricks DeltaLake data connector (Release Candidate)
- Spice.ai Cloud Platform data connector (Release Candidate)
- ODBC Data Connector - Athena, Databricks, MySQL (Release Candidate)

## v1.0-rc.3 (Nov 2024)

- Bugfixes and performance improvements
- Data accelerators: Arrow, DuckDB, SQLite, PostgreSQL (Release Candidate)
- DuckDB data connector (Release Candidate)
- MS SQL data connector (Beta)
- File data connector (Beta)
- Dremio data connector (Beta)

## v1.0-rc.4 (Nov 2024)

- Bugfixes and performance improvements
- MS SQL data connector (Release Candidate)
- File data connector (Release Candidate)
- Dremio data connector (Release Candidate)

## v1.0-rc.5 (Dec 2024)

- Bugfixes and performance improvements

## v1.0-rc.6 (Dec 2024)

- Bugfixes and performance improvements

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
- Model Providers: Anthropic, File (Beta)
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

| Name         | Status |
| ------------ | ------ |
| `unity_catalog`| Stable |
| `spice.ai`     | Stable |
| `databricks`   | Beta   |

### v1.0-stable Model Provider Support

| Name         | Status |
| ------------ | ------ |
| `openai`      | Stable |
| `huggingface` | Stable |
| `anthropic`   | Beta   |
| `file`        | Beta   |

## v1.1

- Bugfixes

## v2 (2025)

- Ingestion
  - Delta Lake write
  - Iceberg write
- AI Gateway
  - Embeddings
  - Model Providers
    - OpenAI
    - Azure OpenAI
    - Anthropic
    - Groq
- Policy
  - Security
  - Data Access
- Extensible Middleware
  - AuthN Providers

## Features being considered

- S3 data connector Iceberg support
- Data Connectors
  - MotherDuck
  - BigQuery
  - Kafka
  - Google Docs
- Catalog Providers
  - Iceberg Catalogs
  - AWS Glue
- APIs
  - GraphQL API
  - Key/Value API
    - SlateDB data accelerator
    - RocksDB data accelerator
    - DynamoDB data connector
