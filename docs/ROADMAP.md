# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## [v1.0-rc (Oct 2024)](https://github.com/spiceai/spiceai/milestone/33)

- Bugfixes and performance improvements
- Databricks DeltaLake data connector (Release Candidate)
- Spice Cloud Platform data connector (Release Candidate)
- S3 data connector (Release Candidate)
- ODBC Data Connector (Release Candidate)
- PostgreSQL data connector (Release Candidate)
- MySQL data connector (Release Candidate)

## v1.0-stable

- Bugfixes and performance improvements
- CLI
  - Spicepod publish to [spicerack.org](https://spicerack.org)
- Databricks DeltaLake data connector (Stable)
- Spice AI data connector (Stable)
- S3 data connector (Stable)
- ODBC Data Connector (Stable)
- PostgreSQL Data Connector (Stable)
- MySQL Data Connector (Stable)
- DuckDB data connector (Beta)
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
| `duckdb`        | Beta   |
| `spark`         | Beta   |
| `snowflake`     | Beta   |
| `ftp`, `sftp`   | Beta   |
| `http`, `https` | Beta   |
| `graphql`       | Beta   |
| `github`        | Beta   |
| `clickhouse`    | Alpha  |
| `dremio`        | Alpha  |
| `flightsql`     | Alpha  |
| `debezium`      | Alpha  |
| `sharepoint`    | Alpha  |

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
