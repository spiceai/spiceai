# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## [v0.17-beta (July 2024)](https://github.com/spiceai/spiceai/milestone/37)

- Bugfixes and performance improvements
- Databricks data connector (Beta)
- Spice AI data connector (Beta)
- PostgreSQL data connector (Beta)
- S3 data connector (Beta)
- ODBC Data Connector (Beta)
- MySQL Data Connector (Beta)

## [v0.18-rc (August 2024)](https://github.com/spiceai/spiceai/milestone/32)

- Bugfixes and performance improvements
- Databricks data connector (Release Candidate)
- Spice AI data connector (Release Candidate)
- S3 data connector (Release Candidate)
- ODBC Data Connector (Release Candidate)
- PostgreSQL data connector (Release Candidate)

## [v1.0-stable (Sep 2024)](https://github.com/spiceai/spiceai/milestone/33)

- Bugfixes and performance improvements
- Databricks data connector (Stable)
- Spice AI data connector (Stable)
- S3 data connector (Stable)
- ODBC Data Connector (Stable)
- PostgreSQL Data Connector (Stable)
- DuckDB data connector (Beta)
- FTP/SFTP Data Connector (Beta)
- Snowflake Data Connector (Beta)
- Spark Data Connector (Beta)

### v1.0-stable Data Connector Support

| Name          | Status |
| ------------- | ------ |
| `databricks`  | Stable |
| `postgres`    | Stable |
| `spiceai`     | Stable |
| `s3`          | Stable |
| `odbc`        | Stable |
| `mysql`       | Stable |
| `duckdb`      | Beta   |
| `spark`       | Beta   |
| `snowflake`   | Beta   |
| `ftp`, `sftp` | Beta   |
| `clickhouse`  | Alpha  |
| `dremio`      | Alpha  |
| `flightsql`   | Alpha  |
| `graphql`     | Alpha  |
| `debezium`    | Alpha  |

## v1.1 (Oct 2024)

- Bugfixes

## v2 (2025)

- AI Gateway
  - Embeddings
  - Model Providers
    - Anthropic
- Policies
  - Security
- Extensible Middleware
  - AuthN Providers

## Features being considered

- S3 data connector Iceberg support
- Data Connectors
  - MotherDuck
  - BigQuery
  - Kafka
  - Sharepoint
  - Google Docs
- Catalog Providers
  - Iceberg Catalogs
  - AWS Glue
- APIs
  - GraphQL API
  - Key/Value API
    - RocksDB data accelerator
    - DynamoDB data connector
- CLI
  - Spicepod publish to [spicerack.org](https://spicerack.org)
