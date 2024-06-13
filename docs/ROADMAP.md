# Spice.ai OSS Roadmap

A living doc that describes the Spice.ai roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## [v0.14-alpha (June 2024)](https://github.com/spiceai/spiceai/milestone/34)

- Accelerated table primary key and indexes
- GraphQL data connector (alpha)

## [v0.15-alpha (June 2024)](https://github.com/spiceai/spiceai/milestone/28)

- CDC replication refresh mode
- MotherDuck data connector (alpha)
- Generic Object-Store connector (alpha)

## v0.16-beta (July 2024)

- Catalog Providers
  - Unity Catalog catalog provider (alpha)
- Dataset management improvements
- Bugfixes and performance improvements
- Databricks data connector (Beta)
- Spice AI data connector (Beta)
- PostgreSQL data connector (Beta)
- S3 data connector (Beta)
- DuckDB data connector (Beta)
- ODBC Data Connector (Beta)
- FTP/SFTP Data Connector (Beta)

## v0.17-rc (August 2024)
- Bugfixes and performance improvements
- Databricks data connector (Release Candidate)
- Spice AI data connector (Release Candidate)
- S3 data connector (Release Candidate)
- DuckDB data connector (Release Candidate)
- ODBC Data Connector (Release Candidate)
- FTP/SFTP Data Connector (Release Candidate)
- PostgreSQL data connector (Release Candidate)

## v1.0-stable (Sep 2024)

- Bugfixes and performance improvements
- Databricks data connector (Stable)
- Spice AI data connector (Stable)
- S3 data connector (Stable)
- DuckDB data connector (Stable)
- ODBC Data Connector (Stable)
- FTP/SFTP Data Connector (Stable)
- PostgreSQL Data Connector (Stable)
- Snowflake Data Connector (Beta)

## v1.0-stable Data Connector Support
  | Name          | Status              |
  | ------------- | ------------------- |
  | `databricks`  | Stable              |
  | `postgres`    | Stable              |
  | `spiceai`     | Stable              |
  | `s3`          | Stable              |
  | `dremio`      | Alpha               |
  | `mysql`       | Alpha               |
  | `duckdb`      | Stable              |
  | `clickhouse`  | Alpha               |
  | `odbc`        | Stable              |
  | `spark`       | Alpha               |
  | `flightsql`   | Alpha               |
  | `snowflake`   | Beta                |
  | `ftp`, `sftp` | Stable              |
  | `graphql`     | Alpha               |
  | `motherduck`  | Alpha               |

## v1.1 (Oct 2024)

- AWS Glue catalog provider (alpha)

## Features being considered

- S3 data connector Iceberg support
- Intelligent (AI-powered) accelerators
- Kafka data connector
- GraphQL API
- BigQuery data connector
- Key/Value API
  - RocksDB data accelerator
  - DynamoDB data connector
- CLI publish Spicepods to [spicerack.org](https://spicerack.org)
