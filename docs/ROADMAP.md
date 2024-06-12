# Spice.ai OSS Roadmap

A living doc that describes the Spice.ai roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#community)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to file a new Issue if you see a bug and let us know on Discord.

## [v0.14-alpha (June 2024)](https://github.com/spiceai/spiceai/milestone/27)

- Accelerated table primary key and indexes


- Data Connector Status
  | Name          | Status |
  | ------------- | ------ |
  | `databricks`  | Alpha  |
  | `postgres`    | Alpha  |
  | `spiceai`     | Alpha  |
  | `s3`          | Alpha  |
  | `dremio`      | Alpha  |
  | `mysql`       | Alpha  |
  | `duckdb`      | Alpha  |
  | `clickhouse`  | Alpha  |
  | `odbc`        | Alpha  |
  | `spark`       | Alpha  |
  | `flightsql`   | Alpha  |
  | `snowflake`   | Alpha  |
  | `ftp`, `sftp` | Alpha  |

## [v0.15-alpha (June 2024)](https://github.com/spiceai/spiceai/milestone/28)

- CDC replication refresh mode
- MotherDuck data connector (alpha)
- GraphQL data connector (alpha)
- Generic Object-Store connector (alpha)

- Data Connector Status
  | Name          | Status |
  | ------------- | ------ |
  | `databricks`  | Alpha  |
  | `postgres`    | Alpha  |
  | `spiceai`     | Alpha  |
  | `s3`          | Alpha  |
  | `dremio`      | Alpha  |
  | `mysql`       | Alpha  |
  | `duckdb`      | Alpha  |
  | `clickhouse`  | Alpha  |
  | `odbc`        | Alpha  |
  | `spark`       | Alpha  |
  | `flightsql`   | Alpha  |
  | `snowflake`   | Alpha  |
  | `ftp`, `sftp` | Alpha  |
  | `graphql`     | Alpha  |
  | `motherduck`  | Alpha  |

## v0.16-beta (July 2024)

- Catalog Providers
  - Unity Catalog catalog provider (alpha)
- Dataset management improvements
- Bugfixes and performance improvements

- Data Connector Status
  | Name          | Status |
  | ------------- | ------ |
  | `databricks`  | Beta   |
  | `postgres`    | Alpha  |
  | `spiceai`     | Beta   |
  | `s3`          | Beta   |
  | `dremio`      | Alpha  |
  | `mysql`       | Alpha  |
  | `duckdb`      | Beta   |
  | `clickhouse`  | Alpha  |
  | `odbc`        | Beta   |
  | `spark`       | Alpha  |
  | `flightsql`   | Alpha  |
  | `snowflake`   | Alpha  |
  | `ftp`, `sftp` | Beta   |
  | `graphql`     | Alpha  |
  | `motherduck`  | Alpha  |

## v0.17-rc (August 2024)
- Bugfixes and performance improvements

- Data Connector Status
  | Name          | Status              |
  | ------------- | ------------------- |
  | `databricks`  | Release Candidate   |
  | `postgres`    | Alpha               |
  | `spiceai`     | Release Candidate   |
  | `s3`          | Release Candidate   |
  | `dremio`      | Alpha               |
  | `mysql`       | Alpha               |
  | `duckdb`      | Release Candidate   |
  | `clickhouse`  | Alpha               |
  | `odbc`        | Release Candidate   |
  | `spark`       | Alpha               |
  | `flightsql`   | Alpha               |
  | `snowflake`   | Alpha               |
  | `ftp`, `sftp` | Release Candidate   |
  | `graphql`     | Alpha               |
  | `motherduck`  | Alpha               |

## v1.0-stable (Sep 2024)

- Bugfixes and performance improvements

- Data Connector Status
  | Name          | Status              |
  | ------------- | ------------------- |
  | `databricks`  | Stable              |
  | `postgres`    | Alpha               |
  | `spiceai`     | Stable              |
  | `s3`          | Stable              |
  | `dremio`      | Alpha               |
  | `mysql`       | Alpha               |
  | `duckdb`      | Stable              |
  | `clickhouse`  | Alpha               |
  | `odbc`        | Stable              |
  | `spark`       | Alpha               |
  | `flightsql`   | Alpha               |
  | `snowflake`   | Alpha               |
  | `ftp`, `sftp` | Stable              |
  | `graphql`     | Alpha               |
  | `motherduck`  | Alpha               |

## v1.1 (Oct 2024)

- AWS Glue catalog provider (alpha)
  
- Data Connector Status
  | Name          | Status              |
  | ------------- | ------------------- |
  | `databricks`  | Stable              |
  | `postgres`    | Beta                |
  | `spiceai`     | Stable              |
  | `s3`          | Stable              |
  | `dremio`      | Beta                |
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

## Features being considered

- S3 data connector Iceberg support
- Intelligent (AI-powered) accelerators
- Kafka data connector
- GraphQL API
- GraphQL data connector
- BigQuery data connector
- Key/Value API
  - RocksDB data accelerator
  - DynamoDB data connector
- CLI publish Spicepods to [spicerack.org](https://spicerack.org)
