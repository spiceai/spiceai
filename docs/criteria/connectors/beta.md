# Spice.ai OSS Data Connectors - Beta Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of Beta quality.

All criteria must be met for the connector to be considered Beta, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## Beta Quality Connectors

| Connector               | Beta Quality | DRI Sign-off |
| ----------------------- | ------------ | ------------ |
| Clickhouse              | ❌           |              |
| Databricks              | ❌           |              |
| Delta Lake              | ❌           |              |
| Dremio                  | ❌           |              |
| File                    | ❌           |              |
| FTP/SFTP                | ❌           |              |
| GraphQL                 | ❌           |              |
| GitHub                  | ✅           | @peasee      |
| HTTP/HTTPS              | ❌           |              |
| Localpod                | ❌           |              |
| MS SQL                  | ❌           |              |
| MySQL                   | ✅           | @peasee      |
| ODBC                    | ❌           |              |
| PostgreSQL              | ✅           | @Sevenannn   |
| Sharepoint              | ❌           |              |
| Snowflake               | ❌           |              |
| Spice.AI Cloud Platform | ❌           |              |
| S3                      | ❌           |              |
| Azure BlobFS            | ❌           |              |
| Spark                   | ❌           |              |

## Beta Release Criteria

Some connectors impose different testing requirements, as by their nature they support different feature requirements.

Some connectors are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test.

This table defines the required features and/or tests for each connector:

| Connector               | [TPC-H Derived Tests (Scale Factor)](#tpc-h) | TPC-DS Derived Tests (Scale Factor) | [Federation](#federation) | [Data Correctness](#data-correctness) | [Streaming](#streaming) | [Native Schema Inference](#schema-inference) |
| ----------------------- | -------------------------------------------- | ----------------------------------- | ------------------------- | ------------------------------------- | ----------------------- | -------------------------------------------- |
| Clickhouse              | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Databricks              | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Delta Lake              | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Dremio                  | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| DuckDB                  | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| File                    | ✅ (1)                                       | ❌                                  | ❌                        | ❌                                    | ✅                      | ❌                                           |
| FTP/SFTP                | ❌                                           | ❌                                  | ❌                        | ❌                                    | ❌                      | ❌                                           |
| GraphQL                 | ❌                                           | ❌                                  | ❌                        | ❌                                    | ❌                      | ❌                                           |
| GitHub                  | ❌                                           | ❌                                  | ❌                        | ❌                                    | ❌                      | ❌                                           |
| HTTP/HTTPS              | ❌                                           | ❌                                  | ❌                        | ❌                                    | ❌                      | ❌                                           |
| MS SQL                  | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| MySQL                   | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| ODBC                    | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| PostgreSQL              | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Sharepoint              | ❌                                           | ❌                                  | ❌                        | ❌                                    | ❌                      | ❌                                           |
| Snowflake               | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Spice.AI Cloud Platform | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| S3                      | ✅ (1)                                       | ❌                                  | ❌                        | ❌                                    | ✅                      | ❌                                           |
| Azure BlobFS            | ✅ (1)                                       | ❌                                  | ❌                        | ❌                                    | ✅                      | ❌                                           |
| Spark                   | ✅ (1)                                       | ❌                                  | ⚠️                        | ❌                                    | ✅                      | ❌                                           |

### All Connectors

The Beta release criteria expand on and require that all [Alpha release criteria](./alpha.md) continue to pass for the connector.

- [ ] All [Alpha release criteria](./alpha.md) pass for this connector.
- [ ] [Core Connector Data Types](../definitions.md) are supported.

#### Documentation

Documentation criteria should be re-checked on every release, to ensure the documentation is still accurate for the connector.

- [ ] Documentation includes all information and steps for a user to set up the connector.
- [ ] Documentation includes all known issues/limitations for the connector.
- [ ] Documentation includes any exceptions made to allow this connector to reach Beta quality (e.g. if a particular data type cannot be supported by the connector).
- [ ] The connector has an easy to follow quickstart.
- [ ] The connector status is updated in the table of connectors in [spiceai/docs](https://github.com/spiceai/docs).

#### Testing

- [ ] Integration tests to cover simple use cases based on the connector type, e.g. a Spicepod sourcing a file from an FTP server, reading latest GitHub issues, etc.

### Conditional Criteria

The following features/tests are dependent on the required features/tests for the specified connector, from the [Connector criteria table](#beta-release-criteria)

#### Streaming

- [ ] Data is streamed when reading/performing queries from this connector.

#### Schema Inference

Support for schema inference in a connector is classified as:

- ❌: No coverage. The connector does not support native schema inference, and typically infers schema from the first row of results.

#### Federation

Support for federation in a connector is classified as:

- ⚠️: Partial coverage. The connector supports partial federation and query push down.
- ❌: No coverage. The connector does not support federation or query push down.

##### Partial Coverage

- [ ] The connector supports filter federation within a single dataset for common use case columns (e.g. `select * from my_dataset where id = 1`)
  - Common use case should be determined at the discretion of the DRI for the connector.
  - For example, the GitHub connector should support filter federation for the author, state and title of issues.

#### Test Coverage

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

##### TPC-H

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-H queries (official and simple).
  - [ ] Connectors should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-H data at the [designated scale factor](#beta-release-criteria) into this connector.
- [ ] The connector can load TPC-H at the [designated scale factor](#beta-release-criteria), and can run all queries with no [Major Bugs](../definitions.md).
- [ ] All [Minor Bugs](../definitions.md) for TPC-H are raised as issues.
- [ ] TPC-H queries pass with a success rate equal or greater than TPC-H execution on Datafusion.

#### Data Correctness

Data Correctness is not tested using an automated method as part of Beta release criteria.
