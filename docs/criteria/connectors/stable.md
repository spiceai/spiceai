# Spice.ai OSS Data Connectors - Stable Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of Stable quality.

All criteria must be met for the connector to be considered Stable, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## Stable Quality Connectors

| Connector                        | Stable Quality | DRI Sign-off |
| -------------------------------- | ---------- | ------------ |
| Clickhouse                       | ❌         |              |
| Databricks (mode: delta_lake)    | ❌         |              |
| Databricks (mode: spark_connect) | ❌         |              |
| Delta Lake                       | ❌         |              |
| Dremio                           | ❌         |              |
| DuckDB                           | ❌         |              |
| File                             | ❌         |              |
| FTP/SFTP                         | ❌         |              |
| GraphQL                          | ❌         |              |
| GitHub                           | ❌         |              |
| HTTP/HTTPS                       | ❌         |              |
| Localpod                         | ❌         |              |
| MS SQL                           | ❌         |              |
| MySQL                            | ❌         |              |
| ODBC                             | ❌         |              |
| PostgreSQL                       | ❌         |              |
| Sharepoint                       | ❌         |              |
| Snowflake                        | ❌         |              |
| Spice.AI Cloud Platform          | ❌         |              |
| S3                               | ❌         |              |
| Azure BlobFS                     | ❌         |              |
| Spark                            | ❌         |              |

## Stable Release Criteria

Some connectors impose different testing requirements, as by their nature they support different feature requirements.

Some connectors are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test.

This table defines the required features and/or tests for each connector:

| Connector                        | [TPC-H Derived Tests (Scale Factor)](#tpc-h) | [TPC-DS Derived Tests (Scale Factor)](#tpc-ds) | [Federation](#federation) | [Data Correctness](./rc.md#data-correctness) | [Streaming](#streaming) | [Native Schema Inference](#schema-inference) |
| -------------------------------- | -------------------------------------------- | ---------------------------------------------- | ------------------------- | ------------------------------------- | ----------------------- | -------------------------------------------- |
| Clickhouse                       | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| Databricks (mode: delta_lake)    | ✅ (5)                                       | ✅ (5)                                         | ⚠️                        | ✅                                    | ✅                      | ✅                                           |
| Databricks (mode: spark_connect) | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| Delta Lake                       | ✅ (5)                                       | ✅ (5)                                         | ⚠️                        | ✅                                    | ✅                      | ✅                                           |
| Dremio                           | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| DuckDB                           | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| File                             | ✅ (5)                                       | ✅ (5)                                         | ❌                        | ❌                                    | ✅                      | ❌                                           |
| FTP/SFTP                         | ❌                                           | ❌                                             | ❌                        | ❌                                    | ❌                      | ❌                                           |
| GraphQL                          | ❌                                           | ❌                                             | ❌                        | ❌                                    | ❌                      | ❌                                           |
| GitHub                           | ❌                                           | ❌                                             | ⚠️                        | ❌                                    | ❌                      | ⚠️                                           |
| HTTP/HTTPS                       | ❌                                           | ❌                                             | ❌                        | ❌                                    | ❌                      | ❌                                           |
| MS SQL                           | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| MySQL                            | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| ODBC                             | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| PostgreSQL                       | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| Sharepoint                       | ❌                                           | ❌                                             | ⚠️                        | ❌                                    | ❌                      | ⚠️                                           |
| Snowflake                        | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| Spice.AI Cloud Platform          | ✅ (1000)                                     | ✅ (1000)                                       | ✅                        | ✅                                    | ✅                      | ✅                                           |
| S3                               | ✅ (5)                                       | ✅ (5)                                         | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Azure BlobFS                     | ✅ (5)                                       | ✅ (5)                                         | ⚠️                        | ❌                                    | ✅                      | ❌                                           |
| Spark                            | ✅ (5)                                       | ✅ (5)                                         | ✅                        | ✅                                    | ✅                      | ✅                                           |

### All Connectors

These requirements are imposed on every connector, regardless of the selected features/tests in the above table.

The Stable release criteria expand on and require that all [RC release criteria](./rc.md) continue to pass for the connector.

- [ ] All [RC release criteria](./rc.md) pass for this connector.

#### Documentation

Documentation criteria should be re-checked on every release, to ensure the documentation is still accurate for the connector.

- [ ] Documentation includes all information and steps for a user to set up the connector.
- [ ] Documentation includes any mappings from [Core Connector Data Types](../definitions.md) to [Core Arrow Data Types](../definitions.md) types.
- [ ] Documentation includes all known issues/limitations for the connector.
- [ ] Documentation includes any exceptions made to allow this connector to reach RC quality (e.g. if a particular data type cannot be supported by the connector).
- [ ] The connector has an easy to follow quickstart.
- [ ] The connector status is updated in the table of connectors in [spiceai/docs](https://github.com/spiceai/docs).

### Conditional Criteria

The following features/tests are dependent on the required features/tests for the specified connector, from the [Connector criteria table](#stable-release-criteria)

#### Test Coverage

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

##### TPC-H

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-H queries (official and simple).
  - [ ] Connectors should run all queries with no [Major or Minor Bugs](../definitions.md).
  - [ ] End-to-end tests should perform [Throughput Tests](../definitions.md) at the required [parallel query count](../definitions.md)
  - [ ] [Throughput Metric](../definitions.md) is calculated and reported with a parallel query count of 1 to serve as a baseline metric.
  - [ ] [Throughput Metric](../definitions.md) is reported as a metric in the test on the overall connector.
  - [ ] Memory usage is collected at the end-to-end test and reported as a metric on the overall connector.
- [ ] A test script exists that can load TPC-H data at the [designated scale factor](#stable-release-criteria) into this connector.
- [ ] The connector can load TPC-H at the [designated scale factor](#stable-release-criteria), and can run all queries with no [Major or Minor Bugs](../definitions.md).
- [ ] TPC-H queries that execute successfully on Datafusion, should execute successfully on the connector.

##### TPC-DS

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-DS queries (official and simple).
  - [ ] Connectors should run all queries with no [Major or Minor Bugs](../definitions.md).
  - [ ] End-to-end tests should perform [Throughput Tests](../definitions.md) at the required [parallel query count](../definitions.md)
  - [ ] [Throughput Metric](../definitions.md) is calculated and reported with a parallel query count of 1 to serve as a baseline metric.
  - [ ] [Throughput Metric](../definitions.md) is reported as a metric in the test on the overall connector.
  - [ ] Memory usage is collected at the end-to-end test and reported as a metric on the overall connector.
- [ ] A test script exists that can load TPC-DS data at the [designated scale factor](#stable-release-criteria) into this connector.
- [ ] The connector can load TPC-DS at the [designated scale factor](#stable-release-criteria), and can run all queries with no [Major or Minor Bugs](../definitions.md).
- [ ] TPC-DS queries that execute successfully on Datafusion, should execute successfully on the connector.

##### ClickBench

- [ ] A test script exists that can load ClickBench data into this connector at the [designated scale factor](#stable-release-criteria).
- [ ] Connectors should run all ClickBench queries with no [Major Bugs](../definitions.md)
