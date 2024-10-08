# Spice.ai OSS Data Connectors - RC Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the connector to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## RC Quality Connectors

| Connector | [Connector Type](../definitions.md) | RC Quality | DRI Sign-off |
| - | - | - | - |
| Clickhouse   | Relational | ❌ |  |
| Databricks   | Relational |❌ |  |
| Delta Lake   | Relational | ❌ |  |
| Dremio       | Relational | ❌ |  |
| File         | Non-Relational | ❌ |  |
| FTP/SFTP     | Non-Relational | ❌ |  |
| GraphQL      | Non-Relational | ❌ |  |
| GitHub       | Non-Relational | ❌ |  |
| HTTP/HTTPS   | Non-Relational | ❌ |  |
| MS SQL       | Relational | ❌ |  |
| MySQL        | Relational | ❌ |  |
| ODBC         | Relational | ❌ |  |
| PostgreSQL   | Relational | ❌ |  |
| Sharepoint   | Non-Relational | ❌ |  |
| Snowflake    | Relational | ❌ |  |
| Spice.AI     | Relational | ❌ |  |
| S3           | Non-Relational | ❌ |  |
| Azure BlobFS | Non-Relational | ❌ |  |
| Spark        | Relational | ❌ |  |

## RC Release Criteria

### Feature complete

#### All connectors

- [ ] [Core Arrow Data Types](../definitions.md) are supported
- [ ] All known [Major Bugs](../definitions.md) are resolved

#### [Relational Connectors](../definitions.md)

- [ ] Data is streamed when reading/performing queries from this connector
- [ ] The connector supports full federation within a single dataset (e.g. `select * from my_dataset`)
- [ ] The connector supports federation push down across multiple datasets within the same connection source (e.g. `select * from first_dataset, second_dataset`)

#### [Non-Relational Connectors](../definitions.md)

- [ ] The connector supports some level of filter federation within a single dataset (e.g. `select * from my_dataset where id = 1`)

### Test Coverage

RC quality connectors should be able to run test packages derived from the following:

| [Connector Type](../definitions.md) | Test Package (Scale Factor) |
| - | - |
| Relational | [TPC-H](https://www.tpc.org/TPC-H/) (100) |
| Relational | [TPC-DS](https://www.tpc.org/TPC-DS/) (100) |
| Relational | [ClickBench](https://github.com/ClickHouse/ClickBench) (1) |
| Non-Relational | [TPC-H](https://www.tpc.org/TPC-H/) (1) |
| Non-Relational | [TPC-DS](https://www.tpc.org/TPC-DS/) (0.5) |

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

#### General

- [ ] Integration tests to cover simple use cases based on the connector type, e.g. a Spicepod sourcing a file from an FTP server.

#### TPC-H

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-H queries (official and simple).
  - [ ] Connectors should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-H data at the [designated scale factor](#test-coverage) into this connector.
- [ ] The connector can load TPC-H at the [designated scale factor](#test-coverage), and can run all queries with no [Major Bugs](../definitions.md).

#### TPC-DS

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-DS queries (official and simple).
  - [ ] Connectors should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-DS data at the [designated scale factor](#test-coverage) into this connector.
- [ ] The connector can load TPC-DS at the [designated scale factor](#test-coverage), and can run all queries with no [Major Bugs](../definitions.md).

#### ClickBench

- [ ] A test script exists that can load ClickBench data into this connector at the [designated scale factor](#test-coverage).
- [ ] All queries are attempted on this connector, and all query failures should be logged as issues. No bug fixes are required for ClickBench.

#### Data correctness

Data correctness can only be validated where the connector has a native CLI to replicate identical results. For connectors that do not have a native CLI to generate identical results, skip these tests:

- [ ] TPC-H queries at the [designated scale factor](#test-coverage) return identical results in Spice and the native connector CLI.
- [ ] TPC-DS queries at the [designated scale factor](#test-coverage) return identical results in Spice and the native connector CLI.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the connector.
- [ ] Documentation includes all known issues/limitations for the connector.
- [ ] Documentation includes any exceptions made to allow this connector to reach RC quality (e.g. if a particular data type cannot be supported by the connector).
- [ ] The connector has an easy to follow quickstart.
- [ ] All [Minor Bugs](../definitions.md) for TPC-DS and TPC-H are raised as issues.
- [ ] All ClickBench bugs are raised as issues.
