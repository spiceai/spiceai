# Spice.ai OSS Data Connectors - RC Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the connector to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## RC Quality Connectors

| Connector | [Connector Type](../definitions.md) | RC Quality | DRI Sign-off |
| - | - | - | - |
| Clickhouse   | Structured | ❌ |  |
| Databricks   | Structured |❌ |  |
| Delta Lake   | Structured | ❌ |  |
| Dremio       | Structured | ❌ |  |
| File         | Unstructured | ❌ |  |
| FTP/SFTP     | Unstructured | ❌ |  |
| GraphQL      | Unstructured | ❌ |  |
| GitHub       | Unstructured | ❌ |  |
| HTTP/HTTPS   | Unstructured | ❌ |  |
| MS SQL       | Structured | ❌ |  |
| MySQL        | Structured | ❌ |  |
| ODBC         | Structured | ❌ |  |
| PostgreSQL   | Structured | ❌ |  |
| Sharepoint   | Unstructured | ❌ |  |
| Snowflake    | Structured | ❌ |  |
| Spice.AI     | Structured | ❌ |  |
| S3           | Unstructured | ❌ |  |
| Azure BlobFS | Unstructured | ❌ |  |
| Spark        | Structured | ❌ |  |

## RC Release Criteria

### Feature complete

#### All connectors

- [ ] [Core Arrow Data Types](../definitions.md) are supported
- [ ] All known [Major Bugs](../definitions.md) are resolved

#### [Structured Connectors](../definitions.md)

- [ ] Data is streamed when reading/performing queries from this connector
- [ ] The connector supports full federation within a single dataset (e.g. `select * from my_dataset`)
- [ ] The connector supports federation push down across multiple datasets within the same connection source (e.g. `select * from first_dataset, second_dataset`)

#### [Unstructured Connectors](../definitions.md)

- [ ] The connector supports filter federation within a single dataset for common use case columns (e.g. `select * from my_dataset where id = 1`)
  - Common use case should be determined at the discretion of the DRI for the connector.
  - For example, the GitHub connector should support filter federation for the author, state and title of issues.

### Test Coverage

RC quality connectors should be able to run test packages derived from the following:

| [Connector Type](../definitions.md) | Test Package (Scale Factor) |
| - | - |
| Structured | [TPC-H](https://www.tpc.org/TPC-H/) (100) |
| Structured | [TPC-DS](https://www.tpc.org/TPC-DS/) (100) |
| Structured | [ClickBench](https://github.com/ClickHouse/ClickBench) (1) |
| Unstructured | [TPC-H](https://www.tpc.org/TPC-H/) (1) |
| Unstructured | [TPC-DS](https://www.tpc.org/TPC-DS/) (0.5) |

Some [Unstructured Connectors](../definitions.md) are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test:

- GitHub
- Sharepoint
- GraphQL

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

#### General

- [ ] Integration tests to cover simple use cases based on the connector type, e.g. a Spicepod sourcing a file from an FTP server, reading latest GitHub issues, etc.

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
