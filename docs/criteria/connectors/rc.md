# Spice.ai OSS Data Connectors - RC Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the connector to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## RC Quality Connectors

| Connector                        | RC Quality | DRI Sign-off |
| -------------------------------- | ---------- | ------------ |
| Clickhouse                       | ➖         |              |
| Databricks (mode: delta_lake)    | ✅         | @Sevenannn   |
| Databricks (mode: spark_connect) | ➖         |              |
| Delta Lake                       | ✅         | @Sevenannn   |
| Dremio                           | ✅         | @Sevenannn   |
| DuckDB                           | ✅         | @peasee      |
| File                             | ✅         | @Sevenannn   |
| FlightSQL                        | ➖         |              |
| FTP/SFTP                         | ➖         |              |
| GraphQL                          | ✅         | @peasee      |
| GitHub                           | ✅         | @peasee      |
| HTTP/HTTPS                       | ➖         |              |
| IMAP                             | ➖         |              |
| Localpod                         | ➖         |              |
| MS SQL                           | ➖         |              |
| MySQL                            | ✅         | @peasee      |
| ODBC                             | ➖         |              |
| PostgreSQL                       | ✅         | @Sevenannn   |
| Sharepoint                       | ➖         |              |
| Snowflake                        | ➖         |              |
| Spice.ai Cloud Platform          | ✅         | @peasee      |
| S3                               | ✅         | @Sevenannn   |
| Azure BlobFS                     | ➖         |              |
| Spark                            | ➖         |              |

## RC Release Criteria

Some connectors impose different testing requirements, as by their nature they support different feature requirements.

Some connectors are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test.

This table defines the required features and/or tests for each connector:

| Connector                        | [TPC-H (Scale)][tpch] | [TPC-DS (Scale)][tpcds] | [Federation][fed] | [Data Correctness][data] | [Streaming][stream] | [Schema Inference][schema] |
| -------------------------------- | --------------------- | ----------------------- | ----------------- | ------------------------ | ------------------- | -------------------------- |
| Clickhouse                       | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Databricks (mode: delta_lake)    | ✅ (1)                | ✅ (1)                  | ☑️                | ✅                       | ✅                  | ✅                         |
| Databricks (mode: spark_connect) | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Delta Lake                       | ✅ (1)                | ✅ (1)                  | ☑️                | ✅                       | ✅                  | ✅                         |
| Dremio                           | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| DuckDB                           | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| File                             | ✅ (1)                | ✅ (1)                  | ➖                | ➖                       | ✅                  | ☑️                         |
| FTP/SFTP                         | ➖                    | ➖                      | ➖                | ➖                       | ➖                  | ☑️                         |
| GraphQL                          | ➖                    | ➖                      | ➖                | ➖                       | ➖                  | ☑️                         |
| GitHub                           | ➖                    | ➖                      | ☑️                | ➖                       | ➖                  | ☑️                         |
| HTTP/HTTPS                       | ✅ (1)                | ✅ (1)                  | ➖                | ➖                       | ➖                  | ☑️                         |
| IMAP                             | ➖                    | ➖                      | ☑️                | ✅                       | ✅                  | ☑️                         |
| Iceberg                          | ✅ (1)                | ✅ (1)                  | ☑️                | ✅                       | ✅                  | ✅                         |
| MS SQL                           | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| MySQL                            | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| ODBC                             | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| PostgreSQL                       | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Sharepoint                       | ➖                    | ➖                      | ☑️                | ➖                       | ➖                  | ☑️                         |
| Snowflake                        | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Spice.AI Cloud Platform          | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| S3                               | ✅ (1)                | ✅ (1)                  | ☑️                | ➖                       | ✅                  | ☑️                         |
| Azure BlobFS                     | ✅ (1)                | ✅ (1)                  | ☑️                | ➖                       | ✅                  | ☑️                         |
| Spark                            | ✅ (1)                | ✅ (1)                  | ✅                | ✅                       | ✅                  | ✅                         |

[tpch]: #tpc-h
[tpcds]: #tpc-ds
[fed]: #federation
[stream]: #streaming
[data]: #data-correctness
[schema]: #schema-inference

### All Connectors

These requirements are imposed on every connector, regardless of the selected features/tests in the above table.

The RC release criteria expand on and require that all [Beta release criteria](./beta.md) continue to pass for the connector.

- [ ] All [Beta release criteria](./beta.md) pass for this connector.
- [ ] For exceptions where a [Core Connector Data Type](../definitions.md) is not supported, print a warning and ignore the column instead of crashing or failing the query.
- [ ] All known [Major Bugs](../definitions.md) are resolved.

#### Documentation

Documentation criteria should be re-checked on every release, to ensure the documentation is still accurate for the connector.

- [ ] Documentation includes all information and steps for a user to set up the connector.
- [ ] Documentation includes any mappings from [Core Connector Data Types](../definitions.md) to [Core Arrow Data Types](../definitions.md) types.
- [ ] Documentation includes all known issues/limitations for the connector.
- [ ] Documentation includes any exceptions made to allow this connector to reach RC quality (e.g. if a particular data type cannot be supported by the connector).
- [ ] The connector has an easy to follow cookbook recipe.
- [ ] The connector status is updated in the table of connectors in [spiceai/docs](https://github.com/spiceai/docs).

### Conditional Criteria

The following features/tests are dependent on the required features/tests for the specified connector, from the [Connector criteria table](#rc-release-criteria)

#### Streaming

- [ ] Data is streamed when reading/performing queries from this connector.

#### Schema Inference

Support for schema inference in a connector is classified as:

- ✅: Native schema. The connector supports natively detecting schema.
- ☑️: Inferred schema. The source does not support natively detecting schema, and schema is inferred from the first row of results.
- ➖: Inferred schema. The connector does not support natively detecting schema, and schema is inferred from the first row of results.

##### Native schema

- [ ] The schema for data returned from queries is determined using a native CLI/library method provided by the connector.

#### Federation

Support for federation in a connector is classified as:

- ✅: Full federation. The connector supports full federation and query push down.
- ☑️: Partial filter push-down. The connector supports partial filter push-down in queries.
- ➖: No federation. The connector does not support federation or query push down.

##### Full Federation

- [ ] The connector supports full federation within a single dataset (e.g. `select * from my_dataset`)
- [ ] The connector supports federation push down across multiple datasets within the same connection source (e.g. `select * from first_dataset, second_dataset`)

##### Partial Filter Push-down

- [ ] The connector supports filter push-down within a single dataset for common use case columns (e.g. `select * from my_dataset where id = 1`)
  - Common use case should be determined at the discretion of the DRI for the connector.
  - For example, the GitHub connector should support filter push-down for the author, state and title of issues.

#### Test Coverage

- ➖: Not required. The test suite is not required for this connector, because it is not applicable (e.g. GraphQL, etc).

Indexes are not required for test coverage, but can be introduced if required for tests to pass (e.g. due to performance characteristics, etc).

##### TPC-H

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-H queries (official and simple).
  - [ ] Connectors should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-H data at the [designated scale factor](#rc-release-criteria) into this connector.
- [ ] The connector can load TPC-H at the [designated scale factor](#rc-release-criteria), and can run all queries with no [Major Bugs](../definitions.md).
- [ ] All [Minor Bugs](../definitions.md) for TPC-H are raised as issues.
- [ ] TPC-H queries that execute successfully on Datafusion, should execute successfully on the connector.

##### TPC-DS

- [ ] End-to-end test to cover connecting to TPC-H SF1 for the connector type and benchmarking TPC-DS queries (official and simple).
  - [ ] Connectors should run all queries with no [Major Bugs](../definitions.md).
- [ ] A test script exists that can load TPC-DS data at the [designated scale factor](#rc-release-criteria) into this connector.
- [ ] The connector can load TPC-DS at the [designated scale factor](#rc-release-criteria), and can run all queries with no [Major Bugs](../definitions.md).
- [ ] All [Minor Bugs](../definitions.md) for TPC-DS are raised as issues.
- [ ] TPC-DS queries that execute successfully on Datafusion, should execute successfully on the connector.

##### ClickBench

- [ ] A test script exists that can load ClickBench data into this connector.
- [ ] All queries are attempted on this connector. No bug fixes are required for ClickBench.
- [ ] All ClickBench bugs are raised as issues.

#### Data Correctness

- ➖: Not required. The test suite is not required for this connector.

- [ ] TPC-H queries at the [designated scale factor](#rc-release-criteria) return identical results in Spice and the native connector CLI.
- [ ] TPC-DS queries at the [designated scale factor](#rc-release-criteria) return identical results in Spice and the native connector CLI.
