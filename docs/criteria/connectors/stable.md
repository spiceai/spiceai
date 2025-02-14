# Spice.ai OSS Data Connectors - Stable Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of Stable quality.

All criteria must be met for the connector to be considered Stable, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## Stable Quality Connectors

| Connector                        | Stable Quality | DRI Sign-off    |
| -------------------------------- | -------------- | --------------- |
| Clickhouse                       | ➖             |                 |
| Databricks (mode: delta_lake)    | ✅             | @Sevenannn      |
| Databricks (mode: spark_connect) | ➖             |                 |
| Delta Lake                       | ✅             | @Sevenannn      |
| Dremio                           | ✅             | @Sevenannn      |
| DuckDB                           | ✅             | @peasee         |
| File                             | ✅             | @ewgenius       |
| FlightSQL                        | ➖             |                 |
| FTP/SFTP                         | ➖             |                 |
| GraphQL                          | ➖             |                 |
| GitHub                           | ✅             | @phillipleblanc |
| HTTP/HTTPS                       | ➖             |                 |
| IMAP                             | ➖             |                 |
| Localpod                         | ➖             |                 |
| MS SQL                           | ➖             |                 |
| MySQL                            | ✅             | @phillipleblanc |
| ODBC                             | ➖             |                 |
| PostgreSQL                       | ✅             | @peasee         |
| Sharepoint                       | ➖             |                 |
| Snowflake                        | ➖             |                 |
| Spice.ai Cloud Platform          | ➖             |                 |
| S3                               | ✅             | @ewgenius       |
| Azure BlobFS                     | ➖             |                 |
| Spark                            | ➖             |                 |

## Stable Release Criteria

Some connectors impose different testing requirements, as by their nature they support different feature requirements.

Some connectors are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test.

This table defines the required features and/or tests for each connector:

| Connector                        | [TPC-H (Scale)][tpch] | [TPC-DS (Scale)][tpcds] | [Federation][fed] | [Data Correctness][data] | [Streaming][stream] | [Schema Detection][schema] |
| -------------------------------- | --------------------- | ----------------------- | ----------------- | ------------------------ | ------------------- | -------------------------- |
| Clickhouse                       | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Databricks (mode: delta_lake)    | ✅ (5)                | ✅ (5)                  | ☑️                | ✅                       | ✅                  | ✅                         |
| Databricks (mode: spark_connect) | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Delta Lake                       | ✅ (5)                | ✅ (5)                  | ☑️                | ✅                       | ✅                  | ✅                         |
| Dremio                           | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| DuckDB                           | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| File                             | ✅ (5)                | ✅ (5)                  | ➖                | ✅                       | ✅                  | ☑️                         |
| FTP/SFTP                         | ➖                    | ➖                      | ➖                | ✅                       | ✅                  | ☑️                         |
| GraphQL                          | ➖                    | ➖                      | ➖                | ✅                       | ✅                  | ☑️                         |
| GitHub                           | ➖                    | ➖                      | ☑️                | ✅                       | ✅                  | ☑️                         |
| HTTP/HTTPS                       | ✅ (5)                | ✅ (5)                  | ➖                | ✅                       | ✅                  | ☑️                         |
| IMAP                             | ➖                    | ➖                      | ☑️                | ✅                       | ✅                  | ☑️                         |
| Iceberg                          | ✅ (5)                | ✅ (5)                  | ☑️                | ✅                       | ✅                  | ✅                         |
| MS SQL                           | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| MySQL                            | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| ODBC                             | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| PostgreSQL                       | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Sharepoint                       | ➖                    | ➖                      | ☑️                | ✅                       | ✅                  | ☑️                         |
| Snowflake                        | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| Spice.AI Cloud Platform          | ✅ (100)              | ✅ (100)                | ✅                | ✅                       | ✅                  | ✅                         |
| S3                               | ✅ (5)                | ✅ (5)                  | ☑️                | ✅                       | ✅                  | ☑️                         |
| Azure BlobFS                     | ✅ (5)                | ✅ (5)                  | ☑️                | ✅                       | ✅                  | ☑️                         |
| Spark                            | ✅ (5)                | ✅ (5)                  | ✅                | ✅                       | ✅                  | ✅                         |

[tpch]: #tpc-h
[tpcds]: #tpc-ds
[fed]: #federation
[stream]: #streaming
[data]: #data-correctness
[schema]: #schema-inference

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
- [ ] The connector has an easy to follow cookbook recipe.
- [ ] The connector status is updated in the table of connectors in [spiceai/docs](https://github.com/spiceai/docs).

### Conditional Criteria

The following features/tests are dependent on the required features/tests for the specified connector, from the [Connector criteria table](#stable-release-criteria)

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

- [ ] End-to-end tests should perform [Throughput Tests](../definitions.md) at the required [parallel query count](../definitions.md) at scale factor 1.
- [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric with a parallel query count of 1 to serve as a baseline metric.
- [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric at the required [parallel query count](../definitions.md).
- [ ] Memory usage is collected at the end of the end-to-end test and reported as a metric on the overall connector.
- [ ] At the scale factor required by the connector criteria:
  - [ ] A test script exists that can load TPC-H data at the [designated scale factor](#stable-release-criteria) into this connector.
  - [ ] The connector can load TPC-H at the [designated scale factor](#stable-release-criteria), and can run all queries with no [Major or Minor Bugs](../definitions.md).
  - [ ] TPC-H queries that execute successfully on Datafusion, should execute successfully on the connector.

##### TPC-DS

- [ ] Connectors should run all queries with no [Major or Minor Bugs](../definitions.md).
- [ ] End-to-end tests should perform [Throughput Tests](../definitions.md) at the required [parallel query count](../definitions.md) at scale factor 1.
- [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric with a parallel query count of 1 to serve as a baseline metric.
- [ ] [Throughput Metric](../definitions.md) is calculated and reported as a metric at the required [parallel query count](../definitions.md).
- [ ] A [Load Test](../definitions.md) runs for a minimum of 8 hours at scale factor 1 as part of the end-to-end test. The 99th percentile of load test query [timing measurements](../definitions.md) must be compared against than the 99th percentile of the baseline throughput test timing measurements.
  - Three or more [Yellow percentile measurements](../definitions.md#stop-light-percentile-measurements) are considered a test failure.
  - One or more [Red percentile measurements](../definitions.md#stop-light-percentile-measurements) are considered a test failure.
  - The service must not become unavailable for the entire duration of the test. A connection failure is considered a test failure.
  - Queries that have a 99th percentile execution time faster than 1000ms are excluded from this check, as they complete so fast that this check is not meaningful.
  - Note: If the load test fails due to inconsistencies in the underlying source's execution, the test is considered passed if documented evidence is provided (e.g., data source telemetry, logs, etc.).
- [ ] Memory usage is collected at the end of the end-to-end test and reported as a metric on the overall connector.
- [ ] At the scale factor required by the connector criteria:
  - [ ] A test script exists that can load TPC-DS data at the [designated scale factor](#stable-release-criteria) into this connector.
  - [ ] The connector can load TPC-DS at the [designated scale factor](#stable-release-criteria), and can run all queries with no [Major or Minor Bugs](../definitions.md).
  - [ ] TPC-DS queries that execute successfully on Datafusion, should execute successfully on the connector.

> **[Load Test](../definitions.md) exceptions for Object Store based connectors**
>
> `S3`, `ABFS`, `File`, `FTP`/`SFTP` and `HTTPS` connectors loading data from Parquet, CSV, TSV and JSON files have reduced requirements for the load test.
>
> - [ ] Load test should run all queries with no [Major or Minor Bugs](../definitions.md) at scale factor 1.
> - [ ] Connector documentation includes any performance limitations and exceptions noted from the load test run (e.g., high CPU and/or memory usage, high network bandwidth consumption, etc.).
>
> Percentile measurements and service availability requirements omitted.

##### ClickBench

- [ ] A test script exists that can load ClickBench data into this connector.
- [ ] Connectors should run all ClickBench queries with no [Major Bugs](../definitions.md)

#### Data Correctness

- [ ] TPC-H queries at the [designated scale factor](#stable-release-criteria) return identical results in Spice and the native connector CLI.
- [ ] TPC-DS queries at the [designated scale factor](#stable-release-criteria) return identical results in Spice and the native connector CLI.
