# Spice.ai OSS Data Connectors - RC Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the connector to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## RC Quality Connectors

| Connector                        | RC Quality | DRI Sign-off |
| -------------------------------- | ---------- | ------------ |
| ADBC                             | ➖          |              |
| Azure BlobFS                     | ➖          |              |
| Clickhouse                       | ➖          |              |
| Cosmos DB (NoSQL)                | ✅          | @lukekim     |
| Databricks (mode: delta_lake)    | ✅          | @Sevenannn   |
| Databricks (mode: spark_connect) | ➖          |              |
| Databricks (mode: sql_warehouse) | ➖          |              |
| Debezium                         | ➖          |              |
| Delta Lake                       | ✅          | @Sevenannn   |
| Dremio                           | ✅          | @Sevenannn   |
| DuckDB                           | ✅          | @peasee      |
| DuckLake                         | ➖          |              |
| DynamoDB                         | ✅          | @krinart     |
| File                             | ✅          | @Sevenannn   |
| FlightSQL                        | ➖          |              |
| FTP/SFTP                         | ➖          |              |
| GCS                              | ➖          |              |
| Git                              | ✅          | @lukekim     |
| GitHub                           | ✅          | @peasee      |
| Glue                             | ➖          |              |
| GraphQL                          | ✅          | @peasee      |
| HTTP/HTTPS                       | ➖          |              |
| Iceberg                          | ➖          |              |
| IMAP                             | ➖          |              |
| Kafka                            | ➖          |              |
| Localpod                         | ➖          |              |
| MongoDB                          | ➖          |              |
| MS SQL                           | ➖          |              |
| MySQL                            | ✅          | @peasee      |
| NFS                              | ➖          |              |
| ODBC                             | ➖          |              |
| Oracle                           | ➖          |              |
| PostgreSQL                       | ✅          | @Sevenannn   |
| S3                               | ✅          | @Sevenannn   |
| ScyllaDB                         | ➖          |              |
| Sharepoint                       | ➖          |              |
| SMB                              | ➖          |              |
| Snowflake                        | ➖          |              |
| Spark                            | ➖          |              |
| Spice.ai Cloud Platform          | ✅          | @peasee      |

## RC Release Criteria

Some connectors impose different testing requirements, as by their nature they support different feature requirements.

Some connectors are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test.

This table defines the required features and/or tests for each connector:

| Connector                        | [TPC-H (Scale)][tpch] | [TPC-DS (Scale)][tpcds] | [Federation][fed] | [Data Correctness][data] | [Streaming][stream] | [Schema Inference][schema] |
| -------------------------------- | --------------------- | ----------------------- | ----------------- | ------------------------ | ------------------- | -------------------------- |
| ADBC                             | ➖                     | ➖                       | ☑️                 | ➖                        | ➖                   | ☑️                          |
| Azure BlobFS                     | ✅ (1)                 | ✅ (1)                   | ☑️                 | ➖                        | ✅                   | ☑️                          |
| Clickhouse                       | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| Cosmos DB (NoSQL)                | ➖                     | ➖                       | ➖                 | ➖                        | ➖                   | ☑️                          |
| Databricks (mode: delta_lake)    | ✅ (1)                 | ✅ (1)                   | ☑️                 | ✅                        | ✅                   | ✅                          |
| Databricks (mode: spark_connect) | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| Databricks (mode: sql_warehouse) | ➖                     | ➖                       | ✅                 | ✅                        | ✅                   | ✅                          |
| Debezium                         | ➖                     | ➖                       | ➖                 | ➖                        | ✅                   | ☑️                          |
| Delta Lake                       | ✅ (1)                 | ✅ (1)                   | ☑️                 | ✅                        | ✅                   | ✅                          |
| Dremio                           | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| DuckDB                           | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| DuckLake                         | ➖                     | ➖                       | ☑️                 | ➖                        | ✅                   | ✅                          |
| DynamoDB                         | ✅ (1)                 | ➖                       | ☑️                 | ✅                        | ✅                   | ✅                          |
| File                             | ✅ (1)                 | ✅ (1)                   | ➖                 | ➖                        | ✅                   | ☑️                          |
| FTP/SFTP                         | ➖                     | ➖                       | ➖                 | ➖                        | ➖                   | ☑️                          |
| GCS                              | ✅ (1)                 | ✅ (1)                   | ➖                 | ➖                        | ✅                   | ☑️                          |
| Git                              | ➖                     | ➖                       | ☑️                 | ➖                        | ➖                   | ☑️                          |
| GitHub                           | ➖                     | ➖                       | ☑️                 | ➖                        | ➖                   | ☑️                          |
| Glue                             | ➖                     | ➖                       | ☑️                 | ➖                        | ✅                   | ✅                          |
| GraphQL                          | ➖                     | ➖                       | ➖                 | ➖                        | ➖                   | ☑️                          |
| HTTP/HTTPS                       | ✅ (1)                 | ✅ (1)                   | ➖                 | ➖                        | ➖                   | ☑️                          |
| Iceberg                          | ✅ (1)                 | ✅ (1)                   | ☑️                 | ✅                        | ✅                   | ✅                          |
| IMAP                             | ➖                     | ➖                       | ☑️                 | ✅                        | ✅                   | ☑️                          |
| Kafka                            | ➖                     | ➖                       | ➖                 | ➖                        | ✅                   | ☑️                          |
| MongoDB                          | ➖                     | ➖                       | ➖                 | ➖                        | ➖                   | ☑️                          |
| MS SQL                           | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| MySQL                            | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| NFS                              | ➖                     | ➖                       | ➖                 | ➖                        | ✅                   | ☑️                          |
| ODBC                             | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| Oracle                           | ➖                     | ➖                       | ✅                 | ✅                        | ✅                   | ✅                          |
| PostgreSQL                       | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| S3                               | ✅ (1)                 | ✅ (1)                   | ☑️                 | ➖                        | ✅                   | ☑️                          |
| ScyllaDB                         | ➖                     | ➖                       | ✅                 | ✅                        | ✅                   | ✅                          |
| Sharepoint                       | ➖                     | ➖                       | ☑️                 | ➖                        | ➖                   | ☑️                          |
| SMB                              | ➖                     | ➖                       | ➖                 | ➖                        | ✅                   | ☑️                          |
| Snowflake                        | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |
| Spark                            | ✅ (1)                 | ✅ (1)                   | ✅                 | ✅                        | ✅                   | ✅                          |
| Spice.ai Cloud Platform          | ✅ (100)               | ✅ (100)                 | ✅                 | ✅                        | ✅                   | ✅                          |

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

#### Connection Resilience

Connectors must implement safeguards to prevent thundering herd issues and ensure graceful behavior under load or when the upstream source is degraded:

- [ ] **Concurrency limiting**: The connector limits the number of concurrent in-flight requests to the upstream source (e.g. via a semaphore). HTTP-based connectors that issue dynamic upstream requests must share this limit per upstream origin across datasets, and the limit must be configurable by both a runtime-level default (e.g. `runtime.params.http_max_concurrent_requests`) and a dataset override (e.g. `max_concurrent_requests`). Structured HTTP file datasets routed through listing-table connectors must either implement the same controls or reject/document the unsupported parameters.
- [ ] **Request-rate limiting**: HTTP-based connectors that can issue multiple upstream requests support configurable request budgets (e.g. `requests_per_second_limit` or `requests_per_minute_limit`) and jitter controls (e.g. `rate_control_jitter_min` and `rate_control_jitter_max`) to reduce synchronized request bursts. Runtime-level defaults should be available for HTTP connector families, with dataset parameters overriding them when needed. Structured HTTP file datasets routed through listing-table connectors must either implement the same controls or reject/document the unsupported parameters.
- [ ] **Retry limiting**: HTTP or RPC retries for transient failures (e.g. 429, 5xx, timeouts) are bounded with a configurable maximum (e.g. `http_max_retries`). Retries must use backoff (exponential or fibonacci) and respect upstream `Retry-After` headers when present.
- [ ] **Permanent error detection**: Non-retryable errors (e.g. 401 Unauthorized, 403 Forbidden, 404 Not Found) are detected and the connector enters a permanent error state to prevent further requests from being issued. This must be configurable via a spicepod parameter (e.g. `disable_on_permanent_error`).
- [ ] **Connection pooling or request budgeting**: For connectors that maintain persistent connections or sessions, the pool size or maximum concurrent sessions must be configurable. For HTTP-based connectors, the request concurrency semaphore fulfills this requirement.
- [ ] **Rate-control observability**: The connector exposes rate-control state through the runtime metrics endpoint, including in-flight requests (for example, `inflight_operations`), configured request budgets, available concurrency permits, cumulative rate-control wait time, acquisition errors, and upstream `Retry-After` cooldown/wait counters where applicable.

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
- [ ] The TPC-DS end-to-end test is added to the scheduled benchmarks by creating a [`testoperator dispatch`](https://github.com/spiceai/spiceai/tree/trunk/tools/testoperator/dispatch) configuration file for the accelerator.

##### ClickBench

- [ ] A test script exists that can load ClickBench data into this connector.
- [ ] All queries are attempted on this connector. No bug fixes are required for ClickBench.
- [ ] All ClickBench bugs are raised as issues.

#### Data Correctness

- ➖: Not required. The test suite is not required for this connector.

- [ ] TPC-H queries at the [designated scale factor](#rc-release-criteria) return identical results in Spice and the native connector CLI.
- [ ] TPC-DS queries at the [designated scale factor](#rc-release-criteria) return identical results in Spice and the native connector CLI.
