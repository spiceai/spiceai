# Spice.ai OSS Data Connectors - Beta Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of Beta quality.

All criteria must be met for the connector to be considered Beta, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular connector).

## Beta Quality Connectors

| Connector                        | Beta Quality | DRI Sign-off    |
| -------------------------------- | ------------ | --------------- |
| Clickhouse                       | ➖           |                 |
| Databricks (mode: delta_lake)    | ✅           | @Sevenannn      |
| Databricks (mode: spark_connect) | ✅           | @Sevenannn      |
| Delta Lake                       | ✅           | @Sevenannn      |
| Dremio                           | ✅           | @Sevenannn      |
| DuckDB                           | ✅           | @peasee         |
| File                             | ✅           | @peasee         |
| FlightSQL                        | ➖           |                 |
| FTP/SFTP                         | ➖           |                 |
| GraphQL                          | ➖           |                 |
| GitHub                           | ✅           | @peasee         |
| HTTP/HTTPS                       | ➖           |                 |
| IMAP                             | ➖           |                 |
| Localpod                         | ➖           |                 |
| Iceberg                          | ✅           | @phillipleblanc |
| MS SQL                           | ✅           | @peasee         |
| MySQL                            | ✅           | @peasee         |
| ODBC                             | ➖           |                 |
| PostgreSQL                       | ✅           | @Sevenannn      |
| Sharepoint                       | ➖           |                 |
| Snowflake                        | ✅           | @phillipleblanc |
| Spice.ai Cloud Platform          | ✅           | @phillipleblanc |
| S3                               | ✅           | @Sevenannn      |
| Azure BlobFS                     | ➖           |                 |
| Spark                            | ✅           | @ewgenius       |

## Beta Release Criteria

Some connectors impose different testing requirements, as by their nature they support different feature requirements.

Some connectors are unable to support TPC derived test packages due to their nature (e.g. GitHub Issues).
These connectors are exempt from running the TPC derived test packages, and rely instead on their general integration test.

This table defines the required features and/or tests for each connector:

| Connector                        | [TPC-H (Scale)][tpch] | TPC-DS (Scale) | [Federation][fed] | [Data Correctness][data] | [Streaming][stream] | [Schema Inference][schema] |
| -------------------------------- | --------------------- | -------------- | ----------------- | ------------------------ | ------------------- | -------------------------- |
| Clickhouse                       | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| Databricks (mode: delta_lake)    | ✅ (1)                | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| Databricks (mode: spark_connect) | ✅ (100)              | ➖             | ☑️                | ➖                       | ➖                  | ➖                         |
| Delta Lake                       | ✅ (1)                | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| Dremio                           | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| DuckDB                           | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| File                             | ✅ (1)                | ➖             | ➖                | ➖                       | ✅                  | ☑️                         |
| FTP/SFTP                         | ➖                    | ➖             | ➖                | ➖                       | ➖                  | ☑️                         |
| GraphQL                          | ➖                    | ➖             | ➖                | ➖                       | ➖                  | ☑️                         |
| GitHub                           | ➖                    | ➖             | ➖                | ➖                       | ➖                  | ☑️                         |
| HTTP/HTTPS                       | ➖                    | ➖             | ➖                | ➖                       | ➖                  | ☑️                         |
| IMAP                             | ➖                    | ➖             | ☑️                | ➖                       | ✅                  | ☑️                         |
| Iceberg                          | ✅ (1)                | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| MS SQL                           | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| MySQL                            | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| ODBC                             | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| PostgreSQL                       | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| Sharepoint                       | ➖                    | ➖             | ➖                | ➖                       | ➖                  | ☑️                         |
| Snowflake                        | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| Spice.ai Cloud Platform          | ✅ (100)              | ➖             | ☑️                | ➖                       | ✅                  | ➖                         |
| S3                               | ✅ (1)                | ➖             | ➖                | ➖                       | ✅                  | ☑️                         |
| Azure BlobFS                     | ✅ (1)                | ➖             | ➖                | ➖                       | ✅                  | ☑️                         |
| Spark                            | ✅ (1)                | ➖             | ☑️                | ➖                       | ➖                  | ➖                         |

[tpch]: #tpc-h
[fed]: #federation
[stream]: #streaming
[data]: #data-correctness
[schema]: #schema-inference

### All Connectors

The Beta release criteria expand on and require that all [Alpha release criteria](./alpha.md) continue to pass for the connector.

- [ ] All [Alpha release criteria](./alpha.md) pass for this connector.
- [ ] [Core Connector Data Types](../definitions.md) are supported.

#### Documentation

Documentation criteria should be re-checked on every release, to ensure the documentation is still accurate for the connector.

- [ ] Documentation includes all information and steps for a user to set up the connector.
- [ ] Documentation includes all known issues/limitations for the connector.
- [ ] Documentation includes any exceptions made to allow this connector to reach Beta quality (e.g. if a particular data type cannot be supported by the connector).
- [ ] The connector has an easy to follow cookbook recipe.
- [ ] The connector status is updated in the table of connectors in [spiceai/docs](https://github.com/spiceai/docs).

#### UX

- [ ] All of the connector's error messages follow the [error handling guidelines](../../dev/error_handling.md)

#### Testing

- [ ] Integration tests to cover simple use cases based on the connector type, e.g. a Spicepod sourcing a file from an FTP server, reading latest GitHub issues, etc.

### Conditional Criteria

The following features/tests are dependent on the required features/tests for the specified connector, from the [Connector criteria table](#beta-release-criteria)

#### Streaming

- [ ] Data is streamed when reading/performing queries from this connector.

#### Schema Inference

Support for schema inference in a connector is classified as:

- ☑️: Inferred schema. The source does not support natively detecting schema, and schema is inferred from the first row of results.
- ➖: Inferred schema. The connector does not support natively detecting schema, and schema is inferred from the first row of results.

#### Federation

Support for federation in a connector is classified as:

- ☑️: Partial filter push-down. The connector supports partial filter push-down in queries.
- ➖: No federation. The connector does not support federation or query push down.

##### Partial Filter Push-down

- [ ] The connector supports filter push-down within a single dataset for common use case columns (e.g. `select * from my_dataset where id = 1`)
  - Common use case should be determined at the discretion of the DRI for the connector.
  - For example, the GitHub connector should support filter push-down for the author, state and title of issues.

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
