# Spice.ai OSS Data Connectors - Alpha Release Criteria

This document defines the set of criteria that is required before a data connector is considered to be of Alpha quality.

All criteria must be met for the connector to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

## Alpha Quality Connectors

| Connector                        | Alpha Quality | DRI Sign-off    |
| -------------------------------- | ------------- | --------------- |
| Clickhouse                       | ➖            |                 |
| Databricks (mode: delta_lake)    | ✅            | @Sevenannn      |
| Databricks (mode: spark_connect) | ✅            | @Sevenannn      |
| Delta Lake                       | ✅            | @Sevenannn      |
| Dremio                           | ✅            | @Sevenannn      |
| DuckDB                           | ✅            | @peasee         |
| File                             | ✅            | @peasee         |
| FlightSQL                        | ➖            |                 |
| FTP/SFTP                         | ➖            |                 |
| GraphQL                          | ➖            |                 |
| GitHub                           | ✅            | @peasee         |
| HTTP/HTTPS                       | ➖            |                 |
| IMAP                             | ✅            | @peasee         |
| Iceberg                          | ✅            | @phillipleblanc |
| Localpod                         | ➖            |                 |
| MS SQL                           | ✅            | @peasee         |
| MySQL                            | ✅            | @peasee         |
| ODBC                             | ➖            |                 |
| PostgreSQL                       | ✅            | @Sevenannn      |
| Sharepoint                       | ➖            |                 |
| Snowflake                        | ✅            | @phillipleblanc |
| Spice.ai Cloud Platform          | ✅            | @phillipleblanc |
| S3                               | ✅            | @Sevenannn      |
| Azure BlobFS                     | ➖            |                 |
| Spark                            | ✅            | @ewgenius       |

## Alpha Release Criteria

The Alpha Release Criteria for connectors is set at a level that ensures the Connector operates in common conditions with a low error rate.

The Alpha Release Criteria is not intended to cover any edge cases or complex functionality, so federation, streaming, and extensive testing requirements are excluded.

### All Connectors

- [ ] The connector implements the basic functionality of the native source.
  - Basic functionality is determined at the discretion of the connector DRI.
  - For example, for MySQL basic functionality is querying tables. For GraphQL, it would be executing a GraphQL query and returning the results as RecordBatch rows.
- [ ] The connector executes common use cases with a low error rate.
  - A common use case is determined at the discretion of the connector DRI.
- [ ] Known [Minor and Major](../definitions.md) bugs are logged, but not required to be fixed unless needed to achieve a low error rate.
  - A "low error rate" indicates that more than 90% of the time, the common use case succeeds.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the connector.
- [ ] Documentation includes all known issues/limitations for the connector.
- [ ] The connector has an easy to follow cookbook recipe.
- [ ] The connector is added to the table of connectors in [spiceai/docs](https://github.com/spiceai/docs).
