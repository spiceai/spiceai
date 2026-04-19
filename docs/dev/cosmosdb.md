# Azure Cosmos DB (NoSQL / Core SQL) Data Connector

Status: **Alpha** — read-only foundation. Tracking towards RC criteria.

## Configuration

```yaml
datasets:
  - from: cosmosdb:mydb.mycontainer
    name: my_table
    params:
      # Option A — connection string (takes precedence)
      cosmosdb_connection_string: ${secrets:cosmosdb_conn}

      # Option B — explicit endpoint + key
      cosmosdb_account_endpoint: https://my-account.documents.azure.com:443/
      cosmosdb_account_key: ${secrets:cosmosdb_key}

      # Optional: override database (otherwise taken from `from:` path)
      cosmosdb_database: mydb
      # Optional: custom Cosmos SQL query (defaults to `SELECT * FROM c`)
      query: SELECT * FROM c
      # Optional: sample size for schema inference (default 100)
      schema_infer_max_records: "100"
```

The dataset path accepts `database.container`, `database/container`, or just
`container` when `cosmosdb_database` is set explicitly.

## Authentication

The first release supports key-based authentication only, via either a full
Cosmos DB connection string or an explicit `AccountEndpoint` + `AccountKey`
pair. Microsoft Entra ID / managed identity support is planned.

## What's supported

- Read-only (`SELECT`) scans via Cosmos SQL.
- Cross-partition query by default.
- Arrow schema inferred from a sample of documents (system fields
  `_rid`, `_self`, `_etag`, `_attachments`, `_ts` are stripped). Pin a schema
  via the dataset `columns:` property when stability is required.
- Standard Spice acceleration (DuckDB / SQLite / etc.) on top of the connector.

## What's not yet supported

- Filter / projection / limit push-down into Cosmos DB.
- Write (`INSERT` / `UPDATE` / `DELETE`).
- Change feed streaming (`RefreshMode::Changes`).
- Microsoft Entra ID / managed identity authentication.
- Fine-grained partition-key routing.

See [`docs/criteria/connectors/rc.md`](../../criteria/connectors/rc.md) for
the full RC checklist.

## Feature flag

Built into the default `spiced` distribution; also available as the
`cosmosdb` Cargo feature for custom builds:

```bash
SPICED_CUSTOM_FEATURES="cosmosdb" make build-runtime
```
