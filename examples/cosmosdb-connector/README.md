# Azure Cosmos DB Connector Example

This example demonstrates the Azure Cosmos DB (NoSQL) data connector, which
lets you query Cosmos DB containers as Spice datasets.

## Prerequisites

- Spice CLI installed (`spice` command available).
- An Azure Cosmos DB account (NoSQL / Core SQL API). The [free tier][free]
  is sufficient for trying this example.
- A container with some documents to query — see "Seeding sample data" below
  if you need to populate one.

[free]: https://learn.microsoft.com/azure/cosmos-db/try-free

## Running the example

1. Export your Cosmos DB connection string (copy it from the Azure portal,
   "Keys" blade under your account):

   ```bash
   export COSMOSDB_CONNECTION_STRING="AccountEndpoint=https://<account>.documents.azure.com:443/;AccountKey=<key>;"
   ```

2. Navigate to this directory:

   ```bash
   cd examples/cosmosdb-connector
   ```

3. Edit `spicepod.yaml` to point the `from:` at your `database.container`.

4. Start Spice:

   ```bash
   spice run
   ```

5. In another terminal, connect to the SQL REPL and try a query:

   ```bash
   spice sql
   ```

   ```sql
   SELECT COUNT(*) FROM products;
   SELECT * FROM products LIMIT 5;
   ```

## Datasets

`spicepod.yaml` declares three datasets that cover the common use cases:

### `products`

Full scan over a single container using the default query
(`SELECT * FROM c`). Schema is inferred from the first 100 documents.

### `active_orders`

Custom Cosmos SQL query with a WHERE clause. Useful when the container is
large and you only want a subset surfaced as a dataset.

### `products_pinned`

Dataset with an explicit Arrow schema via `columns:`. Use this pattern when
the sample-based inference disagrees with your production schema (e.g. an
optional field that is null in the first N documents but not in general).

## Parameters used in this example

### Authentication

- `cosmosdb_connection_string`: Full Azure connection string (takes
  precedence over explicit endpoint + key). Recommended for local dev.
- `cosmosdb_account_endpoint` + `cosmosdb_account_key`: Discrete pieces,
  useful when the endpoint and key are stored separately (e.g. Key Vault).

### Data shape

- `cosmosdb_database`: Overrides the database name parsed from the `from:`
  path. Leave unset when the path is `database.container`.
- `query`: Custom Cosmos SQL query. Defaults to `SELECT * FROM c`.
- `schema_infer_max_records`: Number of documents sampled during schema
  inference. Default `100`. Larger samples produce a more precise schema at
  the cost of additional RU consumption on dataset registration.

### Resilience tuning

- `max_concurrent_requests`: Per-account concurrency budget. Default `4`.
- `http_max_retries`: Retries for transient errors. Default `3`.
- `backoff_method`: `exponential` (default) or `fibonacci`.
- `disable_on_permanent_error`: Default `true`. Latches the connector
  disabled on 401/403/404 to prevent a thundering herd of failed requests.
  Set to `"false"` during development if you'd rather see every failure.

## Seeding sample data

If you need a container to query, the snippet below creates the `products`
container used in `spicepod.yaml` and populates it with a few rows. Requires
the Azure CLI and the `az cosmosdb sql` extension.

```bash
# Replace with your account + resource group
ACCOUNT=your-cosmos-account
RG=your-resource-group
DB=store
CONTAINER=products

az cosmosdb sql database create --account-name "$ACCOUNT" --resource-group "$RG" --name "$DB"
az cosmosdb sql container create --account-name "$ACCOUNT" --resource-group "$RG" \
  --database-name "$DB" --name "$CONTAINER" --partition-key-path /id

# Insert a few documents via the Data Explorer, the REST API, or the SDK of your choice.
```

## Troubleshooting

### Authentication failed (401 / 403)

The connector latches disabled after a 401/403/404. Fix the credentials or
grants in `spicepod.yaml`, then restart `spice run`. Set
`disable_on_permanent_error: "false"` only if you want the connector to
keep retrying every failure.

### `EmptyContainer` error on dataset load

Schema is inferred from the sample. If your query returns zero documents at
load time, the connector cannot produce a schema. Either populate the
container, widen the `query`, or pin a schema via `columns:`.

### High RU consumption on load

Each dataset registration samples up to `schema_infer_max_records`
documents. Lower that value — or pin a schema — if you want to avoid the
upfront RU cost.

## Learn more

- [Cosmos DB Connector Reference](../../docs/dev/cosmosdb.md)
- [RC Release Criteria](../../docs/criteria/connectors/rc.md)
- [Spice Documentation](https://docs.spice.ai)
