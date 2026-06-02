# cayenne-flightsql

A standalone Arrow Flight SQL server backed by a single Cayenne catalog.

## Run

```bash
cargo run -p cayenne-flightsql -- \
  --addr 127.0.0.1:50051 \
  --catalog cayenne \
  --default-schema public
```

## Key options

- `--addr` (`FLIGHTSQL_ADDR`) - listen address.
- `--catalog` (`FLIGHTSQL_CATALOG`) - catalog name registered in DataFusion.
- `--default-schema` (`FLIGHTSQL_DEFAULT_SCHEMA`) - schema used for unqualified table names.
- `--spice-data-base-path` (`CAYENNE_SPICE_DATA_BASE_PATH`) - base path used when explicit Cayenne data/metadata directories are omitted.
- `--cayenne-data-dir` / `--cayenne-metadata-dir` - explicit data + metadata directories.
- `--refresh-interval-secs` (`CAYENNE_REFRESH_INTERVAL_SECS`) - optional periodic catalog refresh.

## Querying

By default, unqualified names resolve to `<catalog>.<default-schema>.<table>`.

On startup, if the configured `--default-schema` does not exist in Cayenne, it is created automatically.

If your table is in a different Cayenne namespace/schema, query with fully-qualified names:

```sql
SELECT * FROM cayenne.analytics.events;
```
