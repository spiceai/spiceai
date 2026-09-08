# Azure Cosmos DB (NoSQL / Core SQL) Data Connector

Status: **RC** — read-only scan with RC-level connection resilience.

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

      # Optional resilience tuning (defaults shown)
      max_concurrent_requests: "4"
      http_max_retries: "3"
      backoff_method: exponential   # or "fibonacci"
      disable_on_permanent_error: "true"
```

The dataset path accepts `database.container`, `database/container`, or just
`container` when `cosmosdb_database` is set explicitly.

## Authentication

Key-based authentication only, via either a full Cosmos DB connection string
or an explicit `AccountEndpoint` + `AccountKey` pair. Microsoft Entra ID /
managed identity support is tracked as a post-RC enhancement.

## What's supported

- Read-only (`SELECT`) scans via Cosmos SQL.
- Cross-partition query by default.
- Arrow schema inferred from a sample of documents (system fields
  `_rid`, `_self`, `_etag`, `_attachments`, `_ts` are stripped). Schema
  pinning is not currently supported — widen `schema_infer_max_records`
  instead to stabilize inference when optional fields are sparse.
- Standard Spice acceleration (DuckDB / SQLite / Arrow in-memory) on top of
  the connector.
- Connection resilience: per-account concurrency semaphore, bounded retries
  with configurable backoff, `Retry-After` / `x-ms-retry-after-ms` handling,
  permanent-error (401/403/404) detection that latches the connector disabled.
- `inflight_operations` metric gauge, exported via the runtime metrics
  endpoint for dashboards. This gauge is scoped per dataset connector
  instance, not as an account-wide aggregate across all datasets using the
  same Cosmos account endpoint.
- `unsupported_type_action` plumbing — all-null sampled fields (inferred as
  `DataType::Null`) are warn-and-dropped by default.

## JSON → Arrow type mapping

Cosmos stores documents as JSON. The connector samples up to
`schema_infer_max_records` documents and hands them to Arrow's JSON inference:

| Cosmos / JSON value         | Arrow data type | Notes                                                                                                                                                       |
| --------------------------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `"abc"`                     | `Utf8`          |                                                                                                                                                             |
| integer (`42`, `-7`, ...)   | `Int64`         | JSON numbers without fractional part infer as `Int64`; widens to `Float64` if any sampled doc contains a decimal.                                           |
| floating (`3.14`, `1.0e9`)  | `Float64`       |                                                                                                                                                             |
| `true` / `false`            | `Boolean`       |                                                                                                                                                             |
| object `{ ... }`            | `Struct`        | Nested objects are preserved as structs.                                                                                                                    |
| array `[ ... ]`             | `List`          | The element type is inferred from the first non-null item; heterogeneous arrays may surface as `Utf8` or require a wider sample to disambiguate.            |
| all-null in sample          | `Null`          | Warn-dropped by default (`unsupported_type_action=warn`). Set `unsupported_type_action=string` to coerce to `Utf8`, or widen the sample so real values appear. |
| System fields (`_rid`, ...) | stripped        | Never appear in the dataset schema.                                                                                                                         |

Cosmos does not emit `Date`, `Time`, `Timestamp`, `Decimal`, or `Binary`
natively — they round-trip as strings and should be handled with `CAST` at
query time.

## RC exceptions

Per `docs/criteria/connectors/rc.md` row 66, the following tracks are
intentionally out-of-scope for this connector's RC:

| Criterion         | Status | Reason                                                                                      |
| ----------------- | ------ | ------------------------------------------------------------------------------------------- |
| TPC-H / TPC-DS    | ➖      | Cosmos DB's SQL surface does not cover TPC workloads; exempt per the per-connector matrix.  |
| Federation        | ➖      | Cosmos SQL does not support joins across containers; no filter or projection push-down yet. |
| Data Correctness  | ➖      | No TPC harness, so no correctness diff against a native CLI.                                |
| Streaming         | ➖      | No change-feed support yet; `RefreshMode::Changes` is not wired.                            |
| Schema Inference  | ☑️     | Inferred from a sample of documents — Cosmos DB has no native schema.                       |

## What's not yet supported (post-RC tracking)

- Filter / projection / limit push-down into Cosmos DB.
- Write (`INSERT` / `UPDATE` / `DELETE`).
- Change feed streaming (`RefreshMode::Changes`).
- Microsoft Entra ID / managed identity authentication.
- Fine-grained partition-key routing.

## Resilience parameters

These parameters satisfy the "Connection Resilience" section of
`docs/criteria/connectors/rc.md`.

| Parameter                    | Default       | Description                                                                                             |
| ---------------------------- | ------------- | ------------------------------------------------------------------------------------------------------- |
| `max_concurrent_requests`    | `4`           | Upper bound on in-flight requests per account endpoint. Shared across datasets targeting the same account. |
| `http_max_retries`           | `3`           | Maximum retries for transient errors (429, 5xx, network).                                               |
| `backoff_method`             | `exponential` | Backoff strategy: `exponential` (500ms × 2ⁿ, capped 30s) or `fibonacci` (500ms × Fₙ, capped 30s).        |
| `disable_on_permanent_error` | `true`        | Latch the connector disabled on 401/403/404 to avoid a thundering herd of failed requests.              |

Retries honor both the standard `Retry-After` header and the Cosmos-specific
`x-ms-retry-after-ms` header. The effective delay is `max(retry_after, backoff)`.

**Retry scope:** `http_max_retries` / `backoff_method` apply to the schema
inference pass that runs at dataset registration. Errors surfaced *during* a
streaming scan propagate immediately to the caller — a `FeedPager` cannot be
safely rewound once rows have been emitted, so mid-stream retry would risk
duplicating output. Spice's dataset refresh layer handles retry at the query
boundary. The permanent-error latch (`disable_on_permanent_error`) still
applies on both paths, so a 401/403/404 from any request disables the
connector account-wide.

The `inflight_operations` metric is automatically registered and reports the
current number of Cosmos requests holding a concurrency permit.

## Integration tests

Unit-level coverage lives in `crates/data_components/src/cosmosdb/` (32 tests
at time of RC) and `crates/runtime/src/dataconnector/cosmosdb.rs`.

End-to-end tests against a live Cosmos account live at
`crates/runtime/tests/cosmosdb/`. Live tests are `#[ignore]`'d by default;
set `COSMOSDB_CONNECTION_STRING` (or `COSMOSDB_ACCOUNT_ENDPOINT` + `COSMOSDB_ACCOUNT_KEY`),
optionally `COSMOSDB_INTEGRATION_DATABASE` / `COSMOSDB_INTEGRATION_CONTAINER`,
then run:

```bash
cargo test --features cosmosdb -p runtime --test integration -- --ignored cosmosdb_live
```

The Azure Cosmos emulator image (`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator`)
is not used in CI — its 3+ GB size and 3–5 minute cold-start exceeds the
shared runner's budgets. A future `cosmosdb-emulator` feature flag can add
on-demand emulator tests.

## Feature flag

Built into the default `spiced` distribution; also available as the
`cosmosdb` Cargo feature for custom builds:

```bash
SPICED_CUSTOM_FEATURES="cosmosdb" make build-runtime
```

## Cookbook recipe

See [`examples/cosmosdb-connector/`](../../examples/cosmosdb-connector/) for
a copy-pasteable Spicepod that connects to Cosmos DB.
