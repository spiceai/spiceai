# Databricks Connector: Resilience Controls, UC Awareness, and Observability

The Databricks connector includes resilience controls, Unity Catalog awareness, and observability instrumentation for production workloads. These features apply primarily to `sql_warehouse` mode but some (UC awareness, task history) also apply to `delta_lake` mode.

## Resilience Controls

### Configurable Retry and Concurrency Parameters

When using `mode: sql_warehouse`, the connector exposes parameters to tune HTTP retry behavior and concurrency limits for the Databricks SQL Statements API.

| Parameter                    | Type    | Default     | Description                                                                                                              |
| ---------------------------- | ------- | ----------- | ------------------------------------------------------------------------------------------------------------------------ |
| `max_concurrent_requests`    | integer | `8`         | Maximum number of concurrent HTTP requests to the SQL Warehouse API. Controls a semaphore that gates all outbound calls. |
| `http_max_retries`           | integer | `3`         | Maximum number of HTTP-level retries for transient failures (429 rate-limit, 5xx server errors).                         |
| `backoff_method`             | string  | `fibonacci` | Backoff strategy for transient HTTP retries. One of `fibonacci` or `exponential`.                                        |
| `statement_max_retries`      | integer | `14`        | Maximum number of poll retries when waiting for an async SQL statement to complete (PENDING/RUNNING states).             |
| `disable_on_permanent_error` | boolean | `true`      | When `true`, non-retryable HTTP errors (401, 403, 404) permanently disable the connector to prevent a thundering herd.   |

#### Example

```yaml
catalogs:
  - from: databricks:my_catalog
    name: my_catalog
    params:
      endpoint: my-workspace.cloud.databricks.com
      mode: sql_warehouse
      sql_warehouse_id: abc123def456
      databricks_client_id: ${env:DBX_CLIENT_ID}
      databricks_client_secret: ${env:DBX_CLIENT_SECRET}
      max_concurrent_requests: '4'
      http_max_retries: '5'
      backoff_method: exponential
      statement_max_retries: '20'
      disable_on_permanent_error: 'true'
```

### Shared Concurrency Semaphore

When multiple datasets target the same SQL Warehouse (same `endpoint` + `sql_warehouse_id`), the connector factory automatically shares a single concurrency semaphore across all of them. This ensures the `max_concurrent_requests` limit is enforced **globally** rather than per-dataset, preventing overloading the warehouse when many datasets are configured.

This applies to both the data connector and catalog connector paths.

### Permanent-Disable Fuse

When `disable_on_permanent_error` is `true` (default), non-retryable HTTP status codes (401 Unauthorized, 403 Forbidden, 404 Not Found) permanently disable the connector instance. Subsequent queries fail immediately with a `PermanentlyDisabled` error instead of issuing further HTTP requests. This prevents cascading failures when credentials are revoked or the warehouse is deleted. To recover, fix the underlying issue and restart the runtime.

### Retry Behavior

The SQL Warehouse connector has two layers of retry:

1. **HTTP-level retries** — Handled by the shared `resilient_http` module. Retries on 429 (rate-limit) and 5xx (server error) responses. Respects `Retry-After`, `retry-after-ms`, and `x-retry-after-ms` headers. Uses the configured `backoff_method` with a maximum backoff of 300 seconds.

2. **Statement poll retries** — When a SQL statement enters PENDING or RUNNING state (async execution), the connector polls `GET /api/2.0/sql/statements/{statement_id}` with fibonacci backoff up to `statement_max_retries` times. If the statement does not reach a terminal state within the retry budget, it returns a `QueryStillRunning` or `InvalidWarehouseState` error.

## Unity Catalog Awareness

### Table Type Filtering

The connector checks each table's type against Unity Catalog metadata before attempting to create a table provider. Only the following table types are supported for querying:

| Table Type          | Supported | Notes                                  |
| ------------------- | --------- | -------------------------------------- |
| `MANAGED`           | Yes       | Standard Delta tables                  |
| `EXTERNAL`          | Yes       | Tables with external storage locations |
| `MATERIALIZED_VIEW` | Yes       | Materialized views                     |
| `VIEW`              | No        | Skipped with a debug log               |
| `STREAMING_TABLE`   | No        | Skipped with a debug log               |

Unsupported table types are:

- **Catalog connector path**: Silently skipped during catalog discovery with a debug-level log message.
- **Data connector path**: Rejected with an `UnsupportedTableType` error when a fully-qualified table reference (`catalog.schema.table`) is used and the table exists in Unity Catalog.

### Permission Checking

Before creating a table provider, the connector calls the UC Effective Permissions API (`GET /api/2.1/unity-catalog/effective-permissions/table/{full_name}`) to verify the current principal has `SELECT` or `ALL_PRIVILEGES` on the table.

- **Catalog connector path**: Tables without read permissions are skipped during discovery with an error-level log.
- **Data connector path**: Returns an `InsufficientPermissions` error when a fully-qualified table reference is used.
- **Graceful degradation**: If the UC API is unreachable or the table is not found in UC, the connector logs a warning and proceeds without validation (the table may not be a UC-managed table).

## Task History Instrumentation

All major Databricks operations are instrumented with `tracing` spans targeting `task_history`. This enables the Spice runtime task history system to track and record each operation.

### SQL Warehouse Spans

| Span Name                      | Input Field  | Description                                                     |
| ------------------------------ | ------------ | --------------------------------------------------------------- |
| `databricks_get_schema`        | Table name   | Schema inference via `information_schema.columns` or `DESCRIBE` |
| `databricks_execute_statement` | SQL text     | SQL statement execution via the Statements API                  |
| `databricks_poll_statement`    | Statement ID | Polling loop for async statement completion                     |

### Unity Catalog Spans

| Span Name                      | Input Field                | Description                             |
| ------------------------------ | -------------------------- | --------------------------------------- |
| `uc_get_table`                 | Fully-qualified table name | Fetch table metadata from Unity Catalog |
| `uc_get_catalog`               | Catalog ID                 | Fetch catalog metadata                  |
| `uc_list_schemas`              | Catalog ID                 | List schemas in a catalog               |
| `uc_list_tables`               | `catalog_id.schema_name`   | List tables in a schema                 |
| `uc_get_effective_permissions` | Fully-qualified table name | Check effective permissions for a table |

All spans include a `warehouse_id` field (SQL Warehouse spans) or the table/catalog identifier as the `input` field for correlation.

## Connector Metrics

### Component-Level Metrics

The SQL Warehouse connector exposes per-dataset operational metrics via the `MetricsProvider` interface. These metrics must be **explicitly enabled** in the dataset's `metrics` section in the spicepod to be registered.

#### Available Metrics

| Metric Name                    | Type    | Category        | Description                                                         |
| ------------------------------ | ------- | --------------- | ------------------------------------------------------------------- |
| `requests_total`               | Counter | Requests        | Total HTTP requests issued to the SQL Warehouse API (incl. retries) |
| `retries_total`                | Counter | Requests        | Total HTTP retries performed for transient failures                 |
| `permanent_errors_total`       | Counter | Requests        | Total non-retryable errors (401, 403, 404) detected                 |
| `inflight_requests`            | Gauge   | Requests        | Current number of in-flight HTTP requests                           |
| `statements_executed_total`    | Counter | Statements      | Total SQL statements submitted for execution                        |
| `statement_polls_total`        | Counter | Statements      | Total polls made when waiting for async statement completion        |
| `statements_failed_total`      | Counter | Statements      | Total SQL statements that completed with FAILED status              |
| `pool_connections_total`       | Counter | Connection Pool | Total virtual pool `connect()` calls                                |
| `pool_active_connections`      | Gauge   | Connection Pool | Current number of active connection handles                         |
| `semaphore_available_permits`  | Gauge   | Concurrency     | Current available permits in the request concurrency semaphore      |
| `chunks_fetched_total`         | Counter | Data Transfer   | Total Arrow result chunks fetched from external links               |
| `connector_disabled`           | Gauge   | Connector State | Whether the connector is permanently disabled (1 = yes, 0 = no)    |

#### Enabling Metrics

Add a `metrics` list to the dataset definition in your spicepod. Each entry names a metric from the table above.

```yaml
datasets:
  - from: databricks:my_catalog.my_schema.my_table
    name: my_table
    params:
      mode: sql_warehouse
      sql_warehouse_id: abc123def456
      endpoint: my-workspace.cloud.databricks.com
      databricks_client_id: ${env:DBX_CLIENT_ID}
      databricks_client_secret: ${env:DBX_CLIENT_SECRET}
    metrics:
      - name: requests_total
      - name: retries_total
      - name: permanent_errors_total
      - name: inflight_requests
      - name: statements_executed_total
      - name: statement_polls_total
      - name: statements_failed_total
      - name: pool_connections_total
      - name: pool_active_connections
      - name: semaphore_available_permits
      - name: chunks_fetched_total
      - name: connector_disabled
```

Individual metrics can be disabled by setting `enabled: false`:

```yaml
    metrics:
      - name: requests_total
      - name: inflight_requests
      - name: pool_active_connections
      - name: semaphore_available_permits
      - name: statement_polls_total
        enabled: false
```

#### Metric Naming and Attributes

Once registered, each metric is exposed as an OpenTelemetry instrument with the naming convention:

```
dataset_databricks_{metric_name}
```

For example, `requests_total` becomes `dataset_databricks_requests_total`.

Each instrument carries a `name` attribute set to the dataset instance name (e.g., `my_table`), so metrics from multiple datasets sharing the same SQL Warehouse can be distinguished.

#### Accessing Metrics

Registered component metrics are available through:

- **Prometheus endpoint** — Scraped from the `/metrics` HTTP endpoint when the metrics server is enabled.
- **`runtime.metrics` SQL table** — Queryable via SQL: `SELECT * FROM runtime.metrics WHERE name LIKE 'dataset_databricks_%'`.
- **OTLP push exporter** — Pushed to any configured OpenTelemetry collector.

### Global Inflight Requests Metric

Additionally, the shared `resilient_http` module emits a global `connector_inflight_requests` OpenTelemetry UpDownCounter with `service` and `operation` dimensions for cross-connector observability. This metric is always active (no opt-in required) and tracks in-flight HTTP requests across all connectors that use the shared retry/concurrency infrastructure.

## Architecture Notes

### Connection Pooling

The SQL Warehouse connector uses a **virtual connection pool** rather than a traditional socket-based pool. This design is appropriate for a stateless HTTP API:

- `SqlWarehouseConnectionPool` implements the `DbConnectionPool` trait but each `connect()` call returns a lightweight wrapper around a shared `Arc<SqlWarehouseApi>`.
- The shared `reqwest::Client` handles TCP connection pooling internally (idle timeout: 300s, keepalive: 60s, max idle per host: 16).
- Concurrency is controlled by a `tokio::Semaphore`, not by limiting pool size.
- Pool creation failure can only occur if the HTTP client builder fails. Connection acquisition (`connect()`) is effectively infallible.

### Token Management

- **M2M (Service Principal)**: A background task refreshes the OAuth2 token 5 minutes before expiry via a `watch` channel. Refresh failures use fibonacci backoff capped at 5 minutes.
- **U2M (User-to-Machine)**: Tokens are retrieved per-request from the request context.
- **Static Token**: Personal access tokens are used as-is with no refresh.
