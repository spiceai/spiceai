# Debezium CDC Ingest (push, no Kafka)

Spice supports a **dual-path** CDC model:

| Path | When to use |
|------|-------------|
| **Native CDC** | Postgres WAL, MySQL binlog, MongoDB change streams, DynamoDB Streams — Spice captures directly from the database |
| **Debezium push ingest** | Any database with a Debezium source plugin — the plugin streams change events **directly into Spice** (JSON or Avro). **No Kafka required** |

This document covers the second path: `from: cdc:…` + `POST /v1/datasets/{name}/cdc`.

## Architecture

```
  OLTP (Oracle, SQL Server, Postgres, …)
           │
           ▼
  Debezium source plugin
  (Server / Embedded Engine)
           │  HTTP POST  (JSON or Avro)
           ▼
  spiced  POST /v1/datasets/{name}/cdc
           │  decode → ChangeBatch
           ▼
  accelerator (Cayenne / DuckDB / Arrow / …)
```

Kafka is optional. If you already run Debezium → Kafka, continue using `from: debezium:…`. For greenfield multi-source capture without a bus, use this push path.

## Spicepod configuration

```yaml
datasets:
  - from: cdc:orders
    name: orders
    columns:
      - name: id
        type: int64
      - name: customer_id
        type: int64
      - name: amount
        type: float64
    acceleration:
      enabled: true
      engine: cayenne   # or duckdb, arrow, …
      mode: file
      refresh_mode: changes
      primary_key: id
      on_conflict:
        id: upsert
    params:
      # Avro only — Confluent Schema Registry base URL
      # schema_registry_url: https://schema-registry:8081
      # Or embed the Avro schema for raw (non-Confluent) bodies:
      # avro_schema: |
      #   { "type": "record", "name": "Envelope", ... }
```

### Requirements

- `acceleration.enabled: true`
- `refresh_mode: changes`
- Declared `columns` with types (schema cannot be peeked from a bus)
- `primary_key` + `on_conflict` upsert (except Arrow append-only)

## HTTP API

```http
POST /v1/datasets/{name}/cdc
Content-Type: application/json
# or application/vnd.debezium+json
# or application/avro / application/vnd.debezium+avro
```

### JSON body shapes

- Single change event (schemaless or with embedded `schema`)
- JSON array of events
- Newline-delimited JSON (NDJSON)

Example create:

```json
{
  "before": null,
  "after": { "id": 1, "customer_id": 9, "amount": 12.5 },
  "source": { "connector": "oracle", "ts_ms": 1710000000000 },
  "op": "c",
  "ts_ms": 1710000000000
}
```

`op` values: `c` create, `u` update, `d` delete, `r` snapshot read, `t` truncate. Internal `m` messages are skipped.

### Avro

| Mode | How |
|------|-----|
| Confluent wire format | Body starts with magic `0` + 4-byte schema id; set `schema_registry_url` on the dataset |
| Raw Avro | Body is a single Avro datum; set `avro_schema` param or `X-Avro-Schema` header |

### Response

```json
{ "applied": 1, "dataset": "orders" }
```

The request **blocks until the batch is applied** (ack). Debezium Server / Embedded sinks should commit source offsets only after HTTP 200.

| Status | Meaning |
|--------|---------|
| 200 | Applied |
| 400 | Bad body / decode / apply failure |
| 404 | Dataset not registered for CDC ingest (not ready or wrong `from:`) |
| 403 | Write access required (auth) |
| 503 | Change stream stopped (dataset unloaded or reloading) — retry once ready |
| 504 | Timed out waiting for capacity (backpressure) or for the change to apply |

## Debezium Server sink

Use Debezium Server (or Embedded) with an HTTP sink pointed at Spice. Example `application.properties`:

```properties
debezium.sink.type=http
debezium.sink.http.url=http://spiced:8090/v1/datasets/orders/cdc
debezium.sink.http.headers.Content-Type=application/json

# Source — any Debezium connector
debezium.source.connector.class=io.debezium.connector.oracle.OracleConnector
debezium.source.database.hostname=oracle
# ... connector-specific settings ...
```

For Avro, set the serializer to Avro and either configure Schema Registry on the Spice dataset or convert to JSON in the sink.

A ready-to-copy config lives under [`examples/cdc-debezium-ingest/`](../../examples/cdc-debezium-ingest/).

## Native vs push

| | Native (`postgres` / `mysql` / `mongodb`) | Push (`cdc`) |
|--|------------------------------------------|--------------|
| Capture | Inside `spiced` | Debezium plugin process |
| Kafka | No | No |
| Formats | Internal | JSON + Avro |
| Sources | Those Spice implements natively | Any Debezium source plugin |

Prefer native when available. Use push for Oracle, SQL Server, Db2, and other engines where Debezium already has a connector.

## Feature flag

Requires the `debezium` feature on `spiced` (enabled in default builds).
