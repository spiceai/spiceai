# Debezium → Spice CDC ingest (no Kafka)

Push Debezium change events directly into Spice over HTTP. Supports **JSON** and **Avro**.

## Quick start

1. Start Spice with the sample spicepod:

```bash
spice run --path examples/cdc-debezium-ingest
```

2. Post a change event:

```bash
curl -sS -X POST "http://localhost:8090/v1/datasets/orders/cdc" \
  -H "Content-Type: application/json" \
  -d '{"before":null,"after":{"id":1,"customer_id":9,"amount":12.5},"op":"c","ts_ms":1,"source":{}}'
```

3. Query:

```bash
spice sql -- "SELECT * FROM orders"
```

## Debezium Server

See [`application.properties`](./application.properties) for a Debezium Server HTTP sink pointed at Spice. Use any Debezium source connector (Postgres, Oracle, SQL Server, …).

Full documentation: [`docs/features/cdc-debezium-ingest.md`](../../docs/features/cdc-debezium-ingest.md).
