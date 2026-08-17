# PostgreSQL Cayenne append refresh

This example exercises a PostgreSQL `timestamptz` source accelerated by Cayenne with append refreshes. PostgreSQL exposes `timestamptz` to Arrow as `Timestamp(ns, "UTC")`; Cayenne stores timestamps at microsecond precision.

Start the source with the fixed sample rows:

```shell
docker compose up -d
```

Start Spice from this directory:

```shell
spice run
```

The initial load contains three fixed rows. Query the runtime to confirm that the
stored timestamps are at Cayenne's supported microsecond precision:

```shell
spice sql --query 'SELECT id, event_timestamp, message, score FROM append_events ORDER BY id'
```

Stop and remove the source when finished:

```shell
docker compose down -v
```
