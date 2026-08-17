# PostgreSQL Cayenne append refresh

This example exercises a PostgreSQL `timestamptz` source accelerated by Cayenne with append refreshes. PostgreSQL exposes `timestamptz` to Arrow as `Timestamp(ns, "UTC")`; Cayenne stores timestamps at microsecond precision.

Start the source and its continuous insert generator:

```shell
docker compose up -d
```

Start Spice from this directory:

```shell
spice run
```

The initial load contains the seeded rows. The generator inserts a row every five seconds, and the append refresh should load those rows without a timestamp schema mismatch.

Stop and remove the source when finished:

```shell
docker compose down -v
```
