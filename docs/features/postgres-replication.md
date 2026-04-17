# PostgreSQL Logical Replication (WAL Streaming)

Stream every `INSERT`, `UPDATE`, and `DELETE` from a PostgreSQL table directly into a Spice-accelerated dataset over Postgres' native logical replication protocol — no Debezium, no Kafka, no external services.

This is the recommended way to keep a Spice accelerator (DuckDB, SQLite, Postgres, Cayenne, Arrow) continuously in sync with a Postgres source.

## How it works

```
┌────────────────┐   WAL (pgoutput)   ┌───────────────────┐   ChangeBatch   ┌───────────────┐
│   PostgreSQL   │──────────────────▶│    Spice runtime  │────────────────▶│  Accelerator  │
│  wal_level=    │   replication     │  (postgres        │    (INSERT/     │  DuckDB /     │
│  logical       │   slot            │   connector)      │     UPDATE /    │  SQLite /     │
│                │                   │                   │     DELETE)     │  Postgres /   │
└────────────────┘                   └───────────────────┘                 │  Cayenne      │
                                                                           └───────────────┘
```

On first start the connector:

1. Creates a **publication** (default name `spice_<dataset>_pub`) containing the source table.
2. Creates a **replication slot** (default name `spice_<dataset>_<instance-hash>`).
3. Runs a **REPEATABLE READ snapshot** of the source table so the accelerator starts with all existing rows (`op = "c"`).
4. Starts streaming WAL changes from the slot. Each committed transaction is delivered as a `ChangeBatch` (grouped `INSERT`/`UPDATE`/`DELETE`) and applied to the accelerator.

On subsequent restarts the connector detects the existing slot and **resumes from Postgres' stored `confirmed_flush_lsn`** — no rebootstrap, no gap.

## Prerequisites

### 1. Enable logical replication on the source Postgres

This requires a server restart.

```
# postgresql.conf
wal_level = logical
max_replication_slots = 10   # at least one per Spice replica per dataset
max_wal_senders = 10
```

Verify:

```sql
SHOW wal_level;        -- must be 'logical'
SHOW max_replication_slots;
```

On managed Postgres services:

| Service              | How to enable                                                                       |
|----------------------|-------------------------------------------------------------------------------------|
| AWS RDS              | Set `rds.logical_replication = 1` in the parameter group and restart.               |
| Aurora PostgreSQL    | Set `rds.logical_replication = 1`; wait for DB reboot.                              |
| GCP Cloud SQL        | Flags: `cloudsql.logical_decoding = on`.                                            |
| Azure Database       | Under **Replication**, set *Replication support* to `LOGICAL`.                      |
| Supabase / Neon      | Logical replication is enabled by default.                                          |

### 2. The source table must have a replica identity

Spice needs the primary key columns in every `UPDATE`/`DELETE` event, so one of the following must be true:

- The table has a **primary key** (default — nothing to do).
- Or the table has `REPLICA IDENTITY FULL`:

  ```sql
  ALTER TABLE public.users REPLICA IDENTITY FULL;
  ```

Tables with `REPLICA IDENTITY NOTHING` are rejected at startup with an actionable error.

### 3. The Postgres role needs these privileges

```sql
GRANT CONNECT ON DATABASE mydb TO spice;
GRANT USAGE ON SCHEMA public TO spice;
GRANT SELECT ON public.users TO spice;
ALTER ROLE spice WITH REPLICATION;            -- or be a superuser
-- If you let Spice create the publication (default):
GRANT CREATE ON DATABASE mydb TO spice;
```

## Minimal configuration

```yaml
datasets:
  - from: postgres:public.users
    name: users
    params:
      pg_host: pg.internal
      pg_port: '5432'
      pg_user: spice
      pg_pass: ${secrets:pg_pass}
      pg_db: myapp
      pg_sslmode: disable        # TLS for the replication path is a follow-up;
                                 #   use a private network or a TLS proxy today.
    acceleration:
      enabled: true
      engine: duckdb           # or: sqlite | postgres | cayenne | arrow
      refresh_mode: changes    # <-- triggers WAL streaming
      primary_key: id
      on_conflict:
        id: upsert             # required for UPDATE to become an upsert
```

Start the runtime. Spice will:

- Auto-create publication `spice_users_pub`.
- Auto-create replication slot `spice_users_<instance-hash>`.
- Snapshot `public.users` into the DuckDB accelerator.
- Stream every subsequent change as it commits on Postgres.

## Full configuration reference

All replication-specific parameters live under `params:` on the dataset and start with `pg_`:

| Parameter                              | Default                             | Description |
|----------------------------------------|-------------------------------------|-------------|
| `pg_replication_slot`                  | `spice_<dataset>_<instance-hash>`   | Name of the replication slot. Must be unique per replica. |
| `pg_publication`                       | `spice_<dataset>_pub`               | Publication name. Shared across replicas. Auto-created if missing. |
| `pg_replication_initial_snapshot`      | `true`                              | If `true`, copy the table's existing rows before streaming. Set to `false` if you are pre-seeding the accelerator yourself. |
| `pg_replication_temporary_slot`        | `false`                             | If `true`, the slot is dropped when Spice disconnects. Every restart re-bootstraps. |
| `pg_replication_status_interval`       | `10s`                               | How often `StandbyStatusUpdate` (LSN acknowledgement) is sent back to Postgres. Lower values free WAL faster; higher values reduce network chatter. Accepts any [fundu](https://docs.rs/fundu) duration string (`500ms`, `30s`, `2m`). |

All existing `pg_host`, `pg_port`, `pg_user`, `pg_pass`, `pg_db`, `pg_sslmode`, `pg_connection_string`, etc. parameters continue to apply.

### Accelerator engines

| Engine        | `INSERT` | `UPDATE` | `DELETE` | Notes |
|---------------|:--------:|:--------:|:--------:|-------|
| `duckdb`      | ✅       | ✅ (upsert) | ✅       | Recommended for most workloads. |
| `sqlite`      | ✅       | ✅ (upsert) | ✅       | Great for small/medium datasets. |
| `postgres`    | ✅       | ✅ (upsert) | ✅       | Use when the accelerator is another Postgres. |
| `cayenne`     | ✅       | ✅ (upsert) | ✅       | S3-backed Vortex format, good for read-heavy analytics. |
| `arrow`       | ✅       | ❌ (becomes insert) | ❌ | Arrow's in-memory engine does not support `on_conflict`. Updates are appended as new rows; deletes are ignored. |

For anything other than append-only tables, use DuckDB, SQLite, Postgres, or Cayenne.

## Multi-replica deployments

Every Spice replica must have its own replication slot. Spice handles this automatically by hashing the replica's identity into the default slot name:

| Source                    | Used for |
|---------------------------|----------|
| `SPICE_INSTANCE_ID` env   | Preferred — set it explicitly per replica. |
| `HOSTNAME` / `COMPUTERNAME` | Fallback — works on k8s where each pod has a distinct hostname. |
| `/etc/hostname`           | Last resort. |

### Example: Kubernetes StatefulSet

Each pod automatically gets a stable, unique `HOSTNAME` (e.g. `spice-0`, `spice-1`), so no extra configuration is required:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: spice
spec:
  replicas: 3
  serviceName: spice
  template:
    spec:
      containers:
        - name: spice
          env:
            - name: SPICE_INSTANCE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name   # spice-0, spice-1, spice-2
```

Postgres will show:

```sql
SELECT slot_name, confirmed_flush_lsn
  FROM pg_replication_slots
 WHERE slot_name LIKE 'spice_users_%';

--  slot_name                | confirmed_flush_lsn
-- --------------------------+---------------------
--  spice_users_3a1f9c80     | 16/B374D848
--  spice_users_7b02e4d1     | 16/B374D848
--  spice_users_c4ea1190     | 16/B374D848
```

### Example: explicit slot names

If you'd rather control the names yourself (e.g. pinning one slot per pod ordinal):

```yaml
# Replica A
params:
  pg_replication_slot: spice_users_a

# Replica B
params:
  pg_replication_slot: spice_users_b
```

Each Spice replica can use a different `pg_replication_slot` while sharing a publication (`pg_publication`).

## Metrics

Spice emits OpenTelemetry observables for every replicated Postgres dataset. Metric names follow the standard Spice pattern `dataset_postgres_<metric>` with a single `name=<dataset>` attribute, so they work unchanged with the built-in Prometheus scrape endpoint and OTLP exporter.

### Core freshness signals

| Metric                                 | Type    | Unit  | Description |
|----------------------------------------|---------|-------|-------------|
| `dataset_postgres_replication_lag_ms`  | Gauge   | ms    | `now() − commit_time(latest ingested txn)`. Primary CDC freshness signal. Alert when this crosses your SLO (e.g. `> 5000`). |
| `dataset_postgres_replication_lag_bytes` | Gauge | bytes | `server_wal_end_lsn − confirmed_flush_lsn`. Indicates unacknowledged WAL still held by Spice's slot. Track alongside Postgres disk headroom. |
| `dataset_postgres_replication_confirmed_flush_lsn` | Gauge | — | Most recent LSN Spice has acknowledged. Matches `pg_replication_slots.confirmed_flush_lsn` for the dataset's slot. |
| `dataset_postgres_replication_server_wal_end_lsn` | Gauge | — | Latest WAL end LSN reported by the server via keepalive. Diff against `confirmed_flush_lsn` to reproduce `lag_bytes`. |

### Throughput counters

| Metric                                        | Type    | Description |
|-----------------------------------------------|---------|-------------|
| `dataset_postgres_replication_transactions_total` | Counter | Committed transactions applied. |
| `dataset_postgres_replication_inserts_total`  | Counter | `INSERT` rows from WAL. |
| `dataset_postgres_replication_updates_total`  | Counter | `UPDATE` rows from WAL. |
| `dataset_postgres_replication_deletes_total`  | Counter | `DELETE` rows from WAL. |
| `dataset_postgres_replication_truncates_total` | Counter | `TRUNCATE` operations received (currently skipped — see Limitations). Non-zero means source truncates happened without being reflected in the accelerator. |

### Bootstrap progress

| Metric                                        | Type    | Description |
|-----------------------------------------------|---------|-------------|
| `dataset_postgres_replication_bootstrap_rows_total` | Counter | Rows loaded during the initial `REPEATABLE READ` snapshot. |
| `dataset_postgres_replication_bootstrap_complete`   | Gauge   | `1` once bootstrap has finished (or was skipped on resume); `0` while snapshotting. Use as a readiness probe. |

### Errors and resilience

| Metric                                                        | Type    | Description |
|---------------------------------------------------------------|---------|-------------|
| `dataset_postgres_replication_decode_errors_total`            | Counter | pgoutput decoder errors. Non-zero usually means a Postgres version mismatch or a replication protocol bug — check logs. |
| `dataset_postgres_replication_schema_mismatch_errors_total`   | Counter | Source relation no longer matches the dataset's declared schema. The stream errors out; fix the schema and restart. |
| `dataset_postgres_replication_recv_errors_total`              | Counter | Transport-level errors receiving from the replication connection. Each one triggers a reconnect attempt. |
| `dataset_postgres_replication_reconnects_total`               | Counter | Number of times the stream has reconnected after a transient failure (network drop, Postgres restart, TLS reset). A non-zero value with no stream-level error means the connection wobbled and we recovered automatically. |

### Example Prometheus queries

```promql
# Dataset freshness SLI — alert when lag > 5s
max by (name) (dataset_postgres_replication_lag_ms) > 5000

# Change-apply throughput (events/sec over 1m)
sum by (name) (
  rate(dataset_postgres_replication_inserts_total[1m])
  + rate(dataset_postgres_replication_updates_total[1m])
  + rate(dataset_postgres_replication_deletes_total[1m])
)

# Error rate across all replicated datasets
sum by (name) (
  rate(dataset_postgres_replication_decode_errors_total[5m])
  + rate(dataset_postgres_replication_schema_mismatch_errors_total[5m])
  + rate(dataset_postgres_replication_recv_errors_total[5m])
)

# WAL backpressure — Spice lagging the server
max by (name) (dataset_postgres_replication_lag_bytes) > 100 * 1024 * 1024   # > 100 MiB
```

### Auto-registered vs opt-in

To keep the default metric cardinality reasonable, only operationally critical metrics auto-register. Everything else shows up once you explicitly enable it under the dataset's `metrics` block:

| Metric name                                       | Auto-registered |
|---------------------------------------------------|-----------------|
| `replication_lag_ms`                              | ✅              |
| `replication_lag_bytes`                           | ✅              |
| `replication_transactions_total`                  | ✅              |
| `replication_inserts_total` / `updates_total` / `deletes_total` | ✅ |
| `replication_bootstrap_complete`                  | ✅              |
| `replication_decode_errors_total`                 | ✅              |
| `replication_schema_mismatch_errors_total`        | ✅              |
| `replication_recv_errors_total`                   | ✅              |
| `replication_reconnects_total`                    | ✅              |
| `replication_confirmed_flush_lsn`                 | — enable manually |
| `replication_server_wal_end_lsn`                  | — enable manually |
| `replication_truncates_total`                     | — enable manually |
| `replication_bootstrap_rows_total`                | — enable manually |

Enable an opt-in metric on a dataset:

```yaml
datasets:
  - from: postgres:public.users
    name: users
    metrics:
      metrics:
        - name: replication_confirmed_flush_lsn
          enabled: true
```

## Operations

### Monitoring replication lag

```sql
SELECT
  slot_name,
  active,
  confirmed_flush_lsn,
  pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots
WHERE slot_name LIKE 'spice_%';
```

If `lag_bytes` keeps growing without a corresponding Spice workload, one of your replicas may be unhealthy — see below.

### Decommissioning a replica (important!)

A permanent replication slot **holds on to WAL** until it is dropped. If you retire a Spice replica without cleaning up its slot, Postgres will keep accumulating WAL indefinitely and can run out of disk.

After removing a Spice replica, drop its slot:

```sql
SELECT pg_drop_replication_slot('spice_users_<old-instance-hash>');
```

To find orphaned slots (slots that have been inactive for more than an hour). On PostgreSQL 14+:

```sql
SELECT slot_name
  FROM pg_replication_slots
 WHERE slot_name LIKE 'spice_%'
   AND NOT active
   AND inactive_since IS NOT NULL
   AND (NOW() - inactive_since) > INTERVAL '1 hour';
```

On PostgreSQL 13 or older, `pg_replication_slots` has no inactivity timestamp — use `pg_stat_replication` or your monitoring system instead.

### Rebooting a replica

Nothing special required. Spice rejoins its existing slot and resumes from the last acknowledged LSN. The accelerator catches up automatically.

### Resilience

The replication stream is designed to survive transient failures without operator intervention:

- **Network blips / TCP resets / Postgres restarts**: classified as transient and retried with exponential backoff (500 ms → 30 s, ±20 % jitter). The slot's server-side state is the source of truth, so reconnects resume from the last acknowledged LSN — no data loss.
- **Auth failures, slot missing, schema mismatch, permission denied**: classified as fatal and surfaced as a stream-level error so you can fix the configuration. These are not retried.
- **Setup / bootstrap phase**: transient errors during initial slot setup or snapshot bootstrap are retried for up to 2 minutes before giving up.
- **Postgres SQLSTATE classes 08xxx (connection exception) and 57P0x (admin shutdown, cannot-connect-now) are retried**; other server-side errors (23xxx constraint, 42xxx syntax/permission) are not.
- **Watch `dataset_postgres_replication_reconnects_total`** to detect flaky networks — the stream may be healthy end-to-end while continuously reconnecting under the hood.
- **No thundering herd across replicas**: the ±20 % jitter means N replicas reconnecting after a common outage don't synchronise their retry attempts.

### Rebuilding an accelerator from scratch

Delete the accelerator's local storage (DuckDB file, SQLite file, etc.) and drop the replication slot:

```sql
SELECT pg_drop_replication_slot('spice_users_<instance-hash>');
```

On next start, Spice will create a fresh slot, snapshot the table, and resume streaming.

### Changing the source schema

pgoutput re-emits a `Relation` message whenever the source table's schema changes. Spice validates it against the declared accelerator schema on every change; an incompatible change (e.g. a required column disappears) becomes a fatal error for that dataset. The recommended workflow:

1. Add new columns as nullable on the source first.
2. Update the Spice dataset to include the new column.
3. Reload the Spicepod.

Dropping or renaming columns in use by Spice will require rebuilding the accelerator.

## Troubleshooting

| Symptom                                                                                     | Cause and fix |
|---------------------------------------------------------------------------------------------|---------------|
| Error: *`Table public.X has REPLICA IDENTITY NOTHING`*                                      | Run `ALTER TABLE public.X REPLICA IDENTITY FULL;` (or add a primary key). |
| Error: *`Table public.X has no primary key and no REPLICA IDENTITY FULL`*                   | Either add a primary key, or run `ALTER TABLE ... REPLICA IDENTITY FULL;`. |
| Error: *`Source table public.X does not exist`*                                             | The fully qualified table in `from: postgres:<schema>.<table>` is wrong or the role lacks SELECT. |
| Error: *`replication slot "..." already exists`* on startup                                 | Another Spice replica is using the same slot name. Set `pg_replication_slot` uniquely, or ensure `SPICE_INSTANCE_ID` differs between replicas. |
| Error mentioning *permission denied for database* during setup                              | The role needs `CREATE` on the database, or you need to pre-create the publication/slot yourself. |
| `pg_replication_slots.active` is `true` but the accelerator isn't updating                  | Check the Spice logs for schema-mismatch errors. The replication task will still hold the slot even after a failure — restart Spice after fixing the schema to advance. |
| `wal` on the source disk growing forever                                                    | An abandoned slot. Drop it with `pg_drop_replication_slot`. |
| `UPDATE`s on Arrow-engine dataset don't replace rows                                        | Arrow does not support `on_conflict`. Switch to `duckdb`, `sqlite`, `postgres`, or `cayenne`. |
| Huge `TEXT`/`JSONB` columns show as `NULL` after `UPDATE`                                   | Unchanged TOASTed columns are omitted by pgoutput. Run `ALTER TABLE ... REPLICA IDENTITY FULL;` if you need them in every event. |
| Logged *`TRUNCATE received... skipping`*                                                    | `TRUNCATE` replication is not yet implemented. Either avoid truncating the source or rebuild the accelerator (see above). |

## Limitations (current release)

- **One table per dataset.** Each Spice dataset replicates exactly one source table; each dataset gets its own slot and publication.
- **No DDL replication.** Schema changes on the source are not propagated automatically. See *Changing the source schema* above.
- **TRUNCATE is not applied** to the accelerator — it is logged as a warning and skipped.
- **No TLS to the replication port** in this initial release. Setting `pg_sslmode` to anything other than `disable` is rejected at startup with an actionable error. Use a private network or a TLS-terminating proxy today. (The underlying client supports TLS; exposing it as a parameter is on the roadmap.)
- **Arrow engine** does not support upsert or delete — `UPDATE`s appear as duplicate inserts and `DELETE`s are silently dropped.

## Comparison with Debezium + Kafka

| Aspect                   | Debezium + Kafka                                | Native WAL streaming (this feature) |
|--------------------------|-------------------------------------------------|--------------------------------------|
| External services        | Kafka + Schema Registry + Debezium + Connect    | None — Spice connects to Postgres directly |
| Deployment footprint     | JVM stack + ZooKeeper/KRaft                     | Zero extra pods                       |
| Setup complexity         | Multiple topics, connector configs, ACLs        | One connector config                  |
| Operational model        | Consumer groups, topic retention                | One replication slot per replica     |
| Schema registry required | Yes (Avro/Protobuf)                             | No — schema derived from Postgres catalog + Spice dataset |
| Latency                  | Kafka-bound (~100ms+)                           | Commit-driven, typically <100ms       |

If you are already running Kafka for other reasons, the Debezium path still works via the existing `kafka` / `debezium` connectors. For greenfield Postgres → Spice CDC, prefer this feature.

## See also

- [PostgreSQL: Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [`pgwire-replication` crate](https://crates.io/crates/pgwire-replication) — the Rust client Spice uses internally
- Spice [`refresh_mode`](../../crates/spicepod/src/acceleration/mod.rs) reference
