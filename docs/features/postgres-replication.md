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

1. Creates a **publication** (default name `spice_<dataset>_<hash>_pub`) containing the source table. The short hash disambiguates long dataset names that would otherwise truncate to the same identifier.
2. Creates a **replication slot** (default name `spice_<dataset>_<dataset-hash>_<instance-hash>`). The `<dataset-hash>` disambiguates dataset names that truncate to the same prefix; the `<instance-hash>` gives each Spice replica its own slot. These are the names operators will see in `pg_replication_slots` when inspecting or decommissioning replicas.
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

| Service           | How to enable                                                         |
| ----------------- | --------------------------------------------------------------------- |
| AWS RDS           | Set `rds.logical_replication = 1` in the parameter group and restart. |
| Aurora PostgreSQL | Set `rds.logical_replication = 1`; wait for DB reboot.                |
| GCP Cloud SQL     | Flags: `cloudsql.logical_decoding = on`.                              |
| Azure Database    | Under **Replication**, set *Replication support* to `LOGICAL`.        |
| Supabase / Neon   | Logical replication is enabled by default.                            |

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
      pg_sslmode: verify-full      # or: disable | prefer | require | verify-ca
      pg_sslrootcert: /etc/ssl/pg-ca.pem   # optional; omit to use system root CAs
    acceleration:
      enabled: true
      engine: duckdb           # or: sqlite | postgres | cayenne | arrow
      refresh_mode: changes    # <-- triggers WAL streaming
      primary_key: id
      on_conflict:
        id: upsert             # required for UPDATE to become an upsert
```

Start the runtime. Spice will:

- Auto-create publication `spice_users_<dataset-hash>_pub`.
- Auto-create replication slot `spice_users_<dataset-hash>_<instance-hash>`.
- Snapshot `public.users` into the DuckDB accelerator.
- Stream every subsequent change as it commits on Postgres.

## Full configuration reference

All replication-specific parameters live under `params:` on the dataset and start with `pg_`:

| Parameter                             | Default                                          | Description                                                                                                                                                                                                                            |
| ------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pg_replication_slot`                 | `spice_<dataset>_<dataset-hash>_<instance-hash>` | Name of the replication slot. Must be unique per replica. The dataset-hash protects against truncation collisions between long dataset names. Datasets on the same connection that name the **same** slot share it — see [Sharing one slot across multiple datasets](#sharing-one-slot-across-multiple-datasets). |
| `pg_publication`                      | `spice_<dataset>_<dataset-hash>_pub`             | Publication name. Shared across replicas. Auto-created if missing. The short hash disambiguates datasets whose names share a long truncated prefix. When `pg_replication_slot` is set explicitly, the default becomes `<slot>_pub` so datasets sharing a slot land on the same publication.                       |
| `pg_replication_initial_snapshot`     | `true`                                           | If `true`, take an initial snapshot of the table's existing rows before streaming. Set to `false` if you are pre-seeding the accelerator yourself. Non-persistent accelerators (`arrow`, or `duckdb`/`sqlite` with `mode: memory`/`file_create`) snapshot on **every** start — including slot resume — since they boot empty.            |
| `pg_replication_temporary_slot`       | `false`                                          | If `true`, the slot is dropped when Spice disconnects. Every restart re-bootstraps.                                                                                                                                                    |
| `pg_replication_status_interval`      | `10s`                                            | How often `StandbyStatusUpdate` (LSN acknowledgement) is sent back to Postgres. Lower values free WAL faster; higher values reduce network chatter. Accepts any [fundu](https://docs.rs/fundu) duration string (`500ms`, `30s`, `2m`). |
| `pg_replication_bootstrap_batch_size` | `8192`                                           | Rows per batch emitted by the initial snapshot stream. Increase for large tables to reduce per-batch write/planning overhead; decrease to reduce peak memory. Maximum: `1048576`.                                                      |

All existing `pg_host`, `pg_port`, `pg_user`, `pg_pass`, `pg_db`, `pg_sslmode`, `pg_connection_string`, etc. parameters continue to apply.

Connection footprint: a `refresh_mode: changes` dataset uses its regular connection pool only for schema probes at initialization — replication itself runs over dedicated connections (setup, snapshot, and the WAL stream). The pool therefore defaults to `pg_connection_pool_size: 2` with `pg_connection_pool_min_idle: 0` for changes-mode datasets, so N CDC datasets hold no idle pool connections at steady state. Set either parameter explicitly to override.

### Runtime CDC apply tuning

For high-throughput catch-up workloads, the runtime CDC apply loop can be tuned under `runtime.params`:

```yaml
runtime:
  params:
    cdc_prefetch_buffer: '128'
    cdc_max_coalesced_envelopes: '256'
    cdc_max_coalesced_bytes: '134217728'
    cdc_max_coalesce_age_ms: '0'
    cdc_commit_timeout_ms: '30000'
```

  `cdc_prefetch_buffer` controls decoded envelope buffering between the source reader and accelerator writer. `cdc_max_coalesced_envelopes` and `cdc_max_coalesced_bytes` control how many envelopes are merged into one accelerator write. Larger values improve catch-up throughput by amortizing planning and write overhead, but increase peak memory. `cdc_max_coalesce_age_ms` controls the runtime CDC coalesce age used by accelerators that apply age-based maintenance; `0` keeps the accelerator default.

For standalone analytical query benchmarks, `runtime.query.target_partitions` can be set to control DataFusion's local query parallelism:

```yaml
runtime:
  query:
    target_partitions: 64
```

Set this near the number of CPU cores or expected scan partitions, then verify with `EXPLAIN ANALYZE`. In cluster mode, Spice sets target partitions dynamically from executor slots.

#### `pg_sslmode` for WAL streaming

The table below reflects how each `pg_sslmode` value behaves for the replication stream. `verify-full` is the recommended production default.

| `pg_sslmode`       | Replication transport | Cert chain verified | Hostname verified |
| ------------------ | --------------------- | :-----------------: | :---------------: |
| `disable`          | plaintext             |          —          |         —         |
| `prefer` (default) | plaintext             |          —          |         —         |
| `require`          | TLS                   |          ❌          |         ❌         |
| `verify-ca`        | TLS                   |          ✅          |         ❌         |
| `verify-full`      | TLS                   |          ✅          |         ✅         |

Note: `prefer` behaves as plaintext here because the replication transport does not expose a safe "try TLS, fall back to plaintext" path. Set `require`, `verify-ca`, or `verify-full` to force TLS on the WAL stream. A `tracing::warn!` is emitted at startup whenever a non-verifying mode is in effect.

### Accelerator engines

| Engine     | `INSERT` |          `UPDATE`           | `DELETE` | Notes                                                                                                                                                                                                 |
| ---------- | :------: | :-------------------------: | :------: | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `duckdb`   |    ✅     |         ✅ (upsert)          |    ✅     | Recommended for most workloads.                                                                                                                                                                       |
| `sqlite`   |    ✅     |         ✅ (upsert)          |    ✅     | Great for small/medium datasets.                                                                                                                                                                      |
| `postgres` |    ✅     |         ✅ (upsert)          |    ✅     | Use when the accelerator is another Postgres.                                                                                                                                                         |
| `cayenne`  |    ✅     |         ✅ (upsert)          |    ✅     | S3-backed Vortex format, good for read-heavy analytics.                                                                                                                                               |
| `arrow`    |    ✅     | ✅ (upsert with primary key) |    ✅     | Arrow's in-memory engine uses a hash index for primary-key upserts. Without a primary key, updates are appended as new rows. `DELETE` and `TRUNCATE` are applied via Arrow's `DeletionTableProvider`. |

For Arrow workloads that need true upsert semantics (so `UPDATE`s replace existing rows instead of duplicating them), configure a primary key. DuckDB, SQLite, Postgres, and Cayenne also support upsert behavior.

## Sharing one slot across multiple datasets

Each logical replication slot runs its own walsender + decoder over the **entire** WAL stream on the source server (the publication filter applies after decoding), and each slot independently pins WAL retention. Postgres also defaults to `max_replication_slots = 10` (restart required to raise it). A spiced instance that mirrors several small tables from one database therefore should not burn one slot per table.

To share, give the datasets the same explicit `pg_replication_slot` (and the same connection parameters):

```yaml
datasets:
  - from: postgres:public.users
    name: users
    params: &repl
      pg_host: db.internal
      pg_db: app
      pg_user: spice
      pg_pass: ${secrets:pg_pass}
      pg_replication_slot: spice_app_cdc
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: changes
      primary_key: id
      on_conflict:
        id: upsert

  - from: postgres:public.orders
    name: orders
    params: *repl
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: changes
      primary_key: id
      on_conflict:
        id: upsert
```

Spice runs **one replication connection, one slot, and one publication** (`spice_app_cdc_pub`, covering both tables) and routes decoded changes by `(schema, table)` to each dataset's accelerator. A slot named by only one dataset behaves exactly like a dedicated slot. Datasets that don't set `pg_replication_slot` keep their own per-dataset slot as before.

Sharing semantics worth knowing:

- **Same consistency model as dedicated slots.** At-least-once across the snapshot/WAL boundary, made convergent by the required `primary_key` + `on_conflict: upsert`. Each member table is snapshotted when the slot is first created, or when the table is later added to the publication (e.g. a dataset added via Spicepod reload — the publication is extended with `ALTER PUBLICATION ... ADD TABLE` and just that table is snapshotted).
- **Acknowledgement is collective.** `confirmed_flush_lsn` is per-slot, so Spice acknowledges the *minimum* LSN durably applied across all member datasets. A dataset whose accelerator stalls or whose stream fails **pins WAL retention for the whole slot** — deliberately, because acking past it would permanently lose its changes. This is visible as a growing `dataset_postgres_replication_lag_bytes` and a WARN log naming the detached dataset; restarting spiced (or the dataset rejoining) replays from the held LSN and every member re-applies the overlap idempotently.
- **One dataset per source table per slot.** Two datasets replicating the same table must use different slots.
- **All members must use the same publication.** Leave `pg_publication` unset (the `<slot>_pub` default agrees automatically) or set it identically on every member.
- **Removing a dataset** stops routing its changes but does not remove its table from the publication, and its last-applied LSN keeps pinning WAL until spiced restarts. After a restart the remaining members resume and the slot acknowledges freely again. If you later **re-add a previously removed dataset**, drop its table from the publication first (`ALTER PUBLICATION spice_app_cdc_pub DROP TABLE public.users;`) so Spice re-adds it and takes a fresh snapshot — changes that committed while the dataset was absent across a restart are not replayable from the slot.
- **Replicas still need distinct slots.** Slots are single-consumer: sharing happens *within* a spiced instance, never across replicas. With explicit slot names, include the replica identity in the name (see [Multi-replica deployments](#multi-replica-deployments)); a second consumer of the same slot fails loudly with "replication slot is active".

## Multi-replica deployments

Every Spice replica must have its own replication slot. Spice handles this automatically by hashing the replica's identity into the default slot name:

| Source                          | Used for                                                        |
| ------------------------------- | --------------------------------------------------------------- |
| `SPICE_INSTANCE_ID` env         | Preferred — set it explicitly per replica.                      |
| `HOSTNAME` / `COMPUTERNAME` env | Fallback — works on k8s where each pod has a distinct hostname. |

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

--  slot_name                       | confirmed_flush_lsn
-- ---------------------------------+---------------------
--  spice_users_7a9c1b_3a1f9c80     | 16/B374D848
--  spice_users_7a9c1b_7b02e4d1     | 16/B374D848
--  spice_users_7a9c1b_c4ea1190     | 16/B374D848
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

Spice emits OpenTelemetry observables for every replicated Postgres dataset. Metric names follow the pattern `dataset_postgres_replication_<metric>` with a single `name=<dataset>` attribute, so they work unchanged with the built-in Prometheus scrape endpoint and OTLP exporter.

### Core freshness signals

| Metric                                             | Type  | Unit  | Description                                                                                                                                  |
| -------------------------------------------------- | ----- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `dataset_postgres_replication_lag_ms`              | Gauge | ms    | `now() − commit_time(latest ingested txn)`. Primary CDC freshness signal. Alert when this crosses your SLO (e.g. `> 5000`).                  |
| `dataset_postgres_replication_lag_bytes`           | Gauge | bytes | `server_wal_end_lsn − confirmed_flush_lsn`. Indicates unacknowledged WAL still held by Spice's slot. Track alongside Postgres disk headroom. |
| `dataset_postgres_replication_confirmed_flush_lsn` | Gauge | —     | Most recent LSN Spice has acknowledged. Matches `pg_replication_slots.confirmed_flush_lsn` for the dataset's slot.                           |
| `dataset_postgres_replication_server_wal_end_lsn`  | Gauge | —     | Latest WAL end LSN reported by the server via keepalive. Diff against `confirmed_flush_lsn` to reproduce `lag_bytes`.                        |

### Throughput counters

| Metric                                            | Type    | Description                                                                       |
| ------------------------------------------------- | ------- | --------------------------------------------------------------------------------- |
| `dataset_postgres_replication_transactions_total` | Counter | Committed transactions applied.                                                   |
| `dataset_postgres_replication_inserts_total`      | Counter | `INSERT` rows from WAL.                                                           |
| `dataset_postgres_replication_updates_total`      | Counter | `UPDATE` rows from WAL.                                                           |
| `dataset_postgres_replication_deletes_total`      | Counter | `DELETE` rows from WAL.                                                           |
| `dataset_postgres_replication_truncates_total`    | Counter | `TRUNCATE` operations received and applied to the accelerator (all rows deleted). |

### Bootstrap progress

| Metric                                              | Type    | Description                                                                                                   |
| --------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------- |
| `dataset_postgres_replication_bootstrap_rows_total` | Counter | Rows loaded during the initial `REPEATABLE READ` snapshot.                                                    |
| `dataset_postgres_replication_bootstrap_complete`   | Gauge   | `1` once bootstrap has finished (or was skipped on resume); `0` while snapshotting. Use as a readiness probe. |

### Errors and resilience

| Metric                                                      | Type    | Description                                                                                                                                                                                                                |
| ----------------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dataset_postgres_replication_decode_errors_total`          | Counter | pgoutput decoder errors. Non-zero usually means a Postgres version mismatch or a replication protocol bug — check logs.                                                                                                    |
| `dataset_postgres_replication_schema_mismatch_errors_total` | Counter | Source relation no longer matches the dataset's declared schema. The stream errors out; fix the schema and restart.                                                                                                        |
| `dataset_postgres_replication_recv_errors_total`            | Counter | Transport-level errors receiving from the replication connection. Each one triggers a reconnect attempt.                                                                                                                   |
| `dataset_postgres_replication_reconnects_total`             | Counter | Number of times the stream has reconnected after a transient failure (network drop, Postgres restart, TLS reset). A non-zero value with no stream-level error means the connection wobbled and we recovered automatically. |

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

| Metric name                                                     | Auto-registered   |
| --------------------------------------------------------------- | ----------------- |
| `replication_lag_ms`                                            | ✅                 |
| `replication_lag_bytes`                                         | ✅                 |
| `replication_transactions_total`                                | ✅                 |
| `replication_inserts_total` / `updates_total` / `deletes_total` | ✅                 |
| `replication_bootstrap_complete`                                | ✅                 |
| `replication_decode_errors_total`                               | ✅                 |
| `replication_schema_mismatch_errors_total`                      | ✅                 |
| `replication_recv_errors_total`                                 | ✅                 |
| `replication_reconnects_total`                                  | ✅                 |
| `replication_confirmed_flush_lsn`                               | — enable manually |
| `replication_server_wal_end_lsn`                                | — enable manually |
| `replication_truncates_total`                                   | — enable manually |
| `replication_bootstrap_rows_total`                              | — enable manually |

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

This is only necessary for **persistent** accelerators (`mode: file`, or external engines like `postgres`/`cayenne`), where deleting the storage desynchronizes it from the slot's checkpoint. Non-persistent accelerators (`arrow`, or `duckdb`/`sqlite` with `mode: memory`/`file_create`) start empty on every boot, so Spice automatically re-runs the initial snapshot on each start — resuming WAL from the existing slot afterwards — and no manual slot intervention is needed.

### Changing the source schema

pgoutput re-emits a `Relation` message whenever the source table's schema changes. Spice validates it against the declared accelerator schema on every change; an incompatible change (e.g. a required column disappears) becomes a fatal error for that dataset. The recommended workflow:

1. Add new columns as nullable on the source first.
2. Update the Spice dataset to include the new column.
3. Reload the Spicepod.

Dropping or renaming columns in use by Spice will require rebuilding the accelerator.

## Troubleshooting

| Symptom                                                                    | Cause and fix                                                                                                                                                           |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Error: *`Table public.X has REPLICA IDENTITY NOTHING`*                     | Run `ALTER TABLE public.X REPLICA IDENTITY FULL;` (or add a primary key).                                                                                               |
| Error: *`Table public.X has no primary key and no REPLICA IDENTITY FULL`*  | Either add a primary key, or run `ALTER TABLE ... REPLICA IDENTITY FULL;`.                                                                                              |
| Error: *`Source table public.X does not exist`*                            | The fully qualified table in `from: postgres:<schema>.<table>` is wrong or the role lacks SELECT.                                                                       |
| Error: *`replication slot "..." already exists`* on startup                | Another Spice replica is using the same slot name. Set `pg_replication_slot` uniquely, or ensure `SPICE_INSTANCE_ID` differs between replicas.                          |
| Error mentioning *permission denied for database* during setup             | The role needs `CREATE` on the database, or you need to pre-create the publication/slot yourself.                                                                       |
| `pg_replication_slots.active` is `true` but the accelerator isn't updating | Check the Spice logs for schema-mismatch errors. The replication task will still hold the slot even after a failure — restart Spice after fixing the schema to advance. |
| `wal` on the source disk growing forever                                   | An abandoned slot — drop it with `pg_drop_replication_slot`. On a shared slot, also check the logs for a "shared replication member detached" WARN: one failed/removed dataset holds the slot's `confirmed_flush_lsn` until spiced restarts.             |
| `UPDATE`s on Arrow-engine dataset don't replace rows                       | Configure a `primary_key` so Arrow can use its hash index for upserts, or switch to `duckdb`, `sqlite`, `postgres`, or `cayenne`.                                       |
| Huge `TEXT`/`JSONB` columns show as `NULL` after `UPDATE`                  | Unchanged TOASTed columns are omitted by pgoutput. Run `ALTER TABLE ... REPLICA IDENTITY FULL;` if you need them in every event.                                        |
| Logged *`TRUNCATE from postgres replication queued for accelerator`*       | Informational. A source `TRUNCATE` is being applied — the accelerated table will be emptied.                                                                            |

## Limitations (current release)

- **One table per dataset.** Each Spice dataset replicates exactly one source table. By default each dataset gets its own slot and publication; datasets that name the same `pg_replication_slot` share one slot, publication, and replication connection (see [Sharing one slot across multiple datasets](#sharing-one-slot-across-multiple-datasets)).
- **No DDL replication.** Schema changes on the source are not propagated automatically. See *Changing the source schema* above.
- **Multidimensional arrays are not supported.** Single-level arrays (`text[]`, `int4[]`, ...), enums, `uuid`, and `json`/`jsonb` columns replicate fine; nested arrays (`text[][]`) must be cast to a scalar on the source or excluded from the dataset.
- **Arrow engine** supports `on_conflict` upserts when a primary key is configured. Without a primary key, `UPDATE`s appear as additional inserts rather than replacing existing rows. `DELETE` and `TRUNCATE` are applied.

## Comparison with Debezium + Kafka

| Aspect                   | Debezium + Kafka                             | Native WAL streaming (this feature)                       |
| ------------------------ | -------------------------------------------- | --------------------------------------------------------- |
| External services        | Kafka + Schema Registry + Debezium + Connect | None — Spice connects to Postgres directly                |
| Deployment footprint     | JVM stack + ZooKeeper/KRaft                  | Zero extra pods                                           |
| Setup complexity         | Multiple topics, connector configs, ACLs     | One connector config                                      |
| Operational model        | Consumer groups, topic retention             | One replication slot per replica                          |
| Schema registry required | Yes (Avro/Protobuf)                          | No — schema derived from Postgres catalog + Spice dataset |
| Latency                  | Kafka-bound (~100ms+)                        | Commit-driven, typically <100ms                           |

If you are already running Kafka for other reasons, the Debezium path still works via the existing `kafka` / `debezium` connectors. For greenfield Postgres → Spice CDC, prefer this feature.

## See also

- [PostgreSQL: Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [`pgwire-replication` crate](https://crates.io/crates/pgwire-replication) — the Rust client Spice uses internally
- Spice [`refresh_mode`](../../crates/spicepod/src/acceleration/mod.rs) reference
