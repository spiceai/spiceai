# MySQL Binlog Replication (`refresh_mode: changes`)

Spice can subscribe to a MySQL database's binary log and stream row changes
directly into a local accelerator — the MySQL analog of
[PostgreSQL logical replication](./postgres-replication.md). No Kafka, no
Debezium, no external CDC infrastructure: one `spicepod.yml` entry gives you a
locally materialized, continuously updated copy of a MySQL table.

```
┌────────────┐   binlog (ROW events)   ┌──────────────────────────────┐
│   MySQL    │ ──────────────────────► │            spiced            │
│  (source)  │                         │  decode → ChangeBatch →      │
│            │                         │  accelerator upsert/delete   │
└────────────┘                         └──────────────────────────────┘
       ▲                                        │
       └── resume position persisted in the ────┘
           accelerator (spice_sys_mysql_binlog)
```

## Minimal configuration

```yaml
datasets:
  - from: mysql:mydb.orders
    name: orders
    params:
      mysql_host: db.internal
      mysql_tcp_port: "3306"
      mysql_user: replicator
      mysql_pass: ${secrets:mysql_pass}
      mysql_db: mydb
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: changes
      primary_key: id
      on_conflict:
        id: upsert
```

`primary_key` + `on_conflict: upsert` are **required** (except on the
append-only `arrow` engine): UPDATE events apply as upserts keyed on the
primary key and DELETE events are routed by it. The connector fails fast at
startup with an actionable message if either is missing.

All upsert-capable accelerator engines are supported — `duckdb`, `sqlite`,
`cayenne`, `postgres`, and `turso` — and each persists the binlog resume
position in its `spice_sys_mysql_binlog` sidecar when file-backed. The
`arrow` engine works append-only (UPDATEs insert new rows).

## Source prerequisites

| Setting | Required value | Notes |
| --- | --- | --- |
| `log_bin` | `ON` | Default on MySQL 8.0+. |
| `binlog_format` | `ROW` | Default on MySQL 8.0+. Validated at startup. |
| `binlog_row_image` | `FULL` | Default. Validated at startup; `MINIMAL` images are rejected. |
| `binlog_row_value_options` | `''` (empty) | Validated at startup; partial JSON row images cannot be applied. |
| `binlog_expire_logs_seconds` | ≥ longest expected spiced downtime | See [position recovery](#when-the-position-is-purged). |

The connecting user needs:

```sql
GRANT SELECT ON mydb.orders TO 'replicator'@'%';           -- snapshot + layout discovery
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
```

## How it works

1. **Validate + discover.** At startup Spice validates the server settings
   above and reads the table's column layout from `information_schema`
   (binlog row events are positional — they carry no column names).
2. **Capture the head position.** The current binlog file + offset is
   captured *before* the snapshot, so changes racing the snapshot are
   delivered at least once and converge via the primary-key upsert.
3. **Snapshot.** The table's existing rows stream in over a
   `START TRANSACTION WITH CONSISTENT SNAPSHOT` read, in batches of
   `mysql_replication_bootstrap_batch_size`. A truncate barrier is applied
   first so a re-bootstrap over a persistent accelerator starts clean.
4. **Persist the position.** Once the snapshot is durably applied, the
   captured head position is written to the accelerator's
   `spice_sys_mysql_binlog` sidecar table. This is Spice's replacement for a
   Postgres replication slot — MySQL keeps no per-replica cursor server-side.
5. **Stream.** Spice attaches as a replica (`COM_BINLOG_DUMP`) and turns
   committed transactions into insert/update/delete/truncate changes. The
   committed position checkpoints to the sidecar every
   `mysql_replication_checkpoint_interval`.
6. **Restart.** On restart with a persisted position, Spice resumes from it
   directly — no snapshot — and marks the dataset ready immediately. Because
   the position lives inside the accelerator itself, data and cursor share
   one lifecycle: a non-persistent accelerator (`arrow`, `mode: memory`)
   boots with no position and naturally re-snapshots — no special
   configuration needed.

Delivery is **at-least-once**: a crash between applying a change and
checkpointing its position replays up to one checkpoint interval of history,
which the primary-key upsert absorbs idempotently. This is the same contract
as the Postgres connector's snapshot/WAL boundary.

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `mysql_replication_server_id` | derived | The `server_id` this replica registers with. Must be unique among all replicas attached to the source; the default is derived from the dataset name and process, so two spiced instances don't collide. |
| `mysql_replication_initial_snapshot` | `auto` | When existing rows load: `auto` snapshots when no resumable position exists; `disabled` streams changes only; `always` re-snapshots on every start. |
| `mysql_replication_checkpoint_interval` | `10s` | How often the committed position persists to the sidecar. Bounds crash-replay volume. |
| `mysql_replication_bootstrap_batch_size` | `8192` | Rows per emitted snapshot batch (max `1048576`). |
| `mysql_replication_invalid_checkpoint_behavior` | `error` | What to do when the persisted position was purged from the source: `error` or `restart` (drop the position and re-snapshot). |
| `mysql_replication_ready_lag` | `2s` | For `refresh_mode: changes`, the dataset is marked Ready once its replication lag (now minus the newest applied change's source-commit time) falls below this — it stays not-ready while snapshotting or draining a backlog, so it never serves stale or incomplete data. Accepts any [fundu](https://docs.rs/fundu) duration string. |

The runtime-level CDC apply tunables (`cdc_prefetch_buffer`,
`cdc_max_coalesced_envelopes`, `cdc_max_coalesced_bytes`,
`cdc_max_coalesce_age_ms`, `cdc_commit_timeout_ms`) apply to this connector
the same way they do to the Postgres one.

## When the position is purged

MySQL expires binary logs on its own schedule (`binlog_expire_logs_seconds`,
default 30 days; much shorter on some managed services). If spiced is down
long enough for the persisted position's file to be purged, the stream cannot
resume losslessly. By default this surfaces as an error naming the fix; set

```yaml
params:
  mysql_replication_invalid_checkpoint_behavior: restart
```

to instead drop the stale position, truncate the accelerator, and re-snapshot
the table automatically.

## Semantics and type notes

- **TRUNCATE TABLE** on the source applies as a truncate on the accelerator.
- **Primary-key updates** (`UPDATE ... SET id = ...`) apply as a delete of the
  old key plus an upsert of the new row, so no orphan rows linger.
- **TIMESTAMP columns** replicate as UTC (that is how the binlog stores them),
  and the snapshot pins its session to UTC to match. Set
  `mysql_time_zone: '+00:00'` (the default) so federated reads agree.
- **Zero dates** (`0000-00-00`) coerce to `NULL`, matching the read
  connector's default `mysql_zero_date_behavior: null`.
- **ENUM / SET** columns resolve to their label strings using the definition
  in `information_schema`.
- **UNSIGNED integers** map to the same signed Arrow types the read connector
  uses; values above the signed maximum fail loudly rather than wrap.
- **Negative `TIME` values** and spatial/geometry types are not supported —
  exclude such columns from the dataset schema.

## Schema changes

On `ALTER TABLE` against the replicated table, Spice re-fetches the table's
layout from `information_schema` and keeps streaming as long as every dataset
column still exists on the source — the same tolerance the Postgres
connector's block mode has:

- **Columns added on the source** are not replicated (a warning names them);
  add them to the dataset schema and restart to capture them.
- **Dropping or renaming a dataset column** (or `RENAME TABLE`/`DROP TABLE`)
  stops the stream with an actionable error. The durable binlog checkpoint is
  **not** advanced past that failure, so a restart still sees the last
  known-good position.
- **Retyping a dataset column** keeps streaming while values remain
  convertible to the dataset's Arrow type; an unconvertible value stops the
  stream with a decode error.

Binlog row images are positional. Each checkpoint therefore stores a
fingerprint of the source ordinal layout (column names, types, order, and
primary-key membership) alongside the dataset schema. On restart, Spice
refuses to resume when either has drifted — including source-only reorders
that leave the dataset schema unchanged — and either errors or re-bootstraps
per `mysql_replication_invalid_checkpoint_behavior`. Checkpoints written before
layout fingerprinting (legacy) are treated the same way: set
`mysql_replication_invalid_checkpoint_behavior: restart` once to rebuild
(see [#11763](https://github.com/spiceai/spiceai/issues/11763) for the
upgrade/release-note tracking).

If the stream stopped across a DDL boundary with an un-checkpointed tail,
re-bootstrap with `mysql_replication_invalid_checkpoint_behavior: restart`.
Quiescing writes to the table around DDL avoids that case entirely.

**Lag + multiple same-count DDLs:** mid-stream adopt re-reads today's
`information_schema`. If the stream is behind and the source applies several
compatible same-count reorders/retypes before Spice processes the first
`ALTER`, Spice can adopt the final layout while still decoding intermediate
row images. Prefer quiescing DDL until replication lag is near zero; see
[#11764](https://github.com/spiceai/spiceai/issues/11764).

## Metrics

Exposed under `dataset_mysql_*` alongside the connection-pool metrics:

| Metric | Meaning |
| --- | --- |
| `replication_lag_ms` | Now minus the newest applied source commit timestamp (1s granularity). |
| `replication_lag_bytes` | Binlog bytes between the source head and the resume position. Reported only while both are in the same binlog file; absent otherwise. |
| `replication_source_head_file` / `_pos` | The source server's binlog head, polled every checkpoint interval. |
| `replication_committed_binlog_file` / `_pos` | The checkpointed resume position. |
| `replication_transactions_total` | Source transactions observed. |
| `replication_inserts_total` / `_updates_total` / `_deletes_total` / `_truncates_total` | Row events applied. |
| `replication_bootstrap_rows_total` / `_expected` / `_complete` | Snapshot progress. |
| `replication_decode_errors_total` | Binlog decode failures. |
| `replication_schema_mismatch_errors_total` | Mid-stream DDL detections. |
| `replication_recv_errors_total` / `replication_reconnects_total` | Transport health. |
| `replication_checkpoint_persists_total` / `_errors_total` | Sidecar checkpoint writes. |
| `replication_gtid_enabled` | `1` when positioning by GTID auto-positioning (cold bootstrap or resume), `0` for file+offset. |

## Failover-safe resume (GTID)

Binlog file names and byte offsets are **server-local** — after a source
failover (managed-MySQL promotion, group-replication switchover, planned
maintenance), a persisted `binlog.000042:12345` is meaningless on the new
primary and the dataset would re-snapshot. **GTID auto-positioning** avoids
this: a GTID (`server_uuid:sequence`) is a globally unique transaction identity,
so the executed GTID set is server-independent. On resume Spice sends the set
via `COM_BINLOG_DUMP_GTID` and *any* server in the topology computes the correct
start point.

This is **fully automatic — there is no configuration**. When a dataset is first
bootstrapped, Spice uses GTID positioning if the source runs with
`gtid_mode = ON`, and file+offset otherwise. The executed set is captured
atomically with the snapshot head, extended per committed transaction, and
persisted alongside the file position in the `spice_sys_mysql_binlog` sidecar
(`gtid_executed` column, plus an explicit `file`|`gtid` `cursor_type` so the
type does not need to be inferred from the GTID set on resume).

Because it's automatic and otherwise silent, Spice **logs the resolved
positioning** at stream start so you can confirm a dataset is failover-safe:

- `INFO … GTID auto-positioning active (failover-safe resume)` — GTID in use.
- `WARN … file+offset positioning (source gtid_mode is 'OFF', not 'ON') …
  resume is NOT failover-safe; a source promotion will force a full re-snapshot`
  — the source is GTID-capable but not `ON` (the value is named: `OFF`,
  `ON_PERMISSIVE`, …).
- `WARN … file+offset positioning (this server does not support GTIDs) …` — a
  MariaDB or pre-GTID MySQL source.

The `replication_gtid_enabled` metric mirrors this (`1` = GTID, `0` =
file+offset) for dashboards/alerts.

**Requirements & notes:**

- To get failover-safe resume, run the source with `gtid_mode = ON` (which
  implies `enforce_gtid_consistency = ON`) *before* the dataset is bootstrapped.
  Transitional states (`ON_PERMISSIVE`) are treated as not enabled.
- **MariaDB** uses an incompatible GTID scheme, so datasets there bootstrap on
  file+offset automatically.
- **The cursor type is fixed at bootstrap.** A dataset keeps resuming with the
  type it was bootstrapped with, and the type is never switched implicitly:
  - A dataset bootstrapped on **file+offset** keeps using file+offset even after
    you later turn on `gtid_mode` — turning GTIDs on never disturbs a running
    dataset.
  - A dataset bootstrapped on **GTID** must keep resuming via GTID. If the source
    can no longer do GTID (`gtid_mode` turned off, or repointed at a non-GTID
    server), resume is a **hard error** rather than a silent downgrade to a
    server-local file position that does not correspond to the applied GTID set.
  - To change a dataset's cursor type, drop the accelerator's persisted state
    (its `spice_sys_mysql_binlog` row) and let it re-bootstrap.
  (`mysql_replication_invalid_checkpoint_behavior` governs only same-type
  problems — a purged position or a layout/schema change.)

## Limitations (current)

- **One table per dataset**, one binlog connection per dataset. (Postgres
  offers shared slots; a shared binlog connection is a follow-up.)
- **Schema evolution is block-mode only** — compatible `ALTER TABLE` is
  tolerated (see above), but `on_schema_change` policies that *adopt* new
  columns (`append_new_columns` / `sync_all_columns`) are not yet wired to
  this connector. Same-count mid-stream reshapes under replication lag can
  still mis-map until [#11764](https://github.com/spiceai/spiceai/issues/11764)
  lands; quiesce DDL while lag is non-zero.
- **XA (two-phase) transactions are not supported.** An XA transaction that
  touches the replicated table stops the stream with an error; XA activity on
  other tables logs a warning and is ignored.
- Not supported source types: geometry/spatial, vectors, negative `TIME`.
