---
name: Enhancement
about: Suggest an Enhancement
title: 'Enhancement: Durable Acceleration Writes'
type: enhancement
assignees: ''
---

<!--
_REMEMBER, BE **SMART**!_

_S: Specific_
_M: Measurable_
_A: Achievable_
_R: Relevant_
_T: Time-Bound_
-->

## Goal-State/What/Result

Enable `write_mode: write_back` to reliably deliver writes to the federated source (e.g. PostgreSQL). Today, writes committed to the accelerator that have not yet been delivered to the federated source are permanently lost if Spice fails to deliver them immediately after local write. 

The goal is to upgrade the delivery guarantee from "best-effort consistency" to "eventual consistency": once a write is ACK'd to the client, it will eventually reach the federated source regardless of process restarts or network failures.

## Why/Purpose

### The Use Case

A common deployment pattern is: Spice acts as both the read cache and the write path for a table. The application writes exclusively through Spice — no other writer touches the federated table directly. The requirements are:

- **Fast reads** — reads are served from the accelerator (in-memory or local DuckDB file), not from the remote database
- **Fast write ACK** — the write is immediately visible through Spice after the client receives the ACK
- **Eventual persistence** — the write reaches the federated source (PostgreSQL) shortly after, for durability and reporting

From the client's perspective, Spice is fully ACID — writes are atomic, immediately visible, and durable within Spice. Delivery to federated source is an internal replication concern, not an application concern.

### The Problem: Writes Are Not Durably Delivered

There are two failure scenarios that produce the same result — the accelerator has the write, the federated source does not:

1. **Federated source unresponsive** — the background delivery task fails because PostgreSQL is temporarily unavailable (network partition, restart, overload). Spice is still running but the write is stuck undelivered.

2. **Spice crashes** — Spice terminates between the accelerator write and the background task completing. The pending write is permanently lost — PostgreSQL never receives it and there is no record of it anywhere.

Today's `write_back` implementation handles neither case durably. Delivery is fire-and-forget: transient federated failures are not retried, and in-flight deliveries are lost on Spice restart.

## By When

**Issue/Spec written and reviewed:** _Target Date_
**Done-Done:** _Target Date_

## Done-Done

- [ ] [Principles Driven](https://github.com/spiceai/spiceai/blob/trunk/docs/PRINCIPLES.md)
- [ ] The Algorithm
- [ ] PM/Design Review
- [ ] DX/UX Review
- [ ] Release Notes / PRFAQ
- [ ] Threat Model / Security Review
- [ ] Tests
- [ ] Telemetry / Metrics / Task History
- [ ] Performance / Benchmarks
- [ ] Documentation
- [ ] Cookbook Recipes/Tutorials

## The Algorithm

- [ ] Every requirement questioned?
- [ ] Delete (Scope) any part you can.
- [ ] Simplify.
- [ ] Break down into smaller iterations/milestones.
- [ ] Opportunities for automation.

## Specification

### Proposal

The proposed solution introduces a Write-Ahead Log (WAL) embedded in the accelerator storage. Before ACKing a write to the client, Spice atomically commits both the data change and a WAL entry describing it in a single accelerator transaction. A background worker then reads undelivered WAL entries and applies them to the federated source with retry and backoff. On restart, any entries that were committed but not yet delivered are replayed before new writes are accepted.

This design keeps the write path entirely local — no network call to the federated source is in the critical path of the client ACK. The federated source is updated asynchronously, so a temporary PostgreSQL outage neither blocks writes nor increases write latency. Once the source recovers, the WAL worker drains the backlog automatically.

#### Write Path

For each DML operation under `write_mode: write_back`, the WAL records the **final row state** — not the original SQL expression. This is what makes replay idempotent: the WAL worker can upsert concrete rows into the federated source without re-evaluating any predicates.

INSERT is straightforward: the rows being inserted already are the final state, so they go directly into the WAL.

UPDATE is different. The SQL statement carries a filter predicate (`WHERE status = 'pending'`) and a change expression (`SET status = 'done'`), not the final row values. To record final state, Spice must first SELECT the affected rows within the same transaction — resolving which rows match and what their new values will be — before writing the WAL entry and applying the update. This SELECT must happen before the UPDATE because the predicate (`WHERE status = 'pending'`) will no longer match the rows after they are changed. Doing the SELECT after the UPDATE would find nothing.

**INSERT:**
```
BEGIN transaction
  INSERT INTO "__spice_wal_<table_name>" (op, pks, new_values) VALUES ('INSERT', [...], {...})
  INSERT INTO data_table ...
COMMIT
ACK to client
```

**UPDATE:**
```
BEGIN transaction (exclusive)
  SELECT pk FROM data_table WHERE <filters>        -- resolve affected PKs
  INSERT INTO "__spice_wal_<table_name>" (op, pks, new_values)
         VALUES ('UPDATE', [pk1, pk2, ...], {col: new_val, ...})
  UPDATE data_table SET ... WHERE pk IN (pk1, pk2, ...)
COMMIT
ACK to client
```

**DELETE:**
```
BEGIN transaction (exclusive)
  SELECT pk FROM data_table WHERE <filters>        -- resolve affected PKs
  INSERT INTO "__spice_wal_<table_name>" (op, pks, new_values)
         VALUES ('DELETE', [pk1, pk2, ...], null)
  DELETE FROM data_table WHERE pk IN (pk1, pk2, ...)
COMMIT
ACK to client
```

A background WAL worker reads undelivered entries in `seq` order and applies them to the federated source. On success, it advances the checkpoint.

#### WAL Table Schema (DuckDB)

Three system tables are created per accelerated table, using the dataset table name as a suffix (non-alphanumeric characters replaced with `_`).

```sql
CREATE SEQUENCE IF NOT EXISTS "__spice_wal_seq_<table_name>";

CREATE TABLE IF NOT EXISTS "__spice_wal_<table_name>" (
    seq        BIGINT  PRIMARY KEY DEFAULT nextval('"__spice_wal_seq_<table_name>"'),
    op         VARCHAR NOT NULL,          -- 'INSERT', 'UPDATE', 'DELETE'
    pks        VARCHAR NOT NULL,          -- JSON array of PK objects: [{"id":1},{"id":2}]
    new_values VARCHAR,                   -- Arrow NDJSON of new row values; NULL for DELETE
    written_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "__spice_wal_cp_<table_name>" (
    last_delivered_seq BIGINT NOT NULL    -- initialized to -1; seq starts at 0
);
```

All three tables live in the **same DuckDB file** as the data, so WAL writes and data writes are covered by the same transaction. Delivered WAL entries are deleted immediately after checkpoint advancement — there is no separate `delivered` flag.

#### Crash Recovery Path

On startup, the WAL worker starts immediately and resumes from `last_delivered_seq` — no blocking startup phase. New writes are accepted right away and appended to the WAL with higher `seq` values, so the worker naturally drains pre-crash entries before reaching any new ones.

```
[startup] WAL worker started for dataset 'orders': 42 undelivered entries pending delivery.
[startup] Dataset 'orders' is available (reads served from accelerator; writes queued for delivery).
```

If the federated source is unreachable at startup, the WAL worker retries with its standard backoff loop — startup is not affected. Blocking startup on a remote dependency would create a new availability failure mode worse than today's behavior: a temporary PostgreSQL outage during a Spice restart would prevent reads entirely.

All replay operations are idempotent by PK — safe to re-apply if Spice crashes mid-delivery.

#### WAL Cleanup

Delivered entries are deleted immediately after each successful checkpoint advancement, within the same transaction:

```sql
-- After advancing the checkpoint to seq N:
DELETE FROM "__spice_wal_<table_name>" WHERE seq <= N;
```

This keeps the WAL table small at all times. No separate periodic truncation job is needed.

### Requirements

#### Accelerator Requirements

The accelerator must satisfy all of the following:

1. **Cross-table transactions** — the WAL entry and the data change must be committed atomically in a single transaction. If the WAL write succeeds but the data write fails (or vice versa), the system is in an inconsistent state.

2. **Read-before-write within a transaction** — for UPDATE and DELETE, the set of affected primary keys must be resolved by querying the data table *before* applying the change, and within the same transaction. This is necessary because the filter predicate (e.g. `WHERE status = 'pending'`) may no longer match the rows after the change is applied — so the WAL entry must record the concrete PKs, not the original filter expression.

3. **Exclusive write locking** — the transaction must hold an exclusive lock on the affected rows (or table) from the PK resolution through the data write, preventing concurrent writes from modifying the same rows between steps.

4. **Durable storage** — the WAL table must survive process restarts. In-memory storage is not sufficient.

#### Accelerator Engine Compatibility

| Engine | Cross-table transactions | Read-before-write | Durable | Verdict |
|---|---|---|---|---|
| **DuckDB (file)** | ✅ Full ACID transactions | ✅ Native | ✅ File on disk | **Supported** |
| **Cayenne** | ⚠️ Metastore transactions only; data in S3 Parquet files outside transaction boundary | ⚠️ Table-level lock via metastore, not row-level | ✅ S3 + metastore | **Possible with limitations** (table-level locking, no write concurrency) |
| **DuckDB (in-memory)** | ✅ Transactions | ✅ Native | ❌ Lost on restart | **Not supported** (not durable) |
| **Arrow** | ❌ No transactions | ❌ No locking | ❌ In-memory | **Not supported** |
| **SQLite** | ✅ Full ACID transactions | ✅ Native | ✅ File on disk | **Supported** (limited write concurrency) |

#### Federated Source Requirements

The federated destination must support **idempotent upsert by primary key**, so that WAL replay is safe even if a crash occurs mid-replay and some entries are replayed more than once:

- `INSERT` replay: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`
- `UPDATE` replay: `UPDATE ... SET new_values WHERE pk IN (...)`
- `DELETE` replay: `DELETE ... WHERE pk IN (...)`

PostgreSQL satisfies all three. Any federated source that supports upsert and PK-based delete qualifies.

### Telemetry and Metrics

The WAL introduces a new async delivery gap between the accelerator and the federated source. Without visibility into this gap, users have no way to detect delivery failures or growing backlogs until data loss or divergence is already severe.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `spice_wal_pending_entries` | Gauge | `dataset` | Number of WAL entries not yet delivered to the federated source. A sustained non-zero value indicates the federated source is falling behind or unreachable. |
| `spice_wal_delivery_failures_total` | Counter | `dataset` | Cumulative count of failed delivery attempts. Incremented on each WAL worker retry. |
| `spice_wal_delivery_latency_seconds` | Histogram | `dataset` | Time from WAL entry written to successful delivery to the federated source. Tracks end-to-end write-back latency under normal conditions. |
| `spice_wal_replay_entries_total` | Counter | `dataset` | WAL entries replayed on startup. Incremented once per Spice restart that finds undelivered entries. Non-zero indicates a prior crash or unclean shutdown. |

Log thresholds:
- `WARN` when `spice_wal_pending_entries` exceeds 1,000 entries for a dataset — emit at most once per minute to avoid log flooding.
- `INFO` on startup when replay begins and when it completes (or is deferred due to source unavailability).

## How/Implementation Plan

1. **WAL table management** — create `__spice_wal_<table_name>`, `__spice_wal_seq_<table_name>`, and `__spice_wal_cp_<table_name>` tables on accelerator initialization for supported engines (DuckDB file, SQLite)
2. **Write path** — wrap existing INSERT/UPDATE/DELETE accelerator operations in a transaction that also appends to `__spice_wal_<table_name>`; requires PK resolution before UPDATE/DELETE
3. **Background WAL worker** — a tokio task per accelerated table that reads undelivered WAL entries and applies them to the federated source with retry and backoff; delivers entries individually and advances the checkpoint after each successful delivery
4. **Crash recovery** — on startup, the WAL worker starts immediately and resumes from `last_delivered_seq`; new writes are accepted right away and appended with higher `seq` values, so pre-crash entries are drained before new ones naturally
5. **Compaction** — delete delivered entries immediately after each checkpoint advancement; compact consecutive same-PK entries before delivery
6. **Telemetry** — emit `spice_wal_pending_entries`, `spice_wal_delivery_failures_total`, `spice_wal_delivery_latency_seconds`, and `spice_wal_replay_entries_total` metrics; log WAL depth warnings at threshold
7. **Validation** — reject WAL-enabled configuration for unsupported engines (Arrow, in-memory DuckDB) with a clear error message

## QA Plan

- Unit tests: WAL append, checkpoint advancement, compaction logic
- Integration tests: crash simulation (kill Spice mid-write, verify replay on restart), concurrent writes, idempotent replay
- Test each supported engine: DuckDB file, SQLite
- Verify unsupported engines produce clear error messages
- Performance benchmarks: write latency overhead of WAL append vs. baseline

## Release Notes

**Durable Acceleration Writes**: `write_mode: write_back` now guarantees that writes committed to the Spice accelerator will survive process restarts and eventually reach the federated source. A Write-Ahead Log (WAL) is maintained in the accelerator — on restart, any undelivered writes are automatically replayed to the federated source before normal operation resumes. Supported for DuckDB (file mode) and SQLite accelerators.

Example configuration:

```yaml
datasets:
  - from: postgres:orders
    name: orders
    access: read_write
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      write_mode: write_back
```

With this configuration, writes to `orders` through Spice are immediately visible and will durably reach PostgreSQL even if Spice restarts.
