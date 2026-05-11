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

**Issue/Spec written and reviewed:**
**Done-Done:**

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

UPDATE requires two SELECTs within the same transaction. The filter predicate (`WHERE status = 'pending'`) must be resolved to concrete PKs **before** the UPDATE — it will not match after the change is applied. The final row values must be read **after** the UPDATE — that is when they reflect the new state. The WAL entry is written last, once both PKs and final values are known.

DELETE only needs one SELECT — to resolve the concrete PKs before the rows are removed.

**INSERT:**
```
BEGIN transaction
  INSERT INTO "__spice_wal_<table_name>" (op, pks, new_values) VALUES ('INSERT', <pks ipc>, <rows ipc>)
  INSERT INTO data_table ...
COMMIT
ACK to client
```

**UPDATE:**
```
BEGIN transaction
  SELECT pk FROM data_table WHERE <filters>              -- resolve affected PKs (filter still matches)
  UPDATE data_table SET ... WHERE pk IN (pk1, pk2, ...)
  SELECT * FROM data_table WHERE pk IN (pk1, pk2, ...)   -- read final state after update
  INSERT INTO "__spice_wal_<table_name>" (op, pks, new_values)
         VALUES ('UPDATE', <pks ipc>, <new rows ipc>)
COMMIT
ACK to client
```

**DELETE:**
```
BEGIN transaction
  SELECT pk FROM data_table WHERE <filters>        -- resolve affected PKs
  INSERT INTO "__spice_wal_<table_name>" (op, pks, new_values)
         VALUES ('DELETE', <pks ipc>, NULL)
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
    pks        BLOB    NOT NULL,          -- Arrow IPC bytes: PK columns of affected rows
    new_values BLOB,                      -- Arrow IPC bytes: full row state for INSERT/UPDATE; NULL for DELETE
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

1. **Atomic WAL + data commit** — the WAL entry and the data change must be committed atomically. If the WAL write succeeds but the data write fails (or vice versa), the system is in an inconsistent state.

2. **Read-before-write isolation** — for UPDATE and DELETE, the set of affected primary keys must be resolved by querying the data table *before* applying the change. This is necessary because the filter predicate (e.g. `WHERE status = 'pending'`) may no longer match the rows after the change is applied — so the WAL entry must record the concrete PKs, not the original filter expression. No concurrent writer must be able to modify those rows between the SELECT and the UPDATE, which can be enforced in one of two ways:
   - **Pessimistic**: acquire an exclusive lock before the SELECT and hold it through the UPDATE (e.g. SQLite table lock, Cayenne write mutex). No concurrent writer can enter the window.
   - **Optimistic**: allow concurrent writes; detect a write-write conflict at commit time and abort the transaction. Spice retries the full operation (SELECT → WAL insert → UPDATE) from scratch with exponential backoff. The WAL entry is rolled back with the transaction, so no inconsistent entry is ever committed. Retries are unbounded — the conflict is always transient (another writer made progress), so a successful commit is guaranteed eventually.

3. **Durable storage** — the WAL table must survive process restarts. In-memory storage is not sufficient.

#### Accelerator Engine Compatibility

| Engine | Atomic WAL + data | Read-before-write isolation | Durable | Verdict |
|---|---|---|---|---|
| **DuckDB (file)** | ✅ Single transaction | Optimistic: conflict aborts transaction; Spice retries | ✅ File on disk | **Supported** |
| **Cayenne** | ✅ Via metastore transaction (see below) | Pessimistic: per-table write mutex held across SELECT → commit | ✅ Vortex files + SQLite metastore | **Supported** (requires metastore WAL extension) |
| **DuckDB (in-memory)** | ✅ Single transaction | Optimistic (same as file mode) | ❌ Lost on restart | **Not supported** (not durable) |
| **Arrow** | ❌ No transactions | ❌ No isolation | ❌ In-memory | **Not supported** |
| **SQLite** | ✅ Single transaction | Pessimistic: table-level write lock | ✅ File on disk | **Supported** (limited write concurrency) |

#### Cayenne WAL Design

Cayenne stores data as Vortex columnar files on disk or S3, with a SQLite metastore that tracks which files belong to which snapshot. The critical property is that **a Vortex file written to disk is invisible to readers until the metastore is updated to include it in the current snapshot**. This two-phase visibility is what makes atomic WAL integration possible.

Since making data visible and committing the WAL entry are both SQLite operations, they can be included in the same SQLite transaction. The write path becomes:

```
1. Acquire per-table write lock
2. (UPDATE/DELETE only) SELECT affected rows from Vortex — resolve final state
3. Write Vortex file to disk   ← durable, but invisible (no snapshot points to it yet)
4. BEGIN metastore (SQLite) transaction
5.   INSERT INTO __spice_wal_<table_name> (WAL entry with final row state)
6.   UPDATE snapshot to reference new Vortex file  ← makes data visible
7. COMMIT  ← atomically: WAL entry recorded + data becomes visible to readers
8. ACK to client
```

The per-table write lock (held across steps 1–7) ensures no concurrent writer modifies the data table between the pre-UPDATE SELECT (step 2) and the snapshot commit (step 7).

**What needs to change in Cayenne**

The `MetastoreTransaction` trait currently only handles single-table catalog operations. To support WAL:

1. Add `__spice_wal_<table_name>` and `__spice_wal_cp_<table_name>` tables to the Cayenne metastore schema.
2. Extend `MetastoreTransaction` to allow inserting a WAL entry within the same transaction that updates the snapshot — so both are committed together in step 7.
3. Expose a WAL read API from the metastore for the background WAL worker to query undelivered entries.

#### Federated Source Requirements

WAL delivery is at-least-once: the checkpoint is advanced in Spice's local storage after a successful federated write, but these two operations are on different systems and cannot be made atomic. If Spice crashes after PostgreSQL commits but before the checkpoint advances, the entry will be replayed on restart.

`UPDATE` and `DELETE` replay are naturally idempotent — applying the same values again or deleting an already-deleted row is a no-op in SQL. `INSERT` is the only operation that can fail on replay: if the row already exists in PostgreSQL, a plain `INSERT` would return a duplicate key error. The federated source must therefore support upsert for INSERT replay:

- `INSERT` replay: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` — overwrites with the same values if the row is already present; no-op in effect
- `UPDATE` replay: `UPDATE ... SET new_values WHERE pk IN (...)` — idempotent by nature
- `DELETE` replay: `DELETE ... WHERE pk IN (...)` — idempotent by nature

PostgreSQL satisfies all three. Any federated source that supports upsert and PK-based delete qualifies.

### Telemetry and Metrics

The WAL introduces a new async delivery gap between the accelerator and the federated source. Without visibility into this gap, users have no way to detect delivery failures or growing backlogs until data loss or divergence is already severe.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `wal_pending_entries` | Gauge | `dataset` | Number of WAL entries not yet delivered to the federated source. A sustained non-zero value indicates the federated source is falling behind or unreachable. |
| `wal_delivery_failures_total` | Counter | `dataset` | Cumulative count of failed delivery attempts. Incremented on each WAL worker retry. |
| `wal_delivery_latency_seconds` | Histogram | `dataset` | Time from WAL entry written to successful delivery to the federated source. Tracks end-to-end write-back latency under normal conditions. |
| `wal_replay_entries_total` | Counter | `dataset` | WAL entries replayed on startup. Incremented once per Spice restart that finds undelivered entries. Non-zero indicates a prior crash or unclean shutdown. |

Log thresholds:
- `WARN` when `spice_wal_pending_entries` exceeds 1,000 entries for a dataset — emit at most once per minute to avoid log flooding.
- `INFO` on startup when replay begins and when it completes (or is deferred due to source unavailability).

## How/Implementation Plan

### M1: WAL Machinery + DuckDB Support

1. **WAL table management** — create `__spice_wal_<table_name>`, `__spice_wal_seq_<table_name>`, and `__spice_wal_cp_<table_name>` tables on accelerator initialization for DuckDB (file mode) and SQLite
2. **Write path** — wrap existing INSERT/UPDATE/DELETE accelerator operations in a DuckDB/SQLite transaction that also appends to `__spice_wal_<table_name>`; requires PK resolution before UPDATE/DELETE
3. **Background WAL worker** — a tokio task per accelerated table that reads undelivered WAL entries and applies them to the federated source with retry and backoff; delivers entries individually and advances the checkpoint after each successful delivery
4. **Crash recovery** — on startup, the WAL worker starts immediately and resumes from `last_delivered_seq`; new writes are accepted right away and appended with higher `seq` values, so pre-crash entries are drained before new ones naturally
5. **Compaction** — delete delivered entries immediately after each checkpoint advancement; compact consecutive same-PK entries before delivery
6. **Telemetry** — emit `spice_wal_pending_entries`, `spice_wal_delivery_failures_total`, `spice_wal_delivery_latency_seconds`, and `spice_wal_replay_entries_total` metrics; log WAL depth warnings at threshold
7. **Validation** — reject WAL-enabled configuration for unsupported engines (Arrow, in-memory DuckDB) with a clear error message

### M2: Cayenne Support

1. **Metastore schema** — add `__spice_wal_<table_name>` and `__spice_wal_cp_<table_name>` tables to the Cayenne metastore (SQLite) schema
2. **MetastoreTransaction extension** — extend `MetastoreTransaction` to allow inserting a WAL entry in the same transaction that updates the snapshot, so both are committed atomically
3. **Write path** — integrate WAL append into the Cayenne write path: write Vortex file to disk, then commit WAL entry + snapshot update together in a single metastore transaction
4. **WAL worker** — wire the shared WAL worker from M1 to Cayenne; implement the metastore read API for querying undelivered entries and advancing the checkpoint

## QA Plan

- Unit tests: WAL append, checkpoint advancement, compaction logic
- Integration tests: crash simulation (kill Spice mid-write, verify replay on restart), concurrent writes, idempotent replay
- Test each supported engine: DuckDB file, SQLite, Cayenne
- For Cayenne: verify crash between Vortex file write and metastore commit leaves a clean pre-write state (no phantom WAL entry, no orphaned Vortex file visible to readers)
- Verify unsupported engines produce clear error messages
- Performance benchmarks: write latency overhead of WAL append vs. baseline

## Release Notes

**Durable Acceleration Writes**: `write_mode: write_back` now guarantees that writes committed to the Spice accelerator will survive process restarts and eventually reach the federated source. A Write-Ahead Log (WAL) is maintained in the accelerator — on restart, any undelivered writes are automatically replayed to the federated source before normal operation resumes. Supported for DuckDB (file mode), SQLite, and Cayenne accelerators.

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
