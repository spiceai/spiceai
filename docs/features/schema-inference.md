# Schema Inference

Every connector always infers a dataset's **column schema** (names and types) from
the source. On top of that, Spice **always attempts the deepest schema inference it
can** — auto-detecting the source table's **primary key**, **secondary indexes**,
**sort/clustering order**, a **rough table size**, and (where available) **per-column
statistics** — and applies them to the acceleration settings (and table statistics)
you did **not** specify in the Spicepod.

There is **no configuration** for this: inference is on by default and requires no
Spicepod field. It runs read-only catalog queries against the source and **degrades
gracefully** to whatever the source permits — see [Graceful degradation](#graceful-degradation).

```yaml
datasets:
  - from: postgres:public.orders
    name: orders
    # No primary_key / indexes / sort needed — schema inference fills them.
    acceleration:
      engine: duckdb
      refresh_mode: full
```

Inference is strictly **gap-filling**: a value you configure explicitly always wins.
It applies to **all refresh modes** (`full`, `append`, `changes`, `snapshot`,
`caching`), and is applied before registration so change-data-capture
(`refresh_mode: changes`) sees the inferred primary key too. The one exception is
inferred **sort** order, which is *not* applied for `refresh_mode: changes`: CDC is
driven by the upsert (primary) key rather than a refresh-time sort, and applying a
sort can perturb the initial snapshot. Primary key, `on_conflict`, and indexes are
still inferred for CDC.

## Supported connectors

Inference detects as much as each source exposes. Connectors not listed here still
infer the column schema; they simply do not yet emit the deeper metadata (it is a
no-op for them, never an error).

| Connector | Primary key | Indexes | Sort / clustering | Table size |
| --- | --- | --- | --- | --- |
| [PostgreSQL](#postgresql) | `pg_index` (`indisprimary`) | unique & non-unique `pg_index` entries | clustered index (`indisclustered`) with ASC/DESC, else primary key | `pg_class.reltuples` + `pg_relation_size` |
| [MySQL](#mysql) | `information_schema` key columns | — | — | `information_schema.tables` rows + data length |
| [MongoDB](#mongodb) | always `_id` | `listIndexes` (unique & non-unique) | clustered collection key (5.3+), else `_id` | `collStats` count + size |

### PostgreSQL

Inferred from `pg_catalog` with a few read-only catalog queries
(`pg_index`/`pg_class`/`pg_attribute` for keys and indexes, plus the partition
key, table sizing via `pg_class.reltuples`/`pg_relation_size`, and per-column
statistics from `pg_stats`):

- **Primary key** → `acceleration.primary_key` (plus an `upsert` `on_conflict`
  on that key, so the accelerator upserts by primary key).
- **Indexes** → `acceleration.indexes` (a unique index becomes a `unique`
  index; others become `enabled`). The primary key's own index is not
  duplicated. **Partial** and **expression** indexes are skipped (a partial
  unique index is not a table-wide guarantee, and expression keys have no plain
  column to apply).
- **Sort columns** → strongest signal first: (1) the clustered index's columns
  and their `ASC`/`DESC` direction (`pg_index.indoption`); else (2) the range/list
  partition key, then any remaining primary-key columns; else (3) a natural-order
  column from `pg_stats` — a high-cardinality column whose physical-order
  correlation is near `±1` (an append-mostly heap ordered by e.g. `created_at`) —
  then the remaining primary key; else (4) the primary key, ascending.

### MySQL

Inferred from `information_schema` (read-only):

- **Primary key** → `acceleration.primary_key` (plus an `upsert` `on_conflict`
  on that key). This makes `refresh_mode: changes` (binlog replication) work
  without a hand-declared `primary_key`.
- **Table size** → an estimated row count and data byte size from
  `information_schema.tables`.

Secondary indexes and sort/clustering order are not inferred for MySQL.

### MongoDB

Inferred from `listIndexes` / `listCollections` / `collStats`:

- **Primary key** → always `_id`, MongoDB's document key. This is structural and
  needs no catalog access, so it is inferred even when the rest of the catalog is
  restricted. It makes **MongoDB Streams** (`refresh_mode: changes`) work without
  manual configuration — the change-stream path requires `primary_key: _id` and a
  matching `on_conflict` upsert, both of which inference supplies.
- **Indexes** → secondary indexes from `listIndexes` (unique & non-unique).
  The `_id_` index, partial indexes, and non-b-tree key types (`text`,
  `2dsphere`, `hashed`, …) are skipped.
- **Sort columns** → a clustered collection's cluster key (MongoDB 5.3+) with
  direction, else `_id` ascending.

## Graceful degradation

Schema inference always **attempts the maximum**, then falls back a level at a time
to whatever the source lets it read — driven by the permissions of the connection's
role/user on the source database. It never fails a dataset: if a catalog query is
blocked (commonly the connection role lacks read access to the catalog), the runtime
logs an **info** message describing exactly what it dropped and continues.

- **PostgreSQL / MySQL** — if the catalog queries cannot run, the dataset registers
  with **base column/type inference only**: no primary key, indexes, sort, table
  sizing, or — for PostgreSQL — per-column statistics are inferred. The runtime logs,
  e.g.: *"Schema inference degraded to base column/type inference (postgres): could
  not read the PostgreSQL catalog … grant catalog read access for full inference."*
- **MongoDB** — the `_id` primary key is structural and is always inferred. If
  `listIndexes`/`listCollections`/`collStats` are blocked or time out, inference
  degrades to **`_id`-only** (no secondary indexes, sort, or sizing) and logs an info
  message.

Because inference is gap-filling, degradation only means the runtime auto-fills
*fewer* acceleration settings — you can always set `primary_key`, `indexes`, or a
sort parameter explicitly to supply what the source did not expose.

## Table sizing

Inference also captures a **rough table size** — an estimated row count and data
byte size — from the source catalog (no table scan): PostgreSQL reads
`pg_class.reltuples` and `pg_relation_size`; MySQL reads `information_schema.tables`;
MongoDB reads `collStats` `count` and `size`. The estimate is surfaced as DataFusion
**table statistics** (`num_rows` and `total_byte_size`, both marked *inexact*) on the
source table provider, so it serves three purposes from one place:

- **Query planning** — the optimizer uses the row/byte estimates (federated
  sources otherwise report unknown statistics).
- **Acceleration sizing** — the acceleration path can read the source
  statistics to inform sizing decisions.
- **Observability** — the estimate is logged at registration and is available
  via the table statistics.

The size is a **registration snapshot**: estimated once when the dataset loads
and re-estimated on restart/re-register. It is stored in schema metadata
(`spice.inferred_row_count` / `spice.inferred_table_bytes`), so a future
refresh-driven or periodic updater can keep it current by simply re-emitting
those keys — no other change required.

## How sort columns are applied

Sort columns map to the engine's existing sort parameter, only when you have not
set one yourself:

- **DuckDB** → `on_refresh_sort_columns` (direction-qualified, e.g.
  `"created_at DESC, id ASC"`). DuckDB on-refresh sorting is experimental.
- **Arrow** → `sort_columns` (direction-qualified).
- **Cayenne** → `cayenne_sort_columns` (bare column names; Cayenne sorts by
  column and ignores direction).
- **SQLite / Turso / PostgreSQL accelerators** have no sort parameter, so sort
  inference is skipped.
- **`refresh_mode: changes` (CDC)** — sort inference is skipped regardless of
  engine; the change-stream accelerator is driven by the upsert key, not a
  refresh-time sort.

## Notes & limitations

- Inference never overrides explicitly-configured `primary_key`, `indexes`, or
  sort parameters.
- A column that is projected away by a `refresh_sql` is dropped from the
  inferred set so the accelerator never rejects it; if a `refresh_sql` can't be
  parsed, inference is skipped for that dataset.
- Inferred metadata is internal and is not surfaced in `information_schema` or
  query results.
