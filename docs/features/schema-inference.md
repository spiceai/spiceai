# Extended Schema Inference (`schema_inference`)

Every connector always infers a dataset's **column schema** (names and types)
from the source. *Extended* schema inference goes deeper: it auto-detects the
source table's **primary key**, **secondary indexes**, and **sort/clustering
order**, and applies them to the dataset's acceleration settings that you did
**not** specify in the Spicepod.

This is opt-in per dataset:

```yaml
datasets:
  - from: postgres:public.orders
    name: orders
    schema_inference: extended      # default: standard
    acceleration:
      engine: duckdb
      refresh_mode: full
```

- `standard` (default) — only the column schema is inferred, exactly as before.
- `extended` — additionally infer and apply primary key, indexes, and sort
  columns when they are unset.

Inference is strictly **gap-filling**: a value you configure explicitly always
wins. It applies to **all refresh modes** (`full`, `append`, `changes`,
`snapshot`, `caching`), and is applied before registration so change-data-capture
(`refresh_mode: changes`) sees the inferred primary key too.

## Supported connectors

| Connector | Primary key | Indexes | Sort / clustering |
| --- | --- | --- | --- |
| [PostgreSQL](#postgresql) | `pg_index` (`indisprimary`) | unique & non-unique `pg_index` entries | clustered index (`indisclustered`) with ASC/DESC, else primary key |
| [MongoDB](#mongodb) | always `_id` | `listIndexes` (unique & non-unique) | clustered collection key (5.3+), else `_id` |

Other connectors treat `extended` as a no-op (they do not yet emit inferred
metadata).

### PostgreSQL

Inferred from `pg_catalog` (a single read-only query against
`pg_index`/`pg_class`/`pg_attribute`, run only when `extended`):

- **Primary key** → `acceleration.primary_key` (plus an `upsert` `on_conflict`
  on that key, so the accelerator upserts by primary key).
- **Indexes** → `acceleration.indexes` (a unique index becomes a `unique`
  index; others become `enabled`). The primary key's own index is not
  duplicated. **Partial** and **expression** indexes are skipped (a partial
  unique index is not a table-wide guarantee, and expression keys have no plain
  column to apply).
- **Sort columns** → the clustered index's columns and their `ASC`/`DESC`
  direction (`pg_index.indoption`); if the table has no clustered index, the
  primary key (ascending) is used.

### MongoDB

Inferred from `listIndexes` / `listCollections` (run only when `extended`):

- **Primary key** → always `_id`, MongoDB's document key. This makes
  **MongoDB Streams** (`refresh_mode: changes`) work without manual
  configuration — the change-stream path requires `primary_key: _id` and a
  matching `on_conflict` upsert, both of which inference now supplies.
- **Indexes** → secondary indexes from `listIndexes` (unique & non-unique).
  The `_id_` index, partial indexes, and non-b-tree key types (`text`,
  `2dsphere`, `hashed`, …) are skipped.
- **Sort columns** → a clustered collection's cluster key (MongoDB 5.3+) with
  direction, else `_id` ascending.

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

## Notes & limitations

- Inference never overrides explicitly-configured `primary_key`, `indexes`, or
  sort parameters.
- A column that is projected away by a `refresh_sql` is dropped from the
  inferred set so the accelerator never rejects it; if a `refresh_sql` can't be
  parsed, inference is skipped for that dataset.
- Inferred metadata is internal and is not surfaced in `information_schema` or
  query results.
