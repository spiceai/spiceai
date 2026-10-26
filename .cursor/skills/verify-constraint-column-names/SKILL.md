---
name: verify-constraint-column-names
description: Check against a running spiced that acceleration primary_key and indexes accept column names that are not bare SQL identifiers (service.instance.id, sentry-environment, 2xx_count), bare or double-quoted, on duckdb, sqlite, arrow and cayenne. Use for before/after evidence on constraint DDL quoting.
---

# Verify constraint column names

Project-local check that a running `spiced` loads accelerations whose
`primary_key` (and a unique index on the same column) names a column that is
not a plain identifier, and that the key is still enforced.

## When to use

- After changing how an accelerator writes `PRIMARY KEY`, `UNIQUE`, or index
  DDL (DuckDB DDL lives in `datafusion-table-providers`
  `core/src/duckdb/creator.rs`).
- To get trunk-vs-branch before/after evidence for this defect.

## What it checks

It writes one CSV with columns `service.instance.id`, `sentry-environment`,
`2xx_count` and `a`. For each engine (`duckdb`, `sqlite`, `arrow`,
`cayenne`), each column, and each spelling (bare, or wrapped in `"..."`), it
declares a dataset with:

- `primary_key: <column>`
- `indexes: { <column>: unique }`

It also declares one composite key over all three columns per engine.

The script waits for `/v1/datasets?status=true`, then requires:

1. Every dataset is `Ready`.
2. `SELECT count(*)` is 2 on every dataset.
3. The log has no `Parser Error` and no `syntax error at or near`.

A failing run prints each dataset that is not Ready and the log lines that
explain why.

## Run

You need a `spiced` binary and `curl`. The `jq` tool is optional. A launched
`spiced` is killed on `EXIT` unless `KEEP=1`.

```bash
SPICED=/path/to/spiced .cursor/skills/verify-constraint-column-names/run.sh
```

Optional environment:

- `SPICED`: the `spiced` binary (default: `spiced` on `PATH`)
- `HTTP`, `FLIGHT`, `METRICS`: bind addresses (defaults
  `127.0.0.1:18590`, `127.0.0.1:18591`, `127.0.0.1:18592`)
- `ENGINES`: a space-separated subset (default `duckdb sqlite arrow cayenne`)
- `WORKDIR`: an existing directory (default: a fresh temp dir)
- `KEEP=1`: leave `spiced` running

For trunk vs branch, run once with each binary and save both transcripts.
Datasets are named `t_<engine>_...` (SQLite reserves the `sqlite_` prefix). Before the DuckDB fix, every `t_duckdb_*` dataset fails with `Parser Error:
syntax error at or near "."` (or `"-"`, or `"2"`).
