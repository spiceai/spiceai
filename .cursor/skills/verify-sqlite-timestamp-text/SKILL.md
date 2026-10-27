---
name: verify-sqlite-timestamp-text
description: Check against a running spiced that a SQLite acceleration's time retention deletes every expired row, including rows from the cutoff's own calendar day, and that a filter on now() - INTERVAL runs and matches the same instant spelled out. Use for before/after evidence on SQLite timestamp filter rendering.
---

# Verify SQLite timestamp text

This is a project-local check for two defects in how SQLite accelerations compare timestamps. Both are fixed in `datafusion-table-providers`, in `core/src/sqlite/write.rs` and `core/src/sqlite/sqlite_interval.rs`.

1. **Retention leaves rows from the cutoff's own day.** The SQLite writer stores a timestamp as ISO-8601 text, for example `2026-10-26T05:56:16.25`. A retention delete rendered its cutoff as `datetime(<secs>, 'unixepoch')`, which gives `2026-10-26 05:56:16`. SQLite compares the two as text, and `'T' > ' '`, so no row from the cutoff's day was ever deleted.
2. **`now()` fails on SQLite.** `ts > now() - INTERVAL '24 hours'` was federated to SQLite still calling `now()`, which SQLite does not have, so the query failed with `no such function: now`.

## What it checks

The script writes a parquet file with one row a minute over the last 48 hours. Each row's time has a fraction of a second. Two SQLite datasets read it:

- `x_retention` uses `refresh_mode: append`. The first start loads every row with no retention. The second start enables `retention_period: 24h` with a `5s` check interval.
- `x_now` has no retention.

The script then requires two things:

1. `x_retention` keeps rows, and none of them are older than 24 hours plus one minute.
2. On `x_now`, the counts for `ts > now() - INTERVAL '24 hours'` and for the same instant written as a `TIMESTAMP` literal differ by at most 1. Rows are one minute apart, so the two moments the counts are taken at may straddle a row.

The script prints `PASS`, or `FAIL` with the matching log lines.

## Run

You need a `spiced` binary, `curl`, and Python with `pyarrow`.

```bash
SPICED=/path/to/spiced PY=/path/to/python .cursor/skills/verify-sqlite-timestamp-text/run.sh
```

Optional environment:

- `HTTP`, `FLIGHT`, `METRICS`: bind addresses. The defaults are `127.0.0.1:18690`, `127.0.0.1:18691` and `127.0.0.1:18692`.
- `WORKDIR`: an existing directory. The default is a fresh temp dir.
- `KEEP=1`: leave `spiced` running.

## Expected output on trunk (before the fix)

```
retention: rows=1808  older than ... (expected 0)=367
FAIL: retention kept expired rows
now(): by now()=ConnectionError Error("no such function: now in ... datetime(now(), '-24 hours') ...")
FAIL: now() - INTERVAL did not match the literal
```
