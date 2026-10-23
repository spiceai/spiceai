---
name: verify-pk-insert-semantics
description: Prove PK-accelerated INSERT semantics against a running spiced using the project-local repro script. Use when changing DuckDB, SQLite, or Cayenne write paths, on_conflict defaults, or INSERT counts.
---

# Verify PK-accelerated INSERT semantics

Run this against a **running `spiced`** built from the commit under test. It is the
pstack-style check for user `INSERT` into a PK-accelerated dataset
(`primary_key: id`, `write_mode: acceleration`, no `on_conflict`).

## What it proves

- Existing-key `INSERT` still upserts (default unchanged) and reports `count: 1`.
- In-batch duplicate keys:
  - DuckDB / SQLite: a uniqueness / constraint error, never `Query is not read-only`.
  - Cayenne: last-arrival-wins, **incoming** count `2`, last row kept.
- NULL primary key: rejected on DuckDB, SQLite, and Cayenne. DuckDB must not leak
  `__data_<dataset>_<n>`.

## Prerequisites

- `spiced` on `PATH` (data-only is enough: `make install-data-only-dev`).
- `curl`, `python3`, and `pyarrow` (`pip install pyarrow`).
- Ports `8394` / `50394` / `9394` free.

## Run

From the repo root:

```bash
.cursor/skills/verify-pk-insert-semantics/50_pk_insert_semantics.sh
```

The script writes a throwaway Spicepod under `/tmp`, starts `spiced`, waits on
`/v1/ready`, and prints each SQL response.

## Record

Capture the full transcript. Pair a **trunk** run with a **branch** run of the
same script. The commit SHA and `spiced --version` travel with the artifact.
