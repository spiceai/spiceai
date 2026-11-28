---
name: verify-arrow-on-conflict-dedup
description: Verify that acceleration `on_conflict` `upsert_dedup`, `upsert_dedup_by_row_id` and `drop` resolve duplicate primary keys within one load the same way on the Arrow and DuckDB engines, against a running spiced. Use when changing Arrow accelerator writes, on_conflict handling, or UpsertOptions plumbing.
---

# Verify Arrow on_conflict dedup

Runs `run.sh` against a spiced binary. It loads two small CSVs (`exact.csv` with an exact
duplicate row; `two.csv` where id 1 appears as `1,a` then `1,z`) into Arrow and DuckDB
accelerations for each `on_conflict` mode and checks:

| case | expected |
| --- | --- |
| `upsert_dedup`, exact duplicate | `1,a;2,b` |
| `upsert_dedup`, different rows share a key | load error (uniqueness) |
| `upsert_dedup_by_row_id`, exact duplicate | `1,a;2,b` |
| `upsert_dedup_by_row_id`, different rows share a key | `1,z;2,b` (last row wins) |
| `drop`, exact duplicate | `1,a;2,b` |
| `drop`, different rows share a key | `1,a;2,b` (first row wins) |

```bash
SPICED=target/release/spiced .cursor/skills/verify-arrow-on-conflict-dedup/run.sh
```

Env: `SPICED` (binary), `ENGINES` (default `arrow duckdb`), `HTTP`/`FLIGHT`/`METRICS`
(listen addresses, default 127.0.0.1:1869x), `WORKDIR`, `KEEP=1` to leave spiced running.
Exit code 0 means every engine matched the table. Run it on the base binary first to capture
the before state, then on the branch build.
