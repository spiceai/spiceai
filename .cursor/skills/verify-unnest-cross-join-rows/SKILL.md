---
name: verify-unnest-cross-join-rows
description: Verify that joins against an UNNEST subquery return every row on DuckDB and SQLite accelerations, as on Arrow. Use when changing the DataFusion unparser's Unnest or Projection handling, or which plans the DuckDB or SQLite federation pushes down.
---

# Verify UNNEST joins on accelerations

A `CROSS JOIN` against an `UNNEST` subquery whose columns the query never reads used to be sent
to DuckDB and SQLite as `CROSS JOIN (SELECT 1)`, so the join returned 1/N of its rows with no error.

## Run

```bash
SPICED=/path/to/spiced .cursor/skills/verify-unnest-cross-join-rows/run.sh
```

- Needs `python3` with `pyarrow`, and `curl`. Ports 18790-18792 (`HTTP`, `FLIGHT`, `METRICS` override them).
- `KEEP=1` keeps the work directory with `spiced.log`.

## Reading the output

Each check runs on `t_arrow`, `t_duckdb` and `t_sqlite` over the same 1000 rows and must return
the expected count on all three. `RESULT: PASS` means every check passed; the script exits 1 otherwise.

- The `generate_series` check is a control that passed before the fix.
- The `explain` line shows the SQL sent to DuckDB: it must keep `UNNEST(range(0, 100))`, not `(SELECT 1)`.
- SQLite has no `UNNEST`, so a plan with one runs in DataFusion over the SQLite scan rather than in SQLite.
