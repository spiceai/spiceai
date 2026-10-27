---
name: verify-retention-time-filters
description: Check retention_period and refresh_data_window time filters against a running spiced. Use after changing TimestampFilterConvert, retention deletes, or the refresh window filter, to confirm ISO 8601 string columns compare as instants, NULL-time rows survive a retention-driven load, and a DATE time_partition_column loads the whole 24h window on Arrow, DuckDB and Cayenne.
---

# Verify retention and refresh-window time filters

## What it checks

`gen.py` writes one parquet file with data relative to now: 48h of rows at
1 per minute, 1 per second around `now - 24h`, 3 future rows and 4 NULL-time
rows. `ts_iso` (`...Z`), `ts_iso_off` (mixed UTC offsets) and `ts_iso_naive`
(`YYYY-MM-DD HH:MM:SS`) hold the same instant as `ts`.

For each engine in `ENGINES` (default `arrow duckdb cayenne`):

1. **ISO 8601 retention.** One dataset per string column, `time_format: iso8601`,
   `retention_period: 24h`. No row older than `now - 24h` stays, the live count is
   within 10 rows of the parquet, and the 4 NULL-time rows remain. A text compare
   of the cutoff keeps or drops rows by string order. On Cayenne it failed with
   `LargeUtf8 >= Utf8`.
2. **NULL time with retention only.** `retention_period: 24h` without
   `refresh_data_window` keeps the 4 NULL-time rows.
3. **DATE partition window.** `time_partition_column: ts_date`,
   `time_partition_format: date`, `refresh_data_window: 24h` loads the same rows
   (within 10) as a control with no partition column, not only rows since UTC midnight.

## Run

Needs a `spiced` built with `duckdb` and `cayenne`, `curl`, and `python3` with
`pyarrow`.

```bash
SPICED=/path/to/spiced .cursor/skills/verify-retention-time-filters/run.sh
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `SPICED` | `spiced` | binary under test |
| `PY` | `python3` | interpreter with `pyarrow` |
| `HP` / `FP` | `18190` / `50161` | HTTP / Flight ports (metrics at `HP+1000`) |
| `ENGINES` | `arrow duckdb cayenne` | engines to cover |
| `KEEP` | `0` | `1` keeps the workdir with each `spiced.log` |

Each check prints `PASS` or `FAIL`. The script ends with `ALL PASS` (exit 0) or
`SOME CHECKS FAILED` (exit 1).
