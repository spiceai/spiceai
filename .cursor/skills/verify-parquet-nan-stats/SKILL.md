---
name: verify-parquet-nan-stats
description: Verify federated parquet NaN MAX/pruning against a running spiced. Use for the listing-table parquet stats shortcut and row-group pruning of Float16/32/64 columns.
---

# Verify federated parquet NaN stats and pruning

Parquet writers omit `NaN` from float min/max. A running `spiced` over a federated
`file:` parquet dataset must not answer `MAX(f)` from those stats or prune
`f = NaN` / `f > inf`. HTTP JSON renders `NaN`/`inf` as null; use CSV or Flight.

## Dataset

Write a 20-row parquet (same `id`/`f` layout as the reporter's `tricky.parquet`):

- `id` 1..=20
- `f` Float64: `1.5, NaN, +inf, -inf, -0, 0, NULL, 1e308, -1e-308, 2.5, 3.5, NaN, 0.1, 0.2, 0.30000000000000004, NULL, 1, 1, -2.5, 100`
- `k` Int64 `0..20` (so `MAX(k)` can still use the stats shortcut)
- `amt` Decimal128(10,2) `0, 100, …, 1900`

NaN is at id 2 and 12. Prefer a local venv + `pyarrow`; keep the file out of the
repo (`*.parquet` and root `spicepod.yaml` are gitignored).

```python
import math
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

out = Path("data/tricky.parquet")
out.parent.mkdir(parents=True, exist_ok=True)
n = 20
f = [1.5, float("nan"), float("inf"), float("-inf"), -0.0, 0.0, None, 1e308, -1e-308, 2.5,
     3.5, float("nan"), 0.1, 0.2, 0.30000000000000004, None, 1.0, 1.0, -2.5, 100.0]
t = pa.table({
    "id": pa.array(range(1, n + 1), pa.int64()),
    "k": pa.array(range(n), pa.int64()),
    "amt": pa.array([None if v is None else v for v in range(0, n * 100, 100)], pa.decimal128(10, 2)),
    "f": pa.array(f, pa.float64()),
})
pq.write_table(t, out)
```

## Spicepod

Federated parquet plus an Arrow acceleration of the same file (oracle). Use a
temp directory; do not write `spicepod.yaml` at the repo root.

```yaml
version: v1
kind: Spicepod
name: parquet-nan-stats
datasets:
  - from: file:data/tricky.parquet
    name: t_fed
    params:
      file_format: parquet
  - from: file:data/tricky.parquet
    name: t_arrow
    params:
      file_format: parquet
    acceleration:
      enabled: true
      engine: arrow
```

## Start `spiced`

Pick unused ports (defaults 8090 / 50051). From the spicepod directory:

```bash
spiced --http 127.0.0.1:18490 --flight 127.0.0.1:18491 --metrics 127.0.0.1:18492
```

Poll `http://127.0.0.1:18490/v1/ready` until it returns ready. Do not `sleep` as
the only readiness wait.

## Queries (CSV)

```bash
curl -sS -XPOST -H 'Accept: text/csv' -H 'Content-Type: text/plain' \
  127.0.0.1:18490/v1/sql --data-binary "$SQL"
```

Required results on `t_fed` (must match `t_arrow`):

| SQL | Expected |
| --- | --- |
| `SELECT MAX(f) FROM t_fed` | `NaN` |
| `SELECT MAX(f) FROM t_fed WHERE f IS NOT NULL` | `NaN` |
| `SELECT COUNT(*) FROM t_fed WHERE f = CAST('NaN' AS DOUBLE)` | `2` |
| `SELECT id FROM t_fed WHERE f > CAST('inf' AS DOUBLE) ORDER BY id` | `2`, `12` |
| `SELECT MIN(f) FROM t_fed` | `-inf` |
| `SELECT MAX(k) FROM t_fed` | `19` |
| `EXPLAIN SELECT MAX(k) FROM t_fed` | contains `PlaceholderRowExec` |
| `EXPLAIN SELECT MAX(amt) FROM t_fed` | contains `PlaceholderRowExec` |
| `EXPLAIN SELECT MAX(f) FROM t_fed` | does **not** contain `PlaceholderRowExec` folding `inf` |

## Trunk vs branch

Run the same spicepod and queries against a `spiced` built from `trunk` and from
the branch, same ports protocol (stop the first process by PID before starting
the second). Capture SHA, command, and verbatim CSV / EXPLAIN for both. Trunk
`MAX(f)` is `inf` and `f = NaN` counts `0`; the branch must match the table
above.

Leave `spiced` running after the branch pass so a reviewer can re-query.
