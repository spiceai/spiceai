#!/usr/bin/env python3
"""Write the 4-row overflow table used by the verify skill."""

from __future__ import annotations

import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit("pyarrow is required to write overflow.parquet") from exc

out = sys.argv[1] if len(sys.argv) > 1 else "overflow.parquet"
table = pa.table(
    {
        "id": pa.array([1, 2, 3, 4], pa.int64()),
        "big": pa.array(
            [2**63 - 1, 2**63 - 2, 1, 2],
            pa.int64(),
        ),
        "i": pa.array(
            [2**31 - 1, 2**31 - 2, 1, 2],
            pa.int32(),
        ),
    }
)
pq.write_table(table, out)
print(out, table.num_rows, table.schema)

# (k, v) rows for SUM(v), COUNT(DISTINCT k): group 1 is MAX+MAX, group 2
# cancels with -MAX so the true total is MAX. A second file has no cancel.
i64_max = 2**63 - 1
cancel_out = sys.argv[2] if len(sys.argv) > 2 else "cancel.parquet"
cancel = pa.table(
    {
        "k": pa.array([1, 1, 2], pa.int64()),
        "v": pa.array([i64_max, i64_max, -i64_max], pa.int64()),
    }
)
pq.write_table(cancel, cancel_out)
print(cancel_out, cancel.num_rows, cancel.schema)

ovf_out = sys.argv[3] if len(sys.argv) > 3 else "ovf.parquet"
ovf = pa.table(
    {
        "k": pa.array([1, 1, 2], pa.int64()),
        "v": pa.array([i64_max, i64_max, i64_max], pa.int64()),
    }
)
pq.write_table(ovf, ovf_out)
print(ovf_out, ovf.num_rows, ovf.schema)
