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
