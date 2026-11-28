#!/usr/bin/env python3
"""Write the three-row DECIMAL(38,10) fixture used to verify decimal /."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "tricky.parquet")
    table = pa.table(
        {
            "id": pa.array([1, 5, 8], pa.int64()),
            "amt": pa.array(
                [
                    Decimal("1.0000000001"),
                    Decimal("99999999999999999999.9999999999"),
                    Decimal("12345678901234567890.1234567890"),
                ],
                pa.decimal128(38, 10),
            ),
            "price": pa.array(
                [Decimal("0.10"), Decimal("19.99"), Decimal("0.01")],
                pa.decimal128(10, 2),
            ),
            "n": pa.array([7, 7, 7], pa.int64()),
        }
    )
    pq.write_table(table, out)
    print(f"wrote {out} rows={table.num_rows} schema={table.schema}")


if __name__ == "__main__":
    main()
