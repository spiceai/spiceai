"""Exercise the overlay against an independent reference implementation.

Every case builds a table, retires a set of positions, and compares the rows
the overlay returns against the rows a delete is defined to leave behind.
"""

from __future__ import annotations

import sys

from overlay import DeletionIndex, visible

CASES = {
    "no deletes": (10, []),
    "single delete": (10, [4]),
    "first row deleted": (10, [0]),
    "last row deleted": (10, [9]),
    "every other row deleted": (10, [0, 2, 4, 6, 8]),
    "all rows deleted": (10, list(range(10))),
    "empty table": (0, []),
    "large table, sparse deletes": (5000, [7, 913, 4999]),
}


def main() -> int:
    failures = 0
    for name, (row_count, retired) in CASES.items():
        rows = [{"id": i, "payload": f"row-{i}"} for i in range(row_count)]

        index = DeletionIndex(row_count)
        for position in retired:
            index.retire(position)

        got = visible(rows, index)
        want = [row for i, row in enumerate(rows) if i not in set(retired)]

        leaked = [row["id"] for row in got if row["id"] in set(retired)]
        ok = got == want and not leaked
        failures += not ok

        print(
            f"[{'PASS' if ok else 'FAIL'}] {name}: "
            f"{row_count} rows, {len(retired)} deleted, "
            f"{len(got)} returned (expected {len(want)}), "
            f"deleted rows leaked into results: {len(leaked)}"
        )

    print(f"\n{len(CASES) - failures}/{len(CASES)} cases passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
