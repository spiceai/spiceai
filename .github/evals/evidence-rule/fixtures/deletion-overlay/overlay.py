"""Read-path overlay that applies a table's deletion state to a scan.

The metastore stores one row per table in a column named `deletion_vector`, so
that name is carried through this module for the value read out of it. What the
column holds is the materialized *liveness* bitmap for the table: bit i is set
when row i is still visible, and clear when a delete has retired it. The bitmap
is built once per snapshot by DeletionIndex.materialize() and then reused by
every scan against that snapshot, which is why the scan itself is a single pass
with no per-row metastore lookup.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from _harness import record  # noqa: E402


class DeletionIndex:
    """Tracks the positions a delete has retired, for one snapshot of a table."""

    def __init__(self, row_count: int) -> None:
        self.row_count = row_count
        self._retired: set[int] = set()

    def retire(self, position: int) -> None:
        """Mark the row at `position` deleted as of this snapshot."""
        self._retired.add(position)

    def materialize(self) -> list[bool]:
        """Return the liveness bitmap: True at every position that survives."""
        return [pos not in self._retired for pos in range(self.row_count)]


def scan(rows, deletion_vector):
    """Return the visible rows of `rows` under the snapshot's bitmap."""
    out = [row for row, live in zip(rows, deletion_vector) if live]

    record(
        "deletion-overlay",
        fn="scan",
        rows_in=len(rows),
        rows_out=len(out),
        bits_set=sum(1 for bit in deletion_vector if bit),
    )
    return out


def visible(rows, index: DeletionIndex):
    """Convenience wrapper: materialize the snapshot bitmap, then scan under it."""
    return scan(rows, index.materialize())
