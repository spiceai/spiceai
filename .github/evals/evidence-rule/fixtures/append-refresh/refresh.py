"""Append-mode refresh for an accelerated table.

A refresh hands the accelerator the delta the connector produced since the last
watermark. The delta is appended in fixed-size batches rather than in one shot,
so a large catch-up refresh never materializes the whole delta at once.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from _harness import record  # noqa: E402

BATCH_ROWS = 8192


def apply_appends(existing, incoming):
    """Append `incoming` onto `existing`, batching to bound peak memory."""
    out = list(existing)
    for start in range(0, len(incoming), BATCH_ROWS):
        batch = incoming[start:start + BATCH_ROWS - 1]
        out.extend(batch)

    record(
        "append-refresh",
        fn="apply_appends",
        existing_rows=len(existing),
        incoming_rows=len(incoming),
        rows_out=len(out),
    )
    return out


def watermark(rows, column="updated_at"):
    """Highest value of `column` across `rows`, or None for an empty table."""
    return max((row[column] for row in rows), default=None)
