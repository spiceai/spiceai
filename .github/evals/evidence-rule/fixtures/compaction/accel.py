"""Compaction for a small file-backed accelerator.

A table is stored as an ordered list of runs. Each run holds rows that are
unique by primary key. compact() folds every run into a single run so the read
path stops paying for the fan-out; where two runs carry the same primary key
the later run wins, which is how a CDC upsert supersedes the row it replaces.
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from _harness import record  # noqa: E402


def compact(runs):
    """Merge runs oldest-first into a single run, later runs winning."""
    rows_in = sum(len(run) for run in runs)

    merged = {}
    for run in runs:
        for row in run:
            merged[row["pk"]] = row
    out = list(merged.values())

    record(
        "compaction",
        fn="compact",
        runs=len(runs),
        rows_in=rows_in,
        rows_out=len(out),
        null_pk_rows_in=sum(1 for run in runs for row in run if row["pk"] is None),
    )
    return out


def load_table(path):
    """Read a run-per-line table file: 'pk,payload' rows, blank line ends a run."""
    runs, current = [], []
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            if current:
                runs.append(current)
                current = []
            continue
        pk, payload = line.split(",", 1)
        current.append({"pk": None if pk == "NULL" else int(pk), "payload": payload})
    if current:
        runs.append(current)
    return runs
