# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Assertion collection, the verdict ladder, and artifact writers.

An ``AssertionSet`` accumulates named pass/fail checks with a human-readable
detail string. ``decide_verdict`` applies the harness's fixed precedence:
correctness violations fail outright, an absent/blocked feature is reported
(never greened), otherwise the run passes only if every assertion holds.

``write_samples_csv`` and ``write_verdict`` centralize artifact writing so
every phase gets the same format — and the same empty-input guard, so a run
that collected no samples writes an empty file instead of crashing.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict
from typing import Any


class AssertionSet:
    """Ordered collection of ``{"name", "pass", "detail"}`` oracle checks."""

    def __init__(self) -> None:
        self._items: list[dict[str, Any]] = []

    def add(self, name: str, passed: bool, detail: str) -> None:
        self._items.append({"name": name, "pass": bool(passed), "detail": detail})

    def all_pass(self) -> bool:
        return all(a["pass"] for a in self._items)

    def as_list(self) -> list[dict[str, Any]]:
        return list(self._items)

    def __iter__(self) -> Any:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)


def decide_verdict(
    *,
    correctness_ok: bool,
    assertions: Iterable[dict[str, Any]],
    blocked: bool = False,
) -> tuple[str, int]:
    """Map the run outcome to ``(verdict, exit_code)``.

    Precedence, highest first:
      - a correctness violation -> ``("FAIL", 1)``
      - an absent/blocked feature -> ``("BLOCKED", 2)`` (reported, never greened)
      - every assertion holds -> ``("PASS", 0)``
      - otherwise -> ``("FAIL", 1)``
    """
    if not correctness_ok:
        return "FAIL", 1
    if blocked:
        return "BLOCKED", 2
    if all(a["pass"] for a in assertions):
        return "PASS", 0
    return "FAIL", 1


def write_samples_csv(
    path: str,
    samples: Sequence[Any],
    sort_key: Callable[[Any], Any] | None = None,
) -> None:
    """Write dataclass ``samples`` to ``path`` as CSV.

    Columns are taken from the first sample's fields. An empty ``samples`` writes
    an empty file rather than raising, so a run that produced no samples still
    completes and reports its verdict.
    """
    rows = sorted(samples, key=sort_key) if sort_key is not None else list(samples)
    with open(path, "w", newline="", encoding="utf-8") as f:
        if not rows:
            return
        writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_dicts_csv(path: str, rows: Sequence[dict[str, Any]]) -> None:
    """Write a list of plain dicts to ``path`` as CSV (e.g. driver events).

    Columns are taken from the first row. An empty ``rows`` writes an empty file.
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        if not rows:
            return
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_verdict(path: str, verdict: dict[str, Any]) -> None:
    """Write the verdict object to ``path`` as pretty-printed JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2)
