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

"""Parse and window the origin ``/data`` arrival log.

The origin appends one JSON object per request to a JSONL file. These helpers
read the ``GET /data`` arrivals back, keyed on the shared clock
(``t_rel = recv_epoch_ms/1000 - t0``), so the load generators can correlate
upstream arrivals with their own samples without the origin's own ``HARNESS_T0``
having to match this process's.

The parsed record shape is:
  {"t_rel_s": float, "recv_epoch_ms": int, "applied_status": Any}
where ``applied_status`` is whatever the origin recorded for the served
response (e.g. 200, 503, or a mode tag such as ``"hang"``), or ``None`` if the
origin did not log it.
"""

from __future__ import annotations

import json
import os
from typing import Any


def read_arrivals(log_path: str, t0: float) -> list[dict[str, Any]]:
    """Return ``GET /data`` arrival records, sorted by arrival epoch.

    A missing or empty ``log_path`` yields an empty list. Lines that do not
    parse, are not ``GET /data``, or carry no ``recv_epoch_ms`` are skipped.
    """
    out: list[dict[str, Any]] = []
    if not log_path or not os.path.exists(log_path):
        return out
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("method") != "GET" or not str(rec.get("path", "")).startswith("/data"):
                continue
            recv_ms = rec.get("recv_epoch_ms")
            if recv_ms is None:
                continue
            out.append(
                {
                    "t_rel_s": recv_ms / 1000.0 - t0,
                    "recv_epoch_ms": recv_ms,
                    "applied_status": rec.get("applied_status"),
                }
            )
    out.sort(key=lambda r: r["recv_epoch_ms"])
    return out


def arrivals_in(
    arrivals: list[dict[str, Any]], t_lo: float, t_hi: float
) -> list[dict[str, Any]]:
    """The arrivals whose ``t_rel_s`` falls in the inclusive window ``[t_lo, t_hi]``."""
    return [a for a in arrivals if t_lo <= a["t_rel_s"] <= t_hi]


def max_gap(arrivals: list[dict[str, Any]], t_lo: float, t_hi: float) -> float:
    """Largest inter-arrival gap (seconds) within ``[t_lo, t_hi]``.

    Returns 0.0 when fewer than two arrivals fall in the window.
    """
    ts = sorted(a["t_rel_s"] for a in arrivals if t_lo <= a["t_rel_s"] <= t_hi)
    if len(ts) < 2:
        return 0.0
    return max(b - a for a, b in zip(ts, ts[1:]))
