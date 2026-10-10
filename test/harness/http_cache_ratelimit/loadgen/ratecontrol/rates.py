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

"""Arrival rates at the origins, sliced by origin, dataset and replica.

The origins' own arrival logs are the measurement. A rate limit is a claim
about what reaches the upstream, so counting what the load generator *sent*,
or what it got back, would measure the generator instead.

Two decisions shape every number here:

* **Buckets are whole unix seconds.** The rate-control window is
  `epoch_ms // window_ms`, so a bucket counted from the start of a phase would
  straddle two windows and report a peak that was never spent.
* **The statistic is p99 and peak, never the mean.** A limit is a claim about
  the worst second; a mean hides exactly the breach the limit exists to
  prevent. Empty seconds inside the measured range are counted, so a throttled
  phase is not flattered by dropping its idle seconds.
"""

from __future__ import annotations

import json
import math
import os
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class Arrival:
    """One request that reached an origin."""

    epoch_ms: int
    origin: str
    path: str
    dataset: str
    replica: str
    status: str  # "200", "503", "hang", "refuse", ...

    @property
    def failed(self) -> bool:
        """Whether the controller would record this as a failure.

        408/429/5xx, a connection abort and a hang past the client timeout all
        record `RequestOutcome::Failure`; a 2xx records success. Anything else
        (a non-retryable 4xx) is recorded as neither.
        """
        if self.status in ("hang", "refuse"):
            return True
        if not self.status.isdigit():
            return False
        code = int(self.status)
        return code in (408, 429) or 500 <= code <= 599


@dataclass(frozen=True)
class Slice:
    """Which arrivals a bound is about. `None` means "every value"."""

    origin: str | None = None
    dataset: str | None = None
    replica: str | None = None

    def matches(self, arrival: Arrival) -> bool:
        return (
            (self.origin is None or arrival.origin == self.origin)
            and (self.dataset is None or arrival.dataset == self.dataset)
            and (self.replica is None or arrival.replica == self.replica)
        )

    def label(self) -> str:
        parts = [
            f"{name}={value}"
            for name, value in (
                ("origin", self.origin),
                ("dataset", self.dataset),
                ("replica", self.replica),
            )
            if value is not None
        ]
        return ", ".join(parts) if parts else "all traffic"


@dataclass(frozen=True)
class RateStats:
    """Per-second arrival counts over a window, summarised by percentile."""

    seconds: int
    total: int
    p50: float
    p99: float
    peak: int

    def as_dict(self) -> dict[str, float | int]:
        return {
            "seconds": self.seconds,
            "total": self.total,
            "p50_rps": self.p50,
            "p99_rps": self.p99,
            "peak_rps": self.peak,
        }

    def __str__(self) -> str:
        return f"p50={self.p50:g} p99={self.p99:g} peak={self.peak} total={self.total}"


def percentile(values: Sequence[int], q: float) -> float:
    """Nearest-rank percentile. `q` in [0, 1]."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(q * len(ordered)))
    return float(ordered[rank - 1])


def rate_stats(arrivals: Iterable[Arrival], start_ms: int, end_ms: int) -> RateStats:
    """Per-second arrival statistics over `[start_ms, end_ms)`.

    Only whole unix seconds fully inside the range are counted, so a partial
    second at either edge cannot read as a quiet one.
    """
    if end_ms <= start_ms:
        return RateStats(0, 0, 0.0, 0.0, 0)
    first_second = -(-start_ms // 1000)
    last_second = end_ms // 1000  # exclusive
    if last_second <= first_second:
        return RateStats(0, 0, 0.0, 0.0, 0)
    buckets: Counter[int] = Counter()
    for arrival in arrivals:
        second = arrival.epoch_ms // 1000
        if first_second <= second < last_second:
            buckets[second] += 1
    counts = [buckets.get(second, 0) for second in range(first_second, last_second)]
    return RateStats(
        seconds=len(counts),
        total=sum(counts),
        p50=percentile(counts, 0.50),
        p99=percentile(counts, 0.99),
        peak=max(counts) if counts else 0,
    )


def read_arrivals(log_path: str, origin: str, path_to_dataset: dict[str, str]) -> list[Arrival]:
    """Parse one origin's JSONL arrival log.

    `path_to_dataset` maps the request path to the dataset that owns it; a path
    with no dataset is recorded under its own name, so a stray request shows up
    rather than being silently dropped.
    """
    out: list[Arrival] = []
    if not log_path or not os.path.exists(log_path):
        return out
    with open(log_path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("method") != "GET":
                continue
            request_path = str(row.get("path", ""))
            if not request_path.startswith("/data"):
                continue
            epoch_ms = row.get("recv_epoch_ms")
            if epoch_ms is None:
                continue
            out.append(
                Arrival(
                    epoch_ms=int(epoch_ms),
                    origin=origin,
                    path=request_path,
                    dataset=path_to_dataset.get(request_path, f"?{request_path}"),
                    replica=str(row.get("replica", "") or "?"),
                    status=str(row.get("applied_status", "")),
                )
            )
    out.sort(key=lambda arrival: arrival.epoch_ms)
    return out


def max_rolling(arrivals: Iterable[Arrival], start_ms: int, end_ms: int, seconds: int = 5) -> int:
    """The most arrivals any `seconds` consecutive whole seconds carried.

    A rate limit is a claim about the sustained rate, and the window has to be
    long enough for that claim to be the one under test. A token bucket holds
    slack: a wall-clock second can carry its own budget plus a token due at the
    boundary plus whatever request crossed into it, so short windows run a
    little hot without the rate ever being exceeded. Measured against a 20 rps
    limit, a single second reaches 21 and two adjacent seconds 42, while five
    seconds stay within 101 of 100 in every scenario.
    """
    counts = per_second(arrivals)
    first = -(-start_ms // 1000)
    last = end_ms // 1000
    best = 0
    for second in range(first, max(first, last - seconds + 1)):
        best = max(best, sum(counts.get(second + offset, 0) for offset in range(seconds)))
    return best


def per_second(arrivals: Iterable[Arrival]) -> dict[int, int]:
    """Arrivals per whole unix second, for the seconds that had any."""
    counts: Counter[int] = Counter()
    for arrival in arrivals:
        counts[arrival.epoch_ms // 1000] += 1
    return dict(counts)


def first_second_where(
    arrivals: Iterable[Arrival],
    from_ms: int,
    to_ms: int,
    predicate,
    sustained_s: int = 3,
) -> int | None:
    """When the rate first *stays* at a level, as an offset from `from_ms`.

    `sustained_s` consecutive seconds must satisfy `predicate`, and the offset
    returned is the first of them. One second is not enough: a request that
    parks for its whole timeout leaves quiet seconds between bursts, and a
    single-second test would read that sawtooth as a throttle that never
    happened.
    """
    counts = per_second(arrivals)
    first = -(-from_ms // 1000)
    last = to_ms // 1000
    run = 0
    for second in range(first, last):
        if predicate(counts.get(second, 0)):
            run += 1
            if run >= sustained_s:
                return second - first - (sustained_s - 1)
        else:
            run = 0
    return None


def select(arrivals: Iterable[Arrival], where: Slice) -> list[Arrival]:
    return [arrival for arrival in arrivals if where.matches(arrival)]


def status_counts(arrivals: Iterable[Arrival]) -> dict[str, int]:
    return dict(Counter(arrival.status for arrival in arrivals))
