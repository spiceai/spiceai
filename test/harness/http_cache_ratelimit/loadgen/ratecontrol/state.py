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

"""The shared cluster rate-control state object, read back as evidence.

One JSON object per origin holds every replica's lease for the last 60
windows, so for a run shorter than that the final snapshot is the whole
history: how the replicas split each window, and what upstream outcomes each
published.

**The throttled budget is not in the file.** Each replica derives
`effective_burst` from the shared counts and holds it for the life of the
window; only the counts are shared. What the file still bounds is the
*configured* burst: a grant is capped at `effective_burst - granted_by_others`
and `effective_burst` is itself capped at `burst_per_window`, so
`sum(granted) <= burst_per_window` holds in every window whatever the
coefficient is doing.

So the coefficient is checked across two sources instead of within one: it is
recomputed from the `ok`/`failed` counts in this file and compared with the
`adaptive_admission_ratio` the replicas published to their own `/metrics`.
Agreement there means the published formula, the shared counts and the
runtime's own telemetry all say the same thing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterable, Sequence

from .rates import Arrival

#: Back to 3: `effective_burst` left the wire format, and `ok`/`failed` are
#: optional additive fields that an older reader ignores, so no bump is owed.
SCHEMA_VERSION = 3
def min_age_for(replicas: int) -> int:
    """How many of the youngest source windows the deciding replica cannot see.

    A replica fixes a window's budget one window ahead, and a source window is
    evidence only once written back past its own end. With peers that takes an
    extra window, because the window is not final until *every* replica has
    written it back; a lone replica's own write-back is the only one owed, so
    its evidence is one window fresher.

    Measured, over the throttled span of three runs, as the gap between the
    coefficient the shared counts imply and the one the replicas published:

        offset   1 replica   2 replicas   3 replicas
             0       0.126        0.203        0.227
             1     **0.031**      0.121        0.139
             2       0.122      **0.009**    **0.000**
             3       0.321        0.204        0.190
    """
    return 1 if replicas <= 1 else 2


LOOKBACK_WINDOWS = 5



@dataclass(frozen=True)
class Lease:
    instance: str
    granted: int
    consumed: int
    attempted: int
    ok: int | None
    failed: int | None
    expires_at_ms: int
    updated_at_ms: int

    @property
    def is_final(self) -> bool:
        """Whether this lease's counts are evidence.

        A lease written back after its own window ended carries a timestamp
        past the window end, and that timestamp is the proof the counts are
        final. A lease that reported no outcome at all is absent, not zero.
        """
        return (
            self.ok is not None
            and self.failed is not None
            and self.updated_at_ms > self.expires_at_ms
        )


@dataclass(frozen=True)
class Window:
    window_id: int
    budget_remaining: int
    leases: tuple[Lease, ...]

    @property
    def granted(self) -> int:
        return sum(lease.granted for lease in self.leases)

    @property
    def ok(self) -> int:
        return sum(lease.ok or 0 for lease in self.leases)

    @property
    def failed(self) -> int:
        return sum(lease.failed or 0 for lease in self.leases)

    def final_counts(self) -> tuple[int, int]:
        """(requests, accepts) this window holds final evidence for."""
        requests = accepts = 0
        for lease in self.leases:
            if not lease.is_final:
                continue
            requests += (lease.ok or 0) + (lease.failed or 0)
            accepts += lease.ok or 0
        return requests, accepts


@dataclass(frozen=True)
class Limiter:
    key: str
    burst_per_window: int
    windows: dict[int, Window]


@dataclass(frozen=True)
class SharedState:
    schema_version: int
    window_ms: int
    limiters: tuple[Limiter, ...]

    @staticmethod
    def load(path: str) -> "SharedState":
        raw = json.load(open(path, encoding="utf-8"))
        limiters = []
        for key, limiter in raw["limiters"].items():
            windows = {}
            for window_id, window in limiter["windows"].items():
                leases = tuple(
                    Lease(
                        instance=instance,
                        granted=lease["granted"],
                        consumed=lease["consumed"],
                        attempted=lease["attempted"],
                        ok=lease.get("ok"),
                        failed=lease.get("failed"),
                        expires_at_ms=lease["expires_at_unix_ms"],
                        updated_at_ms=lease["updated_at_unix_ms"],
                    )
                    for instance, lease in sorted(window["leases"].items())
                )
                windows[int(window_id)] = Window(
                    window_id=int(window_id),
                    budget_remaining=window.get("budget_remaining", 0),
                    leases=leases,
                )
            limiters.append(
                Limiter(key=key, burst_per_window=limiter["burst_per_window"], windows=windows)
            )
        return SharedState(
            schema_version=raw["schema_version"],
            window_ms=raw["window_ms"],
            limiters=tuple(limiters),
        )

    @property
    def instances(self) -> set[str]:
        return {
            lease.instance
            for limiter in self.limiters
            for window in limiter.windows.values()
            for lease in window.leases
        }


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class Coefficients:
    """The admission coefficient each window's evidence implies."""

    by_window: dict[int, float] = field(default_factory=dict)

    def over(self, window_ids: Iterable[int]) -> list[float]:
        return [self.by_window[w] for w in window_ids if w in self.by_window]


def median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def coefficients(
    limiter: Limiter,
    failure_threshold: float,
    half_life_windows: int = 1,
    min_age: int = 2,
) -> Coefficients:
    """Recompute the admission coefficient per window from the shared counts.

        age         = t - s - 1                       (whole windows)
        weight      = 0.5 ** (age / half_life_windows)
        requests    = sum_age weight * sum_leases (ok + failed)
        accepts     = sum_age weight * sum_leases  ok
        K           = 1 / (1 - failure_threshold)
        coefficient = min( (K*accepts + 1) / (requests + 1), 1 )

    `min_age` drops the youngest source windows, which the replica deciding a
    window's budget could not yet see: it fixes the budget one window ahead, and
    a source window is evidence only once written back past its own end. The
    weight is always the absolute age, so skipping a window contributes nothing
    rather than shifting the decay curve.
    """
    k = 1.0 / (1.0 - failure_threshold)
    out = Coefficients()
    for target in sorted(limiter.windows):
        requests = accepts = 0.0
        for age in range(min_age, LOOKBACK_WINDOWS):
            source = limiter.windows.get(target - age - 1)
            if source is None:
                continue
            window_requests, window_accepts = source.final_counts()
            weight = 0.5 ** (age / max(1, half_life_windows))
            requests += weight * window_requests
            accepts += weight * window_accepts
        out.by_window[target] = min((k * accepts + 1.0) / (requests + 1.0), 1.0)
    return out


def check_state(
    state: SharedState,
    arrivals: Iterable[Arrival],
    failure_threshold: float | None,
    half_life_windows: int = 1,
    published_ratio: dict[int, float] | None = None,
    replicas: int = 2,
) -> list[Check]:
    """Every invariant the shared state object can settle.

    `published_ratio` maps a unix second to the `adaptive_admission_ratio` a
    replica published then, which is what the recomputed coefficient is
    compared against.
    """
    checks: list[Check] = []
    arrivals = list(arrivals)

    checks.append(
        Check(
            "state: schema version is current",
            state.schema_version == SCHEMA_VERSION,
            f"schema_version={state.schema_version}, expected {SCHEMA_VERSION}",
        )
    )

    for limiter in state.limiters:
        prefix = f"state[{limiter.key}]"
        windows = limiter.windows
        window_ids = sorted(windows)
        if not window_ids:
            checks.append(Check(f"{prefix}: has windows", False, "the limiter holds no window"))
            continue

        # The budget is derived per replica and never written back, so the file
        # cannot say what the throttled budget for a window was. What it can
        # say is that no window ever handed out more than the CONFIGURED burst,
        # which is the bound the design actually guarantees: a grant is capped
        # at `effective_burst - granted_by_others`, and `effective_burst` is
        # capped at `burst_per_window`.
        oversold = [
            (window.window_id, window.granted)
            for window in windows.values()
            if window.granted > limiter.burst_per_window
        ]
        checks.append(
            Check(
                f"{prefix}: sum(granted) <= burst_per_window",
                not oversold,
                f"{len(oversold)} of {len(windows)} windows over {limiter.burst_per_window}:"
                f" {oversold[:3]}"
                if oversold
                else f"all {len(windows)} windows within the configured burst",
            )
        )

        per_window: dict[int, int] = {}
        for arrival in arrivals:
            key = arrival.epoch_ms // state.window_ms
            per_window[key] = per_window.get(key, 0) + 1
        inner = window_ids[1:-1]
        pair_over = [
            (w, per_window.get(w, 0) + per_window.get(w + 1, 0))
            for w in inner[:-1]
            if per_window.get(w, 0) + per_window.get(w + 1, 0) > 2 * limiter.burst_per_window + 1
        ]
        checks.append(
            Check(
                f"{prefix}: arrivals within the configured budget over adjacent windows",
                not pair_over,
                f"{len(pair_over)}/{max(0, len(inner) - 1)} adjacent window pairs over"
                f" {2 * limiter.burst_per_window + 1}" + (f": {pair_over[:3]}" if pair_over else ""),
            )
        )

        # Published outcomes against the origin's own view. An acquire timeout
        # never reaches the origin, so it must never appear here however many
        # queries were refused a permit -- that would show as published
        # failures the origin never served.
        #
        # Checked as a total and as a per-window bound, not per-window
        # equality. A request near a window boundary can be counted in window N
        # by the origin and N+1 by the replica, so with a partial error rate
        # adjacent windows come out +1/-1 against each other while the total is
        # exact. A leak would be a one-way excess and survives both.
        published_failed = served_failed = 0
        worst_window_delta = 0
        for window_id, window in windows.items():
            arrived = [a for a in arrivals if a.epoch_ms // state.window_ms == window_id]
            window_served = sum(1 for a in arrived if a.failed)
            published_failed += window.failed
            served_failed += window_served
            worst_window_delta = max(worst_window_delta, abs(window.failed - window_served))
        checks.append(
            Check(
                f"{prefix}: published `failed` is the origin's failures",
                published_failed == served_failed and worst_window_delta <= 1,
                f"total published {published_failed} vs origin {served_failed};"
                f" worst single window differs by {worst_window_delta}",
            )
        )

        if failure_threshold is None:
            continue

        min_age = min_age_for(replicas)
        implied = coefficients(limiter, failure_threshold, half_life_windows, min_age)
        throttled = [w for w, c in implied.by_window.items() if c < 0.99]
        if published_ratio is None or state.window_ms != 1000:
            continue
        if not throttled:
            # Nothing to compare: a healthy origin implies 1.0 everywhere, and
            # so does any formula, so agreement would mean nothing.
            checks.append(
                Check(
                    f"{prefix}: the shared counts imply no throttling",
                    all(ratio > 0.99 for ratio in published_ratio.values()),
                    f"{len(published_ratio)} published ratios,"
                    f" lowest {min(published_ratio.values(), default=1.0):.3f}",
                )
            )
            continue

        # Window ids are unix seconds here, so a published ratio at second S is
        # comparable with the coefficient for window S. The replicas publish at
        # their own tick phase, so compare the distributions over the throttled
        # span rather than second by second.
        predicted = implied.over(throttled)
        observed = [ratio for second, ratio in published_ratio.items() if second in set(throttled)]
        predicted_median, observed_median = median(predicted), median(observed)
        checks.append(
            Check(
                f"{prefix}: the coefficient the shared counts imply is the one the replicas published",
                predicted_median is not None
                and observed_median is not None
                and abs(predicted_median - observed_median) <= 0.1,
                f"over {len(throttled)} throttled windows at min_age={min_age}: implied median"
                f" {predicted_median if predicted_median is None else round(predicted_median, 3)},"
                f" published median"
                f" {observed_median if observed_median is None else round(observed_median, 3)}"
                f" ({len(observed)} scrapes)",
            )
        )
    return checks


def timeline_rows(state: SharedState, arrivals: Iterable[Arrival]) -> list[str]:
    """One printable row per window: budget, split, outcomes, arrivals."""
    per_window: dict[int, int] = {}
    for arrival in arrivals:
        key = arrival.epoch_ms // state.window_ms
        per_window[key] = per_window.get(key, 0) + 1
    rows = [f"schema_version={state.schema_version} window_ms={state.window_ms}"]
    for limiter in state.limiters:
        rows.append(f"\nlimiter {limiter.key}  burst_per_window={limiter.burst_per_window}")
        header = (
            f"{'window':>12}{'granted':>9}{'remaining':>11}{'consumed':>10}"
            f"{'ok':>5}{'failed':>8}{'arrivals':>10}  per-replica granted"
        )
        rows.append(header)
        rows.append("-" * len(header))
        for window_id in sorted(limiter.windows):
            window = limiter.windows[window_id]
            per_replica = " ".join(
                f"{lease.instance[:8]}={lease.granted}" for lease in window.leases
            )
            rows.append(
                f"{window_id:>12}{window.granted:>9}{window.budget_remaining:>11}"
                f"{sum(l.consumed for l in window.leases):>10}"
                f"{window.ok:>5}{window.failed:>8}{per_window.get(window_id, 0):>10}  {per_replica}"
            )
    return rows
