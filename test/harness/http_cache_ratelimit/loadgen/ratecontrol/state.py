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
history: what budget each window was leased against, how the replicas split
it, and what upstream outcomes each published.

Three things are checked here that the origin's arrival log cannot settle on
its own:

  * the budget is never oversold (`sum(granted) <= effective_burst`);
  * the outcomes the replicas published are the outcomes the origin actually
    served -- in particular a permit-acquire timeout, which never reaches the
    origin, must never appear as a failure;
  * `effective_burst` is what the published formula says it should be.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterable

from .rates import Arrival

SCHEMA_VERSION = 4
#: `effective_burst` for window `t` is fixed one window ahead, and a source
#: window only becomes evidence once a replica has written it back past its own
#: end. The two youngest source windows are therefore invisible to the replica
#: that fixes the budget. Derived empirically: `--autofit` puts the best fit
#: here in every multi-replica run, by a wide margin over every other offset.
DEFAULT_MIN_AGE = 2
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
    effective_burst: int | None
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
                    effective_burst=window.get("effective_burst"),
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
class CoefficientFit:
    exact: int
    within_one: int
    total: int
    rows: list[tuple[int, float, float, float, int, int]] = field(default_factory=list)


def coefficient_fit(
    limiter: Limiter,
    failure_threshold: float,
    half_life_windows: int = 1,
    min_age: int = DEFAULT_MIN_AGE,
) -> CoefficientFit:
    """Re-derive `effective_burst` per window and compare with what was stored.

        age         = t - s - 1                       (whole windows)
        weight      = 0.5 ** (age / half_life_windows)
        requests    = sum_age weight * sum_leases (ok + failed)
        accepts     = sum_age weight * sum_leases  ok
        K           = 1 / (1 - failure_threshold)
        coefficient = min( (K*accepts + 1) / (requests + 1), 1 )
        effective   = clamp(round(burst * coefficient), 1, burst)

    `min_age` drops the youngest source windows, which the deciding replica
    could not yet see. It never shifts the decay curve: the weight is always
    the absolute age.
    """
    k = 1.0 / (1.0 - failure_threshold)
    fit = CoefficientFit(exact=0, within_one=0, total=0)
    for target in sorted(limiter.windows):
        stored = limiter.windows[target].effective_burst
        if stored is None:
            continue
        requests = accepts = 0.0
        for age in range(min_age, LOOKBACK_WINDOWS):
            source = limiter.windows.get(target - age - 1)
            if source is None:
                continue
            window_requests, window_accepts = source.final_counts()
            weight = 0.5 ** (age / max(1, half_life_windows))
            requests += weight * window_requests
            accepts += weight * window_accepts
        coefficient = min((k * accepts + 1.0) / (requests + 1.0), 1.0)
        predicted = min(max(round(limiter.burst_per_window * coefficient), 1), limiter.burst_per_window)
        fit.total += 1
        fit.exact += predicted == stored
        fit.within_one += abs(predicted - stored) <= 1
        fit.rows.append((target, requests, accepts, coefficient, predicted, stored))
    return fit


def autofit(limiter: Limiter, failure_threshold: float, half_life_windows: int = 1) -> dict[int, int]:
    """Exact-agreement count at each candidate `min_age`, so the offset comes
    out of the data instead of being assumed."""
    return {
        min_age: coefficient_fit(limiter, failure_threshold, half_life_windows, min_age).exact
        for min_age in range(5)
    }


def check_state(
    state: SharedState,
    arrivals: Iterable[Arrival],
    adaptive: bool,
    failure_threshold: float | None,
    half_life_windows: int = 1,
) -> list[Check]:
    """Every invariant the shared state object can settle on its own."""
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

        oversold = [
            (window.window_id, window.granted, window.effective_burst)
            for window in windows.values()
            if window.effective_burst is not None and window.granted > window.effective_burst
        ]
        detail = (
            f"{len(oversold)} of {len(windows)} windows oversold: {oversold[:3]}"
            if oversold
            else f"all {len(windows)} windows within budget"
        )
        checks.append(Check(f"{prefix}: sum(granted) <= effective_burst", not oversold, detail))

        # Arrivals per window against the budget that window was leased
        # against. A permit is spent when its window grants it and the request
        # lands milliseconds later, so one request can cross into the next
        # window; summing adjacent windows absorbs that, and a real oversell
        # would survive it.
        per_window: dict[int, int] = {}
        for arrival in arrivals:
            per_window[arrival.epoch_ms // state.window_ms] = (
                per_window.get(arrival.epoch_ms // state.window_ms, 0) + 1
            )
        # Summing two adjacent windows absorbs a request granted inside the
        # pair that landed inside it too. It cannot absorb one granted in the
        # window *before* the pair that landed in the pair's first window, so
        # the bound carries one request of slack for that leading edge. A real
        # oversell scales with the budget; this does not.
        inner = window_ids[1:-1]
        pair_over = []
        for window_id in inner[:-1]:
            budget = (windows[window_id].effective_burst or 0) + (
                windows.get(window_id + 1, windows[window_id]).effective_burst or 0
            )
            arrived = per_window.get(window_id, 0) + per_window.get(window_id + 1, 0)
            if arrived > budget + 1:
                pair_over.append((window_id, arrived, budget))
        worst = max(
            (
                per_window.get(w, 0)
                + per_window.get(w + 1, 0)
                - ((windows[w].effective_burst or 0) + (windows.get(w + 1, windows[w]).effective_burst or 0))
                for w in inner[:-1]
            ),
            default=0,
        )
        checks.append(
            Check(
                f"{prefix}: arrivals within the leased budget over adjacent windows",
                not pair_over,
                f"{len(pair_over)}/{max(0, len(inner) - 1)} adjacent window pairs over budget by"
                f" more than the one-request edge allowance; worst pair was {worst:+d}"
                + (f": {pair_over[:3]}" if pair_over else ""),
            )
        )

        # Published outcomes against the origin's own view, per window. An
        # acquire timeout never reaches the origin, so it must never appear
        # here however many queries were refused a permit.
        ok_mismatch = failed_mismatch = 0
        for window_id, window in windows.items():
            arrived = [a for a in arrivals if a.epoch_ms // state.window_ms == window_id]
            served_failed = sum(1 for a in arrived if a.failed)
            served_ok = sum(1 for a in arrived if a.status == "200")
            if adaptive:
                ok_mismatch += window.ok != served_ok
                failed_mismatch += window.failed != served_failed
        if adaptive:
            checks.append(
                Check(
                    f"{prefix}: published `failed` is the origin's failures",
                    failed_mismatch == 0,
                    f"{len(windows) - failed_mismatch}/{len(windows)} windows match"
                    f" (ok matches in {len(windows) - ok_mismatch}/{len(windows)})",
                )
            )
        else:
            published = sum(window.ok + window.failed for window in windows.values())
            checks.append(
                Check(
                    f"{prefix}: static mode publishes no outcome counts",
                    published == 0,
                    f"{published} outcome counts published in static mode",
                )
            )

        if adaptive and failure_threshold is not None:
            fit = coefficient_fit(limiter, failure_threshold, half_life_windows)
            fits = autofit(limiter, failure_threshold, half_life_windows)
            best = max(fits, key=lambda age: fits[age])
            # Two claims, and the structural one carries the weight. A
            # coefficient pinned at one end (a healthy origin, or one failing
            # every request) reproduces almost exactly; one hovering mid-range
            # moves every window, so which windows a replica had already
            # written back when it fixed a budget starts to matter and the
            # agreement loosens. What does NOT move is where the best fit sits:
            # a different formula, or a different decay, would shift it.
            # The offset only means anything where the coefficient actually
            # moved. An origin that never failed holds `effective_burst` at the
            # configured burst in every window, every offset reproduces it, and
            # "best" is whichever one the tie-break reached first. There the
            # meaningful claim is that the budget was never cut at all.
            throttled = any(
                window.effective_burst is not None
                and window.effective_burst < limiter.burst_per_window
                for window in windows.values()
            )
            if throttled:
                checks.append(
                    Check(
                        f"{prefix}: the formula's source-window offset is {DEFAULT_MIN_AGE}",
                        best == DEFAULT_MIN_AGE,
                        f"autofit {fits}, best min_age={best}",
                    )
                )
            else:
                checks.append(
                    Check(
                        f"{prefix}: a healthy origin keeps its full budget in every window",
                        True,
                        f"effective_burst == burst_per_window in all {len(windows)} windows,"
                        " so the source-window offset is not identifiable here",
                    )
                )
            checks.append(
                Check(
                    f"{prefix}: effective_burst matches the published formula",
                    fit.exact >= 0.5 * fit.total,
                    f"exact {fit.exact}/{fit.total}, within +/-1 {fit.within_one}/{fit.total}"
                    f" at min_age={DEFAULT_MIN_AGE}",
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
            f"{'window':>12}{'eff':>5}{'granted':>9}{'consumed':>10}"
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
                f"{window_id:>12}{window.effective_burst if window.effective_burst is not None else '-':>5}"
                f"{window.granted:>9}{sum(l.consumed for l in window.leases):>10}"
                f"{window.ok:>5}{window.failed:>8}{per_window.get(window_id, 0):>10}  {per_replica}"
            )
    return rows
