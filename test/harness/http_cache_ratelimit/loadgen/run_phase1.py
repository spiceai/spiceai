#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Phase 1 stale-if-error scenario driver, load generator and oracle.

One process owns the shared wall clock ``T0`` so every artifact lines up:
a background timeline thread steps the origin fault profile (healthy ->
faulting -> healthy) at wall-clock offsets, while the main loop polls the
caching dataset and records one sample per query. Both stamp
``t_rel = now - T0``, and the origin is started with the SAME ``T0`` so
its request log ``t_rel_s`` correlates with the samples here.

Scenarios (Section 7 of the harness plan):
  caching-sie-503      origin returns 503 for a window, then recovers.
  caching-sie-timeout  origin hangs past client_timeout (a Spice-side
                       timeout, a send error), then recovers. MANDATORY.
  caching-sie-refuse   origin resets the connection (a connect error),
                       then recovers. (A second send-error scenario.)
  caching-sie-expiry   duration-bounded SIE; expect stale then fail-closed.
                       Handled by run_phase1.sh (the prebuilt binary
                       rejects the duration form at load), reported PENDING.

What the prebuilt spiced v2.3.1 actually does, verified with this harness
(see README, "Findings"): stale-if-error serves the last cached copy on a
SEND error (timeout / connection reset) but NOT on a retryable HTTP 503,
which yields an empty result set instead. So:
  - caching-sie-timeout / caching-sie-refuse -> PASS (stale served).
  - caching-sie-503 -> BLOCKED: the oracle observes empty-on-503 and
    reports it, never a green pass.

Verdict and exit code:
  PASS     0   all assertions hold (SIE served stale through the window).
  BLOCKED  2   the feature under test is absent/pending in this binary
               (empty-on-503, or the dataset failed to load); reported,
               never greened.
  FAIL     1   a correctness violation (e.g. a version the origin never
               produced) or an otherwise unexpected result.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import multi_key  # noqa: E402
from harness import arrivals, cli, http, oracle  # noqa: E402
from harness.timeline import Step, Timeline  # noqa: E402


# Per-scenario fault profile POSTed to the origin during the fault window.
SCENARIOS: dict[str, dict[str, Any]] = {
    "caching-sie-503": {
        "id": "p1-503",
        "mode": "status",
        "error_status": 503,
        "error_rate": 1.0,
        "seed": 12345,
    },
    "caching-sie-timeout": {
        "id": "p1-timeout",
        "mode": "hang",
        "timeout_hang_ms": 3000,
        "error_rate": 1.0,
        "seed": 12345,
    },
    "caching-sie-refuse": {
        "id": "p1-refuse",
        "mode": "refuse",
        "error_rate": 1.0,
        "seed": 12345,
    },
    "caching-sie-expiry": {
        "id": "p1-expiry-503",
        "mode": "status",
        "error_status": 503,
        "error_rate": 1.0,
        "seed": 12345,
    },
}

# Scenarios whose fault is a SEND error (timeout / reset). On the prebuilt
# binary these DO trigger stale-if-error serving.
SEND_ERROR_SCENARIOS = {"caching-sie-timeout", "caching-sie-refuse"}


@dataclass
class Sample:
    seq: int
    query_key: str
    t_send_rel_s: float
    phase: str  # warmup | fault | sie_window | recovery
    latency_ms: float
    http_status: int
    rows: int
    version_seen: Optional[int]
    fetched_at: Optional[str]
    hwm_at_send: Optional[int]
    hwm_after: Optional[int]
    lag_versions: Optional[int]
    freshness: str  # FRESH | STALE | EMPTY | ERROR
    error_kind: str  # none | empty | http_5xx | http_4xx | transport | parse
    fault_id: str


def classify(
    status: int, body: Any, hwm: Optional[int], lag_tol: int
) -> tuple[int, Optional[int], str, str]:
    """Return (rows, version_seen, freshness, error_kind)."""
    if status == 0:
        return 0, None, "ERROR", "transport"
    if status == 503 or (500 <= status < 600):
        return 0, None, "ERROR", "http_5xx"
    if 400 <= status < 500:
        return 0, None, "ERROR", "http_4xx"
    if status == 200 and isinstance(body, list):
        if not body:
            return 0, None, "EMPTY", "empty"
        version_seen = body[0].get("version")
        if version_seen is None:
            return len(body), None, "ERROR", "parse"
        if hwm is None:
            return len(body), version_seen, "STALE", "none"
        lag = hwm - version_seen
        fresh = lag <= lag_tol
        return len(body), version_seen, ("FRESH" if fresh else "STALE"), "none"
    return 0, None, "ERROR", "parse"


def run(args: argparse.Namespace) -> int:
    scenario = args.scenario
    if scenario not in SCENARIOS:
        print(f"unknown scenario: {scenario}", file=sys.stderr)
        return 1
    fault_profile = SCENARIOS[scenario]

    os.makedirs(args.out_dir, exist_ok=True)
    base_query = (
        f"SELECT version, _fetched_at FROM {args.dataset} "
        f"WHERE origin = '{args.origin_name}'"
    )
    pick_key = multi_key.make_key_picker(args.seed)
    query_keys = multi_key.QUERY_KEYS

    t0 = args.t0 if args.t0 > 0 else time.time()

    fault_start = args.warmup_s
    fault_end = args.warmup_s + args.fault_s
    run_end = fault_end + args.recovery_s
    # The cached entry is fresh/SWR until (max_age + swr) after its last
    # successful refresh (~fault_start). Past that, with the origin failing,
    # the SIE decision applies.
    sie_start = fault_start + args.max_age_s + args.swr_s

    steps = [
        Step(at_s=0.0, profile={"id": "healthy", "mode": "healthy", "error_rate": 0.0},
             note="warmup-healthy"),
        Step(at_s=fault_start, profile=fault_profile, note="inject-fault"),
        Step(at_s=fault_end, profile={"id": "recovered", "mode": "healthy", "error_rate": 0.0},
             note="recover-healthy"),
    ]
    timeline = Timeline(t0, args.origin_control_url, steps)

    # Make sure the origin starts healthy and the clocks agree.
    try:
        http.post_json(args.origin_control_url, steps[0].profile)
    except Exception as e:  # noqa: BLE001
        print(f"failed to reach origin control API: {e}", file=sys.stderr)
        return 1

    stats0 = http.get_json(args.origin_stats_url)

    # Prime one cache entry per key while the origin is healthy, so every key
    # is warm (and its per-key high-water mark exists) before the fault window
    # freezes each entry independently.
    for key in query_keys:
        http.sql(
            args.spiced_sql_url,
            multi_key.with_request_query(base_query, key),
            timeout=args.request_timeout_s,
        )

    timeline.start()

    samples: list[Sample] = []
    seq = 0
    last_good_version: Optional[int] = None

    # Align the load loop to the shared clock. Poll at a target interval; a
    # blocked fetch during the fault window naturally stretches the gap.
    while True:
        now = time.time()
        if now - t0 >= run_end:
            break
        seq += 1
        query_key = pick_key()

        # Per-key high-water mark for the key THIS query uses; a global mark
        # would mix keys that diverge from initialization jitter.
        try:
            hwm = http.get_json(args.origin_stats_url).get("hwm_by_key", {}).get(query_key)
        except Exception:  # noqa: BLE001
            hwm = None

        query = multi_key.with_request_query(base_query, query_key)
        t_send = time.time()
        t_rel = t_send - t0
        status, body = http.sql(args.spiced_sql_url, query, timeout=args.request_timeout_s)
        latency_ms = (time.time() - t_send) * 1000.0

        # The origin advances while a blocked fetch is in flight, so the
        # correctness invariant (never serve a version the origin has not
        # produced) is checked against the per-key high-water mark AFTER the
        # query.
        try:
            hwm_after = http.get_json(args.origin_stats_url).get("hwm_by_key", {}).get(query_key)
        except Exception:  # noqa: BLE001
            hwm_after = None

        rows, version_seen, freshness, error_kind = classify(
            status, body, hwm, args.lag_tol
        )
        lag = (hwm - version_seen) if (hwm is not None and version_seen is not None) else None
        fetched_at: Optional[str] = None
        if status == 200 and isinstance(body, list) and body:
            fetched_at = body[0].get("_fetched_at")

        # Track the last fresh version the origin served BEFORE the fault, so
        # the report can show which frozen version SIE then serves.
        if (
            freshness == "FRESH"
            and version_seen is not None
            and t_rel < fault_start
            and (last_good_version is None or version_seen > last_good_version)
        ):
            last_good_version = version_seen

        phase = "warmup"
        if t_rel >= fault_end:
            phase = "recovery"
        elif t_rel >= sie_start:
            phase = "sie_window"
        elif t_rel >= fault_start:
            phase = "fault"

        # The fault id active at send time (from the timeline events).
        fault_id = "healthy"
        for ev in list(timeline.events):
            if ev.t_rel_s <= t_rel:
                fault_id = ev.profile_id

        samples.append(
            Sample(
                seq=seq,
                query_key=query_key,
                t_send_rel_s=round(t_rel, 3),
                phase=phase,
                latency_ms=round(latency_ms, 1),
                http_status=status,
                rows=rows,
                version_seen=version_seen,
                fetched_at=fetched_at,
                hwm_at_send=hwm,
                hwm_after=hwm_after,
                lag_versions=lag,
                freshness=freshness,
                error_kind=error_kind,
                fault_id=fault_id,
            )
        )

        # Pace: keep a minimum gap between sends; blocked fetches already
        # exceed it during the fault window.
        elapsed = time.time() - t_send
        if elapsed < args.poll_interval_s:
            time.sleep(args.poll_interval_s - elapsed)

    stats1 = http.get_json(args.origin_stats_url)

    # ---- Persist samples + driver events ----
    samples_path = os.path.join(args.out_dir, "samples.csv")
    oracle.write_samples_csv(samples_path, samples)

    events_path = os.path.join(args.out_dir, "driver_events.csv")
    oracle.write_dicts_csv(events_path, [asdict(e) for e in timeline.events])

    # ---- Origin request-log evidence over the SIE window ----
    # Arrivals are keyed on this process's shared clock, so the origin's own
    # HARNESS_T0 does not have to match.
    fault_status_counts: dict[str, int] = {}
    fetches_in_sie_window = 0
    for a in arrivals.read_arrivals(args.origin_request_log, t0):
        key = str(a["applied_status"])
        fault_status_counts[key] = fault_status_counts.get(key, 0) + 1
        if sie_start <= a["t_rel_s"] <= fault_end:
            fetches_in_sie_window += 1

    # ---- Oracle ----
    sie_samples = [s for s in samples if s.phase == "sie_window"]
    recovery_samples = [s for s in samples if s.phase == "recovery"]
    warmup_samples = [s for s in samples if s.phase == "warmup"]

    n_stale = sum(1 for s in sie_samples if s.freshness == "STALE")
    n_empty = sum(1 for s in sie_samples if s.freshness == "EMPTY")
    n_error = sum(1 for s in sie_samples if s.freshness == "ERROR")

    # Correctness: never serve a version the origin has not produced. Check
    # against the high-water mark sampled AFTER the query, because a fetch
    # that blocks for seconds during the fault window lets the origin
    # advance legitimately between the pre-query hwm sample and the response.
    ahead = [
        s
        for s in samples
        if s.version_seen is not None
        and s.hwm_after is not None
        and s.version_seen > s.hwm_after
    ]

    assertions = oracle.AssertionSet()
    add = assertions.add

    add(
        "cache_never_ahead_of_origin",
        len(ahead) == 0,
        f"{len(ahead)} samples served a version > hwm_at_send (correctness invariant)",
    )
    add(
        "warmup_served_data",
        len(warmup_samples) > 0
        and all(s.freshness in ("FRESH", "STALE") for s in warmup_samples),
        f"{sum(1 for s in warmup_samples if s.freshness in ('FRESH','STALE'))}"
        f"/{len(warmup_samples)} warmup queries returned rows",
    )

    expect_stale = scenario in SEND_ERROR_SCENARIOS
    add(
        "sie_window_has_samples",
        len(sie_samples) > 0,
        f"{len(sie_samples)} samples fell in the SIE window "
        f"[{round(sie_start,1)}s, {round(fault_end,1)}s]",
    )

    if expect_stale:
        # SIE is proven when the window served the stale copy at least once
        # and never returned an empty set or a propagated error. A sample
        # near the recovery boundary may legitimately turn FRESH because its
        # in-flight fetch completed just after the origin healed, so require
        # "stale served, never empty/error" rather than "all STALE".
        add(
            "sie_serves_stale_through_error",
            len(sie_samples) > 0 and n_stale >= 1 and n_empty == 0 and n_error == 0,
            f"SIE window: stale={n_stale} empty={n_empty} error={n_error} "
            f"(need stale>=1, empty=0, error=0)",
        )
        # The stale copy must be a frozen earlier version, below the origin
        # high-water mark it would have advanced to.
        frozen = {s.version_seen for s in sie_samples if s.freshness == "STALE"}
        add(
            "sie_stale_version_is_frozen_and_below_hwm",
            len(sie_samples) > 0
            and n_stale > 0
            and all(
                s.version_seen is not None
                and s.hwm_at_send is not None
                and s.version_seen < s.hwm_at_send
                for s in sie_samples
                if s.freshness == "STALE"
            ),
            f"stale versions served = {sorted(v for v in frozen if v is not None)}; "
            f"origin hwm advanced to {stats1['hwm_version']}",
        )
        if scenario == "caching-sie-timeout":
            add(
                "connector_timed_out_not_full_hang",
                fault_status_counts.get("hang", 0) > 0 and n_stale > 0,
                f"origin recorded {fault_status_counts.get('hang', 0)} hang fetches "
                f"(delay {fault_profile['timeout_hang_ms']}ms) yet the cache still "
                f"served stale -> the connector abandoned each hang at client_timeout, "
                f"it did not wait the full hang",
            )

    add(
        "recovery_resumes_freshness",
        any(s.freshness == "FRESH" for s in recovery_samples),
        f"{sum(1 for s in recovery_samples if s.freshness=='FRESH')}"
        f"/{len(recovery_samples)} recovery queries were FRESH again",
    )

    # ---- Verdict ----
    correctness_ok = len(ahead) == 0
    # BLOCKED signature: the SIE window returned empty (not stale, not a
    # propagated error), which is exactly the prebuilt-binary behavior for a
    # retryable HTTP status such as 503.
    blocked_empty_signature = (
        not expect_stale
        and len(sie_samples) > 0
        and n_empty > 0
        and n_stale == 0
    )

    verdict, exit_code = oracle.decide_verdict(
        correctness_ok=correctness_ok,
        assertions=assertions,
        blocked=blocked_empty_signature,
    )

    blocked_reason = None
    if blocked_empty_signature:
        blocked_reason = (
            "stale-if-error did NOT serve stale on a retryable HTTP "
            f"{fault_profile.get('error_status', '5xx')}: the SIE window returned "
            f"{n_empty} empty result sets (HTTP 200, []), not the cached copy. On "
            "prebuilt spiced v2.3.1 SIE serves stale for SEND errors (timeout / "
            "reset) only; status-error SIE is absent (see #14126). Not a pass."
        )

    verdict_obj = {
        "scenario": scenario,
        "verdict": verdict,
        "exit_code": exit_code,
        "blocked_reason": blocked_reason,
        "config": {
            "dataset": args.dataset,
            "origin_name": args.origin_name,
            "query_base": base_query,
            "query_keys": query_keys,
            "max_age_s": args.max_age_s,
            "swr_s": args.swr_s,
            "warmup_s": args.warmup_s,
            "fault_s": args.fault_s,
            "recovery_s": args.recovery_s,
            "sie_window_s": [round(sie_start, 1), round(fault_end, 1)],
            "fault_profile": fault_profile,
        },
        "summary": {
            "samples": len(samples),
            "sie_window_samples": len(sie_samples),
            "sie_stale": n_stale,
            "sie_empty": n_empty,
            "sie_error": n_error,
            "last_good_version_before_fault": last_good_version,
            "origin_hwm_start": stats0["hwm_version"],
            "origin_hwm_end": stats1["hwm_version"],
            "origin_fetch_applied_status_counts": fault_status_counts,
            "origin_fetches_in_sie_window": fetches_in_sie_window,
        },
        "assertions": assertions.as_list(),
        "driver_events": [asdict(e) for e in timeline.events],
    }
    verdict_path = os.path.join(args.out_dir, "assertions.json")
    oracle.write_verdict(verdict_path, verdict_obj)

    # ---- Console report ----
    print(f"=== Phase 1 scenario: {scenario} ===")
    print(
        f"warmup {args.warmup_s}s | fault {args.fault_s}s | recovery {args.recovery_s}s; "
        f"max_age {args.max_age_s}s swr {args.swr_s}s; "
        f"SIE window [{round(sie_start,1)}s, {round(fault_end,1)}s]"
    )
    print(f"origin HWM {stats0['hwm_version']} -> {stats1['hwm_version']}")
    print(f"origin /data fetch outcomes: {fault_status_counts}")
    print()
    print("driver events (shared clock):")
    for ev in timeline.events:
        print(f"  t+{ev.t_rel_s:>7.2f}s  {ev.action:<16} profile={ev.profile_id} mode={ev.mode}")
    print()
    print("per-response transitions (one line per freshness/version change):")
    print(f"  {'t_rel_s':>8}  {'phase':<10} {'hwm':>5} {'seen':>5} {'lat_ms':>8}  {'http':>4}  band")
    prev = object()
    for s in samples:
        key = (s.freshness, s.version_seen)
        if key != prev:
            print(
                f"  {s.t_send_rel_s:8.2f}  {s.phase:<10} {str(s.hwm_at_send):>5} "
                f"{str(s.version_seen):>5} {s.latency_ms:8.1f}  {str(s.http_status):>4}  {s.freshness}"
            )
            prev = key
    print()
    print("assertions:")
    for a in assertions:
        print(f"  [{'PASS' if a['pass'] else 'FAIL'}] {a['name']}: {a['detail']}")
    print()
    if blocked_reason:
        print(f"BLOCKED: {blocked_reason}")
    print(f"VERDICT: {verdict}  (exit {exit_code})")
    print(f"wrote {samples_path}")
    print(f"wrote {events_path}")
    print(f"wrote {verdict_path}")
    return exit_code


def main() -> int:
    p = argparse.ArgumentParser(description="Phase 1 stale-if-error scenario runner")
    p.add_argument("--scenario", required=True, choices=sorted(SCENARIOS.keys()))
    p.add_argument(
        "--spiced-sql-url",
        default=cli.env_str("SPICED_SQL_URL", "http://127.0.0.1:8090/v1/sql"),
    )
    p.add_argument(
        "--origin-control-url",
        default=cli.env_str("ORIGIN_CONTROL_URL", "http://127.0.0.1:9001/control"),
    )
    p.add_argument(
        "--origin-stats-url",
        default=cli.env_str("ORIGIN_STATS_URL", "http://127.0.0.1:9001/stats"),
    )
    p.add_argument("--origin-request-log", default=cli.env_str("ORIGIN_REQUEST_LOG", ""))
    p.add_argument("--dataset", default=cli.env_str("DATASET", "d1"))
    p.add_argument("--origin-name", default=cli.env_str("ORIGIN_NAME", "p1"))
    p.add_argument("--t0", type=float, default=cli.env_float("HARNESS_T0", 0.0))
    p.add_argument("--warmup-s", type=float, default=cli.env_float("WARMUP_S", 14.0))
    p.add_argument("--fault-s", type=float, default=cli.env_float("FAULT_S", 40.0))
    p.add_argument("--recovery-s", type=float, default=cli.env_float("RECOVERY_S", 16.0))
    p.add_argument("--max-age-s", type=float, default=cli.env_float("MAX_AGE_S", 3.0))
    p.add_argument("--swr-s", type=float, default=cli.env_float("SWR_S", 6.0))
    p.add_argument("--poll-interval-s", type=float, default=cli.env_float("POLL_INTERVAL_S", 2.0))
    p.add_argument("--request-timeout-s", type=float, default=cli.env_float("REQUEST_TIMEOUT_S", 30.0))
    p.add_argument("--lag-tol", type=int, default=cli.env_int("LAG_TOL", 9))
    p.add_argument("--out-dir", default=cli.env_str("OUT_DIR", "/tmp/http_cache_phase1_run"))
    p.add_argument(
        "--seed",
        type=int,
        default=cli.env_int("SEED", multi_key.DEFAULT_SEED),
        help="seed for the deterministic request_query key picker",
    )
    args = p.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
