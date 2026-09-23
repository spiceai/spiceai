#!/usr/bin/env python3
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

"""Phase 2 adaptive rate-control scenario driver, load generator and oracle.

One process owns the shared wall clock ``T0``. Three things run against it:

  1. An open-loop load generator that fires SQL queries at a fixed target QPS
     per dataset, decoupled from response completion (a slow/blocked fetch
     never lowers the offered rate). Each dataset is a NON-accelerated
     federated HTTP dataset, so one query == one upstream HTTP request the
     per-origin rate controller sees.
  2. A background timeline that steps the p2 origin fault profile on the shared
     clock (healthy -> faulting -> healthy) while p1 stays healthy throughout,
     so cross-origin isolation can be read directly.
  3. A metrics scraper polling spiced's Prometheus /metrics endpoint every 1s,
     keeping the per-origin adaptive / cooldown / static-limiter series.

The oracle correlates the scraped metrics, the origin request logs, and the
per-response samples on ``t_rel = now - T0`` and writes ``assertions.json``.

Scenarios (harness plan Section 7):
  ratecontrol-sre      SRE throttle, K=2. p2 -> 90% 503 (accepts/requests < 1/K
                       so it actually throttles). Expect admission near the SRE
                       steady state (2*accepts/requests) within tolerance.
  ratecontrol-cooldown p2 -> 429 + Retry-After: 2. Expect the retry-after
                       cooldown metrics to move and a ~2s gap in p2 arrivals.
  ratecontrol-ietf-headers  p2 serves 200 + RateLimit / RateLimit-Policy only
                       (no error, no Retry-After). Expect admission does NOT
                       change today (server-advertised quota not yet honored,
                       TODO(#14136)); the assertion flips when that lands.

#14143 ships one admission-coefficient strategy (Google SRE client-side
throttling); there is no separate AIMD control law, so the scenarios above all
exercise the same mechanism under different fault shapes.

Verdict / exit code:
  PASS     0   every assertion holds.
  BLOCKED  2   the feature is absent/misconfigured in this binary (e.g. the
               dataset failed to register, or no adaptive metric series was ever
               observed). Reported, never greened.
  FAIL     1   an assertion failed on a binary that does expose the feature.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import multi_key  # noqa: E402
from metrics_scraper import MetricsScraper, load_config  # noqa: E402
from harness import arrivals, cli, http, oracle  # noqa: E402
from harness.phases import Phases, phase_at  # noqa: E402
from harness.timeline import Step, Timeline  # noqa: E402


# --------------------------------------------------------------------------
# Scenario fault profiles POSTed to the p2 origin during the fault window.
# --------------------------------------------------------------------------
SCENARIOS: dict[str, dict[str, Any]] = {
    "ratecontrol-sre": {
        # SRE with K=2 only throttles when accepts/requests < 1/K = 0.5, so the
        # failure fraction must exceed 0.5. 0.9 -> steady-state admission
        # ~ 2*0.1 = 0.2.
        "id": "p2-503-heavy",
        "mode": "status",
        "error_status": 503,
        "error_rate": 0.9,
        "latency_ms": {"base": 20, "jitter": 10},
        "seed": 12345,
    },
    "ratecontrol-cooldown": {
        "id": "p2-429-retryafter",
        "mode": "status",
        "error_status": 429,
        "error_rate": 1.0,
        "headers": {"Retry-After": "2"},
        "seed": 12345,
    },
    "ratecontrol-ietf-headers": {
        # No error at all: 200s that merely ADVERTISE a quota via the IETF
        # RateLimit headers. Today the controller must ignore them.
        "id": "p2-ietf-advertise",
        "mode": "latency",
        "error_rate": 0.0,
        "latency_ms": {"base": 10, "jitter": 5},
        "headers": {
            "RateLimit": "limit=5, remaining=0, reset=2",
            "RateLimit-Policy": "5;w=1",
        },
        "seed": 12345,
    },
}

HEALTHY = {"id": "healthy", "mode": "healthy", "error_rate": 0.0, "headers": {}}


@dataclass
class Sample:
    seq: int
    query_key: str
    t_send_rel_s: float
    dataset: str
    origin_name: str
    phase: str  # warmup | fault | recovery | burst
    latency_ms: float
    http_status: int
    ok: bool
    rows: int
    version_seen: Optional[int]
    error_kind: str  # none | http_5xx | http_429 | http_4xx | transport | parse | empty


def classify(status: int, body: Any) -> tuple[bool, int, Optional[int], str]:
    """Return (ok, rows, version_seen, error_kind)."""
    if status == 0:
        return False, 0, None, "transport"
    if status == 429:
        return False, 0, None, "http_429"
    if 500 <= status < 600:
        return False, 0, None, "http_5xx"
    if 400 <= status < 500:
        return False, 0, None, "http_4xx"
    if status == 200 and isinstance(body, list):
        if not body:
            return True, 0, None, "empty"
        version_seen = body[0].get("version")
        if version_seen is None:
            return True, len(body), None, "parse"
        return True, len(body), version_seen, "none"
    return False, 0, None, "parse"


class OpenLoopLoad:
    """Fires SQL queries at a fixed target QPS, decoupled from completion."""

    def __init__(
        self,
        t0: float,
        sql_url: str,
        request_timeout_s: float,
        pool: ThreadPoolExecutor,
        pick_key: "Callable[[], str]",
    ):
        self.t0 = t0
        self.sql_url = sql_url
        self.request_timeout_s = request_timeout_s
        self.pool = pool
        # Single seeded key picker shared by both driver threads; called under
        # `_lock` so the one RNG stream stays consistent (no second stream).
        self._pick_key = pick_key
        self.samples: list[Sample] = []
        self._lock = threading.Lock()
        self._seq = 0
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()

    def _fire(self, dataset: str, origin_name: str, base_query: str, tl: "Phases") -> None:
        with self._lock:
            self._seq += 1
            seq = self._seq
            query_key = self._pick_key()
        query = multi_key.with_request_query(base_query, query_key)
        t_send = time.time()
        t_rel = t_send - self.t0
        status, body = http.sql(self.sql_url, query, self.request_timeout_s)
        latency_ms = (time.time() - t_send) * 1000.0
        ok, rows, version_seen, error_kind = classify(status, body)
        s = Sample(
            seq=seq,
            query_key=query_key,
            t_send_rel_s=round(t_rel, 3),
            dataset=dataset,
            origin_name=origin_name,
            phase=phase_at(t_rel, tl),
            latency_ms=round(latency_ms, 1),
            http_status=status,
            ok=ok,
            rows=rows,
            version_seen=version_seen,
            error_kind=error_kind,
        )
        with self._lock:
            self.samples.append(s)

    def _driver(
        self, dataset: str, origin_name: str, base_query: str, qps: float, tl: "Phases"
    ) -> None:
        """Open-loop pacer for one dataset. Submits at fixed cadence; QPS may
        ramp for the burst window (unused by any current scenario)."""
        interval = 1.0 / qps if qps > 0 else 1.0
        next_send = time.time()
        while not self._stop.is_set():
            now = time.time()
            t_rel = now - self.t0
            if t_rel >= tl.run_end:
                break
            step = interval
            if tl.burst_qps and origin_name == "p2" and t_rel >= tl.burst_start:
                step = 1.0 / tl.burst_qps
            if now >= next_send:
                self.pool.submit(self._fire, dataset, origin_name, base_query, tl)
                next_send = max(now, next_send) + step
            else:
                time.sleep(min(0.01, next_send - now))

    def run(self, plan: list[tuple[str, str, str, float]], tl: "Phases") -> None:
        for dataset, origin_name, base_query, qps in plan:
            th = threading.Thread(
                target=self._driver, args=(dataset, origin_name, base_query, qps, tl)
            )
            th.start()
            self._threads.append(th)

    def join(self) -> None:
        for th in self._threads:
            th.join()
        self._stop.set()


# --------------------------------------------------------------------------
# SRE admission reconstruction
# --------------------------------------------------------------------------
def sre_predicted_admission(
    arrivals: list[dict[str, Any]], at_t: float, k: float, half_life_s: float = 10.0
) -> Optional[float]:
    """Reconstruct the SRE admission coefficient the controller would report at
    ``at_t`` from the origin arrival log: an exponentially time-decayed count of
    requests and accepts (200s), then min(1, (k*accepts+1)/(requests+1))."""
    requests = 0.0
    accepts = 0.0
    seen = False
    for a in arrivals:
        dt = at_t - a["t_rel_s"]
        if dt < 0:
            continue
        w = 0.5 ** (dt / half_life_s)
        requests += w
        if a["applied_status"] == 200:
            accepts += w
        seen = True
    if not seen:
        return None
    return min(1.0, (k * accepts + 1.0) / (requests + 1.0))


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    scenario = args.scenario
    if scenario not in SCENARIOS:
        print(f"unknown scenario: {scenario}", file=sys.stderr)
        return 1
    # Copy, not the shared module-level dict -- an override must not mutate
    # SCENARIOS itself. Optional env-var overrides let an ad-hoc experiment
    # reuse a scenario's spicepod/assertion set with a different fault
    # shape (e.g. a timeout instead of a 503) without adding a new named
    # scenario for every combination.
    fault_profile = dict(SCENARIOS[scenario])
    if os.environ.get("FAULT_MODE"):
        fault_profile["mode"] = os.environ["FAULT_MODE"]
    if os.environ.get("FAULT_ERROR_RATE"):
        fault_profile["error_rate"] = float(os.environ["FAULT_ERROR_RATE"])
    if os.environ.get("FAULT_ERROR_STATUS"):
        fault_profile["error_status"] = int(os.environ["FAULT_ERROR_STATUS"])
    if os.environ.get("FAULT_TIMEOUT_HANG_MS"):
        fault_profile["timeout_hang_ms"] = int(os.environ["FAULT_TIMEOUT_HANG_MS"])

    os.makedirs(args.out_dir, exist_ok=True)
    t0 = args.t0 if args.t0 > 0 else time.time()

    cfg = load_config(args.metrics_config)
    scraper = MetricsScraper(cfg, t0=t0, interval_s=args.scrape_interval_s,
                             endpoint=args.metrics_endpoint or None)

    tl = Phases(
        fault_start=args.warmup_s,
        fault_end=args.warmup_s + args.fault_s,
        run_end=args.warmup_s + args.fault_s + args.recovery_s,
    )

    # p1 stays healthy the whole run; only p2 is stepped.
    steps = [
        Step(at_s=0.0, profile={**HEALTHY, "seed": 12345}, note="warmup-healthy-p2"),
        Step(at_s=tl.fault_start, profile=fault_profile, note="inject-fault-p2"),
        Step(at_s=tl.fault_end, profile={**HEALTHY, "seed": 12345}, note="recover-p2"),
    ]
    timeline = Timeline(t0, args.p2_control_url, steps)

    # Prime both origins healthy and confirm reachability.
    try:
        http.post_json(args.p1_control_url, {**HEALTHY, "seed": 12345})
        http.post_json(args.p2_control_url, {**HEALTHY, "seed": 12345})
    except Exception as e:  # noqa: BLE001
        print(f"failed to reach an origin control API: {e}", file=sys.stderr)
        return 1

    p1_stats0 = http.get_json(args.p1_stats_url)
    p2_stats0 = http.get_json(args.p2_stats_url)

    # Base queries; each fired query appends `AND request_query = '<key>'` so
    # distinct keys become distinct upstream requests the rate controller sees.
    q1 = f"SELECT version FROM {args.p1_dataset} WHERE origin = 'p1'"
    q2 = f"SELECT version FROM {args.p2_dataset} WHERE origin = 'p2'"
    load_plan = [
        (args.p1_dataset, "p1", q1, args.p1_qps),
        (args.p2_dataset, "p2", q2, args.p2_qps),
    ]

    pool = ThreadPoolExecutor(max_workers=args.max_workers)
    load = OpenLoopLoad(
        t0,
        args.spiced_sql_url,
        args.request_timeout_s,
        pool,
        multi_key.make_key_picker(args.seed),
    )

    # Start everything on the shared clock.
    scraper.start()
    timeline.start()
    load.run(load_plan, tl)
    load.join()
    # One trailing scrape so the final gauge state is captured, then stop.
    scraper.scrape_once()
    scraper.stop()
    pool.shutdown(wait=False)

    p1_stats1 = http.get_json(args.p1_stats_url)
    p2_stats1 = http.get_json(args.p2_stats_url)

    # ---- Persist artifacts ----
    samples_path = os.path.join(args.out_dir, "samples.csv")
    oracle.write_samples_csv(samples_path, load.samples, sort_key=lambda x: x.seq)
    scraper.write_csv(os.path.join(args.out_dir, "metrics.csv"))
    scraper.write_discovered(os.path.join(args.out_dir, "discovered_metrics.txt"))
    events_path = os.path.join(args.out_dir, "driver_events.csv")
    oracle.write_dicts_csv(events_path, [asdict(e) for e in timeline.events])

    # ---- Resolve the origin metric label values from the live scrape ----
    adm = cfg["adaptive"]["admission_coefficient_permille"]
    eff = cfg["adaptive"]["effective_limit"]
    thr = cfg["adaptive"]["throttled_total"]
    observed_origins = sorted(scraper.origins(adm) | scraper.origins(eff))

    def origin_key(name: str) -> Optional[str]:
        """Map p1/p2 to the metric `origin` label value observed live."""
        port = args.p1_port if name == "p1" else args.p2_port
        for o in observed_origins:
            if o.endswith(f":{port}") or f":{port}" in o:
                return o
        return None

    p1_key = origin_key("p1")
    p2_key = origin_key("p2")

    p1_arrivals = arrivals.read_arrivals(args.p1_request_log, t0)
    p2_arrivals = arrivals.read_arrivals(args.p2_request_log, t0)

    # ---- Oracle ----
    assertions = oracle.AssertionSet()
    add = assertions.add

    fs, fe, re_end = tl.fault_start, tl.fault_end, tl.run_end
    ceiling = float(args.rps_limit)

    # BLOCKED signature: no adaptive series ever observed for p2 -> the feature
    # is absent or the dataset never registered with rate control.
    p2_adm_series = scraper.series(adm, p2_key) if p2_key else []
    feature_present = len(p2_adm_series) > 0

    # Metric windows.
    p2_adm_min_fault = scraper.min_in_window(adm, p2_key, fs, fe) if p2_key else None
    p2_eff_min_fault = scraper.min_in_window(eff, p2_key, fs, fe) if p2_key else None
    p2_thr_delta = scraper.counter_delta(thr, p2_key, fs, fe) if p2_key else None
    p2_adm_recovery = scraper.latest_in_window(adm, p2_key, fe, re_end) if p2_key else None
    p2_eff_recovery = scraper.latest_in_window(eff, p2_key, fe, re_end) if p2_key else None
    p1_adm_min_all = scraper.min_in_window(adm, p1_key, 0.0, re_end) if p1_key else None

    # Origin arrivals.
    p2_fault_arrivals = arrivals.arrivals_in(p2_arrivals, fs, fe)
    p2_offered_fault = args.p2_qps * (fe - fs)
    p2_arrival_rate = len(p2_fault_arrivals) / max(1e-9, (fe - fs))

    # -- Correctness: never a version the origin has not produced --
    # Compare each sample against the FINAL per-key high-water mark for the
    # origin AND key it actually used. Versions only grow, so the final mark
    # is a valid upper bound; a global max across keys/origins would mix keys
    # that diverge from initialization jitter and raise false failures.
    def _hwm_for(sample: Sample) -> int:
        stats = p1_stats1 if sample.origin_name == "p1" else p2_stats1
        by_key = stats.get("hwm_by_key", {})
        return by_key.get(sample.query_key, stats["hwm_version"])

    ahead = [
        s
        for s in load.samples
        if s.version_seen is not None and s.version_seen > _hwm_for(s)
    ]
    add(
        "cache_never_ahead_of_origin",
        len(ahead) == 0,
        f"{len(ahead)} samples returned a version above the origin high-water mark",
    )

    # -- Warmup served data on both origins (sanity that load is flowing) --
    warm = [s for s in load.samples if s.phase == "warmup"]
    warm_ok = sum(1 for s in warm if s.ok and s.rows > 0)
    add(
        "warmup_load_flowing",
        len(warm) > 0 and warm_ok > 0,
        f"{warm_ok}/{len(warm)} warmup queries returned rows",
    )

    if scenario == "ratecontrol-sre":
        k = args.sre_k  # must match `http_adaptive_rate_control` in the SRE pod
        add(
            "p2_admission_drops_during_fault",
            p2_adm_min_fault is not None and p2_adm_min_fault < args.admission_drop_permille,
            f"min admission_coefficient_permille[p2] during fault = {p2_adm_min_fault} "
            f"(need < {args.admission_drop_permille})",
        )
        add(
            "p2_throttled_total_increases",
            p2_thr_delta is not None and p2_thr_delta > 0,
            f"throttled_total[p2] delta over fault = {p2_thr_delta} (need > 0)",
        )
        # Reconstruct the SRE admission from the p2 arrival log at each scrape
        # time in the second half of the fault window (after the decaying
        # window has filled), and compare to the observed coefficient.
        errs: list[float] = []
        pred_obs: list[tuple[float, float, float]] = []
        for s in (p2_adm_series or []):
            if not (fs + 5.0 <= s.t_rel_s <= fe):
                continue
            pred = sre_predicted_admission(p2_arrivals, s.t_rel_s, k)
            if pred is None:
                continue
            obs = s.value / 1000.0
            errs.append(abs(pred - obs))
            pred_obs.append((round(s.t_rel_s, 1), round(pred, 3), round(obs, 3)))
        mae = sum(errs) / len(errs) if errs else None
        add(
            "sre_admission_matches_formula",
            mae is not None and mae <= args.sre_tolerance,
            f"mean|predicted-observed| admission over fault (2nd half) = "
            f"{None if mae is None else round(mae, 3)} (need <= {args.sre_tolerance}); "
            f"samples (t, pred, obs) = {pred_obs[:12]}",
        )
        add(
            "p2_recovers_admission",
            p2_adm_recovery is not None and p2_adm_recovery >= args.recovery_permille,
            f"end-of-recovery admission[p2] = {p2_adm_recovery} "
            f"(need >= {args.recovery_permille})",
        )
        add(
            "p1_admission_stays_full",
            p1_adm_min_all is not None and p1_adm_min_all >= args.p1_full_permille,
            f"min admission[p1] over the whole run = {p1_adm_min_all} "
            f"(need >= {args.p1_full_permille})",
        )

    elif scenario == "ratecontrol-cooldown":
        upd = cfg["cooldown"]["retry_after_updates_total"]
        rem = cfg["cooldown"]["retry_after_remaining_ms"]
        upd_delta = scraper.counter_delta(upd, p2_key, fs, fe) if p2_key else None
        rem_max = scraper.max_in_window(rem, p2_key, fs, fe) if p2_key else None
        gap = arrivals.max_gap(p2_arrivals, fs, fe)
        add(
            "retry_after_updates_move",
            upd_delta is not None and upd_delta > 0,
            f"retry_after_updates_total[p2] delta over fault = {upd_delta} (need > 0)",
        )
        add(
            "retry_after_remaining_advertised",
            rem_max is not None and rem_max > 0,
            f"max retry_after_remaining_ms[p2] during fault = {rem_max} (need > 0)",
        )
        add(
            "p2_arrival_gap_near_advertised_cooldown",
            gap >= args.cooldown_gap_min_s,
            f"largest p2 inter-arrival gap during fault = {round(gap, 2)}s "
            f"(need >= {args.cooldown_gap_min_s}s; origin advertised Retry-After: 2)",
        )
        # p1 must be untouched by p2's cooldown.
        add(
            "p1_admission_stays_full",
            p1_adm_min_all is None or p1_adm_min_all >= args.p1_full_permille,
            f"min admission[p1] over the whole run = {p1_adm_min_all}",
        )

    elif scenario == "ratecontrol-ietf-headers":
        # Advertised-quota headers are not honored yet (TODO(#14136)): admission
        # must stay full and effective_limit at the ceiling. Flip this assertion
        # when the feature lands.
        p2_adm_min = scraper.min_in_window(adm, p2_key, fs, fe) if p2_key else None
        add(
            "ietf_headers_do_not_change_admission_today",
            p2_adm_min is not None and p2_adm_min >= args.p1_full_permille,
            f"min admission[p2] while only RateLimit/RateLimit-Policy advertised = "
            f"{p2_adm_min} (need >= {args.p1_full_permille}: NOT honored today, "
            f"TODO(#14136); flip to expect a change when it lands)",
        )

    # ---- Verdict ----
    correctness_ok = len(ahead) == 0
    verdict, exit_code = oracle.decide_verdict(
        correctness_ok=correctness_ok,
        assertions=assertions,
        blocked=not feature_present,
    )

    blocked_reason = None
    if not feature_present:
        blocked_reason = (
            "no adaptive_rate_control metric series was ever observed for the p2 "
            f"origin (label {p2_key!r}). Either this spiced was built without the "
            "`rate-control` feature, the dataset failed to register with rate "
            "control, or the metric names differ from metrics_config.json. See "
            "discovered_metrics.txt for what the endpoint actually exposed."
        )

    verdict_obj = {
        "scenario": scenario,
        "verdict": verdict,
        "exit_code": exit_code,
        "blocked_reason": blocked_reason,
        "config": {
            "p1_dataset": args.p1_dataset,
            "p2_dataset": args.p2_dataset,
            "p1_qps": args.p1_qps,
            "p2_qps": args.p2_qps,
            "rps_limit_ceiling": ceiling,
            "warmup_s": args.warmup_s,
            "fault_s": args.fault_s,
            "recovery_s": args.recovery_s,
            "fault_window_s": [round(fs, 1), round(fe, 1)],
            "fault_profile": fault_profile,
            "metric_origin_label_p1": p1_key,
            "metric_origin_label_p2": p2_key,
            "observed_metric_origins": observed_origins,
        },
        "evidence": {
            "samples": len(load.samples),
            "p2_admission_min_permille_fault": p2_adm_min_fault,
            "p2_effective_limit_min_fault": p2_eff_min_fault,
            "p2_throttled_total_delta_fault": p2_thr_delta,
            "p2_admission_permille_end_recovery": p2_adm_recovery,
            "p2_effective_limit_end_recovery": p2_eff_recovery,
            "p1_admission_min_permille_all": p1_adm_min_all,
            "p2_upstream_arrivals_fault": len(p2_fault_arrivals),
            "p2_arrival_rate_fault_per_s": round(p2_arrival_rate, 2),
            "p2_offered_fault": round(p2_offered_fault, 0),
            "p1_upstream_arrivals_total": len(p1_arrivals),
            "p2_upstream_arrivals_total": len(p2_arrivals),
            "origin_p1_hwm": [p1_stats0["hwm_version"], p1_stats1["hwm_version"]],
            "origin_p2_hwm": [p2_stats0["hwm_version"], p2_stats1["hwm_version"]],
            "adaptive_scrape_points_p2": len(p2_adm_series),
        },
        "assertions": assertions.as_list(),
        "driver_events": [asdict(e) for e in timeline.events],
    }
    verdict_path = os.path.join(args.out_dir, "assertions.json")
    oracle.write_verdict(verdict_path, verdict_obj)

    # ---- Console report ----
    print(f"=== Phase 2 scenario: {scenario} ===")
    print(
        f"warmup {args.warmup_s}s | fault {args.fault_s}s | recovery {args.recovery_s}s; "
        f"p1 {args.p1_qps} qps, p2 {args.p2_qps} qps; ceiling {ceiling}"
    )
    print(f"metric origin labels: p1={p1_key} p2={p2_key}")
    print(f"observed metric origins: {observed_origins}")
    print()
    print("driver events (shared clock):")
    for ev in timeline.events:
        print(
            f"  t+{ev.t_rel_s:>7.2f}s  {ev.action:<20} profile={ev.profile_id} mode={ev.mode}"
        )
    print()
    print("p2 adaptive metric trace (one line per scrape):")
    print(f"  {'t_rel_s':>8}  {'adm_permille':>12}  {'eff_limit':>10}")
    for s in (p2_adm_series or []):
        eff_pt = scraper.latest_in_window(eff, p2_key, s.t_rel_s - 0.01, s.t_rel_s + 0.01)
        print(f"  {s.t_rel_s:8.2f}  {s.value:12.0f}  {('' if eff_pt is None else f'{eff_pt:.1f}'):>10}")
    print()
    print("assertions:")
    for a in assertions:
        print(f"  [{'PASS' if a['pass'] else 'FAIL'}] {a['name']}: {a['detail']}")
    print()
    if blocked_reason:
        print(f"BLOCKED: {blocked_reason}")
    print(f"VERDICT: {verdict}  (exit {exit_code})")
    for p in (samples_path, os.path.join(args.out_dir, 'metrics.csv'), verdict_path):
        print(f"wrote {p}")
    return exit_code


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description="Phase 2 adaptive rate-control runner")
    p.add_argument("--scenario", required=True, choices=sorted(SCENARIOS.keys()))
    p.add_argument("--spiced-sql-url", default=cli.env_str("SPICED_SQL_URL", "http://127.0.0.1:8090/v1/sql"))
    p.add_argument("--metrics-endpoint", default=cli.env_str("METRICS_ENDPOINT", ""))
    p.add_argument("--metrics-config", default=cli.env_str("METRICS_CONFIG", os.path.join(here, "metrics_config.json")))
    p.add_argument("--p1-control-url", default=cli.env_str("P1_CONTROL_URL", "http://127.0.0.1:9001/control"))
    p.add_argument("--p2-control-url", default=cli.env_str("P2_CONTROL_URL", "http://127.0.0.1:9002/control"))
    p.add_argument("--p1-stats-url", default=cli.env_str("P1_STATS_URL", "http://127.0.0.1:9001/stats"))
    p.add_argument("--p2-stats-url", default=cli.env_str("P2_STATS_URL", "http://127.0.0.1:9002/stats"))
    p.add_argument("--p1-request-log", default=cli.env_str("P1_REQUEST_LOG", ""))
    p.add_argument("--p2-request-log", default=cli.env_str("P2_REQUEST_LOG", ""))
    p.add_argument("--p1-dataset", default=cli.env_str("P1_DATASET", "d1"))
    p.add_argument("--p2-dataset", default=cli.env_str("P2_DATASET", "d2"))
    p.add_argument("--p1-port", type=int, default=cli.env_int("P1_PORT", 9001))
    p.add_argument("--p2-port", type=int, default=cli.env_int("P2_PORT", 9002))
    p.add_argument("--t0", type=float, default=cli.env_float("HARNESS_T0", 0.0))
    p.add_argument("--warmup-s", type=float, default=cli.env_float("WARMUP_S", 15.0))
    p.add_argument("--fault-s", type=float, default=cli.env_float("FAULT_S", 60.0))
    p.add_argument("--recovery-s", type=float, default=cli.env_float("RECOVERY_S", 45.0))
    p.add_argument("--p1-qps", type=float, default=cli.env_float("P1_QPS", 10.0))
    p.add_argument("--p2-qps", type=float, default=cli.env_float("P2_QPS", 30.0))
    p.add_argument("--rps-limit", type=float, default=cli.env_float("RPS_LIMIT", 20.0))
    p.add_argument("--max-workers", type=int, default=cli.env_int("MAX_WORKERS", 128))
    p.add_argument("--request-timeout-s", type=float, default=cli.env_float("REQUEST_TIMEOUT_S", 15.0))
    p.add_argument("--scrape-interval-s", type=float, default=cli.env_float("SCRAPE_INTERVAL_S", 1.0))
    # assertion thresholds
    p.add_argument("--admission-drop-permille", type=float, default=800.0)
    p.add_argument("--recovery-permille", type=float, default=950.0)
    p.add_argument("--p1-full-permille", type=float, default=1000.0)
    p.add_argument("--sre-tolerance", type=float, default=0.2)
    p.add_argument("--sre-k", type=float, default=cli.env_float("SRE_K", 2.0))
    p.add_argument("--cooldown-gap-min-s", type=float, default=1.5)
    p.add_argument("--out-dir", default=cli.env_str("OUT_DIR", "/tmp/http_cache_phase2_run"))
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
