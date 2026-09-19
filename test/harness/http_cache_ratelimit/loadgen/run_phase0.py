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

"""Phase 0 load generator and freshness oracle.

Drives the caching HTTP dataset at a fixed query rate, reads the integer
``version`` back through Spice, and compares it with the origin's
high-water version (HWM) sampled at send time. From the samples plus the
origin request log it decides whether the acceleration cache behaves like
a caching layer: absorbing most reads without a source fetch, serving
data whose staleness is bounded by ``max_age + stale_while_revalidate``,
and never returning a version the origin has not yet produced.

The dataset MUST be queried WITH a filter. A ``refresh_mode: caching``
scan only runs its staleness check and fetches the source when the query
carries a filter; an unfiltered ``SELECT`` returns cached rows directly
and never refreshes (see CachingAccelerationScanExec::execute). A string
equality filter is used (``WHERE origin = '<name>'``): an integer-column
filter currently trips an internal ExprBoundaries error in the caching
scan.

Outputs (written to --out-dir):
  samples.csv     - one row per query: timing, version_seen, hwm, lag.
  assertions.json - the oracle verdict, one entry per assertion.
Exit code is non-zero if any assertion fails.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
import urllib.request
from dataclasses import dataclass, asdict
from typing import Any, Optional


def _http_get_json(url: str, timeout: float = 5.0) -> dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _http_sql(url: str, sql: str, timeout: float = 5.0) -> tuple[int, Any]:
    req = urllib.request.Request(
        url, data=sql.encode(), headers={"Content-Type": "text/plain"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:  # noqa: PERF203 - error path
        body = e.read().decode(errors="replace")
        return e.code, body
    except Exception as e:  # noqa: BLE001 - surface any transport error as a sample
        return 0, f"{type(e).__name__}: {e}"


@dataclass
class Sample:
    seq: int
    t_send_rel_s: float
    latency_ms: float
    http_status: int
    version_seen: Optional[int]
    fetched_at: Optional[str]
    hwm_at_send: Optional[int]
    lag_versions: Optional[int]
    freshness: str  # FRESH | STALE | ERROR


def run(args: argparse.Namespace) -> int:
    sql_url = args.spiced_sql_url
    stats_url = args.origin_stats_url
    query = (
        f"SELECT version, _fetched_at FROM {args.dataset} "
        f"WHERE origin = '{args.origin_name}'"
    )

    os.makedirs(args.out_dir, exist_ok=True)

    # Baseline origin counters.
    stats0 = _http_get_json(stats_url)
    data_requests_0 = stats0["data_requests"]
    t0 = time.time()

    interval = 1.0 / args.qps
    samples: list[Sample] = []
    seq = 0
    next_send = time.time()
    end_at = t0 + args.duration_s

    while time.time() < end_at:
        now = time.time()
        if now < next_send:
            time.sleep(min(next_send - now, 0.05))
            continue
        next_send += interval
        seq += 1

        # Sample the origin HWM immediately before the read so the
        # comparison uses the freshest producer state the cache could
        # possibly have observed.
        try:
            hwm = _http_get_json(stats_url)["hwm_version"]
        except Exception:  # noqa: BLE001
            hwm = None

        t_send = time.time()
        status, body = _http_sql(sql_url, query, timeout=args.request_timeout_s)
        latency_ms = (time.time() - t_send) * 1000.0

        version_seen: Optional[int] = None
        fetched_at: Optional[str] = None
        if status == 200 and isinstance(body, list) and body:
            version_seen = body[0].get("version")
            fetched_at = body[0].get("_fetched_at")

        if version_seen is None:
            freshness = "ERROR"
            lag = None
        elif hwm is None:
            freshness = "UNKNOWN"
            lag = None
        else:
            lag = hwm - version_seen
            freshness = "FRESH" if lag <= 0 else "STALE"

        samples.append(
            Sample(
                seq=seq,
                t_send_rel_s=round(t_send - t0, 4),
                latency_ms=round(latency_ms, 2),
                http_status=status,
                version_seen=version_seen,
                fetched_at=fetched_at,
                hwm_at_send=hwm,
                lag_versions=lag,
                freshness=freshness,
            )
        )

    # Final origin counters.
    stats1 = _http_get_json(stats_url)
    data_requests_1 = stats1["data_requests"]
    origin_data_fetches = data_requests_1 - data_requests_0

    # Cross-check fetch count against the origin request log, if provided.
    log_fetches_in_window: Optional[int] = None
    if args.origin_request_log and os.path.exists(args.origin_request_log):
        cnt = 0
        with open(args.origin_request_log) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("method") == "GET" and rec.get("path", "").startswith(
                    "/data"
                ):
                    cnt += 1
        log_fetches_in_window = cnt

    # Persist samples.
    samples_path = os.path.join(args.out_dir, "samples.csv")
    with open(samples_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(samples[0]).keys()))
        w.writeheader()
        for s in samples:
            w.writerow(asdict(s))

    # ---- Oracle ----
    ok_samples = [s for s in samples if s.freshness in ("FRESH", "STALE")]
    n_total = len(samples)
    n_ok = len(ok_samples)
    n_error = sum(1 for s in samples if s.freshness == "ERROR")

    lags = [s.lag_versions for s in ok_samples if s.lag_versions is not None]
    max_lag = max(lags) if lags else None

    # Staleness bound in versions: an entry can be served up to
    # (max_age + stale_while_revalidate) old, and the origin advances one
    # version per bump_interval, so worst-case lag is that age over the
    # bump interval. A small margin covers request/refresh latency and the
    # HWM sampled just before the read advancing before the cache answers.
    stale_window_s = args.max_age_s + args.swr_s
    lag_bound = math.ceil(stale_window_s / args.bump_interval_s) + args.lag_margin

    # Expected upper bound on source fetches: with continuous querying an
    # entry is refreshed about once per max_age, so fetches over the run
    # should not greatly exceed duration/max_age. The load-absorption
    # assertion is the weaker, robust claim that fetches are far below the
    # query count.
    expected_fetch_ceiling = math.ceil(args.duration_s / args.max_age_s) * 3 + 3

    # version_seen must never exceed the HWM sampled at send: the cache
    # cannot serve a version the origin has not produced. This is the
    # correctness invariant.
    ahead = [
        s
        for s in ok_samples
        if s.lag_versions is not None and s.lag_versions < 0
    ]

    # Monotonic catch-up: the served version must be non-decreasing over
    # time (the cache advances toward the origin, never regresses).
    regressions = []
    prev = None
    for s in ok_samples:
        if prev is not None and s.version_seen is not None and s.version_seen < prev:
            regressions.append((s.seq, prev, s.version_seen))
        if s.version_seen is not None:
            prev = s.version_seen

    assertions = []

    def add(name: str, passed: bool, detail: str) -> None:
        assertions.append({"name": name, "pass": bool(passed), "detail": detail})

    add(
        "all_queries_returned_data",
        n_error == 0 and n_ok == n_total and n_total > 0,
        f"{n_ok}/{n_total} queries returned a row, {n_error} errored",
    )
    add(
        "cache_absorbs_load",
        origin_data_fetches < n_total and n_total > 0,
        f"origin_data_fetches={origin_data_fetches} vs queries={n_total}",
    )
    add(
        "fetch_count_near_max_age_cadence",
        origin_data_fetches <= expected_fetch_ceiling,
        f"origin_data_fetches={origin_data_fetches} <= ceiling={expected_fetch_ceiling} "
        f"(duration={args.duration_s}s / max_age={args.max_age_s}s)",
    )
    add(
        "staleness_bounded",
        max_lag is not None and max_lag <= lag_bound,
        f"max_lag_versions={max_lag} <= bound={lag_bound} "
        f"((max_age {args.max_age_s}s + swr {args.swr_s}s)/bump {args.bump_interval_s}s + margin {args.lag_margin})",
    )
    add(
        "cache_never_ahead_of_origin",
        len(ahead) == 0,
        f"{len(ahead)} samples had version_seen > hwm_at_send",
    )
    add(
        "served_version_monotonic",
        len(regressions) == 0,
        f"{len(regressions)} version regressions: {regressions[:5]}",
    )

    all_pass = all(a["pass"] for a in assertions)

    verdict = {
        "pass": all_pass,
        "config": {
            "qps": args.qps,
            "duration_s": args.duration_s,
            "max_age_s": args.max_age_s,
            "swr_s": args.swr_s,
            "bump_interval_s": args.bump_interval_s,
            "query": query,
        },
        "summary": {
            "queries": n_total,
            "ok": n_ok,
            "errors": n_error,
            "origin_data_fetches": origin_data_fetches,
            "origin_data_fetches_log_window": log_fetches_in_window,
            "hwm_start": stats0["hwm_version"],
            "hwm_end": stats1["hwm_version"],
            "max_lag_versions": max_lag,
            "lag_bound_versions": lag_bound,
            "fetch_ceiling": expected_fetch_ceiling,
        },
        "assertions": assertions,
    }

    verdict_path = os.path.join(args.out_dir, "assertions.json")
    with open(verdict_path, "w") as f:
        json.dump(verdict, f, indent=2)

    # ---- Console report ----
    print("=== Phase 0 freshness run ===")
    print(f"queries={n_total} ok={n_ok} errors={n_error}")
    print(
        f"origin HWM {stats0['hwm_version']} -> {stats1['hwm_version']}; "
        f"origin /data fetches during run = {origin_data_fetches}"
        + (
            f" (request-log window count = {log_fetches_in_window})"
            if log_fetches_in_window is not None
            else ""
        )
    )
    print(f"max lag = {max_lag} versions (bound {lag_bound})")
    print()
    print("fresh/stale transitions (every version_seen change):")
    print(f"  {'t_rel_s':>8}  {'hwm':>5}  {'seen':>5}  {'lag':>4}  band")
    prev_seen = object()
    for s in samples:
        if s.version_seen != prev_seen:
            print(
                f"  {s.t_send_rel_s:8.2f}  {str(s.hwm_at_send):>5}  "
                f"{str(s.version_seen):>5}  {str(s.lag_versions):>4}  {s.freshness}"
            )
            prev_seen = s.version_seen
    print()
    print("assertions:")
    for a in assertions:
        print(f"  [{'PASS' if a['pass'] else 'FAIL'}] {a['name']}: {a['detail']}")
    print()
    print(f"VERDICT: {'PASS' if all_pass else 'FAIL'}")
    print(f"wrote {samples_path}")
    print(f"wrote {verdict_path}")

    return 0 if all_pass else 1


def main() -> int:
    p = argparse.ArgumentParser(description="Phase 0 caching freshness load generator")
    p.add_argument(
        "--spiced-sql-url",
        default=os.environ.get("SPICED_SQL_URL", "http://127.0.0.1:8090/v1/sql"),
    )
    p.add_argument(
        "--origin-stats-url",
        default=os.environ.get("ORIGIN_STATS_URL", "http://127.0.0.1:9001/stats"),
    )
    p.add_argument("--dataset", default=os.environ.get("DATASET", "d1"))
    p.add_argument("--origin-name", default=os.environ.get("ORIGIN_NAME", "p1"))
    p.add_argument("--qps", type=float, default=float(os.environ.get("QPS", "10")))
    p.add_argument(
        "--duration-s", type=float, default=float(os.environ.get("DURATION_S", "20"))
    )
    p.add_argument("--request-timeout-s", type=float, default=5.0)
    p.add_argument(
        "--out-dir", default=os.environ.get("OUT_DIR", "/tmp/http_cache_phase0_run")
    )
    p.add_argument(
        "--origin-request-log",
        default=os.environ.get("ORIGIN_REQUEST_LOG", ""),
    )
    # Cache profile — must match the spicepod, used only by the oracle to
    # derive the staleness bound and fetch ceiling.
    p.add_argument("--max-age-s", type=float, default=float(os.environ.get("MAX_AGE_S", "3")))
    p.add_argument("--swr-s", type=float, default=float(os.environ.get("SWR_S", "6")))
    p.add_argument(
        "--bump-interval-s",
        type=float,
        default=float(os.environ.get("BUMP_INTERVAL_S", "1")),
    )
    p.add_argument("--lag-margin", type=int, default=int(os.environ.get("LAG_MARGIN", "3")))
    args = p.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
