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

"""Ad-hoc probe: does rate control couple two datasets on the SAME origin
(same host:port, different path), and leave a dataset on a DIFFERENT origin
alone?

Three datasets, each driven by an independent open-loop load generator (one
ThreadPoolExecutor per dataset -- a shared pool let one origin's backpressure
starve another's independent requests of workers in an earlier run of this
harness):

  d1 -> http://127.0.0.1:9001/data       (origin p1, its own host:port)
  d2 -> http://127.0.0.1:9002/data       (origin p2, faulted path)
  d3 -> http://127.0.0.1:9002/data.json  (origin p2, SAME host:port as d2,
                                           but its own path is NEVER faulted)

The fault is scoped to d2's path only via the origin server's `fault_paths`
control field, so d3's own HTTP responses stay healthy content throughout --
any admission/latency effect on d3 during the fault window can only come
from sharing p2's rate limiter and adaptive controller with d2, not from
d3's own requests erroring.

Usage: SPICED_BIN=... ./run_sameorigin_probe.sh  (wrapper starts origins +
spiced against spicepod.ratecontrol.sameorigin.yaml, then this script).
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any, Callable, Optional

sys.path.insert(0, os.path.dirname(__file__))

import multi_key  # noqa: E402
from harness import cli, http  # noqa: E402
from harness.oracle import (  # noqa: E402
    AssertionSet,
    decide_verdict,
    write_dicts_csv,
    write_verdict,
)
from harness.phases import Phases, phase_at  # noqa: E402
from harness.timeline import Step, Timeline  # noqa: E402
from metrics_scraper import MetricsScraper, load_config  # noqa: E402


@dataclass
class Sample:
    seq: int
    t_send_rel_s: float
    dataset: str
    phase: str
    latency_ms: float
    http_status: Any
    ok: bool
    error_kind: str


def classify(status: Any, _body: Any) -> tuple[bool, str]:
    if isinstance(status, int) and status == 200:
        return True, "none"
    if status == "transport":
        return False, "transport"
    return False, "http_error"


class Driver:
    """Open-loop pacer for one dataset: own thread, own pool, own query fn."""

    def __init__(
        self,
        dataset: str,
        query_fn: Callable[[], str],
        sql_url: str,
        request_timeout_s: float,
        max_workers: int,
    ):
        self.dataset = dataset
        self.query_fn = query_fn
        self.sql_url = sql_url
        self.request_timeout_s = request_timeout_s
        self.pool = ThreadPoolExecutor(max_workers=max_workers)
        self.samples: list[Sample] = []
        self._lock = threading.Lock()
        self._seq = 0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _fire(self, t0: float, tl: Phases) -> None:
        with self._lock:
            self._seq += 1
            seq = self._seq
        query = self.query_fn()
        t_send = time.time()
        t_rel = t_send - t0
        status, body = http.sql(self.sql_url, query, self.request_timeout_s)
        latency_ms = (time.time() - t_send) * 1000.0
        ok, error_kind = classify(status, body)
        s = Sample(
            seq=seq,
            t_send_rel_s=round(t_rel, 3),
            dataset=self.dataset,
            phase=phase_at(t_rel, tl),
            latency_ms=round(latency_ms, 1),
            http_status=status,
            ok=ok,
            error_kind=error_kind,
        )
        with self._lock:
            self.samples.append(s)

    def start(self, qps: float, t0: float, tl: Phases) -> None:
        interval = 1.0 / qps if qps > 0 else 1.0

        def _loop() -> None:
            next_send = time.time()
            while not self._stop.is_set():
                now = time.time()
                if now - t0 >= tl.run_end:
                    break
                if now >= next_send:
                    self.pool.submit(self._fire, t0, tl)
                    next_send = max(now, next_send) + interval
                else:
                    time.sleep(min(0.01, next_send - now))

        self._thread = threading.Thread(target=_loop)
        self._thread.start()

    def join(self) -> None:
        if self._thread:
            self._thread.join()

    def shutdown(self) -> None:
        self._stop.set()
        self.pool.shutdown(wait=False)


def latency_stats(vals: list[float]) -> dict[str, float]:
    if not vals:
        return {}
    v = sorted(vals)
    n = len(v)
    return {
        "n": n,
        "min": v[0],
        "p50": v[n // 2],
        "p99": v[int(n * 0.99)] if n > 1 else v[0],
        "max": v[-1],
    }


def run(args: argparse.Namespace) -> int:
    t0 = args.t0 if args.t0 > 0 else time.time()
    os.makedirs(args.out_dir, exist_ok=True)

    tl = Phases(
        fault_start=args.warmup_s,
        fault_end=args.warmup_s + args.fault_s,
        run_end=args.warmup_s + args.fault_s + args.recovery_s,
    )

    # d2's path only -- d3 (same origin, different path) must stay content-healthy.
    fault_profile = {
        "id": "p2-data-503-heavy",
        "mode": "status",
        "error_status": 503,
        "error_rate": args.fault_error_rate,
        "fault_paths": ["/data"],
        "latency_ms": {"base": 20, "jitter": 10},
        "seed": 12345,
    }
    healthy = {"id": "healthy", "mode": "healthy", "error_rate": 0.0, "fault_paths": []}

    steps = [
        Step(at_s=0.0, profile=dict(healthy), note="warmup-healthy"),
        Step(at_s=tl.fault_start, profile=fault_profile, note="inject-fault-d2-path-only"),
        Step(at_s=tl.fault_end, profile=dict(healthy), note="recover"),
    ]
    timeline = Timeline(t0, args.p2_control_url, steps)

    try:
        http.post_json(args.p1_control_url, dict(healthy))
        http.post_json(args.p2_control_url, dict(healthy))
    except Exception as e:  # noqa: BLE001
        print(f"failed to reach an origin control API: {e}", file=sys.stderr)
        return 1

    pick_key = multi_key.make_key_picker(args.seed)

    # origin column value = the origin PROCESS's ORIGIN_NAME env var (see
    # origin/server.py), not the path -- d2 and d3 both come from process p2.
    origin_name_by_dataset = {"d1": "p1", "d2": "p2", "d3": "p2"}

    def make_query_fn(dataset: str) -> Callable[[], str]:
        origin_name = origin_name_by_dataset[dataset]

        def _q() -> str:
            key = pick_key()
            return multi_key.with_request_query(
                f"SELECT version FROM {dataset} WHERE origin = '{origin_name}'", key
            )

        return _q

    drivers = {
        ds: Driver(ds, make_query_fn(ds), args.spiced_sql_url, args.request_timeout_s, args.max_workers)
        for ds in ("d1", "d2", "d3")
    }

    cfg = load_config(args.metrics_config)
    scraper = MetricsScraper(cfg, t0=t0, interval_s=args.scrape_interval_s, endpoint=args.metrics_endpoint)

    scraper.start()
    timeline.start()
    for ds, qps in (("d1", args.p1_qps), ("d2", args.p2_qps), ("d3", args.p3_qps)):
        drivers[ds].start(qps, t0, tl)

    for drv in drivers.values():
        drv.join()

    scraper.scrape_once()
    scraper.stop()
    for drv in drivers.values():
        drv.shutdown()

    samples_path = os.path.join(args.out_dir, "samples.csv")
    all_samples = [s for drv in drivers.values() for s in drv.samples]
    write_dicts_csv(samples_path, [asdict(s) for s in all_samples])
    scraper.write_csv(os.path.join(args.out_dir, "metrics.csv"))
    # report.py's _driver_windows() reads this to shade the fault window on
    # every chart -- without it the dashboard has no visual fault marker.
    write_dicts_csv(os.path.join(args.out_dir, "driver_events.csv"), [asdict(e) for e in timeline.events])

    fs, fe = tl.fault_start, tl.fault_end
    assertions = AssertionSet()

    def origin_key_for(port: int) -> Optional[str]:
        for base in (
            "dataset_http_adaptive_rate_control_admission_coefficient_permille",
            "dataset_http_adaptive_rate_control_effective_limit",
        ):
            for o in scraper.origins(base):
                if f":{port}" in o:
                    return o
        return None

    p1_key = origin_key_for(9001)
    p2_key = origin_key_for(9002)

    p1_adm_min = (
        scraper.min_in_window(
            "dataset_http_adaptive_rate_control_admission_coefficient_permille", p1_key, fs, fe
        )
        if p1_key
        else None
    )
    p2_adm_min = (
        scraper.min_in_window(
            "dataset_http_adaptive_rate_control_admission_coefficient_permille", p2_key, fs, fe
        )
        if p2_key
        else None
    )

    assertions.add(
        "different_origin_unaffected",
        p1_adm_min is not None and p1_adm_min >= 950.0,
        f"d1's origin (:9001) min admission during fault = {p1_adm_min} (need >= 950)",
    )
    assertions.add(
        "faulted_origin_throttles",
        p2_adm_min is not None and p2_adm_min < 800.0,
        f"p2's origin (:9002) min admission during fault = {p2_adm_min} (need < 800)",
    )

    # d3's own content is never faulted (fault_paths scoped to /data), so any
    # d3 error/latency during the fault can only be shared-limiter contention.
    d3_fault = [s for s in all_samples if s.dataset == "d3" and s.phase == "fault"]
    d3_fault_ok = [s for s in d3_fault if s.ok]
    d3_lat = latency_stats([s.latency_ms for s in d3_fault])
    d3_healthy_lat = latency_stats(
        [s.latency_ms for s in all_samples if s.dataset == "d3" and s.phase == "warmup"]
    )
    d3_degraded = bool(d3_lat) and bool(d3_healthy_lat) and d3_lat["p50"] > d3_healthy_lat["p50"] * 3
    assertions.add(
        "d3_shares_p2_throttle_despite_never_faulted_itself",
        d3_degraded,
        f"d3 (origin :9002, path /data.json, own content never faulted) warmup p50={d3_healthy_lat.get('p50')}ms "
        f"vs fault-window p50={d3_lat.get('p50')}ms (n={d3_lat.get('n')}, ok={len(d3_fault_ok)}/{len(d3_fault)}) "
        f"-- expected fault p50 >> warmup p50 if the shared origin limiter couples d2's throttle onto d3",
    )

    verdict_name, exit_code = decide_verdict(correctness_ok=True, assertions=assertions.as_list())
    write_verdict(
        os.path.join(args.out_dir, "assertions.json"),
        {
            "scenario": "sameorigin-probe",
            "verdict": verdict_name,
            "exit_code": exit_code,
            "config": {
                "p1_qps": args.p1_qps,
                "p2_qps": args.p2_qps,
                "p3_qps": args.p3_qps,
                "fault_error_rate": args.fault_error_rate,
                "p1_origin_key": p1_key,
                "p2_origin_key": p2_key,
                "fault_profile": fault_profile,
                "warmup_s": args.warmup_s,
                "fault_s": args.fault_s,
            },
            "evidence": {
                "p1_admission_min_fault": p1_adm_min,
                "p2_admission_min_fault": p2_adm_min,
                "d1_n": sum(1 for s in all_samples if s.dataset == "d1"),
                "d2_n": sum(1 for s in all_samples if s.dataset == "d2"),
                "d3_n": sum(1 for s in all_samples if s.dataset == "d3"),
                "d3_warmup_latency": d3_healthy_lat,
                "d3_fault_latency": d3_lat,
                "d3_fault_ok_count": len(d3_fault_ok),
                "d3_fault_total": len(d3_fault),
            },
            "assertions": assertions.as_list(),
        },
    )

    for a in assertions:
        tag = "PASS" if a["pass"] else "FAIL"
        print(f"[{tag}] {a['name']}: {a['detail']}")
    print(f"\nVERDICT: {verdict_name}  (exit {exit_code})")
    return exit_code


def parse_args() -> argparse.Namespace:
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser()
    p.add_argument("--spiced-sql-url", default=cli.env_str("SPICED_SQL_URL", "http://127.0.0.1:8090/v1/sql"))
    p.add_argument("--metrics-endpoint", default=cli.env_str("METRICS_ENDPOINT", ""))
    p.add_argument(
        "--metrics-config", default=cli.env_str("METRICS_CONFIG", os.path.join(here, "metrics_config.json"))
    )
    p.add_argument("--p1-control-url", default=cli.env_str("P1_CONTROL_URL", "http://127.0.0.1:9001/control"))
    p.add_argument("--p2-control-url", default=cli.env_str("P2_CONTROL_URL", "http://127.0.0.1:9002/control"))
    p.add_argument("--t0", type=float, default=cli.env_float("HARNESS_T0", 0.0))
    p.add_argument("--warmup-s", type=float, default=cli.env_float("WARMUP_S", 15.0))
    p.add_argument("--fault-s", type=float, default=cli.env_float("FAULT_S", 60.0))
    p.add_argument("--recovery-s", type=float, default=cli.env_float("RECOVERY_S", 45.0))
    p.add_argument("--p1-qps", type=float, default=cli.env_float("P1_QPS", 50.0))
    p.add_argument("--p2-qps", type=float, default=cli.env_float("P2_QPS", 50.0))
    p.add_argument("--p3-qps", type=float, default=cli.env_float("P3_QPS", 20.0))
    p.add_argument("--fault-error-rate", type=float, default=cli.env_float("FAULT_ERROR_RATE", 0.9))
    p.add_argument("--max-workers", type=int, default=cli.env_int("MAX_WORKERS", 64))
    p.add_argument("--request-timeout-s", type=float, default=cli.env_float("REQUEST_TIMEOUT_S", 15.0))
    p.add_argument("--scrape-interval-s", type=float, default=cli.env_float("SCRAPE_INTERVAL_S", 0.1))
    p.add_argument("--seed", type=int, default=cli.env_int("SEED", 12345))
    p.add_argument("--out-dir", default=cli.env_str("OUT_DIR", "/tmp/http_cache_sameorigin_run"))
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(run(parse_args()))
