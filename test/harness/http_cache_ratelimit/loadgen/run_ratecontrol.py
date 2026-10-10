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

"""Run the HTTP rate-control scenario catalog and report a verdict per scenario.

    ./run_ratecontrol.sh all
    ./run_ratecontrol.sh cluster
    ./run_ratecontrol.sh multi-origin-isolation

The origins' arrival logs decide the rate bounds, the shared state object
decides the cluster invariants, and each replica's own `/metrics` decides
whether the replicas agree. Artifacts land under `--run-root/<scenario>/`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from harness.oracle import AssertionSet, decide_verdict  # noqa: E402
from harness.phases import Phases  # noqa: E402
from harness.timeline import Step, Timeline  # noqa: E402
from metrics_scraper import MetricsScraper  # noqa: E402
from ratecontrol import rates, runner, state  # noqa: E402
from ratecontrol.scenarios import GROUPS, SCENARIOS, Bound, Scenario  # noqa: E402

SPICEPOD_NAME = "http-ratecontrol"
#: `rate_control_failure_threshold` when a scenario leaves it unset.
DEFAULT_FAILURE_THRESHOLD = 0.10


@dataclass
class PhaseWindow:
    name: str
    start_ms: int
    end_ms: int


def phase_spans(scenario: Scenario) -> dict[str, tuple[float, float]]:
    """Each phase's full span in seconds from t0, before settling is applied."""
    warmup_end = scenario.warmup_s
    fault_end = warmup_end + scenario.fault_s
    run_end = fault_end + scenario.recovery_s
    return {
        "warmup": (0.0, warmup_end),
        "fault": (warmup_end, fault_end),
        "recovery": (fault_end, run_end),
    }


def phase_windows(scenario: Scenario, t0: float) -> dict[str, PhaseWindow]:
    """The measured span of each phase: the phase minus its settling seconds."""
    return {
        name: PhaseWindow(
            name,
            int((t0 + lo + scenario.settle.of(name)) * 1000),
            int((t0 + hi) * 1000),
        )
        for name, (lo, hi) in phase_spans(scenario).items()
    }


def check_timing(
    scenario: Scenario,
    arrivals: list[rates.Arrival],
    t0: float,
    assertions: AssertionSet,
) -> None:
    """How long the controller took to react, and to give the rate back.

    Measured from the moment the fault profile was applied, not from the first
    failed request, so a slow first draw counts against the controller rather
    than being hidden.
    """
    timing = scenario.timing
    if timing is None:
        return
    spans = phase_spans(scenario)
    fault_start, fault_end = spans["fault"]
    _, run_end = spans["recovery"]

    if timing.throttle_to is not None and timing.throttle_within_s is not None:
        took = rates.first_second_where(
            arrivals,
            int((t0 + fault_start) * 1000),
            int((t0 + fault_end) * 1000),
            lambda rate: rate <= timing.throttle_to,
        )
        assertions.add(
            f"timing: the rate falls to {timing.throttle_to:g} rps within"
            f" {timing.throttle_within_s:g}s of the origin failing",
            took is not None and took <= timing.throttle_within_s,
            f"took {took}s" if took is not None else "never reached that rate in the fault phase",
        )

    if timing.recover_to is not None and timing.recover_within_s is not None:
        took = rates.first_second_where(
            arrivals,
            int((t0 + fault_end) * 1000),
            int((t0 + run_end) * 1000),
            lambda rate: rate >= timing.recover_to,
        )
        assertions.add(
            f"timing: the rate returns to {timing.recover_to:g} rps within"
            f" {timing.recover_within_s:g}s of the origin recovering",
            took is not None and took <= timing.recover_within_s,
            f"took {took}s" if took is not None else "never returned to that rate",
        )


def stalled_phases(
    scenario: Scenario,
    queries: list[runner.Query],
    windows: dict[str, PhaseWindow],
) -> list[str]:
    """Phases where the load generator itself stopped asking.

    A phase with no arrivals at the origin reads as a rate of zero, which is
    indistinguishable from a runtime that stopped sending -- and the harness has
    now produced that reading twice for reasons that had nothing to do with the
    runtime (ephemeral-port exhaustion, and a multi-minute stall of the
    generator process). The generator's own query log settles it: if it recorded
    no queries in the window either, nothing was asked, so the run is BLOCKED
    rather than FAIL. A real throttle to zero still shows queries, refused.
    """
    stalled = []
    for bound in scenario.bounds:
        window = windows[bound.phase]
        asked = sum(1 for q in queries if window.start_ms <= q.t_epoch_ms < window.end_ms)
        if asked == 0 and bound.phase not in stalled:
            stalled.append(bound.phase)
    return stalled


def check_bound(
    bound: Bound,
    arrivals: list[rates.Arrival],
    window: PhaseWindow,
    warmup: PhaseWindow,
    assertions: AssertionSet,
) -> None:
    selected = rates.select(arrivals, bound.where)
    stats = rates.rate_stats(selected, window.start_ms, window.end_ms)
    in_window = [a for a in selected if window.start_ms <= a.epoch_ms < window.end_ms]
    label = f"{bound.phase}[{bound.where.label()}]"

    if bound.min_total is not None:
        assertions.add(
            f"{label}: {bound.claim} (traffic present)",
            stats.total >= bound.min_total,
            f"{stats.total} arrivals, need >= {bound.min_total}",
        )
    if bound.min_p99 is not None:
        assertions.add(
            f"{label}: {bound.claim}",
            stats.p99 >= bound.min_p99,
            f"p99={stats.p99:g} >= {bound.min_p99:g}? ({stats})",
        )
    if bound.max_p99 is not None:
        assertions.add(
            f"{label}: {bound.claim}",
            stats.p99 <= bound.max_p99,
            f"p99={stats.p99:g} <= {bound.max_p99:g}? ({stats})",
        )
    if bound.max_rolling_5s is not None:
        rolling = rates.max_rolling(selected, window.start_ms, window.end_ms, 5)
        assertions.add(
            f"{label}: five consecutive seconds stay within five budgets",
            rolling <= bound.max_rolling_5s,
            f"worst 5s window carried {rolling} <= {bound.max_rolling_5s:g}? ({stats})",
        )
    if bound.max_fraction_of_warmup is not None:
        baseline = rates.rate_stats(selected, warmup.start_ms, warmup.end_ms)
        limit = bound.max_fraction_of_warmup * baseline.p99
        assertions.add(
            f"{label}: {bound.claim}",
            baseline.p99 > 0 and stats.p99 <= limit,
            f"p99={stats.p99:g} <= {bound.max_fraction_of_warmup:g} x its warmup"
            f" p99 of {baseline.p99:g} (= {limit:g})?",
        )
    if bound.only_statuses is not None:
        seen = rates.status_counts(in_window)
        unexpected = {s: n for s, n in seen.items() if s not in bound.only_statuses}
        assertions.add(
            f"{label}: {bound.claim}",
            not unexpected,
            f"statuses={seen}",
        )


def check_admission_ratio(
    scenario: Scenario,
    scrapers: dict[str, MetricsScraper],
    fault: PhaseWindow,
    assertions: AssertionSet,
) -> None:
    """Did the runtime's own telemetry say the controller reacted?

    Independent of what reached the origin: the arrival rate also moves when
    the load generator does, the admission ratio only moves when the controller
    does.
    """
    if scenario.fault_admission_ratio_below is None and scenario.fault_admission_ratio_above is None:
        return
    values = [
        sample.value
        for scraper in scrapers.values()
        for sample in scraper.samples
        if sample.metric_name == "dataset_http_rate_control_adaptive_admission_ratio"
        and fault.start_ms <= sample.scrape_epoch_ms < fault.end_ms
    ]
    lowest = min(values) if values else None
    detail = (
        f"lowest published ratio {lowest:.3f} over {len(values)} scrapes"
        if lowest is not None
        else "the metric was never scraped in the fault phase"
    )
    if scenario.fault_admission_ratio_below is not None:
        assertions.add(
            f"metrics: the admission coefficient falls below"
            f" {scenario.fault_admission_ratio_below:g} while the origin fails",
            lowest is not None and lowest < scenario.fault_admission_ratio_below,
            detail,
        )
    if scenario.fault_admission_ratio_above is not None:
        assertions.add(
            f"metrics: the admission coefficient never falls below"
            f" {scenario.fault_admission_ratio_above:g}",
            lowest is not None and lowest >= scenario.fault_admission_ratio_above,
            detail,
        )


def check_metrics_agreement(
    scenario: Scenario, scrapers: dict[str, MetricsScraper], assertions: AssertionSet
) -> None:
    """Do the replicas publish the same throttle, having never spoken?

    Compared only across **settled** seconds: a second counts only when no
    replica's published value changed from the second before it. Each replica
    refreshes its lease on its own tick phase and publishes what the window it
    last leased said, so a 1 Hz scrape catches different replicas at different
    points in the window cycle. With two replicas the phases often align; with
    three one of them reliably does not, and it then reports exactly what its
    peers report one second later. That is a sampling offset, not a
    disagreement, and it cannot be told apart from one while the value is
    moving. Where nothing is moving, a difference can only be a real one.

    `adaptive_admission_ratio` is the only coefficient the runtime exports now;
    `cluster_effective_burst` and the two `lease_acquire_*` series went with the
    persisted budget. The shared state object backs this up from the other side:
    the coefficient its counts imply is compared with the ratio published here.
    """
    if scenario.topology.replicas < 2 or scenario.topology.cluster is None:
        return
    tracked = ("dataset_http_rate_control_adaptive_admission_ratio",)
    by_second: dict[tuple[int, str], dict[str, tuple[tuple[str, float], ...]]] = defaultdict(dict)
    for replica, scraper in scrapers.items():
        for sample in scraper.samples:
            if sample.metric_name not in tracked:
                continue
            key = (sample.scrape_epoch_ms // 1000, sample.origin)
            by_second[key][replica] = by_second[key].get(replica, ()) + (
                (sample.metric_name, sample.value),
            )

    previous: dict[str, dict[str, tuple]] = defaultdict(dict)
    comparable = agree = moving = 0
    for second, origin in sorted(by_second):
        values = {
            replica: tuple(sorted(series)) for replica, series in by_second[(second, origin)].items()
        }
        if len(values) < 2:
            continue
        settled = all(previous[origin].get(replica) == value for replica, value in values.items())
        previous[origin] = values
        if not settled:
            moving += 1
            continue
        comparable += 1
        agree += len(set(values.values())) == 1

    if not comparable:
        assertions.add(
            "metrics: replicas publish a comparable throttle",
            False,
            f"no settled second had two replicas scraped together ({moving} moving)",
        )
        return
    assertions.add(
        "metrics: every replica derives the same throttle, with no replica-to-replica traffic",
        agree >= 0.95 * comparable,
        f"identical in {agree}/{comparable} settled seconds"
        f" ({moving} seconds skipped because a published value was still moving)",
    )

    # Phase-independent corroboration: the replicas should sweep the same
    # range. Set equality was the check while the metric was a whole-token
    # budget with a handful of distinct values; the admission ratio is a float
    # that moves every window, so which exact values a 1 Hz scrape catches is
    # down to tick phase, not agreement. The range is not: a replica deriving
    # its own coefficient from its own view would bottom out somewhere else.
    published: dict[str, list[float]] = defaultdict(list)
    for replica, scraper in scrapers.items():
        for sample in scraper.samples:
            if sample.metric_name == tracked[0]:
                published[replica].append(sample.value)
    spans = {
        replica: (min(values), max(values))
        for replica, values in published.items()
        if values
    }
    if len(spans) >= 2:
        lows = [low for low, _high in spans.values()]
        highs = [high for _low, high in spans.values()]
        assertions.add(
            "metrics: the replicas sweep the same range of coefficients",
            max(lows) - min(lows) <= 0.1 and max(highs) - min(highs) <= 0.1,
            ", ".join(f"{replica} [{low:.3f}, {high:.3f}]" for replica, (low, high) in sorted(spans.items())),
        )


def run_scenario(scenario: Scenario, args: argparse.Namespace) -> dict:
    paths = runner.RunPaths(os.path.join(args.run_root, scenario.name))
    fleet = runner.Fleet()
    assertions = AssertionSet()
    blocked = False
    t0 = time.time()

    busy = [port for port in runner.scenario_ports(scenario) if not runner.port_free(port)]
    if busy:
        raise runner.RunError(f"ports already bound: {busy}")

    scrapers: dict[str, MetricsScraper] = {}
    arrivals: list[rates.Arrival] = []
    queries: list[runner.Query] = []
    snapshots: dict[str, str] = {}

    try:
        origin_logs = runner.start_origins(scenario, paths, fleet, args.python, args.harness_dir, t0)
        if scenario.topology.cluster and scenario.topology.cluster.backend == "s3":
            runner.start_rustfs(paths, fleet, args.rustfs, scenario.topology.cluster.bucket)

        replicas = runner.start_replicas(scenario, paths, fleet, args.spiced, SPICEPOD_NAME)

        if scenario.expect_startup_error is not None:
            needle = scenario.expect_startup_error
            found = runner.wait_for(
                lambda: any(needle in open(p.log_path, encoding="utf-8").read() for p in replicas),
                timeout_s=60,
            )
            logs = "\n".join(open(p.log_path, encoding="utf-8").read() for p in replicas)
            line = next(
                (ln for ln in logs.splitlines() if needle in ln and ("ERROR" in ln or "WARN" in ln)),
                "",
            )
            assertions.add(
                f"startup: {scenario.claim}",
                found,
                line[:300] if line else f"{needle!r} not found in any replica log",
            )
            verdict_name, code = decide_verdict(correctness_ok=True, assertions=assertions)
            return _finish(scenario, paths, assertions, verdict_name, code, {}, [], [], {})

        runner.await_ready(scenario, replicas)

        for index, replica in enumerate(scenario.topology.replica_names()):
            scraper = MetricsScraper(
                args.metrics_config,
                t0,
                interval_s=1.0,
                endpoint=f"http://127.0.0.1:{runner.METRICS_PORT_BASE + index}/metrics",
            )
            scraper.start()
            scrapers[replica] = scraper

        phases = Phases(
            fault_start=scenario.warmup_s,
            fault_end=scenario.warmup_s + scenario.fault_s,
            run_end=scenario.warmup_s + scenario.fault_s + scenario.recovery_s,
        )
        timelines = []
        for fault in scenario.faults:
            origin = scenario.topology.origin(fault.origin)
            timeline = Timeline(
                t0,
                origin.control_url,
                [
                    Step(phases.fault_start, fault.profile, note=f"fault:{fault.origin}"),
                    Step(phases.fault_end, dict(runner.HEALTHY), note=f"healthy:{fault.origin}"),
                ],
            )
            timeline.start()
            timelines.append(timeline)

        stop = threading.Event()
        queries = runner.drive(scenario, phases, t0, stop)
        print(f"  driving {scenario.topology.replicas} replica(s) x "
              f"{len(scenario.topology.datasets)} dataset(s) for {phases.run_end:.0f}s", flush=True)
        while time.time() - t0 < phases.run_end:
            time.sleep(0.5)
        stop.set()
        time.sleep(1.0)

        snapshots = runner.snapshot_state(scenario, paths)
        for scraper in scrapers.values():
            scraper.stop()
        arrivals = runner.collect_arrivals(scenario, origin_logs)
    finally:
        fleet.stop_all()
        runner.wait_for(
            lambda: all(runner.port_free(port) for port in runner.scenario_ports(scenario)),
            timeout_s=30,
            interval_s=0.5,
        )

    windows = phase_windows(scenario, t0)
    stalled = stalled_phases(scenario, queries, windows)
    if stalled:
        blocked = True
        assertions.add(
            "harness: the load generator kept asking for the whole run",
            False,
            f"no query was recorded in phase(s) {stalled}; the generator stalled,"
            " so this run measures the harness rather than the runtime",
        )
    for bound in scenario.bounds:
        check_bound(bound, arrivals, windows[bound.phase], windows["warmup"], assertions)
    check_timing(scenario, arrivals, t0, assertions)
    check_admission_ratio(scenario, scrapers, windows["fault"], assertions)

    cluster = scenario.topology.cluster
    if cluster is not None:
        for origin in scenario.topology.origins:
            path = snapshots.get(origin.name)
            if path is None:
                assertions.add(
                    f"state[{origin.name}]: the shared state object exists",
                    False,
                    "no state object was written for this origin",
                )
                continue
            shared = state.SharedState.load(path)
            origin_arrivals = rates.select(arrivals, rates.Slice(origin=origin.name))
            threshold = origin.rate_control.failure_threshold
            # The ratio each replica published, by unix second. Window ids are
            # unix seconds while `refresh_interval` is 1s, which is what makes
            # the file's counts and this series comparable.
            published_ratio = {
                sample.scrape_epoch_ms // 1000: sample.value
                for scraper in scrapers.values()
                for sample in scraper.samples
                if sample.metric_name == "dataset_http_rate_control_adaptive_admission_ratio"
                and sample.origin.endswith(f":{origin.port}")
            }
            for check in state.check_state(
                shared,
                origin_arrivals,
                failure_threshold=(
                    float(threshold.rstrip("%")) / 100
                    if threshold
                    else DEFAULT_FAILURE_THRESHOLD
                ),
                published_ratio=published_ratio,
                replicas=scenario.topology.replicas,
            ):
                assertions.add(f"{origin.name} {check.name}", check.passed, check.detail)
            with open(paths.path(f"state_timeline_{origin.name}.txt"), "w", encoding="utf-8") as fh:
                fh.write("\n".join(state.timeline_rows(shared, origin_arrivals)) + "\n")

    check_metrics_agreement(scenario, scrapers, assertions)

    verdict_name, code = decide_verdict(
        correctness_ok=True, assertions=assertions, blocked=blocked
    )
    return _finish(scenario, paths, assertions, verdict_name, code, windows, arrivals, queries, scrapers)


def _finish(
    scenario: Scenario,
    paths: runner.RunPaths,
    assertions: AssertionSet,
    verdict_name: str,
    code: int,
    windows: dict,
    arrivals: list,
    queries: list,
    scrapers: dict,
) -> dict:
    if queries:
        runner.write_queries(paths.path("queries.csv"), queries)
    if scrapers:
        runner.write_metrics(paths.path("metrics.csv"), scrapers)

    table = []
    for name, window in windows.items():
        overall = rates.rate_stats(arrivals, window.start_ms, window.end_ms)
        row = {"phase": name, "all": overall.as_dict(), "by": {}}
        for key, slicer in (
            ("origin", lambda a: a.origin),
            ("dataset", lambda a: a.dataset),
            ("replica", lambda a: a.replica),
        ):
            groups: dict[str, list] = defaultdict(list)
            for arrival in arrivals:
                groups[slicer(arrival)].append(arrival)
            if len(groups) > 1:
                row["by"][key] = {
                    value: rates.rate_stats(items, window.start_ms, window.end_ms).as_dict()
                    for value, items in sorted(groups.items())
                }
        row["statuses"] = rates.status_counts(
            [a for a in arrivals if window.start_ms <= a.epoch_ms < window.end_ms]
        )
        table.append(row)

    if arrivals:
        counts = rates.per_second(arrivals)
        first, last = min(counts), max(counts)
        with open(paths.path("arrival_curve.txt"), "w", encoding="utf-8") as handle:
            handle.write(f"arrivals per whole second at the origin(s) — {scenario.name}\n\n")
            for second in range(first, last + 1):
                count = counts.get(second, 0)
                handle.write(f"{second - first:>4}s {count:>4} {'#' * count}\n")

    verdict = {
        "scenario": scenario.name,
        "claim": scenario.claim,
        "verdict": verdict_name,
        "exit_code": code,
        "phases": table,
        "assertions": assertions.as_list(),
    }
    runner.write_json(paths.path("verdict.json"), verdict)

    print(f"  {'phase':<10}{'arrivals':>10}{'p50':>7}{'p99':>7}{'peak':>7}   breakdown")
    for row in table:
        stats = row["all"]
        breakdown = ""
        for key in ("origin", "dataset", "replica"):
            if key in row["by"]:
                breakdown += "  " + " ".join(
                    f"{value}={inner['p99_rps']:g}" for value, inner in row["by"][key].items()
                )
        print(
            f"  {row['phase']:<10}{stats['total']:>10}{stats['p50_rps']:>7g}"
            f"{stats['p99_rps']:>7g}{stats['peak_rps']:>7}  {breakdown}"
        )
    for item in assertions:
        print(f"  [{'PASS' if item['pass'] else 'FAIL'}] {item['name']} — {item['detail']}")
    print(f"  VERDICT {verdict_name}")
    return verdict


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", nargs="?", default="all", help="a scenario name or a group")
    parser.add_argument("--run-root", default="/tmp/http_ratecontrol_run")
    parser.add_argument("--spiced", default=os.path.expanduser("~/.spice/bin/spiced"))
    parser.add_argument("--rustfs", default=os.path.expanduser("~/.spice/bin/rustfs"))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--harness-dir", default=os.path.dirname(HERE))
    parser.add_argument("--list", action="store_true", help="print the catalog and exit")
    args = parser.parse_args()

    if args.list:
        for group, names in GROUPS.items():
            if group == "all":
                continue
            print(f"\n{group}:")
            for name in names:
                print(f"  {name:<32}{SCENARIOS[name].claim}")
        return 0

    names = GROUPS.get(args.target, (args.target,))
    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        print(f"unknown scenario(s): {unknown}", file=sys.stderr)
        return 2

    with open(os.path.join(HERE, "metrics_config.json"), encoding="utf-8") as handle:
        args.metrics_config = json.load(handle)

    results = []
    for name in names:
        scenario = SCENARIOS[name]
        print(f"\n=== {name} — {scenario.claim}", flush=True)
        try:
            results.append(run_scenario(scenario, args))
        except runner.RunError as error:
            print(f"  BLOCKED: {error}", flush=True)
            results.append({"scenario": name, "verdict": "BLOCKED", "exit_code": 2, "error": str(error)})

    print("\n================ summary ================")
    for result in results:
        print(f"{result['verdict']:<8} {result['scenario']}")
    runner.write_json(os.path.join(args.run_root, "summary.json"), results)
    return 0 if all(r["verdict"] == "PASS" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
