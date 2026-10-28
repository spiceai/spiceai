#!/usr/bin/env python3
# Copyright 2026 The Spice.ai OSS Authors
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

"""Extract client P99, throughput, resources and uplink RPC counts from a run.

Retains each repetition rather than averaging quantiles. The RPC and resource
windows cover the whole arm (warmup, baseline and load); client latency and
throughput use testoperator's measured load phase only.
"""

import argparse
import ast
import hashlib
import json
import math
import re
from pathlib import Path


def load_table(log):
    text = log[log.rindex("Load test metrics:") :]
    header, rows = None, []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        values = [value.strip() for value in line.strip("|").split("|")]
        if "query_name" in values:
            header = values
        elif header and len(values) == len(header):
            row = dict(zip(header, values))
            if row["status"] != "passed":
                raise ValueError(f"Query did not pass: {row}")
            rows.append(row)
    if not rows:
        raise ValueError("No load metrics found")
    return rows


def rpc_counts(path):
    counts = {}
    for line in path.read_text().splitlines():
        match = re.fullmatch(r"flight_requests\{(.*)\} ([0-9.]+)", line)
        if not match:
            continue
        labels = dict(re.findall(r'(\w+)="([^"\n]*)"', match[1]))
        key = labels["method"] + "/" + labels.get("command", "")
        counts[key] = counts.get(key, 0) + int(float(match[2]))
    return counts


def percentile(values, quantile):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)] if ordered else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    provenance = json.loads((args.run / "provenance.json").read_text())
    arms = []
    incomplete = []
    for directory in args.run.glob("bench-cache*-c*-*-*"):
        match = re.fullmatch(
            r"bench-cache(True|False)-c(\d+)-(\d+)-(server|sidecar)", directory.name
        )
        if not match:
            continue
        log = directory / "testoperator.log"
        if not (directory / "upstream-metrics-after.txt").exists():
            incomplete.append(directory.name)
            continue
        rows = load_table(log.read_text())
        started = min(int(row["started_at"]) for row in rows)
        finished = max(int(row["finished_at"]) for row in rows)
        iterations = sum(int(row["iterations"]) for row in rows)
        resources = [
            json.loads(line)
            for line in (directory / "resources.jsonl").read_text().splitlines()
        ]
        health = [sample["health_ms"] for sample in resources if "health_ms" in sample]
        before = rpc_counts(directory / "upstream-metrics-after-warmup.txt")
        after = rpc_counts(directory / "upstream-metrics-after.txt")
        arms.append(
            {
                "cache_hit": match[1] == "True",
                "concurrency": int(match[2]),
                "arm": int(match[3]),
                "mode": match[4],
                "directory": str(directory),
                "iterations": iterations,
                "elapsed_ms": finished - started,
                "queries_per_second": iterations * 1000 / (finished - started),
                "queries": [
                    {
                        "name": row["query_name"],
                        "iterations": int(row["iterations"]),
                        "p99_ms": int(row["percentile_99_duration_ms"]),
                        "p95_ms": int(row["percentile_95_duration_ms"]),
                        "min_ms": int(row["min_duration_ms"]),
                        "max_ms": int(row["max_duration_ms"]),
                    }
                    for row in rows
                ],
                "peak_rss_bytes": max(
                    sample.get("rss_bytes", 0) for sample in resources
                ),
                "upstream_peak_rss_bytes": max(
                    (
                        sample["upstream_rss_bytes"]
                        for sample in resources
                        if "upstream_rss_bytes" in sample
                    ),
                    default=None,
                ),
                "health_samples": len(health),
                "health_p99_ms": percentile(health, 0.99),
                "resource_errors": [
                    sample for sample in resources if "error" in sample
                ],
                "uplink_rpcs_whole_arm": {
                    key: after.get(key, 0) - before.get(key, 0)
                    for key in sorted(set(before) | set(after))
                },
                "testoperator_log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
            }
        )
    arms.sort(key=lambda arm: (arm["cache_hit"], arm["concurrency"], arm["arm"]))
    expected = (
        4
        * len(ast.literal_eval(provenance["arguments"]["concurrency"]))
        * len(ast.literal_eval(provenance["arguments"]["cache_states"]))
    )
    args.output.write_text(
        json.dumps(
            {
                "provenance": provenance,
                "expected_arms": expected,
                "complete": len(arms) == expected,
                "incomplete_arms": incomplete,
                "arms": arms,
            },
            indent=2,
        )
    )
    for arm in arms:
        latencies = ", ".join(
            f"{query['name']}={query['p99_ms']}" for query in arm["queries"]
        )
        print(
            f"cache={arm['cache_hit']} c={arm['concurrency']} arm={arm['arm']} {arm['mode']}: "
            f"{arm['queries_per_second']:.1f} queries/s; client P99 ms: {latencies}"
        )


if __name__ == "__main__":
    main()
