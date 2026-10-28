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

"""Check two-hop trace identity without generated-id history reconciliation."""

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

from sidecar import Instance, base_pod, expected, fixture


def history_reads(metrics):
    return sum(
        float(line.rsplit(" ", 1)[1])
        for line in metrics.splitlines()
        if line.startswith("query_executions{")
        and 'cache_namespace_kind="system"' in line
        and 'datasets="runtime.task_history"' in line
        and 'protocol="internal"' in line
    )


def metric(metrics, name):
    return sum(
        float(line.rsplit(" ", 1)[1])
        for line in metrics.splitlines()
        if line.startswith((name + " ", name + "{"))
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.spiced = args.spiced.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    (args.out / "provenance.json").write_text(
        json.dumps(
            {
                "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "source_commit": args.source_commit,
                "harness_commit": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
                "harness_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "common_harness_sha256": hashlib.sha256(
                    Path(__file__).with_name("sidecar.py").read_bytes()
                ).hexdigest(),
                "binary_sha256": hashlib.sha256(args.spiced.read_bytes()).hexdigest(),
                "command": sys.argv,
            },
            indent=2,
        )
    )
    database, source = fixture(args.out, 1000)
    origin = base_pod(
        "trace-origin",
        [
            {
                "from": f"file:{source}",
                "name": "remote_orders",
                "params": {"file_format": "csv"},
                "acceleration": {
                    "enabled": True,
                    "engine": "cayenne",
                    "mode": "memory",
                },
            }
        ],
        cache=True,
    )
    origin["runtime"]["caching"]["sql_results"]["cache_key_type"] = "plan"
    upstream = Instance(args.spiced, args.out / "upstream", origin)
    sidecar = None
    results = []
    try:
        upstream.start()
        sidecar = Instance(
            args.spiced,
            args.out / "sidecar",
            base_pod(
                "trace-sidecar",
                [
                    {
                        "from": "spiceai:remote_orders",
                        "name": "orders",
                        "params": {
                            "spiceai_endpoint": f"http://127.0.0.1:{upstream.flight}"
                        },
                    }
                ],
                "sidecar",
            ),
        )
        sidecar.start()
        sql = "SELECT id, value, label FROM orders WHERE id = 43"
        oracle = expected(database, sql)

        def check(name, headers, pinned, parent, repetitions):
            before = upstream.metrics_text()
            trace_ids = []
            observations = []
            for _ in range(repetitions):
                rows, response_headers = sidecar.query(sql, headers)
                trace_id = {
                    key.lower(): value for key, value in response_headers.items()
                }["spice-trace-id"]
                observations.append({"rows": rows, "trace_id": trace_id})
                assert rows == oracle, observations
                if pinned:
                    assert trace_id == pinned, observations
                trace_ids.append(trace_id)
            ids = ",".join(f"'{trace_id}'" for trace_id in set(trace_ids))
            deadline = time.monotonic() + 15
            while True:
                tasks, _ = upstream.query(
                    "SELECT trace_id, parent_span_id, input FROM runtime.task_history "
                    f"WHERE trace_id IN ({ids}) AND input LIKE 'substrait:%'"
                )
                if len(tasks) >= repetitions or time.monotonic() >= deadline:
                    break
                time.sleep(0.1)
            after = upstream.metrics_text()
            reads = history_reads(after) - history_reads(before)
            cache = {
                key: metric(after, key) - metric(before, key)
                for key in ["results_cache_hits", "results_cache_misses"]
            }
            result = {
                "name": name,
                "requests": observations,
                "expected_rows": oracle,
                "upstream_tasks": tasks,
                "internal_history_reads": reads,
                "cache": cache,
                "correlated": all(
                    any(
                        task["trace_id"] == trace_id
                        and task["parent_span_id"] == parent
                        for task in tasks
                    )
                    for trace_id in trace_ids
                ),
            }
            result["passed"] = result["correlated"] and reads == 0
            results.append(result)
            (args.out / f"{name}-metrics-before.txt").write_text(before)
            (args.out / f"{name}-metrics-after.txt").write_text(after)
            (args.out / "results.json").write_text(json.dumps(results, indent=2))
            print(json.dumps(result), flush=True)

        check("generated", {}, None, None, 10)
        check("pinned", {"spice-trace-id": "1" * 32}, "1" * 32, None, 1)
        traceparent = "00-" + "2" * 32 + "-" + "3" * 16 + "-01"
        check("traceparent", {"traceparent": traceparent}, "2" * 32, "3" * 16, 1)
        check(
            "conflicting-headers",
            {"traceparent": traceparent, "spice-trace-id": "4" * 32},
            "4" * 32,
            None,
            1,
        )
        assert all(result["passed"] for result in results), results
        assert results[0]["cache"]["results_cache_hits"] == 9, results[0]
        assert len({row["trace_id"] for row in results[0]["requests"]}) == 10, results[
            0
        ]
    finally:
        if sidecar:
            sidecar.stop()
        upstream.stop()
        database.close()


if __name__ == "__main__":
    main()
