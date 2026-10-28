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

"""Exercise native sidecar/upstream processes and benchmark both remote protocols.

Uses SQLite as the row oracle and testoperator as the Flight load generator.
The native two-process topology is not expressible by spicescript's current
single-process/cluster lifecycle; no Docker image substitutes for the binary
under test here. Run from the repository root; artifacts stay under --out.
"""

import argparse
import csv
import hashlib
import json
import os
import platform
import random
import signal
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, payload):
    return varint((number << 3) | 2) + varint(len(payload)) + payload


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Instance:
    def __init__(
        self,
        binary,
        directory,
        pod,
        cores=2,
        extra_args=None,
        api_key=None,
        http_port=None,
    ):
        self.binary = binary
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.pod = directory / "spicepod.yaml"
        self.pod.write_text(json.dumps(pod, ensure_ascii=False, indent=2))
        self.http, self.flight, self.metrics = http_port or port(), port(), port()
        self.cores = cores
        self.extra_args = extra_args or []
        self.api_key = api_key
        self.process = None
        self.log = None

    def start(self):
        try:
            self._start()
        except BaseException:
            self.stop()
            raise

    def _start(self):
        self.log = (self.directory / "spiced.log").open("a")
        self.command = [
            str(self.binary),
            "--http",
            f"127.0.0.1:{self.http}",
            "--flight",
            f"127.0.0.1:{self.flight}",
            "--metrics",
            f"127.0.0.1:{self.metrics}",
        ] + self.extra_args
        (self.directory / "command.json").write_text(json.dumps(self.command))
        environment = dict(os.environ, SPICE_CPU_CORES=str(self.cores))
        self.process = subprocess.Popen(
            self.command,
            cwd=self.directory,
            env=environment,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        with (self.directory / "starts.jsonl").open("a") as starts:
            starts.write(
                json.dumps(
                    {
                        "pid": self.process.pid,
                        "time_ns": time.time_ns(),
                        "command": self.command,
                    }
                )
                + "\n"
            )
        limit = time.monotonic() + 90
        while time.monotonic() < limit:
            if self.process.poll() is not None:
                raise RuntimeError((self.directory / "spiced.log").read_text())
            try:
                request = urllib.request.Request(
                    f"http://127.0.0.1:{self.http}/v1/ready",
                    headers={"x-api-key": self.api_key} if self.api_key else {},
                )
                with urllib.request.urlopen(request, timeout=2):
                    return
            except (urllib.error.URLError, TimeoutError):
                time.sleep(0.1)
        raise TimeoutError(f"readiness: {self.directory}")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise RuntimeError(f"unclean shutdown: {self.directory}")
        if self.log:
            self.log.close()
            self.log = None

    def query(self, sql, headers=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http}/v1/sql",
            data=sql.encode(),
            headers={
                "Content-Type": "text/plain",
                **({"x-api-key": self.api_key} if self.api_key else {}),
                **(headers or {}),
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read()), dict(response.headers.items())

    def metrics_text(self):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.metrics}/metrics", timeout=5
        ) as response:
            return response.read().decode()


def startup_error(binary, directory, pod, arguments, message):
    directory.mkdir(parents=True, exist_ok=False)
    manifest = directory / "spicepod.yaml"
    manifest.write_text(json.dumps(pod, indent=2))
    command = [str(binary), *arguments]
    (directory / "command.json").write_text(json.dumps(command))
    with (directory / "spiced.log").open("w") as log:
        result = subprocess.run(
            command,
            cwd=directory,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
    output = (directory / "spiced.log").read_text()
    assert result.returncode != 0 and message in output, (result.returncode, output)
    (directory / "result.json").write_text(
        json.dumps({"exit_code": result.returncode, "expected_message": message})
    )


def base_pod(name, datasets, mode=None, cache=False):
    runtime = {
        "query": {
            "target_partitions": 1,
            "max_concurrent_queries": 16,
            "memory_limit": "512MiB",
        },
        "caching": {
            "sql_results": {"enabled": cache, "item_ttl": "1h", "max_size": "32MiB"}
        },
    }
    if mode and mode != "server":
        runtime["mode"] = mode
    return {
        "version": "v1",
        "kind": "Spicepod",
        "name": name,
        "runtime": runtime,
        "datasets": datasets,
    }


def fixture(directory, count):
    database = sqlite3.connect(":memory:")
    database.execute("CREATE TABLE orders (id BIGINT, value BIGINT, label TEXT)")
    values = [
        (i, None if i % 7 == 0 else i * 3 - 50, f"group{i % 10}") for i in range(count)
    ]
    database.executemany("INSERT INTO orders VALUES (?, ?, ?)", values)
    database.execute("CREATE VIEW other_orders AS SELECT * FROM orders")
    path = directory / "orders.csv"
    with path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["id", "value", "label"])
        writer.writerows(values)
    return database, path


QUERIES = {
    "point": "SELECT id, value, label FROM orders WHERE id = 42",
    "aggregate": "SELECT label, COUNT(*) AS n, SUM(value) AS total FROM orders GROUP BY label ORDER BY label",
    "join": "SELECT l.id, l.value, r.label FROM orders l JOIN other_orders r ON l.id = r.id WHERE l.id BETWEEN 10 AND 20 ORDER BY l.id",
    "topk": "SELECT id, value FROM orders WHERE value IS NOT NULL ORDER BY value DESC, id LIMIT 10 OFFSET 5",
    "nulls": "SELECT COUNT(*) AS n, COUNT(value) AS nonnull, SUM(value) AS total FROM orders",
    "empty": "SELECT id FROM orders WHERE id < 0",
}


def correctness_queries():
    """Seeded integer/NULL predicates avoid cross-engine coercion differences."""
    queries = dict(QUERIES)
    queries["string-view"] = (
        "SELECT arrow_cast(label, 'Utf8View') AS label FROM orders ORDER BY id LIMIT 10"
    )
    rng = random.Random(15707)
    for index in range(32):
        start = rng.randrange(0, 100)
        end = start + rng.randrange(1, 50)
        threshold = rng.randrange(-60, 300)
        if index % 2:
            queries[f"seed15707-filter-{index}"] = (
                "SELECT id, value, label FROM orders "
                f"WHERE id BETWEEN {start} AND {end} AND (value IS NULL OR value > {threshold}) "
                "ORDER BY id"
            )
        else:
            queries[f"seed15707-aggregate-{index}"] = (
                "SELECT label, COUNT(*) AS n, COALESCE(SUM(value), 0) AS total FROM orders "
                f"WHERE id BETWEEN {start} AND {end} AND (value IS NULL OR value <= {threshold}) "
                "GROUP BY label ORDER BY label"
            )
    queries["null-antijoin"] = (
        "SELECT id FROM orders WHERE value NOT IN (SELECT value FROM other_orders WHERE id < 10) ORDER BY id"
    )
    queries["null-antijoin-empty"] = (
        "SELECT id FROM orders WHERE id < 10 AND value NOT IN (SELECT value FROM other_orders WHERE id < 0) ORDER BY id"
    )
    queries["null-not-exists"] = (
        "SELECT id FROM orders o WHERE id < 10 AND NOT EXISTS (SELECT 1 FROM other_orders r WHERE r.value = o.value) ORDER BY id"
    )
    queries["null-not-in-expression"] = (
        "SELECT id FROM orders WHERE NOT (value IN (SELECT value FROM other_orders WHERE id < 10)) ORDER BY id"
    )
    queries["null-in-projection"] = (
        "SELECT id, value IN (SELECT value FROM other_orders WHERE id < 10) AS present FROM orders WHERE id < 12 ORDER BY id"
    )
    queries["null-safe-join"] = (
        "SELECT l.id, r.id AS rid FROM orders l JOIN other_orders r "
        "ON l.id = r.id AND l.value IS NOT DISTINCT FROM r.value WHERE l.id < 10 ORDER BY l.id"
    )
    queries["window"] = (
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY label ORDER BY id) AS rn, "
        "SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running "
        "FROM orders WHERE id < 20 ORDER BY id"
    )
    queries["union-all"] = (
        "SELECT id, value FROM orders WHERE id < 10 UNION ALL "
        "SELECT id, value FROM other_orders WHERE id < 10 ORDER BY id, value"
    )
    queries["left-join"] = (
        "SELECT l.id, r.value FROM orders l LEFT JOIN other_orders r ON l.id = r.id AND r.value > 0 "
        "WHERE l.id < 10 ORDER BY l.id"
    )
    return queries


def expected(database, sql):
    # String views are an Arrow representation of the same SQLite text value.
    cursor = database.execute(sql.replace("arrow_cast(label, 'Utf8View')", "label"))
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]


def observe(instance, upstream, stop, output):
    """Record process RSS and HTTP health latency while Flight clients run."""
    while not stop.is_set():
        sample = {"time_ns": time.time_ns()}
        try:
            process_rows = subprocess.check_output(
                [
                    "ps",
                    "-o",
                    "pid=,rss=",
                    "-p",
                    f"{instance.process.pid},{upstream.process.pid}",
                ],
                text=True,
            )
            rss = {
                int(pid): int(kib) * 1024
                for pid, kib in (row.split() for row in process_rows.splitlines())
            }
            sample["rss_bytes"] = rss[instance.process.pid]
            sample["upstream_rss_bytes"] = rss[upstream.process.pid]
            begin = time.perf_counter_ns()
            with urllib.request.urlopen(
                f"http://127.0.0.1:{instance.http}/health", timeout=5
            ) as response:
                sample["health_status"] = response.status
            sample["health_ms"] = (time.perf_counter_ns() - begin) / 1_000_000
        except (
            OSError,
            ValueError,
            KeyError,
            urllib.error.URLError,
            TimeoutError,
        ) as error:
            sample["error"] = str(error)
        output.write(json.dumps(sample) + "\n")
        output.flush()
        stop.wait(0.25)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spiced", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--source-commit",
        help="Commit that produced the prebuilt spiced binary; defaults to HEAD",
    )
    parser.add_argument("--testoperator", type=Path)
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=["server", "sidecar"],
        default=["server", "sidecar"],
    )
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[2, 8, 32, 128])
    parser.add_argument(
        "--cache-states",
        nargs="+",
        choices=["miss", "hit"],
        default=["miss"],
        help="Measure remote misses by default; hit measures sidecar-cache hits separately",
    )
    parser.add_argument(
        "--executors",
        type=int,
        default=0,
        help="Use a scheduler and this many executors; 0 selects a standalone upstream",
    )
    args = parser.parse_args()
    if any(concurrency < 2 for concurrency in args.concurrency):
        parser.error("testoperator load requires concurrency >= 2")
    args.out = args.out.resolve()
    args.spiced = args.spiced.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    record = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "commit": args.source_commit
        or subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "harness_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "common_harness_sha256": hashlib.sha256(
            Path(__file__).with_name("sidecar.py").read_bytes()
        ).hexdigest(),
        "binary_sha256": hashlib.sha256(args.spiced.read_bytes()).hexdigest(),
        "testoperator_sha256": hashlib.sha256(
            args.testoperator.read_bytes()
        ).hexdigest()
        if args.testoperator
        else None,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_entitlement_per_instance": 2,
        "topology": "standalone"
        if args.executors == 0
        else f"scheduler+{args.executors}executors",
        "upstream_storage": "cayenne-memory"
        if args.executors == 0
        else "csv-federation",
        "arguments": {key: str(value) for key, value in vars(args).items()},
    }
    (args.out / "provenance.json").write_text(json.dumps(record, indent=2))
    invalid_pod = base_pod("invalid-mode", [], "worker")
    startup_error(
        args.spiced,
        args.out / "invalid-mode",
        invalid_pod,
        [],
        "Use 'server' (default) or 'sidecar'",
    )
    for name, arguments in [
        ("scheduler", ["--role", "scheduler"]),
        ("executor", ["--role", "executor"]),
        ("implicit-executor", ["--scheduler-address", "http://127.0.0.1:1"]),
        ("executor-override", ["--role", "executor", "--set-runtime", "mode=sidecar"]),
    ]:
        folder = args.out / f"invalid-{name}"
        # Explicit executor manifests carry local runtime settings; scheduler
        # datasets arrive through the existing cluster protocol.
        if name != "executor-override":
            arguments += [str(folder / "spicepod.yaml")]
        startup_error(
            args.spiced,
            folder,
            base_pod(name, [], "sidecar"),
            arguments,
            "runtime.mode: sidecar cannot run as a cluster scheduler or executor",
        )
    print(
        "configuration: invalid mode and incompatible cluster roles rejected",
        flush=True,
    )
    database, source_path = fixture(args.out, args.rows)
    cluster_pod = base_pod(
        "upstream",
        [
            {
                "from": f"file:{source_path}",
                "name": "remote_orders",
                "params": {"file_format": "csv"},
            },
        ],
    )
    if not args.executors:
        # Keep the standalone origin's working set in Cayenne memory so CSV
        # parsing does not dominate the uplink comparison.
        cluster_pod["datasets"][0]["acceleration"] = {
            "enabled": True,
            "engine": "cayenne",
            "mode": "memory",
        }
    scheduler_port = port()
    scheduler_args = []
    executors = []
    if args.executors:
        cluster_pod["runtime"]["state"] = {
            "location": (args.out / "cluster-state").as_uri()
        }
        scheduler_args = [
            "--role",
            "scheduler",
            "--allow-insecure-connections",
            "--node-bind-address",
            f"127.0.0.1:{scheduler_port}",
            "--node-advertise-address",
            "127.0.0.1",
        ]
    upstream = Instance(
        args.spiced, args.out / "upstream", cluster_pod, extra_args=scheduler_args
    )
    upstream.start()
    try:
        for index in range(args.executors):
            executor = Instance(
                args.spiced,
                args.out / f"executor-{index}",
                base_pod(f"executor-{index}", []),
                extra_args=[
                    "--role",
                    "executor",
                    "--allow-insecure-connections",
                    "--node-bind-address",
                    f"127.0.0.1:{port()}",
                    "--node-advertise-address",
                    "127.0.0.1",
                    "--scheduler-address",
                    f"http://127.0.0.1:{scheduler_port}",
                ],
            )
            # Keep the process handle even if readiness fails, so cleanup
            # also covers partial cluster startup.
            executors.append(executor)
            executor.start()
        cluster_explain, _ = upstream.query(
            "EXPLAIN SELECT SUM(value) FROM remote_orders"
        )
        (args.out / "upstream-explain.json").write_text(
            json.dumps(cluster_explain, indent=2)
        )
        datasets = [
            {
                "from": f"{prefix}:remote_orders",
                "name": name,
                "params": {"spiceai_endpoint": f"http://127.0.0.1:{upstream.flight}"},
            }
            for prefix, name in [("spice.ai", "orders"), ("spiceai", "other_orders")]
        ]
        for mode in args.modes:
            instance = Instance(
                args.spiced,
                args.out / f"correctness-{mode}",
                base_pod(mode, datasets, mode),
            )
            instance.start()
            try:
                results = {}
                for name, sql in correctness_queries().items():
                    try:
                        actual, _ = instance.query(sql)
                    except urllib.error.HTTPError as error:
                        observed = {
                            "mode": mode,
                            "name": name,
                            "sql": sql,
                            "status": error.code,
                            "error": error.read().decode(),
                        }
                        (instance.directory / f"{name}-error.json").write_text(
                            json.dumps(observed, indent=2)
                        )
                        if (
                            name == "null-in-projection"
                            and "Physical plan does not support logical expression InSubquery"
                            in observed["error"]
                        ):
                            observed["expected_unsupported"] = True
                            observed["expected_rows"] = expected(database, sql)
                            (instance.directory / f"{name}-error.json").write_text(
                                json.dumps(observed, indent=2)
                            )
                            continue
                        raise AssertionError(observed) from error
                    oracle = expected(database, sql)
                    results[name] = {"sql": sql, "actual": actual, "expected": oracle}
                    (instance.directory / "results.json").write_text(
                        json.dumps(results, indent=2)
                    )
                    if actual != oracle:
                        plan, _ = instance.query("EXPLAIN ANALYZE " + sql)
                        (
                            instance.directory / f"{name}-explain-analyze.json"
                        ).write_text(json.dumps(plan, indent=2))
                    assert actual == oracle, (mode, name, actual, oracle)
                plan, _ = instance.query("EXPLAIN " + QUERIES["join"])
                plan_text = json.dumps(plan)
                if mode == "sidecar":
                    physical = "\n".join(
                        item["plan"]
                        for item in plan
                        if item["plan_type"] == "physical_plan"
                    )
                    assert physical.count("SubstraitExec:") == 1, plan
                    assert "FlightExec sql=" not in plan_text, plan
                (instance.directory / "results.json").write_text(
                    json.dumps(results, indent=2)
                )
                (instance.directory / "explain.json").write_text(
                    json.dumps(plan, indent=2)
                )
                print(f"{mode}: {len(results)} SQLite comparisons", flush=True)
            finally:
                instance.stop()

        cache_source = args.out / "cache-orders.csv"
        cache_source.write_bytes(source_path.read_bytes())
        cache_origin_pod = base_pod(
            "plan-cache-origin",
            json.loads(json.dumps(cluster_pod["datasets"])),
            cache=True,
        )
        cache_origin_pod["datasets"][0]["from"] = f"file:{cache_source}"
        cache_origin_pod["datasets"][0]["acceleration"] = {
            "enabled": True,
            "engine": "cayenne",
            "mode": "memory",
        }
        cache_origin_pod["runtime"]["caching"]["sql_results"]["cache_key_type"] = "plan"
        cache_origin_pod["runtime"]["caching"]["sql_results"]["item_ttl"] = "10s"
        cache_origin = Instance(
            args.spiced, args.out / "plan-cache-upstream", cache_origin_pod
        )
        cache_origin.start()
        cached_datasets = json.loads(json.dumps(datasets))
        for dataset in cached_datasets:
            dataset["params"]["spiceai_endpoint"] = (
                f"http://127.0.0.1:{cache_origin.flight}"
            )
        cache_client = Instance(
            args.spiced,
            args.out / "plan-cache-sidecar",
            base_pod("plan-cache-client", cached_datasets, "sidecar", cache=False),
        )
        try:
            cache_client.start()
            before = cache_origin.metrics_text()
            (cache_origin.directory / "metrics-before.txt").write_text(before)
            cached_results = []
            for name, sql in QUERIES.items():
                if name == "empty":
                    continue
                for repetition in range(2):
                    actual, _ = cache_client.query(sql)
                    cached_results.append(
                        {
                            "name": name,
                            "repetition": repetition,
                            "sql": sql,
                            "actual": actual,
                            "expected": expected(database, sql),
                        }
                    )
            after = cache_origin.metrics_text()
            (cache_origin.directory / "metrics-after.txt").write_text(after)
            (cache_client.directory / "results.json").write_text(
                json.dumps(cached_results, indent=2)
            )

            def count_metric(metrics, name):
                return sum(
                    float(line.rsplit(" ", 1)[1])
                    for line in metrics.splitlines()
                    if line.startswith((name + " ", name + "{"))
                )

            delta = {
                name: count_metric(after, name) - count_metric(before, name)
                for name in [
                    "results_cache_hits",
                    "results_cache_misses",
                    "results_cache_requests",
                ]
            }
            (cache_origin.directory / "delta.json").write_text(
                json.dumps(delta, indent=2)
            )
            assert all(
                result["actual"] == result["expected"] for result in cached_results
            ), cached_results
            assert delta == {
                "results_cache_hits": 5,
                "results_cache_misses": 5,
                "results_cache_requests": 10,
            }, delta
            print(
                "sidecar cache disabled: upstream serialized-plan cache recorded 5 misses and 5 hits",
                flush=True,
            )
            count_sql = "SELECT COUNT(*) AS n FROM orders"
            before_refresh = [cache_client.query(count_sql)[0] for _ in range(2)]
            assert before_refresh == [[{"n": args.rows}]] * 2, before_refresh
            with cache_source.open("a", newline="") as output:
                csv.writer(output).writerow([args.rows, 123, "fresh"])
            refresh = urllib.request.Request(
                f"http://127.0.0.1:{cache_origin.http}/v1/datasets/remote_orders/acceleration/refresh",
                data=b"{}",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(refresh, timeout=10) as response:
                assert response.status == 201, response.status
            observations = []
            limit = time.monotonic() + 30
            while True:
                rows, _ = cache_origin.query(
                    "SELECT COUNT(*) AS n FROM remote_orders",
                    {"Cache-Control": "no-cache"},
                )
                observations.append(rows)
                if rows == [{"n": args.rows + 1}]:
                    break
                assert time.monotonic() < limit, observations
                time.sleep(0.05)
            after_refresh = [cache_client.query(count_sql)[0] for _ in range(2)]
            # A source refresh preserves eligible results until item_ttl. Client
            # writes have a separate hard-invalidation contract.
            assert after_refresh == [[{"n": args.rows}]] * 2, after_refresh
            expiry_observations = []
            limit = time.monotonic() + 15
            while True:
                rows, _ = cache_client.query(count_sql)
                expiry_observations.append(rows)
                if rows == [{"n": args.rows + 1}]:
                    break
                assert rows == [{"n": args.rows}] and time.monotonic() < limit, (
                    expiry_observations
                )
                time.sleep(0.2)
            after_expiry = cache_client.query(count_sql)[0]
            assert after_expiry == [{"n": args.rows + 1}], after_expiry
            (cache_client.directory / "refresh-results.json").write_text(
                json.dumps(
                    {
                        "sql": count_sql,
                        "before": before_refresh,
                        "upstream_observations": observations,
                        "after": after_refresh,
                        "expected_after": [[{"n": args.rows}]] * 2,
                        "expiry_observations": expiry_observations,
                        "after_expiry": after_expiry,
                    },
                    indent=2,
                )
            )
            print(
                "upstream serialized-plan cache: refresh retained eligible results; TTL expiry returned updated rows",
                flush=True,
            )
        finally:
            cache_client.stop()
            cache_origin.stop()

        for accelerated in [False, True]:
            configured = json.loads(json.dumps(datasets))
            if accelerated:
                for dataset in configured:
                    dataset["acceleration"] = {
                        "enabled": True,
                        "engine": "cayenne",
                        "mode": "memory",
                    }
            instance = Instance(
                args.spiced,
                args.out / f"outage-{accelerated}",
                base_pod("outage", configured, "sidecar", cache=not accelerated),
            )
            instance.start()
            try:
                sql = QUERIES["point"]
                before, _ = instance.query(sql)
                if not accelerated:
                    # Finish consuming the first response before probing a hit.
                    limit = time.monotonic() + 10
                    while True:
                        cached, headers = instance.query(sql)
                        assert cached == expected(database, sql), cached
                        if headers.get("results-cache-status") == "HIT":
                            break
                        assert time.monotonic() < limit, headers
                (instance.directory / "upstream-metrics-before-outage.txt").write_text(
                    upstream.metrics_text()
                )
                for executor in reversed(executors):
                    executor.stop()
                upstream.stop()
                after, headers = instance.query(sql)
                assert after == before == expected(database, sql), (after, before)
                if not accelerated:
                    assert headers.get("results-cache-status") == "HIT", headers
                plan = None
                if accelerated:
                    plan, _ = instance.query("EXPLAIN " + sql)
                    assert "SubstraitExec" not in json.dumps(plan), plan
                (instance.directory / "outage.json").write_text(
                    json.dumps(
                        {
                            "rows": after,
                            "headers": headers,
                            "explain": plan,
                        },
                        indent=2,
                    )
                )
                print(
                    f"upstream stopped: {'Cayenne' if accelerated else 'results cache'} returned exact rows",
                    flush=True,
                )
            finally:
                instance.stop()
                upstream.start()
                for executor in executors:
                    executor.start()

        timeout_pod = base_pod("deadline", datasets, "sidecar")
        timeout_pod["runtime"]["query"]["timeout"] = "250ms"
        timeout_instance = Instance(args.spiced, args.out / "deadline", timeout_pod)
        try:
            timeout_instance.start()
            observations = []
            os.kill(upstream.process.pid, signal.SIGSTOP)
            try:
                for _ in range(5):
                    started = time.perf_counter_ns()
                    try:
                        timeout_instance.query(QUERIES["point"])
                        raise AssertionError(
                            "paused upstream unexpectedly returned a result"
                        )
                    except urllib.error.HTTPError as error:
                        observations.append(
                            {
                                "status": error.code,
                                "body": error.read().decode(),
                                "elapsed_ms": (time.perf_counter_ns() - started)
                                / 1_000_000,
                            }
                        )
                (timeout_instance.directory / "timeouts.json").write_text(
                    json.dumps(observations, indent=2)
                )
                assert [item["status"] for item in observations] == [504] * 5, (
                    observations
                )
            finally:
                os.kill(upstream.process.pid, signal.SIGCONT)
        finally:
            timeout_instance.stop()
        print("paused upstream: five requests returned HTTP 504", flush=True)

        cancel_pod = base_pod("cancel", datasets, "sidecar")
        cancel_pod["runtime"]["query"]["timeout"] = "2s"
        cancel_instance = Instance(args.spiced, args.out / "cancel", cancel_pod)
        cancellation = {}
        worker = None
        try:
            cancel_instance.start()
            os.kill(upstream.process.pid, signal.SIGSTOP)
            try:

                def pending_query():
                    try:
                        cancellation["rows"], _ = cancel_instance.query(
                            QUERIES["point"]
                        )
                    except urllib.error.HTTPError as error:
                        cancellation.update(
                            status=error.code, body=error.read().decode()
                        )
                    except (OSError, ValueError) as error:
                        cancellation["unexpected_error"] = repr(error)

                worker = threading.Thread(target=pending_query)
                worker.start()
                limit = time.monotonic() + 1
                while True:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{cancel_instance.http}/v1/sql/active",
                        timeout=2,
                    ) as response:
                        active = json.load(response)["queries"]
                    active = [
                        query
                        for query in active
                        if query["sql_preview"] == QUERIES["point"]
                    ]
                    if active:
                        break
                    assert time.monotonic() < limit, (
                        "query did not enter the active registry"
                    )
                    time.sleep(0.01)
                query_id = active[0]["query_id"]
                request = urllib.request.Request(
                    f"http://127.0.0.1:{cancel_instance.http}/v1/sql/{query_id}/cancel",
                    data=b"",
                )
                with urllib.request.urlopen(request, timeout=2) as response:
                    cancellation["acknowledgement"] = json.load(response)
                worker.join(timeout=5)
                assert not worker.is_alive(), "cancelled query did not finish"
                (cancel_instance.directory / "result.json").write_text(
                    json.dumps(cancellation, indent=2)
                )
                assert cancellation.get("status") == 499, cancellation
                assert "cancelled" in cancellation["body"].lower(), cancellation
            finally:
                os.kill(upstream.process.pid, signal.SIGCONT)
        finally:
            if worker:
                worker.join(timeout=5)
            cancel_instance.stop()
        print("explicit cancellation before deadline: HTTP 499", flush=True)

        # An upstream-only UDF may use a Substrait operator's conventional
        # name. Local planning must preserve the SQL operator's meaning.
        function_pod = base_pod("upstream-functions", cluster_pod["datasets"])
        function_pod["runtime"]["functions"] = {"enabled": True}
        function_pod["functions"] = [
            {
                "name": "add",
                "from": "sql",
                "signature": {
                    "args": [
                        {"name": "a", "type": "int64"},
                        {"name": "b", "type": "int64"},
                    ],
                    "returns": "int64",
                },
                "body": "a - b",
                "volatility": "immutable",
                "as_tool": False,
            }
        ]
        function_pod["functions"].append(
            {
                "name": "is_null",
                "from": "sql",
                "signature": {
                    "args": [{"name": "a", "type": "int64"}],
                    "returns": "boolean",
                },
                "body": "false",
                "volatility": "immutable",
                "as_tool": False,
            }
        )
        function_pod["functions"].append(
            {
                "name": "is_nan",
                "from": "sql",
                "signature": {
                    "args": [{"name": "x", "type": "float64"}],
                    "returns": "boolean",
                },
                "body": "x > -100",
                "volatility": "immutable",
                "as_tool": False,
            }
        )
        function_upstream = Instance(
            args.spiced, args.out / "function-upstream", function_pod
        )
        function_upstream.start()
        scoped_datasets = json.loads(json.dumps(datasets))
        for dataset in scoped_datasets:
            dataset["params"]["spiceai_endpoint"] = (
                f"http://127.0.0.1:{function_upstream.flight}"
            )
        function_sidecar = Instance(
            args.spiced,
            args.out / "function-sidecar",
            base_pod("functions", scoped_datasets, "sidecar"),
        )
        try:
            udf_rows, _ = function_upstream.query(
                "SELECT add(CAST(4 AS BIGINT), CAST(1 AS BIGINT)) AS n"
            )
            assert udf_rows == [{"n": 3}], udf_rows
            function_sidecar.start()
            null_udf, _ = function_upstream.query(
                "SELECT is_null(CAST(NULL AS BIGINT)) AS n"
            )
            assert null_udf == [{"n": False}], null_udf
            results = []
            for sql in [
                "SELECT id + CAST(1 AS BIGINT) AS n FROM orders WHERE id < 5 ORDER BY id",
                "SELECT id FROM orders WHERE value IS NULL ORDER BY id LIMIT 5",
                "SELECT id, NOT (value IS NULL) AS present, -value AS negated FROM orders WHERE id < 10 ORDER BY id",
                "SELECT id FROM orders WHERE label NOT LIKE 'group0' AND label LIKE 'group%' ORDER BY id LIMIT 5",
            ]:
                actual, _ = function_sidecar.query(sql)
                results.append(
                    {"sql": sql, "actual": actual, "expected": expected(database, sql)}
                )
            alias_udf, _ = function_upstream.query(
                "SELECT is_nan(CAST(1 AS DOUBLE)) AS n"
            )
            assert alias_udf == [{"n": True}], alias_udf
            for sql, wanted in [
                (
                    "SELECT id, isnan(CAST(id AS DOUBLE)) AS n FROM orders WHERE id < 10 ORDER BY id",
                    [{"id": i, "n": False} for i in range(10)],
                ),
                (
                    "SELECT id, log(CAST(2 AS DOUBLE), CAST(id AS DOUBLE)) AS n FROM orders WHERE id = 8",
                    [{"id": 8, "n": 3.0}],
                ),
            ]:
                actual, _ = function_sidecar.query(sql)
                results.append({"sql": sql, "actual": actual, "expected": wanted})
            (function_sidecar.directory / "result.json").write_text(
                json.dumps(results, indent=2)
            )
            assert all(result["actual"] == result["expected"] for result in results), (
                results
            )
            print(
                "upstream UDFs: sidecar arithmetic, NULL predicates, negation, LIKE, builtin names and argument order preserved",
                flush=True,
            )
        finally:
            function_sidecar.stop()
            function_upstream.stop()

        reload_pod = base_pod("reload", datasets, "sidecar")
        reload_instance = Instance(
            args.spiced,
            args.out / "reload",
            reload_pod,
            extra_args=["--pods-watcher-enabled"],
        )
        try:
            reload_instance.start()
            reloaded = json.loads(json.dumps(datasets[0]))
            reloaded["name"] = "reloaded_orders"
            reload_pod["datasets"].append(reloaded)
            reload_pod["runtime"]["mode"] = "server"
            reload_instance.pod.write_text(json.dumps(reload_pod, indent=2))
            limit = time.monotonic() + 20
            while True:
                try:
                    plan, _ = reload_instance.query(
                        "EXPLAIN SELECT id FROM reloaded_orders"
                    )
                    break
                except urllib.error.HTTPError:
                    assert time.monotonic() < limit, (
                        "reloaded dataset did not become available"
                    )
                    time.sleep(0.1)
            assert "SubstraitExec" in json.dumps(plan), plan
            (reload_instance.directory / "explain.json").write_text(
                json.dumps(plan, indent=2)
            )
            log = (reload_instance.directory / "spiced.log").read_text()
            assert "Restart spiced to apply the new mode" in log, log
            print("reload: new connector retained startup sidecar mode", flush=True)
        finally:
            reload_instance.stop()

        (args.out / "upstream-metrics.txt").write_text(upstream.metrics_text())
        if args.testoperator:
            suite = {"name": "sidecar-protocol-comparison", "queries": []}
            for name, sql in QUERIES.items():
                if name == "empty":
                    continue
                # The CSV oracle loader infers an all-NULL column as Null.
                # Use a non-NULL point here; NULL point reads stay in the
                # correctness and outage cases above with exact SQLite rows.
                if name == "point":
                    sql = sql.replace("id = 42", "id = 43")
                data = expected(database, sql)
                columns = list(data[0])
                suite["queries"].append(
                    {
                        "name": name,
                        "sql": sql,
                        "expected_results": {
                            "columns": ",".join(columns),
                            "rows": [
                                ",".join(
                                    "" if row[key] is None else str(row[key])
                                    for key in columns
                                )
                                for row in data
                            ],
                        },
                    }
                )
            suite_path = args.out / "queries.yaml"
            suite_path.write_text(json.dumps(suite, indent=2))
            if not executors:
                upstream.stop()
            # Same candidate binary in both arms; the local partition count,
            # admission, cache policy, CPU and memory are explicitly identical.
            # Only the remote representation differs. Run remote misses first
            # and warm-cache traffic separately so hits cannot hide delegation.
            for cache in [state == "hit" for state in args.cache_states]:
                for concurrency in args.concurrency:
                    for iteration, mode in enumerate(
                        ["server", "sidecar", "sidecar", "server"]
                    ):
                        run = (
                            args.out
                            / f"bench-cache{cache}-c{concurrency}-{iteration}-{mode}"
                        )
                        arm_datasets = json.loads(json.dumps(datasets))
                        if not executors:
                            # A fresh standalone origin gives both protocols the
                            # same warmup and history lifetime on every arm.
                            upstream = Instance(
                                args.spiced, run / "upstream", cluster_pod
                            )
                            upstream.start()
                            for dataset in arm_datasets:
                                dataset["params"]["spiceai_endpoint"] = (
                                    f"http://127.0.0.1:{upstream.flight}"
                                )
                        # testoperator's external-instance adapter probes HTTP on
                        # port 8090 while accepting a configurable Flight port.
                        instance = Instance(
                            args.spiced,
                            run,
                            base_pod(mode, arm_datasets, mode, cache),
                            http_port=8090,
                        )
                        instance.start()
                        try:
                            (run / "instances.json").write_text(
                                json.dumps(
                                    {
                                        "sidecar_pid": instance.process.pid,
                                        "upstream_pid": upstream.process.pid,
                                    }
                                )
                            )
                            plans = {
                                query["name"]: instance.query(
                                    "EXPLAIN " + query["sql"]
                                )[0]
                                for query in suite["queries"]
                            }
                            (run / "explain.json").write_text(
                                json.dumps(plans, indent=2)
                            )
                            (run / "upstream-metrics-before.txt").write_text(
                                upstream.metrics_text()
                            )
                            for query in suite["queries"]:
                                instance.query(query["sql"])
                            (run / "upstream-metrics-after-warmup.txt").write_text(
                                upstream.metrics_text()
                            )
                            command = [
                                str(args.testoperator.resolve()),
                                "run",
                                "load",
                                "-p",
                                str(instance.pod),
                                "-s",
                                f"http://127.0.0.1:{instance.flight}",
                                "--query-set",
                                "scenario",
                                "--scenario-query-file",
                                str(suite_path),
                                "--validate",
                                "--duration",
                                str(args.duration),
                                "--concurrency",
                                str(concurrency),
                                "--disable-progress-bars",
                            ]
                            (run / "testoperator-command.json").write_text(
                                json.dumps(command)
                            )
                            stop = threading.Event()
                            with (run / "resources.jsonl").open("w") as resources:
                                observer = threading.Thread(
                                    target=observe,
                                    args=(instance, upstream, stop, resources),
                                )
                                observer.start()
                                try:
                                    with (run / "testoperator.log").open("w") as output:
                                        subprocess.run(
                                            command,
                                            cwd=run,
                                            stdout=output,
                                            stderr=subprocess.STDOUT,
                                            check=True,
                                        )
                                finally:
                                    stop.set()
                                    observer.join()
                            (run / "metrics.txt").write_text(instance.metrics_text())
                            (run / "upstream-metrics-after.txt").write_text(
                                upstream.metrics_text()
                            )
                            for index, executor in enumerate(executors):
                                (run / f"executor-{index}-metrics.txt").write_text(
                                    executor.metrics_text()
                                )
                            print(
                                f"benchmark: cache={cache} concurrency={concurrency} arm={iteration} mode={mode}",
                                flush=True,
                            )
                        finally:
                            instance.stop()
                            if not executors:
                                upstream.stop()
    finally:
        for executor in reversed(executors):
            executor.stop()
        upstream.stop()
        database.close()


if __name__ == "__main__":
    main()
