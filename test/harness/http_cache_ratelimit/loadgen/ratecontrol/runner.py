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

"""Bring a scenario up, drive it, and take it down.

The order matters in a few places and the reasons are not obvious:

* Every port a scenario needs is checked free *before* anything starts, each
  spiced is started with `exec` so the recorded pid is the runtime itself, and
  teardown waits for the ports to be released. Without all three, a scenario
  can be answered by the previous scenario's leftover process -- which reads
  as a passing run of something else entirely.
* Each (replica, dataset) pair gets its own worker pool. A shared pool lets one
  blocked origin's backpressure starve another origin's independent requests of
  workers, and the starved origin then looks throttled when it is not.
* The state snapshot is taken while the replicas are still up: the object is
  the evidence, and a torn-down replica stops refreshing it.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Sequence

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from harness import http  # noqa: E402
from harness.phases import Phases, phase_at  # noqa: E402
from harness.timeline import Step, Timeline  # noqa: E402
from metrics_scraper import MetricsScraper  # noqa: E402

from . import rates, state  # noqa: E402
from .scenarios import Scenario  # noqa: E402
from .topology import REPLICA_HEADER, Topology, render_spicepod  # noqa: E402

HTTP_PORT_BASE = 8090
FLIGHT_PORT_BASE = 50051
METRICS_PORT_BASE = 9090
RUSTFS_PORT = 9100
RUSTFS_CONSOLE_PORT = 9102
RUSTFS_KEY = "rate-control-test"
RUSTFS_SECRET = "rate-control-test-secret"
HEALTHY = {"id": "healthy", "mode": "healthy", "error_rate": 0.0, "fault_paths": [], "fault_headers": {}}


@dataclass
class Query:
    """One `/v1/sql` call the load generator made."""

    t_epoch_ms: int
    replica: str
    dataset: str
    phase: str
    outcome: str  # "ok" | "rate_limited" | "error"
    status: int
    latency_ms: int


@dataclass
class RunPaths:
    root: str

    def __post_init__(self) -> None:
        os.makedirs(self.root, exist_ok=True)

    def path(self, *parts: str) -> str:
        full = os.path.join(self.root, *parts)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        return full


@dataclass
class Process:
    name: str
    popen: subprocess.Popen
    log_path: str

    def alive(self) -> bool:
        return self.popen.poll() is None

    def stop(self) -> None:
        if self.alive():
            self.popen.send_signal(signal.SIGTERM)
            try:
                self.popen.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.popen.kill()


@dataclass
class Fleet:
    """Everything a scenario started, so teardown is one call."""

    processes: list[Process] = field(default_factory=list)

    def add(self, process: Process) -> Process:
        self.processes.append(process)
        return process

    def stop_all(self) -> None:
        for process in reversed(self.processes):
            process.stop()


class RunError(RuntimeError):
    """A scenario could not be brought up; its verdict is neither pass nor fail."""


def port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def wait_for(predicate, timeout_s: float, interval_s: float = 0.2) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return predicate()


def scenario_ports(scenario: Scenario) -> list[int]:
    ports = [origin.port for origin in scenario.topology.origins]
    for index in range(scenario.topology.replicas):
        ports += [
            HTTP_PORT_BASE + index,
            FLIGHT_PORT_BASE + index,
            METRICS_PORT_BASE + index,
        ]
    if scenario.topology.cluster and scenario.topology.cluster.backend == "s3":
        ports += [RUSTFS_PORT, RUSTFS_CONSOLE_PORT]
    return ports


def spawn(fleet: Fleet, name: str, argv: Sequence[str], log_path: str, env: dict[str, str], cwd: str | None = None) -> Process:
    handle = open(log_path, "w", encoding="utf-8")
    popen = subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT, env=env, cwd=cwd)
    return fleet.add(Process(name=name, popen=popen, log_path=log_path))


# --------------------------------------------------------------------------
# bring-up
# --------------------------------------------------------------------------


def start_origins(scenario: Scenario, paths: RunPaths, fleet: Fleet, python: str, harness_dir: str, t0: float) -> dict[str, str]:
    """Start one origin per spec. Returns origin name -> arrival log path."""
    logs: dict[str, str] = {}
    for origin in scenario.topology.origins:
        log_path = paths.path(f"origin_{origin.name}.jsonl")
        open(log_path, "w").close()
        env = dict(os.environ)
        env.update(
            ORIGIN_NAME=origin.name,
            ORIGIN_PORT=str(origin.port),
            ORIGIN_REQUEST_LOG=log_path,
            HARNESS_T0=str(t0),
        )
        spawn(
            fleet,
            f"origin-{origin.name}",
            [python, os.path.join(harness_dir, "origin", "server.py")],
            paths.path(f"origin_{origin.name}.log"),
            env,
        )
        logs[origin.name] = log_path

    for origin in scenario.topology.origins:
        ok = wait_for(lambda o=origin: _probe(o.health_url), timeout_s=20)
        if not ok:
            raise RunError(f"origin {origin.name} did not start on port {origin.port}")
    return logs


def _probe(url: str) -> bool:
    try:
        http.get_json(url, timeout=1.0)
        return True
    except Exception:  # noqa: BLE001 - a probe miss is just "not up yet"
        try:
            import urllib.request

            with urllib.request.urlopen(url, timeout=1.0) as response:
                return response.status == 200
        except Exception:  # noqa: BLE001
            return False


def start_rustfs(paths: RunPaths, fleet: Fleet, rustfs_bin: str, bucket: str) -> None:
    data_dir = os.path.join(paths.root, "rustfs-data")
    # The whole directory, not its visible contents: rustfs keeps bucket
    # metadata in a hidden `.rustfs.sys`, and leaving that behind while the
    # data is gone makes every CreateBucket fail with NoSuchBucket.
    shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(data_dir, exist_ok=True)
    env = dict(os.environ)
    env.update(RUSTFS_ACCESS_KEY=RUSTFS_KEY, RUSTFS_SECRET_KEY=RUSTFS_SECRET)
    spawn(
        fleet,
        "rustfs",
        [
            rustfs_bin,
            "server",
            "--address",
            f"127.0.0.1:{RUSTFS_PORT}",
            "--console-address",
            f"127.0.0.1:{RUSTFS_CONSOLE_PORT}",
            data_dir,
        ],
        paths.path("rustfs.log"),
        env,
    )

    aws_env = dict(os.environ)
    aws_env.update(
        AWS_ACCESS_KEY_ID=RUSTFS_KEY,
        AWS_SECRET_ACCESS_KEY=RUSTFS_SECRET,
        AWS_DEFAULT_REGION="us-east-1",
        AWS_EC2_METADATA_DISABLED="true",
    )

    def bucket_ready() -> bool:
        subprocess.run(
            ["aws", "--endpoint-url", f"http://127.0.0.1:{RUSTFS_PORT}", "s3api", "create-bucket", "--bucket", bucket],
            env=aws_env,
            capture_output=True,
        )
        head = subprocess.run(
            ["aws", "--endpoint-url", f"http://127.0.0.1:{RUSTFS_PORT}", "s3api", "head-bucket", "--bucket", bucket],
            env=aws_env,
            capture_output=True,
        )
        return head.returncode == 0

    if not wait_for(bucket_ready, timeout_s=60, interval_s=1.0):
        raise RunError(f"rustfs bucket {bucket} could not be created")


def state_location(scenario: Scenario, paths: RunPaths) -> tuple[str | None, str]:
    """(state_location URI, extra spicepod params block)."""
    cluster = scenario.topology.cluster
    if cluster is None:
        return None, ""
    if cluster.backend == "file":
        directory = os.path.join(paths.root, "state")
        os.makedirs(directory, exist_ok=True)
        return f"file://{directory}/", ""
    # `runtime.state.params`, the object-store params for the shared location.
    params = "\n".join(
        [
            "    params:",
            f"      s3_endpoint: http://127.0.0.1:{RUSTFS_PORT}",
            f"      s3_key: {RUSTFS_KEY}",
            f"      s3_secret: {RUSTFS_SECRET}",
            "      s3_region: us-east-1",
            "      s3_auth: key",
            '      allow_http: "true"',
        ]
    )
    return f"s3://{cluster.bucket}/{cluster.prefix}", params


def start_replicas(scenario: Scenario, paths: RunPaths, fleet: Fleet, spiced_bin: str, spicepod_name: str) -> list[Process]:
    location, params = state_location(scenario, paths)
    replicas: list[Process] = []
    for index, replica in enumerate(scenario.topology.replica_names()):
        directory = paths.path(replica, "spicepod.yaml")
        with open(directory, "w", encoding="utf-8") as handle:
            handle.write(render_spicepod(scenario.topology, spicepod_name, replica, location, params))
        process = spawn(
            fleet,
            replica,
            [
                spiced_bin,
                "--http",
                f"127.0.0.1:{HTTP_PORT_BASE + index}",
                "--flight",
                f"127.0.0.1:{FLIGHT_PORT_BASE + index}",
                "--metrics",
                f"127.0.0.1:{METRICS_PORT_BASE + index}",
                "spicepod.yaml",
            ],
            paths.path(f"spiced_{replica}.log"),
            dict(os.environ),
            cwd=os.path.dirname(directory),
        )
        replicas.append(process)
    return replicas


def await_ready(scenario: Scenario, replicas: list[Process]) -> None:
    for index, process in enumerate(replicas):
        url = f"http://127.0.0.1:{HTTP_PORT_BASE + index}/v1/ready"
        # Liveness first: a dead replica whose port another process holds would
        # otherwise sail through the readiness probe.
        ready = wait_for(lambda: not process.alive() or _probe(url), timeout_s=60)
        if not process.alive():
            raise RunError(f"replica {process.name} exited during start-up; see {process.log_path}")
        if not ready:
            raise RunError(f"replica {process.name} did not become ready; see {process.log_path}")


# --------------------------------------------------------------------------
# load
# --------------------------------------------------------------------------


def classify(status: int, body: Any) -> str:
    if status == 200:
        return "ok"
    text = str(body).lower()
    if "rate" in text and ("limit" in text or "budget" in text):
        return "rate_limited"
    return "error"


def drive(
    scenario: Scenario,
    phases: Phases,
    t0: float,
    stop: threading.Event,
) -> list[Query]:
    """Saturate every (replica, dataset) pair until `stop` is set."""
    queries: list[Query] = []
    lock = threading.Lock()

    def worker(replica: str, index: int, dataset: str) -> None:
        # One keep-alive connection per worker. A connection per query exhausts
        # the host's ephemeral ports within minutes of saturating load; see
        # harness.http.SqlSession.
        session = http.SqlSession("127.0.0.1", HTTP_PORT_BASE + index, timeout=30.0)
        query = f"SELECT response_status FROM {dataset}"
        try:
            while not stop.is_set():
                sent = time.time()
                status, body = session.query(query)
                sample = Query(
                    t_epoch_ms=int(sent * 1000),
                    replica=replica,
                    dataset=dataset,
                    phase=phase_at(sent - t0, phases),
                    outcome=classify(status, body),
                    status=status,
                    latency_ms=int((time.time() - sent) * 1000),
                )
                with lock:
                    queries.append(sample)
        finally:
            session.close()

    threads = [
        threading.Thread(target=worker, args=(replica, index, dataset.name), daemon=True)
        for index, replica in enumerate(scenario.topology.replica_names())
        for dataset in scenario.topology.datasets
        for _ in range(dataset.workers or scenario.workers)
    ]
    for thread in threads:
        thread.start()
    return queries


# --------------------------------------------------------------------------
# evidence collection
# --------------------------------------------------------------------------


def snapshot_state(scenario: Scenario, paths: RunPaths) -> dict[str, str]:
    """Copy the shared state object(s) out. Returns origin name -> local path.

    The object key carries the origin's `host_port`, which is how a run with
    more than one origin tells its state objects apart.
    """
    cluster = scenario.topology.cluster
    if cluster is None:
        return {}
    snapshot_dir = os.path.join(paths.root, "state-snapshot")
    os.makedirs(snapshot_dir, exist_ok=True)

    if cluster.backend == "file":
        source_root = os.path.join(paths.root, "state")
    else:
        source_root = os.path.join(paths.root, "s3-state")
        aws_env = dict(os.environ)
        aws_env.update(
            AWS_ACCESS_KEY_ID=RUSTFS_KEY,
            AWS_SECRET_ACCESS_KEY=RUSTFS_SECRET,
            AWS_DEFAULT_REGION="us-east-1",
            AWS_EC2_METADATA_DISABLED="true",
        )
        subprocess.run(
            [
                "aws",
                "--endpoint-url",
                f"http://127.0.0.1:{RUSTFS_PORT}",
                "s3",
                "cp",
                "--recursive",
                f"s3://{cluster.bucket}/{cluster.prefix}",
                source_root,
            ],
            env=aws_env,
            capture_output=True,
        )

    found: dict[str, str] = {}
    for directory, _subdirs, files in os.walk(source_root):
        for name in files:
            if not name.endswith(".json"):
                continue
            source = os.path.join(directory, name)
            for origin in scenario.topology.origins:
                if f"_{origin.port}-" in name:
                    destination = os.path.join(snapshot_dir, f"{origin.name}.json")
                    shutil.copyfile(source, destination)
                    found[origin.name] = destination
    return found


def collect_arrivals(scenario: Scenario, origin_logs: dict[str, str]) -> list[rates.Arrival]:
    arrivals: list[rates.Arrival] = []
    for origin in scenario.topology.origins:
        path_to_dataset = {
            dataset.path: dataset.name for dataset in scenario.topology.datasets_for(origin.name)
        }
        arrivals += rates.read_arrivals(origin_logs[origin.name], origin.name, path_to_dataset)
    arrivals.sort(key=lambda arrival: arrival.epoch_ms)
    return arrivals


def write_queries(path: str, queries: Sequence[Query]) -> None:
    import csv

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(Query(0, "", "", "", "", 0, 0)).keys()))
        writer.writeheader()
        for query in sorted(queries, key=lambda q: q.t_epoch_ms):
            writer.writerow(asdict(query))


def write_metrics(path: str, scrapers: dict[str, MetricsScraper]) -> None:
    import csv

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["scrape_epoch_ms", "t_rel_s", "replica", "metric_name", "origin", "value"])
        for replica, scraper in scrapers.items():
            for sample in scraper.samples:
                writer.writerow(
                    [
                        sample.scrape_epoch_ms,
                        sample.t_rel_s,
                        replica,
                        sample.metric_name,
                        sample.origin,
                        sample.value,
                    ]
                )


def write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
