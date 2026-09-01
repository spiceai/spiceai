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

"""Prove that cancelling a Flight ``DoGet`` releases the BigQuery ADBC query.

Reproduces https://github.com/spiceai/spiceai/issues/13781.

This is an opt-in real-BigQuery harness, not a vacuous skip: it fails at
startup unless a service-account credential, the ADBC BigQuery driver, and a
``spiced`` binary are supplied. The companion shell script installs the pinned
Python dependencies.

Run from the repository root:

    BIGQUERY_SERVICE_ACCOUNT_JSON_FILE=/path/to/service-account.json \
      ADBC_BIGQUERY_DRIVER_PATH=/path/to/libadbc_driver_bigquery.dylib \
      SPICED_BIN=target/debug/spiced \
      test/scripts/bigquery-cancellation.sh

What it measures, per attempt:

* a Flight ``DoGet`` over a deliberately slow BigQuery query, ended either by a
  short client deadline (``BIGQUERY_CANCEL_MODE=deadline``, the default) or by
  the client cancelling the stream (``BIGQUERY_CANCEL_MODE=explicit``), and the
  status the client observes;
* whether the BigQuery job the runtime started is still ``RUNNING`` after the
  Flight client is gone, and when it reaches a terminal state;
* whether the single ADBC pool connection is free afterwards, measured by a
  second query that can only be admitted once the connection is released.

It exits non-zero when any of that fails, so it is a gate rather than a report:
the BigQuery job must reach a terminal state within
``BIGQUERY_CANCEL_LATENCY_BOUND_SECONDS`` of the client going away, the
follow-up query must get the pool connection back within
``BIGQUERY_CANCEL_POOL_BOUND_SECONDS``, and an uncancelled control query must
still return complete results before and after the cancelled attempts.

The slow query is a recursive CTE over no table: BigQuery evaluates each
iteration as its own sequential stage, so the runtime scales with the iteration
count instead of with slot availability, and it scans zero bytes. Each attempt
gets its own view with a distinct iteration count so BigQuery's query cache
cannot serve a later attempt from an earlier attempt's result. Every resource is
created inside one uniquely named dataset and deleted on exit.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow.flight as flight
from google.cloud import bigquery
from google.oauth2 import service_account

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SPICED = ROOT / "target" / "debug" / "spiced"
DATASET_PREFIX = "spice_bigquery_cancellation"
DATASET_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")

TERMINAL_STATES = {"DONE"}
QUICK_ITERATIONS = 5


class HarnessError(RuntimeError):
    """A failed setup, runtime, query, or result invariant."""


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise HarnessError(f"{name} must be a number; received {raw!r}") from error
    if value <= 0:
        raise HarnessError(f"{name} must be positive; received {value}")
    return value


def env_int(name: str, default: int) -> int:
    value = env_float(name, float(default))
    if value != int(value):
        raise HarnessError(f"{name} must be a whole number; received {value}")
    return int(value)


def required_path(name: str, default: Path | None = None) -> Path:
    value = os.environ.get(name)
    path = Path(value).expanduser() if value else default
    if path is None or not path.is_file():
        raise HarnessError(f"{name} must name an existing file; received {path!s}")
    return path.resolve()


def credential_info() -> tuple[dict[str, Any], str]:
    raw = os.environ.get("BIGQUERY_SERVICE_ACCOUNT_JSON")
    credential_file = os.environ.get("BIGQUERY_SERVICE_ACCOUNT_JSON_FILE")
    if not raw and not credential_file:
        raise HarnessError(
            "Set BIGQUERY_SERVICE_ACCOUNT_JSON or BIGQUERY_SERVICE_ACCOUNT_JSON_FILE. "
            "This real-BigQuery test does not skip when credentials are absent."
        )
    if raw and credential_file:
        raise HarnessError(
            "Set exactly one of BIGQUERY_SERVICE_ACCOUNT_JSON and "
            "BIGQUERY_SERVICE_ACCOUNT_JSON_FILE."
        )
    if credential_file:
        raw = required_path("BIGQUERY_SERVICE_ACCOUNT_JSON_FILE").read_text(
            encoding="utf-8"
        )
    assert raw is not None
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as error:
        raise HarnessError(
            f"The BigQuery service-account JSON is invalid: {error}"
        ) from error
    if info.get("type") != "service_account" or not info.get("project_id"):
        raise HarnessError(
            "The credential must be a service-account key with a project_id."
        )
    return info, json.dumps(info, separators=(",", ":"))


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def distinct_free_ports() -> tuple[int, int]:
    http_port = free_port()
    flight_port = free_port()
    while flight_port == http_port:
        flight_port = free_port()
    return http_port, flight_port


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def slow_view_name(token: str, index: int) -> str:
    return f"slow_{token}_{index}"


def quick_view_name(token: str) -> str:
    return f"quick_{token}"


def tiny_table_name(token: str) -> str:
    return f"tiny_{token}"


def slow_view_iterations(base: int, index: int) -> int:
    """Iteration count for one attempt's view.

    Distinct per attempt so that the SQL BigQuery receives differs between
    attempts and no attempt can be served from another's cached result.
    """
    return base + index


def recursive_count(_qualified: str, iterations: int) -> str:
    return f"""WITH RECURSIVE ctr AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM ctr WHERE n < {iterations}
)
SELECT COUNT(*) AS n FROM ctr"""


def setup_sql(
    project: str, dataset: str, token: str, base_iterations: int, attempts: int
) -> str:
    qualified = f"`{project}.{dataset}"
    statements = [
        f"CREATE OR REPLACE VIEW {qualified}.{quick_view_name(token)}` AS\n"
        + recursive_count(qualified, QUICK_ITERATIONS)
        + ";",
        f"CREATE OR REPLACE TABLE {qualified}.{tiny_table_name(token)}` AS\n"
        "SELECT 7 AS probe;",
    ]
    for index in range(1, attempts + 1):
        statements.append(
            f"CREATE OR REPLACE VIEW {qualified}.{slow_view_name(token, index)}` AS\n"
            + recursive_count(qualified, slow_view_iterations(base_iterations, index))
            + ";"
        )
    return "\n\n".join(statements) + "\n"


def spicepod(
    project: str, dataset: str, token: str, driver: Path, attempts: int
) -> str:
    params = f"""      adbc_driver: bigquery
      adbc_driver_path: {driver}
      adbc_driver_options: adbc.bigquery.sql.auth_type=adbc.bigquery.sql.auth_type.json_credential_string;adbc.bigquery.sql.auth_credentials=${{secrets:BIGQUERY_SERVICE_ACCOUNT_JSON}}
      adbc_uri: bigquery:///{project}?DatasetId={dataset}
      connection_pool_size: 1
      connection_pool_min_idle: 1"""
    tables = [tiny_table_name(token)] + [
        slow_view_name(token, index) for index in range(1, attempts + 1)
    ]
    extra = "".join(
        f"""  - from: adbc:{table}
    name: {table}
    params: *bigquery_params
"""
        for table in tables
    )
    return f"""version: v1
kind: Spicepod
name: bigquery-cancellation

datasets:
  - from: adbc:{quick_view_name(token)}
    name: {quick_view_name(token)}
    params: &bigquery_params
{params}
{extra}"""


def wait_until_ready(
    process: subprocess.Popen[bytes], http_port: int, timeout: int
) -> None:
    deadline = time.monotonic() + timeout
    last_observation = "no response"
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise HarnessError(
                f"spiced exited before readiness with status {return_code}"
            )
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{http_port}/v1/ready", timeout=2
            ) as response:
                body = response.read().decode()
                last_observation = f"HTTP {response.status}: {body}"
                if response.status == 200 and body.strip() == "ready":
                    return
        except (urllib.error.URLError, TimeoutError) as error:
            last_observation = str(error)
        time.sleep(0.5)
    raise HarnessError(
        f"spiced was not ready after {timeout}s; last observation: {last_observation}"
    )


def cancel_get(
    client: flight.FlightClient, sql: str, wait_before_cancel: float
) -> tuple[str, float, Any]:
    """Start a DoGet, then cancel it explicitly instead of letting it time out.

    Covers the other way a client goes away: an application that abandons the
    stream rather than one whose deadline expires. Both close the same stream
    boundary on the server.
    """
    started = time.monotonic()
    try:
        reader = client.do_get(flight.Ticket(sql.encode()))
        time.sleep(wait_before_cancel)
        reader.cancel()
        return "cancelled", time.monotonic() - started, "client called cancel()"
    except Exception as error:  # noqa: BLE001
        return "error", time.monotonic() - started, f"{type(error).__name__}: {error}"


def do_get(
    client: flight.FlightClient, sql: str, deadline: float
) -> tuple[str, float, Any]:
    """Run one Flight DoGet with a client deadline.

    Returns (outcome, elapsed_seconds, payload). ``outcome`` is ``ok``,
    ``timeout``, or ``error``; ``payload`` is the rows on success and the
    stringified status otherwise.
    """
    started = time.monotonic()
    try:
        reader = client.do_get(
            flight.Ticket(sql.encode()),
            options=flight.FlightCallOptions(timeout=deadline),
        )
        table = reader.read_all()
        return "ok", time.monotonic() - started, table.to_pylist()
    except flight.FlightTimedOutError as error:
        return "timeout", time.monotonic() - started, f"{type(error).__name__}: {error}"
    except flight.FlightCancelledError as error:
        return (
            "cancelled",
            time.monotonic() - started,
            f"{type(error).__name__}: {error}",
        )
    except Exception as error:  # noqa: BLE001
        # A server-side failure surfaces as whatever Arrow maps the gRPC status
        # to, which is not always a FlightError: a pool timeout arrives as
        # INVALID_ARGUMENT and therefore as ArrowInvalid.
        return "error", time.monotonic() - started, f"{type(error).__name__}: {error}"


def find_job(
    client: bigquery.Client,
    view: str,
    min_creation_time: datetime,
) -> bigquery.QueryJob | None:
    """Locate the BigQuery job the runtime started for one attempt's view.

    Federation pushes down a query that names the table without its dataset
    (the ADBC connection carries the default dataset), so the job is identified
    by the table name, which this harness makes globally unique.
    """
    needle = f"FROM `{view}`"
    # No `max_results`: the project may be busy, and a cap would silently drop
    # this query's job off the end of the page. `min_creation_time` already
    # bounds the listing server-side.
    for job in client.list_jobs(min_creation_time=min_creation_time):
        query = getattr(job, "query", None)
        if not query or job.job_type != "query":
            continue
        if getattr(job, "dry_run", False):
            continue
        if needle in query.replace("\n", " "):
            return job
    return None


def job_snapshot(job: bigquery.QueryJob) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "location": job.location,
        "state": job.state,
        "created": job.created.isoformat() if job.created else None,
        "started": job.started.isoformat() if job.started else None,
        "ended": job.ended.isoformat() if job.ended else None,
        "error_result": job.error_result,
        "total_bytes_processed": job.total_bytes_processed,
        "total_bytes_billed": job.total_bytes_billed,
    }


def stop_spiced(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


class JobWatcher(threading.Thread):
    """Samples one BigQuery job's state until it is terminal or the budget ends.

    Runs while the pool probe is in flight so the two observations can be
    correlated: the question is whether the job is still consuming work at the
    moment the pool refuses a second query.
    """

    def __init__(
        self,
        client: bigquery.Client,
        job: bigquery.QueryJob,
        origin: float,
        budget: float,
        interval: float = 2.0,
    ) -> None:
        super().__init__(daemon=True)
        self._client = client
        self._job = job
        self._origin = origin
        self._budget = budget
        self._interval = interval
        self.samples: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def _record(self, job: bigquery.QueryJob) -> dict[str, Any]:
        sample = job_snapshot(job)
        sample["observed_at"] = now()
        sample["seconds_after_client_gone"] = round(time.monotonic() - self._origin, 1)
        with self._lock:
            self.samples.append(sample)
        return sample

    def latest(self) -> dict[str, Any] | None:
        with self._lock:
            return self.samples[-1] if self.samples else None

    def run(self) -> None:
        sample = self._record(self._job)
        while (
            sample["state"] not in TERMINAL_STATES
            and time.monotonic() - self._origin < self._budget
        ):
            time.sleep(self._interval)
            try:
                refreshed = self._client.get_job(
                    self._job.job_id, location=self._job.location
                )
            except Exception as error:  # noqa: BLE001 - recorded, not raised
                with self._lock:
                    self.samples.append(
                        {
                            "observed_at": now(),
                            "seconds_after_client_gone": round(
                                time.monotonic() - self._origin, 1
                            ),
                            "state": "UNKNOWN",
                            "error": f"{type(error).__name__}: {error}",
                        }
                    )
                continue
            sample = self._record(refreshed)


def run_attempt(
    index: int,
    token: str,
    mode: str,
    flight_client: flight.FlightClient,
    bq_client: bigquery.Client,
    deadline: float,
    pool_probe_deadline: float,
    job_poll_budget: float,
) -> dict[str, Any]:
    view = slow_view_name(token, index)
    probe_table = tiny_table_name(token)
    record: dict[str, Any] = {"attempt": index, "view": view}
    # BigQuery's job list filters on creation time with second granularity, so
    # step back a little rather than racing the boundary.
    window_start = datetime.now(timezone.utc) - timedelta(seconds=5)

    record["client_started"] = now()
    record["cancellation_mode"] = mode
    wall_started = datetime.now(timezone.utc)
    if mode == "explicit":
        outcome, elapsed, payload = cancel_get(
            flight_client, f"SELECT * FROM {view}", deadline
        )
    else:
        outcome, elapsed, payload = do_get(
            flight_client, f"SELECT * FROM {view}", deadline
        )
    terminated_at = datetime.now(timezone.utc)
    record["client_terminated"] = terminated_at.isoformat()
    client_gone = time.monotonic()
    # A host that suspends mid-attempt stops the monotonic clock but not
    # BigQuery: the query finishes and the connection comes back while this
    # process is frozen, which would read as cancellation working. Compare the
    # two clocks and refuse the attempt instead.
    record["host_suspended_seconds"] = round(
        (terminated_at - wall_started).total_seconds() - elapsed, 1
    )
    record["cancel_outcome"] = outcome
    record["cancel_elapsed_seconds"] = round(elapsed, 3)
    record["cancel_status"] = payload

    job = find_job(bq_client, view, window_start)
    record["bigquery_job_found"] = job is not None
    watcher: JobWatcher | None = None
    if job is not None:
        watcher = JobWatcher(bq_client, job, client_gone, job_poll_budget)
        watcher.start()

    # The pool holds exactly one ADBC connection. A query that needs it can only
    # be admitted once the cancelled query's connection is actually released, so
    # this measures release rather than trusting a log line. The probe runs a
    # distinct predicate per attempt so no attempt can be served from another's
    # cached result.
    probe_outcome, probe_elapsed, probe_payload = do_get(
        flight_client,
        f"SELECT probe FROM {probe_table} WHERE probe > {index - 1}",
        pool_probe_deadline,
    )
    record["pool_probe_outcome"] = probe_outcome
    record["pool_probe_elapsed_seconds"] = round(probe_elapsed, 3)
    record["pool_probe_payload"] = probe_payload
    record["pool_probe_finished"] = now()
    record["pool_probe_exhausted_pool"] = (
        isinstance(probe_payload, str)
        and "timed out waiting for connection" in probe_payload
    )
    record["job_state_when_probe_finished"] = (
        (watcher.latest() or {}).get("state") if watcher else None
    )

    if watcher is None:
        record["bigquery_job"] = None
        record["bigquery_states"] = []
        return record

    watcher.join(timeout=job_poll_budget + 30.0)
    states = watcher.samples
    final = states[-1] if states else {}
    record["bigquery_job"] = final
    record["bigquery_states"] = states
    record["bigquery_running_after_client_gone"] = any(
        sample.get("state") not in TERMINAL_STATES for sample in states
    )
    record["bigquery_seconds_running_after_client_gone"] = max(
        (
            sample["seconds_after_client_gone"]
            for sample in states
            if sample.get("state") not in TERMINAL_STATES
        ),
        default=0.0,
    )
    record["bigquery_reached_terminal_after_seconds"] = next(
        (
            sample["seconds_after_client_gone"]
            for sample in states
            if sample.get("state") in TERMINAL_STATES
        ),
        None,
    )
    error_result = final.get("error_result") or {}
    record["bigquery_terminated_by_cancellation"] = (
        "cancel" in str(error_result.get("message", "")).casefold()
    )
    if final.get("started") and final.get("ended"):
        started_at = datetime.fromisoformat(final["started"])
        ended_at = datetime.fromisoformat(final["ended"])
        record["bigquery_job_runtime_seconds"] = round(
            (ended_at - started_at).total_seconds(), 1
        )
    return record


def verdict(
    records: list[dict[str, Any]],
    cancel_latency_bound: float,
    pool_release_bound: float,
) -> list[str]:
    """Acceptance criteria, evaluated against every attempt.

    Returns the failures. An empty list means cancellation propagates: the
    Flight stream ends on the client deadline, the BigQuery job stops promptly,
    and the pool connection comes back.
    """
    failures = []
    for record in records:
        index = record["attempt"]
        if record.get("host_suspended_seconds", 0.0) > 5.0:
            failures.append(
                f"attempt {index}: the host suspended for about "
                f"{record['host_suspended_seconds']}s during the attempt, so its "
                "timings prove nothing — rerun it"
            )
            continue
        expected = (
            "cancelled" if record.get("cancellation_mode") == "explicit" else "timeout"
        )
        if record["cancel_outcome"] != expected:
            failures.append(
                f"attempt {index}: the Flight stream did not end as {expected} "
                f"(got {record['cancel_outcome']}: {record['cancel_status']!r})"
            )
        if not record.get("bigquery_job_found"):
            failures.append(
                f"attempt {index}: no BigQuery job was found for {record['view']}, "
                "so the run proves nothing about cancellation"
            )
            continue
        terminal_after = record.get("bigquery_reached_terminal_after_seconds")
        if terminal_after is None or terminal_after > cancel_latency_bound:
            failures.append(
                f"attempt {index}: the BigQuery job kept running for "
                f"{record.get('bigquery_seconds_running_after_client_gone')}s after the "
                f"Flight client was gone (bound {cancel_latency_bound}s); "
                f"job {record['bigquery_job'].get('job_id')} finished in state "
                f"{record['bigquery_job'].get('state')}, "
                f"terminated_by_cancellation={record.get('bigquery_terminated_by_cancellation')}"
            )
        if record["pool_probe_outcome"] != "ok":
            failures.append(
                f"attempt {index}: the follow-up query did not get the pool "
                f"connection back ({record['pool_probe_outcome']} after "
                f"{record['pool_probe_elapsed_seconds']}s: {record['pool_probe_payload']!r}); "
                f"the BigQuery job was {record['job_state_when_probe_finished']} at that moment"
            )
        elif record["pool_probe_elapsed_seconds"] > pool_release_bound:
            failures.append(
                f"attempt {index}: the follow-up query waited "
                f"{record['pool_probe_elapsed_seconds']}s for the pool connection "
                f"(bound {pool_release_bound}s)"
            )
    return failures


def main() -> int:
    info, compact_credential = credential_info()
    project = os.environ.get("BIGQUERY_PROJECT_ID", str(info["project_id"]))
    if not PROJECT_ID_PATTERN.fullmatch(project):
        raise HarnessError(f"Invalid BIGQUERY_PROJECT_ID: {project!r}")
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    dataset = os.environ.get(
        "BIGQUERY_DATASET_ID",
        f"{DATASET_PREFIX}_{utc_stamp().lower()}_{os.getpid()}",
    )
    if not DATASET_ID_PATTERN.fullmatch(dataset):
        raise HarnessError(f"Invalid BIGQUERY_DATASET_ID: {dataset!r}")
    cleanup = os.environ.get("BIGQUERY_TEST_CLEANUP", "always")
    if cleanup not in {"always", "on_success", "never"}:
        raise HarnessError("BIGQUERY_TEST_CLEANUP must be always, on_success, or never")

    base_iterations = env_int("BIGQUERY_CANCEL_SLOW_ITERATIONS", 400)
    deadline = env_float("BIGQUERY_CANCEL_DEADLINE_SECONDS", 15.0)
    pool_probe_deadline = env_float("BIGQUERY_CANCEL_POOL_PROBE_SECONDS", 180.0)
    job_poll_budget = env_float("BIGQUERY_CANCEL_JOB_POLL_SECONDS", 300.0)
    attempts = env_int("BIGQUERY_CANCEL_ATTEMPTS", 3)
    cancel_latency_bound = env_float("BIGQUERY_CANCEL_LATENCY_BOUND_SECONDS", 20.0)
    pool_release_bound = env_float("BIGQUERY_CANCEL_POOL_BOUND_SECONDS", 20.0)
    token = f"c{utc_stamp().lower()}_{os.getpid()}"
    mode = os.environ.get("BIGQUERY_CANCEL_MODE", "deadline")
    if mode not in {"deadline", "explicit"}:
        raise HarnessError("BIGQUERY_CANCEL_MODE must be deadline or explicit")

    driver = required_path("ADBC_BIGQUERY_DRIVER_PATH")
    spiced = required_path("SPICED_BIN", DEFAULT_SPICED)
    output = Path(
        os.environ.get(
            "BIGQUERY_TEST_OUTPUT",
            ROOT / "target" / "bigquery-cancellation-evidence" / utc_stamp(),
        )
    ).resolve()
    output.mkdir(parents=True, exist_ok=False)

    credentials = service_account.Credentials.from_service_account_info(info)
    bq_client = bigquery.Client(project=project, credentials=credentials)
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    dataset_ref.labels = {"purpose": "spice-bigquery-cancellation"}
    created_dataset = False
    succeeded = False
    process: subprocess.Popen[bytes] | None = None
    log_handle = None

    evidence: dict[str, Any] = {
        "dataset": dataset,
        "location": location,
        "slow_view_base_iterations": base_iterations,
        "client_deadline_seconds": deadline,
        "pool_probe_deadline_seconds": pool_probe_deadline,
        "job_poll_budget_seconds": job_poll_budget,
        "attempts": attempts,
        "token": token,
        "cancellation_mode": mode,
        "cancel_latency_bound_seconds": cancel_latency_bound,
        "pool_release_bound_seconds": pool_release_bound,
    }

    try:
        bq_client.create_dataset(dataset_ref)
        created_dataset = True

        setup = setup_sql(project, dataset, token, base_iterations, attempts)
        (output / "setup.sql").write_text(setup, encoding="utf-8")
        setup_job = bq_client.query(setup, location=location)
        setup_job.result()
        evidence["setup_job_id"] = setup_job.job_id
        print(f"setup_job={setup_job.job_id} dataset={dataset} location={location}")

        pod_path = output / "spicepod.yaml"
        pod_path.write_text(
            spicepod(project, dataset, token, driver, attempts), encoding="utf-8"
        )
        binary_hash = hashlib.sha256(spiced.read_bytes()).hexdigest()
        version = subprocess.run(
            [str(spiced), "--version"], check=False, capture_output=True, text=True
        )
        evidence["spiced"] = {
            "path": str(spiced),
            "sha256": binary_hash,
            "version": version.stdout.strip(),
        }
        (output / "candidate.txt").write_text(
            f"path={spiced}\nsha256={binary_hash}\nversion={version.stdout.strip()}\n",
            encoding="utf-8",
        )

        http_port, flight_port = distinct_free_ports()
        log_handle = (output / "spiced.log").open("wb")
        environment = os.environ.copy()
        environment["BIGQUERY_SERVICE_ACCOUNT_JSON"] = compact_credential
        environment.setdefault(
            "SPICED_LOG",
            "spiced=debug,runtime=debug,datafusion_table_providers=debug,"
            "connector_adbc=debug",
        )
        process = subprocess.Popen(
            [
                str(spiced),
                "--http",
                f"127.0.0.1:{http_port}",
                "--flight",
                f"127.0.0.1:{flight_port}",
                "--telemetry-enabled",
                "false",
                str(pod_path),
            ],
            cwd=ROOT,
            env=environment,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        wait_until_ready(process, http_port, timeout=240)

        flight_client = flight.FlightClient(f"grpc://127.0.0.1:{flight_port}")

        control_outcome, control_elapsed, control_payload = do_get(
            flight_client, f"SELECT * FROM {quick_view_name(token)}", 180.0
        )
        evidence["control"] = {
            "outcome": control_outcome,
            "elapsed_seconds": round(control_elapsed, 3),
            "payload": control_payload,
        }
        if control_outcome != "ok" or control_payload != [{"n": QUICK_ITERATIONS}]:
            raise HarnessError(
                f"control query did not return complete results: {control_outcome} {control_payload!r}"
            )
        print(f"control: ok in {control_elapsed:.1f}s")

        records = []
        for index in range(1, attempts + 1):
            record = run_attempt(
                index,
                token,
                mode,
                flight_client,
                bq_client,
                deadline,
                pool_probe_deadline,
                job_poll_budget,
            )
            records.append(record)
            write_json(output / "attempts.json", records)
            print(
                f"attempt {index}: cancel={record['cancel_outcome']} "
                f"after {record['cancel_elapsed_seconds']}s; "
                f"pool_probe={record['pool_probe_outcome']} "
                f"after {record['pool_probe_elapsed_seconds']}s; "
                f"bq_job={(record['bigquery_job'] or {}).get('job_id')} "
                f"final_state={(record['bigquery_job'] or {}).get('state')} "
                f"ran_after_client_gone={record.get('bigquery_seconds_running_after_client_gone')}s "
                f"job_runtime={record.get('bigquery_job_runtime_seconds')}s"
            )
        evidence["records"] = records
        failures = verdict(records, cancel_latency_bound, pool_release_bound)
        evidence["failures"] = failures

        control_outcome, control_elapsed, control_payload = do_get(
            flight_client,
            f"SELECT n + 0 AS n FROM {quick_view_name(token)}",
            180.0,
        )
        evidence["control_after"] = {
            "outcome": control_outcome,
            "elapsed_seconds": round(control_elapsed, 3),
            "payload": control_payload,
        }
        if control_outcome != "ok" or control_payload != [{"n": QUICK_ITERATIONS}]:
            raise HarnessError(
                f"post-cancellation control query regressed: {control_outcome} {control_payload!r}"
            )

        if failures:
            print("\nFAIL: cancellation did not propagate:")
            for failure in failures:
                print(f"  - {failure}")
            return 1

        succeeded = True
        print("\nPASS: cancellation propagated on every attempt")
        return 0
    finally:
        if process is not None:
            stop_spiced(process)
        if log_handle is not None:
            log_handle.close()
        evidence["succeeded"] = succeeded
        write_json(output / "evidence.json", evidence)
        print(f"evidence={output}")
        if created_dataset and (
            cleanup == "always" or (cleanup == "on_success" and succeeded)
        ):
            bq_client.delete_dataset(
                dataset_ref.reference, delete_contents=True, not_found_ok=True
            )
            print(f"deleted dataset {dataset}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HarnessError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
