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

"""Measure how many BigQuery jobs one inbound statement becomes.

A statement whose tables all live in one BigQuery project under one credential
is eligible to run as a single BigQuery job. This harness runs such a statement
through a real ``spiced`` and counts the jobs BigQuery actually received, so the
count is observed rather than inferred from a plan.

Three scenarios run against the same pod:

``same-dataset``
    Every table is in one BigQuery dataset. The statement references a CTE three
    times, so DataFusion inlines three copies of its subtree. This is the control
    that shows CTE inlining alone does not split the statement.

``cross-dataset``
    The same statement shape, with one table moved to a second dataset in the
    same project under the same credential.

``json-cross-dataset``
    The cross-dataset shape, projecting ``json_get_str``. A function the unparser
    cannot render is left above the federated scan and evaluated locally, which
    the job count alone would not reveal, so this scenario also asserts that the
    BigQuery JSON calls appear in the pushed SQL.

Every scenario is compared against the identical statement submitted straight to
BigQuery, so a job-count change is only meaningful alongside matching rows.

This is an opt-in integration harness, not a vacuous skip. It fails at startup
unless a service-account credential and the ADBC BigQuery driver are supplied.
The companion shell script installs the one Python dependency at a pinned
version.

Run from the repository root:

    BIGQUERY_SERVICE_ACCOUNT_JSON_FILE=/path/to/service-account.json \
      ADBC_BIGQUERY_DRIVER_PATH=/path/to/libadbc_driver_bigquery.dylib \
      SPICED_BIN=target/debug/spiced \
      test/scripts/bigquery-federation.sh

The credential's own project is used unless ``BIGQUERY_PROJECT_ID`` overrides it,
and ``BIGQUERY_LOCATION`` (default ``US``) selects the region. Two datasets are
created and deleted on exit; ``BIGQUERY_TEST_CLEANUP`` accepts ``always``,
``on_success``, or ``never``. ``BIGQUERY_SERVICE_ACCOUNT_JSON`` accepts the raw
credential instead of a file; setting both credential variables is an error. The
harness never falls back to application-default credentials.

Job counting reads ``INFORMATION_SCHEMA.JOBS_BY_USER``, which reports the jobs
this credential itself created, so it needs no project-wide job-list permission.
That view makes a job visible some time after it is created, so each scenario
watches it for a fixed window (``JOB_OBSERVATION_SECONDS``) rather than stopping
when the count settles, and treats the plan's statement count as a floor -- see
``observe_jobs``. Run ``--self-test`` to check that criterion without credentials.

Exit status is 0 when every scenario returns correct rows as a single BigQuery
job. ``BIGQUERY_EXPECT=split`` inverts the job-count expectation for the
cross-dataset scenario, which is how the pre-fix reproduction is recorded as a
pass rather than as a failed run.
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
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.oauth2 import service_account

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SPICED = ROOT / "target" / "debug" / "spiced"
DATASET_PREFIX = "spice_bigquery_federation"
DATASET_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")

# How long to watch the job census, and how often to read it. See `observe_jobs`
# for why this is a fixed window rather than a settle detector.
JOB_OBSERVATION_SECONDS = 90
JOB_POLL_SECONDS = 5

# `clients` is referenced three times, so DataFusion inlines three copies of its
# subtree. The two scenarios differ only in which dataset `entries` is read from.
STATEMENT_TEMPLATE = """WITH active_clients AS (
  SELECT client_id, token
  FROM {clients}
  WHERE archived_at IS NULL
),
advance_totals AS (
  SELECT a.client_id, SUM(a.amount) AS advance_total
  FROM {advances} AS a
  JOIN active_clients AS c ON c.client_id = a.client_id
  GROUP BY a.client_id
),
entry_totals AS (
  SELECT e.client_id, SUM(e.amount) AS entry_total
  FROM {entries} AS e
  JOIN active_clients AS c ON c.client_id = e.client_id
  GROUP BY e.client_id
)
SELECT
  c.token AS token,
  COALESCE(a.advance_total, 0) AS advance_total,
  COALESCE(t.entry_total, 0) AS entry_total
FROM active_clients AS c
LEFT JOIN advance_totals AS a ON a.client_id = c.client_id
LEFT JOIN entry_totals AS t ON t.client_id = c.client_id
ORDER BY c.token"""

# `json_get_str` is a Spice function with no BigQuery spelling of its own. If the
# unparser cannot render it, DataFusion leaves it above the federated scan and the
# JSON work happens locally, so this scenario asserts the pushed SQL carries the
# BigQuery JSON calls as well as counting the jobs.
JSON_STATEMENT = """WITH active_clients AS (
  SELECT client_id, token, {tier} AS tier
  FROM {clients}
  WHERE archived_at IS NULL
),
entry_totals AS (
  SELECT e.client_id, SUM(e.amount) AS entry_total
  FROM {entries} AS e
  JOIN active_clients AS c ON c.client_id = e.client_id
  GROUP BY e.client_id
)
SELECT
  c.token AS token,
  c.tier AS tier,
  COALESCE(t.entry_total, 0) AS entry_total
FROM active_clients AS c
LEFT JOIN entry_totals AS t ON t.client_id = c.client_id
ORDER BY c.token"""

SPICE_TIER = "json_get_str(metadata, 'tier')"
# What `json_get_str` means in BigQuery: the value only when the node is a JSON
# string, NULL for every other kind.
BIGQUERY_TIER = (
    "CASE WHEN STARTS_WITH(FORMAT('%t', JSON_QUERY(metadata, '$.\"tier\"')), '\"') "
    "THEN JSON_VALUE(metadata, '$.\"tier\"') END"
)

# `json_get_str` answers for a JSON string node only: 'gold' is one, 42 is a number,
# and tok-d has no `tier` at all. tok-c is archived and filtered out.
JSON_EXPECTED_ROWS = [
    {"token": "tok-a", "tier": "gold", "entry_total": 5.5},
    {"token": "tok-b", "tier": None, "entry_total": 14.5},
    {"token": "tok-d", "tier": None, "entry_total": 0.0},
]

SCENARIOS = {
    "same-dataset": {"clients": "clients", "advances": "advances", "entries": "entries_core"},
    "cross-dataset": {"clients": "clients", "advances": "advances", "entries": "entries_ledger"},
}

JSON_SCENARIO = "json-cross-dataset"

# Two datasets hold a table of the same name with different rows, reached by bare
# paths. Nothing in the emitted SQL says which dataset a bare name means, so if
# these are merged the statement reads one table twice and answers with the wrong
# rows -- silently. This scenario is a correctness check, not a job count.
BARE_SCENARIO = "bare-path-datasets"
BARE_STATEMENT = """SELECT
  a.src AS core_src,
  b.src AS ledger_src,
  a.n + b.n AS total
FROM t_core AS a
CROSS JOIN t_ledger AS b"""
BARE_CONTROL_TEMPLATE = """SELECT
  a.src AS core_src,
  b.src AS ledger_src,
  a.n + b.n AS total
FROM {core_t} AS a
CROSS JOIN {ledger_t} AS b"""
BARE_EXPECTED_ROWS = [{"core_src": "from_core", "ledger_src": "from_ledger", "total": 3}]


class HarnessError(RuntimeError):
    """A harness precondition or assertion failed."""


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def required_path(name: str, default: Path | None = None) -> Path:
    value = os.environ.get(name)
    if value:
        return Path(value).expanduser().resolve()
    if default is not None and default.exists():
        return default.resolve()
    raise HarnessError(f"Set {name} to an existing path.")


def credential_info() -> tuple[dict[str, Any], str]:
    raw = os.environ.get("BIGQUERY_SERVICE_ACCOUNT_JSON")
    credential_file = os.environ.get("BIGQUERY_SERVICE_ACCOUNT_JSON_FILE")
    if not raw and not credential_file:
        raise HarnessError(
            "Set BIGQUERY_SERVICE_ACCOUNT_JSON or BIGQUERY_SERVICE_ACCOUNT_JSON_FILE. "
            "The harness never falls back to application-default credentials."
        )
    if raw and credential_file:
        raise HarnessError(
            "Set exactly one of BIGQUERY_SERVICE_ACCOUNT_JSON and "
            "BIGQUERY_SERVICE_ACCOUNT_JSON_FILE."
        )
    if credential_file:
        path = required_path("BIGQUERY_SERVICE_ACCOUNT_JSON_FILE")
        raw = path.read_text(encoding="utf-8")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as error:
        raise HarnessError("The credential is not valid JSON.") from error
    if info.get("type") != "service_account" or not info.get("project_id"):
        raise HarnessError("The credential must be a service-account key with a project_id.")
    return info, json.dumps(info, separators=(",", ":"))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def distinct_free_ports() -> tuple[int, int]:
    first = free_port()
    second = free_port()
    while second == first:
        second = free_port()
    return first, second


def setup_sql(project: str, core: str, ledger: str, bare_table: str) -> str:
    return f"""CREATE OR REPLACE TABLE `{project}.{core}.clients` AS
SELECT * FROM UNNEST([
  STRUCT(1 AS client_id, 'tok-a' AS token, CAST(NULL AS TIMESTAMP) AS archived_at,
         JSON '{{"tier":"gold"}}' AS metadata),
  STRUCT(2, 'tok-b', CAST(NULL AS TIMESTAMP), JSON '{{"tier":42}}'),
  STRUCT(3, 'tok-c', TIMESTAMP '2026-01-01 00:00:00+00', JSON '{{"tier":"bronze"}}'),
  STRUCT(4, 'tok-d', CAST(NULL AS TIMESTAMP), JSON '{{"other":"x"}}')
]);

CREATE OR REPLACE TABLE `{project}.{core}.advances` AS
SELECT * FROM UNNEST([
  STRUCT(1 AS advance_id, 1 AS client_id, 100.5 AS amount),
  STRUCT(2, 1, 20.25),
  STRUCT(3, 2, 7.0),
  STRUCT(4, 3, 999.0)
]);

CREATE OR REPLACE TABLE `{project}.{core}.entries_core` AS
SELECT * FROM UNNEST([
  STRUCT(1 AS entry_id, 1 AS client_id, 5.5 AS amount),
  STRUCT(2, 2, 11.25),
  STRUCT(3, 2, 3.25),
  STRUCT(4, 3, 42.0)
]);

CREATE OR REPLACE TABLE `{project}.{core}.{bare_table}` AS SELECT 'from_core' AS src, 1 AS n;

CREATE OR REPLACE TABLE `{project}.{ledger}.{bare_table}` AS SELECT 'from_ledger' AS src, 2 AS n;

CREATE OR REPLACE TABLE `{project}.{ledger}.entries_ledger` AS
SELECT * FROM UNNEST([
  STRUCT(1 AS entry_id, 1 AS client_id, 5.5 AS amount),
  STRUCT(2, 2, 11.25),
  STRUCT(3, 2, 3.25),
  STRUCT(4, 3, 42.0)
]);
"""


def spicepod(project: str, core: str, ledger: str, driver: Path, bare_table: str) -> str:
    """Build the pod the way a real one is written.

    Every dataset shares one URI, one project and one credential, and names its own
    dataset twice over — once as the first part of a dataset-qualified path, and
    once as `adbc.bigquery.sql.dataset_id` in the driver options. Both are how a
    dataset actually reaches the connector, so both belong in the fixture.
    """

    def dataset(name: str, schema: str, table: str) -> str:
        return f"""  - from: adbc:{schema}.{table}
    name: {name}
    params:
      adbc_driver: bigquery
      adbc_driver_path: {driver}
      adbc_driver_options: adbc.bigquery.sql.project_id={project};adbc.bigquery.sql.dataset_id={schema};adbc.bigquery.sql.auth_type=adbc.bigquery.sql.auth_type.json_credential_string;adbc.bigquery.sql.auth_credentials=${{secrets:BIGQUERY_SERVICE_ACCOUNT_JSON}}
      adbc_uri: bigquery:///{project}
      connection_pool_size: 8
"""

    def bare_dataset(name: str, schema: str, table: str) -> str:
        """A dataset named only by its table, with the dataset in the driver options.

        The emitted reference is bare, so these two must not be joined into one
        statement however alike their configuration looks.
        """
        return f"""  - from: adbc:{table}
    name: {name}
    params:
      adbc_driver: bigquery
      adbc_driver_path: {driver}
      adbc_driver_options: adbc.bigquery.sql.project_id={project};adbc.bigquery.sql.dataset_id={schema};adbc.bigquery.sql.auth_type=adbc.bigquery.sql.auth_type.json_credential_string;adbc.bigquery.sql.auth_credentials=${{secrets:BIGQUERY_SERVICE_ACCOUNT_JSON}}
      adbc_uri: bigquery:///{project}
      connection_pool_size: 8
"""

    entries = "".join(
        [
            dataset("clients", core, "clients"),
            dataset("advances", core, "advances"),
            dataset("entries_core", core, "entries_core"),
            dataset("entries_ledger", ledger, "entries_ledger"),
            bare_dataset("t_core", core, bare_table),
            bare_dataset("t_ledger", ledger, bare_table),
        ]
    )
    return f"""version: v1
kind: Spicepod
name: bigquery-federation

datasets:
{entries}"""


def http_sql(http_port: int, sql: str, timeout: int = 120) -> tuple[int, dict[str, str], str]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{http_port}/v1/sql",
        data=sql.encode(),
        headers={"Content-Type": "text/plain", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers.items()), response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers.items()), error.read().decode()


def wait_until_ready(process: subprocess.Popen[bytes], http_port: int, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_observation = "no response"
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise HarnessError(f"spiced exited before readiness with status {return_code}")
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
    raise HarnessError(f"spiced was not ready after {timeout}s; last observation: {last_observation}")


def physical_plan(explain_body: str) -> str:
    plans = json.loads(explain_body)
    plan = next(
        (entry["plan"] for entry in plans if entry["plan_type"] == "initial_physical_plan"),
        None,
    )
    if plan is None:
        raise HarnessError("EXPLAIN VERBOSE did not contain an initial physical plan")
    return plan


def logical_plan(explain_body: str) -> str:
    plans = json.loads(explain_body)
    plan = next(
        (entry["plan"] for entry in plans if entry["plan_type"] == "logical_plan"),
        None,
    )
    if plan is None:
        raise HarnessError("EXPLAIN VERBOSE did not contain a logical plan")
    return plan


VIRTUAL_PLAN = re.compile(
    r"VirtualExecutionPlan name=(?P<name>\S+) compute_context=(?P<context>\S+) base_sql=(?P<sql>.*)"
)


def federated_nodes(plan: str) -> list[dict[str, str]]:
    """Each VirtualExecutionPlan is one federated sub-plan, so one remote statement.

    `compute_context` is the identity the federation optimizer groups on: sub-plans
    only merge when it matches. For ADBC it is a hash, so it names the boundary
    without disclosing the connection.
    """
    return [match.groupdict() for match in VIRTUAL_PLAN.finditer(plan)]


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def stop_spiced(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def data_jobs(
    client: bigquery.Client,
    project: str,
    location: str,
    since: datetime,
    until: datetime,
    datasets: tuple[str, ...],
    exclude: set[str],
) -> list[dict[str, Any]]:
    """Return the query jobs spiced ran against the synthetic tables.

    The harness shares one credential with spiced, so its own control query would
    otherwise be counted too; `exclude` drops it. Schema discovery reads
    INFORMATION_SCHEMA and is excluded as well, being no part of executing the
    statement.
    """
    region = f"region-{location.lower()}"
    predicate = " OR ".join(f"STRPOS(query, '{dataset}') > 0" for dataset in datasets)
    sql = f"""
SELECT job_id, creation_time, query, total_bytes_processed, state, error_result
FROM `{project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE job_type = 'QUERY'
  AND creation_time BETWEEN @since AND @until
  AND ({predicate})
  AND STRPOS(UPPER(query), 'INFORMATION_SCHEMA') = 0
ORDER BY creation_time
"""
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("since", "TIMESTAMP", since),
            bigquery.ScalarQueryParameter("until", "TIMESTAMP", until),
        ]
    )
    rows = list(client.query(sql, location=location, job_config=config).result())
    return [dict(row) for row in rows if row["job_id"] not in exclude]


def observe_jobs(
    sample: Any,
    expected: int,
    *,
    window: int = JOB_OBSERVATION_SECONDS,
    interval: int = JOB_POLL_SECONDS,
    sleep: Any = time.sleep,
    clock: Any = time.monotonic,
) -> list[dict[str, Any]]:
    """Watch the job census for a fixed window and return the final reading.

    `INFORMATION_SCHEMA.JOBS_BY_USER` makes a job visible some time after it is
    created, and nothing the harness can see bounds that lag. Stopping as soon as
    the count holds steady therefore cannot tell "the engine ran one job" apart
    from "the rest are not visible yet" — and a split becomes visible one job at a
    time, so the first steady reading of three jobs is usually one. That criterion
    fails towards a pass, which is the one direction this measurement must never
    fail in, since a single job is what the fix claims.

    So the window is fixed and always runs to completion, and `expected` — the
    number of statements the plan will push — is a floor. A census that never
    reaches it has not observed the run and says so, rather than returning a
    partial count for an assertion to read as success.
    """
    deadline = clock() + window
    observed: list[dict[str, Any]] = []
    while clock() < deadline:
        sleep(interval)
        observed = sample()
    if len(observed) < expected:
        raise HarnessError(
            f"the job census saw {len(observed)} job(s) in {window}s but the plan pushes "
            f"{expected} statement(s); the run was not observed, so its job count is unknown"
        )
    return observed


class _StepClock:
    """A clock that only advances when the code under test sleeps."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _scheduled_sample(schedule: list[list[str]]) -> Any:
    """A census that returns `schedule[n]` on the nth read, holding the last entry."""
    reads = {"n": 0}

    def sample() -> list[dict[str, Any]]:
        index = min(reads["n"], len(schedule) - 1)
        reads["n"] += 1
        return [{"job_id": job_id, "query": ""} for job_id in schedule[index]]

    return sample


def _stops_when_steady(
    sample: Any, *, interval: int, sleep: Any, clock: Any, window: int = 120
) -> list[dict[str, Any]]:
    """Stop as soon as the count holds steady for three reads.

    This is the tempting way to end the observation, and `observe_jobs` must never
    be simplified back into it: a split becomes visible one job at a time, so this
    returns the first job on its own and an assertion of "exactly one job" reads
    that as success. `self_test` keeps a counter-example on hand.
    """
    deadline = clock() + window
    observed: list[dict[str, Any]] = []
    stable = 0
    while clock() < deadline and stable < 3:
        sleep(interval)
        latest = sample()
        if latest and len(latest) == len(observed):
            stable += 1
        else:
            stable = 0
        observed = latest
    return observed


def self_test() -> int:
    """Check the observation criterion without touching BigQuery."""
    failures: list[str] = []

    # One job visible first, the other two only after four reads -- what a split
    # looks like through a lagging census.
    delayed = [["a"], ["a"], ["a"], ["a"], ["a", "b", "c"]]

    clock = _StepClock()
    steady = _stops_when_steady(
        _scheduled_sample(delayed), interval=JOB_POLL_SECONDS, sleep=clock.sleep, clock=clock
    )
    if len(steady) != 1:
        failures.append(
            f"counter-example did not reproduce: a settle detector returned {len(steady)}, expected 1"
        )

    clock = _StepClock()
    observed = observe_jobs(
        _scheduled_sample(delayed), 3, sleep=clock.sleep, clock=clock
    )
    if len(observed) != 3:
        failures.append(
            f"a delayed split was miscounted: observed {len(observed)} job(s), expected 3"
        )

    clock = _StepClock()
    single = observe_jobs(_scheduled_sample([["a"]]), 1, sleep=clock.sleep, clock=clock)
    if len(single) != 1:
        failures.append(f"a steady single job was miscounted as {len(single)}")

    clock = _StepClock()
    try:
        observe_jobs(_scheduled_sample([["a"]]), 3, sleep=clock.sleep, clock=clock)
    except HarnessError:
        pass
    else:
        failures.append("a census short of the plan's statement count did not fail")

    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(
        "self-test ok: a settle detector reports 1 job for a delayed split; "
        "the fixed window reports 3, counts a steady single job as 1, and fails "
        "when the census never reaches the plan's statement count"
    )
    return 0


def main() -> int:
    info, compact_credential = credential_info()
    project = os.environ.get("BIGQUERY_PROJECT_ID", str(info["project_id"]))
    if not PROJECT_ID_PATTERN.fullmatch(project):
        raise HarnessError(f"Invalid BIGQUERY_PROJECT_ID: {project!r}")
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    cleanup = os.environ.get("BIGQUERY_TEST_CLEANUP", "always")
    if cleanup not in {"always", "on_success", "never"}:
        raise HarnessError("BIGQUERY_TEST_CLEANUP must be always, on_success, or never")
    expect = os.environ.get("BIGQUERY_EXPECT", "single")
    if expect not in {"single", "split"}:
        raise HarnessError("BIGQUERY_EXPECT must be single or split")

    stamp = f"{utc_stamp().lower()}_{os.getpid()}"
    core = f"{DATASET_PREFIX}_{stamp}_core"
    ledger = f"{DATASET_PREFIX}_{stamp}_ledger"
    # Both datasets hold a table of this name. It is stamped so the job census can
    # match it: a bare reference does not name its dataset, so the dataset-name
    # predicate cannot find those jobs.
    bare_table = f"t_{stamp}"
    for dataset in (core, ledger):
        if not DATASET_ID_PATTERN.fullmatch(dataset):
            raise HarnessError(f"Invalid dataset id: {dataset!r}")

    driver = required_path("ADBC_BIGQUERY_DRIVER_PATH")
    spiced = required_path("SPICED_BIN", DEFAULT_SPICED)
    output = Path(
        os.environ.get(
            "BIGQUERY_TEST_OUTPUT",
            ROOT / "target" / "bigquery-federation-evidence" / utc_stamp(),
        )
    ).resolve()
    output.mkdir(parents=True, exist_ok=False)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=project, credentials=credentials)
    created: list[bigquery.Dataset] = []
    succeeded = False
    process: subprocess.Popen[bytes] | None = None
    log_handle = None
    failures: list[str] = []

    try:
        for dataset in (core, ledger):
            reference = bigquery.Dataset(f"{project}.{dataset}")
            reference.location = location
            reference.labels = {"purpose": "spice-bigquery-federation"}
            try:
                created.append(client.create_dataset(reference))
            except Conflict as error:
                raise HarnessError(
                    f"Dataset {project}.{dataset} already exists; rerun for a fresh stamp"
                ) from error

        setup = setup_sql(project, core, ledger, bare_table)
        (output / "setup.sql").write_text(setup, encoding="utf-8")
        setup_job = client.query(setup, location=location)
        setup_job.result()
        # The setup job references both datasets, so it matches the same predicates
        # the job census uses. A scenario's window opens five seconds before its
        # query, which a fast startup could stretch back over this job.
        harness_jobs = {setup_job.job_id}
        print(f"setup_job={setup_job.job_id} datasets={project}.{{{core},{ledger}}} location={location}")

        pod = spicepod(project, core, ledger, driver, bare_table)
        pod_path = output / "spicepod.yaml"
        pod_path.write_text(pod, encoding="utf-8")

        binary_hash = hashlib.sha256(spiced.read_bytes()).hexdigest()
        version = subprocess.run([str(spiced), "--version"], check=False, capture_output=True, text=True)
        (output / "candidate.txt").write_text(
            f"path={spiced}\nsha256={binary_hash}\nstdout={version.stdout.strip()}\n"
            f"stderr={version.stderr.strip()}\nexit={version.returncode}\n",
            encoding="utf-8",
        )
        print(f"spiced={spiced} sha256={binary_hash} version={version.stdout.strip()}")

        http_port, flight_port = distinct_free_ports()
        log_handle = (output / "spiced.log").open("wb")
        environment = os.environ.copy()
        environment["BIGQUERY_SERVICE_ACCOUNT_JSON"] = compact_credential
        # Planning this statement recurses deeply enough to overflow a worker
        # thread's default 2 MiB stack in an unoptimized build, which aborts the
        # process. Tokio's workers take std's default stack size, so raising it
        # here keeps a debug `spiced` usable as the harness binary.
        environment.setdefault("RUST_MIN_STACK", str(64 * 1024 * 1024))
        process = subprocess.Popen(
            [
                str(spiced),
                "--http", f"127.0.0.1:{http_port}",
                "--flight", f"127.0.0.1:{flight_port}",
                "--telemetry-enabled", "false",
                str(pod_path),
            ],
            cwd=ROOT,
            env=environment,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        wait_until_ready(process, http_port, timeout=300)

        summary: dict[str, Any] = {
            "project": project,
            "location": location,
            "datasets": {"core": core, "ledger": ledger},
            "spiced_sha256": binary_hash,
            "expect": expect,
            "scenarios": {},
        }

        plan = []
        for name, tables in SCENARIOS.items():
            plan.append(
                (
                    name,
                    STATEMENT_TEMPLATE.format(**tables),
                    STATEMENT_TEMPLATE.format(
                        clients=f"`{project}.{core}.clients`",
                        advances=f"`{project}.{core}.advances`",
                        entries=f"`{project}.{core}.entries_core`"
                        if name == "same-dataset"
                        else f"`{project}.{ledger}.entries_ledger`",
                    ),
                )
            )
        plan.append(
            (
                JSON_SCENARIO,
                JSON_STATEMENT.format(
                    clients="clients", entries="entries_ledger", tier=SPICE_TIER
                ),
                JSON_STATEMENT.format(
                    clients=f"`{project}.{core}.clients`",
                    entries=f"`{project}.{ledger}.entries_ledger`",
                    tier=BIGQUERY_TIER,
                ),
            )
        )

        plan.append(
            (
                BARE_SCENARIO,
                BARE_STATEMENT,
                BARE_CONTROL_TEMPLATE.format(
                    core_t=f"`{project}.{core}.{bare_table}`",
                    ledger_t=f"`{project}.{ledger}.{bare_table}`",
                ),
            )
        )

        for name, statement, control_statement in plan:
            (output / f"{name}.sql").write_text(statement + ";\n", encoding="utf-8")

            status, _, explain_body = http_sql(http_port, f"EXPLAIN VERBOSE {statement}")
            if status != 200:
                raise HarnessError(f"{name} EXPLAIN returned HTTP {status}: {explain_body}")
            physical = physical_plan(explain_body)
            logical = logical_plan(explain_body)
            (output / f"{name}.physical_plan.txt").write_text(physical, encoding="utf-8")
            (output / f"{name}.logical_plan.txt").write_text(logical, encoding="utf-8")
            nodes = federated_nodes(physical)
            statements = [node["sql"].strip() for node in nodes]
            contexts = sorted({node["context"] for node in nodes})
            (output / f"{name}.pushed_sql.txt").write_text(
                "\n\n".join(statements) + "\n", encoding="utf-8"
            )
            federated_markers = len(re.findall(r"^\s*Federated\s*$", logical, re.MULTILINE))

            # A margin either side absorbs clock skew between this host and
            # BigQuery's own creation_time.
            since = datetime.now(timezone.utc) - timedelta(seconds=5)
            status, headers, body = http_sql(http_port, statement)
            until = datetime.now(timezone.utc) + timedelta(seconds=5)
            write_json(output / f"{name}.headers.json", headers)
            (output / f"{name}.body").write_text(body, encoding="utf-8")
            if status != 200:
                raise HarnessError(f"{name} returned HTTP {status}: {body}")
            rows = json.loads(body)

            (output / f"{name}.control.sql").write_text(
                control_statement + ";\n", encoding="utf-8"
            )
            control_job = client.query(control_statement, location=location)
            control = [
                {key: value for key, value in dict(row).items()} for row in control_job.result()
            ]
            # This credential is spiced's too, so the harness's own queries would
            # otherwise show up as jobs spiced ran.
            harness_jobs.add(control_job.job_id)

            observed = observe_jobs(
                lambda: data_jobs(
                    client,
                    project,
                    location,
                    since,
                    until,
                    (core, ledger, bare_table),
                    harness_jobs,
                ),
                len(statements),
            )

            texts = [job["query"] for job in observed]
            scenario = {
                "statement": statement,
                "federated_logical_nodes": federated_markers,
                "federated_sql_statements": len(statements),
                "distinct_compute_contexts": contexts,
                "bigquery_job_count": len(observed),
                "distinct_bigquery_sql": len(set(texts)),
                "bigquery_job_ids": [job["job_id"] for job in observed],
                "rows": rows,
                "control_rows": control,
                "control_job_id": control_job.job_id,
                "rows_match_control": rows == control,
            }
            summary["scenarios"][name] = scenario
            write_json(output / f"{name}.jobs.json", observed)

            print(
                f"{name}: federated_roots={federated_markers} federated_sql={len(statements)} "
                f"compute_contexts={len(contexts)} bigquery_jobs={len(observed)} "
                f"distinct_sql={len(set(texts))} rows_match_control={rows == control}"
            )

            if rows != control:
                failures.append(
                    f"{name}: rows differ from the direct BigQuery control\n"
                    f"  spice  ={rows!r}\n  bigquery={control!r}"
                )
            expected_single = (
                expect == "single" or name == "same-dataset"
            ) and name != BARE_SCENARIO
            if expected_single and len(observed) != 1:
                failures.append(
                    f"{name}: expected one BigQuery job, observed {len(observed)} "
                    f"({len(set(texts))} distinct SQL texts)"
                )
            if not expected_single and len(observed) <= 1:
                failures.append(
                    f"{name}: BIGQUERY_EXPECT=split expected more than one BigQuery job, "
                    f"observed {len(observed)}"
                )
            if name == BARE_SCENARIO:
                if rows != BARE_EXPECTED_ROWS:
                    failures.append(
                        f"{name}: bare table references were resolved against the wrong "
                        f"dataset\n  expected={BARE_EXPECTED_ROWS!r}\n  actual  ={rows!r}"
                    )
                if len(contexts) < 2:
                    failures.append(
                        f"{name}: the two datasets share one compute context, so a merged "
                        f"statement can read one table twice; contexts={contexts}"
                    )
            if name == JSON_SCENARIO:
                pushed = "\n".join(statements)
                missing = [
                    call for call in ("JSON_QUERY", "JSON_VALUE") if call not in pushed
                ]
                if missing:
                    failures.append(
                        f"{name}: json_get_str was not pushed to BigQuery; the generated SQL "
                        f"is missing {missing}. It is being evaluated locally instead."
                    )
                if rows != JSON_EXPECTED_ROWS:
                    failures.append(
                        f"{name}: json_get_str returned the wrong rows\n"
                        f"  expected={JSON_EXPECTED_ROWS!r}\n  actual  ={rows!r}"
                    )

        write_json(output / "summary.json", summary)
        succeeded = not failures
        if failures:
            print("\nFAILED:", file=sys.stderr)
            for failure in failures:
                print(f"  {failure}", file=sys.stderr)
        print(f"\nevidence={output}")
        return 0 if succeeded else 1
    finally:
        if process is not None:
            stop_spiced(process)
        if log_handle is not None:
            log_handle.close()
        if cleanup == "always" or (cleanup == "on_success" and succeeded):
            for dataset in created:
                client.delete_dataset(dataset.reference, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    try:
        if "--self-test" in sys.argv[1:]:
            raise SystemExit(self_test())
        raise SystemExit(main())
    except HarnessError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2) from error
