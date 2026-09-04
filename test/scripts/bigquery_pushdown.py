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

"""Run mutation-resistant BigQuery SQL-pushdown checks through a real spiced.

This is intentionally an opt-in integration harness, not a vacuous skip. It
fails at startup unless a service-account credential and the pinned ADBC
BigQuery driver are supplied. The companion shell script installs the one
Python dependency at a pinned version.

Run from the repository root:

    BIGQUERY_SERVICE_ACCOUNT_JSON_FILE=/path/to/service-account.json \
      ADBC_BIGQUERY_DRIVER_PATH=/path/to/libadbc_driver_bigquery \
      SPICED_BIN=target/debug/spiced \
      test/scripts/bigquery-pushdown.sh

By default the harness creates a unique dataset in the credential's project
and deletes it on exit. Set ``BIGQUERY_PROJECT_ID`` and ``BIGQUERY_LOCATION``
to override the project and location. To rerun an existing synthetic dataset,
set ``BIGQUERY_DATASET_MODE=reuse``, ``BIGQUERY_DATASET_ID``, and
``BIGQUERY_TEST_CLEANUP=never``. ``BIGQUERY_SERVICE_ACCOUNT_JSON`` accepts the
raw credential instead of a file; setting both credential variables is an
error. The harness never falls back to application-default credentials.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from google.oauth2 import service_account

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SPICED = ROOT / "target" / "debug" / "spiced"
DATASET_PREFIX = "spice_bigquery_pushdown"
DATASET_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")

QUERIES = {
    # --- fork PR #212 renderings, one per case ---
    # `date - date` is an Int64 day count in the plan and an INTERVAL in
    # BigQuery, so a bare `-` hands the next operator a duration.
    "date-difference": """SELECT id, d - e AS days
FROM temporal_values
ORDER BY id""",
    # BigQuery has no cast from DATE to INT64.
    "date-to-integer": """SELECT id, CAST(d AS BIGINT) AS epoch_day
FROM temporal_values
ORDER BY id""",
    # A civil timestamp compared against a civil literal: typed TIMESTAMP it
    # becomes an instant, which BigQuery gives no supertype against DATETIME.
    "naive-timestamp-compare": """SELECT id
FROM temporal_values
WHERE naive >= CAST('2026-05-11 00:00:00' AS TIMESTAMP)
ORDER BY id""",
    # BigQuery refuses a literal grouping key outright.
    "constant-group-by": """SELECT 'POOLED' AS bucket, COUNT(*) AS n
FROM temporal_values
GROUP BY 1""",
    # `array_element` is 1-based; a bare BigQuery subscript is 0-based.
    "array-element-literal": """SELECT id, array_element(arr, 1) AS first_el
FROM temporal_values
ORDER BY id""",
    # Control: an index whose sign is unknown cannot be rendered, so it must
    # evaluate locally rather than fail or read the neighbouring element.
    "array-element-non-literal-control": """SELECT id, array_element(arr, idx) AS nth_el
FROM temporal_values
ORDER BY id""",
    "union-distinct": """SELECT value
FROM union_values
WHERE value <= 2
UNION
SELECT value
FROM union_values
WHERE value >= 2
ORDER BY value""",
    "union-all-control": """SELECT value
FROM union_values
WHERE value <= 2
UNION ALL
SELECT value
FROM union_values
WHERE value >= 2
ORDER BY value""",
    "json-get-str": """SELECT
  case_id,
  json_get_str(native_doc, 'value') AS native_value,
  json_get_str(string_doc, 'value') AS string_value
FROM json_values
ORDER BY case_id""",
    "row-number": """SELECT
  grp,
  ord,
  amount,
  ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ord) AS row_num
FROM window_values
ORDER BY grp, ord""",
    "aggregate-window-control": """SELECT
  grp,
  ord,
  amount,
  SUM(amount) OVER (
    PARTITION BY grp
    ORDER BY ord
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_sum
FROM window_values
ORDER BY grp, ord""",
    # The PostgreSQL-idiom NULL-check over regexp_match. Pushes down whole:
    # the BigQuery provider's optimizer rule rewrites it into regexp_like
    # before its federation capability check, and the BigQuery dialect renders
    # that as REGEXP_CONTAINS. On a build without the rewrite this query fails with
    # `invalidQuery: Function not found: regexp_match`.
    "regexp-null-check": """SELECT
  id,
  CASE
    WHEN code IS NULL OR code = '' THEN 'UNKNOWN'
    WHEN NOT regexp_match(code, '^R[0-9]{2}') IS NULL THEN SUBSTRING(code, 1, 3)
    ELSE 'OTHER'
  END AS code_class
FROM regexp_values
ORDER BY id""",
    # Literal flags fold into the pattern as an inline (?i) group.
    "regexp-like-flags": """SELECT
  id,
  regexp_like(code, '^r[0-9]{2}', 'i') IS TRUE AS matches
FROM regexp_values
ORDER BY id""",
    # Control: a consumed match list has no BigQuery rendering (a NULL
    # top-level ARRAY comes back from BigQuery as an empty one), so the deny
    # keeps the call local — the scan federates, the NULLs stay NULL.
    "regexp-match-projection-control": """SELECT
  id,
  regexp_match(code, '^R[0-9]{2}') AS first_match
FROM regexp_values
ORDER BY id""",
    # Control: Rust's \d is Unicode-aware where BigQuery's RE2 \d is [0-9],
    # so this pattern must evaluate locally. If the engine-agreement gate is
    # ever removed, the pushed-down query keeps different rows (id 2) and this
    # case fails on its rows, not just its plan.
    "regexp-like-unicode-digit-control": """SELECT
  id,
  regexp_like(word, '^\\d+$') IS TRUE AS all_digits
FROM regexp_values
ORDER BY id""",
    # A GROUP BY expression reached through a wrapper in the SELECT list.
    # BigQuery matches a whole select item and a column reference and nothing in
    # between, so flattening the Projection onto the Aggregate makes it report
    # `booked_at` as neither grouped nor aggregated and refuse the statement. The
    # aggregate has to reach it in a scope of its own.
    "group-by-expr-nested-in-select": """SELECT
  CAST(CAST(date_trunc('week', booked_at) AS DATE) AS VARCHAR) AS week_start,
  COUNT(*) AS n
FROM bucket_values
GROUP BY date_trunc('week', booked_at)
ORDER BY week_start""",
    # Control: a grouped *column* wrapped in the select list needs no scope — a
    # column reference is matched wherever it appears. Without this the scope
    # would be paid on most grouped statements a BigQuery connector emits.
    "group-by-column-nested-in-select-control": """SELECT
  UPPER(tok) AS k,
  COUNT(*) AS n
FROM bucket_values
GROUP BY tok
ORDER BY k""",
    # A correlated subquery whose *outer* relation scans nothing. The federation
    # provider map is keyed off relations that scan something, so the constant
    # relation is absent from it; the whole statement still has to reach BigQuery
    # as one query rather than one scan per table reference.
    "correlated-subquery-over-constant-relation": """WITH keys AS (
  SELECT 1 AS k UNION ALL SELECT 2 AS k UNION ALL SELECT 3 AS k
)
SELECT
  keys.k,
  (SELECT COUNT(*) FROM union_values WHERE union_values.value = keys.k) AS n
FROM keys
ORDER BY keys.k""",
    # An aggregate window whose frame a plan normalizes to RANGE. BigQuery accepts
    # no NULL placement but its own inside a RANGE clause, and an ORDER BY with no
    # explicit frame implies RANGE for an aggregate, so `ASC NULLS LAST` is
    # refused. `aggregate-window-control` above cannot reach this: it names an
    # explicit ROWS frame, which accepts either placement.
    "aggregate-window-range-frame": """SELECT
  tok,
  COUNT(*) OVER (ORDER BY booked_at) AS running
FROM bucket_values
ORDER BY tok""",
    # Control: the same shape whose outer relation is a BigQuery table, which
    # federated whole before this change too. It tells a regression in the
    # scanless case apart from a regression in correlated pushdown generally.
    "correlated-subquery-over-scanning-relation-control": """SELECT
  u.value,
  (SELECT COUNT(*) FROM union_values v WHERE v.value = u.value) AS n
FROM union_values u
WHERE u.value = 3
ORDER BY u.value""",
}

EXPECTED_ROWS = {
    "date-difference": [
        {"id": 1, "days": 10},
        {"id": 2, "days": 18},
        {"id": 3, "days": -10},
    ],
    "date-to-integer": [
        {"id": 1, "epoch_day": 20584},
        {"id": 2, "epoch_day": 20592},
        {"id": 3, "epoch_day": 20574},
    ],
    "naive-timestamp-compare": [{"id": 1}, {"id": 2}],
    "constant-group-by": [{"bucket": "POOLED", "n": 3}],
    "array-element-literal": [
        {"id": 1, "first_el": 10},
        {"id": 2, "first_el": 40},
        {"id": 3, "first_el": 60},
    ],
    "array-element-non-literal-control": [
        {"id": 1, "nth_el": 10},
        {"id": 2, "nth_el": 50},
        {"id": 3, "nth_el": 60},
    ],
    "union-distinct": [{"value": 1}, {"value": 2}, {"value": 3}],
    "union-all-control": [
        {"value": 1},
        {"value": 1},
        {"value": 2},
        {"value": 2},
        {"value": 2},
        {"value": 2},
        {"value": 3},
    ],
    "json-get-str": [
        {"case_id": "array", "native_value": None, "string_value": None},
        {"case_id": "json_null", "native_value": None, "string_value": None},
        {"case_id": "missing", "native_value": None, "string_value": None},
        {"case_id": "object", "native_value": None, "string_value": None},
        {
            "case_id": "quoted_string",
            "native_value": 'say "hi"',
            "string_value": 'say "hi"',
        },
        {
            "case_id": "scalar_boolean",
            "native_value": None,
            "string_value": None,
        },
        {
            "case_id": "scalar_number",
            "native_value": None,
            "string_value": None,
        },
        {
            "case_id": "scalar_string",
            "native_value": "alpha",
            "string_value": "alpha",
        },
        {"case_id": "sql_null", "native_value": None, "string_value": None},
    ],
    "row-number": [
        {"grp": "a", "ord": 1, "amount": 10, "row_num": 1},
        {"grp": "a", "ord": 2, "amount": 20, "row_num": 2},
        {"grp": "a", "ord": 3, "amount": 5, "row_num": 3},
        {"grp": "b", "ord": 1, "amount": 7, "row_num": 1},
    ],
    "aggregate-window-control": [
        {"grp": "a", "ord": 1, "amount": 10, "running_sum": 10},
        {"grp": "a", "ord": 2, "amount": 20, "running_sum": 30},
        {"grp": "a", "ord": 3, "amount": 5, "running_sum": 35},
        {"grp": "b", "ord": 1, "amount": 7, "running_sum": 7},
    ],
    "regexp-null-check": [
        {"id": 1, "code_class": "R01"},
        {"id": 2, "code_class": "OTHER"},
        {"id": 3, "code_class": "OTHER"},
        {"id": 4, "code_class": "UNKNOWN"},
        {"id": 5, "code_class": "UNKNOWN"},
        {"id": 6, "code_class": "OTHER"},
    ],
    "regexp-like-flags": [
        {"id": 1, "matches": True},
        {"id": 2, "matches": True},
        {"id": 3, "matches": False},
        {"id": 4, "matches": False},
        {"id": 5, "matches": False},
        {"id": 6, "matches": False},
    ],
    "regexp-match-projection-control": [
        {"id": 1, "first_match": ["R01"]},
        {"id": 2, "first_match": None},
        {"id": 3, "first_match": None},
        {"id": 4, "first_match": None},
        {"id": 5, "first_match": None},
        {"id": 6, "first_match": None},
    ],
    "regexp-like-unicode-digit-control": [
        {"id": 1, "all_digits": True},
        {"id": 2, "all_digits": True},
        {"id": 3, "all_digits": False},
        {"id": 4, "all_digits": False},
        {"id": 5, "all_digits": False},
        {"id": 6, "all_digits": True},
    ],
    # The NULL timestamp buckets on its own, and sorts last: the plan's ORDER BY
    # normalizes to NULLS LAST, and the scope must carry that out to the caller
    # rather than leaving it inside the derived table.
    "group-by-expr-nested-in-select": [
        {"week_start": "2026-05-11", "n": 2},
        {"week_start": "2026-05-18", "n": 1},
        {"week_start": None, "n": 1},
    ],
    "group-by-column-nested-in-select-control": [
        {"k": "A", "n": 1},
        {"k": "B", "n": 1},
        {"k": "C", "n": 1},
        {"k": "D", "n": 1},
    ],
    "correlated-subquery-over-constant-relation": [
        {"k": 1, "n": 2},
        {"k": 2, "n": 2},
        {"k": 3, "n": 1},
    ],
    "correlated-subquery-over-scanning-relation-control": [
        {"value": 3, "n": 1},
    ],
    # `booked_at` is NULL for tok 'd'. The plan asks for NULLS LAST, so 'd' is the
    # last row of the ordering and its running count is 4; the other three follow
    # their timestamps. Under the reversed placement BigQuery would default to,
    # 'd' would be first and every one of these four numbers would differ — which
    # is what makes this case test the ordering and not just the SQL shape.
    "aggregate-window-range-frame": [
        {"tok": "a", "running": 1},
        {"tok": "b", "running": 2},
        {"tok": "c", "running": 3},
        {"tok": "d", "running": 4},
    ],
}


class HarnessError(RuntimeError):
    """A failed setup, runtime, query, or result invariant."""


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
        path = required_path("BIGQUERY_SERVICE_ACCOUNT_JSON_FILE")
        raw = path.read_text(encoding="utf-8")
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


def setup_sql(project: str, dataset: str) -> str:
    prefix = f"`{project}.{dataset}"
    return f"""CREATE OR REPLACE TABLE {prefix}.union_values` AS
SELECT value
FROM UNNEST([1, 1, 2, 2, 3]) AS value;

CREATE OR REPLACE TABLE {prefix}.json_values` AS
WITH source AS (
  SELECT 'scalar_string' AS case_id, JSON '{{"value":"alpha"}}' AS native_doc
  UNION ALL SELECT 'quoted_string', JSON r'{{"value":"say \\"hi\\""}}'
  UNION ALL SELECT 'scalar_number', JSON '{{"value":42}}'
  UNION ALL SELECT 'scalar_boolean', JSON '{{"value":true}}'
  UNION ALL SELECT 'object', JSON '{{"value":{{"key":"value"}}}}'
  UNION ALL SELECT 'array', JSON '{{"value":["x","y"]}}'
  UNION ALL SELECT 'json_null', JSON '{{"value":null}}'
  UNION ALL SELECT 'sql_null', CAST(NULL AS JSON)
  UNION ALL SELECT 'missing', JSON '{{"other":"value"}}'
)
SELECT
  case_id,
  native_doc,
  CASE WHEN case_id = 'sql_null' THEN NULL ELSE TO_JSON_STRING(native_doc) END AS string_doc
FROM source;

CREATE OR REPLACE TABLE {prefix}.window_values` AS
SELECT *
FROM UNNEST([
  STRUCT('a' AS grp, 1 AS ord, 10 AS amount),
  STRUCT('a' AS grp, 2 AS ord, 20 AS amount),
  STRUCT('a' AS grp, 3 AS ord, 5 AS amount),
  STRUCT('b' AS grp, 1 AS ord, 7 AS amount)
]);

CREATE OR REPLACE TABLE {prefix}.bucket_values` AS
SELECT *
FROM UNNEST([
  STRUCT(TIMESTAMP '2026-05-11 03:00:00' AS booked_at, 'a' AS tok),
  STRUCT(TIMESTAMP '2026-05-12 04:00:00', 'b'),
  STRUCT(TIMESTAMP '2026-05-19 05:00:00', 'c'),
  STRUCT(CAST(NULL AS TIMESTAMP), 'd')
]);

CREATE OR REPLACE TABLE {prefix}.temporal_values` AS
SELECT * FROM UNNEST([
  STRUCT(1 AS id, DATE '2026-05-11' AS d, DATE '2026-05-01' AS e,
         DATETIME '2026-05-11 03:00:00' AS naive, [10, 20, 30] AS arr, 1 AS idx),
  STRUCT(2, DATE '2026-05-19', DATE '2026-05-01',
         DATETIME '2026-05-19 05:00:00', [40, 50], 2),
  STRUCT(3, DATE '2026-05-01', DATE '2026-05-11',
         DATETIME '2026-05-01 00:00:00', [60], 1)
]);

CREATE OR REPLACE TABLE {prefix}.regexp_values` AS
SELECT *
FROM UNNEST([
  STRUCT(1 AS id, 'R01x' AS code, '123' AS word),
  STRUCT(2, 'r02y', '٣٤٥'),
  STRUCT(3, 'X99', 'abc'),
  STRUCT(4, '', ''),
  STRUCT(5, CAST(NULL AS STRING), CAST(NULL AS STRING)),
  STRUCT(6, 'zzR03', '42')
]);
"""


def spicepod(project: str, dataset: str, driver: Path) -> str:
    params = f"""      adbc_driver: bigquery
      adbc_driver_path: {driver}
      adbc_driver_options: adbc.bigquery.sql.auth_type=adbc.bigquery.sql.auth_type.json_credential_string;adbc.bigquery.sql.auth_credentials=${{secrets:BIGQUERY_SERVICE_ACCOUNT_JSON}}
      adbc_uri: bigquery:///{project}?DatasetId={dataset}"""
    return f"""version: v1
kind: Spicepod
name: bigquery-pushdown

datasets:
  - from: adbc:union_values
    name: union_values
    params: &bigquery_params
{params}
  - from: adbc:json_values
    name: json_values
    params: *bigquery_params
  - from: adbc:window_values
    name: window_values
    params: *bigquery_params
  - from: adbc:regexp_values
    name: regexp_values
    params: *bigquery_params
  - from: adbc:bucket_values
    name: bucket_values
    params: *bigquery_params
  - from: adbc:temporal_values
    name: temporal_values
    params: *bigquery_params
"""


def http_sql(http_port: int, sql: str) -> tuple[int, dict[str, str], str]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{http_port}/v1/sql",
        data=sql.encode(),
        headers={"Content-Type": "text/plain", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return (
                response.status,
                dict(response.headers.items()),
                response.read().decode(),
            )
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers.items()), error.read().decode()


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


def initial_physical_sql(explain_body: str) -> str:
    plans = json.loads(explain_body)
    plan = next(
        (
            entry["plan"]
            for entry in plans
            if entry["plan_type"] == "initial_physical_plan"
        ),
        None,
    )
    if plan is None or "base_sql=" not in plan:
        raise HarnessError(
            "EXPLAIN VERBOSE did not contain an initial physical base_sql plan"
        )
    return plan.split("base_sql=", 1)[1].strip()


def pushed_statement_count(explain_body: str) -> int:
    """How many statements the plan sends BigQuery, one per federated node.

    `initial_physical_sql` returns the first, which is all a dialect check needs.
    A pushdown check needs the count: a plan the federation analyzer refuses
    degrades to one scan per table reference, and every one of those is a
    separate BigQuery job.
    """
    plans = json.loads(explain_body)
    plan = next(
        (
            entry["plan"]
            for entry in plans
            if entry["plan_type"] == "initial_physical_plan"
        ),
        None,
    )
    if plan is None:
        raise HarnessError("EXPLAIN VERBOSE did not contain an initial physical plan")
    return sum(
        1
        for line in plan.splitlines()
        if "base_sql=" in line and "VirtualExecutionPlan" in line
    )


def assert_generated_sql(name: str, sql: str) -> None:
    if name == "date-difference":
        if "DATE_DIFF(" not in sql or re.search(r"`d` - `e`|`e` - `d`", sql):
            raise HarnessError(
                f"DATE - DATE was not pushed as DATE_DIFF, so BigQuery types the day "
                f"count as an INTERVAL: {sql}"
            )
    if name == "date-to-integer":
        if "UNIX_DATE(" not in sql or re.search(r"CAST\(`[^`]*`\.?`?d`? AS INT64\)", sql):
            raise HarnessError(
                f"CAST(date AS INT64) was not pushed as UNIX_DATE, a cast BigQuery does "
                f"not have: {sql}"
            )
    if name == "naive-timestamp-compare":
        if "DATETIME" not in sql:
            raise HarnessError(
                f"the civil timestamp was not typed DATETIME, so BigQuery has no "
                f"supertype for the comparison: {sql}"
            )
    if name == "constant-group-by":
        group_by = sql.split("GROUP BY", 1)[1] if "GROUP BY" in sql else ""
        if "CAST(" not in group_by:
            raise HarnessError(
                f"the constant grouping key was not cast, which BigQuery refuses as a "
                f"literal and other engines read as an ordinal: {sql}"
            )
    if name == "array-element-literal":
        if "SAFE_ORDINAL(1)" not in sql:
            raise HarnessError(
                f"array_element was not pushed 1-based, so a bare 0-based subscript "
                f"reads the neighbouring element: {sql}"
            )
    if name == "array-element-non-literal-control":
        if "SAFE_ORDINAL" in sql or "array_element" in sql:
            raise HarnessError(
                f"an index whose sign is unknown must not be pushed down; it has to "
                f"evaluate locally: {sql}"
            )
    if name == "union-distinct" and " UNION DISTINCT " not in sql:
        raise HarnessError(f"distinct union is not explicit in pushed SQL: {sql}")
    if name == "union-all-control" and " UNION ALL " not in sql:
        raise HarnessError(f"UNION ALL lost its duplicate-preserving quantifier: {sql}")
    if name == "json-get-str":
        normalized = "FORMAT('%t', JSON_QUERY("
        if sql.count(normalized) != 2 or "STARTS_WITH(JSON_QUERY(" in sql:
            raise HarnessError(
                f"JSON_QUERY results are not normalized before inspection: {sql}"
            )
    if name == "row-number":
        match = re.search(r"row_number\(\) OVER \((.*?)\) AS `row_num`", sql)
        if match is None:
            raise HarnessError(f"ROW_NUMBER was not pushed as a window function: {sql}")
        frame = match.group(1)
        if re.search(r"\b(ROWS|RANGE|GROUPS)\b", frame):
            raise HarnessError(f"ROW_NUMBER retained a forbidden frame: {sql}")
    if name == "aggregate-window-control":
        required = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        if required not in sql:
            raise HarnessError(f"the aggregate window frame was lost: {sql}")
    if name == "regexp-null-check":
        if "REGEXP_CONTAINS(" not in sql or "IS TRUE" not in sql:
            raise HarnessError(
                f"the NULL-check idiom was not pushed as REGEXP_CONTAINS: {sql}"
            )
        if "regexp_match" in sql or "regexp_like" in sql:
            raise HarnessError(
                f"a DataFusion regexp function leaked into BigQuery SQL: {sql}"
            )
    if name == "regexp-like-flags":
        if "R'(?i)^r[0-9]{2}'" not in sql:
            raise HarnessError(f"the i flag was not folded into the pattern: {sql}")
    if name == "regexp-match-projection-control":
        # Function calls only — the fixture table is itself named
        # `regexp_values`, so a bare substring check trips on the identifier.
        if "regexp_match(" in sql or "regexp_like(" in sql or "REGEXP_" in sql:
            raise HarnessError(
                f"a consumed match list has no BigQuery rendering and must stay local: {sql}"
            )
    if name == "regexp-like-unicode-digit-control":
        if "REGEXP_CONTAINS" in sql:
            raise HarnessError(
                f"a Unicode-divergent pattern must not push down, RE2 reads it differently: {sql}"
            )
    if name == "group-by-expr-nested-in-select":
        # The grouping expression must be rendered only inside the scope. Checked
        # on the rendered call, not on the base column: the dialect sanitises the
        # derived output's alias out of the schema name, which spells the base
        # column inside it.
        outer_select = sql.split(" FROM ", 1)[0]
        if "TIMESTAMP_TRUNC" in outer_select:
            raise HarnessError(
                f"the outer select list still re-derives the grouping expression, which "
                f"BigQuery cannot bind against its GROUP BY: {sql}"
            )
        if "GROUP BY" not in sql or "TIMESTAMP_TRUNC" not in sql:
            raise HarnessError(
                f"the scope has to carry the grouping expression and its GROUP BY: {sql}"
            )
    if name == "aggregate-window-range-frame":
        # Only the window's own ORDER BY is at issue; the statement's top-level
        # ORDER BY carries its NULLS clause perfectly well.
        over_clause = sql.split("OVER (", 1)[1].split(")", 1)[0]
        if "IS NULL ASC" not in over_clause:
            raise HarnessError(
                f"the NULL placement was not spelled as an ascending leading key, so "
                f"BigQuery refuses this RANGE window or orders NULLs at the wrong end: "
                f"{sql}"
            )
        if "NULLS" in over_clause:
            raise HarnessError(
                f"a NULLS clause survived inside the RANGE frame: {sql}"
            )
        if "RANGE" not in over_clause:
            raise HarnessError(f"the RANGE frame itself was lost: {sql}")
    if name == "group-by-column-nested-in-select-control":
        # A grouped column binds flattened, so the scope must not be paid here:
        # one statement, one SELECT, no derived table.
        if "FROM (SELECT" in sql:
            raise HarnessError(
                f"{name} binds as one SELECT for BigQuery, so it must not be scoped: {sql}"
            )


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


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


def main() -> int:
    info, compact_credential = credential_info()
    project = os.environ.get("BIGQUERY_PROJECT_ID", str(info["project_id"]))
    if not PROJECT_ID_PATTERN.fullmatch(project):
        raise HarnessError(f"Invalid BIGQUERY_PROJECT_ID: {project!r}")
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    dataset_mode = os.environ.get("BIGQUERY_DATASET_MODE", "create")
    if dataset_mode not in {"create", "reuse"}:
        raise HarnessError("BIGQUERY_DATASET_MODE must be create or reuse")
    supplied_dataset = os.environ.get("BIGQUERY_DATASET_ID")
    dataset = (
        supplied_dataset or f"{DATASET_PREFIX}_{utc_stamp().lower()}_{os.getpid()}"
    )
    if not DATASET_ID_PATTERN.fullmatch(dataset):
        raise HarnessError(f"Invalid BIGQUERY_DATASET_ID: {dataset!r}")
    if dataset_mode == "reuse" and not supplied_dataset:
        raise HarnessError("BIGQUERY_DATASET_MODE=reuse requires BIGQUERY_DATASET_ID")
    cleanup = os.environ.get("BIGQUERY_TEST_CLEANUP", "always")
    if cleanup not in {"always", "on_success", "never"}:
        raise HarnessError("BIGQUERY_TEST_CLEANUP must be always, on_success, or never")

    driver = required_path("ADBC_BIGQUERY_DRIVER_PATH")
    spiced = required_path("SPICED_BIN", DEFAULT_SPICED)
    output = Path(
        os.environ.get(
            "BIGQUERY_TEST_OUTPUT",
            ROOT / "target" / "bigquery-pushdown-evidence" / utc_stamp(),
        )
    ).resolve()
    output.mkdir(parents=True, exist_ok=False)

    credentials = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=project, credentials=credentials)
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    dataset_ref.labels = {"purpose": "spice-bigquery-pushdown"}
    created_dataset = False
    succeeded = False
    process: subprocess.Popen[bytes] | None = None
    log_handle = None
    started = datetime.now(timezone.utc)

    try:
        if dataset_mode == "create":
            try:
                client.create_dataset(dataset_ref)
                created_dataset = True
            except Conflict as error:
                raise HarnessError(
                    f"Dataset {project}.{dataset} already exists; choose a fresh BIGQUERY_DATASET_ID"
                ) from error
        else:
            existing = client.get_dataset(dataset_ref.reference)
            if existing.location.casefold() != location.casefold():
                raise HarnessError(
                    f"Dataset {project}.{dataset} is in {existing.location}, not {location}"
                )

        setup = setup_sql(project, dataset)
        (output / "setup.sql").write_text(setup, encoding="utf-8")
        setup_job = client.query(setup, location=location)
        setup_job.result()
        print(
            f"setup_job={setup_job.job_id} dataset={project}.{dataset} location={location}"
        )

        pod = spicepod(project, dataset, driver)
        pod_path = output / "spicepod.yaml"
        pod_path.write_text(pod, encoding="utf-8")
        binary_hash = hashlib.sha256(spiced.read_bytes()).hexdigest()
        version = subprocess.run(
            [str(spiced), "--version"], check=False, capture_output=True, text=True
        )
        (output / "candidate.txt").write_text(
            f"path={spiced}\nsha256={binary_hash}\nstdout={version.stdout.strip()}\n"
            f"stderr={version.stderr.strip()}\nexit={version.returncode}\n",
            encoding="utf-8",
        )

        http_port, flight_port = distinct_free_ports()
        log_handle = (output / "spiced.log").open("wb")
        environment = os.environ.copy()
        environment["BIGQUERY_SERVICE_ACCOUNT_JSON"] = compact_credential
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
        wait_until_ready(process, http_port, timeout=180)

        generated_sql: dict[str, str] = {}
        for name, query in QUERIES.items():
            (output / f"{name}.sql").write_text(query + ";\n", encoding="utf-8")
            status, headers, body = http_sql(http_port, query)
            write_json(output / f"{name}.headers.json", headers)
            (output / f"{name}.body").write_text(body, encoding="utf-8")
            if status != 200:
                raise HarnessError(f"{name} returned HTTP {status}: {body}")
            rows = json.loads(body)
            if rows != EXPECTED_ROWS[name]:
                raise HarnessError(
                    f"{name} returned wrong rows\nexpected={EXPECTED_ROWS[name]!r}\nactual={rows!r}"
                )

            explain_status, explain_headers, explain_body = http_sql(
                http_port, f"EXPLAIN VERBOSE {query}"
            )
            write_json(output / f"{name}.explain.headers.json", explain_headers)
            (output / f"{name}.explain.body").write_text(explain_body, encoding="utf-8")
            if explain_status != 200:
                raise HarnessError(
                    f"EXPLAIN VERBOSE for {name} returned HTTP {explain_status}: {explain_body}"
                )
            statements = pushed_statement_count(explain_body)
            if statements != 1:
                raise HarnessError(
                    f"{name} reaches BigQuery as {statements} statements, not one; "
                    f"every extra one is another BigQuery job:\n{explain_body[:2000]}"
                )
            pushed_sql = initial_physical_sql(explain_body)
            assert_generated_sql(name, pushed_sql)
            generated_sql[name] = pushed_sql
            print(f"{name}: ok ({statements} statement)")

        write_json(output / "generated-sql.json", generated_sql)
        jobs = []
        for job in sorted(
            client.list_jobs(min_creation_time=started, max_results=100),
            key=lambda item: item.created,
        ):
            query = getattr(job, "query", None)
            if query and not any(
                table in query
                for table in (
                    "union_values",
                    "json_values",
                    "window_values",
                    "regexp_values",
                    "bucket_values",
                )
            ):
                continue
            jobs.append(
                {
                    "job_id": job.job_id,
                    "created": job.created.isoformat(),
                    "ended": job.ended.isoformat() if job.ended else None,
                    "state": job.state,
                    "error_result": job.error_result,
                    "query": query,
                }
            )
        write_json(output / "bigquery-jobs.json", jobs)
        succeeded = True
        print(f"PASS evidence={output}")
        return 0
    finally:
        if process is not None:
            stop_spiced(process)
        if log_handle is not None:
            log_handle.close()
        should_cleanup = created_dataset and (
            cleanup == "always" or (cleanup == "on_success" and succeeded)
        )
        if should_cleanup:
            client.delete_dataset(
                dataset_ref.reference, delete_contents=True, not_found_ok=True
            )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HarnessError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
