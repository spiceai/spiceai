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

"""Execute the anonymized BigQuery corpus through the release's real spiced.

Uses the credential, driver and binary contract in bigquery_federation.py. Every
run creates isolated tables from the offline corpus schemas and deletes its
datasets on exit. Most statements execute against empty tables, checking remote
SQL acceptance and the connector path without a result-correctness oracle. The
cohort-ratio statements execute last against synthetic rows with exact expected
results. The release gate also runs the nonempty federation, pushdown and JSON
harnesses with their result oracles.

Run with the pinned google-cloud-bigquery dependency, or use --self-test to
validate fixtures and the plan guard without credentials or a running service.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

import bigquery_federation as harness

FIXTURES = (
    harness.ROOT
    / "crates/data-connectors/connector-adbc/src/function_support_tests/fixtures/bigquery"
)
TABLE_FREE = {0, 1, 2, 3, 4, 80, 229, 230}
COHORT_CASES = {168, 169}
TRANSPORT = {
    "SchemaCastScanExec",
    "CoalescePartitionsExec",
    "CooperativeExec",
    "BytesProcessedExec",
}
PLAN_NODE = re.compile(r"^( *)(\w+Exec|VirtualExecutionPlan)(?=[: ]|$)", re.MULTILINE)


def corpus() -> list[tuple[int, str]]:
    sections = (FIXTURES / "queries.sql").read_text().split("-- query ")
    queries = [
        (int(index), sql.strip())
        for index, sql in (section.split("\n", 1) for section in sections[1:])
    ]
    if sections[0] or [index for index, _ in queries] != list(range(271)):
        raise harness.HarnessError(
            "Corpus must contain every index from 000 through 270 exactly once"
        )
    if any(not sql for _, sql in queries):
        raise harness.HarnessError("Corpus statements must not be empty")
    return queries


def bq_field(field: dict[str, Any]) -> bigquery.SchemaField:
    """Preserve native JSON, timestamp timezone and nested/repeated source types."""
    source = field["metadata"]["BIGQUERY:type"]
    dtype = field["data_type"]
    mode = "NULLABLE" if field["nullable"] else "REQUIRED"
    children = ()
    if isinstance(dtype, dict) and "List" in dtype:
        if source != "ARRAY<JSON>" or dtype["List"]["data_type"] != "Utf8":
            raise harness.HarnessError(f"Unsupported corpus array type: {source}")
        source, mode = "JSON", "REPEATED"
    elif isinstance(dtype, dict) and "Struct" in dtype:
        if not source.startswith("STRUCT<"):
            raise harness.HarnessError(f"Unsupported corpus struct type: {source}")
        source, children = "RECORD", tuple(bq_field(child) for child in dtype["Struct"])
    else:
        expected = {
            "STRING": "Utf8",
            "JSON": "Utf8",
            "INTEGER": "Int64",
            "FLOAT": "Float64",
            "BOOLEAN": "Boolean",
            "BYTES": "Binary",
            "DATE": "Date32",
            "TIMESTAMP": {"Timestamp": ["Microsecond", "UTC"]},
            "DATETIME": {"Timestamp": ["Microsecond", None]},
            "NUMERIC": {"Decimal128": [38, 9]},
        }
        if source not in expected or dtype != expected[source]:
            raise harness.HarnessError(
                f"Unsupported corpus type pair: {source}/{dtype}"
            )
    return bigquery.SchemaField(field["name"], source, mode=mode, fields=children)


def fixtures() -> tuple[
    list[dict[str, Any]], dict[tuple[str, str], list[bigquery.SchemaField]]
]:
    aliases = json.loads((FIXTURES / "schemas.json").read_text())
    tables = {}
    if len(aliases) != 72 or len({entry["alias"] for entry in aliases}) != 72:
        raise harness.HarnessError("Expected 72 unique corpus aliases")
    for entry in aliases:
        if entry["project"] != "federation-test":
            raise harness.HarnessError("Unexpected corpus project")
        key = (entry["dataset"], entry["table"])
        schema = [bq_field(field) for field in entry["schema"]["fields"]]
        if key in tables and tables[key] != schema:
            raise harness.HarnessError(f"Aliases disagree on source schema: {key}")
        tables[key] = schema
    if len(tables) != 70:
        raise harness.HarnessError("Expected 70 distinct corpus source tables")
    return aliases, tables


def check_plan(index: int, explain: str) -> None:
    physical = harness.physical_plan(explain)
    nodes = [(len(match[1]), match[2]) for match in PLAN_NODE.finditer(physical)]
    expected = 0 if index in TABLE_FREE else 1
    if (
        not nodes
        or sum(name == "VirtualExecutionPlan" for _, name in nodes) != expected
    ):
        raise harness.HarnessError(
            f"Query {index:03}: expected {expected} remote nodes"
        )
    if index in TABLE_FREE:
        if "TableScan:" in harness.logical_plan(explain):
            raise harness.HarnessError(
                f"Query {index:03}: table-free query acquired a scan"
            )
        return
    for line in physical.splitlines():
        match = PLAN_NODE.match(line)
        if match and match[2] in TRANSPORT and re.search(r"\bfetch=(?!None\b)", line):
            raise harness.HarnessError(
                f"Query {index:03}: transport must not impose a limit"
            )
    if nodes[-1][1] != "VirtualExecutionPlan" or [depth for depth, _ in nodes] != list(
        range(0, 2 * len(nodes), 2)
    ):
        raise harness.HarnessError(
            f"Query {index:03}: expected one chain ending at the remote leaf"
        )
    windows = 0
    for _, name in nodes[:-1]:
        if name in TRANSPORT:
            continue
        if index == 241:
            if name == "WindowAggExec":
                windows += 1
                continue
            if name in {"SortExec", "SortPreservingMergeExec", "RepartitionExec"}:
                continue
            if name == "ProjectionExec" and windows == 0:
                continue
        raise harness.HarnessError(f"Query {index:03}: unexpected local work: {name}")
    if index == 241 and (
        windows != 1
        or "median(" not in physical
        or "approx_percentile_cont(" not in physical
    ):
        raise harness.HarnessError(
            "Query 241 requires its median/approximate-percentile local window"
        )


def spicepod(
    aliases: list[dict[str, Any]], project: str, datasets: dict[str, str], driver: Path
) -> str:
    # JSON is also YAML, so paths and aliases cannot change the pod's structure.
    return (
        json.dumps(
            {
                "version": "v1",
                "kind": "Spicepod",
                "name": "bigquery-corpus-federated",
                "runtime": {"caching": {"sql_results": {"enabled": False}}},
                "datasets": [
                    {
                        "from": f"adbc:{datasets[entry['dataset']]}.{entry['table']}",
                        "name": entry["alias"],
                        "params": {
                            "adbc_driver": "bigquery",
                            "adbc_driver_path": str(driver),
                            "adbc_uri": f"bigquery:///{project}",
                            "connection_pool_size": "8",
                            "adbc_driver_options": "adbc.bigquery.sql.auth_type=adbc.bigquery.sql.auth_type.json_credential_string;adbc.bigquery.sql.auth_credentials=${secrets:BIGQUERY_SERVICE_ACCOUNT_JSON}",
                        },
                    }
                    for entry in aliases
                ],
            },
            indent=2,
        )
        + "\n"
    )


def seed_cohorts(
    client: bigquery.Client,
    project: str,
    location: str,
    datasets: dict[str, str],
    aliases: list[dict[str, Any]],
    tables: dict[tuple[str, str], list[bigquery.SchemaField]],
) -> dict[str, Any]:
    """Populate nonzero denominators and conversions on days one, three, four, seven."""
    conversions = {
        101: "2026-04-11T00:00:00Z",
        102: "2026-04-14T00:00:00Z",
        201: "2026-04-14T00:00:00Z",
        301: "2025-04-08T00:00:00Z",
    }
    rows = {
        "v0215": [
            {
                "v0177": person,
                "v0031": "value0110",
                "v0629": "value0192",
                "v0367": "2026-04-10T00:00:00Z",
            }
            for person in (101, 102)
        ]
        + [
            {
                "v0177": 201,
                "v0031": "value0073",
                "v0565": "value0230",
                "v0367": "2026-04-11T00:00:00Z",
            }
        ],
        "v0212": [{"id": 401, "v0177": 301}],
        "v0056": [{"v0175": 401, "v0201": 1.0, "v0090": "2025-04-01T00:00:00Z"}],
        "v0176": [
            {"id": person, "v0058": f"person-{person}"} for person in conversions
        ],
        "v0426": [
            {
                "v0171": f"person-{person}",
                "type": "value0016",
                "v0031": "value0231",
                "v0839": "(value0092)",
                "v0837": timestamp,
            }
            for person, timestamp in conversions.items()
        ],
        "v0326": [
            {"v0177": person, "v0323": timestamp}
            for person, timestamp in conversions.items()
        ],
    }
    sources = {entry["alias"]: entry for entry in aliases}
    evidence = {}
    for alias, records in rows.items():
        source = sources[alias]
        table = f"{project}.{datasets[source['dataset']]}.{source['table']}"
        # LOAD jobs do not enter the QUERY job census. WRITE_EMPTY also rejects
        # an accidentally reused fixture instead of appending duplicate rows.
        job = client.load_table_from_json(
            records,
            table,
            location=location,
            job_config=bigquery.LoadJobConfig(
                schema=tables[(source["dataset"], source["table"])],
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
            ),
        )
        job.result(timeout=90)
        evidence[alias] = {"rows": records, "load_job_id": job.job_id}
    return evidence


def cohort_expected_rows(index: int) -> list[dict[str, Any]]:
    counts = (
        ("v0845", "v0846", "v0847") if index == 168 else ("v0849", "v0850", "v0851")
    )
    return [
        {
            "v0808": day,
            counts[0]: 1 if day < 4 else 2,
            "v0810": 2,
            "v0811": 0.5 if day < 4 else 1.0,
            counts[1]: int(day >= 3),
            "v0813": 1,
            "v0814": float(day >= 3),
            counts[2]: int(day >= 7),
            "v0816": 1,
            "v0817": float(day >= 7),
        }
        for day in range(1, 8)
    ]


def execute_case(
    index: int,
    sql: str,
    port: int,
    output: Path,
    expected_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {"index": index, "errors": []}
    if expected_rows is not None:
        harness.write_json(output / f"{index:03}.expected.json", expected_rows)
    start = time.monotonic()
    for explain in (True, False):
        label = "explain" if explain else "result"
        try:
            status, _, body = harness.http_sql(
                port, f"EXPLAIN VERBOSE {sql}" if explain else sql
            )
            (output / f"{index:03}.{label}.json").write_text(body)
            record[f"{label}_status"] = status
            if status != 200:
                raise harness.HarnessError(f"{label}: HTTP {status}: {body}")
            if explain:
                check_plan(index, body)
            else:
                rows = json.loads(body)
                if not isinstance(rows, list):
                    raise harness.HarnessError("Result must be a JSON row array")
                record["rows"] = len(rows)
                if expected_rows is not None and rows != expected_rows:
                    raise harness.HarnessError(
                        f"Query {index:03}: rows differ from the nonempty fixture oracle"
                    )
        except (harness.HarnessError, ValueError, OSError) as error:
            record["errors"].append(str(error))
    record["seconds"] = round(time.monotonic() - start, 3)
    print(
        f"query {index:03}: {'FAIL' if record['errors'] else 'PASS'} rows={record.get('rows', 'unknown')}",
        flush=True,
    )
    return record


def main() -> int:
    queries = corpus()
    aliases, tables = fixtures()
    info, credential = harness.credential_info()
    project = os.environ.get("BIGQUERY_PROJECT_ID", info["project_id"])
    if not harness.PROJECT_ID_PATTERN.fullmatch(project):
        raise harness.HarnessError("Invalid BIGQUERY_PROJECT_ID")
    driver = harness.required_path("ADBC_BIGQUERY_DRIVER_PATH")
    binary = harness.required_path("SPICED_BIN", harness.DEFAULT_SPICED)
    stamp = f"{harness.utc_stamp().lower()}_{os.getpid()}"
    output = Path(
        os.environ.get(
            "BIGQUERY_TEST_OUTPUT",
            harness.ROOT / "target" / "bigquery-corpus-evidence" / stamp,
        )
    ).resolve()
    output.mkdir(parents=True, exist_ok=False)
    location = os.environ.get("BIGQUERY_LOCATION", "US")
    datasets = {dataset: f"spice_bq_corpus_{stamp}_{dataset}" for dataset, _ in tables}
    client = bigquery.Client(
        project=project,
        credentials=service_account.Credentials.from_service_account_info(info),
    )
    created = []
    process = None
    summary: dict[str, Any] = {
        "initial_fixture_rows_per_table": 0,
        "nonempty_query_indexes": sorted(COHORT_CASES),
        "queries": [],
        "errors": [],
        "datasets": datasets,
    }
    try:
        for dataset in datasets.values():
            reference = bigquery.Dataset(f"{project}.{dataset}")
            reference.location = location
            reference.labels = {"purpose": "spice-bigquery-corpus"}
            reference.default_table_expiration_ms = 86_400_000
            created.append(client.create_dataset(reference, timeout=30))
        for (dataset, table), schema in tables.items():
            client.create_table(
                bigquery.Table(f"{project}.{datasets[dataset]}.{table}", schema=schema),
                timeout=30,
            )
        pod_path = output / "spicepod.yaml"
        pod_path.write_text(spicepod(aliases, project, datasets, driver))
        environment = os.environ.copy()
        environment["BIGQUERY_SERVICE_ACCOUNT_JSON"] = credential
        environment.setdefault("RUST_MIN_STACK", str(64 * 1024 * 1024))
        http, flight = harness.distinct_free_ports()
        with (output / "spiced.log").open("wb") as log:
            process = subprocess.Popen(
                [
                    str(binary),
                    "--http",
                    f"127.0.0.1:{http}",
                    "--flight",
                    f"127.0.0.1:{flight}",
                    "--telemetry-enabled",
                    "false",
                    str(pod_path),
                ],
                cwd=output,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            harness.write_json(
                output / "candidate.json",
                {
                    "binary": str(binary),
                    "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                    "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
                    "pid": process.pid,
                    "http": http,
                    "flight": flight,
                },
            )
            harness.wait_until_ready(process, http, timeout=300)
            since = datetime.now(timezone.utc)
            for index, sql in queries:
                if index in COHORT_CASES:
                    continue
                summary["queries"].append(execute_case(index, sql, http, output))
                harness.write_json(output / "summary.json", summary)
            # Cohort ratios require nonzero denominators. Seed them only after
            # the other statements have executed against their empty fixtures.
            seed = seed_cohorts(client, project, location, datasets, aliases, tables)
            harness.write_json(output / "cohort-fixtures.json", seed)
            for index, sql in queries:
                if index not in COHORT_CASES:
                    continue
                summary["queries"].append(
                    execute_case(index, sql, http, output, cohort_expected_rows(index))
                )
                harness.write_json(output / "summary.json", summary)
            until = datetime.now(timezone.utc)
            # One shared observation window avoids waiting 90 seconds per query.
            # Query-result caching is disabled, including for duplicate SQL.
            jobs = harness.observe_jobs(
                lambda: harness.data_jobs(
                    client,
                    project,
                    location,
                    since,
                    until,
                    tuple(datasets.values()),
                    set(),
                ),
                expected=len(queries) - len(TABLE_FREE),
            )
            harness.write_json(output / "jobs.json", jobs)
            summary["execution_jobs_checked"] = True
            summary["execution_jobs"] = len(jobs)
            if len(jobs) != 263 or any(
                job["error_result"] or job["state"] != "DONE" for job in jobs
            ):
                summary["errors"].append(
                    "Expected 263 successful corpus execution jobs; see jobs.json"
                )
    except Exception as error:
        summary["errors"].append(str(error))
    finally:
        if process is not None:
            try:
                harness.stop_spiced(process)
            except Exception as error:
                summary["errors"].append(f"Stop spiced: {error}")
        # Attempt every owned dataset even if one deletion fails.
        for dataset in created:
            try:
                client.delete_dataset(
                    dataset.reference,
                    delete_contents=True,
                    not_found_ok=True,
                    timeout=30,
                )
            except Exception as error:
                summary["errors"].append(f"Cleanup {dataset.dataset_id}: {error}")
        if len(summary["queries"]) != 271:
            summary["errors"].append(
                f"Executed {len(summary['queries'])}/271 corpus cases"
            )
        summary["passed"] = (
            bool(summary.get("execution_jobs_checked"))
            and not summary["errors"]
            and all(not row["errors"] for row in summary["queries"])
        )
        harness.write_json(output / "summary.json", summary)
        client.close()
    print(
        f"corpus: {'PASS' if summary['passed'] else 'FAIL'}; evidence={output}",
        flush=True,
    )
    for error in summary["errors"]:
        print(f"ERROR: {error}", file=sys.stderr)
    return 0 if summary["passed"] else 1


def self_test() -> int:
    """Exercise the live gate's failure decisions without executing remote SQL."""
    import unittest
    import test_bigquery_corpus

    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromModule(test_bigquery_corpus)
    )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    try:
        raise SystemExit(self_test() if "--self-test" in sys.argv[1:] else main())
    except harness.HarnessError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(2) from error
